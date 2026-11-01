package blobfs

// 本文件是 blob 仓库的 **GC 回收**（缺口 4 后半）：清崩溃残留（.tmp 临时文件、孤儿目录），
// 可选按 mtime 回收超期的完整 blob。CLI 面：`alasol blob-gc`（读 [blob].dir）。
//
// # 三扫（确定性顺序：按目录名排序遍历——os.ReadDir 按文件名排序）
//
//  1. **.tmp 残留**：`.content-*.tmp` / `.meta-*.tmp`（writeContent/writeMeta 的
//     CreateTemp 实际模式）超 Grace → 删。**只认这两种模式**，别的文件一律不动。
//  2. **孤儿目录**：root 下合法 ULID 形态目录、meta.json **不存在**、目录 mtime 超 Grace
//     → 整目录删。崩溃残留（content 先落 meta 后落）= 有 content 无 meta.json → 按包 doc
//     的**权威存在标记**口径"视为不存在"，删掉安全。meta.json 存在的目录**不是孤儿**；
//     非法 ULID 名的目录/文件一律不动（不是我们建的）。
//  3. **mtime 清扫**（仅 Retention > 0）：meta.json 存在的完整 blob，meta 的 mtime 超
//     Retention → 整目录删。**龄期 = 最后外置时间**：meta.json 的 mtime 由**幂等 Put 刷新**
//     （见 blobfs.Put 的幂等分支：内容寻址 id 下"重复引用"是常态，不刷新就会把仍在被引用
//     的 blob 当超期残留删掉）。
//
// # Grace：在途写保护（年轻残留不动）
//
// 年轻的 .tmp / 孤儿目录可能是一次**正在进行的 Put**（创建目录 → 写 content → 落 meta
// 的窗口），删了会把在途写打断 → 一律等满 Grace（默认 1h）才回收。
// **Grace ≤ 0 一律取默认 1h**（defaultGCGrace）：零宽限 = "立刻全删" = 会误删在途写，
// 不是一个有意义的配置（CLI 的 -grace 0 同款口径）。
//
// # Retention=0 = 只做安全两扫（默认）
//
// mtime 清扫会删**事件流里还有 binary_ref 指向**的完整 blob——读侧取不到时按
// core/storage/blob.go 的契约报"**内容已回收**"（Delete 不级联删事件的既有口径：
// 历史不可重写 F6，引用原样留着）。这是**合法语义**但不是默认行为：想要安全模式就
// Retention=0（默认）只做前两扫（残留回收，不碰有人引用的完整 blob）。
//
// # 与并发 Put 的关系（**同进程内**互斥；跨进程零互斥）
//
// 删除持对应 id 的分片写锁（与 Put 同一把），删前**锁内复核**——复核只认**内容证据**
// （meta.json 是否已落、目录里是否仍有 .tmp），**不用目录 mtime**（扫 1 删 .tmp 会刷新
// 所在目录的 mtime，用它判"在途写"会把刚清过 .tmp 的孤儿误判成年轻 → 漏删，且 DryRun
// 与真跑数字对不上；见 orphanStillOrphan）。
//
// **锁只在同进程内互斥**（sync.Mutex 挂在 Store 实例上）：跨进程（`alasol blob-gc` CLI
// 与常驻 hub 并行）**没有共享锁**，唯一的防线是 **Grace 宽限**（在途写的目录年轻）。
// 跨进程锁**不做**：文件锁的跨平台语义碎（Windows 强制打开/共享模式、POSIX flock 与
// fcntl 各有坑），收益配不上复杂度——如实标注这条边界，不假装跨进程安全。
//
// # 删不掉 = 跳过，不中止整轮
//
// 单个目标删不掉（如 Windows 上目录里的文件被别的进程打开 → sharing violation）→
// **跳过该目标、计入报告的 Skipped、继续扫**（运维工具不该一撞就半途而废：跑一半就退
// 等于让人不得不反复重跑）。扫完照常给完整报告。
//
// # 报告口径
//
// 中途出错 → 返回**已累计**的报告 + error（部分完成如实报，不假装跑完了）。
// DryRun：一个不删，但 Removed* / FreedBytes 照实算（"会删什么"如实呈现）——
// **DryRun 与真跑走同一套判据**（含锁内复核的等价版本），数字必须一致。

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// defaultGCGrace 是 Grace 的默认宽限（**<=0 也取这个**：零值不该等于"立刻全删"——
// 年轻残留可能是在途写，误删是数据损失；要更快回收就显式传一个正的 Grace）。
const defaultGCGrace = time.Hour

// GCOptions 是 GC 的开关面。
type GCOptions struct {
	// Grace 是孤儿目录 / .tmp 残留的宽限期（年轻残留可能是在途写，不动）。
	// **<=0 → 默认 1h**（见 defaultGCGrace：零宽限 = 立刻全删 = 误删在途写，不是合法配置）。
	Grace time.Duration
	// Retention 是完整 blob（meta.json 存在）的 mtime 保留期；
	// **0 = 不清扫**——只做安全两扫（.tmp + 孤儿，不动有人引用的完整 blob）。
	// 回收完整 blob 不级联删事件：读侧按引用取不到时报"内容已回收"（契约既有口径）。
	// **龄期 = 最后外置时间**：幂等 Put（同 id 同内容）会刷新 meta.json 的 mtime，
	// 故"重复引用"过的 blob 不会被当成超期残留删掉。
	Retention time.Duration
	// DryRun 只报不删（Removed* / FreedBytes 照实算 = "会删什么"）。
	DryRun bool
}

// GCReport 是一次 GC 的结果（单行 JSON 输出到 stdout，形态照 walfile.Report）。
type GCReport struct {
	// ScannedBlobs 是扫到的**完整 blob**（meta.json 存在）个数——含未被清扫的年轻 blob
	// （"看了多少"不是"删了多少"）。孤儿目录不算（没有 meta = 视为不存在）。
	ScannedBlobs int64 `json:"scanned_blobs"`
	// RemovedTmp 是回收的 .content-*.tmp / .meta-*.tmp 临时文件数。
	RemovedTmp int64 `json:"removed_tmp"`
	// RemovedOrphans 是回收的孤儿目录数（合法 ULID + 无 meta.json + 超 Grace）。
	RemovedOrphans int64 `json:"removed_orphans"`
	// RemovedAged 是按 Retention 回收的完整 blob 数（Retention=0 时恒 0）。
	RemovedAged int64 `json:"removed_aged"`
	// FreedBytes 是（将被）释放的字节数：逐文件 Size 累加。
	// DryRun 下同一文件只计一次（.tmp 已计入的不再随目录重复计）。
	FreedBytes int64 `json:"freed_bytes"`
	// Skipped 是**删不掉而跳过**的目标数（如 Windows 上目录里的文件被别的进程打开 →
	// sharing violation）。单个失败**不中止整轮**（运维工具不该一撞就半途而废）：跳过该
	// 目标、继续扫，失败数在这里如实报（DryRun 下恒 0：没删就没有失败）。
	Skipped int64 `json:"skipped"`
	// DryRun 回显本次是否只报不删。
	DryRun bool `json:"dry_run"`
}

// tmpFileSnap 是扫 1 之前快照到的一个 .tmp 残留。
type tmpFileSnap struct {
	path  string
	size  int64
	mod   time.Time
	dirID contracts.Ulid // 所属 blob 目录的 id（root 级 .tmp 为空串——不持锁，我们不建这种文件）
}

// dirSnap 是扫 1 之前快照的一个 root 条目（合法 ULID 目录）。
//
// **先量后删**：扫 1 删 .tmp 会刷新所在**目录的 mtime**（unlink 更新父目录）——
// 若孤儿判定用删除后的 mtime，刚清过 .tmp 的孤儿目录会显得"年轻"而漏删。
// 故全部 mtime 在任何删除之前快照，三扫都吃快照。
type dirSnap struct {
	name    string
	path    string
	dirMod  time.Time
	hasMeta bool
	metaMod time.Time
	tmps    []tmpFileSnap
}

// GC 执行三扫回收（口径见文件头）。确定性：条目按 os.ReadDir 的文件名序遍历。
//
// 中途出错返回**已累计**的报告 + error（部分完成如实报）；ctx 取消同理。
func (s *Store) GC(ctx context.Context, opts GCOptions) (GCReport, error) {
	rep := GCReport{DryRun: opts.DryRun}
	grace := opts.Grace
	if grace <= 0 {
		grace = defaultGCGrace
	}
	retention := opts.Retention
	if retention < 0 {
		retention = 0
	}
	now := time.Now()

	entries, err := os.ReadDir(s.root)
	if err != nil {
		return rep, fmt.Errorf("blobfs: GC 列根目录: %w", err)
	}

	// ---- 快照（任何删除之前）：root 条目 + 各目录的 mtime/meta/.tmp ----
	var dirs []dirSnap
	var rootTmps []tmpFileSnap
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		full := filepath.Join(s.root, e.Name())
		if !e.IsDir() {
			// root 级文件：只认两种 .tmp 模式（别的一律不动；我们只在 blob 目录里建 .tmp，
			// 这里兜住异常挪出来的残留），非 .tmp 文件（外来文件）不动。
			if isGCTmpName(e.Name()) {
				fi, err := e.Info()
				if err != nil {
					return rep, fmt.Errorf("blobfs: GC stat %s: %w", full, err)
				}
				rootTmps = append(rootTmps, tmpFileSnap{path: full, size: fi.Size(), mod: fi.ModTime()})
			}
			continue
		}
		if !core.ValidUlid(contracts.Ulid(e.Name())) {
			continue // 非法 ULID 名的目录：不是我们建的，一律不动
		}
		fi, err := e.Info()
		if err != nil {
			return rep, fmt.Errorf("blobfs: GC stat 目录 %s: %w", e.Name(), err)
		}
		d := dirSnap{name: e.Name(), path: full, dirMod: fi.ModTime()}
		mfi, err := os.Stat(filepath.Join(full, metaFileName))
		switch {
		case err == nil:
			d.hasMeta = true
			d.metaMod = mfi.ModTime()
		case os.IsNotExist(err):
			// 孤儿候选（meta.json 是权威存在标记，见包 doc）。
		default:
			return rep, fmt.Errorf("blobfs: GC stat meta %s: %w", e.Name(), err)
		}
		files, err := os.ReadDir(full)
		if err != nil {
			return rep, fmt.Errorf("blobfs: GC 列目录 %s: %w", e.Name(), err)
		}
		for _, f := range files {
			if f.IsDir() || !isGCTmpName(f.Name()) {
				continue // 只认两种 .tmp 模式；content/meta.json/别的文件扫 2/3 按目录处置
			}
			ff, err := f.Info()
			if err != nil {
				return rep, fmt.Errorf("blobfs: GC stat %s: %w", filepath.Join(full, f.Name()), err)
			}
			d.tmps = append(d.tmps, tmpFileSnap{
				path:  filepath.Join(full, f.Name()),
				size:  ff.Size(),
				mod:   ff.ModTime(),
				dirID: contracts.Ulid(e.Name()),
			})
		}
		dirs = append(dirs, d)
	}

	// counted 是已计入 FreedBytes 的路径（DryRun 防 .tmp 与其所在目录重复计）。
	counted := make(map[string]struct{})

	// ---- 扫 1：.tmp 残留（root 级 + 各 blob 目录内），超 Grace 才删 ----
	sweepTmp := func(t tmpFileSnap) error {
		if now.Sub(t.mod) <= grace {
			return nil // 年轻：可能是在途写，不动
		}
		if _, ok := counted[t.path]; ok {
			return nil
		}
		if !opts.DryRun {
			var release func()
			if t.dirID != "" {
				// 持该 id 的分片写锁：与并发 Put 串行（Put 写 .tmp 期间我们等它写完）。
				l := s.idLock(t.dirID)
				l.Lock()
				release = l.Unlock
				// 锁内复核：Put 可能已把 .tmp rename 走（写完）——没了就不算我们删的。
				if fi, err := os.Stat(t.path); err != nil {
					release()
					if os.IsNotExist(err) {
						return nil
					}
					return fmt.Errorf("blobfs: GC 复核 %s: %w", t.path, err)
				} else if now.Sub(fi.ModTime()) <= grace {
					release() // 窗口内被重写变年轻 → 保护
					return nil
				}
			}
			err := os.Remove(t.path)
			if release != nil {
				release()
			}
			if err != nil && !os.IsNotExist(err) {
				// 删不掉（如 Windows 上文件被别的进程打开 → sharing violation）→ **跳过**
				// 这个文件、计入 Skipped、**不中止整轮**（运维工具不该一撞就半途而废）。
				// 不计入 RemovedTmp / FreedBytes：它没被删（"报了却没删"是假账）。
				rep.Skipped++
				return nil
			}
		}
		counted[t.path] = struct{}{}
		rep.RemovedTmp++
		rep.FreedBytes += t.size
		return nil
	}
	for _, t := range rootTmps {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		if err := sweepTmp(t); err != nil {
			return rep, err
		}
	}
	for _, d := range dirs { // dirs 按目录名排序（快照序 = os.ReadDir 序）
		for _, t := range d.tmps {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			if err := sweepTmp(t); err != nil {
				return rep, err
			}
		}
	}

	// ---- 扫 2：孤儿目录（无 meta.json + 目录 mtime 超 Grace）→ 整目录删 ----
	//
	// 年龄判据吃**快照**的目录 mtime（任何删除之前量的）；复核吃**内容证据**（不用目录
	// mtime——扫 1 删 .tmp 会刷新所在目录的 mtime，用它判"在途写"会漏删孤儿，见
	// orphanStillOrphan）。DryRun 与真跑走**同一套判据**（复核的等价版本），数字一致。
	for _, d := range dirs {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		if d.hasMeta {
			rep.ScannedBlobs++ // 完整 blob：记扫描数（是否清扫看扫 3）
			continue
		}
		if now.Sub(d.dirMod) <= grace {
			continue // 年轻孤儿：可能是在途写（content 先 meta 后的窗口），不动
		}
		if opts.DryRun {
			// 复核（内容证据）：真跑时这一步在锁内做；DryRun 用同一判据（只读，不取锁——
			// DryRun 不删任何东西，取锁没有意义，还可能与在途 Put 互相等）。
			// 例外只有一个：**删除失败**（Skipped）那种异常态——DryRun 假定删除会成功，
			// 真跑里删不掉的 .tmp 仍在盘上 → 那时真跑更保守（不删该孤儿）。
			orphan, err := orphanStillOrphan(d.path, counted)
			if err != nil {
				return rep, err
			}
			if !orphan {
				continue // 不是孤儿了 / 在途写 → 不删（与真跑同口径）
			}
			n, err := dirFileSize(d.path, counted)
			if err != nil {
				return rep, err
			}
			rep.RemovedOrphans++
			rep.FreedBytes += n
			continue
		}
		// 持 id 锁 + 锁内复核：快照→删除窗口内可能有同 id 的重放 Put 落了 meta
		// （孤儿被补成完整 blob）——复核不过就不删（宁可留下次 GC）。
		l := s.idLock(contracts.Ulid(d.name))
		l.Lock()
		removed, freed, rerr := func() (bool, int64, error) {
			orphan, err := orphanStillOrphan(d.path, counted)
			if err != nil {
				return false, 0, err
			}
			if !orphan {
				return false, 0, nil // 复核不过：不是孤儿（或在途写）→ 不删
			}
			n, err := dirFileSize(d.path, counted)
			if err != nil {
				return false, 0, err
			}
			if err := os.RemoveAll(d.path); err != nil {
				// 删不掉（如 Windows 上文件被占用 → sharing violation）→ 跳过该目录、
				// 计入 Skipped、**继续扫**（不中止整轮）。
				rep.Skipped++
				return false, 0, nil
			}
			return true, n, nil
		}()
		l.Unlock()
		if rerr != nil {
			return rep, rerr
		}
		if removed {
			rep.RemovedOrphans++
			rep.FreedBytes += freed
		}
	}

	// ---- 扫 3：mtime 清扫（仅 Retention > 0）：完整 blob 的 meta mtime 超期 → 整目录删 ----
	// 口径（CLI -retention 帮助同款）：事件流的 binary_ref **不动**，读侧取不到时按契约
	// 报"内容已回收"（Delete 不级联的既有语义）；安全模式 Retention=0 只做前两扫。
	//
	// **龄期 = 最后外置时间**：判据是 meta.json 的 mtime，而**幂等 Put（同 id 同内容）会刷新
	// 它**（见 blobfs.Put 的幂等分支）——内容寻址 id 下"重复引用同一份内容"是常态（重放 /
	// 再次引用），不刷新就会把**仍在被引用**的 blob 当超期残留删掉。
	if retention > 0 {
		for _, d := range dirs {
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			if !d.hasMeta {
				continue // 孤儿已由扫 2 处置
			}
			if now.Sub(d.metaMod) <= retention {
				continue // 未超期：留
			}
			if opts.DryRun {
				n, err := dirFileSize(d.path, counted)
				if err != nil {
					return rep, err
				}
				rep.RemovedAged++
				rep.FreedBytes += n
				continue
			}
			l := s.idLock(contracts.Ulid(d.name))
			l.Lock()
			removed, freed, rerr := func() (bool, int64, error) {
				mfi, err := os.Stat(filepath.Join(d.path, metaFileName))
				if err != nil {
					if os.IsNotExist(err) {
						return false, 0, nil // 已被并发改掉（删了/降级成孤儿走扫 2）
					}
					return false, 0, fmt.Errorf("blobfs: GC 复核 meta %s: %w", d.name, err)
				}
				if now.Sub(mfi.ModTime()) <= retention {
					return false, 0, nil // 窗口内被重写变年轻 → 保护
				}
				n, err := dirFileSize(d.path, counted)
				if err != nil {
					return false, 0, err
				}
				if err := os.RemoveAll(d.path); err != nil {
					// 删不掉（文件被占用等）→ 跳过、计入 Skipped、继续扫（不中止整轮）。
					rep.Skipped++
					return false, 0, nil
				}
				return true, n, nil
			}()
			l.Unlock()
			if rerr != nil {
				return rep, rerr
			}
			if removed {
				rep.RemovedAged++
				rep.FreedBytes += freed
			}
		}
	}
	return rep, nil
}

// isGCTmpName 报告文件名是否是 CreateTemp 的两种模式之一（`.content-*.tmp` / `.meta-*.tmp`，
// 见 writeContent/writeMeta）。**只认这两种**，别的文件一律不动（外来文件不是我们的残留）。
func isGCTmpName(name string) bool {
	return (strings.HasPrefix(name, ".content-") && strings.HasSuffix(name, ".tmp")) ||
		(strings.HasPrefix(name, ".meta-") && strings.HasSuffix(name, ".tmp"))
}

// orphanStillOrphan 复核一个目录**此刻还是不是孤儿**（扫 2 的锁内复核；DryRun 用同一判据）。
//
// # 判据（**内容证据**，不是目录 mtime）
//
// 目录不是孤儿（→ 不删）当且仅当：
//
//   - **meta.json 在** = 完整 blob（快照→删除的窗口里，重放 Put 把它补齐了）；
//   - **目录里仍有 .content-*.tmp / .meta-*.tmp** = 在途写（CreateTemp 正在写；
//     扫 1 只删**超 Grace** 的老 .tmp，剩下的就是"活的"）。
//
// 目录里什么都没有（扫 1 清完 .tmp 后的**空壳**）或只剩**无法识别的外来文件** → 仍是孤儿
// → 照删（年龄口径不变：快照的目录 mtime 超 Grace）。
//
// # 为什么不用目录 mtime（这正是 F1 修掉的坑）
//
// 扫 1 删 .tmp 会**刷新所在目录的 mtime**（unlink 更新父目录）——若复核用删除后的目录
// mtime 判"在途写"，刚清过 .tmp 的孤儿会显得"年轻" → **漏删**；且 DryRun（不删 → mtime
// 不变 → 照删）与真跑（mtime 被刷新 → 不删）**数字对不上**。目录 mtime 是 GC 自己改过
// 的量，不能当证据。
//
// # 为什么 `content` 不算"在途写"证据
//
// content 在、meta.json 不在 = **崩溃残留的典型形态**（Put 的落盘序：content 先、meta.json
// 后）——它正是扫 2 要回收的对象（包 doc 的权威存在标记口径：meta.json 不在 = 视为不存
// 在，删掉安全）。把它当"在途写"证据 = 扫 2 永远删不掉任何崩溃残留，孤儿扫形同虚设。
// 在途写由两道证据兜住：**目录年轻**（快照 mtime 判龄）+ **仍有临时文件**（本函数）。
//
// ignore 是"扫 1 已删（DryRun = 会删）的 .tmp 路径"集合：算"仍在"时跳过它们——真跑里它们
// 已经不在盘上，DryRun 里它们还在；跳过才能让两种模式判出同一个结果。
func orphanStillOrphan(dir string, ignore map[string]struct{}) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // 已被并发改掉（不算孤儿，也不报错）
		}
		return false, fmt.Errorf("blobfs: GC 复核目录 %s: %w", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		if name == metaFileName {
			return false, nil // meta 已落：完整 blob，不是孤儿
		}
		if !isGCTmpName(name) {
			continue // 外来文件：不是我们的写入痕迹
		}
		if _, ok := ignore[filepath.Join(dir, name)]; ok {
			continue // 扫 1 已删（DryRun = 会删）的临时文件：视为不在
		}
		return false, nil // 仍有临时文件 = 在途写 → 保护
	}
	return true, nil
}

// dirFileSize 累加目录下所有文件的 Size（逐文件累加 = FreedBytes 的简版口径）。
// skip 里的路径不计（DryRun：.tmp 已在扫 1 计过，随目录不重复计）。
func dirFileSize(dir string, skip map[string]struct{}) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if _, ok := skip[path]; ok {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("blobfs: GC 量目录 %s: %w", dir, err)
	}
	return total, nil
}
