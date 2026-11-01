package blobfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
)

// 布局常量（见 doc.go 的磁盘布局）。
const (
	metaFileName    = "meta.json"
	contentFileName = "content"
)

// 编译期证明：*Store 就是 core/storage.Blob（换实现面不变，G2b）。
var _ storage.Blob = (*Store)(nil)

// blobLockShards 是写锁的**固定分片数**（F8）。
//
// 为什么不用 per-id 的 map：per-id 锁表**只增不减**——每个新 id 一把锁，进程越跑越久表越大
// （内存单调增长；旧 id 早没人用了还不释放）。固定分片把锁的总数**钉死**在 256 把：
// 同一 id 恒落同一片 → 互斥语义与 per-id 锁**完全一致**；不同 id 大概率落不同片（并发度
// 几乎不损失，偶发碰撞只是多串行一次，不影响正确性）。
const blobLockShards = 256

// Store 是 core/storage.Blob 的文件系统实现（并发安全；见包 doc）。
type Store struct {
	root string

	// shards 是固定分片的写锁（256 片，按 id 哈希）。只护 Put/Delete 的临界区；
	// 不护任何文件 IO（Get/Stat 无锁：落位后内容不可变）。
	shards [blobLockShards]sync.Mutex

	// writes 是**写计数探针**（测试钩子；生产恒 nil → 每次落盘只多一次 nil 判断）。
	//
	// 为什么需要它：幂等口径（"同 id 同内容 → **不写第二份**"）用**内容比对**是查不出来的
	// ——二次 Put 之后内容一样、元数据一样，**看不出有没有重写**。生产上唯一的观测点是
	// mtime，但 Windows 的 mtime 精度不稳（同一 tick 内的两次写可能同值）、且"只改内容不改
	// meta"的场景会漏判。故给实现挂一个可选探针：内容文件**真正落盘**时计数 +1。
	writes *WriteCounter
}

// WriteCounter 是**写计数探针**（测试钩子；生产恒 nil → 不存在）。
//
// 口径：数的是**内容文件落盘次数**——一次新建 Put = 1 次；**幂等二次 Put = +0 次**
// （这正是要钉的那件事：不写第二份）。
type WriteCounter struct {
	n atomic.Int64
}

// Count 返回已落盘的内容文件次数（幂等二次 Put 后应与首次相同）。
func (c *WriteCounter) Count() int64 {
	if c == nil {
		return 0
	}
	return c.n.Load()
}

// SetWriteCounter 装一个写计数探针（**测试钩子**；传 nil = 不装 = 生产默认）。
//
// 装配期调用（并发使用前）：探针是计数器指针，替换它与并发写之间**没有同步**——
// 生产路径恒 nil，测试里装完再开跑即可。
func (s *Store) SetWriteCounter(c *WriteCounter) {
	s.writes = c
}

// countContentWrite 记一次**内容落盘**（写计数探针；未装探针 = 零开销）。
func (s *Store) countContentWrite() {
	if s.writes != nil {
		s.writes.n.Add(1)
	}
}

// New 建一个 blob 文件系统存储并确保 root 目录存在（幂等：已存在直接用）。
//
// root 空 → 拒绝（没有落地目录就无从谈"外置仓库"）。root 是文件（非目录）→ MkdirAll 报错。
func New(root string) (*Store, error) {
	r := strings.TrimSpace(root)
	if r == "" {
		return nil, errors.New("blobfs: root 目录必填（大对象外置仓库的落地根）")
	}
	if err := os.MkdirAll(r, 0o755); err != nil {
		return nil, fmt.Errorf("blobfs: 建根目录 %q: %w", r, err)
	}
	return &Store{root: r}, nil
}

// Root 返回存储根目录（诊断用；不属 Blob 接口）。
func (s *Store) Root() string { return s.root }

// idLock 取某 id 的写锁（**固定分片**；同一 id 恒落同一片 → 互斥语义与 per-id 锁一致）。
//
// 哈希用 FNV-1a（32 位，自己算，不引依赖）：分片选择只影响并发度，不影响正确性
// （同 id 必同片才是关键）。
func (s *Store) idLock(id contracts.Ulid) *sync.Mutex {
	h := uint32(2166136261) // FNV-1a offset basis
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619 // FNV-1a prime
	}
	return &s.shards[h%blobLockShards]
}

// Put 写入一个大对象，返回落库后的元数据（ID 一定非空）。幂等/冲突口径见包 doc。
func (s *Store) Put(_ context.Context, meta storage.BlobMeta, content io.Reader) (storage.BlobMeta, error) {
	if content == nil {
		return storage.BlobMeta{}, errors.New("blobfs: content 不能为 nil")
	}
	id := meta.ID
	if id == "" {
		id = core.NewUlid() // 实现发号（框架 id 唯一生成处）
	}
	if !core.ValidUlid(id) {
		// 防路径穿越（contracts.Ulid 只是字符串类型）：不合则拒，不修复（S1/S9）。
		return storage.BlobMeta{}, fmt.Errorf("blobfs: id %q 不是合法 ULID（26 字符 Crockford base32）", string(id))
	}

	lock := s.idLock(id)
	lock.Lock()
	defer lock.Unlock()

	dir := filepath.Join(s.root, string(id))

	// 已提交过（meta.json 在）→ 走幂等/冲突判定（内容一致 → 原样返回；不一致 → ErrConflict）。
	if existing, ok, err := s.readMeta(id); err != nil {
		return storage.BlobMeta{}, err
	} else if ok {
		equal, cerr := s.sameContent(filepath.Join(dir, contentFileName), content)
		if cerr != nil {
			return storage.BlobMeta{}, cerr
		}
		if !equal {
			return storage.BlobMeta{}, fmt.Errorf("blobfs: id %s 已存在且内容不一致（调用方须换新 id，不许静默覆盖）: %w",
				string(id), storage.ErrConflict)
		}
		// 幂等：原样返回已有元数据，**不写第二份**（内容文件零写入、meta 内容零改写）。
		//
		// 但**触碰 meta.json 的 mtime**（F3：龄期 = 最后外置时间）：内容寻址 id 下"重复引用
		// 同一份内容"是常态（重放、再次引用同一份大 result）——GC 的 mtime 清扫按 meta 的
		// mtime 判龄，不刷新就会把**仍在被引用**的 blob 当超期残留删掉。刷 mtime **不算
		// "写第二份"**：meta 的**内容**一个字节都没改（契约"原样返回已有元数据"不动）。
		// 触碰失败**不影响 Put 返回**（尽力而为：GC 少回收一点 ≠ 数据损失）。
		s.touchMeta(filepath.Join(dir, metaFileName))
		return existing, nil
	}

	// 新建：content 先落（临时文件 + rename），meta.json 后落（权威存在标记）。
	size, err := s.writeContent(dir, content)
	if err != nil {
		return storage.BlobMeta{}, err
	}
	out := storage.BlobMeta{
		ID:        id,
		Size:      size,
		MediaType: meta.MediaType,
		Name:      meta.Name,
		CreatedAt: nowRFC3339(),
	}
	if err := s.writeMeta(dir, out); err != nil {
		return storage.BlobMeta{}, err
	}
	return out, nil
}

// Get 打开一个大对象供读取（不存在 → ErrNotFound）。返回的 ReadCloser 由调用方关闭。
//
// 流式：不把内容整个读进内存（大对象要能流式读，F12）。返回前校验 content 实际大小与
// meta.Size 一致——不一致 = 半截/损坏，明确报错（不静默返回截断的内容）。
func (s *Store) Get(_ context.Context, id contracts.Ulid) (io.ReadCloser, error) {
	if !core.ValidUlid(id) {
		return nil, fmt.Errorf("blobfs: id %q 不是合法 ULID", string(id))
	}
	meta, ok, err := s.readMeta(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("blobfs: blob %s: %w", string(id), storage.ErrNotFound)
	}
	f, err := os.Open(filepath.Join(s.root, string(id), contentFileName))
	if err != nil {
		if os.IsNotExist(err) {
			// meta 在但 content 不在 = 存储损坏（不是"从未存在"）→ 明确错误，区别于 ErrNotFound。
			return nil, fmt.Errorf("blobfs: blob %s 的 meta 存在但内容文件缺失（存储损坏，非 ErrNotFound）", string(id))
		}
		return nil, fmt.Errorf("blobfs: 开 blob %s 的内容: %w", string(id), err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("blobfs: stat blob %s 的内容: %w", string(id), err)
	}
	if fi.Size() != meta.Size {
		// 半截/损坏：实际大小与 meta 记录不符 → 明确错误（读到非预期内容不静默）。
		_ = f.Close()
		return nil, fmt.Errorf("blobfs: blob %s 内容大小 %d 与 meta 记录 %d 不符（半截/损坏）",
			string(id), fi.Size(), meta.Size)
	}
	return f, nil
}

// Stat 取元数据（不含内容；不存在 → ErrNotFound）。给"只想看大小/类型"的消费方。
func (s *Store) Stat(_ context.Context, id contracts.Ulid) (storage.BlobMeta, error) {
	if !core.ValidUlid(id) {
		return storage.BlobMeta{}, fmt.Errorf("blobfs: id %q 不是合法 ULID", string(id))
	}
	m, ok, err := s.readMeta(id)
	if err != nil {
		return storage.BlobMeta{}, err
	}
	if !ok {
		return storage.BlobMeta{}, fmt.Errorf("blobfs: blob %s: %w", string(id), storage.ErrNotFound)
	}
	return m, nil
}

// Delete 删除一个大对象（不存在 → 幂等成功）。**不级联删事件**（见包 doc）。
func (s *Store) Delete(_ context.Context, id contracts.Ulid) error {
	if !core.ValidUlid(id) {
		return fmt.Errorf("blobfs: id %q 不是合法 ULID", string(id))
	}
	lock := s.idLock(id)
	lock.Lock()
	defer lock.Unlock()
	// os.RemoveAll 对不存在目录返回 nil（幂等）；整目录删（content + meta.json 一起）。
	if err := os.RemoveAll(filepath.Join(s.root, string(id))); err != nil {
		return fmt.Errorf("blobfs: 删 blob %s: %w", string(id), err)
	}
	return nil
}

// readMeta 读 meta.json（不存在 → ok=false；损坏 → error）。meta.ID 必须自洽（防串目录）。
func (s *Store) readMeta(id contracts.Ulid) (storage.BlobMeta, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.root, string(id), metaFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return storage.BlobMeta{}, false, nil
		}
		return storage.BlobMeta{}, false, fmt.Errorf("blobfs: 读 meta %s: %w", string(id), err)
	}
	var m storage.BlobMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return storage.BlobMeta{}, false, fmt.Errorf("blobfs: 解析 meta %s（损坏）: %w", string(id), err)
	}
	if m.ID != id {
		return storage.BlobMeta{}, false, fmt.Errorf("blobfs: meta %s 的 id 字段 %q 与目录名不符（损坏）", string(id), string(m.ID))
	}
	return m, true, nil
}

// sameContent 流式比较已有内容文件与新 reader 是否同字节（都不整个进内存，F12）。
//
// 返回 (equal, err)：err 只表示**读取出错**（含已有内容缺失 = 损坏），与"内容不一致"分开——
// 内容不一致是 (false, nil)（调用方折成 ErrConflict），读坏了是 error（明确报存储问题）。
func (s *Store) sameContent(existingPath string, r io.Reader) (bool, error) {
	ef, err := os.Open(existingPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, fmt.Errorf("blobfs: meta 存在但内容文件缺失（存储损坏）: %s", existingPath)
		}
		return false, fmt.Errorf("blobfs: 开已有内容 %s: %w", existingPath, err)
	}
	defer ef.Close()

	eh := sha256.New()
	nh := sha256.New()
	esize, err := io.Copy(eh, ef)
	if err != nil {
		return false, fmt.Errorf("blobfs: 读已有内容 %s: %w", existingPath, err)
	}
	nsize, err := io.Copy(nh, r)
	if err != nil {
		return false, fmt.Errorf("blobfs: 读新内容: %w", err)
	}
	if esize != nsize {
		return false, nil // 大小不同 → 内容必不同
	}
	return bytes.Equal(eh.Sum(nil), nh.Sum(nil)), nil
}

// writeContent 把 content 写进 <dir>/content（临时文件 + fsync + rename 原子落位）。
// 返回字节数。任一步失败 → 清理临时文件，不留半截 content 被 rename 出去。
func (s *Store) writeContent(dir string, r io.Reader) (int64, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, fmt.Errorf("blobfs: 建 blob 目录 %q: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".content-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("blobfs: 建内容临时文件: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	n, err := io.Copy(tmp, r)
	if err != nil {
		return 0, fmt.Errorf("blobfs: 写内容: %w", err)
	}
	if err := tmp.Sync(); err != nil { // fsync：落盘，别只进 OS 缓冲
		return 0, fmt.Errorf("blobfs: fsync 内容: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("blobfs: 关内容临时文件: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, contentFileName)); err != nil {
		return 0, fmt.Errorf("blobfs: rename 内容: %w", err)
	}
	committed = true
	s.countContentWrite() // 写计数探针：内容真落盘了（幂等分支不会走到这里）
	return n, nil
}

// writeMeta 把 meta 写进 <dir>/meta.json（临时文件 + fsync + rename；**最后落** = 权威存在标记）。
func (s *Store) writeMeta(dir string, meta storage.BlobMeta) error {
	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("blobfs: 序列化 meta: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".meta-*.tmp")
	if err != nil {
		return fmt.Errorf("blobfs: 建 meta 临时文件: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(raw); err != nil {
		return fmt.Errorf("blobfs: 写 meta: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("blobfs: fsync meta: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("blobfs: 关 meta 临时文件: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, metaFileName)); err != nil {
		return fmt.Errorf("blobfs: rename meta: %w", err)
	}
	committed = true
	return nil
}

// nowRFC3339 返回 UTC 的 RFC 3339 时间戳（BlobMeta.CreatedAt；诊断用）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// touchMeta 刷 meta.json 的 mtime（= "最后外置时间"标记；见 Put 幂等分支的 F3 口径）。
//
// **尽力而为**：失败**不报错、不影响调用方**——刷不动只是让 GC 少回收一点（真数据损失
// 的方向相反：GC 删得太多才是损失）。故这里吞掉错误，不往 Put 的错误路径上抛。
func (s *Store) touchMeta(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
}
