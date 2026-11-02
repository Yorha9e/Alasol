package walfile

// 本文件是 WAL 的 **GC 回收**（缺口 5 第一件）：按**显式龄期**回收"这个 session 的 WAL
// 已经没用了"的文件。CLI 面：`alasol wal-gc`（读 [wal].dir）。形态照 blobfs.GC /
// `alasol blob-gc`（上一刀的运维 GC 先例：dry/real 同判据、Skipped 不中止、单行 JSON 报告）。
//
// # 合法性依据：WAL 不是事实源（core/wal.go）
//
// 事件流（core.Store）仍是唯一事实源——删掉整个 WAL 目录不丢任何事实（只丢"当时在干什么"
// 这段观察），事件流照常完整回放。GC 删的是**观察面**，不碰事实；这是它与 blob 回收
// （删的是可能被引用的内容）本质不同的地方。
//
// # 判据（**全部满足**才删；删除动作 = Remove 整个 session 的 WAL 文件，不是 Truncate）
//
//  1. **龄期**：文件 mtime 超 Retention。**Retention 必给**（<=0 → 库层直接报错）：龄期是
//     运维决定，没有安全默认——删 WAL = 丢观察面，虽然不丢事实但**不可逆**；"何时没用了"
//     不该由库猜。在途写的 session 由 mtime 兜底：追加写会刷新 mtime（年轻 = 不动）。
//  2. **reconcile 干净**：该 session 的恢复报告里**两个"没走完"的信号都没有**——
//     `InFlightTurn == nil`（无在飞轮次）且 `OrphanBlobToolCalls` 空（无孤儿 blob 候选）。
//     崩溃上下文还有用 = 留（宁留不删）。
//
//     **StaleBlobPlans（作废计划）不挡回收**（critic F1 修掉的语义错误）：作废计划 =
//     "**走完了但没外置**"（completed 落了但没 binary_ref）——绝大多数是"结果不大"的正常
//     收尾，不是崩溃上下文。把它当脏信号 = 回收面落空（正常收尾的 session 永远回收不掉，
//     GC 白做）。它是**信息性信号**：照常报给人看，但不参与"留还是删"。
//
//     口径三个钉子：**CorruptLines 不算脏**（半截行是崩溃现场的常态，reconcile 本来就是
//     容忍设计）；reconcile **读失败** → 该文件跳过计入 Skipped（宁留不删，不猜）；
//     **Facts 为 nil（空事实）时口径不变、仍保守留**——空事实分不出"orphan（没走完 = 脏）"
//     与"stale（走完了 = 干净可删）"，分不清就不删（见下面的 Facts 一节）。
//  3. **合法形态**：文件名是 `<合法 ULID>.wal`（Path 的命名口径）。外来文件/子目录一律
//     不动（不扫、不计数、不删——不是我们建的东西）。
//
// Truncate 与 GC 的分工（包 doc 的口径落地）：Truncate 是**原语**（截成空文件，手工用）；
// GC 的目的是**回收**——删掉整个文件（Remove），不是留一个空壳。
//
// # Facts：对账事实的来源（nil = 空事实，保守不删）
//
// reconcile 需要事件流一侧的事实（Facts）来判孤儿/作废；walfile **不读事件库**（依赖方向：
// walfile 不认识 sqlite）。故事实经 GCOptions.Facts 注入：
//
//   - **nil（CLI 默认）= 空事实**：任何 blob.plan 都会被对账成"孤儿候选"（计划在 WAL、
//     事实里既无 completed 也无 failed）→ 判脏 → 保守不删。`alasol wal-gc` 不带 -db，
//     走的就是这条保守路径——有 blob.plan 的 session WAL 恒留（宁留不删，如实写明）；
//   - 有事件库的调用方可以传入 FactsFromEvents 折出的事实，让"completed 已落"的 plan
//     不再挡住回收——**事实的用途就是区分 orphan（脏，留）与 stale（干净，可删）**：
//     completed 有 binary_ref → 干净；completed 无 binary_ref → 作废计划（**干净可删**，
//     走完了只是没外置）；failed → 干净（工具调用没产出结果，没写 blob）；既无 completed
//     也无 failed → **真孤儿**（这个工具调用没走完）→ 脏，留。
//
// # DryRun 与真跑：同一判据、数字完全一致（**除"删不掉"这一种情况**，见下）
//
// DryRun 一个不删，但 Removed / FreedBytes / Skipped 照实算（"会删什么"如实呈现）；
// 两者走**同一套判据**（blob GC 的 p1 教训：判据分叉 = 数字对不上 = 报告不可信）。
//
// **已知分叉（critic F10，如实写明）**：DryRun 不真删 → **测不出"删不掉"**；真跑删不掉
// → 该文件计入 **Skipped**（不是 Removed）。故在"删不掉"发生的那一刻，dry 的 Removed 会
// 比真跑**多** 1（真跑把它记成 Skipped）。**判据本身同源**（同一个"该删"判定），分叉只
// 来自"删除动作是否真的成功"这一执行事实——这是**如实**，不是判据不一致。
//
// # 确定性顺序 + 删不掉不中止整轮
//
// 条目按 os.ReadDir 的文件名序遍历（确定性）。单个文件删不掉（Windows 上被别的进程打开 →
// sharing violation 等）→ **跳过该文件、计入 Skipped、继续扫**（运维工具不该一撞就半途
// 而废）；它没被删就不计入 Removed / FreedBytes（"报了却没删"是假账）。
//
// # 跨进程零互斥（如实标注，不假装安全）
//
// 与常驻 hub 并行跑时没有共享锁（WAL 的在途写由 fd 持有）；防线是两条：**龄期**（在途
// 写的 mtime 年轻）+ **reconcile 脏判据**（在飞轮次/真孤儿还在 → 判脏）。被占用删不掉
// → Skipped。跨进程锁不做（同 blobfs.GC 的口径：文件锁的跨平台语义碎，收益配不上复杂度）。
//
// **GC 删掉活 session 的 WAL = 观察面重置**（不是静默丢数据）：Writer 的写路径**自愈**
// ——下一次写发现自己的文件没了就**重建新文件**再写（见 walfile.go 的 Writer 一节）。
// POSIX 上"unlink 后 fd 续写死 inode"那个坑因此不成立：WAL **不是事实源**（事件流照常
// 完整回放），观察面从头再来一遍即可。

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// GCOptions 是 GC 的开关面。
type GCOptions struct {
	// Retention 是 WAL 文件 mtime 的保留期，**必给**（<=0 → 直接报错）。
	//
	// 为什么没有安全默认：删 WAL = 丢观察面——虽然不丢事实（WAL 不是事实源），但"当时
	// 在干什么"这段恢复现场是**不可逆**的。"何时没用了"是运维决定，不该由库猜一个默认龄期。
	Retention time.Duration
	// DryRun 只报不删（Removed / FreedBytes / Skipped 照实算 = "会删什么"）。
	// 与真跑**同一套判据**（判据同源）；**已知分叉**：真跑"删不掉"会计 Skipped（不是
	// Removed），dry 测不出删不掉 → 那一刻 dry.Removed 比真跑多（见文件头的分叉说明）。
	DryRun bool
	// Facts 提供某 session 的**事件流对账事实**（reconcile 的孤儿/作废判据要用；
	// 由调用方从 core.Store 折出——FactsFromEvents）。
	//
	// nil = 不提供（**空事实**）：任何 blob.plan 都会被对账成"孤儿候选" → 判脏 → 保守
	// 不删（宁留不删）。`alasol wal-gc` 不读事件库，走的就是这条路径。
	//
	// 事实的用途 = **区分 orphan（脏，留）与 stale（干净，可删）**：completed 有
	// binary_ref → 干净；completed 无 binary_ref → 作废计划（走完了，只是没外置）→
	// **干净可删**；failed → 干净；既无 completed 也无 failed → 真孤儿 → 脏，留。
	Facts func(sessionID contracts.Ulid) Facts
}

// GCReport 是一次 GC 的结果（单行 JSON 输出到 stdout，形态照 blobfs.GCReport）。
type GCReport struct {
	// ScannedSessions 是扫到的**合法 session WAL 文件**数（含未被回收的年轻/脏文件——
	// "看了多少"不是"删了多少"）。外来文件/子目录不算。
	ScannedSessions int64 `json:"scanned_sessions"`
	// Removed 是（将被）回收的 WAL 文件数（**判据命中数**）。
	//
	// **口径（critic F10：删失败场景下两模式会分叉，如实写明）**：Removed = **判据命中的
	// 目标数**；真跑时"判据命中但**删不掉**"的那一个会计入 **Skipped**（不是 Removed），
	// 而 DryRun **不真删**、测不出删不掉 → 仍计入 Removed。故在"删不掉"发生的那一刻，
	// dry.Removed 比真跑**多** 1（真跑把它记成 Skipped）。**判据本身同源**（同一个
	// "该删"判定），分叉只来自"删除动作是否真的成功"这一执行事实——这是如实，不是判据
	// 不一致（其余情况两模式数字完全一致）。
	Removed int64 `json:"removed"`
	// Skipped 是**跳过**的目标数：reconcile 脏（没走完的信号还在）、读失败、stat 失败、
	// 删不掉。单个失败**不中止整轮**：跳过、继续扫，失败数在这里如实报。
	Skipped int64 `json:"skipped"`
	// StalePlans 是扫到的**作废计划**总数（信息性信号，critic F1：不挡回收、只报数）。
	// 语义 = "blob.plan 在 WAL 里、tool_call 走完了但没外置"（结果不大的正常收尾占多数）
	// ——不是崩溃上下文，但运维值得看见（比例异常高可能意味着外置阈值配得太大）。
	// 计数**与留/删无关**（判脏后留下的与被删文件里的都照数）；未超龄的文件不评估
	// （不读内容），其作废计划不进本计数。
	StalePlans int64 `json:"stale_plans"`
	// FreedBytes 是（将被）释放的字节数（逐文件 Size 快照累加）。
	FreedBytes int64 `json:"freed_bytes"`
	// DryRun 回显本次是否只报不删。
	DryRun bool `json:"dry_run"`
}

// gcReadFile 是 GC 的读入口（**测试缝**：生产恒为 ReadFile）。
//
// 为什么要有缝："读失败 → Skipped"这条路径在黑盒面**不可靠构造**——各平台挡住一个普通
// 文件的读的手段各不相同（Windows 要 ACL/共享模式、POSIX 有 root 豁免），拼出来的钉子
// 在有的机器上是空的。与 sync_seam_test.go 注入记账假文件同一思路：把入口换成可注入的
// 包级变量，钉子在任何机器上都钉得住。
var gcReadFile = ReadFile

// gcRemove 是 GC 的删除入口（**测试缝**：生产恒为 Remove）。
//
// 为什么要有缝：**"删不掉 → Skipped"这条路径在黑盒面不可靠构造**（各平台挡住删除的手段
// 各不相同：Windows 上打开的文件不可删、POSIX 上 unlink 打开的文件是允许的）。注入这个
// 缝就能在任何机器上钉住"删失败时 Removed/Skipped 怎么记"这条口径（critic F10 的分叉
// 钉子）。
var gcRemove = Remove

// GC 扫 WAL 目录、按判据回收"已没用"的 session WAL（口径见文件头）。
//
// 确定性：条目按 os.ReadDir 的文件名序遍历。Retention <= 0 → 报错（库层就拒，不猜默认）；
// 目录列不出来 → 已累计的报告 + error（部分完成如实报）。
func GC(dir string, opts GCOptions) (GCReport, error) {
	rep := GCReport{DryRun: opts.DryRun}
	if opts.Retention <= 0 {
		return rep, fmt.Errorf("walfile: GC 必须显式给 Retention（>0；得到 %v）——龄期是运维决定，删 WAL 不可逆，库层不猜安全默认", opts.Retention)
	}
	d := strings.TrimSpace(dir)
	if d == "" {
		return rep, errors.New("walfile: GC 的 WAL 目录必填（空 = 未配置；\"没配\"由调用方判，GC 没有可扫的对象）")
	}
	entries, err := os.ReadDir(d)
	if err != nil {
		return rep, fmt.Errorf("walfile: GC 列 WAL 目录 %q: %w", d, err)
	}
	now := time.Now()

	for _, e := range entries { // os.ReadDir 按文件名排序 → 确定性顺序
		if e.IsDir() {
			continue // 子目录不是 WAL 文件（外来物一律不动）
		}
		sid, ok := sessionIDOfWALName(e.Name())
		if !ok {
			continue // 外来文件：不是 `<session>.wal` 形态，一律不动
		}
		fi, err := e.Info()
		if err != nil {
			rep.Skipped++ // 连 stat 都失败：宁留不删，跳过继续
			continue
		}
		rep.ScannedSessions++
		if now.Sub(fi.ModTime()) <= opts.Retention {
			continue // 未超龄：留（不是"脏"，只是还年轻——不计 Skipped）
		}
		// reconcile：**两个"没走完"的信号**都没有才算干净（崩溃上下文没用了）。
		// CorruptLines 不算脏（半截行容忍是 reconcile 的设计）。
		//
		// **StaleBlobPlans 不挡回收**（critic F1）：作废计划 = "走完了但没外置"（completed
		// 落了但没 binary_ref）——绝大多数是"结果不大"的正常收尾，**不是**崩溃上下文。
		// 把它当脏信号 = 回收面落空（正常收尾的 session 永远回收不掉，GC 白做）。
		// 它是信息性信号：照常进报告，但不参与"留还是删"。
		//
		// 空事实（Facts == nil）下口径不变：任何 blob.plan 都落进 OrphanBlobToolCalls
		// （分不出 orphan/stale）→ 判脏 → 保守留（宁留不删）。
		scan, rerr := gcReadFile(filepath.Join(d, e.Name()))
		if rerr != nil {
			rep.Skipped++ // 读失败：宁留不删（不猜半个报告能不能信）
			continue
		}
		var facts Facts
		if opts.Facts != nil {
			facts = opts.Facts(sid)
		}
		r := Reconcile(scan, sid, facts)
		rep.StalePlans += int64(len(r.StaleBlobPlans)) // 信息性信号：照常报数（与留/删无关）
		if r.InFlightTurn != nil || len(r.OrphanBlobToolCalls) > 0 {
			rep.Skipped++ // "没走完"的信号还在 = 崩溃上下文还有用 → 留
			continue
		}
		if !opts.DryRun {
			// 删除动作 = Remove 整个 session 的 WAL（GC 的目的是回收，不是留空壳——
			// Truncate 是手工原语，见包 doc）。删不掉 → Skipped、继续扫（不中止整轮）。
			if err := gcRemove(d, sid); err != nil {
				rep.Skipped++
				continue
			}
		}
		rep.Removed++
		rep.FreedBytes += fi.Size()
	}
	return rep, nil
}

// sessionIDOfWALName 从文件名解析 session id：`<26 字符合法 ULID>.wal` 才是我们的
// session WAL（Path 的命名口径）；别的名字一律 false（外来文件不动）。
func sessionIDOfWALName(name string) (contracts.Ulid, bool) {
	if !strings.HasSuffix(name, walSuffix) {
		return "", false
	}
	sid := contracts.Ulid(strings.TrimSuffix(name, walSuffix))
	if !core.ValidUlid(sid) {
		return "", false
	}
	return sid, true
}
