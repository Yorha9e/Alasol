package main

// 本文件是 `alasol wal-gc` 子命令（缺口 5 第一件）：扫 [wal].dir 的 per-session WAL，
// 按**显式龄期**回收"已没用"的文件，打印**单行 JSON 报告**（形态照 `alasol blob-gc`：
// JSON 到 stdout、人话/错误到 stderr、负值/缺省即拒退出码 2）。
//
// # -retention 必给（缺省 0 = 拒，退出码 2）
//
// 删 WAL = 丢观察面（不丢事实——WAL 不是事实源，但"当时在干什么"这段现场**不可逆**）。
// 龄期是运维决定，没有安全默认：缺省 0 / 负值一律**退出码 2 + 点名 -retention**（与库层
// walfile.GC 的 Retention<=0 拒绝同口径）。与 blob-gc 的 0 语义不同（那边 0 = 只安全两扫
// 是合法策略；这边"没给龄期"不是策略，是还没决定）。
//
// # dir 未配置/不存在 = 明确报错（同 blob-gc）
//
// [wal].dir 空 → 退出码 2 + 点名（WAL 未配对 `alasol wal` 是合法状态，对 GC 是"没有
// 操作对象"——静默"扫了 0 个"会让人以为清过了）；目录不存在同样退出码 2（GC 不替用户建
// 目录）。
//
// # 目录来源：-wal-dir flag 优先于配置文件 [wal].dir（与 `alasol wal` 同口径）
//
// `-wal-dir` 显式给了就用它（运维临时对某个目录跑一次 GC，不必改配置文件）；没给才读
// 配置文件的 `[wal].dir`。两个都没有 → 退出码 2 + 点名（同 blob-gc）。
//
// # 诚实边界：不读事件库 → 空事实对账（宁留不删）
//
// 本命令没有 -db：reconcile 的事实面（孤儿 blob / 作废计划）对不上事件流 → walfile.GC
// 收到 nil Facts = **空事实** → 任何 blob.plan 都被对账成"孤儿候选" → 判脏 → 保守不删。
// 即：**有 blob.plan 的 session WAL 恒留**（这条保守路径是刻意的——要按事实回收就等
// 事件库接线的后续刀；报告的 Skipped 如实数着它们）。
//
// # DryRun 与真跑：同一判据；**"删不掉"那一刻两模式会分叉**（如实写明）
//
// 判据同源（同一个"该删"判定），但 **Removed = 判据命中数**：真跑时"判据命中却**删不掉**
// （Windows 上被别的进程打开 → sharing violation）"的那一个会计入 **Skipped**（不是
// Removed），而 DryRun **不真删**、测不出删不掉 → 仍计入 Removed。故删失败场景下
// dry.Removed 会比真跑**多** 1。这是执行事实的分叉（报告如实呈现），不是判据不一致。

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// walGCMain 是 `alasol wal-gc`：WAL 目录的 GC 报告（单行 JSON 到 stdout）。
//
// flags：
//
//	-config <path>    用户级配置文件路径（TOML；空=平台默认位，H15）——读 [wal].dir
//	-wal-dir <dir>    WAL 根目录（**显式 flag 优先**；空 = 读配置文件的 [wal].dir）
//	-retention <dur>  WAL 的 mtime 保留期（**必给**：缺省 0 = 明确报错退出码 2，
//	                  负值同；删 WAL 不可逆，龄期由运维显式决定）
//	-dry-run          只报不删（报告如实算 Removed/Skipped/FreedBytes；**Removed =
//	                  判据命中数**，真跑"删不掉"会计 Skipped → 删失败场景下两模式的
//	                  Removed 会分叉，判据本身同源）
func walGCMain(args []string) int {
	fs := flag.NewFlagSet("wal-gc", flag.ExitOnError)
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	walDir := fs.String("wal-dir", "", "WAL 根目录（显式 flag > 配置文件的 [wal].dir；空=读配置）")
	retention := fs.Duration("retention", 0, "WAL 的 mtime 保留期（**必给**：缺省 0 = 报错退出码 2，负值同；删 WAL 不可逆，龄期由运维显式决定）")
	dryRun := fs.Bool("dry-run", false, "只报不删（报告如实算，磁盘不动；Removed = 判据命中数——真跑删不掉会计 Skipped，故删失败场景下两模式的 Removed 会分叉）")
	_ = fs.Parse(args)

	// 缺省 0 / 负值即拒（与库层 walfile.GC 同口径）：龄期没有安全默认——
	// 静默按 0 跑等于悄悄换了一套回收策略（"全删"？"不删"？都不是用户说的）。
	if *retention <= 0 {
		fmt.Fprintf(os.Stderr, "wal-gc: -retention 必须显式给一个正的龄期（得到 %v）——删 WAL 不可逆，龄期由运维决定，缺省不替你猜\n", *retention)
		return 2
	}

	// 目录：**显式 -wal-dir flag 优先**，没给才读配置文件的 [wal].dir（与 `alasol wal`
	// 同一条口径：flag > 配置文件）。
	fc, err := host.LoadFileConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}
	dir := strings.TrimSpace(*walDir)
	if dir == "" {
		dir = strings.TrimSpace(fc.Wal.Dir)
	}
	if dir == "" {
		fmt.Fprintln(os.Stderr, "wal-gc: WAL 目录未配置（-wal-dir 没给且 [wal].dir 为空）——没有可 GC 的目录；"+
			"用 -wal-dir 指定，或先在配置文件的 [wal] 段设置 dir")
		return 2
	}
	// 目录不存在 → 明确报错（同 blob-gc：建出一个空目录再报"扫了 0 个"是假成功）。
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		if err == nil {
			err = fmt.Errorf("不是目录")
		}
		fmt.Fprintf(os.Stderr, "wal-gc: WAL 目录不可用 %q: %v\n", dir, err)
		return 2
	}

	// Facts 不传（nil = 空事实）：本命令不读事件库，见文件头的诚实边界。
	rep, gerr := walfile.GC(dir, walfile.GCOptions{
		Retention: *retention,
		DryRun:    *dryRun,
	})
	// 报告**打到 stdout**（机器可消费，单行 JSON）；中途出错也打印部分报告 + 退出码 1。
	out, merr := json.Marshal(rep)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "序列化报告失败: %v\n", merr)
		return 1
	}
	fmt.Println(string(out))
	if gerr != nil {
		fmt.Fprintln(os.Stderr, "wal-gc 中途出错（以上为部分报告）:", gerr)
		return 1
	}
	return 0
}
