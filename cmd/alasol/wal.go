package main

// 本文件是 `alasol wal <session>` 子命令（v4-4-B 恢复面）：扫某 session 的 WAL 并与事件流
// 对账，打印**崩溃恢复报告**（单行 JSON 到 stdout；人话摘要到 stderr）。
//
// # 报告是观察面，不是写回面（F5 不受影响）
//
// 它**只读**两侧（WAL 文件 + 事件库），不写事件流、不补 delta、不改任何状态。WAL 里的
// delta 文本进报告供展示（"当时吐到哪儿了"），但绝不自动回填成 durable 事件——那会让 WAL
// 变成第二事实源（F5 与陷阱 11 一起破）。
//
// # 什么时候用
//
// 进程崩了（panic / kill -9 / OOM / 断电）之后：`alasol wal -session <id>` 告诉你"上次崩在
// 第几轮、跑的什么模型、吐过几段 delta、哪些 tool_call 的 blob 可能成了孤儿"。
//
// 未配置 WAL（`[wal].dir` 空）→ wal_exists=false，报告如实说"没有 WAL"（**不是错误**：
// "没配"是合法状态，退出码 0）。

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// walMain 是 `alasol wal -session <id>`：打印崩溃恢复报告。
//
// flags：
//
//	-session <id>   要查的 session id（必填，26 字符 ULID）
//	-db <path>      事件库路径（SQLite；与 run/resume 同口径）
//	-wal-dir <dir>  WAL 根目录（显式 flag 优先；空 = 读用户级 TOML 的 [wal].dir）
//	-config <path>  用户级配置文件路径（TOML；空=平台默认位，H15）
//	-json           只打印 JSON（不打印人话摘要；默认人话摘要走 stderr）
func walMain(args []string) int {
	fs := flag.NewFlagSet("wal", flag.ExitOnError)
	session := fs.String("session", "", "要查的 session id（必填，26 字符 ULID）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
	walDir := fs.String("wal-dir", "", "WAL 根目录（显式 flag > 配置文件的 [wal].dir；空=关）")
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	jsonOnly := fs.Bool("json", false, "只打印 JSON 报告（人话摘要仍走 stderr）")
	_ = fs.Parse(args)

	if strings.TrimSpace(*session) == "" {
		fmt.Fprintln(os.Stderr, "-session 不能为空")
		return 2
	}
	sid := contracts.Ulid(strings.TrimSpace(*session))
	if !core.ValidUlid(sid) {
		fmt.Fprintf(os.Stderr, "-session 不是合法 ULID（26 字符 Crockford base32）：%q\n", *session)
		return 2
	}

	// WAL 目录：显式 flag > 配置文件（与 run/resume/serve 的 [wal] 同口径）。
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

	st, err := sqlite.Open(*dbPath, sqlite.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "打开事件库:", err)
		return 1
	}
	defer st.Close()

	rep, rerr := walReportFor(context.Background(), st, dir, sid)
	// 报告**打到 stdout**（机器可消费，单行 JSON）；读坏了也打印部分报告 + 退出码 1。
	out, merr := json.Marshal(rep)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "序列化报告失败: %v\n", merr)
		return 1
	}
	fmt.Println(string(out))
	if !*jsonOnly {
		if s := rep.Summary(); s != "" {
			fmt.Fprintln(os.Stderr, s)
		}
	}
	if rerr != nil {
		fmt.Fprintln(os.Stderr, "对账 WAL 失败（以上为部分报告）:", rerr)
		return 1
	}
	return 0
}

// walReportFor 扫 dir 下某 session 的 WAL 并与事件流对账（cmd 侧的**唯一**实现；
// resume 的一行摘要也走它——口径只定义一次，D21）。
//
// dir 空 = WAL 未配置 → 报告如实说"没有 WAL"（wal_exists=false），**不是错误**。
// 读坏了仍返回**部分报告**（扫描已解析成功的记录）+ 错误（调用方决定怎么呈现）。
func walReportFor(ctx context.Context, st core.Store, dir string, sid contracts.Ulid) (walfile.Report, error) {
	if strings.TrimSpace(dir) == "" {
		return walfile.Report{SessionID: string(sid), WALExists: false}, nil
	}
	events, err := st.ReadFrom(ctx, &sid, 0, 0)
	if err != nil {
		return walfile.Report{SessionID: string(sid), WALExists: false},
			fmt.Errorf("读 session %s 的事件流: %w", sid, err)
	}
	return walfile.ReconcileSession(dir, sid, walfile.FactsFromEvents(events))
}

// walResumeSummary 是 resume 时自动打印的**一行** WAL 摘要（v4-4-B：崩溃后跑 resume 就能
// 看见"上次崩在哪儿"，不必另开一个命令）。
//
// 无异常 / 未配置 WAL → 空串（不打印：没话说的时候不说话，比每行都挂一句噪音好）。
func walResumeSummary(ctx context.Context, st core.Store, dir string, sid contracts.Ulid) string {
	if strings.TrimSpace(dir) == "" {
		return "" // 未配置 WAL：零行为变化
	}
	rep, err := walReportFor(ctx, st, dir, sid)
	if err != nil {
		return fmt.Sprintf("[wal] 对账失败（忽略，不影响恢复流程）: %v", err)
	}
	return rep.Summary()
}
