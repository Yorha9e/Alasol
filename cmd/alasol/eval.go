package main

// 本文件是 `alasol eval -session <id>` 子命令（L4/L5 最小评测面，v4-7 后半）：
// **事件流即数据集**——从事件库回放一场 session 的 durable 事件（复用
// runtime.ReplaySession），折叠出逐 turn 指标，JSON 报告打到 stdout。
//
// # 观察面，不是评测框架（L5 口径）
//
// 不建独立评测框架：没有 runner、没有 case 库、没有打分器、没有新存储——
// 事件流（core.Store）就是唯一数据集，本命令只是它的一个**只读折叠视图**。
// 想要别的指标 → 写另一个折叠函数读同一个事件流，而不是往本命令里加插件系统。
//
// # 指标口径（逐 turn）
//
//   - turn = 一条 model.turn 事件（provider 每返回一轮记一条，F1）；
//   - tool_calls = 该 model.turn 之后、下一个 model.turn 之前的 tool.call.intent 条数
//     （X1 三段之①——intent 是"发起调用"的事实，completed/failed 不重复计数；
//     首个 model.turn 之前的 intent 只进 totals、不归任何轮——正常 X1 流里不会出现）；
//   - input/output tokens = 该 model.turn 载荷里的 usage（J21：provider 不上报则为 0）；
//   - duration_ms = 本 model.turn 的 ts 到**下一个** model.turn（或全场最后一条事件）的
//     ts 差——"这一轮花了多久"（含工具执行与审批等待；RFC3339 时间戳，解析失败记 0）。
//
// totals 是全场汇总：turns/model.turn 条数、tool_calls/intent 总条数、tokens 求和、
// duration_ms = 首条事件到最后一条事件的墙钟差（整场时长）。**context.compacted 的
// usage 不计入**（压缩调用的成本在 /api/cost 的索引器口径里；评测面只看对话轮）。
//
// 报告是**单行 JSON 到 stdout**（机器可消费，与 wal 同口径）；无事件的 session =
// 空报告（turns=[]），不是错误（"跑过但没产出"与"没跑过"在指标上都是零，如实报零）。

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// 本包内 fold 用的 kind 常量（与 core/loop 落的 kind 一致；不 import loop——
// cmd 只依赖 contracts 的字面量，与 runtime/replay 同款纪律）。
const (
	evalKindModelTurn      = "model.turn"
	evalKindToolCallIntent = "tool.call.intent"
)

// EvalTurn 是一个对话轮的指标（一条 model.turn + 它之后的工具调用窗口）。
type EvalTurn struct {
	// Turn 轮次号（按 model.turn 出现序，从 1 起）。
	Turn int `json:"turn"`
	// Model 本轮模型名（provider 上报/装配层记录；可空）。
	Model string `json:"model,omitempty"`
	// ToolCalls 该轮之后、下一轮之前的 tool.call.intent 条数。
	ToolCalls int `json:"tool_calls"`
	// InputTokens / OutputTokens 本轮 usage（provider 不上报 = 0）。
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	// DurationMS 本轮 ts 到下一轮（或全场最后事件）ts 的毫秒差。
	DurationMS int64 `json:"duration_ms"`
}

// EvalTotals 是全场汇总。
type EvalTotals struct {
	Turns        int   `json:"turns"`
	ToolCalls    int   `json:"tool_calls"`
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	DurationMS   int64 `json:"duration_ms"`
}

// EvalReport 是 `alasol eval` 的 JSON 报告（stdout 单行）。
type EvalReport struct {
	SessionID string     `json:"session_id"`
	Events    int        `json:"events"`
	Turns     []EvalTurn `json:"turns"`
	Totals    EvalTotals `json:"totals"`
}

// evalMain 是 `alasol eval -session <id>`：回放事件流 → 逐 turn 指标 → 单行 JSON。
//
// flags：
//
//	-session <id>  要评测的 session id（必填，26 字符 ULID）
//	-db <path>     事件库路径（SQLite；与 run/resume/wal 同口径）
func evalMain(args []string) int {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	session := fs.String("session", "", "要评测的 session id（必填，26 字符 ULID）")
	dbPath := fs.String("db", "alasol.db", "事件库路径（SQLite）")
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

	st, err := sqlite.Open(*dbPath, sqlite.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "打开事件库:", err)
		return 1
	}
	defer st.Close()

	events, err := runtime.ReplaySession(context.Background(), st, sid)
	if err != nil {
		fmt.Fprintln(os.Stderr, "回放事件流:", err)
		return 1
	}

	rep := evalReport(sid, events)
	out, err := json.Marshal(rep)
	if err != nil {
		fmt.Fprintf(os.Stderr, "序列化报告失败: %v\n", err)
		return 1
	}
	fmt.Println(string(out))
	return 0
}

// evalReport 从按 seq 升序的 durable 事件折叠出评测报告（纯函数：cmd 侧唯一实现，
// 测试直接喂事件断言指标）。
//
// 容错（D7 宽容）：载荷不可解析的 model.turn 被跳过（不中断 fold、不影响其余计数），
// 与 indexer / FoldConversation 同源口径。
func evalReport(sid contracts.Ulid, events []contracts.Event) EvalReport {
	rep := EvalReport{SessionID: string(sid), Events: len(events), Turns: []EvalTurn{}}

	turns := make([]EvalTurn, 0, 8)
	turnTS := make([]time.Time, 0, 8) // 每轮 model.turn 的 ts（duration 结算用）
	turnTSOK := make([]bool, 0, 8)
	cur := -1 // 当前轮下标（-1 = 还没有 model.turn）
	toolCallsTotal := 0
	var firstTS, lastTS time.Time
	haveTS := false

	for i := range events {
		ev := &events[i]
		var ts time.Time
		_tsOK := false
		if t, err := time.Parse(time.RFC3339, ev.TS); err == nil {
			ts, _tsOK = t, true
			if !haveTS {
				firstTS, haveTS = t, true
			}
			lastTS = t
		}
		switch ev.Kind {
		case evalKindModelTurn:
			var p contracts.ModelTurnPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				continue
			}
			// 结算上一轮 duration：到本条 model.turn 为止。
			if cur >= 0 && _tsOK && turnTSOK[cur] {
				if d := ts.Sub(turnTS[cur]); d > 0 {
					turns[cur].DurationMS = d.Milliseconds()
				}
			}
			nt := EvalTurn{Turn: len(turns) + 1}
			if p.Model != nil {
				nt.Model = *p.Model
			}
			if p.Usage != nil {
				nt.InputTokens = p.Usage.InputTokens
				nt.OutputTokens = p.Usage.OutputTokens
			}
			turns = append(turns, nt)
			turnTS = append(turnTS, ts)
			turnTSOK = append(turnTSOK, _tsOK)
			cur = len(turns) - 1
		case evalKindToolCallIntent:
			toolCallsTotal++
			if cur >= 0 {
				turns[cur].ToolCalls++
			}
			// cur < 0（intent 在首个 model.turn 之前）只进 totals、不归任何轮——
			// 正常 X1 流里 intent 恒在 model.turn 之后，这里是防御性口径。
		}
	}
	// 末轮 duration：到最后一条可解析 ts 的事件为止。
	if cur >= 0 && haveTS && turnTSOK[cur] {
		if d := lastTS.Sub(turnTS[cur]); d > 0 {
			turns[cur].DurationMS = d.Milliseconds()
		}
	}

	rep.Turns = turns
	rep.Totals = EvalTotals{Turns: len(turns), ToolCalls: toolCallsTotal}
	for _, tr := range turns {
		rep.Totals.InputTokens += tr.InputTokens
		rep.Totals.OutputTokens += tr.OutputTokens
	}
	if haveTS {
		if d := lastTS.Sub(firstTS); d > 0 {
			rep.Totals.DurationMS = d.Milliseconds()
		}
	}
	return rep
}
