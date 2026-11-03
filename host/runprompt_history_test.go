package host_test

// P1-3（RunPrompt 每轮带历史）的验收用例：
//
//	两轮 prompt → 第二轮的模型请求里**含第一轮的 user prompt 与 assistant 回复**。
//
// 为什么要有这条：协议面 / CLI chat / TUI / ACP 的多轮对话此前**恒空历史**
// （lp.Run = RunWithHistory(ctx, nil, prompt)）——每轮都失忆。设计 §2 要求"每轮折叠
// 事件流带历史（多轮对话同源）"，机制现成：runtime.FoldConversation（事件流 → 对话）
// + loop.RunWithHistory（把历史喂回模型）。
//
// 手法：脚本 provider 记录**每次**模型调用收到的 messages（按调用序）——于是"第二轮
// 带没带上一轮"可被直接断言（不靠猜）。

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/host"
)

// historyProvider 是脚本 provider：每轮返回一段带序号的文本（无工具调用 → 单轮收敛），
// 并**记录每次请求的 messages**。
type historyProvider struct {
	mu   sync.Mutex
	reqs [][]core.Message
}

func (p *historyProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, append([]core.Message(nil), req.Messages...))
	n := len(p.reqs)
	p.mu.Unlock()
	if emit != nil {
		emit(core.ProviderEvent{Kind: "done"})
	}
	return core.ModelResult{
		Text:  fmt.Sprintf("答复-%d", n),
		Usage: core.Usage{InputTokens: 1, OutputTokens: 1},
	}, nil
}

// requests 返回每次模型调用收到的 messages（副本）。
func (p *historyProvider) requests() [][]core.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]core.Message, len(p.reqs))
	copy(out, p.reqs)
	return out
}

// hasMessage 报告 messages 里有没有一条 role=role、content 含 sub 的消息。
func hasMessage(msgs []core.Message, role, sub string) bool {
	for _, m := range msgs {
		if m.Role == role && strings.Contains(m.Content, sub) {
			return true
		}
	}
	return false
}

// TestRunPromptCarriesHistoryAcrossRounds（P1-3）：两轮 prompt → 第二轮带上一轮历史。
func TestRunPromptCarriesHistoryAcrossRounds(t *testing.T) {
	prov := &historyProvider{}
	h := newTestHub(t, func(c *host.Config) { c.Provider = prov })
	sid := host.NewSession()

	if _, err := h.RunPrompt(context.Background(), sid, "第一轮问题"); err != nil {
		t.Fatalf("第一轮 RunPrompt: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "第二轮问题"); err != nil {
		t.Fatalf("第二轮 RunPrompt: %v", err)
	}

	reqs := prov.requests()
	if len(reqs) != 2 {
		t.Fatalf("模型调用次数 = %d，want 2（每轮一次）", len(reqs))
	}
	// 第一轮：新 session 无历史 → 只有本轮 prompt。
	if !hasMessage(reqs[0], "user", "第一轮问题") {
		t.Fatalf("第一轮请求应含本轮 prompt: %+v", reqs[0])
	}
	if hasMessage(reqs[0], "assistant", "答复-1") {
		t.Fatalf("第一轮请求不该带历史（还没有上一轮）: %+v", reqs[0])
	}
	// 第二轮：**带上一轮的历史**（P1-3 的正例）。
	if !hasMessage(reqs[1], "user", "第一轮问题") {
		t.Fatalf("第二轮请求应含第一轮的 user prompt（P1-3：每轮 fold 事件流带历史）: %+v", reqs[1])
	}
	if !hasMessage(reqs[1], "assistant", "答复-1") {
		t.Fatalf("第二轮请求应含第一轮的 assistant 回复: %+v", reqs[1])
	}
	if !hasMessage(reqs[1], "user", "第二轮问题") {
		t.Fatalf("第二轮请求应含本轮 prompt: %+v", reqs[1])
	}
	// 历史**排在**本轮 prompt 之前（顺序即对话顺序）：第一轮的问题不能排在第二轮之后。
	firstIdx, secondIdx := -1, -1
	for i, m := range reqs[1] {
		if firstIdx < 0 && m.Role == "user" && strings.Contains(m.Content, "第一轮问题") {
			firstIdx = i
		}
		if secondIdx < 0 && m.Role == "user" && strings.Contains(m.Content, "第二轮问题") {
			secondIdx = i
		}
	}
	if firstIdx < 0 || secondIdx < 0 || firstIdx >= secondIdx {
		t.Fatalf("历史应排在本轮 prompt 之前（第一轮 idx=%d, 第二轮 idx=%d）: %+v", firstIdx, secondIdx, reqs[1])
	}
}

// ---------------------------------------------------------------------------
// F1：多 agent 历史污染（按主 agent id 预过滤）
// ---------------------------------------------------------------------------

// 三个哨兵串（测试用）：子任务描述（子 agent 的 user.prompt）、子 agent 的自述
// （子 agent 的 model.turn）、子 agent 的 summary（经 spawn 的 tool.call.completed
// 回灌主上下文）。
const (
	childTaskSentinel    = "子任务哨兵ZZZ-请勘察现场"
	childSpeakSentinel   = "子agent自言ZZZ-我看完了"
	childSummarySentinel = "子agent汇报ZZZ-现场已勘察"
)

// multiAgentHistoryProvider：主 agent 第一轮 spawn 一个子 agent（task 带哨兵串），
// 子 agent 吐一句带哨兵的文本再 complete 收敛，主 agent 之后收敛。
//
// 它**记录每次模型调用收到的 messages**——按"有没有 spawn 工具"区分主/子 agent
// （子 agent 是 observer 类型：没有 spawn，只有 complete）。
type multiAgentHistoryProvider struct {
	mu       sync.Mutex
	reqs     []multiAgentCall
	mainTurn int
}

// multiAgentCall 是一次模型调用（hasSpawn = 主 agent 的标志）。
type multiAgentCall struct {
	hasSpawn bool
	msgs     []core.Message
}

func (p *multiAgentHistoryProvider) Run(_ context.Context, req core.ModelRequest, _ func(core.ProviderEvent)) (core.ModelResult, error) {
	hasSpawn, hasComplete := false, false
	for _, tl := range req.Tools {
		switch tl.Name {
		case loop.ToolSpawn:
			hasSpawn = true
		case loop.ToolComplete:
			hasComplete = true
		}
	}
	p.mu.Lock()
	p.reqs = append(p.reqs, multiAgentCall{hasSpawn: hasSpawn, msgs: append([]core.Message(nil), req.Messages...)})
	p.mu.Unlock()
	switch {
	case hasComplete && !hasSpawn:
		// 子 agent：自述一句（→ 它的 model.turn）+ complete 收敛（→ summary）。
		args, _ := json.Marshal(map[string]any{"text": childSummarySentinel, "taint": "clean"})
		return core.ModelResult{
			Text:      childSpeakSentinel,
			ToolCalls: []core.ToolCallReq{{ID: "child-done", Name: loop.ToolComplete, Args: args}},
			Usage:     core.Usage{InputTokens: 1, OutputTokens: 1},
		}, nil
	case hasSpawn:
		p.mu.Lock()
		p.mainTurn++
		n := p.mainTurn
		p.mu.Unlock()
		if n == 1 {
			args, _ := json.Marshal(map[string]string{
				"agent_type": "observer",
				"role":       "勘察",
				"task":       childTaskSentinel,
				"request_id": string(core.NewUlid()),
			})
			return core.ModelResult{
				ToolCalls: []core.ToolCallReq{{ID: "spawn-1", Name: loop.ToolSpawn, Args: args}},
				Usage:     core.Usage{InputTokens: 1, OutputTokens: 1},
			}, nil
		}
		return core.ModelResult{Text: "主 agent 收尾", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	default:
		return core.ModelResult{Text: "done", Usage: core.Usage{InputTokens: 1, OutputTokens: 1}}, nil
	}
}

// lastMainRequest 返回主 agent（带 spawn 工具）的最后一次模型调用收到的 messages。
func (p *multiAgentHistoryProvider) lastMainRequest() []core.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.reqs) - 1; i >= 0; i-- {
		if p.reqs[i].hasSpawn {
			return p.reqs[i].msgs
		}
	}
	return nil
}

// TestRunPromptHistoryIsMainAgentView（F1）：多 agent session 里，主 agent 下一轮的
// 历史**不含**子 agent 自己的 user.prompt / model.turn，但**含** spawn 工具的 tool
// 消息（子 agent 的产出经 completed 回灌——那才是主 agent 该看到的形态）。
//
// 修之前的病：主 agent 会看到"没发生过的用户话"（子任务描述）与子 agent 的自述——
// 同一 session 的事件流里混着多个 agent 的对话，而 FoldConversation 的契约要求
// "调用方应按 agent_id 预过滤"（本刀之前没人过滤）。
func TestRunPromptHistoryIsMainAgentView(t *testing.T) {
	prov := &multiAgentHistoryProvider{}
	h := newTestHub(t, func(c *host.Config) {
		c.MultiAgent = true
		c.Provider = prov
		c.Allow = []string{"read"} // observer 的底座工具（子 agent 只 complete，不真调 read）
	})
	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "第一轮：派个 observer"); err != nil {
		t.Fatalf("第一轮 RunPrompt: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "第二轮：继续"); err != nil {
		t.Fatalf("第二轮 RunPrompt: %v", err)
	}

	msgs := prov.lastMainRequest()
	if msgs == nil {
		t.Fatal("没抓到主 agent 的模型请求（provider 一次都没被主 agent 调用？）")
	}
	// ① 主 agent 自己的历史还在（过滤不能把主对话也滤掉）。
	if !hasMessage(msgs, "user", "第一轮：派个 observer") {
		t.Fatalf("主 agent 的历史应保留自己的 user prompt: %+v", msgs)
	}
	// ② 子 agent 的 user.prompt（子任务描述）不该出现在主 agent 的历史里。
	if hasMessage(msgs, "user", childTaskSentinel) {
		t.Fatalf("子 agent 的 user.prompt 混进了主 agent 的历史（F1：应按主 agent id 预过滤）: %+v", msgs)
	}
	// ③ 子 agent 的 model.turn（自述）不该出现。
	if hasMessage(msgs, "assistant", childSpeakSentinel) {
		t.Fatalf("子 agent 的 model.turn 混进了主 agent 的历史（F1）: %+v", msgs)
	}
	// ④ 子 agent 的产出经 spawn 的 tool.call.completed 回灌（tool 消息）——信息不丢。
	if !hasMessage(msgs, "tool", childSummarySentinel) {
		t.Fatalf("主 agent 的历史应含 spawn 工具的 tool 消息（子产出经 completed 回灌）: %+v", msgs)
	}
}
