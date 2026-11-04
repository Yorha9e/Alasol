package contracts

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// sampleEventsPath is the NDJSON sample shared with the TypeScript consumer
// (tools/ts-consumer/sample-events.ndjson). Both sides read the same file, so
// a change of the contract breaks the Go and the TypeScript check at once.
const sampleEventsPath = "../tools/ts-consumer/sample-events.ndjson"

// ulidPattern is the framework id rule of common.json#/$defs/Ulid
// (26 characters, Crockford base32). The Go type is a plain string, so the
// pattern is the only place where the rule can be enforced.
var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

// TestSampleEventsDecodeIntoContracts is the Go side of the cross-language
// check: every line of the shared sample must decode into the generated types
// and survive a round trip unchanged.
func TestSampleEventsDecodeIntoContracts(t *testing.T) {
	data, err := os.ReadFile(sampleEventsPath)
	if err != nil {
		t.Fatalf("read %s: %v", sampleEventsPath, err)
	}

	lines := bytes.Split(data, []byte("\n"))
	seen := 0
	kinds := map[string]int{}
	for i, raw := range lines {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			continue
		}
		seen++
		lineNo := i + 1

		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Errorf("line %d: unmarshal into Event: %v", lineNo, err)
			continue
		}
		kinds[ev.Kind]++

		// The id fields are ULIDs: the type is a string, the rule is the pattern.
		checkULID(t, lineNo, "id", string(ev.ID))
		checkULID(t, lineNo, "session_id", string(ev.SessionID))
		checkULID(t, lineNo, "agent_id", string(ev.AgentID))
		if ev.CausationID != nil {
			checkULID(t, lineNo, "causation_id", string(*ev.CausationID))
		}
		if ev.V != 1 {
			t.Errorf("line %d: v = %d, want 1", lineNo, ev.V)
		}
		if ev.TS == "" {
			t.Errorf("line %d: ts is empty", lineNo)
		}

		// The payload stays raw JSON: the bytes must not be touched.
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Errorf("line %d: unmarshal into envelope: %v", lineNo, err)
			continue
		}
		if !bytes.Equal(envelope["payload"], ev.Payload) {
			t.Errorf("line %d: payload bytes changed:\n got %s\nwant %s", lineNo, ev.Payload, envelope["payload"])
		}
		if len(ev.Payload) == 0 {
			t.Errorf("line %d: payload is empty", lineNo)
			continue
		}

		// The payload of a known kind decodes into its own type.
		switch ev.Kind {
		case "tool.call.intent":
			var p ToolCallIntentPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallIntentPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if p.ToolName == "" {
				t.Errorf("line %d: tool_name is empty", lineNo)
			}
			if len(p.Arguments) == 0 {
				t.Errorf("line %d: arguments are empty", lineNo)
			}
			if p.Replay != ReplayKindSafe {
				t.Errorf("line %d: replay = %q, want %q", lineNo, p.Replay, ReplayKindSafe)
			}
		case "tool.call.completed":
			var p ToolCallCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallCompletedPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if len(p.Result.Content) == 0 {
				t.Errorf("line %d: result.content is empty", lineNo)
				break
			}
			if p.Result.Content[0].Type != "text" {
				t.Errorf("line %d: result.content[0].type = %q, want %q", lineNo, p.Result.Content[0].Type, "text")
			}
			if len(p.Result.GraphEdges) == 0 {
				t.Errorf("line %d: result.graph_edges is empty", lineNo)
			}
		case "tool.call.failed":
			var p ToolCallFailedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallFailedPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if p.Error.Message == "" {
				t.Errorf("line %d: error.message is empty", lineNo)
			}
			if p.Error.Code == nil {
				t.Errorf("line %d: error.code is missing", lineNo)
			}
		case "context.assembled":
			var p ContextSnapshot
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ContextSnapshot: %v", lineNo, err)
				break
			}
			if p.SessionID == "" {
				t.Errorf("line %d: context snapshot session_id is empty", lineNo)
			}
			if p.TokensBudget == 0 {
				t.Errorf("line %d: context snapshot tokens_budget is zero", lineNo)
			}
			if len(p.Parts) == 0 {
				t.Errorf("line %d: context snapshot parts are empty", lineNo)
			}
		case "user.prompt":
			var p UserPromptPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into UserPromptPayload: %v", lineNo, err)
				break
			}
			if p.Text == "" {
				t.Errorf("line %d: user.prompt text is empty", lineNo)
			}
		case "model.turn":
			var p ModelTurnPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ModelTurnPayload: %v", lineNo, err)
				break
			}
			// usage 只做成本记录（J21）：样本里必须带得上，且 token 数非零。
			if p.Usage == nil {
				t.Errorf("line %d: model.turn 应带 usage（token 计量）", lineNo)
				break
			}
			if p.Usage.InputTokens == 0 || p.Usage.OutputTokens == 0 {
				t.Errorf("line %d: model.turn usage 应带非零 token 数: %+v", lineNo, p.Usage)
			}
			// model 是本轮模型名（成本统计按模型分组用）：样本里必须带得上且非空——
			// "成本按模型分组"依赖这个样本事实（每处契约增量都要有钉子）。
			if p.Model == nil || *p.Model == "" {
				t.Errorf("line %d: model.turn 应带非空 model（成本统计按模型分组）", lineNo)
			}
			// reasoning 是思维链整段（G14）：带了就必须非空（不拿空串占位——
			// "解析不到就缺省"才是诚实口径）。
			if p.Reasoning != nil && *p.Reasoning == "" {
				t.Errorf("line %d: model.turn reasoning 带了就必须非空（解析不到应缺省）", lineNo)
			}
		case "model.delta":
			var p ModelDeltaPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ModelDeltaPayload: %v", lineNo, err)
				break
			}
			// G13：流事件 durable=false 且不占 seq（F8/陷阱 11）——样本必须钉住这两条。
			if ev.Durable {
				t.Errorf("line %d: model.delta 必须 durable=false（流事件）", lineNo)
			}
			if ev.Seq != nil {
				t.Errorf("line %d: model.delta 不得携带 seq（durable=false ⇒ seq 缺省）", lineNo)
			}
			if p.Turn < 1 {
				t.Errorf("line %d: model.delta turn = %d，应 ≥1（同一 Run 内从 1 递增）", lineNo, p.Turn)
			}
			// text/reasoning 至少一个非空（没有可显示内容的片段不得发布）。
			if (p.Text == nil || *p.Text == "") && (p.Reasoning == nil || *p.Reasoning == "") {
				t.Errorf("line %d: model.delta 的 text/reasoning 至少一个非空", lineNo)
			}
		case "gate.left":
			var p GateLeftPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into GateLeftPayload: %v", lineNo, err)
				break
			}
			// G15：形状与 gate.joined 对称（gate_id + source），来源在同一枚举内。
			if p.GateID == "" {
				t.Errorf("line %d: gate.left gate_id 不能为空", lineNo)
			}
			switch p.Source {
			case GateJoinSourceAutoRepo, GateJoinSourceAutoWorkflow, GateJoinSourceExplicit:
			default:
				t.Errorf("line %d: gate.left source = %q 不在枚举内", lineNo, p.Source)
			}
		case "gate.joined":
			var p GateJoinedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into GateJoinedPayload: %v", lineNo, err)
				break
			}
			if p.GateID == "" {
				t.Errorf("line %d: gate.joined gate_id 不能为空", lineNo)
			}
			switch p.Source {
			case GateJoinSourceAutoRepo, GateJoinSourceAutoWorkflow, GateJoinSourceExplicit:
			default:
				t.Errorf("line %d: gate.joined source = %q 不在枚举内", lineNo, p.Source)
			}
		case "agentlink.broadcast":
			var p AgentLinkBroadcastPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into AgentLinkBroadcastPayload: %v", lineNo, err)
				break
			}
			checkAgentLinkTarget(t, lineNo, p.Target)
			if p.Text == "" {
				t.Errorf("line %d: agentlink.broadcast text 不能为空", lineNo)
			}
		case "agentlink.handoff":
			var p AgentLinkHandoffPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into AgentLinkHandoffPayload: %v", lineNo, err)
				break
			}
			checkAgentLinkTarget(t, lineNo, p.Target)
			if p.Text == "" {
				t.Errorf("line %d: agentlink.handoff text 不能为空", lineNo)
			}
			if len(p.Payload) == 0 {
				t.Errorf("line %d: agentlink.handoff 应带结构化 payload", lineNo)
			}
		case "context.compacted":
			var p ContextCompactedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ContextCompactedPayload: %v", lineNo, err)
				break
			}
			// refs 永不指摘要：ref 必须是 stage:N 指针（指向持久化原文，给 LLM 调 recall）。
			if !strings.HasPrefix(p.Ref, "stage:") {
				t.Errorf("line %d: context.compacted ref = %q，应以 stage: 前缀指向持久化原文（refs 永不指摘要）", lineNo, p.Ref)
			}
			// seq 区间给机器：下界 ≤ 上界且非负（被压原文的事件区间）。
			if p.SeqFrom < 0 || p.SeqTo < p.SeqFrom {
				t.Errorf("line %d: context.compacted seq 区间非法：[%d, %d]", lineNo, p.SeqFrom, p.SeqTo)
			}
			switch p.Action {
			case ContextCompactionActionDowngrade, ContextCompactionActionSplit,
				ContextCompactionActionBudgetWarning, ContextCompactionActionFallback:
			default:
				t.Errorf("line %d: context.compacted action = %q 不在枚举内", lineNo, p.Action)
			}
			switch p.Tier {
			case ContextTierFull, ContextTierL1, ContextTierL2:
			default:
				t.Errorf("line %d: context.compacted tier = %q 不在枚举内", lineNo, p.Tier)
			}
			// F28：reason 取值集合要与实现产出的口径对齐（契约描述里列举的那几个）。
			if p.Reason != nil {
				switch *p.Reason {
				case "validation_pass", "budget_fallback", "orchestrator_oversize",
					"fallback_truncate", "stage_over_w_div_3",
					"injection_oversize", "total_oversize":
				default:
					if !strings.HasPrefix(*p.Reason, "retention_downgrade_") {
						t.Errorf("line %d: context.compacted reason = %q 不在实现产出的取值集合内", lineNo, *p.Reason)
					}
				}
			}
			// 压缩模型调用的 usage（成本统计口径，F16）：带 usage 时必须带 model 名。
			if p.Usage != nil && p.Model == nil {
				t.Errorf("line %d: context.compacted 带 usage 时应带 model 名（成本按模型分组）", lineNo)
			}
		case "review.recorded":
			var p ReviewRecordedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ReviewRecordedPayload: %v", lineNo, err)
				break
			}
			// V6：被 review 的分支 + tip 都必须记上（tip 是失效判定基准，缺了就无法检测失效）。
			if p.Branch == "" {
				t.Errorf("line %d: review.recorded branch 不能为空", lineNo)
			}
			if p.TipCommit == "" {
				t.Errorf("line %d: review.recorded tip_commit 不能为空（V6 失效判定基准）", lineNo)
			}
			switch p.Conclusion {
			case ReviewConclusionApproved, ReviewConclusionChangesRequested:
			default:
				t.Errorf("line %d: review.recorded conclusion = %q 不在枚举内", lineNo, p.Conclusion)
			}
		case "conflict.predicted":
			var p ConflictPredictedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ConflictPredictedPayload: %v", lineNo, err)
				break
			}
			if p.MergedBranch == "" {
				t.Errorf("line %d: conflict.predicted merged_branch 不能为空", lineNo)
			}
			// 逐分支撞车预判：collides=true 必须带 basis（依据）；Available=false 不可当作"无冲突"（N2 诚实降级）。
			for i, r := range p.Risks {
				if r.Branch == "" {
					t.Errorf("line %d: conflict.predicted risks[%d].branch 不能为空", lineNo, i)
				}
				if r.Collides && len(r.Basis) == 0 {
					t.Errorf("line %d: conflict.predicted risks[%d] collides=true 必须带 basis（撞车依据）", lineNo, i)
				}
				// basis 条目必须带命中类型前缀（direct=同触点 / one-side=传递命中）：分级可辨。
				for _, b := range r.Basis {
					if !strings.HasPrefix(b, "direct:") && !strings.HasPrefix(b, "one-side:") {
						t.Errorf("line %d: conflict.predicted risks[%d].basis 条目 %q 缺命中类型前缀（direct:/one-side:）", lineNo, i, b)
					}
				}
				// 覆盖度信号（unknown_roots / truncated）出现时，可用性必须为 false：
				// 覆盖不足还说"可用"= 假"无冲突"。
				truncated := r.Truncated != nil && *r.Truncated
				if (len(r.UnknownRoots) > 0 || truncated) && r.Available {
					t.Errorf("line %d: conflict.predicted risks[%d] 带覆盖度信号时 available 必须为 false", lineNo, i)
				}
				// Available=false 且没撞车 → 理由不得说"无交集"（那是"可用"的结论）。
				if !r.Available && !r.Collides && r.Reason != nil && strings.Contains(*r.Reason, "无交集") {
					t.Errorf("line %d: conflict.predicted risks[%d] available=false 时理由不得说\"无交集\"：%q", lineNo, i, *r.Reason)
				}
			}
			// N-1 聚合诚实化：**不许只读聚合层**——available=true 时不得存在
			// risks[].available=false 的分支（否则只读聚合的消费端会把"可用且 0 撞车"
			// 读成"无冲突"）；有不可评估分支时聚合层必须 available=false + unevaluated>0。
			unevaluated := 0
			for i, r := range p.Risks {
				if !r.Available {
					unevaluated++
					if p.Available {
						t.Errorf("line %d: conflict.predicted risks[%d] available=false 时聚合层 available 必须为 false", lineNo, i)
					}
				}
			}
			if unevaluated > 0 {
				if p.Unevaluated == nil || *p.Unevaluated != int64(unevaluated) {
					t.Errorf("line %d: conflict.predicted 有 %d 个不可评估分支，unevaluated 应为 %d（得到 %v）", lineNo, unevaluated, unevaluated, p.Unevaluated)
				}
			}
			if p.Available && p.Unevaluated != nil && *p.Unevaluated > 0 {
				t.Errorf("line %d: conflict.predicted available=true 时 unevaluated 必须为 0/缺省（得到 %d）", lineNo, *p.Unevaluated)
			}
			// collisions 只数**确认命中**：不可评估的分支不计入（恒等式）。
			confirmed := 0
			for _, r := range p.Risks {
				if r.Collides {
					confirmed++
				}
			}
			if p.Collisions != int64(confirmed) {
				t.Errorf("line %d: conflict.predicted collisions = %d，期望确认命中数 %d", lineNo, p.Collisions, confirmed)
			}
		case "hub.alert":
			var p HubAlertPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into HubAlertPayload: %v", lineNo, err)
				break
			}
			// K9 告警事件（v2-S5 清账②）：指标名/实测值/阈值/人读正文四要素齐全才可复盘。
			if p.Metric == "" {
				t.Errorf("line %d: hub.alert metric 不能为空", lineNo)
			}
			if p.Threshold <= 0 {
				t.Errorf("line %d: hub.alert threshold 应 >0（0 = 实现未提供阈值，不该出现在事实事件里）", lineNo)
			}
			if p.Message == "" {
				t.Errorf("line %d: hub.alert message 不能为空（人读正文）", lineNo)
			}
			// 伪归属的三个 id 的**框架形态**由通用的 checkULID 断言（每行都查：
			// 26 字符 Crockford + 首字符 '0'..'7'——p3-E）。
		case "btw.forked":
			var p BtwForkedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into BtwForkedPayload: %v", lineNo, err)
				break
			}
			// K29 fork+/btw：来源 session（fork 的上下文来源）+ 问题原文两要素齐全才可审计。
			checkULID(t, lineNo, "btw.forked source_session_id", string(p.SourceSessionID))
			if p.Question == "" {
				t.Errorf("line %d: btw.forked question 不能为空（/btw 问题原文）", lineNo)
			}
		case "workflow.activated":
			var p WorkflowActivatedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into WorkflowActivatedPayload: %v", lineNo, err)
				break
			}
			// v3-S3 工作流引擎（W1/W3）：能力包 id + 注入的哲学原文两要素齐全才可审计。
			if p.WorkflowID == "" {
				t.Errorf("line %d: workflow.activated workflow_id 不能为空", lineNo)
			}
			if p.Philosophy == "" {
				t.Errorf("line %d: workflow.activated philosophy 不能为空（W3：注入 system prompt 供主 agent 选择的哲学原文）", lineNo)
			}
		case "workflow.milestone":
			var p WorkflowMilestonePayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into WorkflowMilestonePayload: %v", lineNo, err)
				break
			}
			// v3-S3 工作流引擎（W14 MilestoneHandler）：能力包 id + 里程碑名 + 通过结论三要素齐全。
			if p.WorkflowID == "" {
				t.Errorf("line %d: workflow.milestone workflow_id 不能为空", lineNo)
			}
			if p.Milestone == "" {
				t.Errorf("line %d: workflow.milestone milestone 不能为空（里程碑名）", lineNo)
			}
		case "config.changed":
			var p ConfigChangedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ConfigChangedPayload: %v", lineNo, err)
				break
			}
			// G-c per-prompt 配置：变更集语义 = 字段出现即覆盖。样本钉住首批字段的
			// 真实形状：model + provider 两件套（provider 凭据纪律：改 base_url 必须
			// 同时给 api_key_env——契约层只保证两个字段都在，成对性由 host 层校验）。
			if p.Model == nil || *p.Model == "" {
				t.Errorf("line %d: config.changed 应带非空 model（出现 = 覆盖）", lineNo)
			}
			if p.Provider == nil {
				t.Errorf("line %d: config.changed 应带 provider 覆盖项", lineNo)
				break
			}
			if p.Provider.BaseURL == nil || *p.Provider.BaseURL == "" {
				t.Errorf("line %d: config.changed provider.base_url 应带非空值", lineNo)
			}
			if p.Provider.APIKeyEnv == nil || *p.Provider.APIKeyEnv == "" {
				t.Errorf("line %d: config.changed provider.api_key_env 应带非空值（凭据纪律：改 base_url 必须同时显式给 api_key_env）", lineNo)
			}
			// reasoning_effort 带了就必须非空（空串 = 清除回默认是合法语义，
			// 但样本演示的是覆盖形状，不该拿空串占位——与 model.turn reasoning 同判）。
			if p.ReasoningEffort != nil && *p.ReasoningEffort == "" {
				t.Errorf("line %d: config.changed reasoning_effort 带了就不该是空串（空串 = 清除，样本应演示覆盖）", lineNo)
			}
			// G-e 工具白名单：样本钉住 tools 的真实形状（only + except 两项都带，
			// 且**空数组 ≠ 缺失**——出现 = 覆盖，空数组 = 清空该项）。
			if p.Tools == nil {
				t.Errorf("line %d: config.changed 应带 tools 白名单（G-e）", lineNo)
				break
			}
			if p.Tools.Only == nil {
				t.Errorf("line %d: config.changed tools.only 应出现（出现 = 覆盖；缺失 = 该项未设置）", lineNo)
			} else if len(*p.Tools.Only) == 0 {
				t.Errorf("line %d: config.changed tools.only 不该是空数组（样本演示覆盖形状）", lineNo)
			}
			if p.Tools.Except == nil {
				t.Errorf("line %d: config.changed tools.except 应出现（出现 = 覆盖；缺失 = 该项未设置）", lineNo)
			} else if len(*p.Tools.Except) == 0 {
				t.Errorf("line %d: config.changed tools.except 不该是空数组（样本演示覆盖形状）", lineNo)
			}
			// 工具名非空（空名 = 点名"什么都没有"，host 层校验也会拒）。
			if p.Tools.Only != nil {
				for i, name := range *p.Tools.Only {
					if strings.TrimSpace(name) == "" {
						t.Errorf("line %d: config.changed tools.only[%d] 不能是空串", lineNo, i)
					}
				}
			}
			if p.Tools.Except != nil {
				for i, name := range *p.Tools.Except {
					if strings.TrimSpace(name) == "" {
						t.Errorf("line %d: config.changed tools.except[%d] 不能是空串", lineNo, i)
					}
				}
			}
		default:
			// Unknown kinds are tolerated (D7): the payload stays raw JSON.
		}

		// Round trip: re-serializing must be semantically the same document.
		out, err := json.Marshal(ev)
		if err != nil {
			t.Errorf("line %d: marshal: %v", lineNo, err)
			continue
		}
		if !jsonSemanticallyEqual(t, lineNo, raw, out) {
			t.Errorf("line %d: round trip changed the document:\n got %s\nwant %s", lineNo, out, raw)
		}
	}

	if seen == 0 {
		t.Fatalf("%s has no events", sampleEventsPath)
	}
	// Every kind with a frozen payload mapping must be covered by the sample
	// and must decode into its type (x-kind-payload in event.json is the source).
	for _, kind := range []string{
		"tool.call.intent", "tool.call.completed", "tool.call.failed",
		"context.assembled", "user.prompt", "model.turn", "model.delta",
		"gate.joined", "gate.left",
		"agentlink.broadcast", "agentlink.handoff", "context.compacted",
		"review.recorded", "conflict.predicted", "hub.alert",
		"btw.forked", "workflow.activated", "workflow.milestone",
		"config.changed",
	} {
		if kinds[kind] == 0 {
			t.Errorf("sample has no %q event", kind)
		}
	}
}

// checkAgentLinkTarget asserts that an agentlink message target is well formed:
// exactly one of gate_id / agent_id, matching the declared type (K11's two
// orthogonal dimensions: gate = horizontal, subtree = vertical).
func checkAgentLinkTarget(t *testing.T, line int, target AgentLinkTarget) {
	t.Helper()
	switch target.Type {
	case AgentLinkTargetTypeGate:
		if target.GateID == nil || *target.GateID == "" {
			t.Errorf("line %d: agentlink target type=gate 必须带 gate_id", line)
		}
		if target.AgentID != nil {
			t.Errorf("line %d: agentlink target type=gate 不应带 agent_id", line)
		}
	case AgentLinkTargetTypeSubtree:
		if target.AgentID == nil || *target.AgentID == "" {
			t.Errorf("line %d: agentlink target type=subtree 必须带 agent_id", line)
		}
		if target.GateID != nil {
			t.Errorf("line %d: agentlink target type=subtree 不应带 gate_id", line)
		}
		if target.AgentID != nil {
			checkULID(t, line, "target.agent_id", string(*target.AgentID))
		}
	default:
		t.Errorf("line %d: agentlink target type = %q 不在枚举内", line, target.Type)
	}
}

// checkULID asserts that an id field is a well-formed ULID **in the framework's
// actual shape**.
//
// 两层口径：
//   - 契约层（common.json#/$defs/Ulid）：26 字符 Crockford base32（ulidPattern）；
//   - **框架实际形态**（p3-E）：首字符落在 '0'..'7'——128bit 编进 26×5=130bit，
//     最高字符只用 3 个有效位，core.NewUlid / core.ShortHash 生成的 id 一律如此。
//     首字符 >'7' 的串是**手写**出来的假 ULID：消费方按"框架发的 id"理解它，
//     会读出错误的语义（时间戳前缀、随机位分布全都不对）。
//
// 样本里的 id 必须是框架**发出来**的形态——否则跨语言消费端拿到的样本就是"假数据"。
func checkULID(t *testing.T, line int, field, value string) {
	t.Helper()
	if !ulidPattern.MatchString(value) {
		t.Errorf("line %d: %s = %q is not a ULID", line, field, value)
		return
	}
	if value[0] < '0' || value[0] > '7' {
		t.Errorf("line %d: %s = %q 的首字符必须是 '0'..'7'（框架实际形态：128bit 编进 130bit，"+
			"最高字符只用 3 个有效位）", line, field, value)
	}
}

// jsonSemanticallyEqual compares two JSON documents by value, not by bytes:
// the key order and the number formatting may differ, the meaning may not.
func jsonSemanticallyEqual(t *testing.T, line int, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Errorf("line %d: unmarshal %s: %v", line, a, err)
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Errorf("line %d: unmarshal %s: %v", line, b, err)
		return false
	}
	return reflect.DeepEqual(va, vb)
}

// TestEventPayloadIsRawJSON pins the D2 exception: the payload field keeps
// its bare json.RawMessage form (nil means "absent").
func TestEventPayloadIsRawJSON(t *testing.T) {
	var ev Event
	ev.Kind = "model.delta"
	if ev.Payload != nil {
		t.Errorf("a fresh Event has a payload: %s", ev.Payload)
	}
	out, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "payload") {
		t.Errorf("an empty payload must be omitted: %s", out)
	}
}
