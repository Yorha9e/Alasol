package walfile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件是 WAL 的**恢复面对账**（v4-4-B）：把 WAL 记录与事件流的事实放在一起，
// 产出"上次崩溃时在干什么"的恢复报告。
//
// # 报告是观察面，不是写回面（F5 不受影响）
//
// Reconcile **只读**两侧（WAL 文件 + 调用方给定的事件流事实），不写事件流、不补 delta、
// 不改任何状态。WAL 里的 delta 文本进报告供**展示**（人能看到"当时吐到了哪儿"），
// 但绝不自动回填成 durable 事件——那会让 WAL 变成第二事实源（F5 与陷阱 11 一起破）。
//
// # 对账口径（三个问题）
//
//  1. **在飞轮次**：按**记录流分段**归组——**每条 turn.begin 开一个新段**（= 一个 Run 的
//     一轮），段内的 turn.delta / turn.end / turn.failed 都归到这一段；**段没有收口**
//     （有 begin、无 end、无 failed）→ 这段**在飞** = 崩溃点（第 N 轮在跑什么模型、吐过
//     几段 delta、吐了什么）。turn.end 在 model.turn **落盘之后**才写（core/loop/wal.go），
//     所以"无 end"忠实地意味着"这轮没走完"。
//     **turn.failed 是第三种收口**：模型调用失败（429/超时/端点报错）是**普通失败**，
//     不是崩溃——loop 在 provider.Run 返错时写它。有 failed → 不算在飞（否则一次模型
//     报错就被误报成"疑似崩溃残留"：假阳性比漏报更坏，使用者据此以为丢了东西）。
//
//     # 为什么必须**分段**而不是按轮次号归集（F1）
//
//     WAL 载荷里的 turn 号是"**每次 Run 内从 1 起**"（core/loop/loop.go 的轮次循环）→
//     **跨 Run 撞号**：同一个 session 跑第二次 Run 时，轮次号又从 1 开始。若按号全局归集，
//     第二次 Run 的 turn.end 会把第一次 Run 留下的**悬空 turn.begin** 判成"已收口"→
//     报告谎报"无崩溃残留"（**假阴性**：崩溃现场被抹掉——比误报更坏，使用者据此以为没丢
//     东西）。故归组单位是**段**（一次 Run 的一轮），不是轮次号。
//     （另一条修法是给载荷加 run id——那要动 core 的写点，面大；本刀走纯对账侧的分段。）
//  2. **孤儿 blob**：blob.plan 在 WAL 里，但事件流里那个 tool_call **既没有 completed
//     也没有 failed** → "这个工具调用没走完"（结果可能已写成 blob、引用没落 → v4-3-A 的
//     崩溃窗口）→ 报告列出候选（可选手动清理）。
//  3. **作废计划**：blob.plan 在 WAL 里，事件流里那个 tool_call **有 completed 但没有
//     binary_ref** → 两种情况混在一起辨不细（ Put 失败 fail-open 原样内联 vs 阈值变了），
//     如实单列一类，不谎报成孤儿。
//
// **诚实边界（计划写在 intent 之前的代价）**：blob.plan 挪到 intent **之前**后（critic F1：
// 写点全在三段窗口外），计划里**没有结果体量**（结果那时还不存在）→ 对账分不出"这个
// 工具调用本来就不大"与"本来要外置但 Put 失败了"。故：
//
//   - "孤儿候选"= 这次工具调用**没走完**（既无 completed 也无 failed）——结果**可能**写成
//     了 blob、引用没落（**候选**，不是断言）；
//   - "作废计划"= 走完了但**没外置**——绝大多数是"结果不大"（正常），少数是 Put 失败
//     fail-open。名单因此偏长（如实写明，不假装它是"故障清单"）。
//
// # 不误报（有对应 completed 就不该报）
//
// 对账只用事件流事实说话：completed 落了（带 binary_ref）→ 那个 plan 两个名单都不进。
// 这是本文件最容易被写坏的地方（把"计划过"当成"孤儿"），测试里有钉子正反各一枚。
//
// **failed 也算"收口了"**：deny/参数坏/执行失败都会落 tool.call.failed——那种工具调用
// 根本没写 blob（结果都没产出），把它算成"孤儿候选"是**误报**（还会让 Crashed() 假阳性：
// 一次正常的策略拒绝被报成"疑似崩溃"）。

// RecoveredTextMaxBytes 是报告里带回的 in-flight 轮次文本上限（字节）。
//
// 为什么要有上限：报告是给人看的一屏JSON，而一轮 delta 拼起来可能是几十 KB（长输出）。
// 超限就截断 + 置 Truncated 标记（诚实说明"还有，但没全带回来"），不静默丢。
const RecoveredTextMaxBytes = 64 * 1024

// Facts 是**事件流一侧**的对账事实（由调用方从 core.Store 折出来；本包不读事件库——
// 依赖方向：walfile 不认识 sqlite，host/cmd 各自读自己的 store）。
type Facts struct {
	// CompletedToolCalls 是事件流里 tool.call.completed 的 tool_call_id 集合
	//（provider 黑盒 id，S4；WAL 的 blob.plan 带同一个 id → 对账键）。
	CompletedToolCalls map[string]bool
	// CompletedBinaryRefs 是事件流里**带 binary_ref** 的 completed 的 tool_call_id 集合。
	// （completed 落了但没外置 = 计划作废，不是孤儿——见文件头的第三类。）
	CompletedBinaryRefs map[string]bool
	// FailedToolCalls 是事件流里 tool.call.failed 的 tool_call_id 集合。
	//
	// 为什么要有它：deny/参数坏/执行失败是**正常收口**（X1 三段之③b），那种工具调用
	// 根本没产出结果、更没写 blob——把它算成"孤儿候选"是误报（还会让 Crashed() 假阳性：
	// 一次策略拒绝被报成"疑似崩溃残留"）。
	FailedToolCalls map[string]bool
}

// FactsFromEvents 从一批事件折出对账事实（host/cmd 共用，口径只定义一次）。
//
// 只认 tool.call.completed（另两条 completed 写路径也在事件流里，形状同一）与
// tool.call.failed；payload 解不出来的条目**跳过**（损坏事件不该让整份报告失败，但也绝不猜）。
func FactsFromEvents(events []contracts.Event) Facts {
	f := Facts{
		CompletedToolCalls:  map[string]bool{},
		CompletedBinaryRefs: map[string]bool{},
		FailedToolCalls:     map[string]bool{},
	}
	for _, ev := range events {
		switch ev.Kind {
		case "tool.call.completed":
			var p contracts.ToolCallCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				continue
			}
			id := strings.TrimSpace(string(p.ToolCallID))
			if id == "" {
				continue
			}
			f.CompletedToolCalls[id] = true
			if ev.BinaryRef != nil {
				f.CompletedBinaryRefs[id] = true
			}
		case "tool.call.failed":
			var p contracts.ToolCallFailedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				continue
			}
			id := strings.TrimSpace(string(p.ToolCallID))
			if id == "" {
				continue
			}
			f.FailedToolCalls[id] = true
		}
	}
	return f
}

// TurnReport 是一个轮次在 WAL 里的样子。
//
// # 一项 = 一**段**（F1：按记录流分段，不按轮次号）
//
// 报告里的每一项对应 WAL 记录流里的**一段** = 一次 Run 的一轮（从那条 turn.begin 到它的
// 收口）。因为轮次号是"**每次 Run 内从 1 起**"→ **跨 Run 会重复**，同一个号可能在报告里
// 出现多次（那是不同的 Run，各有各的一轮）——**别把它当全局唯一的轮次号**。
//
// 项的顺序 = **记录流序**（不是按轮次号排序：排序会把"哪一次 Run 的哪一轮"打乱）。
type TurnReport struct {
	// Turn 轮次号（**每次 Run 内从 1 起** → 跨 Run 会重复，见上面的说明）。
	Turn int `json:"turn"`
	// Model 模型名（turn.begin 带的）。
	Model string `json:"model"`
	// Messages / Tools 是该轮发起时的现场（诊断用）。
	Messages int `json:"messages"`
	Tools    int `json:"tools"`
	// Deltas 是该轮记下的 delta 条数（text + reasoning 合计）。
	Deltas int `json:"deltas"`
	// Ended 是否有对应的 turn.end（true = 这轮正常走完）。
	Ended bool `json:"ended"`
	// Failed 是否有对应的 turn.failed（true = 这轮**失败收口**：模型调用失败，不是崩溃）。
	Failed bool `json:"failed,omitempty"`
}

// InFlightTurn 是**崩溃点**：最后一个有 begin 无 end 的轮次。
type InFlightTurn struct {
	Turn int `json:"turn"`
	// Model 当时在跑哪个模型。
	Model string `json:"model"`
	// Deltas 该轮吐过的 delta 条数（"吐过 M 段 delta"）。
	Deltas int `json:"deltas"`
	// TextChars / ReasoningChars 是拼回来的字符数（rune 计）。
	TextChars      int `json:"text_chars"`
	ReasoningChars int `json:"reasoning_chars"`
	// Text / Reasoning 是 WAL 里的 delta 拼回来的内容（**供展示**，截断到
	// RecoveredTextMaxBytes；绝不自动补进事件流——观察面不是写回面）。
	Text      string `json:"text,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
	// TextTruncated / ReasoningTruncated 说明"还有，但没全带回来"（诚实，不静默丢）。
	TextTruncated      bool `json:"text_truncated,omitempty"`
	ReasoningTruncated bool `json:"reasoning_truncated,omitempty"`
}

// Report 是恢复报告（`alasol wal <session>` 打印的就是它；一行 JSON）。
type Report struct {
	SessionID string `json:"session_id"`
	// WALPath 是被对账的 WAL 文件路径。
	WALPath string `json:"wal_path"`
	// WALExists 是 WAL 文件是否存在（false = 没配 WAL 或这个 session 没写过）。
	WALExists bool `json:"wal_exists"`
	// Records / CorruptLines 是扫描的诚实计数（半截/坏行数要看得见）。
	Records      int `json:"records"`
	CorruptLines int `json:"corrupt_lines"`

	// Turns 是所有轮次（**按记录流分段**：一项 = 一次 Run 的一轮；含已结束的）。
	Turns []TurnReport `json:"turns,omitempty"`
	// DeltaTotal 是全文件的 delta 总条数。
	DeltaTotal int `json:"delta_total"`

	// InFlightTurn 非 nil = 发现崩溃点（有 begin 无 end 的轮次）。
	InFlightTurn *InFlightTurn `json:"in_flight_turn,omitempty"`
	// InFlightTurns 是**未收口段数**（有 begin、无 end、无 failed 的段数；0 = 没有）。
	//
	// 为什么要有计数（诚实）：轮次号跨 Run 撞号 → 一个 session 的 WAL 里可能有**多段**
	// 未收口（每崩一次留一段：Run1 崩一段、Run2 又崩一段）。InFlightTurn 只带**最后一段**
	// （最近的一次崩溃点），计数把"其实崩了 N 次"如实说出来——只报一段 = **藏证据**
	// （与 F1 修掉的"把悬空 begin 判成已收口"同一类毛病，只是方向相反）。
	InFlightTurns int `json:"in_flight_turns,omitempty"`

	// OrphanBlobToolCalls 是**孤儿 blob** 候选：plan 在 WAL、事件流里那个 tool_call
	// **既没有 completed 也没有 failed**（这次工具调用没走完）→ 结果可能已写成 blob、
	// 引用没落（v4-3-A 的崩溃窗口）→ 可手动清理。
	OrphanBlobToolCalls []string `json:"orphan_blob_tool_calls,omitempty"`
	// StaleBlobPlans 是**作废计划**：plan 在 WAL、completed 落了但没 binary_ref
	//（Put 失败 fail-open 或阈值变化；与孤儿辨不细，如实单列）。
	StaleBlobPlans []string `json:"stale_blob_plans,omitempty"`
}

// Crashed 报告是否显示"疑似崩溃残留"（有在飞轮次或孤儿）。
//
// 供调用方决定要不要把报告顶到前面（如 resume 时打一行摘要）。
func (r Report) Crashed() bool {
	return r.InFlightTurn != nil || len(r.OrphanBlobToolCalls) > 0
}

// Summary 是一行人话摘要（resume 时自动打印用；无异常 → 空串 = 不打印）。
func (r Report) Summary() string {
	if !r.WALExists {
		return "" // 没有 WAL：没什么可说的（未配置 = 零行为变化）
	}
	if !r.Crashed() {
		out := fmt.Sprintf("[wal] session %s：WAL %d 条记录、%d 段 delta，无在飞轮次/孤儿（已正常收口或 WAL 已过期）",
			r.SessionID, r.Records, r.DeltaTotal)
		// 作废计划不是崩溃残留（fail-open 的正常结局），但值得带一句——否则"干净"的措辞会把"计划过却没外置"这件事一起藏掉。
		if len(r.StaleBlobPlans) > 0 {
			out += fmt.Sprintf("；另有 %d 个作废外置计划（completed 落了但没 binary_ref）", len(r.StaleBlobPlans))
		}
		return out
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[wal] session %s：疑似崩溃残留——", r.SessionID)
	if t := r.InFlightTurn; t != nil {
		fmt.Fprintf(&b, "第 %d 轮在跑 %s（吐过 %d 段 delta、%d 字正文）未收口；", t.Turn, t.Model, t.Deltas, t.TextChars)
		// 多段未收口 = 崩过不止一次（如实说，别只报一段把其余藏掉）。
		if r.InFlightTurns > 1 {
			fmt.Fprintf(&b, "另有 %d 段更早的未收口轮次；", r.InFlightTurns-1)
		}
	}
	if len(r.OrphanBlobToolCalls) > 0 {
		fmt.Fprintf(&b, "%d 个孤儿 blob 候选（tool_call: %s）；",
			len(r.OrphanBlobToolCalls), strings.Join(r.OrphanBlobToolCalls, ", "))
	}
	if len(r.StaleBlobPlans) > 0 {
		fmt.Fprintf(&b, "%d 个作废外置计划；", len(r.StaleBlobPlans))
	}
	b.WriteString("（恢复报告是观察面不是写回面：delta 不自动补进事件流，孤儿可手动清理）")
	return b.String()
}

// ReconcileSession 扫 dir 下某 session 的 WAL 并与 facts 对账（ReadSession + Reconcile）。
func ReconcileSession(dir string, sessionID contracts.Ulid, facts Facts) (Report, error) {
	scan, err := ReadSession(dir, sessionID)
	if err != nil {
		// 读坏了也尽量给部分报告（崩溃恢复的第一步不该被一行坏数据堵死）：
		// ScanResult 已带解析成功的部分 + CorruptLines。
		rep := Reconcile(scan, sessionID, facts)
		return rep, err
	}
	return Reconcile(scan, sessionID, facts), nil
}

// walSegment 是 WAL 记录流里的**一段** = 一个 Run 的一轮（turn.begin … 它的收口）。
//
// # 为什么按段归组（F1）
//
// WAL 载荷里的轮次号是"**每次 Run 内从 1 起**"（core/loop/loop.go 的轮次循环）→ **跨 Run
// 撞号**：同一个 session 的第二次 Run 又从 1 开始。按号全局归集 = 第二次 Run 的 turn.end
// 把第一次 Run 留下的**悬空 turn.begin** 判成"已收口"→ 对账**假阴性**（谎报"无崩溃残留"）。
//
// 故归组单位是**段**：每条 turn.begin 开一个新段，段内的 delta/end/failed 都归这一段；
// 段没有收口（有 begin、无 end、无 failed）→ 这段**在飞**。
type walSegment struct {
	// turn 是段里的轮次号（**每次 Run 内从 1 起** → 跨 Run 会重复；段才是归组单位）。
	turn int
	// model / messages / tools 是 turn.begin 带的现场（诊断用）。
	model    string
	messages int
	tools    int
	// deltas 是段内记下的 delta 条数；text / reason 是拼回来的内容（**展示**用）。
	deltas int
	text   strings.Builder
	reason strings.Builder
	// began 是"有 turn.begin"（没有 begin 的段 = WAL 头部被截断出来的残段 → 不算在飞）。
	began bool
	// ended / failed 是两种收口（正常结束 / 普通失败——模型调用失败，不是崩溃）。
	ended  bool
	failed bool
}

// segCollector 是对账侧的**分段归集器**（F1：按记录流分段，不按轮次号）。
type segCollector struct {
	// list 是各段（记录流序）。
	list []*walSegment
	// cur 是"当前段"（最近一条 turn.begin 开的段）；nil = 还没见过 begin。
	cur *walSegment
}

// begin 开一个新段（一条 turn.begin = 一个 Run 的一轮）。
func (c *segCollector) begin(turn int) *walSegment {
	seg := &walSegment{turn: turn, began: true}
	c.list = append(c.list, seg)
	c.cur = seg
	return seg
}

// open 取当前段；**没有当前段**时（WAL 头部被截断 / 记录乱序）开一个**残段**
// （began=false）：数据不丢，但**不算在飞**（没有 begin 就没有"这一轮开跑了"的事实）。
func (c *segCollector) open(turn int) *walSegment {
	if c.cur == nil {
		seg := &walSegment{turn: turn}
		c.list = append(c.list, seg)
		c.cur = seg
	}
	return c.cur
}

// Reconcile 把扫描结果与事件流事实对账成恢复报告（**纯函数**：不碰文件、不碰库）。
func Reconcile(scan ScanResult, sessionID contracts.Ulid, facts Facts) Report {
	rep := Report{
		SessionID:    string(sessionID),
		WALPath:      scan.Path,
		WALExists:    scan.Exists,
		Records:      len(scan.Records),
		CorruptLines: scan.CorruptLines,
	}

	// 按**记录流分段**归集（F1）：每条 turn.begin 开一个新段；段内的 delta/end/failed 归
	// 这一段。**不按轮次号归集**——轮次号每次 Run 内从 1 起 → 跨 Run 撞号 → 全局归集会把
	// 上一次 Run 的悬空 begin 判成"已收口"（假阴性：谎报"无崩溃残留"）。
	var segs segCollector
	// blob.plan 的 tool_call_id 出现序（去重；一个 tool_call 只计划一次）。
	planOrder := []string{}
	planned := map[string]bool{}

	for _, rec := range scan.Records {
		// turn.retry（缺口 5 第二件）**不进对账**：它是 provider 内部重试的纯观测
		//（不参与在飞判定——收口仍只认 end/failed 两态）。switch 无 default → 未知 kind
		// 天然跳过，这里写明免得后人以为是漏了。
		switch rec.Kind {
		case core.KindWalTurnBegin:
			var p core.WalTurnBeginPayload
			if !decodePayload(rec, &p) {
				continue
			}
			// **新段**：一条 turn.begin = 一个 Run 的一轮。
			seg := segs.begin(p.Turn)
			seg.model, seg.messages, seg.tools = p.Model, p.Messages, p.Tools
		case core.KindWalTurnEnd:
			var p core.WalTurnEndPayload
			if !decodePayload(rec, &p) {
				continue
			}
			segs.open(p.Turn).ended = true
		case core.KindWalTurnFailed:
			var p core.WalTurnFailedPayload
			if !decodePayload(rec, &p) {
				continue
			}
			// 失败收口：**不是崩溃**（模型调用失败）→ 这一段不算在飞。
			segs.open(p.Turn).failed = true
		case core.KindWalTurnDelta:
			var p core.WalTurnDeltaPayload
			if !decodePayload(rec, &p) {
				continue
			}
			seg := segs.open(p.Turn)
			seg.deltas++
			rep.DeltaTotal++
			if p.Text != nil {
				seg.text.WriteString(*p.Text)
			}
			if p.Reasoning != nil {
				seg.reason.WriteString(*p.Reasoning)
			}
		case core.KindWalBlobPlan:
			var p core.WalBlobPlanPayload
			if !decodePayload(rec, &p) {
				continue
			}
			id := strings.TrimSpace(p.ToolCallID)
			if id == "" || planned[id] {
				continue
			}
			planned[id] = true
			planOrder = append(planOrder, id)
		}
	}

	// 段 → 报告条目（**记录流序**：轮次号跨 Run 会重复，按号排序会打乱"哪次 Run 的哪轮"）。
	var inFlight *walSegment
	for _, seg := range segs.list {
		rep.Turns = append(rep.Turns, TurnReport{
			Turn: seg.turn, Model: seg.model, Messages: seg.messages, Tools: seg.tools,
			Deltas: seg.deltas, Ended: seg.ended, Failed: seg.failed,
		})
		// 在飞段 = "began && !ended && !failed"。**记最后一段**（最近的一次崩溃点）：
		// 更早的未收口段是更早的崩溃现场，数量由 InFlightTurns 如实报出（不藏证据）。
		//
		// **failed 也算收口**：模型调用失败（429/超时/端点报错）是**普通失败**，不是崩溃——
		// 把它算成在飞 = 一次模型报错被报成"疑似崩溃残留"（假阳性：使用者据此以为丢了东西，
		// 其实只是模型报错）。只有"begin 无 end 无 failed"才是真的崩在半路。
		if seg.began && !seg.ended && !seg.failed {
			inFlight = seg
			rep.InFlightTurns++
		}
	}
	if inFlight != nil {
		rep.InFlightTurn = &InFlightTurn{
			Turn: inFlight.turn, Model: inFlight.model, Deltas: inFlight.deltas,
			TextChars:      runeLen(inFlight.text.String()),
			ReasoningChars: runeLen(inFlight.reason.String()),
		}
		rep.InFlightTurn.Text, rep.InFlightTurn.TextTruncated = capText(inFlight.text.String())
		rep.InFlightTurn.Reasoning, rep.InFlightTurn.ReasoningTruncated = capText(inFlight.reason.String())
	}

	// blob 对账：plan 在 WAL，事件流说了算（不误报）。
	for _, id := range planOrder {
		switch {
		case !facts.CompletedToolCalls[id] && !facts.FailedToolCalls[id]:
			// 既没 completed 也没 failed = 这个工具调用**没走完**（崩在半路）：结果可能
			// 已写成 blob、引用没落 → 孤儿候选（人工核对）。
			//
			// **诚实边界**：计划写在 intent 之前时**结果体量未知**，故"写了 blob 内容"只是
			// 可能、不是断言——报告里它是**候选**（可手动清理），不是断言。
			rep.OrphanBlobToolCalls = append(rep.OrphanBlobToolCalls, id)
		case facts.CompletedToolCalls[id] && !facts.CompletedBinaryRefs[id]:
			// completed 落了但没 binary_ref = 计划作废（Put 失败 fail-open 或阈值变了）。
			rep.StaleBlobPlans = append(rep.StaleBlobPlans, id)
		}
	}
	return rep
}

// decodePayload 解一条记录的 payload（解不动 → false：按损坏行跳过，不猜）。
func decodePayload(rec core.WalRecord, out any) bool {
	if len(rec.Payload) == 0 {
		return false
	}
	return json.Unmarshal(rec.Payload, out) == nil
}

// runeLen 数字符数（rune 计：与"字"的直觉一致，CJK 一字算一）。
func runeLen(s string) int { return len([]rune(s)) }

// capText 把文本裁到 RecoveredTextMaxBytes（rune 边界，不劈坏 UTF-8）。
// 返回 (裁剪后的文本, 是否裁过)。
func capText(s string) (string, bool) {
	if len(s) <= RecoveredTextMaxBytes {
		return s, false
	}
	budget := RecoveredTextMaxBytes
	for i, r := range s {
		size := runeSize(r)
		if size > budget {
			return s[:i], true // i 是 rune 边界（range 的下标）
		}
		budget -= size
	}
	return s, false
}

// runeSize 取一个 rune 的 UTF-8 字节数（非法 rune 按 1 字节算，保守）。
func runeSize(r rune) int {
	if r < 0x80 {
		return 1
	}
	if r < 0x800 {
		return 2
	}
	if r < 0x10000 {
		return 3
	}
	return 4
}
