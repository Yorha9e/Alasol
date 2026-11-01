package loop

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"unicode/utf8"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/storage"
)

// 本文件是 D4（大 payload 外置 blob，G2e）在 loop 侧的挂接（v4-3-A）。
//
// # 挂接点与 X1 纪律
//
// 只动 tool.call.completed 的 **result 载荷内容** 与 envelope 的 **binary_ref**：
// intent→(completed|failed) 三段结构与落盘序**一字不动**（见 toolcall.go runToolCall）。
// tool.call.failed **不外置**（保持原样）——失败说明本就是给模型读的短文本，没有"大输出"问题。
//
// # 载荷内容变、形状不变（零契约改动）
//
// 外置时把**完整 ToolResult JSON** 写进 blob，payload 的 Result 换成**摘要形态**：
// Content 里**前 K 个** part（K = blobSummaryMaxPreviewParts）的 Text 换成「占位标记 + 前
// blobSummaryTextRunes 个字符」、Data 置空（结构化载荷整体在 blob 里），**其余 part 合并成
// 一条**「…还有 N 个 part（全文见 blob <id>）」占位（否则 part 一多，摘要自己就比原文还大——
// F2），摘要整体还有**字节上限**（blobSummaryMaxBytes）。GraphEdges / Taint / Replay **原样保留**
// （小结构字段，索引器靠 GraphEdges 建边，G22）。**ToolResult 结构一字不加字段**（不碰契约）——
// "这条被外置了"的信号就是 envelope 的 `BinaryRef != nil`：读侧据此知 payload 的 Result 是摘要，
// 按 `*ev.BinaryRef` 调 blob.Get 取回全文（host 暴露 GET /api/blobs/{id}）。
// 见 core/storage/blob.go 的挂接口径。
//
// # 摘要必须**自解释**（F3：折叠侧不看 BinaryRef）
//
// 占位标记（"[已外置：全文见 blob <id>；前 N 字如下]"）不是装饰，是**必需**：
// 折叠侧（runtime.FoldConversation，resume 续跑）**只读 payload、不看 BinaryRef**——它把
// completed 的 Result 拍成 tool 消息喂回模型。若摘要只是"截断的正文"，读者看到的是一段
// 无头无尾的残文；若 Data-only 结果只把 Data 置空，读者看到的是**空块**（"这里原本有东西
// 吗？"无从判断）。占位让摘要**自己说清**：这条被外置了、全文在哪个 blob id 里、下面这截是
// 预览。折叠侧无标记、续跑看到摘要/占位（挂账见 11 施工计划）。
//
// # 关 = 零行为变化（酸测试）
//
// blob 未挂（BlobSink==nil）或阈值 ≤0（BlobMaxInline<=0）→ externalizeResult 直接返回 nil、
// payload 一字不动 → completed 载荷与现状**逐字节等价**、BinaryRef 空。这是默认路径。
//
// # 外置 = 瘦身（F4：有效阈值 + 摘要预算，结构性保证）
//
// "外置"的意义是**事件流变小**（大载荷进 blob，事件流只留引用 + 摘要）。但摘要本身有
// 固定成本（每个预览 part 一条占位 ≈ 70 字节 + JSON 结构，4 个 part 时约 500 字节）——
// **阈值很小的时候，外置可能反而把事件流撑大**：刚过阈值的原文（1.1KB）折出来的摘要
// （4 个占位 + 每 part 256 字预览）能到 1.4KB > 原文。那就不是外置，是**搬运 + 膨胀**。
//
// 故本实现把"外置 = 瘦身"做成**结构性事实**（不靠"碰巧"）：
//
//	摘要预算 B = min(2KB, max(阈值/2, 768))   （见 blobSummaryBudget）
//	有效阈值 T = max(阈值, B)                （见 blobEffectiveThreshold）
//	外置当且仅当 原文 > T ≥ B ≥ 摘要
//
// 即：**过了有效阈值才外置**，而那时原文一定比摘要大（摘要 ≤ B ≤ T < 原文）。
//
// 代价（如实写明）：阈值配得比摘要预算还小时，(阈值, B] 区间的 result **不再外置**
// （它们不到 1KB，留在事件流里无害）；摘要在极小阈值下预览正文会被裁短（占位与合并占位
// **不可裁**——自解释是硬要求）。
//
// # 外置失败 = fail-open（不破坏 X1）
//
// blob.Put 失败时**不中止** loop：告警一条 + 原样保留完整 result 内联（不外置、BinaryRef 空）。
// 理由：completed 必须落（X1：有 intent 就要有 result），为一次 blob 写失败而丢弃 completed
// 会制造"有意图无结果"——那正是 X1 要避免的。代价是这一条的 payload 偏大（我们本想避免的，
// 但仅在 blob 故障时），可观测（告警可见）。
//
// # 崩溃窗口（不可消除，如实记口径）
//
// fail-open **只覆盖 Put 返错**这一种失败（blob 写失败 → 原样内联、BinaryRef 空）。**崩溃
// 窗口消不掉**：blob 写成功、completed 事件落盘前进程崩掉 → 磁盘上多一份**没有引用指向它**
// 的 blob（孤儿内容）。本实现的顺序是"**先写 blob 成功、再落引用**"（见 externalizeResult）
// → **引用与内容最终一致**：引用只会指向已落地的内容，崩溃只会留下孤儿 blob，**不会**留下
// 指向空处的悬空引用。
//
// 崩溃续跑口径：引用最终一致——续跑按事件里的 `binary_ref` 调 blob.Get **补取全文**；取不到
// （已回收/未落地）时报"内容已回收"，不假装事件不存在。孤儿 blob / .tmp 残留的**回收**走
// blobfs.GC（三扫：.tmp → 孤儿目录 → 可选 mtime 清扫；CLI：`alasol blob-gc`，Retention=0
// 默认只做安全两扫）。mtime 清扫的**龄期 = 最后外置时间**：幂等 Put（同 id 同内容）会刷新
// meta.json 的 mtime（见 blobfs.Put）→ 仍在被引用的 blob 不会被当成超期残留删掉。
//
// # 接线覆盖面：哪些 completed 写路径走外置
//
// core/loop 里写 tool.call.completed 的**三条**路径**全部**走 externalizeResult（同一口径）：
//
//   - runToolCall —— 工具执行结果（core/loop/toolcall.go）；
//   - Replay —— X2 重放落的结果（core/loop/replay.go）；**重放与原执行同口径**（落盘形状一致）；
//   - finishSpawn —— 生命周期工具 spawn 的结果（core/loop/agenttools.go）。
//
// 另有两条**不写大载荷**的 completed 路径，**有意不外置**：
//
//   - core/durable 的 writeVerifiedCompleted（verify 核验通过的**合成**结果）：result 是一句
//     固定短文案，体量恒定很小——阈值口径下永不触发，外置它没有收益；且 core/durable 不持有
//     blob 仓库（依赖方向：core 不 import runtime，仓库由宿主装配时注入）。
//   - tool.call.failed（失败说明）：本就是给模型读的短文本，没有"大输出"问题（见文件头）。

// blobSummaryTextRunes 是外置后摘要形态里每个 Content part 的 Text **预览**保留字符数
// （rune 计，不劈坏 UTF-8）。只影响"内联预览"，全文经 BinaryRef 取——故取一个够辨识的小值。
const blobSummaryTextRunes = 256

// blobSummaryMaxPreviewParts 是摘要里**给预览**的 part 数上限（只前 K 个 part 有预览）。
//
// 为什么要有上限（F2）：预览 = 「占位 + 前 N 字」，**每个 part 都带一份占位**——part 越多摘要
// 越大。100 个 part 各带 256 字预览 + 100 条占位 → 摘要比原文还大（线性膨胀 3 倍+）：
// "外置是为了让事件流变小"就落空了（更糟：事件流里同时躺着摘要和 blob 引用）。
// 故只给**前 K 个** part 预览，其余**合并成一条**占位（见 blobSummaryRestPlaceholder）。
//
// K=4 的取法：前几个 part 通常是"结论/头部"（读者最需要的那几段），四个够辨识；
// 再多就只是重复占位、稀释信号。
const blobSummaryMaxPreviewParts = 4

// blobSummaryMaxBytes 是摘要（ToolResult 序列化后）的**总字节上限**。
//
// 为什么 K 之外还要字节上限：K 限住了 part **数量**，没限住**单 part 预览的体积**——
// 4 个 part × 256 rune 的 CJK 文本 ≈ 3KB（再加占位约 300 字节）→ 仍可能超过"该外置"的
// 阈值（阈值通常 64KB 量级不算大，但摘要是**常驻**在事件流里的，每条都几 KB 就是浪费）。
// 故再压一道总字节上限：超了就按**字节预算**收缩预览。
//
// **占位与合并占位不可裁**（自解释是硬要求，见文件头 F3）——超限时裁的是**预览正文**。
const blobSummaryMaxBytes = 2048

// blobSummaryMinBytes 是摘要预算的**下限**（字节；见 blobSummaryBudget）。
//
// 为什么要有下限：摘要的**固定开销**（占位 + 合并占位 + JSON 结构，4 个预览 part 时约 500 字节）
// 是**不可裁**的（自解释是硬要求，见文件头 F3）——预算压到它以下就只剩一堆占位、**一点预览
// 都没有**了（摘要失去"可辨识"的价值：读者知道"有全文在 blob 里"，却看不出这是什么内容）。
// 故预算不低于 768（固定开销之上还留得出几十字节的预览正文）。
const blobSummaryMinBytes = 768

// blobSummaryBudget 算本次外置的**摘要预算**（字节）：随阈值收缩，但有下限与上限。
//
//	B = min(blobSummaryMaxBytes, max(阈值/2, blobSummaryMinBytes))
//
// 为什么随阈值收缩（F4，"外置 = 瘦身"失效）：阈值很小时（例如 1KB），一份**固定 2KB 的
// 摘要上限**根本兜不住——被外置的原文可能只有 1.1KB，而摘要（4 个占位 + 每 part 256 字
// 预览）能到 1.4KB：**"外置"反而把事件流撑大**（更糟：事件流里同时躺着摘要和 blob 引用）。
// 摘要预算按阈值的**一半**给，外置就一定划算（摘要 ≤ 阈值/2 < 被外置的原文）。
//
// 为什么有下限（blobSummaryMinBytes）：预算压到固定开销以下就只剩占位、没有预览了——
// 那份摘要虽然小，却**不可辨识**（读者看不出这是什么内容）。768 = 固定开销（约 500）+ 一点预览。
//
// 为什么有上限（blobSummaryMaxBytes）：摘要是**常驻**在事件流里的，每条几 KB 就是浪费；
// 阈值放大到 64KB 量级时，摘要仍守在 2KB 内（阈值是"该不该外置"，摘要预算是"摘要多大"，
// 两件事）。
func blobSummaryBudget(threshold int64) int {
	b := threshold / 2
	if b < blobSummaryMinBytes {
		b = blobSummaryMinBytes
	}
	if b > blobSummaryMaxBytes {
		b = blobSummaryMaxBytes
	}
	return int(b)
}

// blobEffectiveThreshold 算**有效外置阈值**（字节）：T = max(阈值, 摘要预算 B)。
//
// 为什么要把阈值抬到预算之上（F4）：外置的**收益**是"事件流变小"，**代价**是"多一次 blob
// 读 + 摘要占位"。若阈值比摘要预算还小，就会出现"刚过阈值的原文 → 摘要比原文还大"——
// 外置成了**负收益**（事件流更大）。故有效阈值不低于摘要预算：过了有效阈值才外置，
// 这时原文 > T ≥ B ≥ 摘要 → **外置一定让事件流变小**（结构性保证，不靠"碰巧"）。
//
// 代价（如实写明）：阈值配得比摘要预算还小时，(阈值, B] 区间的 result **不再外置**
// （它们本来就很小——不到 1KB——留在事件流里无害）。这是"外置 = 瘦身"的必要代价，
// 不是能力退化。
func blobEffectiveThreshold(threshold int64) int64 {
	b := int64(blobSummaryBudget(threshold))
	if threshold < b {
		return b
	}
	return threshold
}

// blobSummaryMarker 是摘要里占位标记的**固定词**（读侧/审计按它认出"这条被外置了"）。
//
// 为什么要有标记（F3）：折叠侧（resume 续跑）**不看 BinaryRef**——它只读 payload。若摘要只是
// 截断后的残文，读者无从判断"这是全文还是片段"；若 Data-only 结果只把 Data 置空，读者看到
// 的是**空块**。标记让摘要自解释（全文在哪个 blob id 里），也让测试能钉住"占位确实在"。
const blobSummaryMarker = "[已外置：全文见 blob "

// blobSummaryPlaceholder 拼出摘要的占位前缀（含 blob id + 预览字数）。
//
// 形态："[已外置：全文见 blob <id>；前 N 字如下]"。N 是**实际预览的字符数**（rune 计），
// 不是阈值——短文本不会被谎报成"前 256 字"。
func blobSummaryPlaceholder(blobID contracts.Ulid, previewRunes int) string {
	return fmt.Sprintf("%s%s；前 %d 字如下]", blobSummaryMarker, string(blobID), previewRunes)
}

// blobSummaryDataPlaceholder 是 **Data-only** 块的占位（结构化载荷整体在 blob 里）。
//
// 为什么要单列：Data 块没有正文可预览，若只把 Data 置空 → 摘要里留下一个**空块**
// （type 在、text/data 全无）——读者看不出"这里原本有大载荷"。占位说明它被外置了。
func blobSummaryDataPlaceholder(blobID contracts.Ulid) string {
	return fmt.Sprintf("%s%s；结构化载荷（data）见 blob]", blobSummaryMarker, string(blobID))
}

// blobSummaryRestPlaceholder 是「**其余 part 合并**」的那一条占位（F2）。
//
// 形态："…还有 N 个 part（全文见 blob <id>）"。N 是**被合并掉的 part 数**（不含前 K 个）。
//
// 为什么合并成一条而不是给每个 part 各留一条占位：占位本身有固定开销（点名 blob id +
// 说明文字 ≈ 70 字节），part 一多，光占位就把摘要撑成原文的量级——外置的意义（事件流变小）
// 就没了。合并成一条，读者仍然知道"后面还有 N 块，全文在 blob <id> 里"。
func blobSummaryRestPlaceholder(blobID contracts.Ulid, n int) string {
	return fmt.Sprintf("…还有 %d 个 part（全文见 blob %s）", n, string(blobID))
}

// blobLog 是 D4 外置失败（fail-open）时的告警出口。用 slog（结构化、可被宿主接管）。
var blobLog = slog.Default()

// blobIDDomain 是 blob id 派生的**域前缀**（与 host/vectors.go 的 "vec\x00" 隔开，
// 防跨用途撞键——同是短哈希 id，域不同才不会互相覆盖）。
const blobIDDomain = "blob\x00"

// contentBlobID 由**完整 ToolResult JSON 字节**派生内容寻址 blob id（重放幂等）。
//
// # 公式（与 host/vectors.go 的 vectorEntryID 同款纪律；本文 = 实现口径）
//
//	id = Crockford26( SHA-256( "blob\x00" ‖ raw )[:16] )
//
// 其中：
//   - "blob\x00" 是**域前缀**（与 "vec\x00" 等其它短哈希隔开，防跨用途撞键）；
//   - raw 是单输入（完整 ToolResult JSON 字节）→ **不需要长度前缀**（域前缀 + 内容
//     拼接无歧义，不像 vectorEntryID 的双输入需要 u64be(len) 分段）；
//   - 取 SHA-256 的**前 128bit** 编 Crockford base32（core.EncodeCrockford26，框架
//     26 编码的单一实现处）→ 恒为合法 ULID 形态（blobfs.Put 的 ValidUlid 校验过）。
//
// # 口径（内容寻址：同内容同 id）
//
//   - 同一内容重放（或不同工具产出同内容）→ 同一 id → blobfs.Put 走**既有幂等路径**
//     （内容一致原样返回，不写第二份）——重放不再多造 blob；
//   - 不同内容 → 不同 id（SHA-256 抗碰撞）；
//   - 哈希碰撞（不同内容同 id，概率 2^-128）→ Put 报 ErrConflict → 外置 **fail-open**
//     原样内联（见 externalizeResult 的既有失败路径），不覆盖已落库内容。
//
// meta.Name（tool_result_<callid>.json）保留作诊断；共享 blob 的 Name 记**首个写入者**
// （幂等二次 Put 原样返回已有 meta，不改 Name）。
func contentBlobID(raw []byte) contracts.Ulid {
	h := sha256.New()
	h.Write([]byte(blobIDDomain))
	h.Write(raw)
	sum := h.Sum(nil)
	return contracts.Ulid(core.EncodeCrockford26(sum[:16]))
}

// planExternalize（v4-4-B）：在 **tool.call.intent 发布之前**记一条外置计划（blob.plan）。
//
// 为什么单独一个写点、且在 intent 之前：写点插在 intent→completed 之间 = 在 X1 关键路径
// 上多一次落盘，还让"所有写点都在三段路径外"这句话不成立（critic F1）。计划先行、intent
// 随后 → 窗口内一个写点都不加（X1 落盘序一字不动）。
//
// 关（blob 未启用 / 阈值<=0）→ **不记**：没开外置就没有"计划"可写（零行为变化）。
//
// turn=0 = 重放路径（重放没有"第几轮"的概念，如实记 0）。
func (l *Loop) planExternalize(turn int, toolCallID contracts.ProviderId) {
	if l.blob == nil || l.blobMaxInline <= 0 {
		return // 关：零行为变化
	}
	l.walBlobPlan(turn, string(toolCallID), blobEffectiveThreshold(l.blobMaxInline))
}

// externalizeResult（D4）：result 超阈值时整体外置 blob，返回 envelope 的 BinaryRef
// （nil = 不外置）。可能**就地**把 payload.Result 换成摘要形态。
//
// 关（blob==nil 或阈值≤0）或 result 不超过**有效阈值** → 原样返回 nil、payload 一字不动
// （酸测试）。
// blob 写失败 → fail-open：告警 + 原样保留（返回 nil），保 X1（completed 必须落）。
//
// **有效阈值**（F4）= max(配置阈值, 摘要预算)：保证"外置 = 瘦身"是结构性事实（见
// blobEffectiveThreshold）；摘要预算随阈值收缩（见 blobSummaryBudget）。
//
// **WAL 计划不在这里写**（v4-4-B）：计划写在 intent **之前**（见 planExternalize），
// 本函数只管"写内容 + 换引用"——窗口内一个写点都不加。
func (l *Loop) externalizeResult(ctx context.Context, payload *contracts.ToolCallCompletedPayload) *contracts.Ulid {
	if l.blob == nil || l.blobMaxInline <= 0 {
		return nil // 关：零行为变化
	}
	// 序列化完整 result，量它**原本要进 payload 的字节数**（阈值判在这上面）。
	raw, err := json.Marshal(payload.Result)
	if err != nil {
		// 不可达（ToolResult 可序列化）；真失败 → 不外置，原样落（fail-open）。
		blobLog.Warn("loop: D4 外置跳过——result 序列化失败（原样内联）", "err", err.Error())
		return nil
	}
	threshold := blobEffectiveThreshold(l.blobMaxInline)
	if int64(len(raw)) <= threshold {
		return nil // 不大：原样内联（酸测试等价）
	}
	// 内容寻址 id（见 contentBlobID）：同内容同 id（重放幂等、天然去重）——重放路径
	// （replay.go）同口径外置时走 blobfs.Put 的既有幂等路径，不写第二份；碰撞概率
	// 2^-128，撞了 Put 报 ErrConflict → 走下方 fail-open（原样内联，不覆盖已落库内容）。
	// meta.Name 保留诊断用（tool_result_<callid>.json）；共享 blob 的 Name 记首个写入者。
	meta, err := l.blob.Put(ctx, storage.BlobMeta{
		ID:        contentBlobID(raw),
		MediaType: "application/json",
		Name:      "tool_result_" + string(payload.ToolCallID) + ".json", // 诊断用
	}, bytes.NewReader(raw))
	if err != nil {
		blobLog.Warn("loop: D4 外置失败——原样内联完整 result（completed 仍落，X1 不破）",
			"tool_call_id", string(payload.ToolCallID), "result_bytes", len(raw), "err", err.Error())
		return nil
	}
	// 落库成功才换摘要 + 出引用（顺序：先写 blob 成功，再让引用指向它——反了会留下指向空处的引用）。
	// 摘要里点名这个 blob id（占位自解释，折叠侧不看 BinaryRef 也能知道去哪取全文）。
	payload.Result = summarizeToolResult(payload.Result, meta.ID, blobSummaryBudget(l.blobMaxInline))
	return meta.Ref()
}

// summarizeToolResult 把完整 ToolResult 折成**摘要形态**（形状不变、只瘦身；全文在 blob）。
//
//   - Content：**前 K 个** part（K = blobSummaryMaxPreviewParts）保留原 Type，Text 换成
//     「占位标记 + 前 blobSummaryTextRunes 个字符」（rune 计）；Data 置空（结构化载荷可能很大，
//     整体在 blob 里，读侧按 BinaryRef 取）。
//   - **其余 part 合并成一条**占位（blobSummaryRestPlaceholder）——part 一多，摘要自己就比
//     原文还大（F2：外置的意义是事件流变小，摘要不能反过来把它撑大）。
//   - Data-only 块（无 Text 只有 Data）：**不留空块**——Text 置为数据占位说明（F3）。
//   - 摘要整体有**字节预算**（budget 字节，随阈值收缩；见 blobSummaryBudget）：超了按字节
//     预算收缩**预览正文**（占位与合并占位不可裁——自解释是硬要求）。
//   - GraphEdges / Taint / Replay：**原样保留**（小结构字段；索引器靠 GraphEdges 建边，G22）。
//
// blobID 是这次外置的 blob id（占位里点名它——读侧据此知道去哪取全文）。
//
// 不修改入参（拷出新切片），调用方持有的完整 result 不受影响（它还要回填给模型）。
func summarizeToolResult(res contracts.ToolResult, blobID contracts.Ulid, budget int) contracts.ToolResult {
	if len(res.Content) == 0 {
		return res // 没有 part：形状原样（Content 保持 nil）
	}
	keep := len(res.Content)
	if keep > blobSummaryMaxPreviewParts {
		keep = blobSummaryMaxPreviewParts
	}
	rest := len(res.Content) - keep // 被合并成一条占位的 part 数

	// build 按给定的**每 part 预览字节预算**组装一份摘要（previewBytes < 0 = 不按字节裁）。
	build := func(previewBytes int) contracts.ToolResult {
		out := res
		out.Content = make([]contracts.ContentPart, 0, keep+1)
		for i := 0; i < keep; i++ {
			out.Content = append(out.Content, summarizeContentPart(res.Content[i], blobID, previewBytes))
		}
		if rest > 0 {
			t := blobSummaryRestPlaceholder(blobID, rest)
			out.Content = append(out.Content, contracts.ContentPart{Type: "text", Text: &t})
		}
		return out
	}

	// 常态：按 rune 上限（256 字）预览即已低于本次预算 → 直接用（预览保持满额）。
	sum := build(-1)
	if summaryBytes(sum) <= budget {
		return sum
	}
	// 超预算：按**字节预算**收缩预览正文。
	// 固定开销（占位 + 合并占位 + JSON 结构）先量出来，剩下的字节平分给各 part 的预览。
	overhead := summaryBytes(build(0))
	avail := budget - overhead
	if avail < 0 {
		avail = 0
	}
	per := avail / max(keep, 1)
	for {
		sum = build(per)
		if summaryBytes(sum) <= budget || per <= 0 {
			// 到极限仍超（固定开销本身就超上限）→ 返回最小摘要（占位不可裁，自解释优先）。
			return sum
		}
		per = per * 3 / 4 // 逐步收缩（3/4：一次到位，又不至于过度裁剪）
	}
}

// summarizeContentPart 把一个 part 折成摘要形态。
//
// previewBytes < 0 = 只按 rune 上限裁；>= 0 = 再按**字节预算**裁（总字节上限那一道）。
func summarizeContentPart(p contracts.ContentPart, blobID contracts.Ulid, previewBytes int) contracts.ContentPart {
	np := p // 拷 Type（Text/Data 下面各自处理）
	switch {
	case p.Text != nil:
		preview := truncateRunes(*p.Text, blobSummaryTextRunes)
		if previewBytes >= 0 {
			preview = truncateRunesBytes(preview, previewBytes)
		}
		t := blobSummaryPlaceholder(blobID, len([]rune(preview))) + preview
		np.Text = &t
	case len(p.Data) > 0:
		// Data-only 块：占位说明（F3：不留空块）。
		t := blobSummaryDataPlaceholder(blobID)
		np.Text = &t
	}
	np.Data = nil // 结构化载荷整体外置
	return np
}

// summaryBytes 量一份摘要的**序列化字节数**（总字节上限的度量口径 = 落进事件流的字节）。
//
// 序列化失败（不可达：ToolResult 可序列化）→ 按 0 算（度量失败不该把外置搞崩）。
func summaryBytes(res contracts.ToolResult) int {
	raw, err := json.Marshal(res)
	if err != nil {
		return 0
	}
	return len(raw)
}

// truncateRunes 把字符串截断到前 n 个字符（rune 计，不劈坏 UTF-8；n<=0 → 空串）。
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// truncateRunesBytes 把字符串裁到**字节**预算内（rune 边界，不劈坏 UTF-8）。
//
// 为什么按字节而不是 rune：总字节上限是按字节算的（事件流的大小单位就是字节），而 rune 数
// 与字节数不是一回事（CJK 一字 3 字节）——只按 rune 裁，字节上限照样会被撑破。
func truncateRunesBytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	budget := maxBytes
	for i, r := range s {
		size := utf8.RuneLen(r)
		if size > budget {
			return s[:i] // i 是 rune 边界（range 的下标）→ 不劈坏 UTF-8
		}
		budget -= size
	}
	return s
}
