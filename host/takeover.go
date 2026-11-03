package host

import (
	"context"
	"fmt"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// 本文件是缺口 G-b「默认接管（懒接管）」的落点：POST /api/sessions/{id}/prompt 与
// WS command.submit 对**未托管**的 session 不再回 404，而是就地懒接管——调 loopFor
// （Claim + 建 loop + 事件流恢复主 agent id），接管成功落一条 hub.alert 留痕。
//
// # G-b 语义与"唯一拒绝线"
//
//	POST /prompt → hostsSession?
//	    ├─ 是 → 照常跑（**不留痕**：本进程本来就托管它，没有"接管"这回事）
//	    └─ 否 → loopFor 懒接管：
//	        ├─ Claim 成功（原宿主失管：重启/崩溃丢的）→ 跑 + hub.alert 留痕（本文件记）
//	        └─ Claim 冲突（另一活宿主持有）→ 拒（**唯一拒绝线**，G25 防双写）
//
// 策略上的拒绝**只有 Claim 冲突这一条线**（HTTP 409 host_conflict / WS 同码错误帧）；
// 其余成功路径一律放行。历史进模型不用新做（**G-b 起已落实**：RunPrompt 每轮折叠事件
// 流带历史——见 host/hub.go 的 RunPrompt），主 agent id 解析 loopFor 已有（已登记 →
// 事件流 → 新造）。loopFor 自身的技术故障（建 loop 失败等）按 500 takeover_failed 回
// ——那是故障，不是口径拒，不算"拒绝线"。
//
// 留痕复用既有 hub.alert 事件（graph_rebuild.recordGraphAlert 同款），**不加新事件
// kind**（F7 只增：能复用就不扩契约）。
//
// # 同源纪律
//
// HTTP servePrompt 与 WS HandleInbound 的 command.submit 走**同一个** ensureHosted、
// 同一段冲突文案（takeoverConflictText）——同口径同行为，两面不各写一套（G2/P3-3）。
//
// # 挂账（跨进程 Claim 冲突检测不到）
//
// Claim 的宿主登记（发现表 hostHub）与 live 都是**进程本地**状态（core/discovery：
// 不进 Snapshot、不落事件流、无租约无心跳）。于是"同库另一个 hub 进程正持有该
// session"这件事，在**跨进程**时本进程根本看不见——本文件的拒绝线只在**同进程的
// 发现表**上撞得出冲突（预登记/同表双宿主）。也就是说：当前的 fail-close 以
// **"一库一 hub 是预期拓扑"** 为前提（G12：一库多 hub 是例外，读面看得见但写面互不
// 保证）。将来若要跨进程互斥，方向是 durable claim（F7 新事件 kind：宿主登记进事件
// 流 → 各进程 catch-up 即知谁持有），那要扩契约；当前按"接不进就挂账注释"的零契约
// 处理，不预先占 kind。
//
// # 诚实边界（G-b 之后的安全面收缩，明说）
//
//	**G-b 之前**：协议面对非托管 session 一律 404（双写面是**关闭**的——没托管就
//	              不受理，谁也接管不了）。
//	**G-b 之后**：协议面改成懒接管，于是"能不能接管"这件事的安全性**全部寄托在
//	              Claim 上**——而 Claim 是**进程本地**的：**只有同进程冲突可拒**
//	              （同一个发现表撞得出另一个宿主 id），跨进程撞不出。
//
// 也就是说：**预期拓扑"一库一 hub"（G12）之外的共享库部署，请自觉不要用协议面
// prompt**——两个 hub 进程共享一库时，各自都可能接管同一个 session 并各自 append
// 它的事件（G25 破：全局 seq = 混排提交序）。这不是待修的 bug，是**部署约束**：
// 跨进程互斥要 durable claim（F7 新事件 kind：宿主登记进事件流 → 各进程 catch-up
// 即知谁持有），那是另一刀（要扩契约），本刀不预先占 kind。

// sessionTakeoverMetric 是懒接管留痕的 hub.alert 指标名（metric 字段取值；
// 命名照 graph.rebuild 的"对象.动作"风格）。
const sessionTakeoverMetric = "session.takeover"

// ensureHosted 保证 sid 由本 hub 托管（G-b 懒接管的唯一入口，HTTP/WS 两面共用）。
//
//	hostsSession(sid) → nil（照常跑，**不留痕**）
//	否则 → loopFor 懒接管：成功 → 按"托管权是否易主"决定要不要落 hub.alert 留痕
//	      （session.takeover）；失败 → 原样返回（调用方按
//	      errors.Is(err, discovery.ErrHostConflict) 判 409 host_conflict，其余按 500
//	      takeover_failed）。
//
// ctx 供 loopFor 内部用（事件流 catch-up / 自动 gate 加入）；nil ctx 按 Background 兜底。
func (h *Hub) ensureHosted(ctx context.Context, sid contracts.Ulid) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// 接管前是否已托管（loop 缓存 = "能否直发"，**不是**托管权归属——见下面的留痕判据）。
	wasHosted := h.hostsSession(sid)
	if wasHosted {
		return nil // 已托管：照常，不留痕（只有"真接管"才值得复盘）
	}
	// **托管权快照**（F3）：在 loopFor/Claim **之前**取——"接管前这个 session 登记在谁名下"。
	// 这是留痕的真正判据（见下）。
	//
	// 为什么不看 hostsSession：loop 缓存会被**主动清掉**（workflow 激活会 delete
	// h.loops——那是"重建 loop"，托管权从未易主）。拿"没缓存"当接管信号，会把重建
	// 误报成一次"懒接管"（假留痕：复盘时会读出一次没发生过的易主）。
	// 判据换成**发现表的宿主登记**：接管前 HostOf 为空（从没被谁托管过 / 原宿主进程
	// 已失管）或 ≠ 本 hub id（**真易主**）才算接管。
	prevHost, hadHost := h.disc.HostOf(sid)
	// created = 本次调用**真正建成并发布**了 loop（loopFor 的双检落败方 = false：
	// 别人已经接管并发布了同一个 session 的 loop——那不是"本进程接管了它"）。
	_, created, err := h.loopFor(ctx, sid)
	if err != nil {
		return err // Claim 冲突在 loopFor 里就地失败（**未发布**，无需回滚），错误链带 ErrHostConflict
	}
	// 留痕条件（P0-1 + F3）：**接管前宿主登记不是本 hub（真易主）&& created**。
	//
	// ① created：并发接管同一 session 时，N 个请求都看到"没托管"，但只有一个真正建成了
	//    并发布了 loop（双检落败的那些拿到的是别人发布的 loop）——按旧口径会落 N 条
	//    "接管成功"留痕（复盘时会读出 N 次没发生过的接管）。created 把"真接管"与
	//    "搭便车"分开：双并发只留一条痕。
	// ② 宿主登记易主（hadHost==false 或 prevHost≠h.ID()）：本 hub 之前**不是**这个
	//    session 的宿主 → 托管权确实易主了。HostOf == h.ID() 的**重建**（loop 缓存被
	//    workflow 激活清掉后重建）**不留痕**——托管权没易主。
	//    （快照取自 Claim 之前：Claim 之后 HostOf 就是本 hub id了，取不到"易主前"的事实。）
	hostChanged := !hadHost || prevHost != h.ID()
	if hostChanged && created {
		h.recordTakeoverAlert(ctx, sid)
	}
	return nil
}

// recordTakeoverAlert 记一条 hub.alert 留痕（懒接管成功：G-b 的可复盘事实）。
//
// 样板照 graph_rebuild.recordGraphAlert：建 sink → RecordAlert → 失败只 warn、
// **不**影响主流程（接管本身已经成功，下一句就去受理 prompt 了）。
// 走 hub.* 事件族（durable，进事件流）——不新造 kind（F7 只增：能复用就不扩契约）。
// 事件 id 内容寻址（alertEventID：metric+采样时刻）→ 重试天然幂等，不会落成两条。
func (h *Hub) recordTakeoverAlert(ctx context.Context, sid contracts.Ulid) {
	if h == nil || h.bus == nil {
		return
	}
	sink := newHubAlertSink(h.bus, h.cfg.DBPath)
	err := sink.RecordAlert(ctx, Alert{
		At:        time.Now(),
		Metric:    sessionTakeoverMetric,
		Value:     1,
		Threshold: 0,
		Message: fmt.Sprintf("懒接管（G-b）：session %s 由本 hub %s 懒接管"+
			"（Claim + 建 loop + 事件流恢复主 agent id）", sid, h.ID()),
	})
	if err != nil {
		hubLog.Warn("host: 懒接管留痕落事件流失败（接管本身不受影响）",
			"session_id", string(sid), "err", err.Error())
	}
}

// takeoverConflictText 是 Claim 冲突的**唯一**文案（HTTP 409 与 WS 错误帧共用一段，
// 同源同口径）。带指引（G12）：一库一 hub 是预期拓扑，要继续就在持有它的 hub 上发，
// 或开新会话——不是"修一修这个 id"。err 链带上登记详情（哪个 hub 持有）。
func takeoverConflictText(sid contracts.Ulid, err error) string {
	msg := fmt.Sprintf(
		"session %s 正由另一个 hub 托管，拒绝接管（G25：一个 session 的事件只由其宿主进程"+
			"append——双写会把全局 seq 变成混排提交序；G12：一库一 hub 是预期拓扑，请在持有"+
			"它的 hub 上继续，或 POST /api/sessions 开新会话）", sid)
	if err != nil {
		msg += "；登记详情: " + err.Error()
	}
	return msg
}
