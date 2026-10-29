package host

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// 本文件处理 v3-S3 的 **N2 砖化修复**：图投影重建被崩溃打断后，graph_meta.rebuilding=1
// 会**永久留在库里**——此后读侧引擎每次 Sync 都读到它 → Reachable/Impact 一直 fail-close
// 报 ErrRebuilding → 影响面 / 冲突预测**永久不可用**（砖化）。而 Rebuild 当时**没有生产
// 调用方**（只有测试调），没人恢复它。
//
// 处置（三件套，缺一不可）：
//
//  1. **启动期检测**：组合根（host.New）读一眼 rebuilding 标记（便宜：只读 graph_meta 一行，
//     不加载图——见 graph.Engine.RebuildingState）；
//  2. **自动恢复**：检测到就自动重跑一次重建（indexer.Rebuild：清空投影 + 从 0 重放 +
//     清标记），恢复读侧作答能力；
//  3. **运维入口**：Hub.RebuildGraph（+ HTTP `POST /api/graph/rebuild`）——自动恢复失败时
//     人工重试的入口（**明确告警**而不是静默砖化）。
//
// # 中断要留痕（事件流）
//
// 重建是**派生层**操作（投影可从事件流重建），但"上次重建被谁打断、什么时候恢复的"是
// **hub 级事实**——hub.* 事件族就是这类事实的载体（见 host/alert_event.go）。故检测到崩溃
// 遗留的重建窗口时记一条 hub.alert（durable，进事件流）：事后可复盘"这个库的图投影曾经
// 处于不可用窗口"。不新造事件 kind（F7 只增：能复用就不扩契约）。
//
// # 不做的事
//
//   - **不**在检测失败时拒绝启动：图投影是**可选**能力（G2f），它坏了不该拖垮整个 hub；
//     处置是"明确告警 + 读侧继续 fail-close"（拿半图/空图当"无冲突"才是危险的）。
//   - **不**在启动期无条件重建：重建要清空派生表并从 0 重放（代价随事件量增长）——只在
//     真的检测到 rebuilding=1 时才做。

// graphRebuildMetric 是图投影重建相关告警的指标名（hub.alert 的 metric 字段）。
//
// 口径：与 monitor 的 cpu/mem/goroutines 同族（开放字符串），但**不是资源指标**——它是
// "图投影状态"指标（值 1 = 处于/曾处于重建窗口）。
const graphRebuildMetric = "graph.rebuild"

// recoverGraphRebuild 是**启动期**的 N2 恢复（组合根调用；见文件头）。
//
// 流程：读重建标记 → 是"重建中" → 记 hub.alert（中断留痕）→ 自动重跑一次重建 →
// 成功再记一条 hub.alert（已恢复）；失败则明确告警（读侧保持 fail-close，不拿半图答题）。
//
// 图投影未启用（graphEngine == nil）→ 直接返回（没有图投影就没有重建窗口）。
func (h *Hub) recoverGraphRebuild(ctx context.Context) {
	if h == nil || h.graphEngine == nil {
		return
	}
	rebuilding, err := h.graphEngine.RebuildingState(ctx)
	if err != nil {
		// 读不到标记 ≠ 正在重建：明确告警（不猜），但**不**拒绝启动（图投影是可选能力）。
		hubLog.Warn("host: 读图投影重建标记失败（无法判断是否处于重建窗口；读侧按 fail-close 处理）",
			"err", err.Error())
		return
	}
	if !rebuilding {
		return // 正常状态：没有崩溃遗留的重建窗口
	}
	hubLog.Warn("host: 检测到上次图投影重建未完成（rebuilding=1）→ 自动重跑一次重建（此前影响面/冲突预测一直不可用）")
	h.recordGraphAlert(ctx, "检测到上次图投影重建未完成（rebuilding=1，进程在重建窗口内被中断）：影响面/冲突预测此前一直不可用；正在自动重跑重建。")
	if rerr := h.RebuildGraph(ctx); rerr != nil {
		hubLog.Error("host: 自动重建图投影失败（读侧保持 fail-close：影响面/冲突预测仍不可用；可用 POST /api/graph/rebuild 重试）",
			"err", rerr.Error())
		h.recordGraphAlert(ctx, "自动重建图投影失败："+rerr.Error()+"（读侧保持 fail-close：影响面/冲突预测仍不可用；可用 POST /api/graph/rebuild 重试）")
		return
	}
	hubLog.Info("host: 图投影重建已恢复（重建窗口关闭）")
	h.recordGraphAlert(ctx, "图投影重建已恢复（重建窗口关闭）：影响面/冲突预测恢复作答。")
}

// RebuildGraph 重跑图/派生表重建（v3-S3，N2 **运维入口**）。
//
// 做什么：走索引器的 Rebuild（清空五张派生表 + 图投影 + 水位归零，再从 0 重放追平；
// 重放完成后清 rebuilding 标记 → 读侧恢复作答）。
//
// 什么时候用：① 启动期自动恢复失败（见 recoverGraphRebuild）；② 运维怀疑投影与事件流
// 不一致时手工重建。**代价**：清空 + 全量重放（事件量越大越慢）。
//
// 图投影未启用 → 仍然可用（重建的是派生表：sessions / ledger_state / board_state /
// cost_stats / cost_tokens）——重建与图无关，是索引器的通用恢复手段。
func (h *Hub) RebuildGraph(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("host: Hub 为 nil")
	}
	if err := h.ensureOpen(); err != nil {
		return err
	}
	if h.indexer == nil {
		return fmt.Errorf("host: 索引器未装配（无法重建）")
	}
	if err := h.indexer.Rebuild(ctx); err != nil {
		return fmt.Errorf("host: 重建投影失败: %w", err)
	}
	// 重建推进了重建代 → 读侧引擎下次 Sync 会整体重载；这里主动失效一次缓存，
	// 免得"重建前算的闭包"被当成"重建后的答案"（G20/F3 同款纪律）。
	if h.graphEngine != nil {
		h.graphEngine.Invalidate()
	}
	return nil
}

// recordGraphAlert 记一条 hub.alert（图投影重建相关的事实：中断 / 恢复 / 失败）。
//
// 走 hub.* 事件族（durable，进事件流）——不新造 kind（F7 只增：能复用就不扩契约）。
// 告警落不下**不**影响主流程（重建恢复本身已经尽力做过）：只记一条日志。
func (h *Hub) recordGraphAlert(ctx context.Context, message string) {
	if h == nil || h.store == nil {
		return
	}
	sink := newHubAlertSink(h.store, h.cfg.DBPath)
	err := sink.RecordAlert(ctx, Alert{
		At:        time.Now(),
		Metric:    graphRebuildMetric,
		Value:     1,
		Threshold: 0,
		Message:   message,
	})
	if err != nil {
		hubLog.Warn("host: 图投影告警落事件流失败（重建恢复本身不受影响）", "err", err.Error())
	}
}

// serveGraphRebuild 服务 POST /api/graph/rebuild（v3-S3，N2 运维入口）。
//
// body: 空（无参数）
//
//	200 → {"rebuilt": true}
//	503 → 图投影/索引器不可用（graph_disabled）
//	500 → 重建失败（rebuild_failed）
func (h *Hub) serveGraphRebuild(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		writeJSONError(w, http.StatusInternalServerError, "hub_nil", "host: Hub 为 nil")
		return
	}
	if err := h.RebuildGraph(r.Context()); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "rebuild_failed", err.Error())
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"rebuilt": true})
}
