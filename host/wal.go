package host

import (
	"context"
	"fmt"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

// 本文件是 WAL 预写日志（v4-4-B）的宿主装配面：Config.WalDir → per-session Writer
// → loop.Config.WalSink，以及恢复报告的对账入口。
//
// # 定位：WAL 不是事实源（F5 不受影响）
//
// 事件流仍是唯一事实源。WAL 记录"正式写之前"的中间态，崩溃后至少知道"当时在干什么、
// 吐过什么"——**可丢弃 / 可截断 / 可 GC**：删掉整个 WAL 目录不丢任何事实，事件流照常完整
// 回放。恢复报告是**观察面不是写回面**（WAL 里的 delta 可供展示，绝不自动补进事件流）。
//
// # 关 = 零行为变化（酸测试）
//
// Config.WalDir 空（默认）→ 不建 walfile.Store、不建任何文件、loop.Config.WalSink 恒 nil
// → 一个字节都不多写，行为与配置前**逐事件等价**。这是默认路径。

// walFor 取（或建）某 session 的 WAL 追加写入口。**未启用 → (nil, nil)**（调用方把 nil
// 交给 loop.Config.WalSink = 关 = 零行为变化）。
//
// 懒建（第一次用到那个 session 才开文件）：hub 不预建全部 session 的 WAL——一个从没跑过
// 的 session 不该在磁盘上留空文件（那也是"配了 WAL 之后的可见变化"，酸测试要盯的）。
//
// 开文件失败**不拒绝**这一轮：WAL 是尽力而为的补充面，为它拒绝一次模型调用是把观测的
// 重要性置于事实之上。告警一条 + 本次 Run 无 WAL（事件流照常完整）。
//
// **锁序**：调用方（loopFor）持 h.mu，本函数只取 walfile.Store 自己的 mutex（walfile 不
// 认识 hub → 不存在反向边）→ 无环。代价是开文件这一次 IO 落在 h.mu 内（微秒级），可接受；
// 真要抠就改成锁外懒建 + 双重检查，但那会引入"两个 loop 同时拿到两个 Writer"的分叉。
func (h *Hub) walFor(sessionID contracts.Ulid) core.WalSink {
	if h == nil || h.wal == nil {
		return nil // 未启用：零行为变化
	}
	w, err := h.wal.Writer(sessionID)
	if err != nil {
		hubLog.Warn("host: 开 session 的 WAL 失败（本轮无 WAL，事件流照常完整）",
			"session_id", string(sessionID), "err", err.Error())
		return nil
	}
	return w
}

// WALDir 返回 WAL 根目录（空 = 未启用；诊断/测试用）。
func (h *Hub) WALDir() string {
	if h == nil || h.wal == nil {
		return ""
	}
	return h.wal.Dir()
}

// WALReport 扫某 session 的 WAL 并与事件流对账，产出崩溃恢复报告（v4-4-B 恢复面）。
//
// 未启用 WAL → (Report{WALExists:false}, nil)：不是错误（"没有 WAL"是合法状态，
// 报告如实说"没有"而不是报错）。
//
// 报告是**观察面不是写回面**：它不改事件流、不补 delta、不改任何状态。
//
// **成本口径（如实写明）**：对账要把该 session 的 completed 事件读一遍（ReadFrom 全量 +
// 折 Facts），是 **O(该 session 事件数)** 的一次性扫描。这是诊断命令/resume 摘要的路径，
// 不是每轮热路径；session 极大时（几十万事件）这一次扫描会慢——真要在 UI 上常驻就改成
// 索引器派生表（与 cost_tokens 同款），本刀不做（挂账）。
func (h *Hub) WALReport(ctx context.Context, sessionID contracts.Ulid) (walfile.Report, error) {
	if h == nil {
		return walfile.Report{}, fmt.Errorf("host: Hub 为 nil")
	}
	if !core.ValidUlid(sessionID) {
		return walfile.Report{}, fmt.Errorf("host: session_id 不是合法 ULID: %q", sessionID)
	}
	if h.wal == nil {
		return walfile.Report{
			SessionID: string(sessionID),
			WALPath:   "",
			WALExists: false,
		}, nil
	}
	events, err := h.store.ReadFrom(ctx, &sessionID, 0, 0)
	if err != nil {
		return walfile.Report{}, fmt.Errorf("host: 读 session %s 的事件流: %w", sessionID, err)
	}
	rep, rerr := walfile.ReconcileSession(h.wal.Dir(), sessionID, walfile.FactsFromEvents(events))
	if rerr != nil {
		// 读坏了也返回部分报告（崩溃恢复的第一步不该被一行坏数据堵死）——错误一并带回，
		// 调用方决定是当失败还是"用部分结果 + 说明"。
		return rep, fmt.Errorf("host: 对账 WAL: %w", rerr)
	}
	return rep, nil
}

// closeWAL 收口 hub 建过的所有 WAL Writer（Close 里调；幂等）。
//
// 放在**等在跑的 prompt 收场之后**：那一轮可能还在写 WAL，关早了就是往已关的 fd 上写
// （"file already closed" 噪声 + 丢尾部记录）。
func (h *Hub) closeWAL() error {
	if h == nil || h.wal == nil {
		return nil
	}
	if err := h.wal.Close(); err != nil {
		return fmt.Errorf("host: 关 WAL: %w", err)
	}
	return nil
}

// walDirOf 归一化 WAL 目录配置（trim；空 = 关）。
func walDirOf(dir string) string { return strings.TrimSpace(dir) }
