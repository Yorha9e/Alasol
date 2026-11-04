package host

// 本文件是 standby 待机工具在**恢复路径**（durable.Resume 的重放）上的装配面。
//
// # 为什么需要它（崩溃后 standby 的 in-flight 处置）
//
// 崩溃（kill -9 / panic）时，standby 的 tool.call.intent **已落盘**、completed **还没落**
// → 恢复流程把它当 in-flight op（X2 第一类：Replay=safe = 重放 = 重新执行 = 重新挂起）。
//
// 重放要"重新挂起"得满足两个前提：
//
//	① 重放走的是**同一个执行器**（host/standby.go 的 standbyExecutor）；
//	② 重放带的是**原 tool_call_id**——执行器据此回查事件流里那条原 tool.call.intent，
//	   拿到归属 session 与**重放游标**（= 原 intent 的 seq）。游标取原 intent 的 seq
//	   （不是"现在"）是"中断期间到达的消息能被看见"的前提：消息已到 → 立即 completed
//	   （如实收口）；未到 → 重新挂起（有界）。
//
// ①的前提在**装配侧**：durable.Replayer 的实现是 *core/loop.Loop，它按 op.Intent.ToolName
// 去**注册表**取执行器（core/loop/replay.go）。standby 此前**只在 hub 里注册**
// （host/hub.go），而 hub 没有恢复入口——恢复入口是 CLI（`alasol resume`），它的注册表
// 只有内置工具（read/write/bash + MCP）→ standby **查不到执行器** → 重放 fail-close
// （落 tool.call.failed、处置结论 replayed_failed = "需人工重做"）。
//
// 故本文件把 standby 的执行器**再装配一次**给恢复路径用：同一个实现、同一套依赖口径，
// 只是依赖的宿主能力换成"恢复路径能提供的那几样"（见 RegisterStandbyForResume）。
//
// # 恢复路径的依赖差异（与 hub 的四处差别，逐条说明）
//
//	① 事件流读面：两边同款（core.Store + Tip）——恢复路径直接用 CLI 手里的事件库。
//	② agentlink：hub 用 hub 级 Link；恢复路径自建一个（读面 = 事件库，可见性 = 本地发现表）。
//	③ 发现表：hub 的表带**活状态**（本进程托管的 session 是 live 的）；恢复路径的表
//	   是**冷表**（进程本地活状态重启后全失活，G17b）→ 必须显式 Activate 正在恢复的
//	   session，否则 K11 的 gate 成员过滤（GateMembers 只认**活跃成员**）看不见
//	   "中断期间到达的消息" → 唤醒判定永远等不到 → 白等一个超时。
//	④ hub 生命周期：hub 有关库信号（关库即中止待机）；恢复路径没有 hub——取消靠**执行
//	   ctx**（resume 的 ctx 取消 → 等待中止），故 hubDone 传 nil（nil channel 永不触发）。

import (
	"context"
	"errors"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/agentlink"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/registry"
	"github.com/Yorha9e/alasol/core/storage"
)

// StandbyEventSource 是 standby 执行器需要的**最小事件流读面**（core.Store + Tip）。
//
// 为什么要 Tip：standby 拿不到 intent 事件时要用"当前 seq"当降级游标（如实报出这个降级值）。
// core.Store 接口本身没有 Tip（只有具体实现有），故用窄接口断言——装配残缺时如实失败。
type StandbyEventSource interface {
	core.Store
	Tip(ctx context.Context) (int64, error)
}

// RegisterStandbyForResume 把 standby 工具注册进**恢复路径**用的注册表，
// 让 durable.Resume 的重放（Replay=safe）能真正再次执行它——带原 tool_call_id、
// 走同一个执行器（游标 = 原 intent 的 seq → 中断期间到达的消息能被看见）。
//
// sessionID 是**正在恢复的 session**：恢复路径自建的发现表是冷表（进程本地活状态重启后
// 全失活），故这里显式 Activate 它——否则 K11 的 gate 成员过滤（只认活跃成员）看不见
// 投递给本 session 的消息，standby 只能等到超时（"消息已到"却收不到 = 误报成"未等到"）。
//
// blob 是大 payload 外置仓库（storage.Blob；nil = 未启用）：**交付标记被外置**时经它
// 读回（host/standby.go 的 deliverySeqOf；critic P2：外置丢标记 → 已交付集合退化 →
// 重复交付）。恢复路径的 blob 由调用方建（CLI 的 [blob].dir 口径）。
//
// 幂等：同一个注册表重复注册同名工具会失败（注册表按名字唯一）——调用方注册一次即可。
func RegisterStandbyForResume(reg *registry.Registry, store StandbyEventSource, sessionID contracts.Ulid, blob storage.Blob) error {
	if reg == nil {
		return errors.New("host: 注册表为 nil——无法装配待机工具（恢复路径）")
	}
	if store == nil {
		return errors.New("host: 事件流读面缺失——无法装配待机工具（恢复路径）")
	}
	// 发现表 + agentlink（唤醒判定要经 agentlink 读收件箱；可见性判定要经发现表）。
	disc := discovery.New()
	link := agentlink.New(agentlink.Deps{Table: disc, Reader: store})
	feed := newDiscoveryFeed(store, disc)
	// 活状态：正在恢复的 session 由本进程托管 → live（否则 gate 成员过滤看不见它）。
	if core.ValidUlid(sessionID) {
		disc.Activate(sessionID)
	}
	return reg.RegisterTool(standbyDescriptor(), newStandbyExecutor(standbyDeps{
		store:   store,
		link:    link,
		disc:    disc,
		catchUp: feed.catchUp,
		hubDone: nil,  // 恢复路径没有 hub 生命周期；取消靠执行 ctx（见文件头）
		blob:    blob, // 交付标记被外置时经它读回（critic P2；nil = 未启用）
	}))
}
