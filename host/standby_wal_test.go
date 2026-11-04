package host

// G-f：standby 待机期间的 WAL 报告**不误报**（待机不是崩溃）。
//
// # 核实过的口径（为什么会有 blob.plan）
//
//	core/loop/blob.go 的 planExternalize：**blob 开着就记**一条 blob.plan（写在
//	tool.call.intent 之前）——与**结果体量无关**（阈值只是记在计划里）。所以：
//	  - blob 关（默认）：standby 不落计划 → 报告天然干净；
//	  - blob 开（含小阈值）：standby **会**落计划。
//
// 但 standby 的结果是短文本（量级几百字节，默认阈值下不外置；即便外置，blob 也是
// 等待**结束后**才写的）→ 等待期没有 blob 写出来 → 它那条计划**不可能**对应一份孤儿
// blob（"孤儿候选"名单的含义是"结果可能已写成 blob、引用没落"）；待机中（intent 有、
// completed 没有）更是**正在等**，不是崩溃残留。
//
// 故走 **facts 侧排除**（walfile.Facts.PendingToolCalls）：待机中的 standby 照实列进
// Report.PendingToolCalls（不藏证据），但**不进**孤儿候选 → Crashed() 为假。
//
// 本文件两组用例：
//
//	① blob 开（小阈值）：待机期间报告零误报（无在飞轮次 / 无孤儿候选 / Crashed() 假）；
//	② blob 关（默认口径）：同样零误报——且**照样列出**那个未收口的调用（列示来自
//	   事实面，与 blob.plan 无关；critic F8）。

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/loop"
)

// standbyWALHub 装配一个带 WAL（可选带 blob）的 hub：脚本 provider 让模型调 standby。
func standbyWALHub(t *testing.T, withBlob bool) *Hub {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		DBPath:   filepath.Join(dir, "events.db"),
		Model:    "standby-wal-test",
		System:   "standby-wal-test",
		MaxTurns: 4,
		Allow:    []string{ToolStandby},
		Provider: &standbyScriptProvider{
			plans: map[string][]standbyStep{
				"waiter": {{tool: ToolStandby, args: `{"wait":"message","timeout_min":10,"on_timeout":"return"}`}},
			},
			counts: map[string]int{},
		},
		CWD:    t.TempDir(),
		WalDir: filepath.Join(dir, "wal"),
	}
	if withBlob {
		// 小阈值：standby 的结果恒为短文本 → 永不外置（阈值再小也不触发）。
		cfg.BlobDir = filepath.Join(dir, "blobs")
		cfg.BlobMaxInline = 1024
	}
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// standbyIntentCallID 取本 session 那条 standby intent 的 tool_call_id（X1 配对键）。
func standbyIntentCallID(t *testing.T, h *Hub, sid contracts.Ulid) string {
	t.Helper()
	evs, err := h.store.ReadFrom(context.Background(), &sid, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	for _, ev := range evs {
		if ev.Kind != loop.KindToolCallIntent {
			continue
		}
		var p contracts.ToolCallIntentPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 intent 载荷失败: %v", err)
		}
		if p.ToolName == ToolStandby {
			return string(p.ToolCallID)
		}
	}
	t.Fatal("事件流里没有 standby 的 tool.call.intent")
	return ""
}

// TestStandbyWALReportNoFalseAlarm：待机**期间**的 WAL 报告零误报。
//
// 两种口径各跑一遍：blob 开（小阈值 → standby 也会落 blob.plan）与 blob 关（默认）。
//
// **两种口径都要列出那个没收口的调用**（critic F8）：列示来自**事实面**
// （事件流：intent 有、completed 没有），与 blob.plan 无关——旧口径把列示挂在
// 计划循环里，blob 关（默认）时未收口的 standby 调用在报告里一言不发（藏证据）。
func TestStandbyWALReportNoFalseAlarm(t *testing.T) {
	cases := []struct {
		name     string
		withBlob bool
	}{
		{"blob 开（小阈值）", true},
		{"blob 关（默认）", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// 待机挂起 10 秒（生产口径的 timeout_min=10）：报告在挂起期间取。
			standbyTimeKnobs(t, 10*time.Second, 30*time.Second, 20*time.Millisecond)
			h := standbyWALHub(t, tc.withBlob)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sid := NewSession()
			done := make(chan error, 1)
			go func() {
				_, err := h.RunPrompt(ctx, sid, "waiter")
				done <- err
			}()
			// 等到挂起（waiting 态亮起 = intent 已落、completed 还没落）。
			standbyWaitWaiting(t, h, sid, true)
			callID := standbyIntentCallID(t, h, sid)

			rep, err := h.WALReport(context.Background(), sid)
			if err != nil {
				t.Fatalf("WALReport: %v", err)
			}
			if rep.InFlightTurn != nil {
				t.Errorf("待机不该有在飞轮次（turn.end 在 model.turn 落盘后即写）：%+v", rep.InFlightTurn)
			}
			if len(rep.OrphanBlobToolCalls) != 0 {
				t.Errorf("待机不该报孤儿 blob 候选（standby 的结果永不外置）：%v", rep.OrphanBlobToolCalls)
			}
			if rep.Crashed() {
				t.Error("待机不是崩溃：Crashed() 应为 false")
			}
			// 不藏证据：那个没收口的调用要照实列出来（**两种 blob 口径都要**）。
			found := false
			for _, id := range rep.PendingToolCalls {
				if id == callID {
					found = true
				}
			}
			if !found {
				t.Errorf("未收口的调用里应有 %s，得到 %v", callID, rep.PendingToolCalls)
			}
			// 收尾：取消这一轮（测试不等 10 秒）。
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("取消后 standby 没收场")
			}
		})
	}
}

// TestStandbyWALReportAfterCompletionNoFalseAlarm：待机**收口之后**的报告同样零误报
// （completed 落了 → 不是孤儿候选；standby 的结果没外置 → 只是"作废计划"，不是崩溃）。
func TestStandbyWALReportAfterCompletionNoFalseAlarm(t *testing.T) {
	standbyTimeKnobs(t, 150*time.Millisecond, 5*time.Second, 20*time.Millisecond)
	h := standbyWALHub(t, true) // blob 开 → standby 也会落 blob.plan
	sid := NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "waiter"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	rep, err := h.WALReport(context.Background(), sid)
	if err != nil {
		t.Fatalf("WALReport: %v", err)
	}
	if rep.InFlightTurn != nil {
		t.Errorf("收口后不该有在飞轮次：%+v", rep.InFlightTurn)
	}
	if len(rep.OrphanBlobToolCalls) != 0 {
		t.Errorf("收口后不该报孤儿 blob 候选：%v", rep.OrphanBlobToolCalls)
	}
	if rep.Crashed() {
		t.Error("收口后不是崩溃：Crashed() 应为 false")
	}
	if len(rep.PendingToolCalls) != 0 {
		t.Errorf("收口后不该还有在飞工具调用：%v", rep.PendingToolCalls)
	}
}
