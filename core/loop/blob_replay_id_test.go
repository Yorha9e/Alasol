package loop_test

// 缺口 4 前半的钉子：**重放幂等**——外置 blob 的 id 改为**内容寻址**
// （core/loop/blob.go contentBlobID：Crockford26(SHA-256("blob\x00"‖raw)[:16])），
// 同一 result 重放（或不同工具产出同内容）→ 同一 id → blobfs.Put 走既有幂等路径
// （不写第二份）；不同内容 → 不同 id。
//
// # 原症状
//
// externalizeResult 原先不传 meta.ID（实现发号 = 每次外置一个新 ULID）——重放路径
// （replay.go）**同口径**外置，同一内容每重放一次就多一份 blob：磁盘单调增长，
// 重放次数 = blob 份数（崩溃续跑每 resume 一轮就翻倍）。
//
// # 变异自证
//
// contentBlobID 改回 fresh ULID（meta.ID 留空让实现发号）→ 本文件前两条钉子红
// （重放后 blob 份数 = 2、两次 BinaryRef 不同）。
//
// 第三条钉子走**真实 blobfs**（tmp 目录 + 写计数探针）：这是 core/loop 测试里唯一
// import runtime/store/blobfs 的地方——blob_test.go 的"core 测试边界"（不 import
// runtime）是**生产代码**的边界；"重放不产生第二份**目录**"必须在真实文件系统上验
// （内存 fake 的 map 语义天然"去重"，抓不住"写了两份落盘"），故此处刻意破例，
// 只用公开 API（BlobSink 接线 + blobfs.Store），不碰 loop 内部。

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/durable"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// replayOpOf 造一个与 harness 同 session/agent 的重放 op（intent 指向指定 tool_call）。
func replayOpOf(h *harness, toolCallID, toolName string) durable.Op {
	return durable.Op{
		Intent: contracts.ToolCallIntentPayload{
			ToolCallID: contracts.ProviderId(toolCallID),
			ToolName:   toolName,
			Arguments:  json.RawMessage(`{"text":"hi"}`),
		},
		IntentEventID: core.NewUlid(),
		SessionID:     h.session,
		AgentID:       h.agent,
	}
}

// TestD4ReplaySameResultOneBlob（重放幂等的主钉子）：同一 result 外置两次
// （原执行 Run + 重放 Replay）→ **一个 blob**、两次 completed 的 BinaryRef **相同**。
//
// 为什么用 Run + Replay 而不是两次 Run：缺口正是"重放路径同口径外置"——Replay 是
// 与 runToolCall **不同的写路径**（replay.go 自造 completed），只有把两条路径接在
// 同一个内容上才能钉住"重放不多造 blob"。
func TestD4ReplaySameResultOneBlob(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("R", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	blob := newFakeBlob()
	l := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64 // 小阈值 → 5000 字节必外置
	})
	ctx := context.Background()

	// 原执行。
	h.script(stepTools(call("tc_CID", "echo", `{"text":"hi"}`)), stepText("完成"))
	if _, err := l.Run(ctx, "echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 重放同一 tool_call（执行器产出**同一份** result → 同内容）。
	if _, err := l.Replay(ctx, replayOpOf(h, "tc_CID", "echo")); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	done1 := findEvent(t, h.events(), loop.KindToolCallCompleted, 0)
	done2 := findEvent(t, h.events(), loop.KindToolCallCompleted, 1)
	if done1.BinaryRef == nil || done2.BinaryRef == nil {
		t.Fatalf("两次外置的 BinaryRef 都应非空：got %v / %v", done1.BinaryRef, done2.BinaryRef)
	}
	if *done1.BinaryRef != *done2.BinaryRef {
		t.Fatalf("同一内容两次外置应得**同一** blob id（内容寻址）：run=%s replay=%s",
			*done1.BinaryRef, *done2.BinaryRef)
	}
	// 一个 blob（Put 幂等：同 id 同内容不产生第二份）。
	if blob.count() != 1 {
		t.Fatalf("同一内容重放后应只有 1 个 blob，got %d（重放又外置了一份？）", blob.count())
	}
	// 读侧幂等：Get 两次同 id 同内容。
	var got [][]byte
	for i := 0; i < 2; i++ {
		rc, err := blob.Get(ctx, *done1.BinaryRef)
		if err != nil {
			t.Fatalf("Get #%d: %v", i+1, err)
		}
		raw, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("读 blob #%d: %v", i+1, err)
		}
		got = append(got, raw)
	}
	if !bytes.Equal(got[0], got[1]) {
		t.Fatalf("两次 Get 内容应逐字节相同（%d vs %d 字节）", len(got[0]), len(got[1]))
	}
	// blob 里仍是完整 ToolResult（重放不改变内容，只复用 id）。
	var full contracts.ToolResult
	if err := json.Unmarshal(got[0], &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != big {
		t.Fatal("blob 里应是完整的原始 result")
	}
}

// TestD4DifferentContentDifferentID：不同内容 → 不同 id（一次 Run 里两个工具各外置
// 一份**不同**的大 result → 两个 blob、两个不同的 BinaryRef）。
//
// 为什么必须钉反面：只钉"同内容同 id"抓不住"id 派生把内容弄丢了"（恒定 id / 只取
// 前缀哈希都会让不同内容撞同一个 id——blobfs 会报 ErrConflict 走 fail-open，
// 表现为"第二个工具的结果没外置"）。
func TestD4DifferentContentDifferentID(t *testing.T) {
	h := newHarness(t, allowRule("echo"), allowRule("cat"))
	bigA := strings.Repeat("A", 5000)
	bigB := strings.Repeat("B", 5000) // 同长度不同内容（防"只比大小"的浅判）
	execA := h.addTool("echo", nil)
	execA.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &bigA}}}
	execB := h.addTool("cat", nil)
	execB.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &bigB}}}
	blob := newFakeBlob()
	h.script(
		stepTools(
			call("tc_CID_A", "echo", `{"text":"a"}`),
			call("tc_CID_B", "cat", `{"text":"b"}`),
		),
		stepText("完成"),
	)
	if _, err := h.loop(func(c *loop.Config) {
		c.BlobSink = blob
		c.BlobMaxInline = 64
	}).Run(context.Background(), "go"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	doneA := findEvent(t, h.events(), loop.KindToolCallCompleted, 0)
	doneB := findEvent(t, h.events(), loop.KindToolCallCompleted, 1)
	if doneA.BinaryRef == nil || doneB.BinaryRef == nil {
		t.Fatalf("两份大 result 都应外置：got %v / %v", doneA.BinaryRef, doneB.BinaryRef)
	}
	if *doneA.BinaryRef == *doneB.BinaryRef {
		t.Fatalf("不同内容应得不同 blob id（都撞同一个 = fail-open 或派生丢内容），got %s", *doneA.BinaryRef)
	}
	if blob.count() != 2 {
		t.Fatalf("不同内容应落 2 个 blob，got %d", blob.count())
	}
}

// TestD4ReplayNoSecondDirRealBlobfs：走**真实 blobfs**（tmp 目录 + 写计数探针）——
// 重放不产生第二份**目录**、内容文件不写第二遍（写计数停在 1）。
//
// 为什么必须真实文件系统：内存 fake 的 map 按 id 键控，"同 id 覆盖"天然只留一份——
// 抓不住"实现发号 + 两次落盘两个目录"（原症状）；blobfs.WriteCounter 数的是
// **内容文件真正落盘次数**（幂等二次 Put = +0），这正是重放幂等的可观测面。
func TestD4ReplayNoSecondDirRealBlobfs(t *testing.T) {
	st, err := blobfs.New(t.TempDir())
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	counter := &blobfs.WriteCounter{}
	st.SetWriteCounter(counter)

	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("D", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	l := h.loop(func(c *loop.Config) {
		c.BlobSink = st
		c.BlobMaxInline = 64
	})
	ctx := context.Background()

	h.script(stepTools(call("tc_CID_FS", "echo", `{"text":"hi"}`)), stepText("完成"))
	if _, err := l.Run(ctx, "echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := l.Replay(ctx, replayOpOf(h, "tc_CID_FS", "echo")); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	done1 := findEvent(t, h.events(), loop.KindToolCallCompleted, 0)
	done2 := findEvent(t, h.events(), loop.KindToolCallCompleted, 1)
	if done1.BinaryRef == nil || done2.BinaryRef == nil || *done1.BinaryRef != *done2.BinaryRef {
		t.Fatalf("重放应复用同一 blob id：run=%v replay=%v", done1.BinaryRef, done2.BinaryRef)
	}
	// 内容文件只落盘一次（幂等二次 Put 不写第二份——写计数探针是唯一能盯住这件事的面）。
	if got := counter.Count(); got != 1 {
		t.Fatalf("内容落盘次数 = %d，want 1（重放走了幂等路径就不该再写）", got)
	}
	// 磁盘上恰好一个 blob 目录（重放不产生第二份目录）。
	entries, err := os.ReadDir(st.Root())
	if err != nil {
		t.Fatalf("列 blob 根目录: %v", err)
	}
	if len(entries) != 1 || !entries[0].IsDir() {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("blob 根目录应恰好 1 个目录（同内容重放不建第二份），got %d: %v", len(entries), names)
	}
	if entries[0].Name() != string(*done1.BinaryRef) {
		t.Fatalf("目录名应 = blob id %s，got %s", string(*done1.BinaryRef), entries[0].Name())
	}
	// 重开一个实例（= 重开进程）仍能按同一个 id 取到内容。
	st2, err := blobfs.New(st.Root())
	if err != nil {
		t.Fatalf("blobfs.New(reopen): %v", err)
	}
	rc, err := st2.Get(ctx, *done1.BinaryRef)
	if err != nil {
		t.Fatalf("重开后 Get: %v", err)
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("重开后读 blob: %v", err)
	}
	var full contracts.ToolResult
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatalf("blob 内容不是合法 ToolResult JSON: %v", err)
	}
	if len(full.Content) != 1 || full.Content[0].Text == nil || *full.Content[0].Text != big {
		t.Fatal("重开后 blob 里应是完整原文")
	}
}

// TestD4ReplayRefreshesBlobAge（F3）：**重放**（= 同一份内容被再次外置）→ blobfs 的幂等
// Put 刷新 meta.json 的 mtime → 龄期 = **最后外置时间** → 仍在被引用的 blob 不会被 GC
// 当成超期残留删掉。
//
// 为什么这条值得钉：内容寻址 id 下"重复引用同一份内容"是常态（重放/再次引用同一份大
// result）。若幂等 Put 不刷新 mtime，GC 的 mtime 清扫（-retention > 0）会把**仍在被引用**
// 的 blob 当成超期残留删掉——读侧只能报"内容已回收"（引用还在，内容没了）。
//
// 变异自证：幂等 Put 不触碰 meta mtime → 下面的"mtime 被刷新"断言红。
func TestD4ReplayRefreshesBlobAge(t *testing.T) {
	st, err := blobfs.New(t.TempDir())
	if err != nil {
		t.Fatalf("blobfs.New: %v", err)
	}
	counter := &blobfs.WriteCounter{}
	st.SetWriteCounter(counter)

	h := newHarness(t, allowRule("echo"))
	big := strings.Repeat("E", 5000)
	exec := h.addTool("echo", nil)
	exec.result = contracts.ToolResult{Content: []contracts.ContentPart{{Type: "text", Text: &big}}}
	l := h.loop(func(c *loop.Config) {
		c.BlobSink = st
		c.BlobMaxInline = 64
	})
	ctx := context.Background()

	h.script(stepTools(call("tc_CID_AGE", "echo", `{"text":"hi"}`)), stepText("完成"))
	if _, err := l.Run(ctx, "echo"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	done := findEvent(t, h.events(), loop.KindToolCallCompleted, 0)
	if done.BinaryRef == nil {
		t.Fatal("大 result 应外置（BinaryRef 非空）")
	}
	metaPath := filepath.Join(st.Root(), string(*done.BinaryRef), "meta.json")

	// 把 meta 的 mtime 钉老（模拟"很久没人引用"）。
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(metaPath, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	// 重放：同内容再外置一次 → 幂等 Put → 刷新 mtime（但**不写第二份**）。
	if _, err := l.Replay(ctx, replayOpOf(h, "tc_CID_AGE", "echo")); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fi, err := os.Stat(metaPath)
	if err != nil {
		t.Fatalf("stat meta.json: %v", err)
	}
	if time.Since(fi.ModTime()) > time.Minute {
		t.Errorf("重放应刷新 blob 的 mtime（最后外置时间），got %v（仍是老时间）", fi.ModTime())
	}
	if got := counter.Count(); got != 1 {
		t.Errorf("内容落盘次数 = %d，want 1（重放仍走幂等路径：不写第二份）", got)
	}
}
