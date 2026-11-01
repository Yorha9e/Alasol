package walfile_test

// WAL 预写日志（v4-4-B）的文件实现 + 恢复面对账钉子。
//
// 口径（以包 doc 与 core/wal.go 为准）：
//   - **WAL 不是事实源**（F5 不受影响）：可丢弃/可截断/可 GC；报告是观察面不是写回面；
//   - NDJSON 一行一条；per-session 一个 `<session>.wal`；
//   - **半截/坏行容忍**（崩溃后的 WAL 本来就该有半截行）→ 计入 CorruptLines，不当错误；
//   - 对账**不误报**：有对应 completed（带 binary_ref）的 plan 两个名单都不进。

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/store/walfile"
)

func newSession(t *testing.T) contracts.Ulid {
	t.Helper()
	return core.NewUlid()
}

// TestWriterRoundTrip（F1）：写进去的 kind/payload 原样读得回来（NDJSON 一行一条）。
func TestWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer st.Close()
	sid := newSession(t)

	w, err := st.Writer(sid)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	// 同一个 session 两次取 → 同一个 Writer（一个文件、一个 fd）。
	w2, err := st.Writer(sid)
	if err != nil {
		t.Fatalf("Writer(2): %v", err)
	}
	if w != w2 {
		t.Fatal("同一 session 应复用同一个 Writer（一个文件一个 fd）")
	}
	text := "你好"
	recs := []struct {
		kind    string
		payload any
	}{
		{core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m", Messages: 2, Tools: 3}},
		{core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: &text}},
		{core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1, ToolCalls: 0, InputTokens: 7, OutputTokens: 3}},
		{core.KindWalBlobPlan, core.WalBlobPlanPayload{Turn: 1, ToolCallID: "tc_1", ResultBytes: 5000, Threshold: 64}},
	}
	for _, r := range recs {
		if err := w.Append(r.kind, r.payload); err != nil {
			t.Fatalf("Append(%s): %v", r.kind, err)
		}
	}

	scan, err := walfile.ReadSession(dir, sid)
	if err != nil {
		t.Fatalf("ReadSession: %v", err)
	}
	if !scan.Exists {
		t.Fatal("WAL 文件应存在")
	}
	if len(scan.Records) != len(recs) {
		t.Fatalf("记录 = %d，want %d", len(scan.Records), len(recs))
	}
	if scan.CorruptLines != 0 {
		t.Errorf("CorruptLines = %d，want 0", scan.CorruptLines)
	}
	for i, r := range recs {
		if scan.Records[i].Kind != r.kind {
			t.Errorf("记录[%d].kind = %q，want %q", i, scan.Records[i].Kind, r.kind)
		}
		if scan.Records[i].TS == "" {
			t.Errorf("记录[%d] 缺 TS", i)
		}
	}
	var begin core.WalTurnBeginPayload
	if err := json.Unmarshal(scan.Records[0].Payload, &begin); err != nil {
		t.Fatalf("begin payload: %v", err)
	}
	if begin.Model != "m" || begin.Messages != 2 || begin.Tools != 3 || begin.Turn != 1 {
		t.Errorf("begin payload = %+v", begin)
	}
	var delta core.WalTurnDeltaPayload
	if err := json.Unmarshal(scan.Records[1].Payload, &delta); err != nil {
		t.Fatalf("delta payload: %v", err)
	}
	if delta.Text == nil || *delta.Text != "你好" {
		t.Errorf("delta payload = %+v", delta)
	}
}

// TestPerSessionFilesIsolated（F2）：两个 session 各写各的文件，互不串读。
func TestPerSessionFilesIsolated(t *testing.T) {
	dir := t.TempDir()
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer st.Close()
	a, b := newSession(t), newSession(t)

	wa, _ := st.Writer(a)
	wb, _ := st.Writer(b)
	if err := wa.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := wb.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "B"}); err != nil {
		t.Fatal(err)
	}
	if err := wb.Append(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}); err != nil {
		t.Fatal(err)
	}

	sa, err := walfile.ReadSession(dir, a)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := walfile.ReadSession(dir, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(sa.Records) != 1 || len(sb.Records) != 2 {
		t.Fatalf("A %d 条 / B %d 条，want 1/2（per-session 文件不得互串）", len(sa.Records), len(sb.Records))
	}
	// 文件路径按 session 分。
	if filepath.Base(sa.Path) != string(a)+".wal" || filepath.Base(sb.Path) != string(b)+".wal" {
		t.Errorf("文件名 = %q / %q", sa.Path, sb.Path)
	}
}

// TestMissingWALIsNotError（F3）：文件不存在 → (空, false, nil)——"没有 WAL"是合法状态
// （未配置 / 没写过），不是错误。
func TestMissingWALIsNotError(t *testing.T) {
	dir := t.TempDir()
	scan, err := walfile.ReadSession(dir, newSession(t))
	if err != nil {
		t.Fatalf("文件不存在不该报错: %v", err)
	}
	if scan.Exists || len(scan.Records) != 0 {
		t.Fatalf("exists=%v records=%d，want false/0", scan.Exists, len(scan.Records))
	}
}

// TestTruncatedLastLineTolerated（F4，崩溃容忍）：**半截行**（进程在 write 中途被杀）
// → 计入 CorruptLines 并跳过，不当错误——崩溃恢复的第一步不该被一行坏数据堵死。
func TestTruncatedLastLineTolerated(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, _ := st.Writer(sid)
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(core.KindWalTurnEnd, core.WalTurnEndPayload{Turn: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// 手工接一行**半截** JSON（模拟崩溃现场：写了一半进程没了）。
	path := filepath.Join(dir, string(sid)+".wal")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"kind":"turn.delta","payload":{"turn":1,"text":"半截`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	scan, err := walfile.ReadSession(dir, sid)
	if err != nil {
		t.Fatalf("半截行不该让扫描失败: %v", err)
	}
	if len(scan.Records) != 2 {
		t.Fatalf("解析成功记录 = %d，want 2（半截行应被跳过）", len(scan.Records))
	}
	if scan.CorruptLines != 1 {
		t.Fatalf("CorruptLines = %d，want 1（坏行必须数出来，不静默）", scan.CorruptLines)
	}
}

// TestBadKindAndBadJSONCounted（F5）：坏 JSON 与缺 kind 的行都按损坏计（不猜、不当空载荷）。
func TestBadKindAndBadJSONCounted(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := filepath.Join(dir, string(sid)+".wal")
	lines := []string{
		`{"kind":"turn.begin","payload":{"turn":1,"model":"m"}}`,
		`{"kind":"","payload":{}}`, // 缺 kind → 对不上账
		`{"kind":"turn.end"}`,      // 无 payload → 对账时按损坏跳过
		`not json at all`,          // 坏 JSON
		`{"kind":"turn.delta","payload":{"turn":2,"text":"ok"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	scan, err := walfile.ReadSession(dir, sid)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(scan.Records) != 3 {
		t.Fatalf("记录 = %d，want 3", len(scan.Records))
	}
	if scan.CorruptLines != 2 {
		t.Fatalf("CorruptLines = %d，want 2（缺 kind + 坏 JSON）", scan.CorruptLines)
	}
}

// TestConcurrentAppendLinesDoNotInterleave（F6）：并发 Append 下行与行不相交
// （一行一写 + mutex）；-race 覆盖。
func TestConcurrentAppendLinesDoNotInterleave(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, _ := st.Writer(sid)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			text := strings.Repeat("x", 100)
			_ = w.Append(core.KindWalTurnDelta, core.WalTurnDeltaPayload{Turn: 1, Text: &text})
		}(i)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	scan, err := walfile.ReadSession(dir, sid)
	if err != nil {
		t.Fatalf("ReadSession: %v", err)
	}
	if len(scan.Records) != n || scan.CorruptLines != 0 {
		t.Fatalf("记录 = %d（损坏 %d），want %d/0（行不得相交）", len(scan.Records), scan.CorruptLines, n)
	}
}

// TestCloseIdempotentAndRejectsAfterClose（F7）：Close 幂等；关闭后 Append 返错（不 panic）。
func TestCloseIdempotentAndRejectsAfterClose(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, _ := st.Writer(sid)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("二次 Close 应幂等: %v", err)
	}
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1}); err == nil {
		t.Fatal("关闭后 Append 应返错（不静默丢弃、不 panic）")
	}
	// Store.Close 也幂等。
	if err := st.Close(); err != nil {
		t.Fatalf("Store.Close: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("二次 Store.Close 应幂等: %v", err)
	}
}

// TestRejectsNonUlidSession（F8）：session id 不合法 → 拒（防路径穿越；与 blobfs 同口径）。
func TestRejectsNonUlidSession(t *testing.T) {
	dir := t.TempDir()
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Writer(contracts.Ulid("../../etc/passwd")); err == nil {
		t.Fatal("非法 session id 应被拒（防路径穿越）")
	}
	if _, err := walfile.ReadSession(dir, contracts.Ulid("nope")); err == nil {
		t.Fatal("ReadSession 对非法 session id 应报错")
	}
}

// TestNewRejectsEmptyDir（F9）：空目录 → 拒（"空 = 关"由调用方判，不在这里静默放行）。
func TestNewRejectsEmptyDir(t *testing.T) {
	if _, err := walfile.New("   "); err == nil {
		t.Fatal("空目录应被拒")
	}
}

// TestTruncateAndRemoveIdempotent（F10）：两个 GC 原语幂等（不自动调——策略挂账）。
func TestTruncateAndRemoveIdempotent(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, _ := st.Writer(sid)
	if err := w.Append(core.KindWalTurnBegin, core.WalTurnBeginPayload{Turn: 1, Model: "m"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := walfile.Truncate(dir, sid); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	scan, _ := walfile.ReadSession(dir, sid)
	if !scan.Exists || len(scan.Records) != 0 {
		t.Fatalf("截断后应存在但空记录，got exists=%v records=%d", scan.Exists, len(scan.Records))
	}
	if err := walfile.Remove(dir, sid); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := walfile.Remove(dir, sid); err != nil {
		t.Fatalf("二次 Remove 应幂等: %v", err)
	}
	scan, _ = walfile.ReadSession(dir, sid)
	if scan.Exists {
		t.Fatal("删除后文件不该存在")
	}
}

// TestOverlongLineDoesNotStopScan（F3，critic）：**超过 1MB 的行** → 丢弃该行、计入
// CorruptLines，**继续读后面的行**。
//
// 为什么钉：旧实现用 bufio.Scanner，它的错误是**粘性**的（ErrTooLong 之后每次 Scan 都
// 失败）→ 一行超长就让**后面所有记录全丢**（"崩溃后读不了自己的 WAL"）。现在自己按行读
// （与 runtime/acp 的 frame.go 同款）：超限行跳过后继续。
func TestOverlongLineDoesNotStopScan(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	path := filepath.Join(dir, string(sid)+".wal")

	// 一条超长行（> 1MB）：中间夹一条正常行，验证"后面的行仍读得到"。
	huge := `{"kind":"turn.delta","payload":{"turn":1,"text":"` + strings.Repeat("X", 1<<20+64) + `"}}`
	lines := []string{
		`{"kind":"turn.begin","payload":{"turn":1,"model":"m"}}`,
		huge,
		`{"kind":"turn.end","payload":{"turn":1}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	scan, err := walfile.ReadSession(dir, sid)
	if err != nil {
		t.Fatalf("超长行不该让扫描失败（后续记录要能读到）: %v", err)
	}
	if len(scan.Records) != 2 {
		t.Fatalf("解析成功记录 = %d，want 2（超长行之后的记录必须读得到）", len(scan.Records))
	}
	if scan.Records[0].Kind != core.KindWalTurnBegin || scan.Records[1].Kind != core.KindWalTurnEnd {
		t.Errorf("记录 = %q / %q，want turn.begin / turn.end", scan.Records[0].Kind, scan.Records[1].Kind)
	}
	if scan.CorruptLines != 1 {
		t.Errorf("CorruptLines = %d，want 1（超限行必须数出来，不静默）", scan.CorruptLines)
	}
}

// TestCloseFlushesData（F4，critic）：Close **先 fsync 再关**（文档一直这么说，旧实现没做）。
//
// 证法（黑盒可观测的那一半）：Close 之后文件里的数据**完整可读**（内容已刷出去、fd 已收），
// 且 Close 幂等、不返错。fsync 本身在黑盒面不可观测——这一条钉的是"关完就稳、关完不丢"。
func TestCloseFlushesData(t *testing.T) {
	dir := t.TempDir()
	sid := newSession(t)
	st, err := walfile.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w, _ := st.Writer(sid)
	for i := 0; i < 5; i++ {
		if err := w.Append(core.KindWalTurnDelta, core.WalTurnBeginPayload{Turn: i + 1, Model: "m"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, string(sid)+".wal"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(raw), "\n"); got != 5 {
		t.Errorf("关闭后文件里完整记录 = %d 行，want 5（Close 要把数据刷出去）", got)
	}
}
