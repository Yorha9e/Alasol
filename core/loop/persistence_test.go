package loop_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/policy"
	"github.com/Yorha9e/alasol/core/registry"
)

// ---------------------------------------------------------------------------
// 文件后端 store（JSONL + flush/fsync）：验证"关闭再打开后事件不丢、seq 不重用"
// ---------------------------------------------------------------------------

// fileStore 把事件追加进 JSONL 文件并 fsync（崩溃不丢），支持重新打开。
type fileStore struct {
	mu   sync.Mutex
	path string
	f    *os.File
	seq  int64

	byID   map[contracts.Ulid]contracts.Event
	events []contracts.Event
	log    *orderLog
}

func newFileStore(t *testing.T, path string) *fileStore {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("打开事件文件: %v", err)
	}
	return &fileStore{path: path, f: f, byID: make(map[contracts.Ulid]contracts.Event)}
}

func (s *fileStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]contracts.Event, len(evs))
	for i, ev := range evs {
		if existing, ok := s.byID[ev.ID]; ok {
			out[i] = existing // S7：去重只看 Event.ID
			continue
		}
		s.seq++
		ev.Seq = ptrInt64(s.seq)
		line, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		if _, err := s.f.Write(append(line, '\n')); err != nil {
			return nil, err
		}
		// 落盘 + fsync：写到内核缓冲区还不够，断电/崩溃也不能丢事实事件。
		if err := s.f.Sync(); err != nil {
			return nil, err
		}
		s.byID[ev.ID] = ev
		s.events = append(s.events, ev)
		if s.log != nil {
			if label := storeLabel(ev); label != "" {
				s.log.add(label)
			}
		}
		out[i] = ev
	}
	return out, nil
}

func (s *fileStore) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []contracts.Event
	for _, ev := range s.events {
		if ev.Seq == nil || *ev.Seq < fromSeq {
			continue
		}
		if sessionID != nil && ev.SessionID != *sessionID {
			continue
		}
		out = append(out, ev)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *fileStore) all() []contracts.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]contracts.Event(nil), s.events...)
}

func (s *fileStore) close(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			t.Fatalf("关闭事件文件: %v", err)
		}
		s.f = nil
	}
}

// reopenFileStore 模拟"进程重启"：从 JSONL 文件把事件流 fold 回内存，
// seq 水位取文件里的最大值（重启后继续分配，不重用）。
func reopenFileStore(t *testing.T, path string) *fileStore {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读事件文件: %v", err)
	}
	s := &fileStore{path: path, byID: make(map[contracts.Ulid]contracts.Event)}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev contracts.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("事件行解不开（%q）: %v", line, err)
		}
		if _, dup := s.byID[ev.ID]; dup {
			continue
		}
		s.byID[ev.ID] = ev
		s.events = append(s.events, ev)
		if ev.Seq != nil && *ev.Seq > s.seq {
			s.seq = *ev.Seq
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("重新打开事件文件: %v", err)
	}
	s.f = f
	return s
}

var _ core.Store = (*fileStore)(nil)

// newHarnessWithStore 用自定义 store 造一套 harness（文件后端/并发测试用）。
func newHarnessWithStore(t *testing.T, store core.Store, all func() []contracts.Event) *harness {
	t.Helper()
	eng := policy.New(allowRule("echo"))
	h := &harness{
		t:        t,
		reg:      registry.New(registry.NewPolicyAdjudicator(eng)),
		engine:   eng,
		log:      newOrderLog(),
		provider: &mockProvider{},
		execs:    make(map[string]*mockExecutor),
		session:  core.NewUlid(),
		agent:    core.NewUlid(),
		eventsFn: all,
	}
	h.bus = bus.New(store)
	if fs, ok := store.(*fileStore); ok {
		fs.log = h.log
	}
	return h
}

func TestReopenPreservesEventsAndContinuesSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	// --- 第一次运行：落 4 条事件（两轮各一条 context.assembled + intent + completed）---
	fs := newFileStore(t, path)
	h := newHarnessWithStore(t, fs, fs.all)
	h.addTool("echo", func(tool *contracts.Tool) {
		tool.Idempotent = true
		tool.Replay = contracts.ReplayKindSafe
	})
	h.script(stepTools(call("c1", "echo", `{"text":"before"}`)), stepText("第一次完成"))
	res, err := h.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Events != 4 || res.ToolCalls != 1 {
		t.Fatalf("Result 不符: %+v", res)
	}
	fs.close(t) // 模拟进程退出

	// --- 重新打开：事件必须一条不少、seq 保序、payload 完好 ---
	fs2 := reopenFileStore(t, path)
	evs := writePath(fs2.all())
	if len(evs) != 2 {
		t.Fatalf("重启后应有 2 条写路径事件，得到 %d", len(evs))
	}
	if got, want := []string{evs[0].Kind, evs[1].Kind}, []string{loop.KindToolCallIntent, loop.KindToolCallCompleted}; !equalStrings(got, want) {
		t.Fatalf("重启后事件序 = %v，want %v", got, want)
	}
	if evs[0].Seq == nil || *evs[0].Seq != 4 || evs[1].Seq == nil || *evs[1].Seq != 5 {
		t.Fatalf("重启后 seq 应保序（Run 先落 user.prompt，每轮先落 context.assembled + model.turn，故 intent/completed 占 seq 4/5）: %v %v", *evs[0].Seq, *evs[1].Seq)
	}
	in := decodeIntent(t, evs[0])
	if string(in.ToolCallID) != "c1" || string(in.Arguments) != `{"text":"before"}` {
		t.Fatalf("重启后 intent 载荷不完整: %+v", in)
	}
	if !in.Idempotent || in.Replay != contracts.ReplayKindSafe {
		t.Fatalf("重启后 intent 的幂等/重放标记丢失: %+v", in)
	}
	if evs[0].SessionID != h.session || evs[1].CausationID == nil || *evs[1].CausationID != evs[0].ID {
		t.Fatalf("重启后事件归属/因果指针丢失: %+v %+v", evs[0], evs[1])
	}

	// --- 重启后继续跑：seq 不重用，事件接在后面 ---
	h2 := newHarnessWithStore(t, fs2, fs2.all)
	h2.addTool("echo", nil)
	h2.script(stepTools(call("c2", "echo", `{"text":"after"}`)), stepText("第二次完成"))
	res2, err := h2.loop(nil).Run(context.Background(), "p")
	if err != nil {
		t.Fatalf("重启后 Run: %v", err)
	}
	if res2.Events != 4 {
		t.Fatalf("重启后 Result 不符: %+v", res2)
	}
	evs = fs2.all()
	if len(evs) != 14 {
		t.Fatalf("重启后总事件数 = %d，want 14", len(evs))
	}
	for i, want := range []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14} {
		if evs[i].Seq == nil || *evs[i].Seq != want {
			t.Fatalf("第 %d 条事件 seq = %v，want %d（不得重用）", i, evs[i].Seq, want)
		}
	}
	// 第二次的 intent/completed 仍是一对，id 原样（seq 11/12：Run 先落 user.prompt，每轮先落 context.assembled + model.turn）。
	last := writePath(evs)[2:]
	if got := string(decodeIntent(t, last[0]).ToolCallID); got != "c2" {
		t.Fatalf("重启后 intent id = %q", got)
	}
	if got := string(decodeCompleted(t, last[1]).ToolCallID); got != "c2" {
		t.Fatalf("重启后 completed id = %q", got)
	}
	fs2.close(t)
}

// ---------------------------------------------------------------------------
// 并发：多个 Loop 同时往同一条事件流写（无丢失、无 seq 重用、无 id 碰撞）
// ---------------------------------------------------------------------------

func TestConcurrentLoopsNoOversellOrLoss(t *testing.T) {
	const (
		goroutines = 8
		perLoop    = 2 // 每个 Loop 跑 2 次 Run
	)
	h := newHarness(t, allowRule("echo"))
	h.addTool("echo", nil)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			fail := func(format string, args ...any) {
				mu.Lock()
				errs = append(errs, fmt.Errorf("goroutine %d: %s", g, fmt.Sprintf(format, args...)))
				mu.Unlock()
			}
			// 每个 goroutine 自己的 provider 与自己的 Loop（共享 bus/store/registry）。
			prov := &mockProvider{}
			l, err := loop.New(loop.Config{
				Provider:  prov,
				Bus:       h.bus,
				Registry:  h.reg,
				Policy:    h.engine,
				SessionID: h.session,
				AgentID:   h.agent,
				Model:     "m",
			})
			if err != nil {
				fail("loop.New: %v", err)
				return
			}
			for i := 0; i < perLoop; i++ {
				id := fmt.Sprintf("g%d-r%d", g, i)
				prov.steps = []scriptStep{
					stepTools(call(id, "echo", `{"text":"x"}`)),
					stepText("done"),
				}
				prov.reset()
				res, err := l.Run(context.Background(), "p")
				if err != nil {
					fail("Run: %v", err)
					return
				}
				if res.Events != 4 || res.ToolCalls != 1 || res.Turns != 2 {
					fail("Result = %+v，want 4 events / 1 toolcall / 2 turns", res)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(errs) > 0 {
		t.Fatalf("并发 Run 出错: %v", errs[0])
	}

	want := goroutines * perLoop * 7 // 每次 Run：user.prompt + 两轮各 context.assembled/model.turn + intent + completed
	evs := h.events()
	if len(evs) != want {
		t.Fatalf("事件数 = %d，want %d（无丢失）", len(evs), want)
	}
	seenID := make(map[contracts.Ulid]bool, want)
	seenSeq := make(map[int64]bool, want)
	for _, ev := range evs {
		if seenID[ev.ID] {
			t.Fatalf("Event.ID 碰撞: %s", ev.ID)
		}
		seenID[ev.ID] = true
		if ev.Seq == nil {
			t.Fatalf("事件缺 seq: %+v", ev)
		}
		if seenSeq[*ev.Seq] {
			t.Fatalf("seq 重用: %d", *ev.Seq)
		}
		seenSeq[*ev.Seq] = true
	}
	// seq 恰好覆盖 1..want（每个事件一个号，无空洞无重用）。
	for i := int64(1); i <= int64(want); i++ {
		if !seenSeq[i] {
			t.Fatalf("seq %d 缺失（写路径有事件没落到 store）", i)
		}
	}
}

// TestConcurrentExecutorsShareRegistry 证明注册表的并发读 + 执行器并发调用不互相踩。
func TestConcurrentExecutorsShareRegistry(t *testing.T) {
	h := newHarness(t, allowRule("echo"))
	exec := h.addTool("echo", nil)

	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			prov := &mockProvider{steps: []scriptStep{
				stepTools(call(fmt.Sprintf("c%d", g), "echo", `{}`)),
				stepText("ok"),
			}}
			l, err := loop.New(loop.Config{
				Provider:  prov,
				Bus:       h.bus,
				Registry:  h.reg,
				Policy:    h.engine,
				SessionID: h.session,
				AgentID:   h.agent,
				Model:     "m",
			})
			if err != nil {
				t.Errorf("loop.New: %v", err)
				return
			}
			if _, err := l.Run(context.Background(), "p"); err != nil {
				t.Errorf("Run: %v", err)
			}
		}(g)
	}
	wg.Wait()

	if n := exec.callN(); n != 6 {
		t.Fatalf("执行器应被调用 6 次，得到 %d", n)
	}
	if n := len(h.events()); n != 42 {
		t.Fatalf("应有 42 条事件（6 个 loop × 每次 Run：user.prompt + 两轮 context.assembled/model.turn + intent + completed），得到 %d", n)
	}
}
