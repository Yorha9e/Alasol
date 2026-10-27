package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// ---------------------------------------------------------------------------
// 测试辅助：id / event / drain
// ---------------------------------------------------------------------------

// mkID 生成确定性的 26 字符 ULID 形态字符串（bus 不校验 ULID 格式，只求唯一）。
func mkID(n int) contracts.Ulid {
	return contracts.Ulid(fmt.Sprintf("%026d", n))
}

var (
	sessA = mkID(100)
	sessB = mkID(200)
)

func mkEvent(n int, session contracts.Ulid, kind string, durable bool) contracts.Event {
	return contracts.Event{
		V:         1,
		ID:        mkID(n),
		Kind:      kind,
		TS:        "2024-01-01T00:00:00Z",
		SessionID: session,
		AgentID:   mkID(1),
		Durable:   durable,
	}
}

func ptrInt64(v int64) *int64 { return &v }

// drainNonBlocking 取走通道中当前已缓冲的所有事件。
// Publish 在返回前已同步完成投递（deliver 在 bus.mu 下、非阻塞），故此后通道里的
// 就是全部已投递事件，无需等待。
func drainNonBlocking(sub *Subscription) []contracts.Event {
	var out []contracts.Event
	for {
		select {
		case e := <-sub.Events():
			out = append(out, e)
		default:
			return out
		}
	}
}

// ---------------------------------------------------------------------------
// Store fakes：多个不同实现，证明 bus 只依赖 core.Store 接口（换实现零改动）
// ---------------------------------------------------------------------------

// memStore 是真的内存 store：append + 分配单调 seq + 按 ID 幂等 + ReadFrom。
type memStore struct {
	mu     sync.Mutex
	seq    int64
	byID   map[contracts.Ulid]contracts.Event
	events []contracts.Event // 按 seq 升序
}

func newMemStore() *memStore {
	return &memStore{byID: make(map[contracts.Ulid]contracts.Event)}
}

func (m *memStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]contracts.Event, len(evs))
	for i, ev := range evs {
		if existing, ok := m.byID[ev.ID]; ok {
			out[i] = existing // S7 幂等：已存在的 ID 返回库中原事件，不重复入库
			continue
		}
		m.seq++
		ev.Seq = ptrInt64(m.seq)
		m.byID[ev.ID] = ev
		m.events = append(m.events, ev)
		out[i] = ev
	}
	return out, nil
}

func (m *memStore) ReadFrom(_ context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []contracts.Event
	for _, ev := range m.events {
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

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// failStore 总是失败的 store（第二个不同实现）。
type failStore struct{}

func (failStore) Append(context.Context, []contracts.Event) ([]contracts.Event, error) {
	return nil, errors.New("store: append failed (injected)")
}
func (failStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

// noSeqStore 是违约 store：append 成功但不分配 seq（用于验证 store 违约被 bus 识破）。
type noSeqStore struct{}

func (noSeqStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	out := make([]contracts.Event, len(evs))
	copy(out, evs) // 原样返回，Seq 仍为 nil
	return out, nil
}
func (noSeqStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

// shortStore 是违约 store：返回的事件数少于输入。
type shortStore struct{}

func (shortStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	if len(evs) == 0 {
		return nil, nil
	}
	return evs[:len(evs)-1], nil
}
func (shortStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

// partialSeqStore 是违约 store：条数对，但只给第一条分配 seq（其余没有 seq）。
// 用于验证"违约时仍通知已确认的那部分"（P2-4）。
type partialSeqStore struct{}

func (partialSeqStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	out := make([]contracts.Event, len(evs))
	copy(out, evs)
	for i := range out {
		if i == 0 {
			out[i].Seq = ptrInt64(int64(i + 1))
		}
	}
	return out, nil
}
func (partialSeqStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

// 编译期证明：以上均为 core.Store 的不同实现。
var (
	_ core.Store = (*memStore)(nil)
	_ core.Store = failStore{}
	_ core.Store = noSeqStore{}
	_ core.Store = shortStore{}
	_ core.Store = partialSeqStore{}
)

// ---------------------------------------------------------------------------
// 构造与空批
// ---------------------------------------------------------------------------

func TestNewNilStorePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(nil) 应 panic")
		}
	}()
	New(nil)
}

func TestPublishEmptyBatchIsNoop(t *testing.T) {
	b := New(newMemStore())
	if err := b.Publish(context.Background()); err != nil {
		t.Fatalf("空批应为 no-op，得到 %v", err)
	}
}

// ---------------------------------------------------------------------------
// F8 三态
// ---------------------------------------------------------------------------

// 态①：durable=false 携带 seq → 报错，整批不落库、不通知。
func TestF8StreamWithSeqRejected(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})

	bad := mkEvent(1, sessA, "model.delta", false)
	bad.Seq = ptrInt64(1)
	if err := b.Publish(context.Background(), bad); !errors.Is(err, ErrStreamWithSeq) {
		t.Fatalf("期望 ErrStreamWithSeq，得到 %v", err)
	}
	if st.count() != 0 {
		t.Fatalf("流事件不得入库，count=%d", st.count())
	}
	if got := drainNonBlocking(sub); len(got) != 0 {
		t.Fatalf("被拒的批不得通知，得到 %d 条", len(got))
	}
}

// 态②：durable=false 不带 seq → 不进 store、不占 seq，直接通知订阅者。
func TestF8StreamSkipsStore(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})

	e := mkEvent(1, sessA, "model.delta", false)
	if err := b.Publish(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if st.count() != 0 {
		t.Fatalf("流事件不得入库，count=%d", st.count())
	}
	got := drainNonBlocking(sub)
	if len(got) != 1 {
		t.Fatalf("期望 1 条通知，得到 %d", len(got))
	}
	if got[0].Seq != nil {
		t.Fatalf("流事件不得携带 seq，得到 %d", *got[0].Seq)
	}
	if got[0].ID != e.ID {
		t.Fatalf("事件 id 不符：得到 %s 期望 %s", got[0].ID, e.ID)
	}
}

// 态③：durable=true → 入库，且入库成功后才通知、通知带 seq。
func TestF8DurableGoesToStoreWithSeq(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})

	e := mkEvent(1, sessA, "tool.call.intent", true)
	if err := b.Publish(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if st.count() != 1 {
		t.Fatalf("事实事件必须入库，count=%d", st.count())
	}
	got := drainNonBlocking(sub)
	if len(got) != 1 {
		t.Fatalf("期望 1 条通知，得到 %d", len(got))
	}
	if got[0].Seq == nil {
		t.Fatal("事实事件通知必须携带 seq")
	}
}

// 入库失败：不通知、返回 error。
func TestStoreFailureNoNotify(t *testing.T) {
	b := New(failStore{})
	sub := b.Subscribe(Filter{})

	e := mkEvent(1, sessA, "tool.call.intent", true)
	if err := b.Publish(context.Background(), e); err == nil {
		t.Fatal("store 失败时期望返回 error")
	}
	if got := drainNonBlocking(sub); len(got) != 0 {
		t.Fatalf("store 失败不得通知，得到 %d 条", len(got))
	}
}

// store 违约①：durable=true 但 store 未返回 seq → 报错、不通知。
func TestStoreViolationNoSeq(t *testing.T) {
	b := New(noSeqStore{})
	sub := b.Subscribe(Filter{})

	e := mkEvent(1, sessA, "tool.call.intent", true)
	if err := b.Publish(context.Background(), e); !errors.Is(err, ErrStoreNoSeq) {
		t.Fatalf("期望 ErrStoreNoSeq，得到 %v", err)
	}
	if got := drainNonBlocking(sub); len(got) != 0 {
		t.Fatalf("store 违约不得通知，得到 %d 条", len(got))
	}
}

// store 违约②：返回事件数与输入不符 → 报错。
func TestStoreViolationWrongCount(t *testing.T) {
	b := New(shortStore{})
	err := b.Publish(context.Background(),
		mkEvent(1, sessA, "k", true),
		mkEvent(2, sessA, "k", true),
	)
	if !errors.Is(err, ErrStoreCount) {
		t.Fatalf("期望 ErrStoreCount，得到 %v", err)
	}
}

// P2-4（事实流不分叉）：store 违约时**仍通知已确认（带 seq）的事件**，
// 错误里写明"已入库未通知/部分通知"的条数，调用方据此用 Store.ReadFrom 补读。
func TestStoreViolationStillNotifiesConfirmedEvents(t *testing.T) {
	b := New(partialSeqStore{})
	sub := b.Subscribe(Filter{})

	err := b.Publish(context.Background(),
		mkEvent(1, sessA, "tool.call.intent", true),
		mkEvent(2, sessA, "tool.call.intent", true),
	)
	if err == nil {
		t.Fatal("store 违约必须返回 error")
	}
	if !errors.Is(err, ErrStoreNoSeq) {
		t.Fatalf("期望 ErrStoreNoSeq，得到 %v", err)
	}
	// 已确认的那条（带 seq）必须被通知——它已经入库，是事实。
	got := drainNonBlocking(sub)
	if len(got) != 1 {
		t.Fatalf("期望通知 1 条已确认事件，得到 %d 条", len(got))
	}
	if got[0].ID != mkID(1) {
		t.Fatalf("通知的事件 id=%s, want %s", got[0].ID, mkID(1))
	}
	if got[0].Seq == nil {
		t.Fatal("已确认的事实事件通知必须带 seq")
	}
	// 错误必须写明未通知/已通知的条数，调用方才知道要补读。
	msg := err.Error()
	if !strings.Contains(msg, "未通知") || !strings.Contains(msg, "1") {
		t.Fatalf("错误信息需写明已入库未通知的条数：%v", err)
	}
	if !strings.Contains(msg, "ReadFrom") {
		t.Fatalf("错误信息需提示调用方补读（Store.ReadFrom）：%v", err)
	}
	// N3：错误必须警告"整批重试会重复投递"（批内流事件不幂等）。
	for _, want := range []string{"已投递", "整批重试", "重复投递", "勿整批重试"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误信息需含重试警告 %q：%v", want, err)
		}
	}
}

// N3：批内流事件已投递 → 错误必须警告整批重试会重复投递（流事件不幂等）。
func TestStoreViolationErrorWarnsAgainstWholeBatchRetry(t *testing.T) {
	b := New(partialSeqStore{})
	sub := b.Subscribe(Filter{})

	// 一条流事件（已投递）+ 两条事实事件（其一违约）。
	err := b.Publish(context.Background(),
		mkEvent(1, sessA, "model.delta", false),
		mkEvent(2, sessA, "tool.call.intent", true),
		mkEvent(3, sessA, "tool.call.intent", true),
	)
	if err == nil {
		t.Fatal("store 违约必须返回 error")
	}
	msg := err.Error()
	for _, want := range []string{
		"本批流事件（durable=false）已投递",
		"整批重试会导致重复投递",
		"仅未确认的 durable 事件可用 Store.ReadFrom 按 seq 水位补读",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误信息需含 %q：%v", want, err)
		}
	}
	// 流事件确实已经投递出去了（所以"重试"才危险）。
	got := drainNonBlocking(sub)
	if len(got) != 2 {
		t.Fatalf("期望 2 条通知（1 流 + 1 已确认事实），得到 %d 条", len(got))
	}
	if got[0].Kind != "model.delta" {
		t.Fatalf("第 1 条应是流事件：%+v", got[0])
	}
}

// store 违约时流事件照常通知（流事件不入库，不受 store 违约影响）。
func TestStoreViolationStillNotifiesStreamEvents(t *testing.T) {
	b := New(partialSeqStore{})
	sub := b.Subscribe(Filter{})

	err := b.Publish(context.Background(),
		mkEvent(1, sessA, "model.delta", false),
		mkEvent(2, sessA, "tool.call.intent", true),
		mkEvent(3, sessA, "tool.call.intent", true),
	)
	if err == nil {
		t.Fatal("store 违约必须返回 error")
	}
	got := drainNonBlocking(sub)
	if len(got) != 2 {
		t.Fatalf("期望通知 2 条（1 流 + 1 已确认事实），得到 %d 条", len(got))
	}
	if got[0].Kind != "model.delta" || got[0].Seq != nil {
		t.Fatalf("流事件通知不应带 seq：%+v", got[0])
	}
	if got[1].Seq == nil {
		t.Fatal("已确认的事实事件通知必须带 seq")
	}
}

// ---------------------------------------------------------------------------
// 批内保序（含 durable/stream 混合）
// ---------------------------------------------------------------------------

func TestPublishBatchOrder(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})

	batch := []contracts.Event{
		mkEvent(1, sessA, "k.1", true),
		mkEvent(2, sessA, "k.2", true),
		mkEvent(3, sessA, "k.3", true),
	}
	if err := b.Publish(context.Background(), batch...); err != nil {
		t.Fatal(err)
	}
	got := drainNonBlocking(sub)
	if len(got) != len(batch) {
		t.Fatalf("期望 %d 条，得到 %d", len(batch), len(got))
	}
	for i := range batch {
		if got[i].ID != batch[i].ID {
			t.Fatalf("第 %d 个顺序错乱：得到 %s 期望 %s", i, got[i].ID, batch[i].ID)
		}
		if got[i].Seq == nil {
			t.Fatalf("第 %d 个事件缺 seq", i)
		}
		if i > 0 && *got[i].Seq <= *got[i-1].Seq {
			t.Fatalf("第 %d 个 seq 未递增", i)
		}
	}
}

func TestPublishMixedBatchOrder(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})

	batch := []contracts.Event{
		mkEvent(1, sessA, "k.1", true),  // durable
		mkEvent(2, sessA, "k.2", false), // stream
		mkEvent(3, sessA, "k.3", true),  // durable
	}
	if err := b.Publish(context.Background(), batch...); err != nil {
		t.Fatal(err)
	}
	got := drainNonBlocking(sub)
	if len(got) != 3 {
		t.Fatalf("期望 3 条，得到 %d", len(got))
	}
	for i := range batch {
		if got[i].ID != batch[i].ID {
			t.Fatalf("第 %d 个顺序错乱", i)
		}
	}
	if got[0].Seq == nil || got[2].Seq == nil {
		t.Fatal("durable 事件必须带 seq")
	}
	if got[1].Seq != nil {
		t.Fatal("stream 事件不得带 seq")
	}
	if st.count() != 2 {
		t.Fatalf("只应有 2 条 durable 入库，得到 %d", st.count())
	}
}

// ---------------------------------------------------------------------------
// S7 幂等：store 去重，bus 不去重
// ---------------------------------------------------------------------------

func TestIdempotencyStoreDedupBusPassthrough(t *testing.T) {
	st := newMemStore()
	b := New(st)
	sub := b.Subscribe(Filter{})
	ctx := context.Background()

	e := mkEvent(1, sessA, "tool.call.intent", true)
	if err := b.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, e); err != nil { // 同一 ID 重复发布
		t.Fatal(err)
	}
	if st.count() != 1 {
		t.Fatalf("store 必须按 ID 去重，count=%d", st.count())
	}
	got := drainNonBlocking(sub)
	if len(got) != 2 {
		t.Fatalf("bus 不得去重，期望 2 条通知，得到 %d", len(got))
	}
	if got[0].Seq == nil || got[1].Seq == nil || *got[0].Seq != *got[1].Seq {
		t.Fatalf("两次通知应带同一提交 seq：%+v", got)
	}
}

// ---------------------------------------------------------------------------
// 过滤器
// ---------------------------------------------------------------------------

func TestFilterSessionID(t *testing.T) {
	b := New(newMemStore())
	subA := b.Subscribe(Filter{SessionIDs: []contracts.Ulid{sessA}})
	subAll := b.Subscribe(Filter{})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "k", true))
	_ = b.Publish(ctx, mkEvent(2, sessB, "k", true))

	gotA := drainNonBlocking(subA)
	if len(gotA) != 1 || gotA[0].SessionID != sessA {
		t.Fatalf("session 过滤错误：%+v", gotA)
	}
	if got := drainNonBlocking(subAll); len(got) != 2 {
		t.Fatalf("空 filter 应收全部，得到 %d", len(got))
	}
}

func TestFilterKindPrefix(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{KindPrefixes: []string{"tool.call."}})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "tool.call.intent", true))
	_ = b.Publish(ctx, mkEvent(2, sessA, "tool.result", true))
	_ = b.Publish(ctx, mkEvent(3, sessA, "model.delta", false))

	got := drainNonBlocking(sub)
	if len(got) != 1 || got[0].Kind != "tool.call.intent" {
		t.Fatalf("kind 前缀过滤错误：%+v", got)
	}
}

func TestFilterDurableOnly(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{DurableOnly: true})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "k", true))
	_ = b.Publish(ctx, mkEvent(2, sessA, "k", false))

	got := drainNonBlocking(sub)
	if len(got) != 1 || !got[0].Durable {
		t.Fatalf("durableOnly 过滤错误：%+v", got)
	}
}

func TestFilterAndCombination(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{
		SessionIDs:   []contracts.Ulid{sessA},
		KindPrefixes: []string{"tool."},
		DurableOnly:  true,
	})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "tool.call.intent", true))  // 命中
	_ = b.Publish(ctx, mkEvent(2, sessB, "tool.call.intent", true))  // session 不符
	_ = b.Publish(ctx, mkEvent(3, sessA, "model.delta", true))       // kind 不符
	_ = b.Publish(ctx, mkEvent(4, sessA, "tool.call.intent", false)) // 非 durable

	got := drainNonBlocking(sub)
	if len(got) != 1 || got[0].ID != mkID(1) {
		t.Fatalf("AND 组合过滤错误：%+v", got)
	}
}

func TestFilterEmptyReceivesAll(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{})
	ctx := context.Background()

	_ = b.Publish(ctx,
		mkEvent(1, sessA, "a", true),
		mkEvent(2, sessB, "b", false),
	)
	if got := drainNonBlocking(sub); len(got) != 2 {
		t.Fatalf("空 filter 应收全部，得到 %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// FilterFromRequest：契约 → 总线过滤器（K4 远程订阅）
// ---------------------------------------------------------------------------

func TestFilterFromRequest(t *testing.T) {
	// 全空请求 → 全收。
	f := FilterFromRequest(contracts.SubscribeRequest{})
	if len(f.SessionIDs) != 0 || len(f.KindPrefixes) != 0 || f.DurableOnly {
		t.Fatalf("空请求应得到空过滤器: %+v", f)
	}

	// 完整请求 → 一一对应。
	sid := sessA
	yes := true
	f = FilterFromRequest(contracts.SubscribeRequest{
		SessionID:    &sid,
		KindPrefixes: []string{"tool.call."},
		DurableOnly:  &yes,
	})
	if len(f.SessionIDs) != 1 || f.SessionIDs[0] != sid {
		t.Fatalf("SessionIDs=%v, want [%s]", f.SessionIDs, sid)
	}
	if len(f.KindPrefixes) != 1 || f.KindPrefixes[0] != "tool.call." {
		t.Fatalf("KindPrefixes=%v, want [tool.call.]", f.KindPrefixes)
	}
	if !f.DurableOnly {
		t.Fatalf("DurableOnly=false, want true")
	}

	// 转换出来的过滤器真的按 kind 前缀过滤。
	b := New(newMemStore())
	sub := b.Subscribe(f)
	_ = b.Publish(context.Background(),
		mkEvent(1, sessA, "tool.call.intent", true),
		mkEvent(2, sessB, "tool.call.intent", true),
		mkEvent(3, sessA, "model.delta", true),
	)
	got := drainNonBlocking(sub)
	if len(got) != 1 || got[0].ID != mkID(1) {
		t.Fatalf("过滤结果=%+v, want 只收第 1 条", got)
	}

	// 深拷贝：调用方改写原切片不得影响过滤器。
	req := contracts.SubscribeRequest{KindPrefixes: []string{"tool."}}
	f2 := FilterFromRequest(req)
	req.KindPrefixes[0] = "HACKED"
	if f2.KindPrefixes[0] != "tool." {
		t.Fatalf("过滤器不是快照: %v", f2.KindPrefixes)
	}
}

// ---------------------------------------------------------------------------
// 多订阅者
// ---------------------------------------------------------------------------

func TestMultipleSubscribers(t *testing.T) {
	b := New(newMemStore())
	durable := b.Subscribe(Filter{DurableOnly: true})
	noDurableFilter := b.Subscribe(Filter{DurableOnly: false}) // false 不过滤 → 全收
	toolSub := b.Subscribe(Filter{KindPrefixes: []string{"tool."}})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "tool.call.intent", true))
	_ = b.Publish(ctx, mkEvent(2, sessA, "model.delta", false))

	if got := drainNonBlocking(durable); len(got) != 1 || got[0].ID != mkID(1) {
		t.Fatalf("durable 订阅错误：%+v", got)
	}
	if got := drainNonBlocking(noDurableFilter); len(got) != 2 {
		t.Fatalf("DurableOnly=false 不应过滤，应收 2，得到 %d", len(got))
	}
	if got := drainNonBlocking(toolSub); len(got) != 1 || got[0].ID != mkID(1) {
		t.Fatalf("tool 订阅错误：%+v", got)
	}
}

// ---------------------------------------------------------------------------
// 队列满丢弃 + Lagging 计数
// ---------------------------------------------------------------------------

func TestSubscriptionQueueFullLagging(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{})
	ctx := context.Background()

	const extra = 7
	total := defaultBuffer + extra
	batch := make([]contracts.Event, total)
	for i := 0; i < total; i++ {
		batch[i] = mkEvent(1000+i, sessA, "k", false) // 流事件，避免 store 参与
	}
	if err := b.Publish(ctx, batch...); err != nil {
		t.Fatal(err)
	}

	if l := sub.Lagging(); l != extra {
		t.Fatalf("期望 Lagging=%d，得到 %d", extra, l)
	}
	got := drainNonBlocking(sub)
	if len(got) != defaultBuffer {
		t.Fatalf("缓冲应容纳 %d，得到 %d", defaultBuffer, len(got))
	}
	// 保序：留下的应是前 defaultBuffer 条
	for i := 0; i < defaultBuffer; i++ {
		if got[i].ID != mkID(1000+i) {
			t.Fatalf("第 %d 个顺序错乱", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestCloseStopsDeliveryAndIdempotent(t *testing.T) {
	b := New(newMemStore())
	sub := b.Subscribe(Filter{})
	ctx := context.Background()

	_ = b.Publish(ctx, mkEvent(1, sessA, "k", false))
	if got := drainNonBlocking(sub); len(got) != 1 {
		t.Fatalf("关闭前期望 1 条，得到 %d", len(got))
	}

	sub.Close()
	sub.Close() // 幂等，不得 panic

	// 关闭后再发布：不得投递、不得 panic
	if err := b.Publish(ctx, mkEvent(2, sessA, "k", false)); err != nil {
		t.Fatal(err)
	}
	// 通道已关闭：接收返回 ok=false，且收不到关闭后发布的事件
	if _, ok := <-sub.Events(); ok {
		t.Fatal("已关闭订阅不得再收到事件")
	}
}

// Close 与 Publish 并发竞争：不得发生向已关闭通道发送（panic），-race 下验证无数据竞争。
func TestCloseDuringPublishNoPanic(t *testing.T) {
	b := New(newMemStore())
	ctx := context.Background()

	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = b.Publish(ctx, mkEvent(i, sessA, "k", false))
		}
	}()

	for i := 0; i < 300; i++ {
		sub := b.Subscribe(Filter{})
		sub.Close()
	}
	close(stop)
	pubWG.Wait()
}

// ---------------------------------------------------------------------------
// 并发 Publish + Subscribe + Close（-race）
// ---------------------------------------------------------------------------

func TestConcurrentPublishSubscribeClose(t *testing.T) {
	st := newMemStore()
	b := New(st)
	ctx := context.Background()

	var wg sync.WaitGroup

	// 发布者：混合 durable 与 stream。
	for p := 0; p < 6; p++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				n := seed*1000000 + i
				durable := i%2 == 0
				kind := "model.delta"
				if durable {
					kind = "tool.call.intent"
				}
				_ = b.Publish(ctx, mkEvent(n, sessA, kind, durable))
			}
		}(p)
	}

	// 订阅者 churn：不断 Subscribe → 后台 drain → Close。
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				sub := b.Subscribe(Filter{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					for range sub.Events() {
					}
				}()
				sub.Close()
				<-done
			}
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// 换 Store 实现，bus 零改动
// ---------------------------------------------------------------------------

func TestBusIsStoreAgnostic(t *testing.T) {
	ctx := context.Background()

	// 真 store：durable 入库并带 seq 通知。
	bReal := New(newMemStore())
	subReal := bReal.Subscribe(Filter{})
	if err := bReal.Publish(ctx, mkEvent(1, sessA, "k", true)); err != nil {
		t.Fatalf("memStore：%v", err)
	}
	gotReal := drainNonBlocking(subReal)
	if len(gotReal) != 1 || gotReal[0].Seq == nil {
		t.Fatalf("memStore：期望 1 条带 seq 的事实事件，得到 %+v", gotReal)
	}

	// 失败 store：同一段 bus 代码，durable 入库失败 → 不通知、返回 error。
	bFail := New(failStore{})
	subFail := bFail.Subscribe(Filter{})
	if err := bFail.Publish(ctx, mkEvent(1, sessA, "k", true)); err == nil {
		t.Fatal("failStore：期望返回 error")
	}
	if got := drainNonBlocking(subFail); len(got) != 0 {
		t.Fatalf("failStore：期望无投递，得到 %+v", got)
	}
}
