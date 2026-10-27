package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// defaultBuffer 是每个 Subscription 事件通道的默认缓冲容量（槽位数）。
const defaultBuffer = 1024

// 总线契约错误（调用方可用 errors.Is 判定）。
var (
	// ErrStreamWithSeq：durable=false 的流事件携带了 seq（违反 F8 / x-invariants：
	// durable=false ⇒ seq 必须缺省）。
	ErrStreamWithSeq = errors.New("bus: durable=false event must not carry seq")
	// ErrStoreCount：store 返回的事件数与输入不符（store 违约）。
	ErrStoreCount = errors.New("bus: store returned wrong event count")
	// ErrStoreMissing：store 未返回某个已提交的事件（store 违约）。
	ErrStoreMissing = errors.New("bus: store did not return an appended event")
	// ErrStoreNoSeq：durable=true 但 store 返回的事件仍无 seq（store 违约）。
	ErrStoreNoSeq = errors.New("bus: store returned durable event without seq")
)

// Filter 描述一个订阅的过滤条件；多个条件之间是 AND，全空等于全收。
type Filter struct {
	// SessionIDs 非空时，只收 session_id 命中其一的事件。
	SessionIDs []contracts.Ulid
	// KindPrefixes 非空时，只收 kind 以任一前缀开头的事件（如 "tool.call."）。
	KindPrefixes []string
	// DurableOnly 为 true 时，只收 durable=true 的事实事件。
	DurableOnly bool
}

// FilterFromRequest 把契约的 SubscribeRequest 转成总线过滤器（K4 远程订阅）。
//
// 映射口径与 contracts/schema/transport.json 的 SubscribeRequest 一一对应：
//
//   - session_id    → Filter.SessionIDs（缺省 = 全部会话）；
//   - kind_prefixes → Filter.KindPrefixes（按 kind 前缀匹配，如 "tool.call."；缺省 = 全部 kind）；
//   - durable_only  → Filter.DurableOnly（缺省 = false = 流事件也收）。
//
// 切片会被复制：调用方之后改写原切片不会影响已建好的过滤器。
func FilterFromRequest(req contracts.SubscribeRequest) Filter {
	f := Filter{}
	if req.SessionID != nil {
		f.SessionIDs = []contracts.Ulid{*req.SessionID}
	}
	if len(req.KindPrefixes) > 0 {
		f.KindPrefixes = append([]string(nil), req.KindPrefixes...)
	}
	if req.DurableOnly != nil {
		f.DurableOnly = *req.DurableOnly
	}
	return f
}

// match 判定事件是否满足过滤条件（AND 语义；全空 Filter 恒为 true）。
func (f Filter) match(ev contracts.Event) bool {
	if f.DurableOnly && !ev.Durable {
		return false
	}
	if len(f.SessionIDs) > 0 && !containsUlid(f.SessionIDs, ev.SessionID) {
		return false
	}
	if len(f.KindPrefixes) > 0 && !hasAnyPrefix(ev.Kind, f.KindPrefixes) {
		return false
	}
	return true
}

func containsUlid(list []contracts.Ulid, id contracts.Ulid) bool {
	for _, x := range list {
		if x == id {
			return true
		}
	}
	return false
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// copyFilter 复制 Filter 的切片，避免订阅之后调用方改写原切片造成数据竞争。
func copyFilter(f Filter) Filter {
	out := Filter{DurableOnly: f.DurableOnly}
	if len(f.SessionIDs) > 0 {
		out.SessionIDs = append([]contracts.Ulid(nil), f.SessionIDs...)
	}
	if len(f.KindPrefixes) > 0 {
		out.KindPrefixes = append([]string(nil), f.KindPrefixes...)
	}
	return out
}

// Bus 是进程内事件总线：把事件分发给匹配的订阅者。
//
// 并发模型：单一 sync.Mutex 保护订阅者集合。Publish 先在锁外向 store 提交 durable
// 事件，再在锁内以非阻塞方式通知订阅者。因此：
//   - 单次 Publish 批内保序，且对同一订阅者的通知在该批内连续、不与其它批交错；
//   - 跨 Publish 的顺序不强保证（seq=提交序≠因果序，G25 在部署层约束 session 亲和写）；
//   - Subscribe / Close / Publish 可被多 goroutine 并发安全地调用。
type Bus struct {
	store core.Store

	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

// New 构造一个绑定到 store 的事件总线。
//
// store 必填：传入 nil 会直接 panic。
//
// nil 语义口径（三处统一，见 core/doc.go 的"nil 语义"一节）：
//   - **编程错误**（本处）：store=nil → 直接 panic。总线没有 store 就无法落库 durable 事件，
//     静默降级只会把故障推迟到更难诊断的地方；
//   - **兜底**（registry.New / registry.NewPolicyAdjudicator）：裁决器缺失 → 换成空引擎 →
//     一切能力被拒（fail-close）。
//
// 判据：参数缺失后系统仍能安全地做"拒绝"→ 兜底；否则 panic。
func New(store core.Store) *Bus {
	if store == nil {
		panic("bus: New requires a non-nil core.Store")
	}
	return &Bus{
		store: store,
		subs:  make(map[*Subscription]struct{}),
	}
}

// Publish 发布一批事件（F8 分流 + S7 透传）：
//
//   - durable=false 的流事件：携带 seq 直接返回 ErrStreamWithSeq（整批不落库、不通知）；
//     否则不调用 store，原样通知订阅者。
//   - durable=true 的事实事件：整批一次性提交 store.Append（Seq 由 store 填）；入库成功
//     后才带最终事件通知订阅者；入库失败则不通知、整批返回 error（事实事件未落地）。
//   - **store 违约**（返回条数不对 / 漏了某个事件 / 事件没有 seq）：已入库的事件仍然是事实，
//     所以**仍然通知已确认（带 seq）的那部分**，再返回错误说明"已入库未通知/部分通知"的条数，
//     调用方据此用 Store.ReadFrom 从自己的 seq 水位补读（事实流不分叉，P2-4）。
//
// # 重试契约（重要）
//
// **批内的流事件（durable=false）不幂等**：它们一旦投递出去，订阅者就收到了，
// 整批重试会让订阅者收到**重复事件**。因此：
//
//   - store 违约时**不要整批重试**——错误消息里会写明"已投递/未通知"的条数；
//   - 只有**未确认的 durable 事件**（已入库但没通知的那几条）可以用 Store.ReadFrom
//     按自己的 seq 水位补读；
//   - 已投递的流事件丢了就是丢了（诚实取舍，见 package doc）。
//
// 去重只看 Event.ID 且由 store 负责（S7）：bus 不去重，同一 ID 重复 Publish 允许透传给
// store。单次 Publish 批内保序，对同一订阅者的通知顺序与入库/输入顺序一致。
func (b *Bus) Publish(ctx context.Context, evs ...contracts.Event) error {
	if len(evs) == 0 {
		return nil
	}

	// 1) 契约校验：durable=false 不得携带 seq（F8）。整批先校验，失败则不落库、不通知。
	for i := range evs {
		if !evs[i].Durable && evs[i].Seq != nil {
			return fmt.Errorf("%w: event %s", ErrStreamWithSeq, evs[i].ID)
		}
	}

	// 2) 收集 durable 事件（保持输入顺序），一次性提交 store。
	var durable []contracts.Event
	for i := range evs {
		if evs[i].Durable {
			durable = append(durable, evs[i])
		}
	}

	finalByID := make(map[contracts.Ulid]contracts.Event, len(durable))
	var violations []error
	if len(durable) > 0 {
		appended, err := b.store.Append(ctx, durable)
		if err != nil {
			// 入库失败：不通知、整批返回 error（事实事件未落地，订阅者不应收到）。
			return fmt.Errorf("bus: store append: %w", err)
		}
		if len(appended) != len(durable) {
			// 违约：条数不符。已返回的那部分仍按"已确认"处理（见步骤 3）。
			violations = append(violations, fmt.Errorf("%w: got %d, want %d", ErrStoreCount, len(appended), len(durable)))
		}
		for _, ev := range appended {
			finalByID[ev.ID] = ev
		}
	}

	// 3) 按输入顺序装配通知列表：durable 用 store 返回的最终事件（带 seq），流事件原样。
	//
	// 违约的事件**跳过但记账**（skipped）：它们已经入库却无法确认（没有 seq / 没返回），
	// 不能通知一条没有 seq 的"事实事件"，否则订阅者的 seq 水位会错位。
	notify := make([]contracts.Event, 0, len(evs))
	skipped := 0
	for i := range evs {
		ev := evs[i]
		if !ev.Durable {
			notify = append(notify, ev)
			continue
		}
		final, ok := finalByID[ev.ID]
		if !ok {
			violations = append(violations, fmt.Errorf("%w: id %s", ErrStoreMissing, ev.ID))
			skipped++
			continue
		}
		if final.Seq == nil {
			// store 违约：durable=true 的事件必须带 seq 才通知。
			violations = append(violations, fmt.Errorf("%w: id %s", ErrStoreNoSeq, ev.ID))
			skipped++
			continue
		}
		notify = append(notify, final)
	}

	// 4) 在锁内非阻塞通知：保证本批通知连续，且不与并发批在同一订阅上交错。
	// 违约也要通知已确认的部分——已入库的事实不会因为 store 违约而消失。
	if len(notify) > 0 {
		b.mu.Lock()
		for sub := range b.subs {
			sub.deliver(notify)
		}
		b.mu.Unlock()
	}

	if len(violations) > 0 {
		// 错误必须同时说清两件事：① 哪些事件已入库却没通知；② **整批重试不安全**
		// （批内流事件已投递，重试会重复投递）——调用方只能按 seq 水位补读未确认的 durable 事件。
		return fmt.Errorf("bus: store 违约：%d 条事件已入库但未通知（已通知 %d 条）；"+
			"⚠ 重试不安全：本批流事件（durable=false）已投递，整批重试会导致重复投递"+
			"（批内流事件不幂等，调用方勿整批重试）；"+
			"仅未确认的 durable 事件可用 Store.ReadFrom 按 seq 水位补读：%w",
			skipped, len(notify), errors.Join(violations...))
	}
	return nil
}

// Subscribe 注册一个订阅并返回 Subscription。Filter 会被复制，之后可安全改写原值。
func (b *Bus) Subscribe(f Filter) *Subscription {
	s := &Subscription{
		bus:    b,
		filter: copyFilter(f),
		ch:     make(chan contracts.Event, defaultBuffer),
	}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

// Subscription 是一个订阅：持有独立的带缓冲事件通道。
type Subscription struct {
	bus    *Bus
	filter Filter
	ch     chan contracts.Event

	// lagging 记录因队列满被丢弃的通知条数（原子访问）。
	lagging uint64
}

// Events 返回只读事件通道。Close 后通道被关闭，接收方以 ok=false 感知结束。
func (s *Subscription) Events() <-chan contracts.Event {
	return s.ch
}

// Lagging 返回自订阅以来因队列满而被丢弃的通知条数。
func (s *Subscription) Lagging() uint64 {
	return atomic.LoadUint64(&s.lagging)
}

// Close 关闭订阅：从总线摘除并关闭通道。重复调用安全（幂等）。
func (s *Subscription) Close() {
	b := s.bus
	b.mu.Lock()
	if _, ok := b.subs[s]; ok {
		delete(b.subs, s)
		close(s.ch)
	}
	b.mu.Unlock()
}

// deliver 非阻塞地把一批事件投递给本订阅（调用方持有 bus.mu）。
//
// 队列满即丢弃该条并 Lagging()+1——诚实的取舍：事实事件（durable）已在库，订阅者可经
// store.ReadFrom 从自己的 seq 水位补读；流事件（durable=false）丢了就是丢了。已 Close 的
// 订阅不会被投递（Close 先在锁内摘除再关通道，二者都在 bus.mu 下，与 deliver 互斥），
// 故绝不会发生向已关闭通道发送。
func (s *Subscription) deliver(evs []contracts.Event) {
	for _, ev := range evs {
		if !s.filter.match(ev) {
			continue
		}
		select {
		case s.ch <- ev:
			// 已入队
		default:
			// 队列满：丢弃并计数
			atomic.AddUint64(&s.lagging, 1)
		}
	}
}
