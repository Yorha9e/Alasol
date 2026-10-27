package durable_test

import (
	"context"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// memStore 是测试用内存 store：分配单调 seq、按 Event.ID 幂等、ReadFrom 按 seq 升序。
// （与 core/loop 测试的同名 helper 同形状；durable_test 独立一份，避免跨包依赖测试代码。）
type memStore struct {
	mu     sync.Mutex
	seq    int64
	byID   map[contracts.Ulid]contracts.Event
	events []contracts.Event
}

func newMemStore() *memStore { return &memStore{byID: make(map[contracts.Ulid]contracts.Event)} }

func (m *memStore) Append(_ context.Context, evs []contracts.Event) ([]contracts.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]contracts.Event, len(evs))
	for i, ev := range evs {
		if existing, ok := m.byID[ev.ID]; ok {
			out[i] = existing // S7 幂等
			continue
		}
		m.seq++
		ev.Seq = ptrI64(m.seq)
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

func (m *memStore) all() []contracts.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]contracts.Event(nil), m.events...)
}

func (m *memStore) countKind(kind string) int {
	n := 0
	for _, ev := range m.all() {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

var _ core.Store = (*memStore)(nil)

func ptrI64(v int64) *int64 { return &v }
func ptrS(s string) *string { return &s }
