package indexer

// 内部测试（P1-1）：乱序重放不改终态。
//
// 为什么是内部测试：要验证「同一组事件按不同顺序应用后终态一致」，必须能**直接**
// 调用投影写入入口（applyBoard）并自己控制 seq —— 走 Step 的话事件总是按 seq 升序
// 应用（store 的 ReadFrom 保证），测不到乱序。

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Yorha9e/alasol/contracts"

	// 内部测试自己开一个内存 SQLite 库（驱动注册一次即可）。
	_ "github.com/ncruces/go-sqlite3/driver"
)

var (
	internalSession = contracts.Ulid("MKYTB8D7445Y81ENY5NKBN0JJ6")
	internalAgent   = contracts.Ulid("TEECY7V57SP0J9WZ620EHHP02M")
	internalDBSeq   uint64
)

// openInternalDB 开一个**进程内私有**的内存 SQLite 库（每个测试一个名字 → 互不串库）。
func openInternalDB(t *testing.T) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("indexer-internal-%s-%d",
		strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()),
		atomic.AddUint64(&internalDBSeq, 1))
	db, err := sql.Open("sqlite3", "file:"+name+"?mode=memory&cache=shared&_pragma=busy_timeout%3d5000&_txlock=immediate")
	if err != nil {
		t.Fatalf("open internal db: %v", err)
	}
	// 单连接：共享缓存下多连接会撞表锁（busy_timeout 不覆盖 shared-cache 的表锁）。
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// emptyStore 是空事件源（内部测试只调投影函数，不需要事件流）。
type emptyStore struct{}

func (emptyStore) Append(context.Context, []contracts.Event) ([]contracts.Event, error) {
	return nil, nil
}
func (emptyStore) ReadFrom(context.Context, *contracts.Ulid, int64, int) ([]contracts.Event, error) {
	return nil, nil
}

// boardEvent 造一条带指定 seq 的 board.written 事件。
func boardEvent(seq int64, key, value string, tombstone bool) contracts.Event {
	p := contracts.BoardWrittenPayload{Key: key, Value: json.RawMessage(value)}
	if tombstone {
		tr := true
		p.Tombstone = &tr
	}
	b, _ := json.Marshal(p)
	s := seq
	return contracts.Event{
		V: 1, ID: contracts.Ulid(fmt.Sprintf("%026d", seq)), Kind: kindBoardWritten,
		TS: "2025-03-01T10:00:00.000Z", SessionID: internalSession, AgentID: internalAgent,
		Durable: true, Seq: &s, Payload: b,
	}
}

// readBoardRow 读回 board_state 的一行（value / tombstone / updated_seq）。
func readBoardRow(t *testing.T, db *sql.DB, key string) (value string, tombstone int, seq int64) {
	t.Helper()
	var v sql.NullString
	err := db.QueryRow(`SELECT value, tombstone, updated_seq FROM board_state WHERE key = ?`, key).
		Scan(&v, &tombstone, &seq)
	if err != nil {
		t.Fatalf("read board row %q: %v", key, err)
	}
	return v.String, tombstone, seq
}

// TestApplyBoardOutOfOrderTerminalState：同一组事件按不同顺序（含重复）应用，
// 终态必须一致——旧墓碑不能删新值、旧值不能复活已删的键（P1-1）。
func TestApplyBoardOutOfOrderTerminalState(t *testing.T) {
	scenarios := []struct {
		name string
		// events 是同一组事件（seq 决定真实先后）。
		events []contracts.Event
		// wantValue / wantTombstone 是终态（墓碑行 = 键已删）。
		wantValue     string
		wantTombstone int
		wantSeq       int64
	}{
		{
			name: "值→墓碑→旧值（旧值不能复活已删的键）",
			events: []contracts.Event{
				boardEvent(1, "K", "1", false),
				boardEvent(3, "K", "", true),   // 墓碑（seq 3，更新）
				boardEvent(2, "K", "2", false), // 旧值（seq 2，更旧）
			},
			wantValue: "", wantTombstone: 1, wantSeq: 3,
		},
		{
			name: "墓碑→值→旧墓碑（旧墓碑不能删新值）",
			events: []contracts.Event{
				boardEvent(3, "K", "", true),   // 墓碑（seq 3，更新）
				boardEvent(2, "K", "2", false), // 值（seq 2，更旧）
				boardEvent(1, "K", "", true),   // 旧墓碑（seq 1，更旧）
			},
			wantValue: "", wantTombstone: 1, wantSeq: 3,
		},
		{
			name: "值(新)→墓碑(旧)→值(更旧)（新值不被旧墓碑删掉）",
			events: []contracts.Event{
				boardEvent(3, "K", "3", false), // 新值（seq 3）
				boardEvent(2, "K", "", true),   // 旧墓碑（seq 2）
				boardEvent(1, "K", "1", false), // 更旧的值（seq 1）
			},
			wantValue: "3", wantTombstone: 0, wantSeq: 3,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			orders := [][]contracts.Event{
				sc.events,          // 原序
				reverse(sc.events), // 倒序
				append([]contracts.Event{}, sc.events...), // 原序 + 重复第一条（重放）
			}
			for i, order := range orders {
				db := openInternalDB(t)
				ix, err := New(emptyStore{}, db, 100)
				if err != nil {
					t.Fatalf("new indexer: %v", err)
				}
				ctx := context.Background()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("begin: %v", err)
				}
				for j := range order {
					if err := applyBoard(ctx, tx, &order[j]); err != nil {
						t.Fatalf("apply board: %v", err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit: %v", err)
				}
				value, tombstone, seq := readBoardRow(t, db, "K")
				if tombstone != sc.wantTombstone {
					t.Fatalf("order %d: tombstone = %d, want %d", i, tombstone, sc.wantTombstone)
				}
				if value != sc.wantValue {
					t.Fatalf("order %d: value = %q, want %q", i, value, sc.wantValue)
				}
				if seq != sc.wantSeq {
					t.Fatalf("order %d: updated_seq = %d, want %d", i, seq, sc.wantSeq)
				}
				// 读取面：墓碑行视为不存在。
				_, ok, err := ix.ReadBoard(ctx, internalSession, "K")
				if err != nil {
					t.Fatalf("read board: %v", err)
				}
				if ok != (sc.wantTombstone == 0) {
					t.Fatalf("order %d: ReadBoard ok = %v, want %v", i, ok, sc.wantTombstone == 0)
				}
			}
		})
	}
}

// reverse 复制并倒序（不改动入参）。
func reverse(in []contracts.Event) []contracts.Event {
	out := make([]contracts.Event, len(in))
	for i := range in {
		out[len(in)-1-i] = in[i]
	}
	return out
}

// TestLedgerOutOfOrderTerminalState：ledger 同理——旧快照不能盖掉新快照。
func TestLedgerOutOfOrderTerminalState(t *testing.T) {
	ledgerEvent := func(seq int64, goal string) contracts.Event {
		b, _ := json.Marshal(contracts.LedgerUpdatedPayload{
			Goals: []string{goal}, Constraints: []string{}, Completed: []string{}, Pending: []string{},
		})
		s := seq
		return contracts.Event{
			V: 1, ID: contracts.Ulid(fmt.Sprintf("%026d", seq)), Kind: kindLedgerUpdated,
			TS: "2025-03-01T10:00:00.000Z", SessionID: internalSession, AgentID: internalAgent,
			Durable: true, Seq: &s, Payload: b,
		}
	}
	events := []contracts.Event{ledgerEvent(1, "old"), ledgerEvent(2, "new")}

	for _, order := range [][]contracts.Event{events, reverse(events)} {
		db := openInternalDB(t)
		ix, err := New(emptyStore{}, db, 100)
		if err != nil {
			t.Fatalf("new indexer: %v", err)
		}
		ctx := context.Background()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		for j := range order {
			if err := applyLedger(ctx, tx, &order[j]); err != nil {
				t.Fatalf("apply ledger: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		led, ok, err := ix.ReadLedger(ctx, internalSession)
		if err != nil || !ok {
			t.Fatalf("read ledger: %v (ok=%v)", err, ok)
		}
		if len(led.Goals) != 1 || led.Goals[0] != "new" {
			t.Fatalf("ledger goals = %v, want [new]（旧快照不能盖掉新快照）", led.Goals)
		}
		if led.LastSeq != 2 {
			t.Fatalf("ledger last_seq = %d, want 2", led.LastSeq)
		}
	}
}

// itoa 兜底（避免引入 strconv 冲突时的小工具）。
var _ = strconv.Itoa
