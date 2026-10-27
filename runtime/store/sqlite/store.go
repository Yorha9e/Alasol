package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/runtime/indexer"

	// 注册 database/sql 驱动 "sqlite3"（G2b：ncruces/go-sqlite3，纯 Go wasm2go 真 SQLite）。
	_ "github.com/ncruces/go-sqlite3/driver"
)

// 编译期断言：*Store 实现 core.Store（D15：接口在 core，实现在 runtime）。
var _ core.Store = (*Store)(nil)

// driverName 是 ncruces/go-sqlite3 注册到 database/sql 的驱动名（spike 实测：sqlite3）。
const driverName = "sqlite3"

// defaultBusyTimeoutMS 是锁冲突时的默认等待（G13：busy_timeout 是关键参数，可配）。
const defaultBusyTimeoutMS = 5000

// Options 配置 Store 的打开行为。
type Options struct {
	// BusyTimeoutMS 是 SQLite busy_timeout（毫秒）：锁冲突时等待而非立即失败
	// （G13 实测的两个关键参数之一）。<=0 时取默认 5000。
	BusyTimeoutMS int
}

// Store 是 core.Store 的 SQLite 实现：追加式事件日志（G1/G9）。
//
// 并发模型（G9 Kafka 模式）：多写者只管 append（id UNIQUE 幂等，G11；seq 由
// SQLite AUTOINCREMENT 原子分配 = 全局单调，无需应用层仲裁）；_txlock=immediate
// 让每个 BeginTx 直接拿写锁，busy_timeout 处理冲突（G13 实测并发写零失败）。
// 只存 durable 事件（F8：durable=false 的流事件由总线直发，不入库、不占 seq）。
//
// # seq 的语义：全局单调，但**不保证稠密**
//
// seq 由 AUTOINCREMENT 分配：全局单调递增、绝不重复，但**不保证连续**——
// **只有插入冲突**（ON CONFLICT DO NOTHING，G11 幂等去重那条路径）会吃掉序号，
// 且 AUTOINCREMENT 的冲突序号**不回收**（sqlite_sequence 只增不减）；
// **事务回滚不吃号**（sqlite_sequence 的更新随事务一起回滚）。
//
// 消费端纪律：
//   - **不得用 max(seq) 当事件数**（seq 可能有空洞，count 与 max(seq) 不等）；
//   - 事件条数用 cost_stats.event_count / sessions.event_count 这类**显式计数**；
//   - seq 只用于排序与水位（"已追至 seq N" 表示"seq ≤ N 的都处理过了"，不代表有 N 条）。
type Store struct {
	db   *sql.DB
	opts Options

	// memory 标记这是一个 :memory: 库（测试用）；keepalive 持一个连接不放，
	// 防 cache=shared 的命名内存库因池空而被销毁。
	memory    bool
	keepalive *sql.Conn

	mu     sync.Mutex
	closed bool
}

// Open 打开（或创建）一个事件库并建好 schema。path 可为文件路径或 ":memory:"。
func Open(path string, opts Options) (*Store, error) {
	if opts.BusyTimeoutMS <= 0 {
		opts.BusyTimeoutMS = defaultBusyTimeoutMS
	}
	dsn, memory, err := buildDSN(path, opts.BusyTimeoutMS)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %q: %w", path, err)
	}

	ctx := context.Background()
	// 尽早建立真实连接，fail-fast（sql.Open 是惰性的）。
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping %q: %w", path, err)
	}

	s := &Store{db: db, opts: opts, memory: memory}
	if memory {
		// cache=shared 的命名内存库只要还有一个连接开着就不会被销毁；
		// 这里常驻一条连接，保证池在连接复用间隙库不消失。
		conn, cerr := db.Conn(ctx)
		if cerr != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite: keepalive conn: %w", cerr)
		}
		s.keepalive = conn
	}
	if err := s.ensureSchema(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// NewIndexer 开一个索引器：派生表写句柄由**本包内部**交给索引器
// （G17：派生表写入权只归 indexer，外部包拿不到写派生表的能力）。
//
// 为什么没有 DB()：裸 *sql.DB 落到谁手里谁就能写派生表（连事件表都能写），
// 单写者纪律就守不住了。写句柄只在本包内部流转 → 外部包（indexer 之外）
// 既拿不到写句柄，也读不到派生表（读走索引器的 Read* 方法）。
//
// batch<=0 取索引器默认批大小。
func NewIndexer(st *Store, batch int) (*indexer.Indexer, error) {
	if st == nil {
		return nil, errors.New("sqlite: NewIndexer 需要非 nil 的 Store")
	}
	return indexer.New(st, st.db, batch)
}

// Close 关闭连接池（幂等）：先放掉 keepalive，再关池。重复调用返回 nil。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if s.keepalive != nil {
		if err := s.keepalive.Close(); err != nil {
			errs = append(errs, err)
		}
		s.keepalive = nil
	}
	if err := s.db.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Append 追加事实事件，返回入库后的最终事件（Seq 已填）。
//
// 纪律：
//   - durable=false 直接报错（F8 第二道防线：总线已拦，这里再拒一次）；
//   - 以 Event.ID 幂等（G11/S7）：ON CONFLICT(id) DO NOTHING，重复 ID 不产生新行，
//     返回库中原事件（含原 seq）；
//   - 一批 append 在一个事务内原子提交（G9）；seq 由 AUTOINCREMENT 分配。
func (s *Store) Append(ctx context.Context, events []contracts.Event) ([]contracts.Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	// F8：拒绝非 durable 事件（流事件不入库、不占 seq）。
	for i := range events {
		if !events[i].Durable {
			return nil, fmt.Errorf("sqlite: Append rejected non-durable event id=%s kind=%s (F8: stream events never reach the store)", events[i].ID, events[i].Kind)
		}
	}
	// 同一批内 id 必须唯一，否则输出顺序与"原事件"语义有歧义。
	seen := make(map[contracts.Ulid]int, len(events))
	for i := range events {
		if prev, dup := seen[events[i].ID]; dup {
			return nil, fmt.Errorf("sqlite: Append batch has duplicate id %s at positions %d and %d (S7: dedup key is Event.ID)", events[i].ID, prev, i)
		}
		seen[events[i].ID] = i
	}

	// _txlock=immediate ⇒ BeginTx 发出 BEGIN IMMEDIATE，立即取写锁（G13）。
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for i := range events {
		e := &events[i]
		var payload string
		if len(e.Payload) > 0 {
			payload = string(e.Payload)
		}
		if _, err := tx.ExecContext(ctx, insertEventSQL,
			string(e.ID), e.V, e.TS, string(e.SessionID), string(e.AgentID),
			nullString(e.CausationID), e.Kind, boolToInt(e.Durable), payload,
			nullString(e.BinaryRef),
		); err != nil {
			return nil, fmt.Errorf("sqlite: insert event id=%s: %w", e.ID, err)
		}
	}

	// 读回最终事件（含 seq；重复 ID 返回原行）。在同一事务内读，保证可见性与一致性。
	stored, err := readEventsByIDs(ctx, tx, events)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: commit: %w", err)
	}
	return stored, nil
}

// ReadFrom 从 seq 水位（含）读事实事件，按 seq 升序。
// sessionID 为 nil 读全部；limit<=0 不限。
func (s *Store) ReadFrom(ctx context.Context, sessionID *contracts.Ulid, fromSeq int64, limit int) ([]contracts.Event, error) {
	var sb strings.Builder
	sb.WriteString(selectEventCols)
	sb.WriteString(" WHERE seq >= ?")
	args := []any{fromSeq}
	if sessionID != nil {
		sb.WriteString(" AND session_id = ?")
		args = append(args, string(*sessionID))
	}
	sb.WriteString(" ORDER BY seq ASC")
	if limit > 0 {
		sb.WriteString(" LIMIT ?")
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read from seq=%d: %w", fromSeq, err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// ensureSchema 开启 WAL（仅文件库，一次性；WAL 是数据库持久属性，无需每连接重设——
// 放进 DSN 会让池里每个新连接在写竞争下尝试改 journal_mode 而撞锁），再建事件表与索引。
func (s *Store) ensureSchema(ctx context.Context) error {
	if !s.memory {
		// 文件库：设 WAL（G14 实测两驱动 WAL 均生效）。一次性、无并发，安全拿锁。
		if _, err := s.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
			return fmt.Errorf("sqlite: set journal_mode=WAL: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("sqlite: create schema: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 内部 SQL
// ---------------------------------------------------------------------------

const selectEventCols = `SELECT seq, v, id, ts, session_id, agent_id, causation_id, kind, durable, payload, binary_ref FROM events`

const insertEventSQL = `INSERT INTO events
	(id, v, ts, session_id, agent_id, causation_id, kind, durable, payload, binary_ref)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING`

// schemaSQL：seq 由 DB 原子分配（G9/G10：全局单调）；id UNIQUE（collation 默认 BINARY，
// S 系纪律：provider/框架 id 精确匹配）；payload 存 JSON 文本（空串=缺省）。
//
// seq 列的语义：**全局单调但不保证稠密**——AUTOINCREMENT 的冲突序号不回收
// （**只有插入冲突** ON CONFLICT DO NOTHING 会吃掉号；事务回滚不吃号），因此：
//   - 消费端**不得用 max(seq) 当事件数**（事件条数用 cost_stats/sessions 里的显式计数）；
//   - seq 只用于排序与水位（水位 = "seq ≤ N 都处理过了"，不代表有 N 条事件）。
const schemaSQL = `
CREATE TABLE IF NOT EXISTS events (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT, -- 全局单调、不保证稠密（冲突序号不回收）
  id           TEXT    NOT NULL UNIQUE COLLATE BINARY,
  v            INTEGER NOT NULL,
  ts           TEXT    NOT NULL,
  session_id   TEXT    NOT NULL COLLATE BINARY,
  agent_id     TEXT    NOT NULL COLLATE BINARY,
  causation_id TEXT,
  kind         TEXT    NOT NULL,
  durable      INTEGER NOT NULL,
  payload      TEXT    NOT NULL DEFAULT '',
  binary_ref   TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_kind    ON events(kind, seq);
`

// readEventsByIDs 在事务内按 id 集合读回事件，并按输入顺序返回（幂等：重复 ID 得原行）。
func readEventsByIDs(ctx context.Context, tx *sql.Tx, events []contracts.Event) ([]contracts.Event, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(events)), ",")
	query := selectEventCols + " WHERE id IN (" + placeholders + ")"
	args := make([]any, len(events))
	for i := range events {
		args[i] = string(events[i].ID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read back events: %w", err)
	}
	defer rows.Close()

	byID := make(map[contracts.Ulid]contracts.Event, len(events))
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		byID[ev.ID] = ev
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: read back rows: %w", err)
	}

	out := make([]contracts.Event, len(events))
	for i := range events {
		ev, ok := byID[events[i].ID]
		if !ok {
			return nil, fmt.Errorf("sqlite: event id=%s not found after insert", events[i].ID)
		}
		out[i] = ev
	}
	return out, nil
}

// scanEvents 把整个结果集扫成事件切片（seq 升序由 SQL 保证）。
func scanEvents(rows *sql.Rows) ([]contracts.Event, error) {
	var out []contracts.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: scan rows: %w", err)
	}
	return out, nil
}

// scanEvent 扫一行成 Event。payload 空串 → nil（保留缺省语义）；causation_id/binary_ref
// 可空，用 sql.NullString。
func scanEvent(sc interface{ Scan(dest ...any) error }) (contracts.Event, error) {
	var (
		seq        int64
		v          int64
		id         string
		ts         string
		sessionID  string
		agentID    string
		causation  sql.NullString
		kind       string
		durableInt int64
		payload    string
		binaryRef  sql.NullString
	)
	if err := sc.Scan(&seq, &v, &id, &ts, &sessionID, &agentID, &causation, &kind, &durableInt, &payload, &binaryRef); err != nil {
		return contracts.Event{}, fmt.Errorf("sqlite: scan event: %w", err)
	}
	ev := contracts.Event{
		V:         v,
		ID:        contracts.Ulid(id),
		Kind:      kind,
		Seq:       &seq,
		TS:        ts,
		SessionID: contracts.Ulid(sessionID),
		AgentID:   contracts.Ulid(agentID),
		Durable:   durableInt != 0,
	}
	if causation.Valid {
		u := contracts.Ulid(causation.String)
		ev.CausationID = &u
	}
	if payload != "" {
		ev.Payload = json.RawMessage(payload)
	}
	if binaryRef.Valid {
		u := contracts.Ulid(binaryRef.String)
		ev.BinaryRef = &u
	}
	return ev, nil
}

// ---------------------------------------------------------------------------
// DSN 与工具
// ---------------------------------------------------------------------------

// buildDSN 组装 ncruces DSN（基于 spike 实测格式）：
//   - 文件库：file:<abspath>?_pragma=busy_timeout%3d<ms>&_txlock=immediate
//   - 内存库：file:<uniq>?mode=memory&cache=shared&_pragma=busy_timeout%3d<ms>&_txlock=immediate
//
// busy_timeout 的 "=" 写作 %3d（spike 同款，避免查询值里的裸 "="）。
// 注意：journal_mode=WAL **不**进 DSN——它是数据库持久属性，只需在 Open 时设一次
// （见 ensureSchema）；若放进 DSN，连接池每开一个新连接都会在写竞争下尝试改
// journal_mode 而拿不到锁（并发 append 实测撞 "database is locked"）。
func buildDSN(path string, busyTimeoutMS int) (dsn string, memory bool, err error) {
	busy := fmt.Sprintf("_pragma=busy_timeout%%3d%d", busyTimeoutMS)
	txlock := "_txlock=immediate" // G13：并发写零失败的关键参数之一
	if path == ":memory:" || path == "" {
		name := "alasol-mem-" + randHex(8)
		return "file:" + name + "?mode=memory&cache=shared&" + busy + "&" + txlock, true, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false, fmt.Errorf("sqlite: resolve path %q: %w", path, err)
	}
	// DSN 是 URL 形态，用正斜杠（spike 在 Windows 上实测 file:C:/... 可用）。
	return "file:" + filepath.ToSlash(abs) + "?" + busy + "&" + txlock, false, nil
}

// memFallbackCounter 是内存库兜底名字的进程内计数器（见 randHex）。
var memFallbackCounter uint64

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 几乎不会失败；万一失败也不能让两个 :memory: 库拿到**同一个**名字
		// （cache=shared 的同名内存库是同一个库 → 互相串库，测试结果全乱）。
		// 兜底名必须进程内唯一：pid + 递增计数。
		return fmt.Sprintf("alasol-mem-%d-%d", os.Getpid(), atomic.AddUint64(&memFallbackCounter, 1))
	}
	return hex.EncodeToString(b)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// nullString 把 *Ulid 转成可空 TEXT 列值。
func nullString(u *contracts.Ulid) any {
	if u == nil {
		return nil
	}
	return string(*u)
}
