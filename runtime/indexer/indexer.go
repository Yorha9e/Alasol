// Package indexer 消费全局事件流、增量维护**持久投影**（G17/G24）。
//
// 定位（G9 Kafka 模式）：写者只管 append 事件；**派生表由本索引器单独维护**——
// 它是 sessions 目录 / ledger 当前态 / 黑板当前态 / 成本统计（事件计数 cost_stats +
// token 计量 cost_tokens，J21）的唯一写者（单索引写者）。
// 索引是最终一致的：延迟毫秒级，UI 通过 Watermark 看到「已追至 seq N」（G24③）。
//
// 恢复语义（G24）：① 水位持久化在 indexer_watermark 单行表，崩溃后从 N 续跑；
// ② Rebuild = 清空派生表 + 从 0 重放；③ 水位可见（Watermark）；④ 索引器本身不是
// 多写者——它是派生表的唯一写者。投影应用与水位推进在**同一事务**内，崩溃即整体回滚，
// 不会出现「投影用了但水位没进」的半态。
//
// 状态源（G8/G23/K22/F5）：ledger 与黑板都不是独立可变存储——当前态 = 对
// ledger.updated / board.written 事件的投影，事件流才是唯一事实源。
//
// # 写派生表的能力（G17：只归索引器）
//
// 索引器写派生表用的是**窄 SQL 面**（Writer：只有 Exec / QueryRow / BeginTx 三件事），
// 不是裸 *sql.DB——裸句柄落到谁手里谁就能写派生表（连事件表都能写），单写者纪律
// 就守不住了。写句柄由存储实现在**装配时**交给索引器（见 sqlite.NewIndexer），
// 外部包拿不到它 → 除索引器外没有别的包能写派生表。派生表的**读**也走索引器
// （ReadLedger / ReadBoard / ReadCostStats / ReadSession）。
//
// # 并发
//
// **单索引器实例内部串行**：Step 的「读水位→读事件→应用→推水位」整段由一把锁串起来
// （否则并发 Step 会重复应用同一批 → 计数翻倍）。
// **多个索引器指向同一个库不支持**（两个写者 = 双写 + 双计数；单写者纪律 G17）。
package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// defaultBatch 是 Step 单批默认处理上限。
const defaultBatch = 1000

// pollInterval 是 Run 追平后的轮询间隔。
const pollInterval = 50 * time.Millisecond

// 事件 kind 常量（索引器只对这些 kind 做专属投影；其余忽略但计数，D7）。
const (
	kindLedgerUpdated = "ledger.updated"
	kindBoardWritten  = "board.written"
	kindModelTurn     = "model.turn"
)

// badPayloadKind 是「坏载荷」的诊断名（**只出现在告警里**，不进 by_kind ——
// by_kind 的划分语义是「每个事件恰好落一个 kind」，求和必须等于 event_count）。
const badPayloadKind = "indexer.bad_payload"

// errBadPayload 标记「事件载荷不可解析」：**隔离**（计数 + 告警 + 跳过该事件），
// 不是致命错误——一条坏载荷不该卡死索引器（水位冻结 → Rebuild 同死）。
var errBadPayload = errors.New("indexer: unparsable event payload")

// log 是索引器的告警出口（坏载荷隔离时记一条）。用 slog（结构化、可被宿主接管）。
var log = slog.Default()

// Writer 是索引器写派生表所需的**最小 SQL 面**（G17：派生表写入权只归索引器）。
//
// 只有三件事：执行语句、读一行、开事务。**不暴露 *sql.DB**——裸句柄等于
// "谁拿到谁都能写派生表"，单写者纪律就守不住了。
// 由存储实现在装配时交给索引器（见 sqlite.NewIndexer）。
type Writer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Indexer 是派生表的唯一写者（G17）。它经 core.Store 读事件，经窄 SQL 面写投影。
type Indexer struct {
	store core.Store
	w     Writer
	batch int

	// mu 串行化 Step 的整段「读水位→读事件→应用→推水位」（P1-3）：
	// 单索引器实例内部串行；**多个索引器指向同一个库不支持**。
	mu sync.Mutex
}

// New 建索引器并确保投影表 + 水位行存在。
//
// store 读事件；w 写派生表（窄 SQL 面，只归索引器）；batch<=0 取默认 1000。
//
// 真实存储的装配点用 sqlite.NewIndexer（它把写句柄留在存储实现内部，不外泄）。
func New(store core.Store, w Writer, batch int) (*Indexer, error) {
	if store == nil {
		return nil, errors.New("indexer: 需要非 nil 的 core.Store")
	}
	if w == nil {
		return nil, errors.New("indexer: 需要非 nil 的派生表写句柄（Writer）")
	}
	if batch <= 0 {
		batch = defaultBatch
	}
	ix := &Indexer{store: store, w: w, batch: batch}
	if err := ix.ensureSchema(context.Background()); err != nil {
		return nil, err
	}
	return ix, nil
}

// Watermark 返回已消费到的全局 seq 水位（G24③，供 UI 显示「索引已追至 seq N」）。
//
// 注意：seq **不保证稠密**（AUTOINCREMENT 的冲突序号不回收），水位表示
// "seq ≤ N 的都处理过了"，**不代表有 N 条事件**（事件条数看 ReadCostStats）。
func (ix *Indexer) Watermark() (int64, error) {
	var seq int64
	err := ix.w.QueryRowContext(context.Background(),
		`SELECT seq FROM indexer_watermark WHERE id = 1`).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("indexer: read watermark: %w", err)
	}
	return seq, nil
}

// Step 单步：从水位增量拉取一批 durable 事件，在一个事务内应用投影并推进水位。
// 返回本批处理的事件条数（0 = 已追平）。
//
// 整段「读水位→读事件→应用→推水位」在**一把锁内**完成（P1-3）：
// 并发 Step 不会重复应用同一批（否则计数翻倍）。
func (ix *Indexer) Step(ctx context.Context) (int, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.stepLocked(ctx)
}

// stepLocked 是 Step 的内部实现（调用方必须持有 ix.mu）。
func (ix *Indexer) stepLocked(ctx context.Context) (int, error) {
	wm, err := ix.Watermark()
	if err != nil {
		return 0, err
	}
	events, err := ix.store.ReadFrom(ctx, nil, wm+1, ix.batch)
	if err != nil {
		return 0, fmt.Errorf("indexer: read from seq %d: %w", wm+1, err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	if err := ix.applyBatch(ctx, events); err != nil {
		return 0, err
	}
	return len(events), nil
}

// Run 轮询式消费循环：追平后按 pollInterval 轮询，直到 ctx 取消。
//
// ctx 取消/超时时返回 ctx.Err()（调用方按"被取消"处理，不是故障）：底层 SQL 驱动
// 在查询被中断时报的是 "interrupted" 这类**实现细节错误**，这里统一归一。
func (ix *Indexer) Run(ctx context.Context) error {
	for {
		n, err := ix.Step(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if n > 0 {
			// 还有活，立即继续追水位。
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// Rebuild 重建（G24②）：清空五张派生表 + 水位归零，再从 0 重放到追平。
// 清空与归零在同一事务内原子完成；重放走 Step（同一把锁内），崩溃后可再次
// Rebuild 或续跑。
func (ix *Indexer) Rebuild(ctx context.Context) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	tx, err := ix.w.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("indexer: rebuild begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"sessions", "ledger_state", "board_state", "cost_stats", "cost_tokens"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("indexer: rebuild clear %s: %w", table, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE indexer_watermark SET seq = 0 WHERE id = 1`); err != nil {
		return fmt.Errorf("indexer: rebuild reset watermark: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("indexer: rebuild commit: %w", err)
	}
	for {
		n, err := ix.stepLocked(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// applyBatch 在一个事务内应用整批投影，并把水位推进到最后一条的 seq（G24①：
// 投影 + 水位原子，崩溃整体回滚，不丢不重）。
func (ix *Indexer) applyBatch(ctx context.Context, events []contracts.Event) error {
	tx, err := ix.w.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("indexer: apply begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for i := range events {
		if err := applyEvent(ctx, tx, &events[i]); err != nil {
			return err
		}
	}
	last := events[len(events)-1]
	if last.Seq == nil {
		return fmt.Errorf("indexer: event id=%s has no seq (store must assign it)", last.ID)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE indexer_watermark SET seq = ? WHERE id = 1`, *last.Seq); err != nil {
		return fmt.Errorf("indexer: advance watermark: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("indexer: apply commit: %w", err)
	}
	return nil
}

// applyEvent 把单条事件应用到投影。每个事件都更新 sessions + cost_stats；
// ledger.updated / board.written / model.turn 额外更新对应当前态/成本；未知 kind 忽略但已计数（D7）。
// ledger.updated / board.written 额外更新对应当前态；未知 kind 忽略但已计数（D7）。
//
// **坏载荷隔离**（P1-2）：已知 kind 的载荷不可解析时**不致命**——记一条告警、
// 在 cost_stats 的**独立列** bad_payload_count 里计一次，跳过该事件的专属投影，
// 继续处理本批其余事件（与 D7 宽容哲学一致；否则水位冻结、Rebuild 同死）。
//
// 坏载荷计数**不进 by_kind**：by_kind 的划分语义是「每个事件恰好落一个 kind」
// （各 kind 之和 == event_count），把隔离标记塞进去会让求和失真。
func applyEvent(ctx context.Context, tx *sql.Tx, ev *contracts.Event) error {
	if ev.Seq == nil {
		return fmt.Errorf("indexer: event id=%s has no seq", ev.ID)
	}
	sid := string(ev.SessionID)
	seq := *ev.Seq

	// 1) sessions 目录（session 亲和；first_seq 只在中止/首插时定）。
	if _, err := tx.ExecContext(ctx, upsertSessionSQL, sid, seq, seq, ev.Kind, ev.TS); err != nil {
		return fmt.Errorf("indexer: sessions projection: %w", err)
	}

	// 2) kind 专属当前态（坏载荷 → 隔离，不致命）。
	badPayload := false
	switch ev.Kind {
	case kindLedgerUpdated:
		if err := applyLedger(ctx, tx, ev); err != nil {
			if !errors.Is(err, errBadPayload) {
				return err
			}
			badPayload = true
		}
	case kindBoardWritten:
		if err := applyBoard(ctx, tx, ev); err != nil {
			if !errors.Is(err, errBadPayload) {
				return err
			}
			badPayload = true
		}
	case kindModelTurn:
		// token 成本（J21）：usage 只进成本统计，绝不回填 context 快照/预算。
		if err := applyModelTurn(ctx, tx, ev); err != nil {
			if !errors.Is(err, errBadPayload) {
				return err
			}
			badPayload = true
		}
	default:
		// 未知 kind（如 tool.call.intent）：宽容忽略（D7），但已在 sessions 计数。
	}
	if badPayload {
		log.Warn("indexer: 隔离不可解析的事件载荷（跳过该事件的专属投影）",
			"event_id", string(ev.ID), "kind", ev.Kind, "seq", seq, "session_id", sid,
			"marker", badPayloadKind)
	}

	// 3) cost_stats（事件数按 kind 计数；坏载荷另计独立列 bad_payload_count）。
	if err := bumpCost(ctx, tx, sid, ev.Kind, badPayload); err != nil {
		return err
	}
	return nil
}

// bumpCost 累加 cost_stats：读-改-写 by_kind JSON map（事务内可见自身未提交写）。
//
// by_kind 的划分语义是「每个事件恰好落一个 kind」→ 各 kind 之和 == event_count。
// 坏载荷是**诊断信息**，走独立列 bad_payload_count，不混进 by_kind。
func bumpCost(ctx context.Context, tx *sql.Tx, sid, kind string, badPayload bool) error {
	var count int64
	var byKind string
	var badCount int64
	err := tx.QueryRowContext(ctx,
		`SELECT event_count, by_kind, bad_payload_count FROM cost_stats WHERE session_id = ?`, sid).
		Scan(&count, &byKind, &badCount)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("indexer: cost read: %w", err)
	}
	m := map[string]int64{}
	if err == nil && byKind != "" {
		if uerr := json.Unmarshal([]byte(byKind), &m); uerr != nil {
			return fmt.Errorf("indexer: cost by_kind decode: %w", uerr)
		}
	}
	count++
	m[kind]++
	if badPayload {
		badCount++
	}
	blob, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("indexer: cost by_kind encode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, upsertCostSQL, sid, count, string(blob), badCount); err != nil {
		return fmt.Errorf("indexer: cost write: %w", err)
	}
	return nil
}

// applyLedger 投影 ledger.updated：全量快照 LWW（G23/G8），WHERE seq 守卫保证只被更新者覆盖。
//
// 载荷不可解析 → errBadPayload（隔离，不致命，P1-2）。
func applyLedger(ctx context.Context, tx *sql.Tx, ev *contracts.Event) error {
	var p contracts.LedgerUpdatedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("%w: ledger.updated payload id=%s: %v", errBadPayload, ev.ID, err)
	}
	_, err := tx.ExecContext(ctx, upsertLedgerSQL,
		string(ev.SessionID),
		jsonArray(p.Goals), jsonArray(p.Constraints), jsonArray(p.Completed), jsonArray(p.Pending),
		*ev.Seq)
	if err != nil {
		return fmt.Errorf("indexer: ledger projection: %w", err)
	}
	return nil
}

// applyBoard 投影 board.written：键级 LWW（seq 守卫）；tombstone=true 写**墓碑行**
// （K22 墓碑语义）。
//
// 墓碑**不物理删除**（P1-1）：物理删除没有 seq 守卫，乱序重放时一条**旧**墓碑会
// 删掉**新**值（双向丢数据）。墓碑行保留 (session_id,key)、value=NULL、tombstone=1、
// updated_seq=墓碑事件的 seq；读取时墓碑行**视为不存在**（对外删除语义不变）。
//
// 值与墓碑走**同一条 SQL、同一个「seq 更大才覆盖」守卫**：
//   - 旧墓碑（seq 小）盖不掉新值；
//   - 旧值（seq 小）复活不了已删的键。
//
// 载荷不可解析 → errBadPayload（隔离，不致命，P1-2）。
func applyBoard(ctx context.Context, tx *sql.Tx, ev *contracts.Event) error {
	var p contracts.BoardWrittenPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("%w: board.written payload id=%s: %v", errBadPayload, ev.ID, err)
	}
	sid := string(ev.SessionID)
	author := ""
	if p.Author != nil {
		author = *p.Author
	}
	tombstone := 0
	var value any // 墓碑行 value=NULL
	if p.Tombstone != nil && *p.Tombstone {
		tombstone = 1
	} else {
		if len(p.Value) > 0 {
			value = string(p.Value)
		}
	}
	if _, err := tx.ExecContext(ctx, upsertBoardSQL, sid, p.Key, value, author, tombstone, *ev.Seq); err != nil {
		return fmt.Errorf("indexer: board projection: %w", err)
	}
	return nil
}

// applyModelTurn 投影 model.turn 的 token usage 到 cost_tokens（J21：usage **只进成本
// 统计**，绝不回填 context 快照/预算——那是调用前估算的正式口径，与 provider 事后上报的
// usage 是两件事）。
//
//   - 无 usage（provider 不上报）→ 本事件不计 token（cost_stats 的事件计数已覆盖"有一轮"）；
//   - 有 usage → 累加 input/output tokens，按 model 名分组（by_model），metered turn 计数 +1。
//
// 载荷不可解析 → errBadPayload（隔离，不致命，P1-2）。
func applyModelTurn(ctx context.Context, tx *sql.Tx, ev *contracts.Event) error {
	var p contracts.ModelTurnPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("%w: model.turn payload id=%s: %v", errBadPayload, ev.ID, err)
	}
	if p.Usage == nil {
		return nil // 无 usage：不计 token
	}
	model := ""
	if p.Model != nil {
		model = *p.Model
	}
	return bumpTokenCost(ctx, tx, string(ev.SessionID), model, p.Usage.InputTokens, p.Usage.OutputTokens)
}

// bumpTokenCost 累加 cost_tokens：读-改-写（事务内可见自身未提交写）。
// input/output tokens 累加；metered turn +1；by_model 里对应模型的 input/output/turns 累加。
func bumpTokenCost(ctx context.Context, tx *sql.Tx, sid, model string, in, out int64) error {
	var curIn, curOut, curTurns int64
	var byModel string
	err := tx.QueryRowContext(ctx,
		`SELECT input_tokens, output_tokens, model_turns, by_model FROM cost_tokens WHERE session_id = ?`, sid).
		Scan(&curIn, &curOut, &curTurns, &byModel)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("indexer: token cost read: %w", err)
	}
	byModelMap := map[string]ModelUsage{}
	if err == nil && byModel != "" {
		if uerr := json.Unmarshal([]byte(byModel), &byModelMap); uerr != nil {
			return fmt.Errorf("indexer: token cost by_model decode: %w", uerr)
		}
	}
	curIn += in
	curOut += out
	curTurns++
	mu := byModelMap[model]
	mu.InputTokens += in
	mu.OutputTokens += out
	mu.Turns++
	byModelMap[model] = mu

	blob, err := json.Marshal(byModelMap)
	if err != nil {
		return fmt.Errorf("indexer: token cost by_model encode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, upsertCostTokensSQL, sid, curIn, curOut, curTurns, string(blob)); err != nil {
		return fmt.Errorf("indexer: token cost write: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 派生表读面（G17：读也走索引器——外部包不需要、也拿不到裸 SQL 句柄）
// ---------------------------------------------------------------------------

// LedgerState 是某 session 的 ledger 当前态。
type LedgerState struct {
	Goals       []string
	Constraints []string
	Completed   []string
	Pending     []string
	LastSeq     int64
}

// BoardEntry 是黑板某键的当前值（墓碑行视为不存在 → ok=false）。
type BoardEntry struct {
	Value      string
	Author     string
	UpdatedSeq int64
}

// CostStats 是某 session 的成本统计。
type CostStats struct {
	// EventCount 是该 session 的事实事件总数（= by_kind 各 kind 之和）。
	EventCount int64
	// ByKind 是按事件 kind 的计数（每个事件**恰好落一个 kind** → 求和 == EventCount）。
	ByKind map[string]int64
	// BadPayloadCount 是"载荷不可解析而被隔离"的事件条数（诊断用，不在 ByKind 里）。
	BadPayloadCount int64
}

// SessionStats 是 session 目录条目。
type SessionStats struct {
	FirstSeq   int64
	LastSeq    int64
	EventCount int64
	LastKind   string
}

// ReadLedger 读某 session 的 ledger 当前态（不存在 → ok=false）。
func (ix *Indexer) ReadLedger(ctx context.Context, sessionID contracts.Ulid) (LedgerState, bool, error) {
	var (
		goals, constraints, completed, pending string
		lastSeq                                int64
	)
	err := ix.w.QueryRowContext(ctx,
		`SELECT goals, constraints, completed, pending, last_seq FROM ledger_state WHERE session_id = ?`,
		string(sessionID)).Scan(&goals, &constraints, &completed, &pending, &lastSeq)
	if err == sql.ErrNoRows {
		return LedgerState{}, false, nil
	}
	if err != nil {
		return LedgerState{}, false, fmt.Errorf("indexer: read ledger: %w", err)
	}
	out := LedgerState{LastSeq: lastSeq}
	out.Goals = decodeStrings(goals)
	out.Constraints = decodeStrings(constraints)
	out.Completed = decodeStrings(completed)
	out.Pending = decodeStrings(pending)
	return out, true, nil
}

// ReadBoard 读黑板某键的当前值（墓碑行 = 不存在 → ok=false）。
//
// value 允许为 NULL（墓碑行，以及"写了空值"的事件），用 sql.NullString 承接。
func (ix *Indexer) ReadBoard(ctx context.Context, sessionID contracts.Ulid, key string) (BoardEntry, bool, error) {
	var (
		value      sql.NullString
		author     string
		updatedSeq int64
	)
	err := ix.w.QueryRowContext(ctx,
		`SELECT value, author, updated_seq FROM board_state
		 WHERE session_id = ? AND key = ? AND tombstone = 0`,
		string(sessionID), key).Scan(&value, &author, &updatedSeq)
	if err == sql.ErrNoRows {
		return BoardEntry{}, false, nil
	}
	if err != nil {
		return BoardEntry{}, false, fmt.Errorf("indexer: read board: %w", err)
	}
	return BoardEntry{Value: value.String, Author: author, UpdatedSeq: updatedSeq}, true, nil
}

// ReadCostStats 读某 session 的成本统计（不存在 → ok=false）。
func (ix *Indexer) ReadCostStats(ctx context.Context, sessionID contracts.Ulid) (CostStats, bool, error) {
	var (
		count    int64
		byKind   string
		badCount int64
	)
	err := ix.w.QueryRowContext(ctx,
		`SELECT event_count, by_kind, bad_payload_count FROM cost_stats WHERE session_id = ?`, string(sessionID)).
		Scan(&count, &byKind, &badCount)
	if err == sql.ErrNoRows {
		return CostStats{}, false, nil
	}
	if err != nil {
		return CostStats{}, false, fmt.Errorf("indexer: read cost stats: %w", err)
	}
	m := map[string]int64{}
	if byKind != "" {
		if uerr := json.Unmarshal([]byte(byKind), &m); uerr != nil {
			return CostStats{}, false, fmt.Errorf("indexer: cost by_kind decode: %w", uerr)
		}
	}
	return CostStats{EventCount: count, ByKind: m, BadPayloadCount: badCount}, true, nil
}

// ModelUsage 是单个模型的 token 计量聚合（J21：只做成本记录）。
type ModelUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	Turns        int64 `json:"turns"`
}

// TokenStats 是某 session 的 token 成本统计（J21）。**只进成本，不回填快照/预算**。
type TokenStats struct {
	// InputTokens / OutputTokens 是该 session 累计的输入/输出 token（所有 metered turn 之和）。
	InputTokens  int64
	OutputTokens int64
	// ModelTurns 是携带 usage 的 model.turn 条数（metered turn；无 usage 的轮不计）。
	ModelTurns int64
	// ByModel 按模型名分组的计量（cost 按模型分组）。
	ByModel map[string]ModelUsage
}

// ReadTokenStats 读某 session 的 token 成本统计（不存在 → ok=false）。
func (ix *Indexer) ReadTokenStats(ctx context.Context, sessionID contracts.Ulid) (TokenStats, bool, error) {
	var in, out, turns int64
	var byModel string
	err := ix.w.QueryRowContext(ctx,
		`SELECT input_tokens, output_tokens, model_turns, by_model FROM cost_tokens WHERE session_id = ?`,
		string(sessionID)).Scan(&in, &out, &turns, &byModel)
	if err == sql.ErrNoRows {
		return TokenStats{}, false, nil
	}
	if err != nil {
		return TokenStats{}, false, fmt.Errorf("indexer: read token stats: %w", err)
	}
	m := map[string]ModelUsage{}
	if byModel != "" {
		if uerr := json.Unmarshal([]byte(byModel), &m); uerr != nil {
			return TokenStats{}, false, fmt.Errorf("indexer: token stats by_model decode: %w", uerr)
		}
	}
	return TokenStats{InputTokens: in, OutputTokens: out, ModelTurns: turns, ByModel: m}, true, nil
}

// ReadAllTokenStats 读**所有** session 的 token 成本统计（按 session_id 索引）。
// 用于 /api/cost 的总额聚合（hub 汇总各 session 的 input/output）。
//
// 用 BeginTx + tx.Query 做多行读（不拓宽 Writer 接口：仍是 Exec/QueryRow/BeginTx 三件）。
func (ix *Indexer) ReadAllTokenStats(ctx context.Context) (map[contracts.Ulid]TokenStats, error) {
	tx, err := ix.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("indexer: read all token stats begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx,
		`SELECT session_id, input_tokens, output_tokens, model_turns, by_model FROM cost_tokens`)
	if err != nil {
		return nil, fmt.Errorf("indexer: read all token stats query: %w", err)
	}
	defer rows.Close()
	out := map[contracts.Ulid]TokenStats{}
	for rows.Next() {
		var sid string
		var in, out2, turns int64
		var byModel string
		if err := rows.Scan(&sid, &in, &out2, &turns, &byModel); err != nil {
			return nil, fmt.Errorf("indexer: read all token stats scan: %w", err)
		}
		m := map[string]ModelUsage{}
		if byModel != "" {
			if uerr := json.Unmarshal([]byte(byModel), &m); uerr != nil {
				return nil, fmt.Errorf("indexer: token stats by_model decode: %w", uerr)
			}
		}
		out[contracts.Ulid(sid)] = TokenStats{InputTokens: in, OutputTokens: out2, ModelTurns: turns, ByModel: m}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("indexer: read all token stats rows: %w", err)
	}
	return out, nil
}

// ReadSession 读 session 目录条目（不存在 → ok=false）。
func (ix *Indexer) ReadSession(ctx context.Context, sessionID contracts.Ulid) (SessionStats, bool, error) {
	var (
		first, last, count int64
		lastKind           string
	)
	err := ix.w.QueryRowContext(ctx,
		`SELECT first_seq, last_seq, event_count, last_kind FROM sessions WHERE session_id = ?`,
		string(sessionID)).Scan(&first, &last, &count, &lastKind)
	if err == sql.ErrNoRows {
		return SessionStats{}, false, nil
	}
	if err != nil {
		return SessionStats{}, false, fmt.Errorf("indexer: read session: %w", err)
	}
	return SessionStats{FirstSeq: first, LastSeq: last, EventCount: count, LastKind: lastKind}, true, nil
}

// decodeStrings 解 JSON 字符串数组（失败 → 空切片；当前态永远是合法数组）。
func decodeStrings(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

// ensureSchema 建投影表 + 水位表，并播种水位行（seq=0）。
func (ix *Indexer) ensureSchema(ctx context.Context) error {
	if _, err := ix.w.ExecContext(ctx, indexerSchemaSQL); err != nil {
		return fmt.Errorf("indexer: create schema: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const upsertSessionSQL = `INSERT INTO sessions (session_id, first_seq, last_seq, event_count, last_kind, updated_at)
VALUES (?, ?, ?, 1, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
  last_seq    = excluded.last_seq,
  event_count = sessions.event_count + 1,
  last_kind   = excluded.last_kind,
  updated_at  = excluded.updated_at`

const upsertCostSQL = `INSERT INTO cost_stats (session_id, event_count, by_kind, bad_payload_count)
VALUES (?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
  event_count       = excluded.event_count,
  by_kind           = excluded.by_kind,
  bad_payload_count = excluded.bad_payload_count`

// upsertCostTokensSQL：token 成本（J21）。读-改-写在 bumpTokenCost 里算好全量值后整行覆写。
const upsertCostTokensSQL = `INSERT INTO cost_tokens (session_id, input_tokens, output_tokens, model_turns, by_model)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
  input_tokens  = excluded.input_tokens,
  output_tokens = excluded.output_tokens,
  model_turns   = excluded.model_turns,
  by_model      = excluded.by_model`

const upsertLedgerSQL = `INSERT INTO ledger_state (session_id, goals, constraints, completed, pending, last_seq)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
  goals       = excluded.goals,
  constraints = excluded.constraints,
  completed   = excluded.completed,
  pending     = excluded.pending,
  last_seq    = excluded.last_seq
  WHERE excluded.last_seq > ledger_state.last_seq`

// upsertBoardSQL：值与墓碑**同一条** upsert（统一「seq 更大才覆盖」守卫，P1-1）。
// 墓碑：value=NULL、tombstone=1；值：value=文本、tombstone=0。
const upsertBoardSQL = `INSERT INTO board_state (session_id, key, value, author, tombstone, updated_seq)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, key) DO UPDATE SET
  value       = excluded.value,
  author      = excluded.author,
  tombstone   = excluded.tombstone,
  updated_seq = excluded.updated_seq
  WHERE excluded.updated_seq > board_state.updated_seq`

// indexerSchemaSQL：派生表只由索引器写（G17）；水位表单行（CHECK id=1）。
const indexerSchemaSQL = `
CREATE TABLE IF NOT EXISTS sessions (
  session_id  TEXT PRIMARY KEY COLLATE BINARY,
  first_seq   INTEGER NOT NULL,
  last_seq    INTEGER NOT NULL,
  event_count INTEGER NOT NULL,
  last_kind   TEXT    NOT NULL,
  updated_at  TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_state (
  session_id  TEXT PRIMARY KEY COLLATE BINARY,
  goals       TEXT NOT NULL,
  constraints TEXT NOT NULL,
  completed   TEXT NOT NULL,
  pending     TEXT NOT NULL,
  last_seq    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS board_state (
  session_id  TEXT NOT NULL COLLATE BINARY,
  key         TEXT NOT NULL,
  value       TEXT,
  author      TEXT NOT NULL DEFAULT '',
  tombstone   INTEGER NOT NULL DEFAULT 0,
  updated_seq INTEGER NOT NULL,
  PRIMARY KEY (session_id, key)
);
CREATE TABLE IF NOT EXISTS cost_stats (
  session_id  TEXT PRIMARY KEY COLLATE BINARY,
  event_count INTEGER NOT NULL,
  by_kind     TEXT NOT NULL DEFAULT '{}',
  bad_payload_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cost_tokens (
  session_id   TEXT PRIMARY KEY COLLATE BINARY,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  model_turns  INTEGER NOT NULL DEFAULT 0,
  by_model     TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS indexer_watermark (
  id  INTEGER PRIMARY KEY CHECK (id = 1),
  seq INTEGER NOT NULL
);
INSERT OR IGNORE INTO indexer_watermark (id, seq) VALUES (1, 0);
`

// jsonArray 把字符串切片序列化为 JSON 数组；nil/空规范成 "[]"（当前态永远是合法数组）。
func jsonArray(s []string) string {
	if len(s) == 0 {
		return "[]"
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "[]"
	}
	return string(b)
}
