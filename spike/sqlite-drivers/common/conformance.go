// Package sqlitecommon is a driver-agnostic conformance suite for embedded
// SQLite engines.
//
// This is a throwaway spike, not a library. Its job is to answer four
// questions on the developer's own machine:
//
//  1. Does the pure-Go build work for windows/amd64 with CGO_ENABLED=0?
//     (checked by run.sh)
//  2. Does journal_mode=WAL actually take effect?
//  3. Under concurrent append, does the driver stay BUSY-free — and does the
//     transaction mode (_txlock) change the answer?
//  4. **Across processes**: can two processes open the same database file and
//     both write to it safely (the real hub + CLI scenario)?
//
// Question 4 is the one that matters most for us and the one most embedded
// engines gloss over: single-process concurrency says nothing about whether
// the on-disk locking works.
package sqlitecommon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BusyTimeoutMS is the busy_timeout the suite asks for. It doubles as the
// expected wait in the lock-contention check.
const BusyTimeoutMS = 1500

const (
	concWorkers   = 50
	concPerWorker = 20
	lockHoldMinMS = 300
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  id         TEXT    NOT NULL UNIQUE,
  ts         TIMESTAMP NOT NULL,
  session_id TEXT    NOT NULL,
  agent_id   TEXT,
  kind       TEXT    NOT NULL,
  payload    BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_kind    ON events(kind, seq);

CREATE TABLE IF NOT EXISTS ledger (
  key        TEXT PRIMARY KEY,
  version    INTEGER NOT NULL,
  body       BLOB NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  id    TEXT PRIMARY KEY,
  kind  TEXT NOT NULL,
  props BLOB
);
CREATE TABLE IF NOT EXISTS edges (
  src   TEXT NOT NULL,
  dst   TEXT NOT NULL,
  kind  TEXT NOT NULL,
  props BLOB,
  PRIMARY KEY (src, dst, kind)
);
CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src, kind);
`

const insertEventSQL = `INSERT INTO events (id, ts, session_id, agent_id, kind, payload) VALUES (?, ?, ?, ?, ?, ?)`

// Config selects the driver and how to reach its database file.
type Config struct {
	// DriverName is the name registered with database/sql ("sqlite", "sqlite3", ...).
	DriverName string
	// DSNTemplate may contain {FILE}; it is replaced with the absolute path of
	// the scratch database file inside a fresh temp directory.
	DSNTemplate string
	// TxLock, when non-empty, is appended as "_txlock=<value>" to a second
	// connection so the suite can compare transaction modes.
	TxLock string
	// Label is a human-readable driver name for the report.
	Label string
}

// Check is one conformance result.
type Check struct {
	Name   string
	Passed bool
	Detail string
}

// Report is the outcome of running the suite against one driver.
type Report struct {
	Label   string
	Driver  string
	Checks  []Check
	Metrics map[string]string
}

func (r *Report) add(name string, passed bool, format string, args ...any) {
	r.Checks = append(r.Checks, Check{
		Name:   name,
		Passed: passed,
		Detail: fmt.Sprintf(format, args...),
	})
}

func (r *Report) metric(k, v string) {
	if r.Metrics == nil {
		r.Metrics = map[string]string{}
	}
	r.Metrics[k] = v
}

// Run executes the suite and returns the report. A non-nil error means the
// driver could not be exercised at all (e.g. it failed to open).
func Run(cfg Config) (*Report, error) {
	r := &Report{Label: cfg.Label, Driver: cfg.DriverName}

	dir, err := os.MkdirTemp("", "sqlite-spike-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "spike.db")
	dsn := strings.ReplaceAll(cfg.DSNTemplate, "{FILE}", filepath.ToSlash(file))

	db, err := sql.Open(cfg.DriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// ---- 1. pragmas ------------------------------------------------------
	jm, err := pragmaString(ctx, db, "journal_mode")
	r.add("journal_mode=WAL", err == nil && strings.EqualFold(jm, "wal"),
		"journal_mode=%q err=%v", jm, err)

	bt, err := pragmaInt(ctx, db, "busy_timeout")
	r.add("busy_timeout 已设置", err == nil && bt > 0, "busy_timeout=%d err=%v", bt, err)

	// ---- 2. schema -------------------------------------------------------
	if err := createSchema(ctx, db); err != nil {
		return r, fmt.Errorf("schema: %w", err)
	}
	r.add("schema 建立", true, "events / ledger / nodes / edges 四类表")

	// ---- 3. idempotent append ------------------------------------------
	ok, detail := checkIdempotent(ctx, db)
	r.add("幂等追加（id 唯一约束）", ok, "%s", detail)

	// ---- 4. seq monotonic + timestamp round-trip ------------------------
	ok, detail = checkSeqAndTime(ctx, db)
	r.add("seq 单调 + 时间往返", ok, "%s", detail)

	// ---- 5. concurrent append (default transaction mode) ----------------
	ok, detail, metrics := checkConcurrentAppend(ctx, db, "default")
	r.add("并发追加无 BUSY/locked（默认事务模式）", ok, "%s", detail)
	for k, v := range metrics {
		r.metric(k, v)
	}

	// ---- 6. concurrent append (_txlock=immediate) ------------------------
	if cfg.TxLock != "" {
		immDSN := dsn + "&_txlock=" + cfg.TxLock
		immDB, err := sql.Open(cfg.DriverName, immDSN)
		if err != nil {
			r.add("并发追加无 BUSY/locked（_txlock="+cfg.TxLock+"）", false, "open: %v", err)
		} else {
			defer immDB.Close()
			if err := createSchema(ctx, immDB); err != nil {
				r.add("并发追加无 BUSY/locked（_txlock="+cfg.TxLock+"）", false, "schema: %v", err)
			} else {
				ok, detail, metrics := checkConcurrentAppend(ctx, immDB, "txlock")
				r.add("并发追加无 BUSY/locked（_txlock="+cfg.TxLock+"）", ok, "%s", detail)
				for k, v := range metrics {
					r.metric("imm_"+k, v)
				}
			}
		}
	}

	// ---- 7. read while writing ------------------------------------------
	ok, detail = checkReadDuringWrite(ctx, db)
	r.add("写期间并发读不报错", ok, "%s", detail)

	// ---- 8. lock contention honours busy_timeout ------------------------
	ok, detail = checkLockWait(ctx, db)
	r.add("锁冲突时等待（busy_timeout 生效）", ok, "%s", detail)

	return r, nil
}

func createSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaSQL)
	return err
}

func pragmaString(ctx context.Context, db *sql.DB, name string) (string, error) {
	var v string
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

func pragmaInt(ctx context.Context, db *sql.DB, name string) (int64, error) {
	var v int64
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

func checkIdempotent(ctx context.Context, db *sql.DB) (bool, string) {
	id := "idem-1"
	ins := func() error {
		_, err := db.ExecContext(ctx, insertEventSQL,
			id, time.Now().UTC(), "s1", nil, "test.idem", []byte(`{"n":1}`))
		return err
	}
	first := ins()
	second := ins()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = ?`, id).Scan(&n); err != nil {
		return false, fmt.Sprintf("count failed: %v", err)
	}
	return first == nil && second != nil && n == 1,
		fmt.Sprintf("first_err=%v second_err=%v rows=%d", first, second, n)
}

func checkSeqAndTime(ctx context.Context, db *sql.DB) (bool, string) {
	const n = 20
	ts := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < n; i++ {
		_, err := db.ExecContext(ctx, insertEventSQL,
			fmt.Sprintf("seq-%d", i), ts, "s2", nil, "test.seq", []byte(`{}`))
		if err != nil {
			return false, fmt.Sprintf("insert %d failed: %v", i, err)
		}
	}
	var prev int64 = -1
	monotonic := true
	rows, err := db.QueryContext(ctx, `SELECT seq FROM events WHERE session_id = 's2' ORDER BY seq`)
	if err != nil {
		return false, fmt.Sprintf("query failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			return false, fmt.Sprintf("scan failed: %v", err)
		}
		if prev >= 0 && s <= prev {
			monotonic = false
		}
		prev = s
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Sprintf("rows err: %v", err)
	}

	// timestamp round-trip: what type does the driver hand back for a
	// TIMESTAMP column? This is a portability trap worth surfacing.
	var raw any
	if err := db.QueryRowContext(ctx, `SELECT ts FROM events WHERE session_id = 's2' ORDER BY seq LIMIT 1`).Scan(&raw); err != nil {
		return false, fmt.Sprintf("ts scan failed: %v", err)
	}
	return monotonic,
		fmt.Sprintf("last_seq=%d monotonic=%v ts_roundtrip_type=%T", prev, monotonic, raw)
}

func checkConcurrentAppend(ctx context.Context, db *sql.DB, tag string) (bool, string, map[string]string) {
	var wg sync.WaitGroup
	var okN, errN int64
	var mu sync.Mutex
	classes := map[string]int{}

	start := time.Now()
	for w := 0; w < concWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < concPerWorker; i++ {
				_, err := db.ExecContext(ctx, insertEventSQL,
					fmt.Sprintf("%s-conc-%d-%d", tag, w, i), time.Now().UTC(), "s3", nil, "test.conc", []byte(`{}`))
				if err != nil {
					atomic.AddInt64(&errN, 1)
					mu.Lock()
					classes[classifyErr(err)]++
					mu.Unlock()
				} else {
					atomic.AddInt64(&okN, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := concWorkers * concPerWorker
	metrics := map[string]string{
		"total":       fmt.Sprintf("%d", total),
		"ok":          fmt.Sprintf("%d", okN),
		"failed":      fmt.Sprintf("%d", errN),
		"elapsed":     elapsed.Round(time.Millisecond).String(),
		"write_tps":   fmt.Sprintf("%.0f", float64(okN)/elapsed.Seconds()),
		"max_tps":     "",
	}
	if len(classes) > 0 {
		keys := make([]string, 0, len(classes))
		for k := range classes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s×%d", k, classes[k]))
		}
		metrics["error_classes"] = strings.Join(parts, ", ")
	}
	ok := errN == 0
	return ok, fmt.Sprintf("ok=%d failed=%d elapsed=%s", okN, errN, elapsed.Round(time.Millisecond)), metrics
}

// checkReadDuringWrite runs a write burst while a reader polls the same
// table, and reports whether the reader ever failed.
func checkReadDuringWrite(ctx context.Context, db *sql.DB) (bool, string) {
	const (
		readers  = 4
		readsN   = 200
		writers  = 8
		writesN  = 40
	)

	var wg sync.WaitGroup
	var readErrs, readOK int64

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readsN; j++ {
				var n int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
					atomic.AddInt64(&readErrs, 1)
				} else {
					atomic.AddInt64(&readOK, 1)
				}
			}
		}()
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < writesN; j++ {
				_, _ = db.ExecContext(ctx, insertEventSQL,
					fmt.Sprintf("rdw-%d-%d", i, j), time.Now().UTC(), "s5", nil, "test.rdw", []byte(`{}`))
			}
		}(i)
	}
	wg.Wait()

	errs := atomic.LoadInt64(&readErrs)
	oks := atomic.LoadInt64(&readOK)
	return errs == 0, fmt.Sprintf("reads_ok=%d reads_failed=%d", oks, errs)
}

// checkLockWait verifies that busy_timeout makes a blocked writer WAIT for
// the timeout instead of failing instantly.
func checkLockWait(ctx context.Context, db *sql.DB) (bool, string) {
	connA, err := db.Conn(ctx)
	if err != nil {
		return false, fmt.Sprintf("conn A: %v", err)
	}
	defer connA.Close()

	connB, err := db.Conn(ctx)
	if err != nil {
		return false, fmt.Sprintf("conn B: %v", err)
	}
	defer connB.Close()

	tx, err := connA.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Sprintf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, insertEventSQL,
		"lock-holder", time.Now().UTC(), "s4", nil, "test.lock", []byte(`{}`)); err != nil {
		_ = tx.Rollback()
		return false, fmt.Sprintf("lock holder insert: %v", err)
	}

	start := time.Now()
	_, errB := connB.ExecContext(ctx, insertEventSQL,
		"lock-waiter", time.Now().UTC(), "s4", nil, "test.lock", []byte(`{}`))
	waited := time.Since(start)

	_ = tx.Rollback()

	if errB == nil {
		return false, fmt.Sprintf("writer B unexpectedly succeeded (no contention, waited %s)", waited.Round(time.Millisecond))
	}
	if waited < lockHoldMinMS*time.Millisecond {
		return false, fmt.Sprintf("failed immediately after %s (busy_timeout NOT honoured): %v",
			waited.Round(time.Millisecond), errB)
	}
	return true, fmt.Sprintf("waited %s then failed as expected: %v",
		waited.Round(time.Millisecond), errB)
}

// ---------------------------------------------------------------------------
// Cross-process checks: the real hub + CLI scenario.
// ---------------------------------------------------------------------------

// Hold opens the database at file, keeps writing to it for dur, and reports
// how many rows it managed to commit. It is run as a separate process by the
// prober.
func Hold(cfg Config, file string, dur time.Duration) (int, error) {
	dsn := strings.ReplaceAll(cfg.DSNTemplate, "{FILE}", filepath.ToSlash(file))
	db, err := sql.Open(cfg.DriverName, dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	ctx := context.Background()
	if err := createSchema(ctx, db); err != nil {
		return 0, err
	}

	deadline := time.Now().Add(dur)
	written := 0
	for i := 0; time.Now().Before(deadline); i++ {
		_, err := db.ExecContext(ctx, insertEventSQL,
			fmt.Sprintf("hold-%d-%d", os.Getpid(), i), time.Now().UTC(), "holder", nil, "test.hold", []byte(`{}`))
		if err != nil {
			return written, err
		}
		written++
		time.Sleep(10 * time.Millisecond)
	}
	return written, nil
}

// ProbeResult is what a second process observes while the holder is writing.
type ProbeResult struct {
	// Visible is the number of rows the probe could see (data written by the
	// other process). A pure in-process VFS would show 0.
	Visible int
	// WriteErr is the error from the probe's own write attempt (nil = it
	// succeeded, which is also acceptable as long as nothing was corrupted).
	WriteErr error
	// RowsAfter is the row count after the probe's write.
	RowsAfter int
}

// Probe opens the database while another process is writing to it.
func Probe(cfg Config, file string) (ProbeResult, error) {
	var res ProbeResult
	dsn := strings.ReplaceAll(cfg.DSNTemplate, "{FILE}", filepath.ToSlash(file))
	db, err := sql.Open(cfg.DriverName, dsn)
	if err != nil {
		return res, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&res.Visible); err != nil {
		return res, fmt.Errorf("probe read failed: %w", err)
	}

	_, res.WriteErr = db.ExecContext(ctx, insertEventSQL,
		fmt.Sprintf("probe-%d", os.Getpid()), time.Now().UTC(), "prober", nil, "test.probe", []byte(`{}`))

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&res.RowsAfter); err != nil {
		return res, fmt.Errorf("probe recount failed: %w", err)
	}
	return res, nil
}

func classifyErr(err error) string {
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "busy"):
		return "SQLITE_BUSY"
	case strings.Contains(s, "locked"):
		return "SQLITE_LOCKED"
	case strings.Contains(s, "deadlock"):
		return "SQLITE_DEADLOCK"
	default:
		return "other"
	}
}

// Print renders a report as plain text.
func Print(r *Report) {
	fmt.Printf("=== %s (driver=%q) ===\n", r.Label, r.Driver)
	pass, fail := 0, 0
	for _, c := range r.Checks {
		mark := "PASS"
		if c.Passed {
			pass++
		} else {
			mark = "FAIL"
			fail++
		}
		fmt.Printf("  [%s] %-42s %s\n", mark, c.Name, c.Detail)
	}
	if len(r.Metrics) > 0 {
		keys := make([]string, 0, len(r.Metrics))
		for k := range r.Metrics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Println("  -- metrics --")
		for _, k := range keys {
			if r.Metrics[k] == "" {
				continue
			}
			fmt.Printf("     %-30s %s\n", k, r.Metrics[k])
		}
	}
	fmt.Printf("  -> %d passed, %d failed\n\n", pass, fail)
}
