package telemetry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Yorha9e/alasol/core"
)

// TestNoopEmitNil：noop 恒成功、不落盘（零副作用）。
func TestNoopEmitNil(t *testing.T) {
	s := Noop()
	if err := s.Emit(core.Record{Kind: core.KindTelRunBegin}); err != nil {
		t.Fatalf("noop Emit 应恒 nil，得到 %v", err)
	}
}

// TestFileEmitWritesNDJSON：file sink 落盘一行一条合法 NDJSON（kind/ts 可读回）。
func TestFileEmitWritesNDJSON(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	recs := []core.Record{
		{Kind: core.KindTelRunBegin, TS: "2026-11-01T00:00:00Z", SessionID: "s1", Model: "m"},
		{Kind: core.KindTelToolCall, TS: "2026-11-01T00:00:01Z", SessionID: "s1", Tool: "echo", OK: true},
		{Kind: core.KindTelRunEnd, TS: "2026-11-01T00:00:02Z", SessionID: "s1", Turns: 1},
	}
	for _, r := range recs {
		if err := f.Emit(r); err != nil {
			t.Fatalf("Emit(%s): %v", r.Kind, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(f.Path())
	if err != nil {
		t.Fatalf("读落盘文件: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("落盘行数 = %d，want 3（NDJSON 一行一条）", len(lines))
	}
	for i, line := range lines {
		var got core.Record
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("第 %d 行不是合法 JSON: %v（%s）", i, err, line)
		}
		if got.Kind != recs[i].Kind {
			t.Errorf("第 %d 行 kind = %q，want %q", i, got.Kind, recs[i].Kind)
		}
	}
	// 文件名口径：<dir>/telemetry.ndjson。
	wantPath := filepath.Join(dir, fileName)
	if f.Path() != wantPath {
		t.Errorf("Path() = %q，want %q", f.Path(), wantPath)
	}
}

// TestFileEmitAfterCloseErrs：关后 Emit 返错（不静默丢）；Close 幂等。
func TestFileEmitAfterCloseErrs(t *testing.T) {
	f, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close 应幂等: %v", err)
	}
	if err := f.Emit(core.Record{Kind: core.KindTelRunEnd}); err == nil {
		t.Fatal("关后 Emit 应返错（不静默丢）")
	}
}

// TestNewFileEmptyDirErrs：dir 空 = 配置残缺，fail-close。
func TestNewFileEmptyDirErrs(t *testing.T) {
	if _, err := NewFile("  "); err == nil {
		t.Fatal("空 dir 应返错")
	}
}

// TestNewFileCreatesDir：目录不存在时 MkdirAll 建出来。
func TestNewFileCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b", "tel")
	f, err := NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	defer func() { _ = f.Close() }()
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("目录应已创建: %v", err)
	}
}

// TestFileConcurrentEmit：并发 Emit 行不交错（mutex 下一写一行）。
func TestFileConcurrentEmit(t *testing.T) {
	f, err := NewFile(t.TempDir())
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = f.Emit(core.Record{Kind: core.KindTelToolCall, Turn: i, Tool: "echo"})
		}(i)
	}
	wg.Wait()
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := os.ReadFile(f.Path())
	if err != nil {
		t.Fatalf("读文件: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("并发落盘行数 = %d，want %d（行不得交错丢失）", len(lines), n)
	}
	for i, line := range lines {
		var rec core.Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("第 %d 行坏 JSON（行交错？）: %v（%s）", i, err, line)
		}
	}
}

// TestOTLPEmitErrUnsupported：留位实现诚实报未支持（不静默丢）。
func TestOTLPEmitErrUnsupported(t *testing.T) {
	s := NewOTLP()
	err := s.Emit(core.Record{Kind: core.KindTelRunBegin})
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("OTLP.Emit 应返 ErrUnsupported，得到 %v", err)
	}
}
