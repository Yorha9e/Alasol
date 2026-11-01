package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/core"
)

// fileName 是 file sink 的落盘文件名（单文件 NDJSON，所有 session 追加进同一文件——
// record 自带 session_id，消费方按字段过滤；比 per-session 文件少一组 fd 与锁）。
const fileName = "telemetry.ndjson"

// File 是 telemetry 的 **file 落盘实现**（NDJSON：一行一条 core.Record）。
//
// # 写路径
//
// 每条 = json.Marshal(Record) + "\n"，在一把 mutex 下一次性写出（行与行不交错；
// 并发 Emit 安全）。**不 fsync**——telemetry 是可丢弃的观测出站口（不是事实源、
// 不是崩溃恢复面），逐条同步落盘是把观测的持久性抬到与事实同级；进程死了丢尾部
// 几条可接受（与 WAL 的"轮次边界才 fsync"不同：这里连边界都没有，整体 best-effort）。
//
// # 关 = Close
//
// Close 后 Emit 返错（不静默丢）。幂等：重复 Close 返回 nil。
type File struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// NewFile 在 dir 下建（或打开追加）`<dir>/telemetry.ndjson`。
//
// dir 空 = 配置残缺（fail-close，拒绝装配——"开了 file 却没有目录"不是可静默降级的状态）。
// MkdirAll 建目录；建不出来 = "显式要的能力没起来" → 返错（与 host.New 建 WAL 目录同口径）。
func NewFile(dir string) (*File, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("telemetry: file sink 需要非空目录")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("telemetry: 建目录 %q: %w", dir, err)
	}
	path := filepath.Join(dir, fileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("telemetry: 打开 %q: %w", path, err)
	}
	return &File{f: f, path: path}, nil
}

// Path 返回落盘文件完整路径（诊断/测试用）。
func (f *File) Path() string {
	if f == nil {
		return ""
	}
	return f.path
}

// Emit 追加一条 NDJSON 记录（core.Sink 实现）。
func (f *File) Emit(rec core.Record) error {
	if f == nil {
		return errors.New("telemetry: File 为 nil")
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("telemetry: 序列化 record: %w", err)
	}
	raw = append(raw, '\n')

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f == nil {
		return errors.New("telemetry: file sink 已关闭")
	}
	if _, err := f.f.Write(raw); err != nil {
		return fmt.Errorf("telemetry: 写 %q: %w", f.path, err)
	}
	return nil
}

// Close 关闭文件（幂等；关后 Emit 返错）。
func (f *File) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f == nil {
		return nil
	}
	err := f.f.Close()
	f.f = nil
	if err != nil {
		return fmt.Errorf("telemetry: 关 %q: %w", f.path, err)
	}
	return nil
}
