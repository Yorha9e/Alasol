package local

import (
	"context"
	"fmt"
	"os"

	"github.com/Yorha9e/alasol/core"
)

// FS 是 core.FSCapability 的本机实现（零值可用）。
//
// 并发安全：无共享状态，所有方法都是无状态的 os 调用。
// 路径**原样**传给 os 包——守卫在策略层（见包 doc「路径守卫是策略层的职责」）。
type FS struct{}

// 编译期断言：FS 必须满足 core.FSCapability。
var _ core.FSCapability = FS{}

// ReadFile implements core.FSCapability。
//
// ctx 已结束时直接返回错误（不碰盘）：调用方取消要能立刻生效，
// 否则一个大文件的读取会把取消信号压后很久。
func (FS) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("local: 读取文件失败（path=%s）：%w", path, err)
	}
	return data, nil
}

// WriteFile implements core.FSCapability。
//
// 权限 0o644（尊重 umask）。**不是原子写**（不写临时文件再 rename）：
// v0 已知边界——写到一半崩溃会留下截断的文件。要原子性请在策略层/工具层
// 用"写临时文件 + rename"的两段式，或等 V1 沙箱实现。
func (FS) WriteFile(ctx context.Context, path string, data []byte) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("local: 写入文件失败（path=%s）：%w", path, err)
	}
	return nil
}

// Stat implements core.FSCapability。
func (FS) Stat(ctx context.Context, path string) (core.FileInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return core.FileInfo{}, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return core.FileInfo{}, fmt.Errorf("local: stat 失败（path=%s）：%w", path, err)
	}
	return core.FileInfo{
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		ModTime: fi.ModTime(),
	}, nil
}

// ListDir implements core.FSCapability。
//
// 返回**条目名**（不是完整路径）——调用方自己拼，避免本包对"路径怎么拼"做假设。
// os.ReadDir 已按文件名排序，返回值因此是确定性的（测试可断序）。
func (FS) ListDir(ctx context.Context, path string) ([]string, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("local: 列举目录失败（path=%s）：%w", path, err)
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, nil
}

// ctxErr 把 ctx 状态转成带定位的错误（nil ctx 视为 Background，绝不 panic）。
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("local: 上下文已结束（%w）", err)
	}
	return nil
}
