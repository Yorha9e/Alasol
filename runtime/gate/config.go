package gateexec

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Yorha9e/alasol/core/gate"
)

// 本文件是门禁配置 / baseline 的**文件 I/O**（执行侧的职责；core/gate 只提供字节级
// ParseConfig / ParseBaseline 与路径拼接 ConfigPath / BaselinePath，不含任何 I/O）。
//
// 配置与 baseline 都是仓库级工件（W9/W9c）：配置缺文件 = 门禁未配置（fail-close），
// baseline 缺文件 = 首建模式。

// LoadConfig 读仓库根 .alasol/gate.json 并解析成 Config。
//
// 文件缺失 → gate.ErrNotConfigured（W9 fail-close：未配置 ≠ 通过；由调用方决定跳过还是拒绝，
// 默认应为拒绝）。其它读错误 / 解析错误原样上抛（不吞成"未配置"）。
func LoadConfig(repoRoot string) (gate.Config, error) {
	path := gate.ConfigPath(repoRoot)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return gate.Config{}, gate.ErrNotConfigured
		}
		return gate.Config{}, fmt.Errorf("gateexec: 读 %s 失败：%w", path, err)
	}
	return gate.ParseConfig(data)
}

// LoadBaseline 读仓库根 .alasol/gate-baseline.json。
//
// **文件是否存在**进入判定（这是本函数存在的理由）：
//
//   - 文件缺失 → 返回**未建立**的 baseline（Established=false）→ Judge 进记录模式
//     （首建：红仓第一次跑靠它记录、不阻断，陷阱 20）；
//   - 文件存在 → Established=true（哪怕里面一条条目都没有：旧文件按"存在即已建立"处理）
//     → 空基线**不是**首建，而是 fail-close 拦下（否则任何失败都会被记成"已知"后放行）。
func LoadBaseline(repoRoot string) (*gate.Baseline, error) {
	path := gate.BaselinePath(repoRoot)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &gate.Baseline{}, nil // 首建：Established 保持 false
		}
		return nil, fmt.Errorf("gateexec: 读 %s 失败：%w", path, err)
	}
	return gate.ParseBaseline(data)
}

// SaveBaseline 把 baseline 写到仓库根 .alasol/gate-baseline.json。
//
// **不写空基线**：没有任何条目时直接返回（不建文件、不覆盖既有文件）——绿仓（exit 0）
// 跑 -record 也不该留下一个空文件：空基线对后续判定毫无用处，还会让"首建"与
// "被清空"混为一谈（后者必须 fail-close 拦下）。
//
// 持久化纪律：先确保 .alasol 目录存在，再**原子写**（临时文件 + fsync + rename），
// 避免写一半崩溃留下损坏的基线——坏基线会被 ParseBaseline 拒绝从而 fail-close，
// 但原子写让"损坏"这本就不该发生。
func SaveBaseline(repoRoot string, b *gate.Baseline) error {
	if b == nil {
		return nil
	}
	if len(b.Entries) == 0 {
		return nil // 空基线不落盘（见上）
	}
	b.Established = true // 写盘 = 基线已建立
	data, err := b.Marshal()
	if err != nil {
		return fmt.Errorf("gateexec: 序列化 baseline 失败：%w", err)
	}
	path := gate.BaselinePath(repoRoot)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("gateexec: 建 %s 失败：%w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".gate-baseline-*.tmp")
	if err != nil {
		return fmt.Errorf("gateexec: 建临时文件失败：%w", err)
	}
	tmpName := tmp.Name()
	if err := writeSyncClose(tmp, data); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("gateexec: rename 到 %s 失败：%w", path, err)
	}
	return nil
}

// writeSyncClose 写全 + fsync + 关闭；任一步失败都清理并返回错误（不留下半截临时文件）。
func writeSyncClose(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("gateexec: 写临时文件失败：%w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("gateexec: fsync 失败：%w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("gateexec: 关闭临时文件失败：%w", err)
	}
	return nil
}
