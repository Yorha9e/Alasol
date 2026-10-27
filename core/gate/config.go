package gate

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Config 是仓库级门禁配置（W9：命令靠注入，不靠探测；仓库级唯一，无默认无分层）。
//
// Command 是**不透明 shell 字符串**：引擎原样交给 shell 执行（runtime/gate），
// 不解析、不区分 typecheck/lint/测试。命令对引擎是黑盒，准确性的责任在写命令的人。
type Config struct {
	Command string `json:"command"`
}

// ErrNotConfigured 表示门禁未配置：仓库根缺少 .alasol/gate.json（W9）。
//
// 语义是 fail-close：未配置 ≠ 通过。由调用方决定"跳过门禁"还是"拒绝"，
// 默认应为拒绝（没有门禁配置就不该放行合并）。
var ErrNotConfigured = errors.New("gate: 门禁未配置（仓库根 .alasol/gate.json 缺失）")

// 门禁在仓库根的约定路径（W9c：仓库级作用域，可提交进版本库）。
const (
	configDirName    = ".alasol"
	configFileName   = "gate.json"
	baselineFileName = "gate-baseline.json"
)

// ConfigPath 返回仓库根下的配置文件路径（纯字符串拼接，无 I/O）。
func ConfigPath(repoRoot string) string {
	return filepath.Join(repoRoot, configDirName, configFileName)
}

// BaselinePath 返回仓库根下的 baseline 文件路径（纯字符串拼接，无 I/O）。
func BaselinePath(repoRoot string) string {
	return filepath.Join(repoRoot, configDirName, baselineFileName)
}

// ParseConfig 解析 .alasol/gate.json 的内容（字节级，无文件 I/O——读文件在 runtime/gate）。
//
// 缺 command 字段 = 配置不完整 → 报错（不兜底、不猜默认命令，W9）。
func ParseConfig(data []byte) (Config, error) {
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("gate: 解析 gate.json 失败：%w", err)
	}
	if strings.TrimSpace(c.Command) == "" {
		return Config{}, fmt.Errorf("gate: gate.json 缺少 command 字段（W9：命令靠注入，无默认）")
	}
	return c, nil
}

// Marshal 序列化配置（供写入 .alasol/gate.json；字节级，无文件 I/O）。
func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}
