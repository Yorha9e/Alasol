package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// 本文件提供从**目录**加载纯数据能力包的面（W13：能力包是可分发的纯数据，可被任何
// 语言写 / 组合 / 追加）。目录里每个 `*.json` 是一个 contracts.Workflow。
//
// 里程碑部件（W14）是 Go 代码，无法从 JSON 来——所以目录加载出来的包**不带部件**。
// 要挂部件，用 NewPackage / Register 显式附加（见 tower 包的内建注册）。

// LoadDir 加载一个目录下的所有 `*.json` 能力包（按文件名排序，确定性）。
//
// 语义：
//   - 目录不存在 → 返回 (nil, nil)（"没有配置目录"是合法状态，不是错误）；
//   - 某个 `*.json` 解析失败 → **整体失败**（fail-close：损坏的能力包可能正是用户
//     指望生效的那一个，静默跳过会让行为与预期不符，宁可明确报错并点名文件）；
//   - 非 `.json` 文件、子目录 → 忽略。
//
// 返回的包都不带里程碑部件（数据来自文件，部件是代码）。
func LoadDir(dir string) ([]*Package, error) {
	d := strings.TrimSpace(dir)
	if d == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(d)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("workflow: 读能力包目录 %s: %w", d, err)
	}
	// 收集并排序 *.json（确定性加载序）。
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := make([]*Package, 0, len(names))
	for _, name := range names {
		path := filepath.Join(d, name)
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil, fmt.Errorf("workflow: 读能力包文件 %s: %w", path, rerr)
		}
		p, perr := ParsePackage(data)
		if perr != nil {
			return nil, fmt.Errorf("workflow: 能力包文件 %s: %w", path, perr)
		}
		out = append(out, p)
	}
	return out, nil
}

// LoadRegistry 建一个注册表并把 dir 下的能力包 + 额外内建包一并登记。
//
// builtins 先登记（内建包优先占 id/命令），dir 包后登记；任一重名 → 整体失败
// （fail-close：不许静默遮蔽）。dir 为空 / 不存在 → 只登记 builtins。
func LoadRegistry(dir string, builtins ...*Package) (*Registry, error) {
	reg := NewRegistry()
	for _, p := range builtins {
		if err := reg.Register(p); err != nil {
			return nil, err
		}
	}
	pkgs, err := LoadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, p := range pkgs {
		if err := reg.Register(p); err != nil {
			return nil, err
		}
	}
	return reg, nil
}
