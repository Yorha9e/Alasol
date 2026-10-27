package registry

import (
	"errors"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// RegisterTool 是工具扩展的注册便捷方法（D12/D17）：
//
//   - meta 由 contracts.Tool 派生：Name=tool.Name、Kind="tool"、Reloadable=false
//     （工具是接口实现，换实现通常伴随重启，E6）；其余字段不携带（Capabilities 为空）；
//   - Impl 存的是 core.ToolExecutor（执行器进程内接口，D1）；
//   - **完整工具描述被保留**（P2-10）：Entry.Tool 带回 contracts.Tool 原文
//     （Schema / SideEffect / Approvable / Idempotent / Replay / Description …），
//     调用方不必再自己存一份——跨边界只传 contracts.Tool，执行器是进程内接口；
//   - **换 ToolExecutor 实现不需要改任何已有文件**——只要 Tool 名不变、重新注册即可（酸测试）。
func (r *Registry) RegisterTool(tool contracts.Tool, exec core.ToolExecutor) error {
	if r == nil {
		return errors.New("registry: 注册表为 nil")
	}
	if strings.TrimSpace(tool.Name) == "" {
		return errors.New("registry: tool.Name 不能为空")
	}
	if exec == nil {
		return errors.New("registry: ToolExecutor 不能为 nil（拒绝注册无实现的工具）")
	}
	t := tool // 值拷贝：register 内部再深拷贝一次（Schema 字节）
	return r.register(contracts.ExtensionMeta{
		Name:       tool.Name,
		Kind:       ExtensionKindTool,
		Reloadable: false,
	}, exec, &t)
}
