package loop

import (
	"context"

	"github.com/Yorha9e/alasol/core"
)

// 本文件是 L2 per-session 执行上下文（件 1）的**loop 侧缝**：loop 持有"会话工作区"
// （Config.Workspace，构造期固定），每次工具调用把它经 ctx 带给执行器。
//
// # 为什么是"调用时经 ctx 注入"而不是"注册时绑定"
//
// 工具执行器在宿主里**只注册一份**（tools.NewRead(fs) / NewWrite(fs) / NewBash(exec)），
// 注册表里没有 per-session 的副本——这是刻意的（一份实现、多个 session 共用，改一处
// 全体生效）。但"本次在哪个工作区跑"是**每次调用才知道**的事实（会话各不同）。
// 于是分工是：执行器注册一份（能力面不变），**执行作用域走 ctx**（每次调用一份）。
//
// # 两个执行点（漏一个 = 行为分叉）
//
//   - **执行点 A**：toolcall.go 的 runToolCall —— 正常的工具调用路径；
//   - **执行点 B**：replay.go 的 Replay —— 崩溃续跑的**重放**路径。重放的是"上一次
//     进程发起的同一个已授权操作"，它同样要在**同一个工作区**里落地；漏注入 = 重放
//     把文件写到别处（比不重放更糟：结果是错的，且没人发现）。
//
// 两条路径都**只加 ctx 值**，不碰事件流：X1 三段写路径与落盘序一字不动
// （落盘只看 ctx 生死，不看 ctx 里带了什么值）。

// execContext 取本 loop 的执行作用域（会话工作区；空 = 不锚定）。
//
// 为什么每次都新建一个结构体而不是缓存指针：ExecContext 是**值类型**（core 侧刻意
// 如此——它小、不可变、复制安全），缓存不缓存没有区别，缓存反而多一处可被改写的地方。
func (l *Loop) execContext() core.ExecContext {
	if l == nil {
		return core.ExecContext{}
	}
	return core.ExecContext{Workspace: l.workspace}
}

// withExecScope 把执行作用域叠进 ctx（两个执行点共用这一条路径——避免两处各写一遍
// 拼装逻辑，将来加字段时只改一处）。
func (l *Loop) withExecScope(ctx context.Context) context.Context {
	return core.WithExecContext(ctx, l.execContext())
}
