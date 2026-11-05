package host

// 本文件是 **L2 per-session 执行上下文 · 件 2（宿主接线 + 持久化）**的宿主侧装配面：
// 会话工作区（session workspace）从哪来、怎么落库、怎么变成 loop.Config.Workspace。
//
// # 会话工作区是什么
//
// **一个会话在哪个目录里干活**：工具相对路径的锚点 + bash / git 的工作目录
// （件 1 已在 core/loop + runtime/tools 落地：core.ExecContext 经 ctx 带给执行器）。
// 本文件只做**宿主接线**：把"这个 session 的工作区是什么"这个**会话事实**解析出来，
// 交给 loop.Config.Workspace（构造期固定 = 会话事实，不是每次调用各填一份的参数）。
//
// # 为什么落 config.changed 而不是只写 h.specs
//
// h.specs 是**纯内存登记**（host/discovery.go 的 SessionSpec）：进程重启即丢。
// 而"懒接管"回来的 session（另一个进程/上一次进程留下的）若只能靠 specs 找工作区，
// 就会**悄悄换工作区**——工具在 A 目录建的会话，重启后文件落到 B 目录，事件流里的
// 路径与磁盘上的落点对不上（审计看着像同一会话在两个地方同时干活）。
//
// 故工作区走**会话配置面**：config.changed{exec:{cwd,worktree}} → fold 表 → 重启回放
// 即得（F5 纪律：事件流是唯一事实源，当前态是可重建投影）。创建时那条事件由本文件
// 经**既有写路径**（recordConfigChanged：bus.Publish + 确定性 id）落，与 G-c/G-d/G-e/G-h
// 的配置变更**同一套机制、同一套审计口径**——不另开一条"工作区专用"的私有通道。
//
// # 生效链（与其它配置面同构，但**没有 hub 默认这一层**）
//
//	持久真相：config.changed 的 exec 组（fold）    ← 优先
//	回填源：  SessionSpec（创建时登记，纯内存）      ← fold 缺省时兜底
//	→ 解析 worktree 名到实际路径 → loop.Config.Workspace
//
// **唯一取值口** = ResolveSessionWorkspace（导出）：host 的 loop 装配与 CLI 的 resume
// 重放共用同一份解析——两处各写一份，迟早漂移成"serve 里工具落在 A、resume 重放
// 落在 B"（静默错位：重放报成功，文件却落错地方）。
//
// **hub 级 Config.CWD 不做默认工作区**（有意为之）：Config.CWD 是**自动 gate 的
// repo_id 推导输入**（K12/S14），不是工具的工作目录。把它当默认 workspace 会**静默
// 改变既有部署的工具行为**——今天所有没配工作区的 session 都在**进程 cwd** 里跑，
// 一旦拿 Config.CWD 当默认，它们的相对路径落点就变了（而且没人要求过这件事）。
// 故：三者（fold.exec / spec / hub 级 CWD）都空 → Workspace = "" → 件 1 的零行为
// 变化路径（相对路径按进程 cwd、bash 继承进程 cwd）。
//
// # fold ⊕ spec 的取舍
//
// 两者**一致是常态**（创建时落的那条事件写的就是 spec 的值）。不一致时**以 fold 为准**
// ——config.changed 是**持久真相**（跨重启、跨进程可审计），spec 只是创建时的登记
// （进程内便利）。每个子字段**各自**判"fold 有没有说话"（Written）：fold 写了空串 =
// **清除**（不锚定），fold 没写 = 沿用 spec 的值。
//
// # cwd 与 worktree 互斥（写时 fail-close）
//
// 两者是**同一件事的两种写法**：worktree 优先 → "已绑 worktree 又改 cwd"会让 cwd
// **静默 no-op**（202 零告警，工具还在 worktree 里跑）。故写面**显式拒**这种提交
// （400 bad_config，文案点名"要改用 cwd 需同时把 exec.worktree 写空串清除"）——
// 把坑变成**显式契约**，而不是让它静默生效一半。见 validateExecWorkspaceAmbiguity。
//
// # 边界（v1）
//
//   - **sandbox / remote 执行面不支持 per-session 工作区**：创建时**拒绝**（fail-close），
//     不静默回落成"照旧在进程 cwd 跑"（那会让用户以为工作区生效了）。见 CreateSession。
//     **换面重启**（会话在 local 下建、宿主重启时加了 -sandbox/-remote）→ **不硬拒**
//     （旧会话全废太不合算），但**告警**一条（工作区只是路径锚点，隔离/远端语义由
//     执行面决定）。见 execWorkspaceResolve。
//   - **fork（/btw 旁支）不继承**父 session 的工作区：与 model / tools / context 同口径
//     （fork 出来的是新 session，配置继承 hub 默认 = 不锚定）。见 host/fork.go。
//   - **工作区是构造期值**：运行期改 exec → **失效并重建**缓存的 loop（见
//     recordConfigChanged）→ 新工作区对**之后重建的 Run** 生效；**已经在跑的那一轮
//     不中断**（它用的是本 loop 构造时定下的工作区——事件流里的路径与那一轮的实际
//     落点因此仍自洽）。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
	"github.com/Yorha9e/alasol/runtime/identity"
)

// ---------------------------------------------------------------------------
// ① 生效工作区解析（fold ⊕ spec → worktree 路径）
// ---------------------------------------------------------------------------

// ResolveSessionWorkspace 是**会话工作区的唯一取值口**（host 与 CLI 共用）。
//
// 参数：
//
//   - ex：fold 出来的 exec 组（core/sessioncfg.ExecState；**持久真相**）；
//   - specCwd / specWorktree：创建时登记的执行面（**纯内存**；CLI 侧没有 specs →
//     传空）；
//   - worktreePath：worktree 名 → 实际路径的解析口（nil = 不解析 → 回落 cwd）。
//
// 返回 "" = **不锚定**（工具按进程 cwd = 件 1 的零行为变化路径）。
//
// 口径（见文件头"生效链"）：
//
//	① fold 的 exec 组：每个子字段各自判 Written ——
//	   写了（值或空串）= fold 说话（**空串 = 清除该项**）；没写 = fold 没说 → 沿用 spec；
//	② spec（创建时登记）：fold 缺省时的回填源；
//	③ worktree 名 → 实际路径（受管根查询）；解析不到 → 回落 cwd。
//
// **为什么 worktree 优先于 cwd**：两者是同一件事的两种写法（路径 / 受管根里的具名
// 工作区），常态下二者一致（创建时落盘的 cwd 就是该 worktree 的路径）。真不一致时
// 以 worktree 为准——它是**受管根里的具名实体**（路径由受管根决定），而 cwd 只是
// 一次手写的路径。worktree 解析不到（受管根里没有它 / 已被删）时回落 cwd，cwd 也
// 没有 → ""（不锚定）。
//
// **为什么是导出的共享函数**：host 的 loop 装配与 CLI 的 resume 重放**必须**用同一套
// 解析——两处各写一份，迟早漂移成"serve 里工具落在 A、resume 重放落在 B"（静默错位：
// 重放报成功，文件却落错地方）。
func ResolveSessionWorkspace(ex sessioncfg.ExecState, specCwd, specWorktree string, worktreePath func(name string) (string, bool)) string {
	cwd := specCwd
	if ex.Cwd.Written {
		cwd = ex.Cwd.Value
	}
	worktree := specWorktree
	if ex.Worktree.Written {
		worktree = ex.Worktree.Value
	}
	if worktree != "" && worktreePath != nil {
		if path, ok := worktreePath(worktree); ok {
			return path
		}
	}
	return cwd
}

// execWorkspaceResolve 是**宿主侧**的解析（共享取值口 + 执行面告警）。
//
// 告警（P2-1）：会话有工作区、而执行面不是 local（sandbox / remote）→ 打一条 Warn。
//
// 为什么只告警不硬拒：创建时**已经**拒过（fail-close，见 CreateSession），这里挡的是
// **换面重启**的场景（会话是在 local 下建的，宿主重启时加了 -sandbox / -remote）——
// 为此拒跑整个宿主太不合算（旧会话全废），而静默照跑又会让用户以为工作区的语义在
// 沙箱/远端也完全成立（其实**有限**：隔离由执行面决定，工作区只是路径锚点）。
//
// ex 由调用方取（fold 表自带锁；调用方不要再持 h.mu 做 DB I/O——见 loopFor 的扫平位置）。
func (h *Hub) execWorkspaceResolve(ex sessioncfg.ExecState, specCwd, specWorktree string) string {
	if h == nil {
		return ""
	}
	ws := ResolveSessionWorkspace(ex, specCwd, specWorktree, h.worktreePath)
	if ws != "" && !h.hubExecWorkspaceSupported() {
		hubLog.Warn("per-session 工作区在 sandbox/remote 执行面上语义有限（v1）："+
			"工作区只是路径锚点（相对路径 / bash 工作目录），隔离与远端语义由执行面决定",
			"workspace", ws, "sandbox", h.cfg.Sandbox, "remote", h.cfg.Remote)
	}
	return ws
}

// sessionSpecExec 取该 session 在 specs 里登记的执行面（cwd / worktree 名）。
//
// 纯内存（进程内登记，重启即丢）——故它只是 fold 缺省时的**回填源**，不是持久真相。
func (h *Hub) sessionSpecExec(sid contracts.Ulid) (cwd, worktree string) {
	if h == nil {
		return "", ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	spec := h.specs[sid]
	return spec.CWD, spec.Worktree
}

// worktreePath 把受管 worktree 名解析成实际路径（受管根查询；未登记 → ok=false）。
//
// 走 hub 的 worktree 管理器（host/worktree.go 的既有查询口）——登记表是**进程内活
// 状态**，但启动时会从磁盘上的 git 元数据重建（runtime/worktree 的 Rebuild），
// 故重启后仍能解析（受管根没被删的话）。
//
// 返回**规范化形态**（identity.CanonicalPath：解析符号链接/junction + 归一大小写与
// 分隔符），与创建时落进 config.changed 的 exec.cwd 同一形态——这样"配置里显示的
// 工作区"与"工具实际落点"是同一个字符串。规范化失败（目录已被删等）→ 退回原始
// 路径（宁可给个指过去的路径，也不因为一次路径查询失败就把整次调用判死）。
func (h *Hub) worktreePath(name string) (string, bool) {
	if h == nil || h.worktrees == nil {
		return "", false
	}
	wt, ok := h.worktrees.Get(strings.TrimSpace(name))
	if !ok {
		return "", false
	}
	if canonical, err := identity.CanonicalPath(wt.Path); err == nil {
		return canonical, true
	}
	return wt.Path, true
}

// DefaultWorktreeRoot 派生受管 worktree 根目录（缺省 = <事件库所在目录>/worktrees）。
//
// **唯一派生口径**：host 装配（host.New）与 CLI resume（cmd/alasol）都用它——两边
// 派生出的根不一致的话，CLI 就解析不到 hub 建的 worktree（工作区静默回落 cwd）。
func DefaultWorktreeRoot(dbPath string) string {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(dbPath), "worktrees")
}

// ---------------------------------------------------------------------------
// ② 创建时持久化（config.changed{exec}）
// ---------------------------------------------------------------------------

// recordExecWorkspace 在**创建时**把会话工作区落成一条 config.changed 事件
// （L2 件 2 的持久化入口）。
//
// 走**既有写路径** recordConfigChanged（bus.Publish + 确定性 id + 届数）——不另开
// 私有通道：工作区从此与其它 session 配置**同一套 fold、同一套审计口径**。
//
// agentID = 该 session 的**主 agent**（创建时还没有 loop，故由这里**先登记**一个：
// 事件 envelope 的 agent_id 是必填的，且"这条配置变更归属哪个 agent"要有确定答案）。
// 登记的主 agent id 与事件里的 agent_id 一致 → 后续 loopFor 取主 agent 时拿到的
// 就是它（事件流口径与进程内登记同源）。
func (h *Hub) recordExecWorkspace(ctx context.Context, sid, agentID contracts.Ulid, cwd, worktree string) error {
	if h == nil {
		return errors.New("host: Hub 为 nil")
	}
	if cwd == "" && worktree == "" {
		return nil // 没有执行面 = 无事可落（普通创建零事件）
	}
	exec := &contracts.ExecConfigChange{}
	if cwd != "" {
		v := cwd
		exec.Cwd = &v
	}
	if worktree != "" {
		v := worktree
		exec.Worktree = &v
	}
	return h.recordConfigChanged(ctx, sid, agentID, &contracts.ConfigChangedPayload{Exec: exec})
}

// ---------------------------------------------------------------------------
// ③ 写时校验（exec 组）
// ---------------------------------------------------------------------------

// validateExecChange 校验一次 exec 变更集（包级口径：形状与取值；**不看执行面**——
// 执行面相关的判定在宿主口径那一层，见 validatePromptConfigFor）。
//
// 三条：
//
//  1. cwd 非空 → 必须是**存在的目录**（可用）：工作区是工具的落点，指向不存在的
//     路径 = 每次工具调用都失败（而且失败发生在模型背后）；指向一个**普通文件**同样
//     不行（锚定到文件 = 相对路径无处可落）。
//     **相对路径也接受**（与创建面同口径：相对路径经绝对化后使用——见
//     canonicalizeExecPayload：落进事件的是**规范化后的绝对路径**）。
//  2. worktree 非空 → 必须是个**名字**（不含路径分隔符）：它是受管根里的具名实体，
//     不是路径（路径由受管根解析）；
//  3. 两项都空串 → 合法（= 清除整个执行面 = 回到不锚定）。
func validateExecChange(ex *contracts.ExecConfigChange) error {
	if ex == nil {
		return nil
	}
	if ex.Cwd != nil {
		v := strings.TrimSpace(*ex.Cwd)
		if v != "" {
			info, err := os.Stat(v)
			if err != nil {
				return fmt.Errorf("config.exec.cwd %q 不可用（须是存在的本地目录）: %w", *ex.Cwd, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("config.exec.cwd %q 不是目录（会话工作区必须是目录）", *ex.Cwd)
			}
		}
	}
	if ex.Worktree != nil {
		v := strings.TrimSpace(*ex.Worktree)
		if v != "" {
			if strings.ContainsAny(v, `/\`) || v == "." || v == ".." {
				return fmt.Errorf("config.exec.worktree %q 必须是受管 worktree 的名字（不含路径分隔符）", *ex.Worktree)
			}
		}
	}
	return nil
}

// canonicalizeExecPayload 把 exec.cwd **规范化**（绝对化 + 解析符号链接/junction +
// 归一大小写与分隔符），返回**副本**（不改调用方的结构体）。
//
// 为什么写面也要规范化（**两入口同一口径**，critic P2-5）：创建面（CreateSession）
// 经 identity.CanonicalPath 规范化后再落事件；写面若原样落——
//
//   - **相对路径会随调用点的 cwd 漂移**：同一个 "sub/dir" 在不同进程/不同机器上
//     指向不同地方（重启后工作区悄悄换了位置）；
//   - "配置里显示的工作区"与"工具实际落点"不是同一个字符串（读面对不上账）。
//
// 相对路径按**进程 cwd** 绝对化（与工具侧相对路径的解析同源——都相对进程 cwd）。
// 空串 = 清除该项 → 不规范化（原样落）。
func canonicalizeExecPayload(cfg *contracts.ConfigChangedPayload) (*contracts.ConfigChangedPayload, error) {
	if cfg == nil || cfg.Exec == nil || cfg.Exec.Cwd == nil {
		return cfg, nil
	}
	v := strings.TrimSpace(*cfg.Exec.Cwd)
	if v == "" {
		return cfg, nil // 空串 = 清除该项（不规范化）
	}
	canonical, err := identity.CanonicalPath(v)
	if err != nil {
		return nil, fmt.Errorf("host: 规范化 config.exec.cwd %q 失败: %w", v, err)
	}
	out := *cfg
	exec := *cfg.Exec
	c := canonical
	exec.Cwd = &c
	out.Exec = &exec
	return &out, nil
}

// validateExecWorkspaceAmbiguity 拒掉"**cwd 与 worktree 同时指向工作区**"的提交
// （critic P1-1：worktree 压制 cwd → 运行期改 exec.cwd 静默 no-op）。
//
// 口径（fail-close，把坑变成**显式契约**）：
//
//   - 本次提交带了**非空 cwd**（= 想换工作区到某个路径）；
//   - 且本 session **仍绑着 worktree**（fold 的 exec.worktree 有生效值，或本次又
//     绑了一个）；
//     → **400 bad_config**：文案点名"要改用 cwd 需**同时**把 config.exec.worktree
//     写空串清除"（或只提交 worktree 重新绑定）。
//
// 为什么不在"解析时"再裁决（worktree 优先、cwd 忽略）：那正是静默 no-op 的来源——
// 用户改了 cwd、看到 202、工具却还在 worktree 里跑，**没有任何提示**。
//
// 反过来"只清 worktree"是**允许**的（清掉绑定 → cwd 生效）；"两个都清"也允许
// （回到不锚定）。
func (h *Hub) validateExecWorkspaceAmbiguity(sid contracts.Ulid, ex *contracts.ExecConfigChange) error {
	if h == nil || ex == nil || ex.Cwd == nil {
		return nil
	}
	cwd := strings.TrimSpace(*ex.Cwd)
	if cwd == "" {
		return nil // 本次没给非空 cwd → 不涉及歧义（清空 cwd 或只清 worktree 都合法）
	}
	// 本 session 是否仍绑着 worktree？
	bound := false
	boundName := ""
	if ex.Worktree != nil {
		// 本次提交带了 worktree：非空 = 又绑了一个（同样与 cwd 歧义）；空串 = 清除
		// → 不绑了 → 放行。
		if name := strings.TrimSpace(*ex.Worktree); name != "" {
			bound, boundName = true, name
		}
	} else if h.cfgTable != nil {
		if ex := h.cfgTable.Current(sid).Exec; ex.Worktree.HasValue() {
			bound, boundName = true, ex.Worktree.Value
		}
	}
	if !bound {
		return nil
	}
	return fmt.Errorf("config.exec.cwd 与 config.exec.worktree 是同一件事的两种写法，不能同时指向工作区"+
		"（worktree 优先：worktree=%q 会盖住 cwd=%q，改了 cwd 也不会生效）: "+
		"要改用 cwd 请**同时**把 config.exec.worktree 写空串清除（或只提交 worktree 重新绑定）",
		boundName, cwd)
}

// errExecUnsupportedOnSandbox 是"执行面不支持 per-session 工作区"的错误文案
// （sandbox / remote 执行面下 fail-close，不静默回落）。
//
// 两处共用（创建面 = 新建带工作区的会话；写面 = 给已有会话改工作区）——同一件事的
// 两种入口，故同一句文案。
var errExecUnsupportedOnSandbox = errors.New(
	"per-session workspace 仅支持 local 执行面（v1）：sandbox / remote 执行面下不支持声明或修改会话工作区" +
		"（不静默回落到进程 cwd——那会让用户以为工作区生效了）")

// hubExecWorkspaceSupported 报告本宿主的执行面是否支持 per-session 工作区。
//
// **local** = 支持（工具按会话工作区锚定）；**sandbox / remote** = 不支持（v1）：
// 那两个执行面换掉了整个工具执行环境（受限 token 的沙箱 / 经 ssh 的远端），
// per-session 工作区在它们上面没有确定的语义——**拒绝**好过"看起来生效了"。
func (h *Hub) hubExecWorkspaceSupported() bool {
	if h == nil {
		return true
	}
	return !h.cfg.Sandbox && !h.cfg.Remote
}

// ---------------------------------------------------------------------------
// ④ 落库后的 loop 失效（工作区是构造期值）
// ---------------------------------------------------------------------------

// invalidateLoopForWorkspace 失效某 session 缓存的 loop（工作区变了要重建）。
//
// 为什么需要：Workspace 是 **loop 构造期**的值（件 1 的设计：会话事实，不是每次
// 调用各填一份的参数）。运行期改了 exec（config.changed{exec}）而 loop 还缓存着，
// 新的工作区就**不生效**——用户改了配置却看到老行为（静默失效，最坏的一种）。
//
// 失效手法照 activateWorkflow 的同款（delete h.loops[sid]）：下次 RunPrompt 走
// loopFor 时重建（主 agent id 稳定、agent 树保留——树是 session 活状态，与 loop 的
// 生命周期分开管）。
func (h *Hub) invalidateLoopForWorkspace(sid contracts.Ulid) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.loops, sid)
	h.mu.Unlock()
}
