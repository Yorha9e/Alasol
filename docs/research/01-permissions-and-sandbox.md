# 权限与沙箱模型（深挖）

> 目的：给框架定一套"模型能干什么 / 能干到多大范围 / 什么时候要问人"的三层机制，作为所有工具、subagent、容器、UI 的地基。
> 状态：调研 + 提案草案，待拍板。

---

## 1. 核心结论（先对齐这 8 条）

1. **三层不可互相替代**：决策层（提示模型自限）→ 策略层（规则 + 人审）→ 强制层（OS 沙箱）。Claude Code 官方文档的措辞最准确：权限规则"由 Claude Code 强制执行，而不是由模型强制执行"；沙箱限制防止"即使提示注入绕过 Claude 的决策制定"。
2. **沙箱管能力边界，审批管确认时机**。Codex 的表述：前者决定"能碰到多大范围"，后者决定"动手前问不问你"。两者缺一不可。
3. **默认拒绝（fail-closed）**：没配置 = 不能干。反过来（默认全开）的代价是"忘了配某个敏感工具的限制，它就裸奔了"。
4. **规则求值顺序固定**：deny → ask → allow，首个匹配决定结果，**规则特异性不改变顺序**。因此 deny 里不能写 allow 例外——宽泛的 `Bash(aws *)` 会连带阻止更具体的 `Bash(aws s3 ls)`。
5. **被 deny 的"裸工具名"应从模型可见工具集中移除**（省 token + 减少误调用）；而 `Bash(rm *)` 这种参数化 deny 保留工具可见性、只在调用时拦截。两者的区别是刻意的。
6. **沙箱只覆盖"能执行任意代码"的路径**。Claude Code 文档明确：按命令的沙箱**不覆盖**其他内建工具（Read/Edit/WebFetch）、MCP server、hooks——这些在宿主进程内无约束运行。要全覆盖，必须把**整个 agent 进程**放进 sandbox runtime / dev container。
7. **子 agent 的权限只能收窄不能放宽**。deny 可继承，allow 不可继承。OpenCode 的子 agent 从父会话派生权限并默认禁止再调用 task；AgentScope 的 `mergeParentDenyRulesIntoSlot` 明确"deny 只增不减"。
8. **Windows 是最贵的战场**。macOS 有 Seatbelt、Linux 有 bubblewrap/seccomp，Windows 没有一个能直接映射到 agent 工作负载的原生隔离组件——OpenAI 评估过 Windows Sandbox、MIC、AppContainer 后全部放弃，自研了两代方案。

---

## 2. 三层模型

| 层 | 回答什么 | 机制 | 谁执行 | 能否被 prompt injection 绕过 |
|---|---|---|---|---|
| L0 决策层 | 模型"应该"怎么做 | System prompt / AGENTS.md / skill 提示 | 模型自己 | **能**（提示注入是它的输入） |
| L1 策略层 | 允许做哪些动作 | allow/deny/ask 规则、审批流、hook 拦截 | Harness 代码 | 不能（硬拦截） |
| L2 强制层 | 动作最多能碰到什么 | OS 沙箱、网络隔离、文件 ACL | 内核 | 不能 |

**结论**：L0 永远不能作为安全边界；L2 是最后一道防线；L1 决定"要不要问人"。三层都要有，缺一层会出现特定漏洞：
- 只有 L0 → 提示注入即完全失守
- 只有 L1 → 模型绕过决策直接调 API？L1 在 harness 内仍能拦；但若 L1 漏判（规则解析错误）就没有兜底
- 只有 L2 → 用户体验差（所有越界都失败而不是询问），且无法覆盖进程内工具

---

## 3. L1 策略层：规则模型

### 3.1 规则语法
```
Tool            # 裸工具名，匹配所有调用
Tool(specifier) # 参数化匹配，如 Bash(git push *)、Read(./.env)
```
规则类型：`allow` / `deny` / `ask`。

### 3.2 求值链（参考 Claude Code 与 AgentScope 的六层链）
```
1. 工具级 deny        → 拒绝
2. 工具级 ask         → 请求审批
3. 工具自检           → 可选，如危险路径检查、副作用检查（bypass-immune）
4. 工具级 allow       → 放行
5. 模式兜底           → BYPASS / DEFAULT / DONT_ASK
6. 默认               → ASK（无人可应答时转 DENY）
```
优先级原则：**deny 永远赢**；子 agent 继承父级 deny；工具自检可越过 allow（防止"配置写对了但工具自己知道有风险"被放行）。

### 3.3 规则来源分层（后者覆盖前者，managed 不可被覆盖）
```
managed（组织下发，最高）
  > CLI 参数
  > 项目级 settings.json（提交进仓库，团队共享）
  > 本地级 settings.local.json（不进仓库）
  > 用户级 ~/.claude/settings.json
  > 会话内临时批准（once / always）
```

### 3.4 两种 deny 语义（必须区分）
| 写法 | 行为 | 适用 |
|---|---|---|
| `Bash` | 工具从模型上下文里**完全移除**，模型看不见 | 彻底不提供这个能力 |
| `Bash(rm *)` | 工具保留可见，调用被拦截 | 能力相关但要拦特定用法 |

对 MCP 工具的参数级规则需要特殊前缀（如 `mcp__<server>__<tool>(...)`），且部分实现会跳过含括号的 `mcp__` 规则——这是已知的实现细节差异，接入时要实测。

### 3.5 审批态与"批准"作为一等公民
- 审批决策是 `approve` / `reject` / `always`（写入会话内 approved 规则）/ `once`（仅本次）
- **建议规则（suggested rules）**：审批弹窗时同时给出"本次允许"和"以后允许 `Bash(git status)`"两个选项，让用户一次性把宽泛的 ask 收敛成窄规则
- **`dontAsk` 模式**：无人值守（定时任务、CI）时，ask 直接转 deny，而不是挂起等待——"宁可不做也不错"。这是长时程自治必须内置的语义。
- 审批请求本身要写进 session log（可审计、可回放）

### 3.6 权限模式预设
| 产品 | 预设 |
|---|---|
| Codex | `read-only` / `workspace-write`（本地默认）/ `danger-full-access` |
| Claude Code | `default` / `acceptEdits` / `plan` / `auto` / `bypassPermissions`（可被 managed 设置禁用） |
| AgentScope | `EXPLORE`（只读）/ `ACCEPT_EDITS`（自动放行工作目录内编辑）/ `DEFAULT` / `DONT_ASK` / `BYPASS` |

**推荐默认组合**：`workspace-write` + `on-request`（Codex 官方口径）。"完全访问 + 从不问"等价于 `--yolo`，只有在外层已有容器/VM 隔离时才可用。

**审批疲劳**是真实问题：Codex 引入了"自动审查子代理"来做智能审批。我们的框架若走长时程自治路线，人工审批必须与"无人值守"模式明确分离，不能混用。

---

## 4. L2 强制层：沙箱后端选型

### 4.1 现状对照
| 平台 | 可用机制 | 备注 |
|---|---|---|
| macOS | **Seatbelt**（`sandbox-exec`） | 开箱即用，行业共识默认；支持 Unix domain socket 白名单 |
| Linux | **bubblewrap**（namespace + seccomp）、**Landlock + seccomp**、nsjail（namespaces + cgroups） | bubblewrap 需注意 Ubuntu 24.04 的 AppArmor `unprivileged_userns` 限制，需额外 profile；网络代理常配 `socat` / `slirp4netns` / iptables(nf_tables) |
| Windows | **无标准方案**，三条路（见下） | 需自研或绕开 |

### 4.2 Windows 三条路
| 路线 | 代表 | 机制 | 代价 |
|---|---|---|---|
| A. 自研受限令牌 | Codex v1 "unelevated" | `CreateRestrictedToken` + 合成 SID `sandbox-write` + ACL 写限制；网络用环境级离线控制 | 需要自研 native helper，维护成本高 |
| B. 专用账户 + 防火墙 | Codex v2 "elevated" | 本地账户 `CodexSandboxOffline` / `CodexSandboxOnline` + 受限令牌 + 防火墙规则 + 专用桌面 | 需管理员权限部署；企业策略可能阻止 |
| C. 容器化绕开 | Cursor/Claude Code 在 Windows 的做法 | 把 agent 整体放进 WSL2 / dev container / Docker | 跨文件系统性能损失、工具链需重建 |

参考产品对 Windows 的态度：
- **Anthropic sandbox-runtime**（npm `@anthropic-ai/sandbox-runtime`，Apache-2.0，Claude Code 内部在用）：macOS 走 Seatbelt、Linux 走 bubblewrap，**原生 Windows 官方文档标注不支持**；有二级资料提到其覆盖 "Windows account + WFP"，需核对实际版本能力
- **GitHub Copilot CLI**：Windows 走 **ProcessContainer 的 BaseContainer 层**，明确**不使用 AppContainer 回退**；若 Windows 构建版本不提供 BaseContainer，则直接报告不支持沙箱化
- **Google Antigravity CLI**：Linux 用 nsjail、macOS 用 `sandbox-exec`、Windows 用 AppContainer

OpenAI 放弃原生方案的理由值得记录：AppContainer 要求**预声明能力**、MIC 要求改变对象信任级别，都与"agent 按需调用任意开发工具"的动态工作流不兼容。这是设计约束，不是实现偷懒。

### 4.3 对我们的选型含义
- 若选 **TS 栈**：`@anthropic-ai/sandbox-runtime`（Apache-2.0）是最省力起点 —— macOS/Linux 立刻可用，Windows 走 WSL2/devcontainer 兜底或自研
- 若选 **Rust 栈**：Codex 沙箱是开源参考实现（同一仓库），可直接借鉴受限令牌 + ACL + WFP 的组合
- 若 **Windows 是一等目标**：L2 在 Windows 上建议直接用"整个 agent 进程进容器"（把 L2 边界上移），而不是逐命令沙箱；这样也顺带解决了"MCP server / hooks 不受沙箱约束"的问题
- 通用建议：**默认在沙箱内跑所有命令**（Claude Code 的 `autoAllowBashIfSandboxed` 语义：沙箱内 bash 免提示，沙箱边界替代整体工具提示；但内容范围的 ask 规则、显式 deny、对关键路径的 rm/rmdir 仍然生效）

### 4.4 沙箱覆盖不到的东西（必须明确写进威胁模型）
- 进程内工具（Read/Edit/WebFetch）不受按命令沙箱约束
- MCP server、hooks 作为独立进程在宿主上无约束运行
- 密钥/凭据文件的读取（除非显式 denyRead）

---

## 5. 网络隔离

- **默认拒绝**是共识：Codex `workspace-write` 默认禁网（理由是降低 prompt injection、数据外泄、误连外部资源的风险，不是性能考虑）
- 配置面：`allowedDomains` / `deniedDomains` / `strictAllowlist` / `allowUnixSockets` / `allowLocalBinding` / 代理端口
- Unix domain socket 白名单（`docker.sock`、编辑器 IPC socket）需要单独开关
- 我们的框架应把"网络策略"抽成独立于沙箱后端的策略对象：同一套规则可映射到 seatbelt profile / bwrap+iptables / WFP / 容器 egress policy

---

## 6. 凭据与秘密

- 沙箱配置支持 `denyRead` 指定敏感路径（密钥目录、`.env`、`~/.ssh`、云凭据目录）
- Codex 的做法：敏感路径（含 **Git 元数据目录**）用 ACL 强制保护
- 凭据注入：按作用域（session / agent / task）注入，优先用只读挂载或代理转发而非明文环境变量
- **日志脱敏**：secret 不进 session log、不进 OTEL span 属性、不进错误回传给模型的文本
- 最小权限：沙箱账户/令牌只拿到必要能力（Codex 的专用低权限账户思路）

---

## 7. 多 Agent 场景下的隔离（三种正交维度）

| 维度 | 手段 | 隔离内容 |
|---|---|---|
| 策略 | 权限继承 + 收窄 | 子 agent 只能拿到父级允许的子集，deny 强制继承 |
| 目录 | `git worktree` | 文件系统级隔离，多个 agent 并行改代码不互相污染 |
| 进程 | 沙箱 / 容器 | OS 级强制边界，兜住策略层漏判 |

**git worktree 实操要点**（Windows 尤其注意）
- 一个分支同时只能被一个 worktree 检出 → 分支名可作为并发预占锁（`feat/REQ-123`），创建失败即重新分配编号
- worktree 是**全新检出**：依赖要重装，`.env` 等被 gitignore 的文件不会带过去 → 用 `.worktreeinclude` 之类的机制显式带入
- 未提交改动**不会**同步到其他 worktree
- Windows：删除 worktree 不会删掉外部文件；目录内若是 NTFS junction / 目录符号链接，**只删链接、保留目标**
- 隔离解决"工作区互相污染"，**不解决"业务逻辑冲突"**——最终合并仍需处理

**worktree 不能替代沙箱**：worktree 是目录隔离，不防 agent 访问目录外的任意路径；也不能防 `rm -rf $HOME`。两者是正交的。

---

## 8. 不可信输入与提示注入

必须标注为"不可信数据"的所有输入面：MCP server 返回、网页抓取、issue/PR 内容、代码仓库里被 agent 读到的注释与文档、终端输出。

防御原则：
1. L2 沙箱是最后一道防线（"即使提示注入绕过模型的决策制定"）
2. 工具输出在注入模型上下文时带来源标记
3. 高危动作（写系统路径、发网络请求、发布、提交推送）永远走 L1 审批
4. 沙箱内命令免提示的前提是"沙箱边界已经足够小"——边界太大时免提示就是免审批

---

## 9. 我们的落地方案（草案，待拍板）

### 9.1 三个抽象
```ts
// 策略：纯函数，可单测，可脱离进程运行
interface Policy {
  evaluate(req: ToolRequest, ctx: PolicyContext): Decision;
  // Decision = { kind: "allow" } | { kind: "deny", reason }
  //          | { kind: "ask", prompt, suggestedRules }
}

// 强制：OS 级后端，可替换
interface Sandbox {
  readonly platform: "darwin" | "linux" | "win32";
  run(cmd: CommandSpec, limits: Limits): Promise<SandboxResult>;
  readonly network: NetworkPolicy;
}

// 人审：应答来源可插拔（TUI / GUI / ACP client / headless）
interface Approver {
  request(q: ApprovalRequest): Promise<ApprovalResponse>;
  // 无人在场时必须返回 deny，而不是挂起
}
```

### 9.2 关键设计约束
- **Policy 不依赖模型输出**：纯输入（工具名 + 参数 + 环境）→ 纯输出（决策）
- **规则解析独立成库**：路径 glob 要按"路径每一段向后尝试匹配"，否则 `finance/*` 这类禁区写下来会永远匹配不上 `/data/finance/export.csv` 而形同虚设（现实踩坑点）
- **审批不可绕过**：沙箱不豁免审批，审批不豁免沙箱；两者是"提示时机"和"能力上限"的正交维度
- **子 agent 上下文携带**：child policy = parent policy ∩ child grants，deny 强制并入
- **无人值守模式**：`dontAsk` 语义内建；定时任务与 CI 默认走这个模式

### 9.3 分阶段落地
| 阶段 | 范围 | 风险声明 |
|---|---|---|
| V0 | 仅 L0+L1：规则 + 审批 + 工具自检；**无沙箱** | 必须显式声明"未隔离"，UI 常驻提示 |
| V1 | 接入 `@anthropic-ai/sandbox-runtime`（macOS/Linux），命令默认进沙箱 | Windows 用户看到能力差异 |
| V2 | Windows 方案定档（自研受限令牌 / 容器上移 / WSL2） | 需要原生 helper 或容器编排 |
| V3 | 沙箱覆盖 MCP server 与 hooks（整个 agent 进程进沙箱） | 依赖运行时自身能联网调用 provider API |

---

## 10. 待核对项

1. Anthropic `sandbox-runtime` 当前版本对 Windows 的实际支持度（官方文档与二级资料表述冲突）
2. GitHub Copilot CLI 的 ProcessContainer BaseContainer 对 Windows 版本的要求
3. MCP `2026-07-28` 修订对 Sampling/Roots/Logging 的弃用状态（影响我们是否实现采样）
4. pi 的 LICENSE 与 `pi-tui` 实现形态
5. 本机 Windows 版本是否支持 ProcessContainer / Job Object 相关能力（决定 V2 路线）

---

## 参考来源

- Claude Code 权限：https://code.claude.com/docs/zh-CN/permissions
- Claude Code 沙箱：https://code.claude.com/docs/zh-CN/sandboxing
- Claude Code 沙箱环境：https://code.claude.com/docs/zh-CN/sandbox-environments
- Codex Windows 沙箱：https://developers.openai.ac.cn/codex/windows
- OpenAI Codex 沙箱架构说明（二级转述）：https://www.infoq.cn/article/P3C52z1QFIgM0oBSIiXO
- Anthropic sandbox-runtime 开源报道：https://aihot.virxact.com/items/nj4rndmo45sc6mzp2smlwz4s2
- GitHub Copilot 本地/云沙箱：https://docs.github.com/zh/enterprise-cloud@latest/copilot/concepts/security-governance-and-network-settings/about-cloud-and-local-sandboxes
- Antigravity 沙箱文档：https://antigravity.google/docs/cli/sandbox/
- AgentScope 权限系统：https://github.com/agentscope-ai/agentscope-java/blob/master/docs/v2/zh/docs/building-blocks/permission-system.md
