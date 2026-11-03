# 11 · 施工计划（施工 session 入口）

> 目标：把 `00`–`10` 的设计变成可运行的骨架。
> 用法：按阶段推进，**每个阶段的验收条目是硬门槛**，未过不进下一阶段。
> 配套：`07`（决策总表，唯一裁决来源）· `09`（包边界）· `06`（骨架规格）· `08`（ID 设计）

---

## 0. 开局必读（顺序）

| 顺序 | 文档 | 读它为了知道 |
|---|---|---|
| 1 | `11-construction-plan.md`（本文） | 施工顺序与验收 |
| 2 | `07-design-review-checklist.md` | **唯一裁决来源**——所有已定决策与状态标记 |
| 3 | `09-package-boundaries.md` | 每个包的"为什么存在 / 什么变化会导致它变" |
| 4 | `06-skeleton-design.md` | 分层、依赖方向、契约清单、工作流模型、上下文策略 |
| 5 | `08-identity-model.md` | ID 命名空间与纪律（**最容易出 bug 的地方**） |
| 6 | `01-permissions-and-sandbox.md` | 权限三层模型与规则链 |
| 7 | `10-design-review-report.md` | 上一轮审阅发现了什么（**别重复踩**） |

**不需要重做的调研**（都已有实测结论，勿重复）：
- 存储选型 → `spike/sqlite-drivers/`（已跑出结果，直接用结论）
- 协议 SDK 可用性 → `03-provider-sdks-and-go.md`（含实测版本与日期）
- CI 门禁机制 → `10` 里的 tower 回函结论 + `07` 的 V 区
- 语言选型 → `05`（已定 Go，不再重议）

---

## 0.5 环境与工具链（本机事实，2026-10-27 实测）

| 项 | 值 |
|---|---|
| Go | **1.26.5 windows/amd64**（`/c/Program Files/Go/bin/go`） |
| GOPROXY | `https://goproxy.cn,direct`（可用） |
| module cache | `C:/Users/Yorha/go/pkg/mod` |
| **HTTP 代理** | **`http://127.0.0.1:10808`** ← 见下 |

### 网络：哪些域名需要走代理

| 目标 | 直连 | 走 10808 |
|---|---|---|
| `proxy.golang.org` / `goproxy.cn` | 可用 | — |
| `github.com` / `raw.githubusercontent.com` | **DNS 不通** | 200（0.7s） |
| `registry.npmjs.org` | 403 | 可用 |
| `pkg.go.dev` | 可用（但 anthropic-sdk-go 页面 16MB 超限） | — |

**用 `FetchURL` / `WebSearch` 抓 GitHub 内容时走 10808**（curl 用 `--proxy http://127.0.0.1:10808`）。这个坑在本次调研里踩过两次。

### 其他已知事实
- Go module path 含大写字母时，缓存路径用 `!` 转义（`Yorha9e` → `!yorha!9e`）——**可用**，只是缓存路径不美观
- 私有仓库：需 `GOPRIVATE=github.com/Yorha9e/*` 才能免鉴权拉取本 module
- **Windows 上 `go test -race` 必须 `CGO_ENABLED=1`**（race 检测需 cgo，本机有 gcc）；产品构建仍 `CGO_ENABLED=0` 禁 cgo——两者分开跑，别合成一条命令

### CI 参数（阶段 0 定稿，2026-10-27 施工 session）

| 项 | 值 |
|---|---|
| 形态 | GitHub Actions，单 workflow `.github/workflows/ci.yml` |
| 触发 | `push` + `pull_request` |
| Go 版本 | `1.26.x`（工具链） |
| 平台矩阵 | `windows-latest` / `macos-latest` / `ubuntu-latest`（**Windows 必须跑**） |
| 检查步骤 | `go vet ./...` + `go test ./...` + `CGO_ENABLED=0 go build ./...`（全程 `CGO_ENABLED=0`） |
| 禁 cgo lint | 独立步骤（三平台任一跑即可）：全仓 `*.go` 扫 `import "C"`，命中即 fail |
| schema 校验 | **已点亮**（阶段 1）：`go run ./cmd/codegen -check`（再生成 diff 为空 + 孤儿生成物检测）+ TS 消费端（tsc / 样本 / 坏样本必拒） |
| workflow YAML 门禁 | 独立 workflow `lint-workflows.yml`：解析全部 `.github/workflows/*.yml`（防 workflow 自身语法坏掉导致门禁静默失效） |
| 酸测试矩阵 | **逐步点亮**：矩阵条目随阶段启用，阶段 0 只建骨架占位，**不追全绿**（可换实现要阶段 2–3 才存在） |

纪律：本地开发不需要 CI 代理（goproxy.cn 直连可用）；GitHub runners 上 `proxy.golang.org` 默认可用，CI **不设** `GOPROXY`/`GOPRIVATE`（无内部依赖前无意义）。

---

## 0.6 当前进度与交接状态（2026-10-27）

| 项 | 状态 |
|---|---|
| **代码** | **v0 全部完成并终验收 ACCEPTED**（2026-10-27）：阶段 0–8 全部通过退出评审循环（critic clean），v0 酸测试 11 条逐条实测核销（06 §5）；22 包全测试 + race 干净 + codegen 零漂移 + TS 消费端三件套绿；唯一代办已闭环：**真模型实测通过**（2026-10-27，用户本地网关 + gpt-4o-mini）——模型自主调用 read 工具，事件流三段完整（`tool.call.intent` 携 provider id 黑盒原样 → 真实执行 → `tool.call.completed` 同 id 带真实 result）→ `context.assembled`。E2E 覆盖三层：httptest（协议）+ 脚本 provider（机制）+ **真模型（端到端）**。**v0 无遗留待办。** |
| **设计** | 收敛完成：`07` 共 **280** 条决策（已定 82 / 待议 108 / 默认通过 90；I9 模型名解析 2026-10-28 补入） |
| **文档** | `docs/research/` 12 篇 · `spike/sqlite-drivers/` 一份已跑出结果的实测 |
| **外部审阅** | 一轮已完成（`10`），7 阻断 / 16 应修 / 9 建议 **全部落档并闭环** |
| **命名** | 已定：`alasol` · `github.com/Yorha9e/alasol` · 命令 `alasol` · Apache-2.0 · 先私有后公开 |
| **施工流程** | 已评审并修订（durable 形状前置 / CI baseline 落位 / 阶段 4 审批三段 / env v0 子集 / 酸测试矩阵渐进） |

### handoff 链（全 5 条，按时间序）
```
ho_5c663a3207fc  施工交接（设计 → 施工）
ho_64354db0157c  施工回执 + 流程评审（施工 → 设计）     ← 施工方主动评审施工计划
ho_b2d2a6ee000d  评审确认 + 4 处修法落档（设计 → 施工）
ho_43284b3fcf1c  真机回路闭环 + 模型名修复（施工 → 设计）
ho_536a1eca9585  独立核实 + 结项归档（设计 → 施工）
```

**设计侧 session 已于 2026-10-27 结项，不再是升级对象**（见 §4.5 的升级路径）。

### 原「唯一未闭合的前置项」——已闭合
**`11` 阶段 0 的 CI 参数**已于 2026-10-27 由施工 session 定稿，见 §0.5「CI 参数」表。前置项清零。


## 1. 施工前必须先拍的四项（否则别开仓）

| # | 项 | 定案（源头见 `07` P 区，2026-10-27） | 改动成本 |
|---|---|---|---|
| 1 | **module path / 仓库名** | ✅ `github.com/Yorha9e/alasol`，仓库名 `alasol`（产品名 **alasol** = 拉丁 ala「翼」+ solum「地基」） | **高**——改一次要动所有 import |
| 2 | **命令名** | ✅ `alasol`（原建议 `hx` 作废：与 Helix 编辑器二进制同名） | 低（可后改） |
| 3 | **许可证** | ✅ `Apache-2.0` | 无（但影响后续贡献者） |
| 4 | **开源与否** | ✅ 先私有开发、契约冻结后再公开 | 无 |

> ✅ **已定案**（施工 session 2026-10-27）。module path 现已固定 —— 改一次要动全部 import + 通知下游，且字符串会留旧名。

**命名排除记录（供后人查，避免重走这轮排查）**：`arbor`（人大-微软 RUC-NLPIR/Arbor：同域长时程科研 agent 框架，假设树 + CLI + MCP + worktree）、`baton`（2026-04 Product Hunt 同域产品：多 AI 助手各带独立 Git 工作区）、`norax`（撞 "AI agent runtime with durable memory"）、`lintel / plinth / aileron / avenor / fulcrum`（均撞 agent/AI 工具）、`keelwing`（语义最好，但 bare org 已被 Sailfish 空壳占用；`keelwing.org` 空闲，作备选留档）、`harness`（harness.io 品牌 + 品类词）。

**私有仓库注意**：Go 对 module path 的大写字母用 `!y` 转义（缓存路径），本身可用；但若后续有外部依赖指向本 module，需设 `GOPRIVATE=github.com/Yorha9e/*` 才能免鉴权拉取。

---

## 2. 阶段与验收

**阶段退出评审循环（2026-10-27 用户定，硬门禁）**：每阶段 coding 完成 ≠ 阶段结束。必须走 **critic → reviewer → critic → …** 循环：① critic 只探查不改码（产出问题清单）→ ② reviewer 按清单修正 → ③ 再派 critic 复查 → 重复，**直到 critic 判定无问题**才准进下一阶段。阶段硬验收条目仍是底线，两道门都过才转段。

### 阶段 0 · 地基
- `git init`；按 `09` 的划分建目录；`go.mod`（`github.com/Yorha9e/alasol`）；CI 三平台（**Windows 必须跑**）+ schema 校验 + 禁 cgo lint
- **酸测试矩阵逐步点亮**：矩阵条目分阶段启用（阶段 0 只建骨架与占位，**不追全绿**）——可换的实现要阶段 2–3 才存在
- **验收**：`GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...` 通过；目录与 `09` 一致；**禁 cgo** 有 lint 兜底

### 阶段 1 · contracts
- 唯一清单（D21）：Event / Command / Transport 消息 / Extension 元数据 / Tool.schema / **Tool 返回值** / Policy 规则 / Store 持久化格式 / Workflow 定义 / ContextSnapshot / Approval 消息
- **三段事件契约在此定稿**（X1 前置）：`tool.call.intent` / `tool.call.completed` / `tool.call.failed` —— 这是**工具执行写路径的形状**，必须在契约层定死，否则阶段 4 起每条调用都要返工
- 手写 JSON Schema → **自写受限子集 codegen** → 生成 Go 类型 → 导出
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] 生成物入库，CI「再生成 diff 为空」通过
  - [x] 可选字段**一律指针 + `omitempty`**；枚举用命名类型 + 常量；union 用接口 + Marshal/Unmarshal 包装
  - [x] **TS 侧最小消费端**能读事件流（跨语言中立性验证）
  - [x] `Tool.schema` 的值本身是 JSON Schema（元嵌套）→ codegen 当原始 JSON 处理

### 阶段 2 · core 骨架（无 IO）
- 事件总线（`durable` 区分，F8）/ 扩展注册表 + 生命周期 + 能力裁决 / 策略链（fail-close，deny·ask 跨来源并集，H5）
- **core 只定义能力接口（exec / fs / net / proc），不含任何 IO 实现**（P0-4）
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] **core 在没有 runtime 的情况下跑完全部单测**
  - [x] 换 `Store` 实现 → core 零改动
  - [x] 换 `Approver` → core 零改动
  - [x] 注册新 `Tool` → 不修改任何已有文件

### 阶段 3 · runtime + 索引器
- SQLite（`ncruces/go-sqlite3` v0.35.6，`_txlock=immediate` + `busy_timeout`）/ 事件日志（`seq` 由 AUTOINCREMENT 分配）/ **索引器**
- **ledger 与黑板全部事件化**（G23）：`ledger.updated` / `board.written` → 当前态 = 索引器投影
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] 事件流能**完整回放**一场 session
  - [x] 索引器崩溃后从 seq 水位续跑；支持重建；**UI 显示水位**（`Watermark()` 可查）
  - [x] ledger **不是**独立可变存储（消灭状态源三分）

### 阶段 4 · loop + 真机回路
- agent loop 状态机 / 一个真 provider / read·write·bash 工具 / **env 注入 v0 子集**（H21：session + 单次调用两级作用域，**不给模型看任何 env**）
- **写路径从第一个真实工具调用起就用三段事件**（`intent` → 执行 → `result`，X1）——阶段 8 只补 checkpoint / resume / ownership 的机器，**不回头改写路径**
- **审批策略**：`deny-all` Approver 为**默认**（验 fail-close）；真机回路用 `always-allow` **显式开启**；**阶段 4 末尾补最小终端审批**（顺带验证 Approval 跨边界契约，让用户能自己跑通真机回路）
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean；真模型已实测通过——本地网关 + gpt-4o-mini，模型自主调工具、三段事件完整）：
  - [x] 真实仓库里跑通一次工具调用（E2E：真 SQLite + 真工具 + 真落盘 + 三段事件断言）
  - [x] **凭据永不进 prompt / 事件 / 日志**（注入值精确 redact + 凭据分类收口）
  - [x] 事件流含完整 intent→result 三段（X1，每调用恰好一对）

### 阶段 5 · 多 agent（v0 切片 ②）
- `agent_type` 体系（含 `can_spawn`）/ spawn / **结构化 summary**（`refs` + `taint`）/ agent 树 / **父↔直系子可见**
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] 主 agent 拆任务 → spawn 带 role 的 subagent → 收回 summary → refs 可核验
  - [x] 深度超限**运行时拒绝**（三闸门：深度/类型/权限，K17）
  - [x] tainted summary 被主 agent 按不可信数据处理（taint 只升不降 + ⚠ 前缀）
  - [x] 异常向上冒泡（子 agent 失败/卡住 → 父的 summary 报告，abnormal 标记）

### 阶段 6 · 最小门禁（v0 切片 ③）
- 不透明命令（仓库级注入）/ **一次性快照 worktree** / **receipt 四要素校验** / **最小 CI baseline**（W9c）
- **baseline 的关键细节**：必须支持**「只记录不阻断」地建立基线**（第一次跑 → 记录已知失败指纹集 → 之后只拦**新增**失败）。否则红仓上第一次跑就死锁，基线永远建不起来
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] receipt 的 commit ≠ 当前 tip → 拒绝通过
  - [x] 未提交改动**结构性不进** CI（detached worktree 快照）
  - [x] 命令不匹配当前配置 → 拒绝通过
  - [x] **红仓上能建立基线**，之后只拦新增失败（含静默失败 fail-close、空基线 fail-close）

### 阶段 7 · 客户端
- 极简 CLI（终端原生：对话 / 审批 / 在线表）+ Web UI（深度视图：上下文可视化 / 多 session 面板），走 WS+NDJSON
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] Web 能看到在线表与上下文构成（谁贡献了多少 token、什么被压过——三面板：在线表/上下文构成/事件流）
  - [x] CLI 能对话与审批（`alasol chat` REPL + `/table` + `/ctx` + terminal 审批）

### 阶段 8 · durable（X 区）—— **只做"机器"，写路径形状已在阶段 1/4 定**
- checkpoint（seq 水位 + ledger 版本 + in-flight + 树投影版本）/ resume / ownership（`spawn_request_id` 防重复 spawn）
- **验收**（2026-10-27 全部通过，退出评审循环 critic 判定 clean）：
  - [x] kill -9 后能续跑且**不重复副作用**（真子进程 Kill 测试 + 会话级原子认领防并发重放）
  - [x] `replay:"verify"` 的操作先核验外部状态再决定（Verifier 接口 + git anchor 核验器 + 核验出错转 manual fail-safe）

### v1 · 多 session 数据中心（施工切分，2026-10-28 提议）

> 每阶段照旧过退出评审循环（critic→reviewer→clean）+ 提交 `refs 07/…`。上下文系统是**预备单位**（`13-context-system.md`，⏸ 候选）：主调度先跑 append，级联单元按需挂载；v1 的真实多 session 负载同时是它的**验证载体**。

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v1-S1 地基补齐** | ① `user.prompt` / `model.turn` 事件入流（补 F1 缺口：对话内容进事件流，resume/回放才完整）② S13/S14 路径规范化 + `project_id`/`repo_id`（EvalSymlinks→小写→统一分隔符→去尾；按 git root 聚合） | 事件流能回放**完整对话**；同 repo 多 worktree（含大小写/junction 用例）聚出同一 repo_id | **✅ 完成（2026-10-28，评审循环 clean）**：`runtime/identity` + 对话事件入流 + `FoldConversation`；另闭环 `alasol resume -continue` 续跑（fold→RunWithHistory）。注：EvalSymlinks 字面在 Windows 不解析 junction，落地为逐组件 Readlink（语义等同，详见 07/S13 行内） |
| **v1-S2 hub 与发现表** | hub daemon（多 session 宿管）+ K10 发现表（事件驱动内存活状态）+ K12 gate（自动 `repo:`/`workflow:` + `join_gate`）+ K11 水平可见性 | 两个 session 经 gate 互见主 agent；发现表崩溃重启后由事件流重建 | **✅ 完成（2026-10-28，评审循环 clean）**：`core/discovery` + `gate.joined` 事件 + host 装配（`/api/peers`、`/api/gates/join`）；K10/K12 ★→✅ |
| **v1-S3 agentlink** | K18 五件套：Presence / Discover / Inquiry / Broadcast / Handoff（库形态，不绑协议） | Inquiry 拉原文可核验 summary 的 refs（端到端） | **✅ 完成（2026-10-28，评审循环 clean）**：`core/agentlink` + `agentlink.broadcast`/`agentlink.handoff` 事件；refs 核验 token 锚定 + 含空格路径整串核验 |
| **v1-S4 在线表注入** | K14/K15：50 行 / ≤100 token / gate 过滤，**全可配 + 运行期动态管理**，恒在 prompt 末尾（Q4） | 主 agent prompt 尾部出现在线表；参数运行期可调且不重启 | **✅ 完成（2026-10-28，评审循环 clean）**：`core/loop/onlinetable.go` + host 配置面/HTTP；注入恒在请求尾部、不入持久历史 |
| **v1-S5 宿主补全** | 多 worktree 隔离 + 完整 TUI（O6）+ 模型目录（I5）+ H15 TOML 配置（env > 文件）+ K9 hub 监控 + token 成本统计（J21 usage 进成本） | TUI 对话/审批/在线表；配置文件优先级正确；成本统计可查 | **✅ 完成（2026-10-28，评审循环 2 轮）**：六项全落（worktree 管理/TUI 三面/模型目录/TOML env>文件/hub 监控/cost_tokens）；**挂后续**：worktree 工具级隔离（工具仍跑进程 cwd）、K9 告警入事件流、登记表持久化 |

（原清单里的 MCP / 深度压缩 / 评测已在 v3 / v2 / v4 归位，见 `12-roadmap.md`。）

### v2 · 深度压缩与能力增强（施工切分，2026-10-29）

> 每阶段照旧过退出评审循环（critic→reviewer→clean）+ 提交 `refs 07/…`。v2-S1 是 `13-context-system.md`「预备单位」的转正施工：`context_strategy` **可挂可不挂**，不挂时 append 兜底、换策略 core 零改动（酸测试）。压缩两级锚定 **L1≤10% / L2≤1%**（J9，可配初值），级联走 **goal ledger C 案**（J17/J18/J19），缓存定律照 13 §6（J20 量化留实现校准）。

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v2-S1 深度压缩** | ① `context_strategy` 预备单位装配点（core/loop 装配层；**X1 三段写路径一字不动**、`user.prompt`/`model.turn` 旁路不变）② 两级压缩：L1=摘要**含具体工程操作**+指针→持久化原文、L2=结论+一根指针（13 §5）③ `recall(ref)` 工具取原文（ref=`stage:N`，seq 区间给机器），指针条目自带使用说明；refs **永不指摘要** ④ 阶段边界=验证通过点（critic clean / gate receipt），水位只当兜底护栏 ⑤ 留存策略：已完成阶段不可变尽量留存、预算驱动降级（全文→L1→L2 最老先降、**永不回升**）⑥ 常识区（热区最前）+ 缓存定律（易变贴生长前沿、稳定在最前；ledger 不进最前）⑦ 兜底三级（L1 全降 L2 → 切当前阶段成小段 → W/3 硬预算预防 + prompt 明示"阶段不能过大"）⑧ 级联 C 案：压缩输入=goal ledger+被压区间**全文**（永不读他阶段摘要）；接 J19/X6 ⑨ 压缩模型默认主模型可配；L1/L2 阈值与预算进 `context_strategy.params` **不写死** | 不挂策略时装配行为与 v1 等价（酸测试）；换/摘策略 core 零改动；L1≤10%/L2≤1% 构造负载实测达标（可配初值）；`recall("stage:N")` 取回持久化原文全文、L2 条目自带调用说明；阶段切分只发生在通过点；降级单向且最老先降；压缩后稳定前缀**字节不变**（缓存定律断言）；兜底三级顺序正确、切阶段有诊断事件；压缩输入不含他阶段摘要（断言）；新事件（`context.compacted` 等）F7 只增 + `tools/ts-consumer/sample-events.ndjson` + `contracts_test` 钉子全绿；`context.assembled` parts 出现 `stage:N`/`ledger`/`rules` 来源；13 §9 转正数据观测面（沟通/工作量比、切后回捞率、summary 够用度、阶段大小分布）随实现落 | **✅ 完成（2026-01-29，评审循环 critic 4 轮/reviewer 3 轮至 clean）**：`core/compaction` 两级压缩+recall+留存降级+常识区+兜底三级+级联 C 案 + `core/loop` 薄装配点 + host TOML 配置面 + `context.compacted` 事件（F7 只增）；**挂账（不阻塞）**：账本生产者未接线（07 J19 挂账）、硬边界事件驱动留 v2-S3、常识区采集源留后续、[尾部] 未实现 |
| **v2-S2 图投影简版** | G18 一跳/两跳 SQLite 双向倒排（`edges(src,kind)`/`edges(dst,kind)`）+ 多跳内存 BFS（深度/结果上限 + context 取消）；G19 node id interned（10 万节点/100 万边 ~100–200MB 量级，可配）；G20 闭包缓存简版（内存 map + 图写入失效，不做复杂淘汰）；G21 用途链落地：依赖图可达性 → `change_impact` → escalate 客观依据（W16）；G2f 图=**可选投影**、不进 core 必经路径 | 一跳/两跳走 SQLite、多跳 BFS 结果正确且上限/取消生效；闭包缓存命中与图写入失效正确；change_impact 产出影响面供 escalate 判定；未启用图的工作流零图开销（可选投影酸测试） | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：`core/graph`（接口+MemGraph+闭包缓存+影响面报告）+ `runtime/graph`（SQLite 双向倒排+流式加载）+ 索引器建边（G22）+ host TOML `[graph]` + `GET /api/graph/impact`；**挂账留 v3**：邻居截断信号（N1）、Tx 类型化收尾（N5）、重建窗口口径（N2，喂 escalate 前必补） |
| **v2-S3 门禁增强** | V4 内容寻址共享只读缓存（无锁竞争、无污染）；V6 review-tip 失效检测（review 后分支 tip 变 → 结论失效 → 要求重审）；V7 合并后冲突预测（依赖图可达性，比三点 diff 求交集更准） | 缓存键内容寻址、命中不重复构建、只读无污染；review-tip 变更触发失效并要求重审；冲突预测能基于依赖图对未合并 mission 预警撞车 | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：V4 内容寻址缓存（`core/gate/cache` + `CI_CACHE_KEY`/`CI_CACHE_DIR` 注入）+ V6 review-tip 失效（`core/gate/review` + `review.recorded`）+ V7 冲突预测（`core/graph/conflict` + `conflict.predicted`，非对称判据+聚合诚实化）；**挂账留 v3**：V6/V7 无生产发射方（合并门禁接线）、V4 命中语义归命令侧 |
| **v2-S4 沙箱** | K8/H9 SandboxedExecutionEnv：Windows **job object + 受限 token + 目录 ACL** 务实组合；**可拓展埋点**（Linux/mac seatbelt/bwrap 后续挂）；V8 写保护由此收口 | 工具子进程纳入 job object（进程树清理）；目录外写被 ACL 拒；受限 token 下提权受限；换沙箱 env 不动 agent 逻辑（可插拔酸测试）；埋点接口留出 Linux/mac 挂点 | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：`runtime/cap/sandbox`（job object+受限 token+目录 ACL+WER 压制+Backend 埋点接口）+ host/cmd 装配（run/resume/serve 统一口径）；**已知边界**：默认模式 BUILTIN\Users 授权位置可写可读（`-sandbox-strict-groups` 可关、代价 overlapped I/O）、读只挡用户私有文件、允许根 DACL 追加传播不还原；Net/Proc 维度留挂账 |
| **v2-S5 存储接口收口** | G1/G2「一组可组合的存储接口」随图/blob 落地收口：事件日志/文档/blob/图接口分立、各自多实现可组合（blob/向量实现留 v4）；v1 挂账能清则清 | 接口分立且可组合（换实现零改动酸测试）；图/blob 接口签名与 G18-G21 实现对齐；07 收口注记 + 合计同步 | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：`core/storage` 五族可组合接口（文档/blob/向量只立接口留 v4；`BlobMeta.Ref()`=`Event.binary_ref` 挂接点）**补注（2026-11-02）**：预留的三族实现均已落地——文档=DocStore（缺口 1）、blob=blobfs（v4-3-A）、向量=vecmem（v4-3-B），五族全有实现+ 五族双实现酸测试；挂账清理 ②③⑤⑥（hub.alert 入流/worktree Rebuild/models 读 TOML/agentlink 空格路径判据），①④ 如实留挂（worktree 工具级隔离涉 X1、TUI 真 TTY） |

### v3 · MCP / fork / 工作流引擎（施工切分，2026-01-29）

> 每阶段照旧过退出评审循环（critic→reviewer→clean）+ 提交 `refs 07/…`。v3 沿拍板顺序：**MCP client 先**（N1）→ **fork+/btw**（K29）→ **工作流引擎**（W 系，非强制哲学式）。v2 各段的挂账随做随清（见 S3）。

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v3-S1 MCP client** | N1 拍板 client 先（官方 go-sdk 已实测）：连接外部 MCP server、其工具注册进 registry 走既有工具管线（X1 三段写路径 + 策略链裁决 + R5 可见性）；N2 可选能力**不实现**（sampling/eliciation/roots）；server 声明的工具 schema 进 Tool 契约面 | 连模拟/真实 MCP server 后其工具可被模型调用且三段事件完整、策略链生效；坏 server/断连 fail-close 不拖垮 hub；MCP 工具命名不与内建工具撞名（重名 fail-close） | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：`runtime/mcp`（官方 go-sdk v1.8.0，stdio+Streamable HTTP）+ host/cmd 装配（`[[mcp.servers]]` TOML）；命名 `mcp__<server>__<tool>` 防遮蔽、N2 不广告+拒+留痕、输出三道闸（块数/合并/事件 JSON 字节）；**挂账转 v3-S2**：工具列表快照、stderr 条数闸、装配预算口径、关停文案、tab 变体 |
| **v3-S2 fork + /btw** | K29 拍板：fork **只附带上下文**（不复制 ledger/gate 全套）；`/btw` 旁支命令——结果**只给用户、不回灌**主上下文；**预埋**"从 /btw 生成全新 session"钩子（只埋点不实现） | /btw 结果不进主上下文（事件流/快照断言）；fork 附带上下文且不带 ledger/gate（断言）；生成新 session 钩子埋点存在（埋点≠实现） | **✅ 完成（2026-01-29，评审循环 critic 3 轮/reviewer 3 轮至 clean）**：`host/fork.go`（RunBtw：fork 只带折叠上下文、不带 ledger/gate）+ REPL/TUI `/btw` + `btw.forked` 契约 + `SetBtwPromoteHook` 埋点（未实现生成，护栏钉住）；fork 四面过滤、上下文上限 16 条/6000 字符（含工具参数）；v3-S1 挂账 5 项一并清 |
| **v3-S3 工作流引擎 + tower 验证** | W1 能力包（哲学+agent_types+context_strategy+gates+grants）、**非强制**（W2 阶段=哲学非强制序列、W21 编排主 agent 自决框架不校验）；W3 slash/显式触发 → 哲学文本进 system prompt；W4 追加 loop 可组合；W14 代码扩展点**只有两处**（MilestoneHandler + 自定义上下文策略）；W19 **tower 作首个 workflow 包**（critic→reviewer 循环做成第一个包）；**v2 挂账随做随清**：V6 review-tip / V7 冲突预测的合并门禁接线、N2 重建窗口口径补齐、J19 账本生产者、v2-S1 硬边界事件驱动接线（gate receipt → MarkValidationPassed）、N1 邻居截断信号/N5 Tx 类型化 | workflow 包可挂可不挂（不挂零行为变化，酸测试）；tower 包跑通 critic→reviewer 里程碑（MilestoneHandler 实证）；合并门禁能拦"review 后又推一行"（V6 端到端）+ 冲突预测消费端就位；07 挂账注记同步 | **✅ 完成（2026-01-29，评审循环至 clean——402 断供换通道，详见 07 注记）**：`core/workflow`（能力包+MilestoneHandler+Compose+加载校验）+ `core/workflow/tower`（W19 首包）+ `host/mergegate`（V6/V7 接线）+ J19 账本生产者（LoadLedger 跨 Run 保住关键字段）+ 硬边界 ValidationCursor + N2 重建恢复；**如实注记**：验证游标不跨进程；402 断供期间 reviewer/critic 通道失效，修复与终验经 code-coder 换通道完成并变异自证 |

### v3-S4 · 协议面补全（施工切分，2026-01-30，来源：第三方 GUI 验收 ho_a83f7df3ef9e / REPORT.md G1–G10+A1–A5）

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v3-S4 协议面补全** | 按 `D:\vscode\alasol-gui\REPORT.md` 分诊修复：**G4** hub.alert 过 bus（注释兑现：入库与 WS 广播一致）+ 告警环/事件流两口径文档化；**G3** 事件回放面（`GET /api/events?from&to&limit` 或 subscribe 支持 from_seq 补发——二选一你定）；**G1** 对话/审批端点（**兑现 transport 契约承诺**的 `command.submit`/`approval.request`/`approval.respond`——HTTP/WS 至少一面对话发起+审批应答端到端）；**G6** Worktree json tag（snake_case 对齐全仓）；**G7** API 前缀显式 405（方法不符不再报"静态资源"）；**G8** subscribe ack 回执（生效过滤条件）；**G9** TransportMessage.id 语义写进契约（或给示例值）；**G5/A1/A3/A5** 口径文档（前端须同源/代理、/api/sessions 计数口径、告警时间字段口径、分页游标口径）；**G2** serve 定位口径统一（文档化"观察+协调 daemon"：托管型写面要求本进程托管、协调型写面（merge.*）直查库；**跨进程托管注册记 v4 不做**）；G10（派活 handoff 笔误）记账不修 | G4：WS 实时收到 hub.alert 且与入库一致；G3：断线后按 seq 补读回放无空洞；G1：协议面发 prompt→跑一轮→审批应答端到端（WS 事件可见）；G6/G7/G8/G9 各有回归钉住；口径文档齐（transport/host doc）；评审循环 clean；07 注记同步 | **✅ 完成（2026-01-30，评审循环 critic 2 轮/reviewer 2 轮至 clean）**：G1 对话/审批四面（HTTP+WS，无人应答=deny fail-close）、G3 `GET /api/events` 回放、G4 hub.alert 过 bus、G6/G7/G8/G9、口径文档（G2/G5/A1/A3/A5）；**记 v4**：`POST /api/sessions` 开新会话入口、审批应答鉴权收紧、跨进程托管注册 |

### v4-2 · SSH 远程适配（施工切分，2026-10-31，来源：14-v4-plan 排期表第 2 项；N4b/N5/K19 拍板：SSH 远程控制 harness 机器、鉴权交系统、K19 随 SSH）

> **形态定调**：远端经 SSH 连接 harness 机器进行远程控制——两条路：① SSH 端口转发接 hub 的 loopback HTTP/WS（现有面）；② `ssh host alasol acp` 拉起 ACP agent（stdio，现成协议客户端接入，K19 不自研分布式协议）。**鉴权交系统/SSH 自管**（密钥/agent），我们只做 SSH 适配、不做自研鉴权（N5 的 token scope 体系不做）。

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| **v4-2-A SSH 适配层 + 预留接口** | ① **预留接口**：`runtime/cap/remote` RemoteExecutionEnv（K8 第三档）——ExecCapability 经 SSH 远端执行（鉴权交系统 ssh 密钥/agent），Fs/Proc 显式 ErrUnsupported（预留位）；与 local/sandbox 并列、K8 酸测试同款（换 env 不动 agent 逻辑）② **SSH 适配层**：serve 的隧道友好（banner 给 `ssh -L` 转发提示 + Origin/Host 校验在隧道后兼容性钉住）+ N4b 六条件在 SSH 形态下的口径文档（鉴权/TLS=SSH 负责；审计=事件流已有；降权视图不做=鉴权交 SSH 后无 token scope）③ N4b/N5 落地注记 | remote env 可替换装配（K8 酸测试）；ssh exec 最小可用 + 未支持面显式报错；隧道后 HTTP/WS 全通（Origin 钉子）；07 注记同步 | **✅ 完成（2026-10-31，评审循环 critic 3 轮/reviewer 3 轮）**：`runtime/cap/remote` 落地（系统 ssh，**不引 x/crypto/ssh**；K8 酸测试绿：换 env 工具代码零改动）+ Fs/Proc/Net 显式 ErrUnsupported（errors.Is 可判）+ serve banner `ssh -L` 提示 + 隧道后 Origin 互认（**规范 loopback 三件套**：localhost/127.0.0.1/[::1]，127/8 其余成员不放行；**诚实边界**：三件套互认是隧道便利，同机不同地址同端口可各自 bind，恶意本地页面仍可借互认驱动 hub——彻底堵需注入实际监听地址，**挂账**）+ 诚实边界写进 doc：**远端路径不传凭据**（凭据形态的 **env 与 args** 一律拒绝执行，ErrCredentialEnv——值会落 ssh argv 两端进程表）、Env **整份替换**（`env -i` + **远端侧固定 PATH**（`/usr/bin:/bin`；调用方给的 harness PATH 一律忽略、机器身份键 **Windows + POSIX 两套**（HOME/USER/… 与 USERNAME/USERPROFILE/COMPUTERNAME/…）剔除；可经 Config 覆盖）+ ssh 默认 `-o BatchMode=yes -o ConnectTimeout=10`（远端要交互式口令会挂起；附着式 `-oKey=Value` 也认得出）、ssh 自身 exit 255 可判（ErrSSHTransport，errors.Is；判据收紧到 stderr **首行**的 ssh 强特征，通用措辞不再误报）+ 注入钉子 marker 改 POSIX 安全路径（不再往仓库工作目录里落垃圾文件）+ 终查收尾：F1 补齐 **Windows 身份键**（原先一个都不在表里 → 原样发往远端 = 身份冒充 + 账号/主机名泄漏）、F5 bash 工具 `%v`→`%w` 保住错误链、F3 补 **DefaultRemotePATH 链路钉子** + 如实写明**产品面只给 `-remote`/`-remote-host`**（远端 PATH 固定 /usr/bin:/bin、ssh 选项走 ~/.ssh/config）、F4/F6 作为**已知边界**写进 doc（凭据扫描只认整串/=后值两种形状；255 日志会记 stderr 前 200 字符） |
| **v4-2-B K19 ACP agent 端点** | `alasol acp` 子命令：ACP agent（JSON-RPC 2.0 over stdio、newline-delimited 帧，**不引新依赖**）——initialize（能力协商）/ session/new（cwd）/ session/prompt（跑 hub RunPrompt）/ session/update 通知（桥事件流：agent_message_chunk/tool_call/plan）/ session/cancel / session/request_permission（桥 ask 审批）。跨机 = 客户端以 `ssh host alasol acp` 为子进程接入（现成 ACP client 可用）；鉴权交 SSH | ACP client（脚本模拟）initialize→new→prompt→update 流→stopReason 全程可跑；审批桥 ask 闭环；事件流/审计不旁路；07 K19 落地注记 | **✅ 完成（2026-10-31）**：`runtime/acp` 落地（自实现 JSON-RPC 2.0 newline-delimited 帧编解码，**不引新依赖 / go.mod 不动**；三形态判别用**键在位**标记以正确处理 `result:null`）+ `alasol acp` 子命令（复用 serve 同款 config 装配面 + hub；**runtime 不 import host**，经 cmd 适配层注入 `*host.Hub` + 新增只读 `Hub.Bus()` 订阅口）。核心方法全集：initialize（protocolVersion=1 / loadSession=false / promptSuggestion=false 如实声明）/ session/new（sessionId = **hub ULID 原样**；mcpServers 按口径忽略）/ session/prompt（跑 hub.RunPrompt 同步等整轮，回 stopReason）/ session/update（桥 bus 增量：model.delta→agent_message_chunk / reasoning→agent_thought_chunk / tool.call.intent→tool_call / completed|failed→tool_call_update；**user.prompt 不发**；接不到 delta 降级为整段 agent_message_chunk）/ session/cancel（per-session in-flight turn ctx cancel）/ session/request_permission（acp 包内 core.Approver，H18 脱敏只发工具名+摘要+副作用，无人应答/超时/客户端错误/outcome 非 allow 一切 fail-close deny，超时时长=整轮 ctx 与 host 审批同口径）。**stdout 纯净性铁律**：唯一写 stdout 的是 conn.writeFrame（写互斥保一行一帧），一切日志走 stderr——有回归钉子钉住。测试全绿（帧编解码含坏 JSON/换行边界/1MB 大帧/id 配对；端到端 fake backend + **真 hub 脚本 provider** 两层；cancel→cancelled；审批 allow/deny/timeout/no-conn fail-close；未知 method→-32601；stdout 逐行合法帧）。07 K19 补 ACP 落地注记（合计不动）。**critic 修复轮（2026-10-31，v4-2-B 未提交刀的评审）**：① 帧层终态错分流（bufio.Scanner 粘性错 → 自实现 bufio.Reader 行读取：超限帧**跳到行尾**回一条 -32600 后继续读后续帧；底层读错误回一条 -32700 后**结束连接**——修掉“9MB 帧 → 8 秒 118 万条错误帧、进程永不退出”的自旋）；② 审批面接 ACP 官方 schema：请求侧带 options 清单（allow-once/allow-always/reject-once），应答侧解析 {outcome:{outcome:"selected",optionId}} / cancelled（旧 allow/deny 词表保留兼容）——现成 ACP 客户端的审批闭环才通；③ 后端真出错回 JSON-RPC 错误帧（-32603 + 脱敏摘要），不再谎报 end_turn；④ stopReason 全部落在 ACP 枚举内（取消→cancelled、整轮预算耗尽→max_turn_requests）；⑤ 增量与最终文本对账（不一致以最终文本为准补差量，客户端能收全）；⑥ Hub.Bus() 收窄成只暴露 Subscribe 的 host.EventSource（“只读出口”不再带 Publish 的笔）；⑦ session/new 的 cwd 在服务端不存在/为空 → 回落服务端进程 cwd + Warn（跨机口径），根夹紧本刀不做（诚实边界写进 doc）；⑧ -prompt-timeout 真换算成端点整轮预算（host 与 acp 共用 core.DefaultPromptRunTimeout 同一常量）；⑨ 脱敏补**值级**（命令/URL 里的凭据：sk-/gh?_/JWT 词元 + Authorization 头**值**打码（scheme 留着）+ Bearer token + URL userinfo）；⑩ 文档写全**外发面**清单（写进 runtime/acp 包 doc，逐条：① initialize 能力协商结果 ② sessionId ③ stopReason 或 -32603 错误摘要 ④ session/update（正文/思维链增量、工具名、**脱敏后**的工具参数）⑤ request_permission（工具名+人读摘要+副作用+脱敏参数+选项清单）；**不外发**：凭据、未脱敏的完整工具参数、事件库内容、其它 session 的数据） + **CLI 级** stdout 钉子（真起 `alasol acp` 子进程，stdout 逐行必须是合法帧）；⑪ ctx 取消当**正常收场**（Serve 返 nil、退出码 0、stderr 一条 info）。**第二轮复查修复轮（2026-10-31，未提交刀的 findings）**：① **H18 凭据外泄修**：`Authorization: <scheme> <value>` 的脱敏**打码对象搞反**——旧正则把冒号后第一个词（= scheme）当"值"打掉，真 token **明文外发**（`Authorization: *** abc123def456`），且 scheme 被打掉后 Bearer 规则再也匹配不到（连带废掉）；改为 scheme 可选捕获组、**打码值**（scheme 留着：`Authorization: Bearer ***`），孤立 `Bearer <token>` 仍被遮（既有行为不回归）；② **多轮对账修**：最终文本是**最后一轮**的文本（core/loop 的 lastText 语义），而桥出的增量是**所有轮**的拼接 → 多轮（第 1 轮正文+工具调用、第 2 轮最终答复）判"分叉" → **整段重发** → 客户端收到两份最终答复；改为桥按 model.delta 载荷的 turn **分桶累计**、只与**最后一轮**对账，且两侧**同一 trim 口径**（尾随空白不再触发重发）；③ **stopReason 误报修**：MaxTurns **恰好用满**但正常收敛时被误报 max_turn_requests（"轮数 ≥ 上限"分不清"收敛"与"截断"）→ loop 加**显式**标记 `Result.TruncatedByMaxTurns`（additive 字段；X1 三段写路径一字不动），端点按它判（真 hub MaxTurns=1 单轮收敛 → end_turn；真截断（模型一直发工具调用）→ max_turn_requests）；④ **帧额度 off-by-one**：行分隔符被算进额度 → 恰好 8MiB 的合法帧被误拒（-32600）→ 额度改按**帧体**字节算（分隔符不算）；⑤ **CLI 钉子隔离**：子进程传 `-config <临时目录>/none.toml`（不读用户家目录配置）、buildACPBin 用包目录**绝对路径**（不依赖 cwd="."）；**第三轮终查**：⑥ Authorization 脱敏改**不靠 scheme 白名单**定位值（有第二词遮第二词、无第二词遮首词）+ 参数化鉴权头敏感参数（response=/Credential=/Signature= 等）值也遮——修掉凭据明文外发残留（H18）；⑦ 坏版本**通知**不再回帧（JSON-RPC §4.1：通知无响应）；**评审循环 critic 3 轮/reviewer 3 轮至 clean** |

### v4-3 · blob 外置 + 大 payload + 向量引擎/RAG（施工切分，2026-10-31，来源：14-v4-plan 排期表第 3 项；G2e/D4 拍板做、G2d 向量/RAG 同日拍板做）

> **形态定调**：blob 接口 v2-S5 已立（core/storage.Blob/BlobMeta，`BlobMeta.Ref()`=`Event.binary_ref` 唯一挂接点），本刀落**文件系统实现** + D4 挂接。大工具输出/原始条目——"不该进事件流"的字节——外置进 blob，事件 envelope 只留一个引用。blob **不是第二状态源**：可在引用还在时整体重建，事件流才是"有没有这条引用"的唯一事实。**X1 不可破坏**：只改 completed 的 result 载荷内容 + envelope.BinaryRef，intent→completed/failed 三段结构与落盘序一字不动；**零契约改动**（ToolResult 形状不变，"被外置"的信号就是 BinaryRef 非空，读侧据此取全文）。
>
> **B 半形态定调（G2d 向量/RAG）**：向量接口同样 v2-S5 已立（core/storage.Retriever/VectorIndex），本刀落**内存实现**（可整体重建的缓存——SourceRef 指回事件流，丢了重建，故不做持久化）+ RAG 接入面（`vector_search` 工具 + ingest/search 宿主方法与 HTTP 端点）。**框架不算 embedding**：查询向量由宿主经 `EmbeddingProvider` 槽位转（生产 nil = 工具不注册/端点 503 = 零行为变化）**补注（2026-11-02）**：生产槽位已可接（缺口 2：`openai.Embedder` + `[embedding]` 装配；model 全空才保持 nil）；概率性检索**独立于指针解析**（refs 核验要精确、向量检索要召回，两者不混）。

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| **v4-3-A G2e blob 实现 + D4 大 payload 外置** | ① **blobfs**：`runtime/store/blobfs` 实现 core/storage.Blob（一 id 一目录 = meta.json + content，引用寻址；Put 幂等——同 id 同内容原样返回已有元数据/不一致 ErrConflict/空 id 实现发号；Get 流式 + Stat（ErrNotFound）；Delete 幂等**不级联删事件**；临时文件+rename 防半截、半截/损坏读时明确 error 不静默；并发**固定分片锁**（256 片按 id 哈希：同 id 恒落同一片 → 互斥语义与 per-id 锁一致；锁表**有界**，不只增不减）；MediaType/Size 落 meta）② **D4 挂接**：`core/loop` 新增 BlobSink+BlobMaxInline，tool.call.completed 的 result 超阈值整体写 blob、payload 换摘要形态（text = 「**占位标记**（点名 blob id）+ 前 256 字预览」、Data 置空、**Data-only 块给占位不留空块**、GraphEdges/Taint/Replay 留）、envelope.BinaryRef=meta.Ref()；**子 agent 同样继承**（buildChildLoop 透传 BlobSink/BlobMaxInline → 子 agent 的大 result 也外置）；关（nil/0）= 逐字节等价酸测试；外置失败 fail-open 保 X1；tool.call.failed **不外置** ③ **host 装配**：Config.BlobDir+BlobMaxInline（TOML `[blob]`）+ New 建 blobfs（开了才建）+ loopFor 接 sink + 读面 `GET /api/blobs/{id}`（流式取全文；400 文案**点名 blob id**，不串味成 session_id）④ **组合面**：core/storage.Set.Blob 槽位接实现（构造器在 runtime，Set 不改形状）| blobfs 单元全绿（往返/大文件流式/幂等/ErrConflict/ErrNotFound/Delete 幂等/并发同 id/半截明确 error/meta.Size+MediaType/**锁分片有界**）；D4 集成（小不外置酸测试**逐字节**等价、阈值**恰等于**不外置+逐字节内联、大外置 BinaryRef 非空+落 blob+读侧取回全文==原 result、**fail-open**（Put 恒返错）completed 仍落+载荷逐字节原样+BinaryRef 空、Data-only 摘要**非空占位**、Data 摘要置空而 blob 全文完整+读侧可还原、**子 agent 大 result 也外置**、failed 不外置）；**Set 槽位可承载（单测证明），生产装配面走 Hub.blob 直挂**；`codegen -check`/build/vet/test/gofmt 全绿；X1 三段序钉子不回归；07 D4 ·→✅ + 合计同步 | **✅ 完成（2026-10-31，主 agent 亲手落 + 变异自证）**：blobfs（doc 写明磁盘布局/幂等/损坏口径/Delete 不级联）+ core/loop/blob.go（externalizeResult + summarizeToolResult；只 import core/storage、不引 runtime，守 core 边界）+ host（Config/validate/New/loopFor/serveBlob + Handler 路由）+ cmd TOML `[blob]`。**零契约改动**：ToolResult 一字不加字段（BinaryRef 早在 Event 契约里）→ 不走 F7 全链、schema/codegen/INVENTORY/样本/钉子/TS 三件套零改动。变异自证：幂等分支破坏→钉子红；外置阈值判定取反→D4 集成钉子红（临时改、跑、恢复）。**critic 修复轮（2026-10-31，v4-3-A 未提交刀的评审）**：① **子 agent 不继承 blob**（buildChildLoop 漏传 BlobSink/BlobMaxInline → 子 agent 的大输出留在事件流里，多 agent 模式 D4 落空）→ 透传 + 钉子（子的大 result BinaryRef 非空）；② **fail-open 零覆盖**（注入 Put 恒返错 → completed 仍落 + 载荷逐字节原样 + BinaryRef 空）；③ **Data-only 结果摘要退化为空块**（折叠侧不看 BinaryRef → 摘要必须自解释）→ 占位标记（点名 blob id + 预览），Data-only 块也给占位；④ 酸测试/阈值边界/Data 置空**补强到逐字节**（阈值恰等于不外置、阈值内逐字节等价、Data 摘要空而 blob 全文完整）；⑤ blobfs 写锁由 per-id map（只增不减）改**固定分片**（256 片，同 id 同片）；⑥ `/api/blobs` 400 文案由"session_id 不合法"改**点名 blob id**。**挂账**：① **折叠侧无标记**——FoldConversation/resume **不看 envelope（只读 payload）**，续跑看到的是**摘要/占位**（占位文本里**含 blob id** + 前 N 字预览）；**取全文的两条路**：① 消费方从**占位文本解析 blob id**（折叠侧手上**没有 envelope**，不是“按 BinaryRef 调”）去调 GET /api/blobs/{id}；② 回**事件流**查该条的 envelope.binary_ref 再调同端点；改 fold 直接取全文是后续消费方增强，不影响正确性——full 在 blob 里不丢）；**ACP 桥同样只见摘要/占位**（runtime/acp 的 bridge.go 读 completed 的 **payload**、不看 envelope.binary_ref → 客户端拿到的也是占位 + 预览，全文同样只能靠占位里的 blob id 回查——同一条挂账，不是新问题）；② ~~**blob 回收策略未做**（磁盘单调增长 + 崩溃残留临时文件/孤儿目录）~~ → **缺口 4 后半已落地**（见下 **v4-3-C** 行：`runtime/store/blobfs` 的 `GC` 三扫 + `alasol blob-gc` CLI）；**份数口径（内容寻址后）**：外置 id = `Crockford26(SHA-256("blob\x00"‖raw)[:16])`（**内容寻址**）→ 同一份内容重放 = **同一个 blob**（blobfs 幂等分支生效，**不写第二份**）；不同内容 = 不同 id；③ **崩溃窗口不可消除**——fail-open 只覆盖 Put **返错**；blob 写成功、completed 落盘前崩溃 → 孤儿 blob（引用与内容**最终一致**：顺序保证引用不会指向空处，崩溃续跑按 `binary_ref` 补取全文）；④ 对象存储/DB 大字段作为 blobfs 的替换实现留后续（换实现消费方零改动，G2b）【复查修复轮（2026-10-31，v4-3-A 未提交刀的复查 findings）】：① **F1（p2）D4 只挂 hub 主 session 一条路径**——fork / resume(run) / 重放三条同样写 tool.call.completed 的路径没外置 → **接线全做**（不做挂账）：host/fork.go 的 fork loop 透传 h.blob / h.cfg.BlobMaxInline（与 loopFor 同口径）；core/loop/replay.go 的 Loop.Replay 走 externalizeResult（**重放与原执行同口径**，落盘形状一致）；cmd/alasol 的 wire.go runnerOpts 加 blob 面 + run.go / resume.go 从 TOML `[blob]` 取（wire 侧建 blobfs 传入 loop.Config.BlobSink）；另把 core/loop/agenttools.go 的 finishSpawn（core/loop 里第三条 completed 写路径）一并接上（口径一字不变）。钉子：fork session 的大 result BinaryRef 非空 / Replay 路径外置 / run CLI 外置（临时目录 blobfs）+ 未配 blob 时 BinaryRef 空的酸测试。② **F2（p3）摘要不保证比原 result 小**（多 part 时占位+预览线性膨胀 3 倍+）→ 摘要做**总量上限**：只给**前 4 个** part 预览、其余合并成一条「…还有 N 个 part（全文见 blob <id>）」占位、摘要整体再压一道 **2KB 字节上限**（超限按字节预算裁**预览正文**，占位不可裁——自解释是硬要求）。钉子：100 part 结果 → 摘要 << 原文（断言摘要长度 < 原文长度）+ 预览**恰 256 rune**（原文用**非重复字符**“p0p1p2…”，方向错立刻被逐字比对抓住）+ 单 part 预览叠加时摘要 ≤ 2KB 且占位仍在。③ **F3（p3）blobfs 幂等“不写第二份”无钉子**（变异存活：跳过幂等分支整包仍绿）→ Store 加**写计数探针**（测试钩子字段，生产 nil；数内容落盘次数）+ 钉子：二次 Put（同内容）后写计数 == 1（并用新 id Put 做对照，证明探针确实在数）。【终查收尾轮（2026-10-31，未提交刀的 5 条 p3）】：**F1** finishSpawn 接线**无钉子** → 补钉子（core/loop/blob_spawn_test.go）：spawn 的 completed 超阈值 → BinaryRef 非空 + payload 是摘要 + blob 有全文 + **父拿到的 tool 消息仍是完整 summary**（外置只瘦身事件流，不许改坏回填——子 agent 的结论在父这里被静默截断是最坏形态）；变异自证：撤 finishSpawn 的 externalizeResult → 钉子红。**F2** resume CLI 装配**无钉子** → 补钉子（cmd/alasol/blob_resume_cli_test.go）：in-flight（replay=safe）大 result op → `alasol resume` 重放 → completed.BinaryRef 非空 + blob 有全文 + payload 是摘要；变异自证：撤 resume.go 的 BlobSink → 钉子红。**F3** 负阈值**口径漂移**（host fail-close 拒启动 vs run/resume 静默关）→ 唯一事实源 = host.ValidateBlobMaxInline（host/config.go），host.Config.validate / cmd 的 run / resume **三处复用**；run / resume 在**装配期**（任何执行/任何重放之前）就拒（exit 2 + 同款报错文案），不再静默把外置关掉；钉子：max_inline=-1 → run / resume 报错（host 侧另有参照钉子）；变异自证：撤 run/resume 的校验 → 钉子红。**F4** 小阈值下**“外置 = 瘦身”失效**（摘要的固定开销 > 刚过阈值的原文 → 外置反而把事件流撑大）→ 口径写明：**摘要预算 B = min(2KB, max(阈值/2, 768))**、**有效阈值 T = max(阈值, B)**，外置当且仅当 **原文 > T ≥ B ≥ 摘要**（“外置一定让事件流变小”是结构性保证，不靠碰巧）；代价如实写明：阈值配得比摘要预算还小时，(阈值, B] 区间的 result 不再外置（它们本来就很小）；钉子：max_inline=1024 + 多 part（短 part）结果 → 外置后 payload 字节 < 原样内联（变异自证：撤钳制 → 摘要 1381 > 内联 1222 → 红）。**F5** 重放不幂等（每次新 id）**只补挂账**（不改行为）：挂账② 补“每条被外置的 completed 一份 blob；**重放/重复执行各写一份**（幂等分支只在**同 id** 场景生效）”；挂账① 顺带补“**ACP 桥同样只见摘要/占位**”（runtime/acp 的 bridge.go 读 payload、不看 envelope.binary_ref）。 |

| **v4-3-B G2d 向量引擎 + RAG 接入** | ① **vecmem**：`runtime/store/vecmem` 实现 core/storage.Retriever + VectorIndex（**两面同一实例**；内存 map + 余弦 top-k，纯 Go 零依赖；Upsert 同 ID 覆盖 + **整批先校验后落**（**维度锁只在整批自洽时落**：混维度批次失败 → 库不被毒化（Dim() 仍 0），后续合法写入/检索照常）、Delete 幂等、Clear 供 Rebuild（连维度锁一起清）；K=0 → DefaultK=10、负 K/空 Embedding/维度不符 → ErrBadQuery、**首条维度锁定**（库变空即解锁）；Filter **支持**——Payload JSON 顶层字符串字段字节精确匹配（AND；键缺失/值非字符串/坏 JSON → 不命中，故不报 ErrUnsupported）；同分按 ID 升序定序；Truncated 当候选 > 返回时置位；**不持久化**（可整体重建的缓存：SourceRef 指回事件流，丢了不丢事实 F5）；喂进来即快照）② **RAG 接入**：`vector_search` 框架工具（参数收 **query 文本** + 可选 k/filter；宿主经 `EmbeddingProvider` 转向量——**框架不算 embedding**）+ 喂数据面 `Hub.IngestVectors(texts, source_refs)`（条目 id 按 **(source_ref, 分块文本内容) 哈希**确定性派生——公式 = **`Crockford26(SHA-256("vec\x00" ‖ u64be(len(ref)) ‖ ref ‖ u64be(len(text)) ‖ text)[:16])`**（两段各带 8 字节大端长度前缀 → (ref,text)→字节串**单射**：ref/text 含 NUL 也不撞 id）；**不含批内序号**（批内序号跨批次会漂移 → 撞 id 静默覆盖无关条目）；同一段重喂覆盖同一条；source_refs **可短于** texts（按序号对齐、缺的记空串），更长 → 400；**单批 ≤512 条**→ 超出 400）+ `Hub.SearchVectors` + **重建入口** `Hub.ClearVectors` / `Hub.RebuildVectors`（自带 id 的写入面，绕过 IngestVectors 的 id 派生；对齐 `Hub.RebuildGraph` 形态） ③ **host 装配**：Config.VectorsEnabled/VectorsTopK + `EmbeddingProvider` 槽位（生产 nil）**补注（2026-11-02）**：生产槽位已可接（缺口 2 `openai.Embedder`）+ TOML `[vectors]`（enabled/top_k，**无 dir**——内存实现）+ New 建 vecmem（开了才建）+ 注册表注册 vector_search（**仅当"开向量 + 有提供者"**；deny-all 默认照走）+ HTTP `GET /api/vectors/search?q=&k=&filter=`（**响应 k = 生效上限**：k=0 未指定 → VectorsTopK / vecmem.DefaultK）/ `POST /api/vectors`（texts+source_refs；**未启用 = 路由不注册 → 404 not_found**，与 blob 同口径——此前只是文案不实）④ **组合面**：`core/storage.Set.Vectors` 槽位接实现（写面经带 ok 的 `VectorWriter()` 取；只读实现 ok=false 不硬转型）| vecmem 单元全绿（覆盖/Delete 幂等/Clear/top-k **排序与余弦分数**（已知向量手算值）/维度不一致（写+读两侧，**整批不落**）/ErrBadQuery（负 K、空 Embedding、空 ID）/Filter 精确匹配（AND、大小写敏感、非字符串值不命中、空 Filter 不过滤）/Truncated（K 截断、K>候选不截断）/空库不报错/喂进来即快照/**并发 race**）；RAG 工具（注入 fake 提供者 → Execute 返回命中且文案交代 Score 口径；空 query/坏参数报错；**未配提供者 = 不注册**（酸测试，变异自证：改成无条件注册 → 钉子红））；host 端到端（ingest→search 全绿 + 400/404/503/500 各口径；**未开 = 404 + 方法报未启用**（酸测试）；开了没提供者 = 503 + 工具不注册；负 top_k 启动即拒）；`codegen -check`/build/vet/test/gofmt 全绿；07 G2d 补落地注记（合计不动）；**X1/契约零改动**（core/storage 一字不动）；追加钉子（维度锁不泄漏/跨批 id 不撞/source_refs 可短/k 报生效上限/重建入口可用/未启用 404 not_found） | **✅ 完成（2026-10-31，主 agent 亲手落 + 变异自证）**：vecmem（doc.go 写明定位/余弦口径/Filter 口径/维度锁定/单写者）+ host/vectors.go（EmbeddingProvider + vector_search 描述/执行器 + IngestVectors/SearchVectors + 确定性 id）+ host（Config/validate/New 装配/Handler 路由/serveVectorSearch/serveVectorIngest + writeVectorHubError 状态码映射）+ cmd TOML `[vectors]`。**零契约改动**：接口 v2-S5 已立 → 不走 F7 全链、schema/codegen/INVENTORY/样本/钉子/TS 三件套零改动。变异自证（三条，均临时改、跑、恢复）：① 余弦排序取反 → top-k 钉子红；② Filter 改成 ErrUnsupported / 静默忽略 → Filter 钉子 + host 端到端钉子红；③ vector_search 改成无条件注册 → 装配面酸测试钉子红。**挂账**：ingest 不走 CLI（最小面 = host 方法 + HTTP，CLI `alasol ingest` 未做）；向量库**不持久化**（内存实现；重建**可执行**：调用方按 SourceRef 从事件流重放 → `Hub.RebuildVectors(ctx, items)`（自带 id 的写入面，清空 + 灌入）或 `Hub.ClearVectors(ctx)` + 逐条 `IngestVectors`）；embedding 提供者生产未接（槽位预留，nil = 工具不注册/端点 503）；HTTP search 面的 filter 是 JSON 查询参数（`filter={"kind":"note"}`） **critic 修复轮（2026-10-31，v4-3-B 未提交刀的评审）**：① **维度锁泄漏**（Upsert 先落 s.dim 再校验整批 → 空库 + 混维度批次失败后留下 Len=0/Dim≠0 的毒化状态，此后合法写入/检索恒 ErrBadQuery，进程内不可恢复）→ 改局部 effDim 校验、**整批通过才落锁 + 落条目**（失败路径 s.dim/byID 均不动）+ 钉子；② **source_refs 短于 texts 被宿主方法拒**（文档说可短、实为 500）→ 允许短的（按序号对齐、缺的记空串），只拒"更长"且长度错**包 storage.ErrBadQuery**（→400）+ 端到端钉子；③ **条目 id 按批内序号派生**（跨批次撞 id → 静默覆盖无关条目）→ 改 **(source_ref, 分块文本内容) 哈希**（同段重喂覆盖同一条、不同文本不撞）+ 钉子；④ **"未启用 = 端点不注册"只是文案**（路由无条件注册）→ **真做**：未启用时不注册这两条路由（路径认不得 → 404 not_found，与 blob 同口径）+ 文案/注释/测试断言同步；⑤ **search 响应 k 报"请求的 k"**（k=0 回显 0 无意义）→ 报**生效上限**（k=0 → VectorsTopK / vecmem.DefaultK）+ 钉子；⑥ **重建入口未导出**（Clear/自带 id 写入面都不可达）→ 导出 `Hub.ClearVectors` + `Hub.RebuildVectors`（自带 id 的写入面，绕过 IngestVectors 的 id 派生；对齐 RebuildGraph 形态）+ 钉子。**零契约改动**（core/storage 一字不动；新增的只是宿主方法与实现细节，HTTP 响应形状只补 k 的语义说明） **复审轮（2026-10-31，v4-3-B 第二轮 findings）**：① **条目 id 派生换无歧义编码**——旧口径 `SHA-256("vec\x00" ‖ ref ‖ "\x00" ‖ text)` 的输入**不是单射**（裸 NUL 分隔符：(ref=`a\x00b`,text=`c`) 与 (ref=`a`,text=`b\x00c`) 拼出同一串 → **同一个 id** → 后喂的静默覆盖先喂的；公开 HTTP 面 `\u0000` 可达）→ 现口径 **`Crockford26(SHA-256("vec\x00" ‖ u64be(len(ref)) ‖ ref ‖ u64be(len(text)) ‖ text)[:16])`**（两段各带 8 字节大端长度前缀 → 单射；含 NUL 也不撞 id）+ 钉子（含 NUL 的两组 → 两个 id + 库里两条）；② **文档公式 ≡ 实现**（注释漏了 `"vec\x00"` 域前缀 → 照文档算不出实现给的 id）→ 注释/07/11 **逐字**同一公式 + 钉子（测试里**手算**公式比对 `IngestVectors` 返回的 id）；③ **重建口径写明 + 加重建闸**——`Clear+Upsert` **非原子**（无 staging+swap）：宿主加 `vectorGate`（RWMutex + 在途重建计数）→ 重建跨 Clear+Upsert 持写锁、**重建在途的检索回"重建中"**（HTTP 503 `vector_rebuilding`；不把重建窗口里的空库当"没有命中"）；失败口径：Clear 失败 → 库**保持原样**（可能非空）、Clear 成功而 Upsert 失败 → 库**为空**（整批不落），都不是半重建；重建**不自动回滚**（失败后调用方重试重建）+ 钉子；④ **ingest 单批上限 512 条**（一条文本 = 一次 provider 调用、批内串行；不设上限 = 一个请求串起 N 次外部调用）→ 超出 400 并点名上限（宿主方法包 `storage.ErrBadQuery`）+ 钉子 |
| **v4-3-C 缺口 4 后半 · blob 回收（GC）+ 重放幂等** | ① **重放幂等**：外置 id 改**内容寻址** `Crockford26(SHA-256("blob\x00"‖完整 ToolResult JSON)[:16])`（core/loop/blob.go contentBlobID；26 字符 Crockford，形态兼容 ULID 但**无时间序**）→ 同一份内容重放 = 同一 id → blobfs 幂等分支生效（**不写第二份**；写计数探针钉住）；不同内容 = 不同 id ② **GC**：`runtime/store/blobfs` 的 `GC`（`GCOptions{Grace,Retention,DryRun}` → `GCReport{ScannedBlobs,RemovedTmp,RemovedOrphans,RemovedAged,FreedBytes,Skipped,DryRun}`）**三扫**：扫 1 `.content-*.tmp`/`.meta-*.tmp` 残留（超 Grace 才删）→ 扫 2 **孤儿目录**（合法 ULID 名 + 无 meta.json + 目录 mtime 超 Grace → 整目录删）→ 扫 3 **mtime 清扫**（仅 `Retention>0`：meta.json 存在 = 完整 blob，**龄期 = 最后外置时间**，超 Retention → 整目录删；事件流 binary_ref 不动，读侧按契约报"内容已回收"） ③ **CLI**：`alasol blob-gc`（读 `[blob].dir`；单行 JSON 到 stdout；`-retention` 默认 0 = 只安全两扫；`-grace` 默认 1h、**显式 0 = 仍 1h**（安全网不给关）；**负值即拒** exit 2 + 点名；`[blob].dir` 未配 = exit 2） ④ **口径**：Grace ≤ 0 = 默认 1h（零宽限会误删在途写）；扫 2 的锁内复核用**内容证据**（meta.json 在 / 仍有 .tmp = 可能是活的 → 不删；空壳或只剩外来文件 → 孤儿照删）**不用目录 mtime**（扫 1 删 .tmp 会刷新它 → 漏删孤儿 + DryRun/真跑数字不一致）；id 分片锁**只在同进程内**互斥，跨进程唯一防线是 Grace（跨进程锁不做）；单目标删不掉 → 跳过 + 计入 `Skipped` + 继续扫（不中止整轮） | blobfs GC 单元全绿（三扫各司其职、Grace 保护在途写、Retention=0 只安全两扫、DryRun 与真跑数字一致、**含 .tmp 的孤儿真跑后消失**、**GC 与并发 Put 竞争 blob 存活**、**幂等 Put 刷新龄期 → Retention 下不被删**、**Grace ≤ 0 = 默认 1h**、**一个删不掉不影响其余**）；CLI 端到端（读 dir 出单行 JSON / 未配 dir exit 2 / flag 映射 / **负值 exit 2** / **-grace 0 = 默认 1h** / **-grace 非默认值真生效**）；core/loop 集成（Run+Replay = 一个 blob + 写计数 = 1 + **重放刷新 blob 龄期**）；`codegen -check`/build/vet/test/gofmt 全绿；**零契约改动**（GCReport 是 CLI 报告结构，不在 contracts 面） | ✅ 完成（2026-11-02，缺口 4 后半 + critic findings 修复轮）：① **F1（p1）扫 1 删 .tmp 刷新目录 mtime → 扫 2 复核误判"在途写"漏删孤儿 + DryRun/真跑数字不一致** → 复核改用**内容证据**（meta.json 在 / 仍有 .tmp = 可能是活的 → 不删；空壳或只剩外来文件 → 孤儿照删），**不用目录 mtime**；② **F3（p2）retention 龄期 = 首次写入**（重复引用不刷新）→ **幂等 Put 触碰 meta.json 的 mtime**（**不重写 meta 内容**——"原样返回已有元数据、不写第二份"的契约不动）→ 龄期语义改成**最后外置时间**（重复引用刷新龄期）；③ **F4（p3）CLI 负值/零值静默改写** → `-retention < 0` / `-grace < 0` **退出码 2 + 点名**（负值 = 非法配置即拒，同 host.ValidateBlobMaxInline 纪律）；`-grace 0` 语义**写死并写进帮助** = 用默认 1h；④ **F2（p2）跨进程零互斥** → gc.go 头注释 + CLI 帮助如实标注（id 分片锁只在同进程内互斥；跨进程唯一防线是 Grace；跨进程锁不做），并把删除路径的"文件被占用"改成**跳过该目录 + 计入 `Skipped` + 继续扫**（不中止整轮）；⑤ **F5（p3）测试缺口** → 补 Grace 默认值钉子 / CLI `-grace` 非默认值真生效 / 含 .tmp 的孤儿 / GC 与并发 Put 四枚。**变异自证**：复核改回实时目录 mtime → 含 tmp 孤儿 + dry/real 不一致两枚红；幂等 Put 不触 mtime → 龄期红；CLI 去掉负值拒绝 → 红；`defaultGCGrace` 改 0 → 红；CLI `-grace` 映射改 `Grace:0` → 红。 |
### v4-4 · WAL 预写日志 + 重试过程留痕（施工切分，2026-11-01，来源：用户建议"崩溃后内存/队列中的操作尽量可恢复——正式写之前先写 WAL，至少多保存一些操作"；B 半 = WAL，W15 成本预算顺手小刀）

> **设计定位（必须守住的口径）**：**WAL 不是事实源**（F5 不受影响）——事件流（core.Store）仍是唯一事实源；WAL 是崩溃恢复的**补充面**：记录"正式写之前"的中间态，崩溃后至少知道"当时在干什么、吐过什么"。**可丢弃 / 可截断 / 可 GC**（删掉整个 WAL 目录不丢任何事实，事件流照常完整回放）；**不影响 X1**（四个写点全在 intent→completed/failed 落盘序**之前/之外**，三段结构与落盘序一字不动）；**零契约改动**（不走 F7 全链——schema/codegen/INVENTORY/样本/TS 三件套零改动）。**酸测试：未配置 WAL = 零行为变化**（不写任何文件、行为逐事件等价）。

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| **v4-4-A core/retry 通用重试组件 + provider 级小重试** | 07 M6/M7：`core/retry`（`Do(ctx, policy, fn)`）+ `runtime/provider/openai` 小重试；**core/loop 的模型请求调用点不接**（两层重试会放大） | 见 07 M6/M7 行内 | **✅ 完成（2026-10-31，critic 复查轮 5 条修复；`core/retry` + provider 白名单 429/5xx+net.Error、Retry-After 采信仍受 MaxBackoff 封顶、永久传输错不重试）** |
| **v4-4-B WAL 预写日志（本刀主体）+ W15 成本预算顺手小刀** | ① **WAL 面**：`core/wal.go`（kind 常量 + `WalSink` 写面 + 六种载荷 + `WalRecord` 文件形状；住 core 与 ProviderEvent 的 Kind* 同纪律）+ `core/loop/wal.go` 六个写点——`turn.begin`（provider.Run 前：turn/模型/消息条数/工具条数）、`turn.delta`（**流式片段逐条**，本刀最大价值：model.delta 是 durable=false 的流事件，崩溃后本轮输出整轮蒸发）、`turn.end`（model.turn **落盘后**，顺序有意）、`turn.failed`（provider 返错时：普通失败收口，**不是崩溃**）、`turn.retry`（**provider.Run 内部**重试发生时 → provider 经 emit 通道发 `provider.retry` → loop 的 emitFunc → walAppend；**provider 侧不穿 WAL 句柄**，纯观测、只 write 不 fsync）、`blob.plan`（tool.call.intent **发布前**：tool_call_id + 阈值；体量在计划时未知=0）；loop.Config.WalSink（nil=关）② **实现**：`runtime/store/walfile`——per-session `<session>.wal` 追加式 NDJSON；**fsync 策略：逐条不 fsync、轮次边界（begin/end/failed）+ Close fsync**（进程崩溃不需要 fsync——write 进 OS page cache 就在；逐条 fsync 会把打字机效果卡死；blob.plan 是逐工具调用的，不当边界；代价：机器掉电可能丢尾部未 fsync 的 delta，如实写明）；半截/坏行容忍（计入 `CorruptLines`，不当错误）；**超 1MB 的行跳过后继续读后面的行**（不中断扫描）；`Truncate`/`Remove` 两个原语 + **GC 策略已落地**（缺口 5 第一件：`walfile.GC` + `alasol wal-gc`，判据与报告见行末落地注记）③ **恢复面**：`Reconcile`（纯函数）+ `FactsFromEvents`（从事件流折对账事实）→ 报告 = 在飞轮次（最后一个有 begin 无 end 的轮次 + 拼回的 delta 文本，上限 64KB + 截断标记）/ 孤儿 blob（plan 在 WAL、事件流既无 completed 也无 failed = 这次工具调用没走完）/ 作废计划（completed 落了但没 binary_ref）；**不误报**（有对应 completed+binary_ref 两个名单都不进；failed 也算收口）；`Hub.WALReport` + **`alasol wal -session <id>` 子命令**（单行 JSON 到 stdout、人话摘要到 stderr）+ **resume 时自动打印一行摘要**。**观察面不是写回面**：delta 可供展示，绝不自动补进事件流 ④ **host 装配**：Config.WalDir + TOML `[wal]`（dir；**空 = 关 = 酸测试**）+ per-session Writer 懒建（session 的 loop 建成时开）+ Close 收口；开失败只告警不拒绝本轮（WAL 尽力而为）；目录建不出来 = 拒绝启动（显式要的能力没起来）⑤ **W15 顺手小刀**：`host/budget.go` 成本闸门——**默认不设上限**（W15 拍板；BudgetMaxTokens=0 = 一次查询都不发 = 零行为变化），配了才在 Run 前（超限拒绝发起）/ Run 后（越界告警）设检查点；数据源 = 索引器 cost_tokens（J21）；负值启动即拒 | 写点序列（一轮多 delta 全记录、begin/delta/end 次序、turn.end 在 model.turn 之后——用"收到 turn.end 时回读事件流"钉死）；写失败不阻断 Run（事件流完整 + 恰好一条 Warn + 每条都试过）；blob.plan 先于 tool.call.intent 与 blob.Put（共享顺序日志）；**酸测试**（关/开两次 Run 事件流逐条等价，**含归一化 payload 深度比对** + nil sink 不 panic）；walfile（往返/per-session 隔离/文件缺失不报错/半截行容忍计 CorruptLines/并发行不相交 -race/Close 幂等且关后 Append 返错/非法 session id 拒路径穿越）；对账（在飞轮次含模型与 delta 文本、全 end 不误报、孤儿、有 binary_ref 不误报、作废计划、重复 plan 去重、坏 payload 跳过、长文本截断 + 字符数全量、 FactsFromEvents 只认 completed）；host 端到端（真 hub 跑一轮 → 文件在 + 报告干净；seed 崩溃现场 → 报告列出在飞轮次/孤儿；外置真发生 → 不误报；**未配置 = 一个文件都不建**；坏目录拒绝启动）；CLI 端到端（seed 崩溃现场 → 单行 JSON 含轮次/孤儿、退出码 0；未配置 wal_exists=false 退出码 0；非法 session 退出码 2；从配置文件读 [wal].dir；run CLI 路径同样挂 WAL）；W15（默认不设上限连跑 3 轮不拒、负值拒绝启动、超限拒绝且文案点名 session/上限、边界 ≥ 才拒）；**变异自证**（去掉 turn.delta 写点 → 钉子红；破坏对账孤儿判定 → 钉子红；临时改、跑、恢复） | **✅ 完成（2026-11-01，主 agent 亲手落 + 变异自证两枚；**critic findings 九条修复轮同日落地**：① blob.plan 写点挪到 intent **之前**（写点全在三段窗口外；blob.plan 不再 fsync）② 加 `turn.failed`：普通失败不算在飞、tool.call.failed 也不算孤儿候选 ③ >1MB 行跳过后继续读（Scanner 粘性错 → 后续记录全丢）④ Close 先 fsync 再关 ⑤ 酸测试补归一化 payload 深度比对 ⑥ Warn 去重作用域文案改准（loop 级）⑦ fork 旁支同口径挂 WAL + RunBtw 前接 checkBudget ⑧ 07/14 文档乱码 + config.go 注释复原 ⑨ resume 失败分支也打 WAL 摘要**）**，**终查 findings 四条修复轮同日落地**：⑩ **对账改按记录流分段归组**（轮次号"每次 Run 内从 1 起"→ 跨 Run 撞号；按号全局归集会把上一次 Run 的**悬空 turn.begin** 判成"已收口"→ 报告谎报"无崩溃残留"= 假阴性；现按"每条 turn.begin 开一段"归组，段无收口 = 在飞，并如实报未收口段数）⑪ **ctx 取消路径的 turn.failed 钉子**（loop 层 + host 端到端：被取消不算崩溃，对账不误报）⑫ **planExternalize 的 spawn/replay 两路径钉子**（blob.plan 早于 intent / 早于重放动手）⑬ **Close 的 fsync 观测钉子**（Writer 依赖的文件面抽成接口 → 可注入记账，钉"Close 时 fsync 恰好一次且排在 close 之前"）**）**：`core/wal.go` + `core/loop/wal.go` + `runtime/store/walfile`（walfile.go + reconcile.go）+ `host/wal.go` / `host/budget.go` + `cmd/alasol/wal.go`（`alasol wal` 子命令）+ host/cmd 装配（`[wal]` / `[budget]`）。**零契约改动**（schema/codegen/INVENTORY/样本/TS 三件套零改动）。**挂账**：① ~~WAL GC/截断策略未做~~ → **已落地（2026-11-02，缺口 5 第一件）**（**按龄期回收的策略已落地；"截断"仍限手工 Truncate 原语——按大小截断的自动策略未做**）：`walfile.GC(dir, GCOptions{Retention 必给>0, DryRun, Facts})`——判据三条件全过才删（mtime 超龄 + reconcile 干净：**InFlightTurn / OrphanBlobToolCalls 两个"没走完"信号全空**；**StaleBlobPlans（作废计划）不挡回收**——它 = "走完了但没外置"（completed 落了但没 binary_ref；正常收尾占多数），不是崩溃上下文，当脏信号 = 回收面落空；CorruptLines 不算脏、读失败 Skipped 宁留不删；合法 `<session>.wal` 形态，外来文件/子目录不动），删除动作 = **Remove 整文件**（回收，非 Truncate）；DryRun 与真跑**同一判据**（数字一致，**除"删不掉"那一刻**：真跑记 Skipped、dry 记 Removed——判据同源、执行事实分叉，如实写明）、删不掉 Skipped 不中止、文件名序确定性扫描；**活 session 的 WAL 被删 = 观察面重置**（Writer 写路径自愈：文件没了就重建新文件再写——POSIX 上"unlink 后 fd 续写死 inode → 后续 WAL 静默丢"那个坑因此不成立；跨进程同款生效）；CLI `alasol wal-gc`（读 [wal].dir，**-wal-dir 显式 flag 优先于配置文件**；-retention 缺省 0/负值 exit 2 点名（库层同口径）；[wal].dir 未配/目录不存在 exit 2；单行 JSON 报告；**不读事件库 → 空事实 = 有 blob.plan 的 WAL 保守不删**）；判据与孤儿 blob 清理共用同一批对账键② 机器掉电可能丢尾部未 fsync 的 delta（逐条 fsync 会卡死打字机，有意取舍）③ ~~重试过程留痕挂账：provider 3 次尝试的过程没记进任何观察面（provider 上下文无 WAL 句柄，为它穿层 = core.Provider 接口多带一个 sink，代价远超收益）~~ → **已落地（2026-11-02，缺口 5 第二件）**：provider 内部重试经 **emit 通道**上报（`core.KindProviderRetry` → `ProviderEvent.Retry`：attempt / 上次失败摘要 / 实测退避时长）→ loop 的 emitFunc 收到后经 walAppend 落一条 **turn.retry**；**provider 侧不穿 WAL 句柄**（core.Provider 接口形状不动，跨层只走事件——原挂账担心的事没有发生）；纯观测（不参与对账判定、不改控制流；重试后成功时最终结果与不留痕路径一字不差）、只 write 不 fsync（逐次重试的观测，与 delta 同组）；embeddings 路径没有 emit → 不进 WAL（如实标注）④ ~~W15 逐轮制动未做（per-Run 粒度，turn 内刹车需 loop 侧接线）~~ → **已落地（2026-11-02，缺口 5 第二件）**：`loop.Config.TurnBudget` 逐轮钩子——每轮 provider.Run **之前**查一次该 session 的累计成本，超限 → 本轮**不发起**、Run 以 `Result.TruncatedByBudget` 收口（照 MaxTurns 截断口径：Run 正常返回不报错）；**两层检查**（per-Run：RunPrompt 入口拒 + Run 后越界告警 + fork 路径；per-turn：轮间钩子）+ **子 agent 也走**（`buildChildLoop` 透传同一钩子；闭包绑同一 session id → 父子查同一份账；不传则多 agent 模式下闸门半边失效）⑤ 崩溃残留的 .tmp 类文件不主动清理（walfile 自己不产 .tmp；只认 `<session>.wal` 一个名字，不猜别人的文件是谁的）⑥ 对账是一次 **O(session 事件数) 全量扫描**（读完该 session 的 completed 再折 Facts；诊断命令 / resume 摘要路径，非每轮热路径；要在 UI 常驻得改索引器派生表） |

### v4-5 · extension runtime 收口 + MCP server（B 类）（施工切分，2026-11-01，来源：14-v4-plan 排期表第 5 项「E2 剩余」；extension runtime 收口 + MCP server（B 类））

> **形态定调**：14 第 5 项拍板"extension runtime 重点不是新机制"——本刀**不新造扩展点机制**，只做两件事：① **A 类剩余收口 + E3/E5 做实**（统一扩展注册表 `core/registry`：能力裁决 fail-close、裁决期 panic 隔离、撤销也 fail-close；观测面 = `GET /api/extensions` 只读快照）；② **B 类边界落第一个出口**：**MCP server**（`alasol mcp-serve`，`runtime/mcpserver`）——把 harness 的工具面按**白名单**暴露成 MCP tools（stdio 传输；鉴权 = 谁拉起谁负责，同 ACP 的 ssh 口径）。**compaction / assembler / scheduler 三个 kind 留位、当前不建机制**（它们只在 `ExtensionMeta.Kind` 的开放取值清单里占位，没有注册/解析/调度机制，也没有机制契约）。
>
> **暴露面口径（fail-close，与空白名单同精神）**：白名单是**唯一的暴露闸**；空 = 零暴露；**只收非 `mcp__*` 名**（MCP client 侧的内部命名空间不上公网面——外部 MCP 工具经聚合不二次暴露，白名单点名也拒）；**注册表 State 参与暴露与执行判定**（停用的不暴露，且调用前再查一次，防"list 之后才被停用"）；**非法 MCP 工具名不挂载**（go-sdk 只记日志照样挂 = 假暴露）；**审批闸不适用**（approvable 工具如 bash 被点名即**零审批**执行——要审批语义就别暴露它）。

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| **v4-5 extension runtime 收口（A 类剩余 + E3/E5 做实）+ MCP server（B 类）** | ① **core/registry 做实 E3/E5**：Enable 走 Adjudicator 逐能力裁决（任一非 allow → 拒绝启用，且**先撤销已有 enabled** 再报错，fail-close）；裁决在锁外做 + **裁决方 panic 隔离**（异常一律 deny）；同名重注册判 ErrStale（TOCTOU 不背书）；撤销也 fail-close（enabled→disabled）② **host 观测面**：`GET /api/extensions`（全 kind 快照 + Health，**只读**；**空能力列表归一为 `[]`**，与 /api/sessions 同纪律；**Hub.Registry() 只导出只读接口**——不把可写的 *registry.Registry 递出去）③ **runtime/mcpserver（B 类 MCP server）**：`New(reg, 白名单, opts)` → `tools/list` + `tools/call`（直接调注册表执行器；**不走 loop 的 X1 三段写路径**，不建 session）；fail-close 白名单 + `mcp__*` 拒绝 + **State 过滤** + 非法工具名拒挂 + E5 per-call recover + 审批闸不适用（诚实边界写进包 doc）④ **cmd**：`alasol mcp-serve`（stdio 传输；**stdout 只走 MCP 帧、日志走 stderr**）+ TOML `[mcp_server] expose_tools`（**只走文件配置面**——安全敏感的暴露名单不进命令行，H12/H19） | mcpserver 单元全绿（list/call 全链、空白名单零暴露、白名单过滤、悬空名忽略、**disabled 不暴露 + 调用报错**、**mcp__* 拒绝 + 告警**、**非法工具名拒挂 + 告警**、panic 隔离、执行器错误隔离、参数原样过桥）；host 面（`GET /api/extensions` 200 + 快照 + Health + capabilities=`[]`）；**CLI 级钉子**（真起子进程：initialize/tools/list/tools/call 全链 + **stdout 逐行是合法 JSON-RPC 帧**）；`codegen -check`/build/vet/test/gofmt 全绿；**零契约改动**（X1 一字不动） | **✅ 完成（2026-11-01，v4-5；含 critic findings 修复轮：F1 State 参与暴露与执行判定 / F2 mcp__* 内部命名空间不上公网面 / F3 审批闸不适用进诚实边界清单 / F4 非法工具名不挂载 + 默认 logger 接 stderr / F5 Hub.Registry() 收窄为只读接口 / F6 capabilities 空值归一为 [] / F7 07·11·14 回填；变异自证：去掉 State 过滤 → 钉子红、去掉 mcp__ 拒绝 → 钉子红）**；**缺口 3 补齐（2026-11-02）：POST /api/extensions/{name}/enable\|disable 已挂**（不再是假旋钮——注册表 State **真的**双层门控 core/loop：① 可见集过滤（disabled 不进模型可见集）② 执行前复查（disabled → 落 failed/code=tool_disabled，且**排在策略链之前**：不产生 ask 人审、不误归因 policy_deny）；Replay（崩溃续跑重放）路径同样查 State（同 code=tool_disabled）；enable 写回带 **CAS**（按**状态版本号**：任何状态写含**同值 Disable** 都抬版本 → 裁决期间被改动 → ErrStale/409，不吞掉后来的 Disable）；名字**字节精确**（端点不 TrimSpace，与注册表同口径）；**两条诚实口径**：① **停用不持久**——纯内存态、不落事件流、无审计痕迹、**重启失效**（持久化需契约事件 kind，挂账）② **管理面无鉴权**——默认信任本机调用方（与 /api/approvals/{id}/respond 同类；鉴权交 SSH/系统，07 挂账"过策略/鉴权接入"）；**零契约改动**（X1 三段写路径形状一字不动） |

### v3-S6 · subagent 产品化（施工切分，2026-10-31，来源：14-v4-plan 排期表第 1 项；版本叙事已合并——"现在做的就等于做 v4"）

| 阶段 | 内容 | 验收 | 状态 |
|---|---|---|---|
| **v3-S6-A 契约增量 + core W6-A 消费** | ① F7 只增：`AgentType` 加 `context_strategy`（显式声明、不继承主 agent）+ `share_project_assets`（项目资产显式共享开关）② core 消费：`loop.Agents.ChildStrategy` 装配钩子（子 loop 按类型声明装配策略；未声明/append = 追加式默认）；**fail-close 五分支**（kind 空 / 越枚举 / 无钩子 / 钩子返 nil 工厂 / 工厂产 nil Assembler）绝不静默降级 ③ 装载期闸门：`agents.ValidContextStrategyKind` 唯一枚举表 + TypeRegistry.Register / validateAgentTypes / validateWorkflowContextStrategy 三道装载闸 ④ Compose 深拷贝补全（agent_type 新字段指针+Params、工作流级 Params、Gate.Params、Command 指针）+ 同 id 合并口径写明（base 整条胜出） | W6-A 语义钉死（A 不继承 / B 钩子装配 / C·D·E·F·G fail-close 各分支）；装载期三道闸 + 运行期兜底同口径；变异自证全数会红；X1 三段一字不动；F7 只增（schema 无删字段/改旧义）；全量测试+TS 三件套绿 | **✅ 完成（2026-10-31，评审循环 critic 4 轮/reviewer 4 轮至 p3-1 收口）**：契约+core 全落地；**剩 1 p3 文案级已顺手改**（宿主配置面只接受 ""/tiered、J8 扩展点在代码侧）；**host 未接多 agent 装配**（不建树、loop.Config.Agents 不传）→ W6-A 端到端待 v3-S6-B |
| **v3-S6-B host 装配（五类+TOML+接线）** | host 装配 agent_type 注册表（内建 **planner/coder/critic/reviewer/observer** 五类 + TOML 自定义扩展；observer=只读 K20）→ `loop.Config.Agents` 接线（Tree 归属 session+父 agent，W12）→ spawn/complete 对模型可见（三闸门+策略链既有）→ W6-A 共享开关接注入区；W15 MaxTurns+prompt-timeout 先行 | 五类可 spawn、observer 只读收窄；GUI/协议面 agent 树真能派活；酸测试（不配置多 agent = 零行为变化）；07 回填+合计行同步 | **✅ 完成（2026-10-31，评审循环 critic 3 轮/reviewer 2 轮至 p3-2 放行）**：host/agenttypes.go（五类+root 类型+注册表合并校验）+ MultiAgent opt-in（默认关=零行为变化，A/B 事件流逐条对照钉住）+ agentTrees/session（W12）+ WithRootID（树 root ≡ 主 agent id）+ childStrategyFactory（W6-A：tiered 绑 child.ID / share_project_assets 才 SetRules / 未实现 kind 返 nil）+ GET /api/sessions/{id}/agents 只读快照（树存在即 200）+ 自动补 spawn/complete allow（P2-4 策略链照走）；**挂账**：/btw fork session 不装配 agent 树（派活面在 fork 上不通 + 快照 404 文案已改为中性陈述）→ 记 v4 跨进程/旁支收口 |

### v3-S5 · 框架整备与 UI 联调（施工切分，2026-01-30，用户拍板：v4 权限项冻结，先做"大概能用的完整框架"）

| 阶段 | 内容 | 验收 |
|---|---|---|
| **v3-S5 整备+联调** | ① `POST /api/sessions`（从 v4 提前：创建**本进程托管**的 session 返回 id，之后 `POST /api/sessions/{id}/prompt` 即可用——REST 直觉的开新会话入口，G25 自洽）② **与 UI 侧联调**：起真 hub（用户本地网关 `http://127.0.0.1:51142/compat/v1` + gpt-4omini 真模型）+ `D:\vscode\alasol-gui` 的 GUI（同源代理），走通"开 session→发 prompt→模型跑→事件流→上下文/成本面板→ask 审批应答→/btw→merge check"全流程；③ **联调暴露的阻塞项现场修**（走迷你评审循环）；④ 联调报告（可用/不可用清单） | REST 开新会话→prompt 真模型跑通；GUI 三面板实时可见；审批闭环（request→respond→工具执行/拒绝）；联调发现的阻塞项修完或如实挂账；07 注记同步 | **✅ 完成（2026-01-30，联调+修复评审至 clean）**：REST 开会话/真模型 prompt/三面板/审批 allow+deny 闭环/merge V6/回放**真机全通**（gpt-4o-mini 经用户网关）；联调暴露并修复：审批超时审计断链（settle ctx：审计留痕/执行守约分离）+ 取消护栏回退（执行与 spawn 前显式 ctx 检查）；**GUI 反馈轮**：G11 挂起审批枚举+WS 重发（at-least-once）、G12 一库一 hub 口径（hostable 字段+404 指引+文档明示，跨 hub 根治记 v4）；**GUI 反馈轮②**：G13 流式 model.delta、G14 思维链一条线（reasoning_effort 透传+宽容解析）、G15 gate 管理面（gate.left/枚举/auto gate leave 不复活）；**记 v4**：policy 结构化成因码 |

---


## 3. 施工期陷阱清单（照着抄，别自己发明）

| # | 陷阱 | 纪律 |
|---|---|---|
| 1 | **provider 的 `tool_call_id` 被改写/混用** | 原样存原样回传；`Event.id` 与它**永不共字段**；事件去重**只看 `Event.id`** |
| 2 | **id 长度截断** | ULID 是 **26 字符**；字段别设 `varchar(16)` |
| 3 | **JSON 里 id 存成 number** | 一律 string（>2^53 丢精度） |
| 4 | **Windows 路径大小写 / 符号链接** | `hash(cwd)` 前必须：解析符号链接 → 转小写 → 统一分隔符 → 去尾斜杠 |
| 5 | **Windows 进程树清理** | `taskkill /T /F /PID`；spawn 加 `windowsHide:true`；**压制 WER 弹窗** |
| 6 | **junction 误删** | rmTree 先解 junction 再删，防递归跟穿 |
| 7 | **git 走 shell 会被 cwd 劫持** | 绝对路径 `execFile git.exe`，不经 shell |
| 8 | **Bash 写入绕过路径守卫** | V0 已知边界 → **UI 必须声明**；V1 用沙箱解决 |
| 9 | **审批服务缺席时默认批准** | **fail-close**：缺席 = deny |
| 10 | **压缩重写历史 → 前缀缓存全废** | 压缩时**保留最前面的稳定段**（system / 规则 / ledger）不动 |
| 11 | **非 durable 流事件占 seq** | `durable=false` **不入库、不占 seq** |
| 12 | **多写者下 seq=提交序≠因果序** | **session 亲和写**（一个 session 只由其宿主进程 append） |
| 13 | **summary 是注入载荷出口** | summary 带 `taint`，继承来源事件最高污点级 |
| 14 | **workflow 规则能放宽权限** | workflow 规则**只可收窄**；deny/ask 跨来源并集 |
| 15 | **长工具的 TOCTOU** | 「判定只约束发起时刻」是**已知边界**，判定快照记进事件 |
| 16 | **can_spawn 环** | 环检测必须在 **workflow 加载时**做（不可信配置），不只类型注册时 |
| 17 | **窗口大小写 / Unicode 折叠** | provider id **不折叠大小写、不做规范化** |
| 18 | **改 module path 的代价** | 现在定死（已定 `github.com/Yorha9e/alasol`） |
| 19 | **三段事件后补** | `tool.call.intent` 必须在**阶段 1** 进契约、写路径在**阶段 4** 就用——阶段 8 再补会返工所有工具调用点 |
| 20 | **红仓上建不起 baseline** | baseline 必须支持**「只记录不阻断」**地首次建立，否则第一次跑就死锁 |
| 21 | **exec 维度靠工具名名单（v0 边界）** | 契约 `side_effect` 无 exec 枚举 → 生效权限收窄对执行类工具只能按名字判定（bash/exec/shell/sh/cmd/command/run/powershell/pwsh）；**名单外的执行类工具（python/make 等）不享 exec 收窄**（write 维度仍在）。根治 = 工具注册期显式声明所需能力（阶段 6 可做），别再扩名单 |

---

## 4. 施工期的新决策去哪记

- **改设计** → 改 `07`（唯一裁决来源），并同步被引用的文档
- **只新增一条决策** → 加进 `07` 对应区，编号延续现有体系（A–X）
- **发现文档间矛盾** → **立刻在 `07` 记录并修一处**，不要留到下一轮审阅（上一轮 14 条矛盾里有 4 条是并行演进自己长出来的）
- **数字与清单** → **只在源头定义一次**，其余引用不重抄（D21 纪律）

---

## 4.5 边做边讨论：施工期的设计协作流程

> 设计已冻结在 279 条决策上，**施工 session 拥有设计决策权**。本节规定"边做边讨论"怎么落，避免两个极端：要么事事回函等指示（阻塞），要么默默改设计不记录（腐化）。

### 4.5.1 默认原则：**自决 + 落文档**

| 情形 | 做法 |
|---|---|
| 遇到 `07` 未覆盖的实现细节 | **直接决定**，写进代码注释；若属于设计决策则补进 `07` |
| `07` 有 ★ 条目、你可以判断 | **直接决定**，把该条从 ★ 转 ✅，在提交信息里引用其 ID |
| 与已定决策**冲突** | **先停下**：要么改代码去符合决策，要么**改决策**（更新 `07` + 说明理由） |
| 跨模块契约变更 | **升级**（见 4.5.3） |
| 触及安全边界（权限、凭据、沙箱、可见性） | **升级** |

> 已成先例：施工期的「模型名不设框架级默认值」就是自决 + 落文档的一次正确处理（模型名解析属 provider 层职责）。

### 4.5.2 决策后必须同步三件事

1. **改 `07`**（唯一裁决来源）：编号延续 A–X 区，状态 ★ → ✅，并写清**为什么这么定**
2. **重跑 `07` 的汇总表**（自动生成，表头已注明）——否则汇总表会再次滞后于各区
3. **提交信息里引用决策 ID**（如 `refs 07/I5`），让改动可追溯到决策

### 4.5.3 升级路径

设计侧 session 已结项。升级走 **user-global 的 handoff**：

- 收件：`toProject: "user-global"`，`fromAgent` 回显**自己当前**的地址（`<label>:<sessionId>:<agentId>`）
- 触发条件：**跨模块契约变更**、**与已定决策冲突且改不动**、**安全边界**、**方向性岔路**
- 纯实现细节**不要升级**——自己定，落文档
- 升级时把问题、现状、已尝试的方案、建议一次写全（参考 `ho_64354db0157c` 的写法，它一次就把 4 处修法 + 2 处小修讲清楚了）

### 4.5.4 反腐化纪律

- **只在源头定义一次**（D21）：数字与清单不重抄
- **发现矛盾立刻修一处**，不要留到下一轮审阅（上一轮 14 条矛盾里 4 条是并行演进自己长出来的）
- **每条决策状态变了就重跑汇总表**
- 提交信息引用决策 ID → 决策与代码双向可查

---

## 4.6 下一阶段：session / per-prompt 面（2026-11-03 定稿，设计见 `15`）

缺口 1–8 收口后用户拍板的新工程。设计全部定稿（`15-session-per-prompt-design.md`），施工按批次：

| 批次 | 刀 | 内容 |
|---|---|---|
| 1 | G-a + G-b | 在线表三态（活跃 = running ∨ 最后事件 ts ≤ 15min，窗口进 onlineTableConfig）+ 四消费方对齐（注入/peers/presence/API）+ 默认接管（懒接管；Claim 冲突为唯一拒绝线）。**✅ 完成（2026-11-03）**：critic 3 轮收口（P0 并发绕过/截断次序/历史折叠/F1-F4）+ 变异自证 5 处；RunPrompt 顺手落"每轮折叠历史"（多轮同源）；挂账见 07 尾注 |
| 2 | G-c + G-d + G-f | per-prompt 配置体系（`config.changed` 契约增量 F7 全链 + 生效链 hub 默认→session fold→本轮快照）+ 模型面字段（model/effort/provider+凭据纪律）+ standby 待机工具（waiting 态 + message/timeout；X1/resume/WAL 三交互点） |
| 3 | G-e + G-g + G-j | config.tools 工具白名单修改（白名单语义）+ 权限面（config.policy；**开工前拍安全语义**）+ standby 的 `event` 等待条件（第二批等待条件） |
| 4 | G-h + G-i | 上下文面配置化 + profile 机制 / 执行面 |

验收硬门槛照 §2 阶段纪律（critic → reviewer → critic 循环至 clean + 变异自证 + 07 回填）。

## 5. 相关

`00` 调研总览 · `01` 权限与沙箱 · `03` provider SDK 实测 · `05` 语言定案 · `06` 骨架规格 · `07` 决策总表 · `08` ID 设计 · `09` 包边界 · `10` 审阅报告 · `spike/` 存储实测
