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
| **v2-S4 沙箱** | K8/H9 SandboxedExecutionEnv：Windows **job object + 受限 token + 目录 ACL** 务实组合；**可拓展埋点**（Linux/mac seatbelt/bwrap 后续挂）；V8 写保护由此收口 | 工具子进程纳入 job object（进程树清理）；目录外写被 ACL 拒；受限 token 下提权受限；换沙箱 env 不动 agent 逻辑（可插拔酸测试）；埋点接口留出 Linux/mac 挂点 |
| **v2-S5 存储接口收口** | G1/G2「一组可组合的存储接口」随图/blob 落地收口：事件日志/文档/blob/图接口分立、各自多实现可组合（blob/向量实现留 v4）；v1 挂账能清则清 | 接口分立且可组合（换实现零改动酸测试）；图/blob 接口签名与 G18-G21 实现对齐；07 收口注记 + 合计同步 |

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

## 5. 相关

`00` 调研总览 · `01` 权限与沙箱 · `03` provider SDK 实测 · `05` 语言定案 · `06` 骨架规格 · `07` 决策总表 · `08` ID 设计 · `09` 包边界 · `10` 审阅报告 · `spike/` 存储实测
