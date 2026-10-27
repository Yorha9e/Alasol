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
| **代码** | **阶段 0–3 已完成**（2026-10-27）：仓库/CI/契约 + core 骨架（69 测）+ runtime（SQLite 事件日志 + 索引器 + 回放，含并发/乱序/崩溃续跑测试）；阶段 3 退出评审循环 critic clean（3 p1 + 9 p2 闭环）；下一步：阶段 4 loop + 真机回路 |
| **设计** | 收敛完成：`07` 共 **279** 条决策（已定 81 / 待议 108 / 默认通过 90） |
| **文档** | `docs/research/` 12 篇 · `spike/sqlite-drivers/` 一份已跑出结果的实测 |
| **外部审阅** | 一轮已完成（`10`），7 阻断 / 16 应修 / 9 建议 **全部落档并闭环** |
| **命名** | 已定：`alasol` · `github.com/Yorha9e/alasol` · 命令 `alasol` · Apache-2.0 · 先私有后公开 |
| **施工流程** | 已评审并修订（durable 形状前置 / CI baseline 落位 / 阶段 4 审批三段 / env v0 子集 / 酸测试矩阵渐进） |

### handoff 链
```
ho_5c663a3207fc  施工交接（本 session → 施工 session）
ho_64354db0157c  施工回执 + 流程评审（施工 session → 本 session）
ho_b2d2a6ee000d  评审确认 + 四处修法已落档（本 session → 施工 session）
```
后续回函一律回显 `fromAgent`：`omkc:session_4682b412-6195-41cd-86f2-7220cd288878:main`

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
- **验收**：真实仓库里跑通一次工具调用；**凭据永不进 prompt / 事件 / 日志**；事件流含完整 intent→result 三段

### 阶段 5 · 多 agent（v0 切片 ②）
- `agent_type` 体系（含 `can_spawn`）/ spawn / **结构化 summary**（`refs` + `taint`）/ agent 树 / **父↔直系子可见**
- **验收**：
  - [ ] 主 agent 拆任务 → spawn 带 role 的 subagent → 收回 summary → refs 可核验
  - [ ] 深度超限**运行时拒绝**
  - [ ] tainted summary 被主 agent 按不可信数据处理
  - [ ] 异常向上冒泡（子 agent 失败/卡住 → 父的 summary 报告）

### 阶段 6 · 最小门禁（v0 切片 ③）
- 不透明命令（仓库级注入）/ **一次性快照 worktree** / **receipt 四要素校验** / **最小 CI baseline**（W9c）
- **baseline 的关键细节**：必须支持**「只记录不阻断」地建立基线**（第一次跑 → 记录已知失败指纹集 → 之后只拦**新增**失败）。否则红仓上第一次跑就死锁，基线永远建不起来
- **验收**：
  - [ ] receipt 的 commit ≠ 当前 tip → 拒绝通过
  - [ ] 未提交改动**结构性不进** CI
  - [ ] 命令不匹配当前配置 → 拒绝通过
  - [ ] **红仓上能建立基线**，之后只拦新增失败

### 阶段 7 · 客户端
- 极简 CLI（终端原生：对话 / 审批 / 在线表）+ Web UI（深度视图：上下文可视化 / 多 session 面板），走 WS+NDJSON
- **验收**：Web 能看到在线表与上下文构成（谁贡献了多少 token、什么被压过）；CLI 能对话与审批

### 阶段 8 · durable（X 区）—— **只做"机器"，写路径形状已在阶段 1/4 定**
- checkpoint（seq 水位 + ledger 版本 + in-flight + 树投影版本）/ resume / ownership（`spawn_request_id` 防重复 spawn）
- **验收**：kill -9 后能续跑且**不重复副作用**；`replay:"verify"` 的操作先核验外部状态再决定

### v1（不在 v0）
切片 ①（hub + 多 session 发现表 + gate）/ 多 worktree 隔离 / 完整 TUI / MCP server / 深度压缩 / 评测

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

---

## 4. 施工期的新决策去哪记

- **改设计** → 改 `07`（唯一裁决来源），并同步被引用的文档
- **只新增一条决策** → 加进 `07` 对应区，编号延续现有体系（A–X）
- **发现文档间矛盾** → **立刻在 `07` 记录并修一处**，不要留到下一轮审阅（上一轮 14 条矛盾里有 4 条是并行演进自己长出来的）
- **数字与清单** → **只在源头定义一次**，其余引用不重抄（D21 纪律）

---

## 5. 相关

`00` 调研总览 · `01` 权限与沙箱 · `03` provider SDK 实测 · `05` 语言定案 · `06` 骨架规格 · `07` 决策总表 · `08` ID 设计 · `09` 包边界 · `10` 审阅报告 · `spike/` 存储实测
