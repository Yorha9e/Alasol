# 设计决策总表（逐条过审用）

> 用途：把所有已设计项摊平成可逐条确认的清单。每条含：**决策项 / 我的主张 / 备选 / 状态**。
> 状态图例：✅ 已拍板（用户确认）· ★ **需要你定**（会分岔）· · 我的主张（默认通过，可直接否决）

---

## A. 目标与范围

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| A1 | 终局形态 | 独立产品 / 独立 harness，不并入 omkc | ✅ |
| A2 | omkc 资产复用 | 全部不复用（minidb / transcript / kosong / pi-tui 都不 fork） | ✅ |
| A3 | pi 的使用方式 | 不引用其代码；只读 `pi-durable` / `pi-chord` 设计文档当规格参考 | · |
| A4 | ACP | 不在 v0，后续按需再议 | ✅ |
| A5 | v0 目标 | 骨架成立（酸测试通过）**且**能连真模型跑通一个真 session（真机验证） | · |

## B. 技术栈

| ID | 决策项 | 我的主张 | 备选 | 状态 |
|---|---|---|---|---|
| B1 | 核心语言 | Go | — | ✅（独立产品 + Windows 一等 → 翻转） |
| B2 | TUI | Go + bubbletea / lipgloss。**选型落地（2026-10-28 v1-S5）**：`ui/tui` 独立包（O6）——对话/审批/在线表三面，进程内 hub 后端；依赖 bubbletea+lipgloss（手写输入框，未引 bubbles） | — | ✅ |
| B3 | GUI / IDE 插件 | 语言自由，通过协议接入 hub （2026-10-28）：v2/v3 做完后**外包给另一个 session 模拟第三方制作 UI** 的场景，验收协议面 | — | · |
| B4 | 解释型扩展运行时 | v1 再说；候选 Starlark / CEL / goja / WASM。v0 未做——v3（解释型扩展运行时选型，12 路线图）再定 **拍板（2026-10-28 用户）**：**继续保留候选、暂不选型**——后续有真实需求再定（保留位不废，E1 的 C 型=A 的特殊实现） | — | ✅ |
| B5 | 依赖约束 | 避免 cgo（纯 Go SQLite）；go modules | — | · |

## C. 架构分层与依赖

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| C1 | 中轴 | `contracts` 包，零依赖，纯数据契约 | · |
| C2 | core 依赖 | 只依赖 contracts；**不 import runtime**（接口由消费方定义） | · |
| C3 | runtime 职责 | 实现 core 定义的接口（store / process / sandbox / provider / transport） | · |
| C4 | host 职责 | 唯一组合根（装配 core + runtime） | · |
| C5 | ui 依赖 | **不 import core**，只消费 contracts（事件流 + 命令） | · |
| C6 | 顶层包数量 | 6 个：contracts / core / runtime / extensions / host / ui | · |

## D. 契约层

| ID | 决策项 | 我的主张 | 备选 | 状态 |
|---|---|---|---|---|
| D1 | 契约中立方式 | ✅ **手写 JSON Schema 优先 + codegen**；**只对跨边界契约中立**，清单以 **D21 唯一清单源**为准（当前含 Event / Command / Transport 消息 / Extension 元数据 / Tool.schema / **Tool 返回值** / Policy 规则 / Store 持久化格式 / Workflow 定义 / ContextSnapshot / Approval 消息）；进程内接口用原生 Go 类型 | 全盘 schema 化 / Go 类型优先反射导出 | ✅ |
| D2 | codegen 落地 | **自写受限子集生成器**（object/array/string/number/bool/enum/required/optional/nested/$ref 非循环；禁 $ref 循环与复杂组合）；**可选字段一律指针 + `json:",omitempty"`**；CI 做"再生成 diff 为空"校验 | 引入现成生成器 | ✅ |
| D2b | schema 起草 | 允许"反射生成草稿 → 人工审 → 提交为手写版本"两步法，之后以手写版为准 | 全手写 | · |
| D3 | 序列化格式 | JSON Schema + NDJSON | protobuf / MessagePack | · |
| D4 | 大 payload | `Event.binary_ref` 外置 blob | — | · |
| D5 | 事实源 | `Event` 唯一事实流 | — | · |
| D6 | 事件 kind | 开放字符串 + 命名空间点分（`tool.call.started`）；core 定义核心子集 | 封闭枚举 | · |
| D7 | 未知事件 | **必须宽容**：core 只对核心事件敏感，未知 kind 忽略但持久化 | — | · |
| D8 | 事件分类 | 单一流 + `durable: bool` 区分流事件/事实事件：`Event.Durable` 字段 + 总线 `bus.Publish` 按 durable 分流（false 直发订阅者、不入库不占 seq；true 整批入库成功后才通知）。定单一流而非两套通道，因为全局一条 seq 有序流才能支撑跨 session 统一时间轴与单游标回放（G3）（实现：core/bus + contracts/event.go） | 两套通道 | ✅ |
| D9 | Command | 意图；可从进程外提交 | — | · |
| D10 | Context | provider 无关的中间表示 | — | · |
| D11 | Model/Provider | 契约不含任何 SDK 类型 | — | · |
| D12 | Tool | ✅ `schema`、`side_effect`（none/read/write/destructive）、`cancelable`、`approvable`、**`idempotent`**、**`replay`**、**返回值 `graph_edges`**、执行器 | ✅ |
| D13 | Policy/Decision | `allow` / `deny` / `ask`；ask 携带 `suggested_rules` | — | · |
| D14 | Approver | **无人应答必须 deny，不得挂起** **落地（2026-01-30 v3-S4）**：protocolApprover（approval.request 广播 + approval.respond 应答；无人应答/超时=deny，应答与 ctx 竞态审计区分"已收到应答"与"无人应答"；应答无鉴权——任何能连上 hub 的客户端可批，收紧记 v4） **补注（2026-01-30 v3-S5）**：审批超时路径真机复验——超时 deny 后 intent+failed 成对留痕（此前零留痕断链）；文案区分"人审拒绝"与"超时/无人应答"（成因判据注释留长期修法：结构化成因码） | — | · |
| D15 | Store/Projection | 接口在 core，实现在 runtime | — | · |
| D16 | Session/Agent | 可插拔实体，稳定句柄 | — | · |
| D17 | Extension 元数据 | `kind` / `schema` / `capabilities` / `priority` / `reloadable` / `health` | — | · |
| D18 | Transport | 抽象统一：inproc / ws+ndjson / stdio | — | · |
| D19 | Telemetry | sink 契约（OTLP / 文件 / noop） | — | · |
| D20 | schema 产物 | 导出到 `contracts/schema/`，随契约版本发布 | — | · |
| D21 | **跨边界契约唯一清单源** | ✅ `contracts/` 维护**唯一清单**（含 Workflow、ContextSnapshot、Tool 返回值、Approval 消息）；其他文档一律**引用不重抄**，**计数由清单生成**（消除 7/8/9 三个数并存） | ✅ |
| D22 | **Tool 契约补齐 + 工具返回值契约** | ✅ D12 扩为 `schema / side_effect / cancelable / approvable / idempotent / replay / returns.graph_edges`；**工具返回值契约单独列入 contracts**（跨边界：MCP 与进程级扩展都要传） | ✅ |

## E. 扩展机制

| ID | 决策项 | 我的主张 | 备选 | 状态 |
|---|---|---|---|---|
| E1 | 扩展模型 | ✅ **A 接口实现为主 + B 进程级为边界**（C 解释型 = A 的特殊实现，不需独立机制）。A：Provider/Tool/Policy/Store/Projection/Compaction/ContextAssembler/Scheduler 等；B：MCP server / GUI 面板 / IDE 插件 / 第三方工具 / 不可信扩展 | 全 B / 全 A | ✅ |
| E2 | 分配 | A：Provider/Tool/Policy/Store/Projection/Compaction/ContextAssembler/Scheduler 等 12 个<br>B：MCP server / GUI 面板 / IDE 插件 / 第三方工具 / 不可信扩展。实现与提案有出入：A 类已落地 **tool / provider / policy / store / projection（索引器投影）**；**assembler / compaction / scheduler 未建**（contracts 里只出现在 ExtensionMeta.Kind 的开放取值清单，不是机制契约）；B 类（MCP server / GUI 面板 / IDE 插件 / 第三方不可信扩展）未建，待确认；**v2-S1 补账（2026-01-29）**：assembler / compaction 已建（core/compaction 两级压缩 + core/loop 装配点，J8/J9/J20 系落地），scheduler 仍未建 | — | ★ |
| E3 | 能力裁决 | 安装时申报能力，运行时由策略引擎裁决 | — | · |
| E4 | UI 无关 | 扩展点不假设有 UI | — | · |
| E5 | 失败隔离 | 扩展崩溃不得带崩 core | — | · |
| E6 | 热重载 | 进程级可热插拔；接口实现需重启（换 Provider/Store 通常伴随重启） | — | · |
| E7 | 官方扩展位置 | 顶层 `extensions/`，与第三方同一套注册机制，本身是示例 | — | · |
| E8 | **通用组件区** | 与"扩展点"并列的另一类：**框架自带、可被任何工作流 / 宿主复用的能力组件**，不绑定协议、不绑定工作流。已有三个：`agentlink`（K18 发现与交流）、`gate`/`ci`（W8 门禁）、`retry`（M6 重试）。落点定为 `core/` 子包且对外导出：`core/gate`（CI 门禁）已落地验证该形态；agentlink（K18，v1）、retry（M6，v3）未建，建成后同落 core/ 子包（实现：core/gate；agentlink/retry 待建） | — | ✅ |

## F. 事件与状态

| ID | 决策项 | 我的主张 | 备选 | 状态 |
|---|---|---|---|---|
| F1 | 一切皆事件 | 模型输出/工具调用/策略判定/压缩/审批 全是事件。**user.prompt/model.turn 亦入流**（2026-10-28 v1-S1 补缺口：对话内容进事件流，旁路观测不计 Result.Events；实现：core/loop/events.go + runtime/replay.go FoldConversation）。压缩随 v2 落地同受本条约束 | — | ✅ |
| F2 | 因果关系 | 扁平事件 + `Event.CausationID` 因果指针（completed/failed 指向同一次调用的 intent），**不引入 span 模型**。单 session 内提交序≈因果序（G25 session 亲和写），扁平指针足够追溯，span 树的额外复杂度在 v0 无对应收益（实现：contracts/event.go CausationID） | 完整 span 树 | ✅ |
| F3 | 事件字段 | `kind` / `seq` / `ts` / `session_id` / `agent_id` / `causation_id` / `payload` | — | · |
| F4 | loop 形态 | v0 控制流 + 完整事件记录：`core/loop` 是同步控制流状态机，但每轮落 `context.assembled`、每次工具调用落 intent/completed/failed 三段，事件流完整可回放；v1 再事件化（契约不变，纯增量）。先控制流是因为写路径形状（三段事件、X1 的"intent 先落盘"）比驱动模型更关键，控制流更易保证该不变式（实现：core/loop） | 直接事件驱动 | ✅ |
| F5 | 状态来源 | ✅ **唯一事实源**：一切状态由事件流 fold 得出，**不另设可变状态源**（含 ledger 与黑板，见 G23） | ✅ |
| F6 | 历史不可重写 | 压缩/修复只追加事件。**验证（2026-01-29 v2-S1）**：深度压缩只影响装配视图 + 追加 context.compacted 事件，历史事件不可变、refs 永不指摘要（实现：core/compaction） | — | ✅ |
| F7 | **契约演进铁律** | ✅ **只增不删、不改义**；弃用字段标 `x-deprecated`；事件 envelope 加版本字段 `v`。外部消费者据此判断兼容 | ✅ |
| F8 | **非 durable 流事件语义** | ✅ `durable=false` 的流事件（`model.delta` 等）**不入库、不占 seq**（总线直发订阅者）；**回放对象只有事实事件** **落地（2026-01-30 v3-S4）**：/api/events 回放只含 durable 事实事件（流事件丢了就是丢了），不做 K11 过滤（可见性读走 /api/inquiry） | ✅ |

## G. 存储

| ID | 决策项 | 我的主张 | 备选 | 状态 |
|---|---|---|---|---|
| G1 | Store 形态 | 事件日志（追加）+ 投影（读模型）；**不是一个引擎，是一组可组合的存储接口**。实现与提案有出入：v0 只有事件日志接口（`core.Store`：Append/ReadFrom）+ 索引器窄投影写面（`indexer.Writer`，不暴露裸 *sql.DB），"一组可组合的存储接口"（文档/blob/图/向量）未做，待确认 **收口拍板（2026-10-28）**：v2 随图/blob 接口落地收口 **先行落地（2026-01-29 v2-S2）**：图接口落 core/graph（Graph/GraphProjection/Tx）；可组合接口全集收口（文档/blob/向量扩展位）留 v2-S5 **收口（2026-01-29 v2-S5）**：core/storage 五族可组合接口（EventLog/Graph/Documents/Blob/Vectors + Set 组合面，写面带 ok 取法）；换实现零改动酸测试（五族双实现） | — | ✅ |
| G2 | 存储抽象层 | 按用途分若干接口，各自多个实现，可组合：事件日志 / 文档 / blob / 图 / 向量。实现与提案有出入：只落地"事件日志"一类（`core.Store` + sqlite 实现），文档/blob/图/向量接口未做（blob→v4、图→v2、向量→v4），待确认 同 G1：v2 随图/blob 落地收口（2026-10-28） **先行落地（2026-01-29 v2-S2）**：图类接口分立（core/graph，不泄漏 SQLite 特性）；收口留 v2-S5 **收口（2026-01-29 v2-S5）**：五族接口分立可组合（core/storage）；文档/blob/向量只有接口与 doc（实现留 v4）；Tx 形状未扩大（G2b） | — | ✅ |
| G2b | 引擎路线 | ✅ **定案**：`github.com/ncruces/go-sqlite3` v0.35.6 + `_txlock=immediate` + `busy_timeout`（实测并发写 1000/1000 零失败、5852 tps、跨进程已验证）；`modernc.org/sqlite` v1.61.0 为备选（高并发写 3% SQLITE_BUSY 失败、274 tps）。**后续要上正经数据库** → 接口必须不泄漏 SQLite 特性 | ✅ |
| G2c | 图引擎 | ✅ **SQLite 的 node/edge 表做持久化 + 自己写内存图做多跳**（规模会超万节点，长程工作必然碰到 → 现在就设计）。Kuzu（cgo + Windows 需 MSYS2/UCRT64 + 项目正在被归档）、Oxigraph（RDF/SPARQL 语义不对口）、Cozo（2023 停更）均排除。图是**可选投影**，换引擎不影响 core | — | ✅ |
| G2d | 向量引擎 | 未来 RAG，**独立于指针解析**（概率性，可选）。v0 未做——v4（RAG/向量检索，Retriever.retrieve 保留位接入）再定 **接口先行（2026-01-29 v2-S5）**：Retriever/VectorIndex 接口落 core/storage（实现留 v4） | — | ★ |
| G2e | blob 引擎 | 大工具输出、原始条目（`Event.binary_ref` 外置）；v0 文件系统 → 后期对象存储 / DB 大字段。v0 未做——v4（blob/对象存储）再定 **接口先行（2026-01-29 v2-S5）**：Blob/BlobMeta 接口落 core/storage，BlobMeta.Ref() 是 Event.binary_ref 唯一挂接点（实现留 v4） | — | ★ |
| G2f | 图不是硬依赖 | 图作为**可选投影**，由工作流声明是否启用；**不进 core 必经路径**（否则每个 session 都要起图库）。v0 未做——v2（图投影点亮，图作可选投影）再定 图=**可选投影**、不进 core 必经路径；随 v2 图投影点亮落地（2026-10-28 拍板） **落地（2026-01-29 v2-S2）**：默认关；未启用不建表不建引擎不解析载荷（零图开销酸测试钉住） | — | ✅ |
| G8 | goal ledger 落地 | ✅ **事件化**：`ledger.updated` 作为 durable 事件追加，**当前态 = 索引器物化的投影**（不再 CAS 直写可变存储）；**在上下文之外** | ✅ |
| G9 | 多写者 | ✅ **Kafka 模式：多写者 append + 单索引写者**。写者只管 append（幂等 `id` UNIQUE；`seq` 由 SQLite AUTOINCREMENT 原子分配 = 全局单调且无需应用层仲裁；事务保证一批 append 原子；`_txlock=immediate` + `busy_timeout` 处理冲突，实测跨进程 1000/1000）。**session 目录 / 图投影等派生索引由单独的索引器消费事件流维护**（最终一致，毫秒级延迟，UI 可接受） | — | ✅ |
| G10 | seq 语义 | `seq` 由 SQLite AUTOINCREMENT 全局单调分配（原子、无需应用层仲裁），per-session 单调自然成立；`(session_id, seq)` 索引供 UI 取本 session 视图。seq 全局单调但不保证稠密（冲突序号不回收），消费端不得用 max(seq) 当事件数（实现：runtime/store/sqlite） | — | ✅ |
| G11 | 事件幂等 | `Append` 以 `Event.ID` 幂等去重（`ON CONFLICT(id) DO NOTHING`，重复 ID 返回库中原行含原 seq）→ 崩溃重试不产生重复事件。去重只看 Event.ID，绝不看 payload 里的任何 id（S7）（实现：runtime/store/sqlite） | — | ✅ |
| G12 | 上下文快照存储 | `ContextSnapshot` 作为 durable 事件 `context.assembled` 在每轮模型调用前落进 EventLog（落库失败不中止 loop，只补 loop.error）。"当前"态经事件流读取（UI /ctx 面板读最近一条）；v0 未设专用投影表，与"投影只读"不悖（实现：core/loop/context.go） | — | ✅ |
| G13 | 事务模式 | **必须 `_txlock=immediate`** —— 实测并发写失败率从 1 次降到 0 次（ncruces）；默认 deferred 模式下并发写更差 | — | ✅ |
| G14 | 实测结论（2026-10-26） | 两驱动 WAL 均生效（网上"modernc 不支持 WAL"的说法在 v1.61.0 不成立）；`TIMESTAMP` 列往返均为 `time.Time`（跨驱动一致）；跨进程写均正常（ncruces 的纯 Go VFS 文件锁是对的）；`seq INTEGER PRIMARY KEY AUTOINCREMENT` + `id TEXT UNIQUE` 表结构在两驱动上均工作 | — | ✅ |
| G15 | 单写者仍是主防线 | 并发测试是 50 goroutine 抢写的极端情况；正常只有一个 writer。busy_timeout + txlock 是兜底不是主策略 | — | ✅ |
| G16 | 排除的候选 | `tursodatabase/limbo`（Go module 内 0 个 `.go` 文件，Go 侧无 API）；`surrealdb.go`（cgo + Rust 工具链，嵌入式不"轻"）；`embedded-postgres`（多一个 PG 进程，与"零外部进程"硬约束冲突）；`kuzudb/kuzu`（cgo + Windows 需 MSYS2/UCRT64 + 项目正在被归档）；`oxigraph`（RDF/SPARQL 语义不对口）；`cozodb/cozo`（2023 停更） | — | ✅ |
| G17 | **索引器角色** | 独立角色：`runtime/indexer` 消费全局事件流、增量维护**持久投影**（sessions 目录 / ledger 当前态 / 黑板当前态 / 成本统计），是**唯一写持久派生表的人**——经窄 SQL 面（Writer，不暴露裸 *sql.DB）写、读也走索引器，投影+水位同事务原子。图投影/快照当前态等留待对应机制落地（v2）（实现：runtime/indexer） **落地（2026-01-29 v2-S2）**：图投影落地——WithGraphProjection 可选写面（不传=零开销） | — | ✅ |
| G17b | 持久 vs 活状态 | **持久投影**（落库）：session 目录、图、成本、ledger、快照当前态。**活状态**（只在 core 内存，进程结束自行消失）：presence / 在线状态 / gate 成员。**不需要租约、不需要心跳、不需要"离线 session 保留"**——session 生命周期 = 进程生命周期 | — | ✅ |
| G18 | 图查询分层 | **一跳/两跳走 SQLite 索引**（`edges(src,kind)` / `edges(dst,kind)` 双向倒排）快查；**多跳走内存图 BFS/DFS**（邻接表 + visited set + 深度上限 + 结果上限 + context 取消）。传递闭包在内存图上是 O(V+E)，SQL 递归 CTE 在大图上不可用。v0 未做——v2（图投影）再定 **照主张落地（v2，2026-10-28 拍板）**：一跳/两跳走 SQLite 双向倒排、多跳内存 BFS（深度/结果上限+context 取消） **落地（2026-01-29 v2-S2）**：core/graph MemGraph 多源 BFS（全局结果上限+深度上限+ctx 取消）+ runtime/graph 双向倒排 SQL（恰好 N 跳语义已文档化）；(seq,rowid) 二元组水位防同事件大批次丢边 | — | ✅ |
| G19 | 图内存预算 | node id 用**整数或 interned string**（不用原始长字符串）→ 10 万节点 / 100 万边的邻接表控制在 ~100–200MB 量级；邻接表常驻或 LRU，冷启动从 SQLite 流式加载。v0 未做——v2（图投影）再定 照主张（2026-10-28）：node id interned；10 万节点/100 万边邻接表 ~100–200MB 量级，可配 **落地（2026-01-29 v2-S2）**：整数 interned id + 双向邻接表常驻（取舍：escalate 依据确定性>缓存命中率）+ 冷启动流式加载 + idx_graph_edges_seq（20 万边冷启动 ~0.4s） | — | ✅ |
| G20 | 闭包缓存 | 同一查询反复问很常见（"这次改动的下游影响面"）→ 按 `(起点, kind, 深度)` 缓存闭包结果。v0 未做——v2（图投影）再定 **先做简版**（内存 map + 图写入失效，不做复杂淘汰；2026-10-28 用户拍板） **落地（2026-01-29 v2-S2）**：ClosureCache 内存 map + epoch/applyGen 双护栏写入失效（并发陈旧窗口已封） | — | ✅ |
| G21 | 图的用途链 | 图不是"未来可能用"，而是 **W16 `change_impact` 的底层实现**：依赖图可达性查询 → 改动影响面 → agent 判断"是否大范围架构改动"的客观依据 → escalate 闸门。**三者是一条链，不是三件事**。v0 未做——v2（图投影 → change_impact）再定 用途链照主张：change_impact → escalate 客观依据（W16）；v2 实现（2026-10-28） **落地（2026-01-29 v2-S2）**：ImpactReport（Direct/Transitive/CoreTouched/Diff/Scale/Escalate/Reasons/Direction）+ GET /api/graph/impact；escalate 消费端接线留 v3（接线前必补重建窗口口径——N2 挂账） **已清（2026-01-29 v3-S3）**：N2 重建窗口防护（graph_meta.rebuilding 标记+启动期自动恢复+ErrRebuilding fail-close）+ mergegate 消费端就位 | — | ✅ |
| G23 | **状态源收口** | ✅ **ledger 与黑板一律事件化**：`ledger.updated` / `board.written` 作为 durable 事件追加（**事件版本号 = 全局 seq，天然实现 CAS 语义**）；**当前态 = 索引器物化的投影**。消除 F5 / G8 / K22 / 06§7.1 的四方矛盾，保住「事件流是唯一事实源」 | ✅ |
| G24 | **索引器恢复语义** | 四项全落地：① 水位持久化在 `indexer_watermark` 单行表，崩溃后从 N 续跑；② `Rebuild` = 清空派生表 + 从 0 重放；③ `Watermark()` 可查（UI 显示「索引已追至 seq N」，最终一致可见）；④ 单索引器实例内部串行、单写者纪律（多索引器指向同一库不支持），归属随 hub 进程。投影+水位同事务原子，崩溃整体回滚（实现：runtime/indexer） | ✅ |
| G25 | **session 亲和写** | 多写者跨进程时**全局 seq = 提交序 ≠ 因果序** → 规定**一个 session 的事件只由其宿主进程 append**（session 亲和写）→ 单 session 内提交序≈因果序，`causation_id` + seq 足够，**不需要 span 模型**。v0 单进程宿主天然满足；作为部署层约束文档化在 core/store.go 与 bus.go（实现：约束文档化于 core/bus、core/store） **注记（2026-01-30 v3-S4）**：协议 prompt 端点加托管门（不托管 404）防跨进程双写；重启即失管（活状态进程本地）；跨进程托管注册记 v4 | ✅ |
| G22 | 图的边从哪来 | ✅ **A. 工具产出**：工具返回里带 `graph_edges: [{src, dst, kind}]`，事件落盘时索引器据此建边。不用"按 tool kind 猜"（太脆），也不用"让 agent 自己维护图结构"（让模型出错） **落地（2026-01-29 v2-S2）**：索引器消费 tool.call.completed 的 ToolResult.GraphEdges 建边（与水位同事务，G24） | — | ✅ |
| G3 | 流划分 | ✅ **全局一条事件流 + session_id 区分**。理由：单一 seq 全局有序 → UI 订阅一条流按 session_id 过滤即可；跨 session 的统一时间轴（主 agent 与多个 subagent 事件按真实顺序交错）只有全局流才有；回放一个游标走完；全局成本/预算天然可聚合。代价是单 session 检索要靠 `(session_id, seq)` 索引（SQLite 一条索引即可） | — | ✅ |
| G4 | 并发写 | ⚠️ **作废，被 G9 取代**（多写者 append + 单索引写者） | — | ✅ |
| G5 | 长历史 | 分段 + 偏移索引 + 惰性加载 | 全文重放 | · |
| G6 | 压缩产物 | 结构化摘要 **+ 原文区间指针**（可回捞） | 纯摘要（不可逆） | · |
| G7 | 投影 | 只读，不参与写路径 | — | · |

## H. 权限与沙箱

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| H1 | 三层模型 | 决策层（prompt）/ 策略层（规则+人审）/ 强制层（OS 沙箱） | · |
| H2 | 求值链 | 有序链，首个命中即返回，顺序 deny→ask→allow | · |
| H3 | deny 语义 | deny 优先且**不能写 allow 例外** | · |
| H4 | 两种 deny | 裸工具名 deny = 移出模型可见集；参数化 deny = 拦截调用 | · |
| H5 | 规则来源 | ✅ **deny / ask 跨所有来源取并集**（任一来源命中即 deny/ask，不看优先级）；**只有 allow 才按来源优先级解析**：managed > CLI > project > **workflow** > local > user > session。**workflow 规则只允许收窄、不允许放宽**（与 H11 同构）——workflow 是用户/agent 可写数据，若能放宽就是一条未审计的注入通道 | ✅ |
| H6 | 权限模式 | read-only / workspace-write / danger-full-access 类预设 | · |
| H7 | 无人值守 | 审批缺席即拒绝 **+ 注入拒绝话术**（照 omkc auto 模式） | · |
| H8 | 缺席默认 | 审批服务不可用 → **deny**（不是 approve） **落地（2026-01-30 v3-S4）**：审批器缺席/ctx 结束一律 deny（fail-close），protocol 模式同口径 | · |
| H9 | 沙箱时机 | v0 不做；v1 起 seatbelt / bwrap / Win32 原生 / 容器 **拍板（2026-10-28 用户）**：Windows 无原生沙箱可行面——现在走 **job object + 受限 token + 目录 ACL** 务实组合，**可拓展埋点**留 Linux/mac 沙箱（seatbelt/bwrap 后续挂）；V8 写保护由此收口。实现 v2 **落地（2026-01-29 v2-S4）**：runtime/cap/sandbox——job object（KILL_ON_JOB_CLOSE+ACTIVE_PROCESS 上限）+ 受限 token（组 SID 全量收紧 deny-only + 13 特权删除）+ 目录 ACL（追加式、授权收窄 M）+ WER 压制 + Backend 埋点接口（Linux/mac 挂点桩）；已知边界三条：默认模式 BUILTIN\Users 授权位置（如 C:\ProgramData）仍可写（-sandbox-strict-groups 可关，代价=overlapped I/O 全挂）、读边界只挡用户私有文件、允许根 DACL 追加传播不还原 | ✅ |
| H10 | 网络 | 默认拒绝 + 域名白名单 | · |
| H11 | 子 agent 权限 | 只能收窄，deny 强制继承（omkc 的"只继承不收窄"是反例）；**工作流声明的 scope 是"请求"，实际 = 请求 ∩ 策略上限 ∩ 父级授予** | · |
| H12 | 凭据 | 按作用域注入 + 日志脱敏 | · |
| H13 | 不可信输入 | 工具输出/网页标注来源，视为数据非指令 | · |
| H14 | env 分层 | 四层分离：**凭据 env**（进 provider）/ **执行 env**（进工具进程）/ **上下文 env**（给模型看，默认不给）/ **作用域覆盖**。v0 未做——v1（env 全分层）再定；v0 只做执行 env（H16/H17/H18） → **挪 v2**（2026-10-28 重新标注：原注 v1 未做） | ★ |
| H15 | 凭据来源 | ✅ **拍板（2026-10-28 用户）**：进程 env + **TOML 配置文件**（Windows `%APPDATA%\alasol\config.toml`，其他平台 XDG），**优先级：进程 env > 配置文件**；永不进 prompt/事件/日志（H18/H21 纪律不变）。**落地（2026-10-28 v1-S5）**：`host/config_file.go`（BurntSushi/toml，零传递依赖）+ `Precedence(flag,env,file)` 唯一优先级实现处；`FileConfig.Redacted()` 打码、解析错误不回显值；实测 secret 不进事件/HTTP/日志 | ✅ |
| H16 | 执行 env 默认 | 工具默认继承**净化后**的父 env（凭据类剥离）；普通 env（PATH/HOME 等）保留。落地：`core.ScrubEnv` 剥离凭据类键（`IsCredentialKey`）与凭据形态值（`IsCredentialValue`：sk-/ghp_/gho_/JWT），普通 env 保留；显式注入的 required_env 在 ScrubEnv 之后叠加（获批凭据不被剥）（实现：core/env.go + core/loop/env.go） | ✅ |
| H17 | 凭据按需注入 | 工具显式声明 `required_env`，由策略引擎裁决后注入该次调用。落地：工具 `RequiredEnv` 声明的键每个当能力过同一套策略求值（`Query{Tool:"env:"+key}`，E3），allow 才从进程 env 取值注入；deny/ask 未放行 → 不注入（fail-close），注入值进 secrets 供输出脱敏（实现：core/loop/env.go） | ✅ |
| H18 | 脱敏规则 | deny 名单（`*_KEY`/`*_TOKEN`/`*_SECRET`/`*_PASSWORD`）+ 值形态匹配（`sk-`/`ghp_`/JWT）。落地：deny 名单（`*_KEY`/`*_TOKEN`/`*_SECRET`/`*_PASSWORD`/`*_CREDENTIAL*`，`IsCredentialKey`）+ 值形态（`sk-`/`ghp_`/`gho_`/JWT，`IsCredentialValue`）；框架注入的秘密值在工具输出进事件/下一轮 prompt 前被 `redactResult`/`redactSecrets` 逐个精确替换成 `***`（零误伤，非法 JSON 退化为 `"***"`）（实现：core/env.go + core/loop/toolcall.go） | ✅ |
| H19 | 事件记录 | 只记录 env **键名**，不记值 | · |
| H20 | 注入作用域 | global / session / agent / task / 单次调用。v0 未做——v1（global/agent/task 作用域）再定；v0 只做 session + 单次调用两级（H21） → **挪 v2**（2026-10-28 重新标注） | ★ |
| H21 | env 注入 v0 子集 | ✅ **v0 只做两级作用域（session + 单次调用）+ 不给模型看任何 env**。凭据来源：进程 env + 用户级配置文件，**永不进 prompt / 事件 / 日志**；执行 env 默认继承**净化后**的父 env（凭据类剥离），工具声明 `required_env` → 策略裁决后注入；上下文 env **v0 完全不给模型**（连 allow 名单都不做，最小化）；脱敏 deny 名单 `*_KEY` `*_TOKEN` `*_SECRET` `*_PASSWORD` `*_CREDENTIAL*` + 值形态 `sk-` `ghp_` `gho_` + JWT；事件只记 env **键名**。global / agent / task 级作用域与上下文 env 的 allow 名单留 v1 | ✅ |

| H22 | 权限语义（工作流侧） | `permission_request` / `grants` 是**请求**；实际 = 请求 ∩ 策略上限 ∩ 父级授予。v0 未做——v3（workflow 权限语义）再定；v0 已有等价的 agent 树权限收窄（K17 闸门③ = 请求 ∩ ceiling ∩ 父授予，core/agents/perm.go） | ★ |
| H23 | 命中即放行（工作流侧） | 落在已授予 scope 内的调用直接放行，不走逐次 ask；scope 外走完整策略链。v0 未做——v3（workflow 命中即放行）再定 | ★ |
| H24 | **workflow 规则进权限链** | ✅ 见 H5：workflow 的 `gates[].kind:"policy"` 规则**只允许收窄**；deny/ask 跨来源取并集。**workflow 是用户/agent 可写数据，能放宽就是未审计的注入通道** | ✅ |

## I. Provider 与模型

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| I1 | provider 契约 | 自研，不借 pi-ai（kosong 独立佐证可自研） | · |
| I2 | Go SDK | anthropic/v2、openai/v3、genai（已实测活跃） | ✅ |
| I3 | 统一事件 | 12 类（text/thinking/toolcall 各 start-delta-end）+ done/error | · |
| I4 | 跨 provider 上下文交接 | **必做**：行业真空（kosong 静默丢弃、pi 不完整） **改口降级（2026-10-28 用户）**：不立专项——压缩产物（13 号 L1/L2）是 provider 中立表达，换模型人工/半自动重喂即可；原“必做”降为“随压缩自然可得” | ✅ |
| I5 | 模型目录 | 内置静态快照 + 在线拉取（models.dev 形态）+ 周期刷新。**落地（2026-10-28 v1-S5）**：`runtime/models`（BuiltinSnapshot + FetchFunc + Catalog.Run 周期刷新 + 内置∪在线合并；刷新失败保留旧值）；`alasol models` 默认在线刷新失败回落内置，`-offline` 只用内置 | ✅ |
| I6 | 模型绑定 | per-agent 持久化，subagent/fork 继承 | · |
| I7 | 路由 | 作为扩展点（RoutingPolicy） | · |
| I8 | 成本 | token/成本核算内置 | · |
| I9 | **模型名解析** | ✅ 模型名是**端点相关的**，**不做框架级默认值**（写死默认=换网关就废请求）：`-model` 优先、`ALASOL_MODEL` 兜底、双空明确报错（fail-close）；发现手段 = `alasol models` 在线查 `GET {base}/models`（I5 的 v0 最小形态，内置模型目录留 v1） | ✅ |

## J. 上下文与压缩

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| J1 | 装配 | ContextAssembler 是扩展点，core 不内置具体来源 | · |
| J2 | 项目规则 | 按需注入 + 运行中变更提醒（照 agentsMdReminder 形态） | · |
| J3 | git 状态 | **不进 prompt**，工具按需查 | · |
| J4 | 压缩策略 | CompactionStrategy 是扩展点 | · |
| J5 | 压缩触发 | ✅ 与 J13 合并为**单一触发口径**（见 J20）：阈值 / 保留 / 后台三水位**统一为一套参数**，不再并存两套模型 | ✅ |
| J6 | 可回溯 | 压缩后原文仍可按指针回捞 | · |
| J7 | 溢出处理 | 自动压缩并重试一次 | · |
| J8 | 上下文策略枚举 | `append`（默认）/ `stage_scoped`（阶段独立 + 交接摘要）/ `sliding_window`（热区滑动）/ **`tiered`**（分级压缩）；**用户可再写自己的** **落地（2026-01-29 v2-S1）**：context_strategy 预备单位（compaction.AssemblerFactory）——append 兜底 / tiered 已落，用户可写自己的（换策略 core 零改动，酸测试钉住）；stage_scoped / sliding_window 枚举保留未实现 | ✅ |
| J9 | 压缩等级定义 | **压缩等级 = 目标占用上下文窗口的百分比档位**（非离散编号）；最低档位如 0.1% → 500 个阶段占 50%。v0 未做——v2（深度压缩）再定 **拍板（2026-10-28 用户）**：两级锚定——L1（浅层摘要）目标 ≤**10%** 窗口、深度压缩 L2 ≤**1%**（L2=summary+一根指针，天然极小）；数字为**可配初值**，真实负载数据回头校准。实现 v2 **落地（2026-01-29 v2-S1）**：L1≤10%/L2≤1% 可配初值进 compaction.Params（TOML [context_strategy] l1_pct/l2_pct + CLI flag），引擎硬校验（不守预算的摘要器也守阈值，截断后留 recall 指针说明） | ✅ |
| J10 | 多阶段权衡 | 阶段数多时（500 个），每个阶段的摘要只能是"对当前阶段的简要概括"；深度压缩单阶段时才有更细摘要。v0 未做——v2（深度压缩）再定 由两级设计吸收（13 号：L1=阶段简要概括、L2=深度压缩结论+指针；2026-10-28 拍板） **落地（2026-01-29 v2-S1）**：L1=阶段简要概括（含具体工程操作）+ 指针、L2=结论+一根指针；ref 带稳定锚 stage:N@e:from..e:to（跨 Run 稳定） | ✅ |
| J11 | tier 标签作用 | **给机械化流程的定位标记**：让框架识别出"LLM 要求压缩的范围"→ 取出该范围 → 交 LLM 压缩 → 取回摘要放回上下文 **落地（2026-01-29 v2-S1）**：软边界=stage{label} 工具（显式 tier 标记）；硬边界 Engine.MarkValidationPassed()（验证通过点）API 已落，生产事件驱动接线留 v2-S3 门禁增强（gate receipt） **接线（2026-01-29 v3-S3）**：ValidationCursor 会话级游标（进程内 Run 间不丢）驱动 gate.checked/workflow.milestone → MarkValidationPassed；游标不跨进程（重启不补陈旧信号，如实注记） | ✅ |
| J12 | v0 策略 | **只预留接口**，每阶段先做**最浅层压缩**；不做深度压缩、不做级联 | ✅ |
| J13 | 触发点 | 占用到 **50%** 时压缩（实测：30~50% 区间 LLM 能力损失较小）；**数值用户可调** **落地口径收窄（2026-01-29 v2-S1，13 §3）**：主触发=阶段关节（验证通过点），水位只当兜底护栏（预算驱动降级）；"50% 触发"数值口径被 13 §3/§4 吸收，参数仍可配 | ✅ |
| J14 | 原文与 trace | 原文永远在持久存储；摘要记录覆盖范围 → **全过程可作完整 trace** | ✅ |
| J15 | 检索扩展点 | **指针解析 = core 的确定性基础能力**（`Resolve(ref)`，不是扩展点）；**RAG = 独立的概率性扩展点**（`Retriever.retrieve(query)`），通过 `Assembler` 注入 **落地（2026-01-29 v2-S1）**：Engine.Resolve(ref) + recall(ref) 工具（走 X1 三段写路径）；ref 形态 stage:N@e:from..e:to / e:from..e:to / stage:N（本 Run 别名）；RAG Retriever 独立扩展点仍未实现 | ✅ |
| J16 | 解决的根因 | 长时程任务必超窗口；多次压缩破坏摘要 → **目标与约束偏移**（当前 goal 交付率偏低的根因） | ✅ |
| J17 | **级联问题（未解）** | 深度压缩阶段 A 需"总体局势"+A 全文；若总体局势来自其他阶段摘要，压缩 A 又改变 A 摘要 → 级联失效。v0 未做——v2（深度压缩：级联问题）再定 **方案落定（2026-10-28）**：13 号两级压缩 + goal ledger C 案切断级联链，v2 实现 **落地（2026-01-29 v2-S1）**：压缩输入=goal ledger+被压区间全文、永不读他阶段摘要（测试钉住）；账本写入侧未接线见 J19 挂账（空账本降级运行） | ✅ |
| J18 | 级联候选解 | A 域隔离（限深 1 层）/ B 结论不可变 / **C 独立全局状态 goal ledger（切断级联链）** / D 级联预算。v0 未做——v2（深度压缩：级联候选解）再定；C 案的 goal ledger 已在 v0 事件化落地（J19/X6）备用 **定案 C 案**（goal ledger 切断级联链，J19/X6 基础已备；2026-10-28 拍板） **落地（2026-01-29 v2-S1）**：级联输入走 ledger 源接线（host 索引器投影） | ✅ |
| J19 | goal ledger 提案 | **目标、约束、已完成项、待解项**放在**上下文之外的独立持久存储**，由 agent 在关键点显式更新。压缩任一阶段只读账本 + 目标阶段全文 → 不需要级联；同时根治"目标偏移"。落地：`ledger.updated` durable 事件（全量快照），当前态 = 索引器 `ledger_state` 投影（LWW，seq 守卫），在上下文之外，X6 控制写放大（实现：core/durable/ledger.go + runtime/indexer） **挂账（2026-01-29 v2-S1 发现）**：durable.LedgerWriter 全仓无调用方——账本恒空，级联 C 案以"只有被压区间全文"降级运行（doc 如实注明）；账本生产者接线挂后续 **已清（2026-01-29 v3-S3）**：loop 级 ledger 框架工具接 durable.LoadLedger+Seed（折最后一条 ledger.updated 灌基线，跨 Run 关键字段保住——X6 语义完整）；级联 C 案输入恢复"账本+全文" | ✅ |
| J20 | **压缩触发计入 prompt cache 成本** | 压缩重写历史 = **前缀缓存全失效** → 50% 触发可能比「扛到溢出再压」更贵。**缓解：压缩时保留最前面的稳定段不动**（system / 项目规则 / ledger 留在前缀里），只压中段 → 缓存前缀存活。压缩触发的输入要含「缓存失效成本」；**Q1「编排最后做」改为「与压缩策略联动设计」**。v0 未做——v2（深度压缩）再定 按 13 号缓存定律落地（易变贴生长前沿、压缩不动稳定前缀）；“缓存失效成本进触发输入”的量化留实现时校准（可配）（2026-10-28） **落地（2026-01-29 v2-S1）**：缓存定律落地——稳定前缀（rules+ledger 稳定半）字节不变断言钉住、ledger 拆两半（易变半贴生长前沿）、压缩不动稳定前缀 | ✅ |
| J21 | **tiered% 锚定 + token 计量口径** | ✅ **计量口径拍板（2026-10-28 用户）**：`context.assembled` 是**调用前快照 = 正式估算口径**（len/4 自算）；**provider usage 只进成本统计、不回填快照**（原提案"usage 优先"作废——快照生成时 usage 尚不存在）。tiered% 锚定仍留 v2（随深度压缩）。**落点（2026-10-28 v1-S1）**：usage 落 `model.turn` 事件 `ModelTurnUsage`，只进成本统计（实现：core/loop/events.go）。**成本统计落地（2026-10-28 v1-S5）**：`runtime/indexer` 的 `cost_tokens` 派生表 + `GET /api/cost`（总额）/`/api/cost/{id}`（per-session 按模型分组）+ chat `/cost`；`ModelTurnPayload` 增可选 `model` 字段（按模型分组）；实测 usage 未回填快照 **锚定落地（2026-01-29 v2-S1）**：tiered% 落定 L1≤10%/L2≤1%（J9）；压缩调用 usage 计入成本（随 context.compacted 留痕进 cost_tokens，不走 model.turn——防污染 resume 对话与 recall 回捞） | ✅ |

## K. 进程与多 session

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| K1 | session 隔离粒度 | ✅ **默认 hub 内 goroutine**（逻辑隔离：独立 context / 工具集 / 权限视图；panic 可在 goroutine 边界 recover；独立取消；成本近零）。**不默认独立进程**——subagent 是自 spawn 的自家代码；OS 级隔离是「运行任意第三方代码」的事，属于**工具 / ExecutionEnv**（K8）。**05 §4.2 的「进程默认」建议已被本条取代**（v0 无沙箱时进程隔离也拦不住写盘，「安全默认」名不副实） | ✅ |
| K2 | 无全局单例 | core 不持有全局状态，状态显式传递 | · |
| K3 | 外部命令 | Command 可从进程外提交 | · |
| K4 | 远程订阅 | 事件流可远程订阅，按 session 过滤 | · |
| K5 | hub 节奏 | v0 不做 hub，但契约按多进程设计 | · |
| K6 | 崩溃域 | 单 session 崩溃不影响其他 session，也不拖垮 hub | · |
| K7 | 并发控制 | 资源冲突调度（非裸并发）+ 配额 + provider 限流指数退避 | · |
| K8 | **ExecutionEnv 抽象** | agent 的工具执行环境可插拔：`LocalExecutionEnv`（hub 进程内，v0）→ `SandboxedExecutionEnv`（子进程 + 沙箱，v1）→ `RemoteExecutionEnv`（远端 SSH，v2）。**升级到进程隔离只需换 env 实现，不动 agent 逻辑**。v0 未做 SandboxedExecutionEnv/RemoteExecutionEnv——v2（OS 沙箱）/ v3（远端 SSH）再定（12 路线图）；v0 已有可插拔能力抽象（core Exec/Fs/Net/Proc）+ LocalExecutionEnv（runtime/cap/local）作底座 **拍板（2026-10-28）**：SandboxedExecutionEnv v2 落地——Windows **job object + 受限 token + 目录 ACL** 务实组合 + **可拓展埋点**（Linux/mac 沙箱 seatbelt/bwrap 后续挂）；RemoteExecutionEnv 留后续 **落地（2026-01-29 v2-S4）**：SandboxedExecutionEnv 落 runtime/cap/sandbox（与 local 并列；K8 酸测试钉住：换 env 不动 agent 逻辑）；RemoteExecutionEnv 仍留后续 | ✅ |
| K9 | goroutine 隔离的代价（必须对冲） | OS 级崩溃（OOM / 栈溢出 / cgo 崩溃）会带走整个 hub；死循环 goroutine 抢占其他；内存泄漏拖垮 hub。对冲：turn 超时 + 工具超时 + hub 级 CPU/内存监控告警 + 单 agent panic recover 后记事件并标记失败（不影响其他 agent）。**落地（2026-10-28 v1-S5）**：panic 隔离（`executeSafely` recover）+ MaxTurns 截断 + hub 级 CPU/内存/goroutine 三档监控告警（`host/monitor.go`：SampleSource 可注入、周期采样、阈值 flag>env>文件>默认、有界告警环 + `/api/monitor`；告警走 slog+内存环，**未入事件流**——刻意不动契约面，入流/审计留后续） | ✅ |
| K10 | **发现表（Registry）** | core 的**内存活状态**组件，不是扩展点。属于**容器管理 + subagent 管理**的一部分，不是协议层。事件驱动更新（`agent.started` / `policy.asked` / `workflow.stage.changed` / `agent.finished` → 改内存）。**落地（2026-10-28 v1-S2）**：落 **`core/discovery`**（命名决策：原名"Registry"与扩展注册表 `core/registry`（E3/E5/E6）撞名 → 改 `core/discovery`）；双层语义=事件折叠层（可重建，Apply 幂等）+ 进程本地 live 层（G17b 两分法）；数据流入=**拉式扫描**（跟在线表同口径，防重复计数）；崩溃重启从事件流全量重放重建（真库测试）；来源事件 agent 生命周期已有，`policy.asked`/`workflow.stage.changed` 待 v3 对应 kind 落地后加 case（实现：core/discovery + host/discovery.go） | ✅ |
| K11 | **可见性 = 两个正交维度** | ✅ **口径定死**：**① 水平（gate）**：只有 **session 的主 agent** 之间互相可见（`gates(A) ∩ gates(B) ≠ ∅`）。**② 垂直（agent 树）**：**父 ↔ 直系子 可见**（每个 agent 可见自己的父 agent + 自己的全部直系子 agent）；**兄弟 subagent 互相不可见**；别 session 的 subagent 完全不可见。**跨层上查 = `Inquiry` 沿父链逐级显式调用**（每跳一次是一次调用），不是「全祖先自动可见」。孤儿防护靠「父可见子 + 异常向上冒泡（K23）」闭环。**落地（2026-10-28 v1-S2）**：水平维 = `discovery.VisiblePeers`（对端主 agent ∩ gates 相交 ∩ 存活），别 session subagent 不泄漏（负例测试） | ✅ |
| K12 | 自动 / 显式 Gate | session 创建时自动加入：`repo:<repo_id>`、`workflow:<run_id>`；显式 `join_gate(gate_id)`。Gate **定义持久化**、成员关系是活的（session 结束即失效）。**落地（2026-10-28 v1-S2）**：`gate.joined` durable 事件（`gate_id` + `source` 枚举 auto_repo/auto_workflow/explicit；**无 gate.left**——离开=session 结束，join 历史只增可审计）；**成员"活"语义** = 折叠 join 历史 ∩ 存活 session；自动 `repo:<repo_id>`（复用 identity 的 worktree 聚合）/`workflow:<run_id>`（留钩子，工作流 v3）+ 显式 `JoinGate`；幂等 = 确定性 Event.ID（(session,gate,source) 导出 26 字符 Crockford）+ store ON CONFLICT 去重 + host per-session 锁（实现：core/discovery + host/discovery.go） | ✅ |
| K13 | **role = 部门标签** | role 近似"部门"（backend / frontend / infra）而非"角色扮演"，**由父 agent 在 spawn 时指定**。与 `agent_type` 正交：`agent_type` = 能力模板（能干什么），`role` = 部门（负责哪块）。role 自由文本、可重复。落地：`agents.Node.Role` 字段 + `agent.spawned` 事件带 `role`，由父在 `Tree.Spawn(..., role, ...)` 指定，与 agent_type 正交（实现：core/agents/tree.go） | ✅ |
| K14 | **暴露等级 = 注入长度** | ✅ **拍板（2026-10-28 用户）**：窗口长度语义不变（summary/n/full）；**默认 summary**，n 按需升、full 需显式；随 K15 一起可配。**落地（2026-10-28 v1-S4）**：三级渲染（summary 受每行 token 预算截断（rune 安全）、n 带窗口行数、full 全 id 不截断），长度随等级单调增长；默认 summary、full 需显式 | ✅ |
| K15 | **在线表注入** | ✅ **拍板（2026-10-28 用户）**：默认 **N=50 行、每行 summary ≤100 token、按 gate 过滤**；**全部参数可配 + 支持动态管理（运行期调整，不写死——用户明确要求）**；注入位置恒在 prompt 末尾（Q4）。**落地（2026-10-28 v1-S4）**：`core/loop/onlinetable.go` 配置面（三数值参数统一 0=默认/负数拒/超上界拒）+ 运行期 `GET/POST /api/online-table/config` 不重启生效 + CLI flag；gate 过滤复用 `discovery.VisiblePeers`（K11：别 session subagent 不出现）；注入=本轮请求尾部消息（不入持久历史）+ `context.assembled` 落 `online_table` part | ✅ |
| K16 | **深度限制 = 类型可达性（> 计数器）** | 主设计是**类型可达性**：`agent_type.can_spawn` 声明"我能派哪些类型"→ 第 4 层在类型层面就不可达。**数字计数器只是兜底**（`max_agent_depth` 可配置，默认两级委派）。每层成本线性叠加在三条链上：**summary 链**（深度 3 的 summary 是深度 2 的摘要 → 二次失真，同"多次压缩导致目标偏移"是同一个病）、**权限链**（每层 ∩ 父级 → 深度 3 时已很窄，再深一层纯开销）、**审计链**（责任追溯层数）。落地：`AgentType.CanSpawn` + `TypeRegistry.Register` 时 can_spawn 环检测（有向无环），数字深度（maxDepth 默认 3）兜底；只读型 CanSpawn 为空 → 更深层不可达（实现：core/agents/type.go + tree.go） | ✅ |
| K17 | **spawn 三道闸门** | 全部**运行时强制**（不靠提示词）：① **深度** `child.depth ≤ max_agent_depth` ② **类型** `child.type ∈ parent.can_spawn` ③ **权限** `child.permission ⊆ parent.permission ∩ policy_ceiling`。类型闸门让 `can_spawn` 有向无环，注册时校验。落地：`Tree.Spawn` 三闸门全运行时强制，闸门③经 perm.go 三维 read/write/exec 只收窄（未知级 fail-close 拒）；任一道拒则落 tool.call.failed、不建节点（实现：core/agents/tree.go + perm.go） | ✅ |
| K18 | **交流能力做成可复用组件** | `core/agentlink/`（或 comms/）：`Presence`（在线表）+ `Discover`（按 gate / 树查找）+ `Inquiry`（深入查询某 session 的事件窗口）+ `Broadcast`（向 gate / 子树发消息）+ `Handoff`（结构化交接带 payload）。**library 形态，不绑定任何协议**。**落地（2026-10-28 v1-S3）**：`core/agentlink` 五件套（E8 通用组件区；core 不做 IO——EventWindowReader/EventSink 宿主注入）；Broadcast/Handoff 事件化 `agentlink.broadcast`/`agentlink.handoff`（拉模型投递，幂等键含发送方 + store 去重 + 回读归属校验）；**subtree 射程 = 目标子树 ∩ 发送方一跳可见集（父+直系子）**——K27 显式逐跳、不做传递闭包（"子树"命名大于实际射程，要全树广播须另开口子，不得放宽可见集）；Inquiry = 摘要 + 事件窗口 + refs 核验（token 锚定防子串伪造，含空格路径整串核验；K24 端到端成立）；查询面全带 caller 按 K11 裁剪（实现：core/agentlink + host/agentlink.go） | ✅ |
| K19 | 跨机 | **不做分布式存储 / 分布式发现**。跨机时把 session 暴露成 **ACP agent 端点**，客户端用现成协议接入——不自己发明分布式协议。v0 未做——v4（跨机，session 暴露成 ACP 端点）再定 | ★ |
| K20 | **层级 3 = 只读型** | **由 type 自行声明**（不硬编码"第 3 层必只读"——某些 workflow 需要可写的 scout）；但**内置的几个 type 默认只读**（`scout`：read/glob/grep）。理由：subagent 是**上下文隔离器**（把大量读取外包出去，避免主 agent 上下文被占满）；只读型的 `can_spawn` 为空 → **第 4 层在类型层面不可达**。v0 未做——v1（层级语义细化）再定；v0 类型系统已支持"只读型"表达（CanSpawn 为空 + 只读 Tools），但无内置只读 type → **挪 v3**（2026-10-28 重新标注：随工作流 agent_type 声明一并解） | ★ |
| K21 | **orchestrator = 一个 type，不是硬编码层级** | "主 agent 下加一个 orchestrator"（它对整个项目 + 自己负责的模块做较完整探索，再分派 subagent）→ 表达为**一个 `coordinator` 类型的 agent_type**（`can_spawn` 指向 worker 类型，权限更高）。**层级数不硬编码**；"4 层"只是某种 type 组合的可达结果。落地：orchestrator 表达为一个 agent_type（其 CanSpawn 指向 worker），root 只是 depth=1，层数由 type 的 can_spawn 组合自然涌现；spawn/complete 按 can_spawn/深度注入，无硬编码"主 agent"概念（实现：core/loop/agenttools.go + core/agents） | ✅ |
| K22 | **兄弟协作：A + B，不开 C** | 兄弟 subagent **互相不可见**（可见性要动态变就难审计，也违背「分支」哲学）。解法：**A 共享契约前置**（主 agent 在 spawn 时把表结构草案 / API 签名写进每个 subagent 的提示词 → 把运行时通信问题变成拆解时的设计问题）+ **B 共享黑板**（兄弟经黑板交换信息，不走可见性）。**黑板同样事件化**：`board.written` 作为 durable 事件追加，当前态 = 索引器投影（键级 last-write-wins 由事件顺序天然保证，见 G23）；语义可借鉴 moamcp 的 raw board（键级 LWW + 墓碑删除 + 分区） | ✅ |
| K23 | **通信收敛机制** | ❌ **不做通信预算 / 配额**——靠**协调者居中**（`coordinator` type 承担协调行为）→ 树形沟通天然收敛，不存在"两两互聊"。**冲突防止靠 CI 门禁**（`gates` 里的 `ci.check` 部件）→ 冲突检测从**运行时移到集成时**。✅ **保留基础兜底**（这些是 agent 的基本属性，不是"预算"）：每 agent 的 turn 超时 / 工具超时 + **异常必须向上冒泡**（subagent 失败或卡住 → 直系父 agent 的 summary 必须报告它） | ✅ |
| K24 | **subagent 完成 = function + 结构化 summary** | subagent 写 summary 后调 `complete(summary)` 工具 → 该 summary 作为**主 agent 调用 subagent 结束后的汇报**。summary 应**结构化**（做了什么 / 改了哪些文件 / 结论 / 下一步 / 问题），并带**可验证引用**（文件路径、事件 seq 区间）→ 主 agent 可用 `Inquiry` 拉原文核验。**summary 是 subagent 回到主 agent 上下文的唯一通道**（这正是"上下文隔离器"的关键机制）。落地：`complete(summary)` 收敛，`AgentSummary`（text/refs/taint/abnormal）+ `RefVerifier`/`VerifyRefs` 核验，summary 经 `agent.completed` 落盘作为唯一回收通道（tainted 加 ⚠ 前缀）（实现：core/agents/summary.go + core/loop/agenttools.go） | ✅ |
| K25 | 单流多 agent 视图 | 主 agent 与 subagent **共用全局事件流**（G3），靠 `agent_id` 区分。上下文装配 = **按 `agent_id` 对全局流做投影**（主 agent 的视图 = 自身事件 + 子 agent 的 summary + 注入内容；subagent 的视图 = 自身事件 + 注入内容）。subagent 的 trace 因此天然持久、可回放。落地：主/subagent 同 session_id 单流，`newAgentEvent` 让父 loop 代子落 agent.spawned/completed/failed（agent_id=child），子 loop 与父同 Bus/SessionID 仅 AgentID 不同（实现：core/loop/events.go + agenttools.go） | ✅ |
| K27 | **垂直可见性口径定死** | ✅ 见 K11：**父 ↔ 直系子 可见 + Inquiry 沿父链逐级上查**（不是「全祖先/全后代自动可见」）。三处文档（06 §7.1 / K11 / handoff B#5）已统一 | ✅ |
| K28 | **Durable 与恢复语义（承重墙）** | 见 X 区（单独成节）。承 X 区全部落地：三段写路径（X1）、replay 三分类（X2）、checkpoint 四要素（X3）、resume 五步（X4）、ownership 幂等键（X5）、ledger 写放大（X6）均在 core/durable + core/loop 实现，kill -9 后可续跑且不重复副作用（实现：core/durable + core/loop/replay.go） | ✅ |
| K29 | **fork/clone 复制语义** | ✅ **拍板（2026-10-28 用户，改写原提案）**：fork **只附带上下文**——不复制 ledger/gate 全套（原提案"只复制 ledger 快照与 gate 成员"作废）；主场景 = `/btw` 类旁支命令。**实现后置**（随 /btw 命令落地，不进 v1） **UX 拍板（2026-10-28 用户）**：/btw 结果**只给用户**、不回灌主上下文；**预埋**“从 /btw 生成全新 session”钩子——只埋点不实现（实现需多方面微调） **落地（2026-01-29 v3-S2）**：host/fork.go（RunBtw——fork 只带折叠上下文、不带 ledger/gate，旁支事件归属独立 fork session，主 session 零新增）+ REPL/TUI /btw（space+tab）+ btw.forked 事件 + SetBtwPromoteHook 埋点（确实未实现生成，护栏钉住）；fork 四面过滤（在线表/peers/presence/discover）；上下文上限 16 条/6000 字符（含工具参数）+ 截断/配平分开告知 | ✅ |
| K26 | agent 树的 id | `agent_id` = **ULID**（不是递增序列——递增序列在并发 spawn 下不安全，且跨树无意义）；`depth` = int；`parent_agent_id` = ULID 或 null；树内"第几个 subagent" = int64（仅供 UI 显示，非身份）。落地：`agent_id` = `core.NewUlid()`（并发 spawn 安全）、`depth` = int、`parent_agent_id` = ULID 或 nil（root）；树内 spawn 顺序由 `children` 切片记录供 UI 显示（实现：core/agents/tree.go） | ✅ |

## L. 可观测性与评测

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| L1 | trace | 事件流即 trace | · |
| L2 | telemetry | 独立管道，默认关 / 脱敏 / 单向 | · |
| L3 | 调试 | 按需反射检视内部状态 | · |
| L4 | 评测 | 事件流即数据集（回放 + 逐 turn 指标） | · |
| L5 | v0 评测 | 不建独立评测框架 | · |

## M. 错误与取消

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| M1 | 错误表达 | 错误是普通事件，不是特殊通道 | · |
| M2 | 取消分层 | turn / step / task 三层 | · |
| M3 | 取消识别 | `context.Canceled` 与普通错误区分 | · |
| M4 | 重试 | 可重试 vs 致命有明确分类 | · |
| M5 | 错误文案 | 必须让模型能理解并自我修正 | · |
| M6 | **通用重试组件** | `core/retry`：`Do(ctx, policy, fn)` —— 多次重试无效后**直接返回聚合错误**（含各次尝试）。**主 agent 与 subagent 共用同一套**（工具调用 / 模型请求 / CI 检查都走它）。v0 未做——v3（core/retry 通用重试组件）再定 | ★ |
| M7 | **重试安全性** | ⚠️ 只有**声明为幂等**的操作才自动重试。工具契约加 `idempotent: bool`；**有副作用的工具重试会重复副作用**（照 pi-durable 的 `replay: "safe"` 概念）。模型 API 429/5xx 可重试；CI 检查幂等可重试；`bash` 默认**不可**重试。落地：工具契约加 `Idempotent` + `Replay` 字段（缺省 false/unsafe = 不自动重试），intent 落盘时记录这两个标记供 resume 判定；bash 等有副作用工具默认不声明 → 不可自动重试（实现：contracts/tool.go + core/loop/toolcall.go） | ✅ |
| M9 | **长工具 TOCTOU 声明边界** | 策略在 T0 判定、副作用持续到 Tn → **「判定只约束发起时刻」是已知边界**（与 V8 同款诚实处理）。把判定快照记进调用事件，供事后审计。落地：「判定只约束发起时刻」诚实处理——策略在调用时刻判定，结论（verdict/rule_id/reason）作为 `policy_snapshot` 记进 `tool.call.intent` 供事后审计，不记参数/env；resume 重放同一操作时刻意不重跑策略链（实现：core/loop/toolcall.go + events.go） | ✅ |
| M8 | 抛出 ≠ 崩溃 | 重试耗尽后返回的 error 要**转成模型可见的工具结果**（让模型自我修正）+ 记一条 `error` 事件，**不是进程崩溃**。与 M1（错误是普通事件）一致。落地：执行失败/deny/参数坏/权限不足一律落 `tool.call.failed`（错误文案模型可读、能自我修正）回填给模型让 loop 继续，不是进程崩溃；执行器 panic 也被 `executeSafely` recover 成一次普通工具失败（实现：core/loop/toolcall.go） | ✅ |

## N. 协议与互操作

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| N1 | MCP 方向 | 先做 client 还是先做 server？v0 未做——v3（MCP：先 client 后 server 或反之，官方 go-sdk 已实测可用）再定 **拍板（2026-10-28 用户）**：**client 先**——先消费别人的 MCP server（工具生态立刻变宽），官方 go-sdk 已实测 **落地（2026-01-29 v3-S1）**：runtime/mcp（官方 go-sdk v1.8.0，stdio 优先+Streamable HTTP）——mcp__server__tool 命名隔离防遮蔽、deny-all 默认、三段事件走既有写路径、发现期 deadline+输出三道闸+Close 有界收尸；挂账转 v3-S2：工具列表快照无动态刷新、stderr 记录条数闸、装配预算口径 | ✅ |
| N2 | MCP 可选能力 | 不实现 sampling / elicitation / roots。**落地（2026-01-29 v3-S1）**：不广告+明确拒+留痕（runtime/mcp/n2.go 中间件）；新协议 SDK 自禁 server→client 请求（SEP-2322），中间件治不守规矩的 server | ✅ |
| N3 | ACP | 不在 v0 | ✅ |
| N4 | hub↔UI 协议 | ✅ **WebSocket + NDJSON**（与 D1/D2 已定契约一致，不引入 protobuf 工具链；浏览器/手机原生可用零额外组件；调试友好 `curl` 即可看流；HTTP/1.1 在移动网络重连比 HTTP/2 简单；量级不需要 gRPC 多路复用）。gRPC 排除原因：protobuf 与 JSON Schema 契约冲突 + gRPC-Web 需额外部署 Envoy proxy **补全（2026-01-30 v3-S4）**：GET /api/events 回放面（watermark=事实流 tip）+ WS command.submit/approval.request/approval.respond 遥控面 + subscribed 回执 + API 前缀 405/404 + transport id 语义放宽 string（INVENTORY §5 记账） | — | ✅ |
| N4b | 公网暴露 / 远程访问 | ⏸ **v0 不做，但架构上预留**。**密钥鉴权能做且应该做，但它是必要条件不是充分条件**——认证解决"谁能连"，不解决"连上的人能干什么"。启用远程访问的**必备条件**：① 凭证只走 header/query，**绝不走 cookie**（见 N5）；② 校验 `Origin`；③ token 可撤销 / 可轮换 / 按设备签发；④ **降权视图**（见 N5）；⑤ 全量审计进事件流；⑥ 公网必须 `wss://`（TLS）。默认 hub 只监听 loopback，远程走私有网络（Tailscale/WireGuard，自带设备身份与加密，比自签证书省事得多）。v0 未做——v4（公网/远程访问：凭证/撤销/降权视图/审计/TLS 一整套）再定；v0 只监听 loopback + 无凭据（见 N5b） | — | ★ |
| N5 | 客户端鉴权与降权 | hub 客户端持 token（多设备多 token，可撤销）；**token 带 scope**（`read` / `prompt` / `approve` / `admin`），实际能力 = **token scope ∩ 策略上限 ∩ 降权视图**（同一套交集逻辑，与 H11 一致）。**降权视图**是给远端客户端的受限 UI：只读事件流 + 提交 prompt + 审批决策，且**高危操作的批准在降权视图里不可用**（或强制二次确认 + 设备绑定）。v0 未做——v4（客户端鉴权与降权：token scope ∩ 策略上限 ∩ 降权视图）再定 | — | ★ |
| N5b | ⚠️ loopback 的陷阱 | **WebSocket 不受同源策略约束** → hub 即使只监听 `127.0.0.1`，浏览器里的**任意恶意网页**都能发起 `ws://127.0.0.1:port` 连接。若凭证用 cookie，浏览器会自动携带 → 等于把 hub 暴露给任何网页。因此：**凭证只能放 `Authorization` 头或 query（恶意页读不到），且必须校验 `Origin`**。v0 部分落地：hub 只监听 loopback（`host` checkLoopback 拒非 loopback，含"127.0.0.1.evil.example"域名伪装）、WS 校验 Origin（`runtime/transport` checkOrigin，跨源页面 403）、无 cookie；凭证 token 方案未做（属 N4b/N5）。v0 安全模型 = loopback + 无凭据（实现：host + runtime/transport） | — | ✅ |
| N6 | NDJSON 惯例 | 与 ACP 传输保持一致 | — | · |

## O. UI

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| O1 | UI 依赖 | TUI / GUI 都只依赖 contracts | · |
| O2 | 权限 UI | 只渲染判定结果，不求值 | · |
| O3 | 必须 UI 无关的 5 项 | 事件流格式 / transcript 契约 / 权限语义 / 会话句柄 / 长时程状态机 | · |
| O4 | GUI | v0 不做 | · |
| O5 | **v0 先做 Web UI** | ✅ **v0 优先 Web UI，TUI 后做**。理由：我们的差异化卖点就是**上下文可视化**（token 构成树 / 压缩痕迹 / 多 session 在线表）与**多 session 并行面板**——这两项在 Web 上表达力强一个量级；且传输已定 **WS + NDJSON** → Web UI 成本极低；迭代验证快 | ✅ |
| O6 | **TUI 保留位** | ✅ core 只依赖 contracts → **TUI 随时可加**（独立包 + 独立进程）。TUI 的定位是**终端里的日常交互面**（对话 / 审批 / 流式），Web 是**深度视图**（可视化 / 多面板）。v0 **不承诺 TUI**。**落地（2026-10-28 v1-S5）**：`ui/tui`（bubbletea+lipgloss）三面——对话/审批/在线表；`tui.Approver` 桥接审批进事件循环（fail-close，取消=deny）；`alasol tui` 入口 | ✅ |
| O7 | **Web UI 带来的安全代价** | ✅ Web 让「手机远程」变自然 → 强化 N4b/N5b：hub 只监听 loopback、凭证只在 header、校验 Origin、远端客户端用**降权视图**。另注意 Web UI 与「单二进制即用」的初衷有张力 → hub 是常驻进程，TUI/CLI/Web 都是它的客户端 | ✅ |

## R. 工具与角色（工具集按角色分离）

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| R1 | 工具集关系 | **主 agent 与 subagent 各有自己的工具集，两者有交集**（不是包含关系） | ✅（用户已明确） |
| R2 | 主 agent 专属工具 | 只给主 agent，**subagent 禁止调用**（如 `advance`/`milestone`、`escalate`、`spawn`——否则 subagent 可自己标记里程碑跳过阶段） | ✅ |
| R3 | subagent 专属工具 | agent_type 专精工具**不注入主 agent 上下文**（防上下文污染，也省 token） | ✅ |
| R4 | 共享工具 | 两者都可用（read / glob / grep …） | ✅ |
| R5 | 可见性机制 | **不暴露即不存在**：工具能否被调用 = 该 agent_type 的 `tools[]` 是否包含它；模型看不见就不会调。**不需要 `allowed_callers` 元数据** | ✅ |
| R6 | 注入时机 | 工具 schema **只注入用得到它的 agent 的上下文**。落地：`visibleTools` 按 agent_type 的 `Tools` 列过滤（R5 不暴露即不存在）+ deny 移出可见集，模型只看得到、也只会调用可见工具；生命周期工具（spawn/complete）也按 can_spawn/深度条件注入（实现：core/loop/loop.go visibleTools） | ✅ |
| R9 | 角色统一 | **主 agent 与 subagent 共用同一套 agent_type 体系，main 只是一个 type**（如 coordinator）。"主/子"是**能力差异**而非角色差异 | ✅ |
| R10 | 编排能力 | `spawn` 只是 orchestrator type 的一个工具，worker type 没有 → 多 agent 模型自然涌现，不需要框架内置"主 agent"概念。落地：`spawn`/`complete` 是按类型可达性注入的框架内部工具（can_spawn 非空才可见 spawn、depth>1 才可见 complete），过策略与三闸门；worker type 无 spawn → 多 agent 层级自然涌现，无硬编码"主 agent"概念（实现：core/loop/agenttools.go + visibleTools） | ✅ |
| R11 | 可见 vs 权限分离 | 可见性管"能不能看见"，权限管"能不能做"（agent_type 的 `permission_request`）；需要"可见但禁止"时走策略层 deny。落地：`visibleTools`（type.Tools + deny）管可见，`permDenial`（effectivePerm = 请求∩ceiling∩父授予，RequiredCapabilities 推导）管能否做；"可见但禁止"经策略层 deny/权限维度收窄实现，两层独立（实现：core/loop/loop.go + toolcall.go permDenial） | ✅ |
| R7 | 权限绑定 | 权限跟着 **agent_type** 走，不跟着工作流叠加走（"review 只读"通过选只读 agent_type 实现） | ✅ |
| R8 | 能力池叠加 | 追加 loop 时 system prompt 的哲学**并列追加**，能力池**并集**，权限仍受策略上限约束。v0 未做——v1（能力池叠加：追加 loop 的 prompt 并列 + 能力池并集）再定 → **挪 v3**（2026-10-28 重新标注：随工作流 agent_type 声明一并解） | ★ |

## W. 工作流（能力包 / 设计哲学 / 主 agent 自编排）

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| W1 | 工作流本质 | **能力包的提供与批准**：一套工作哲学 + 子代理类型 + 上下文管理方式 + 关键阶段部件 + 预批准。**不是流程**。v0 未做——v3（工作流引擎）再定 方向已由用户明确，**2026-10-28 再确认**：能力包（哲学+agent_types+context_strategy+gates+grants），**不是流程、非强制** **落地（2026-01-29 v3-S3）**：core/workflow.Package（能力包=契约数据+代码部件，可挂可不挂，不挂零行为变化酸测试钉住；无流程/状态机，W21 框架不校验编排） | ✅ |
| W2 | 阶段含义 | 阶段 = **设计哲学**（如 plan-coder-critic-review），不是强制序列，不规定"这个阶段必须派哪些 subagent"。v0 未做——v3（工作流引擎）再定 **拍板（2026-10-28）**：阶段=哲学非强制序列——“强制反而不适合”（用户） **落地（2026-01-29 v3-S3）**：哲学文本进 system prompt（AugmentSystem），无阶段状态机 | ✅ |
| W3 | 触发方式 | slash 命令 / 显式触发 → **哲学文本进 system prompt 供主 agent 选择**。v0 未做——v3（工作流引擎）再定 **拍板（2026-10-28）**：slash/显式触发 → 哲学文本进 system prompt 供主 agent 选择 **落地（2026-01-29 v3-S3）**：/workflow 与包声明命令（如 /tower）在 chat+TUI 双面触发；workflow.activated 记实际触发串 | ✅ |
| W4 | 追加 loop | 有问题时**在后续追加一个 loop**（如 review-critic）继续解决 → 工作流可组合。v0 未做——v3（工作流引擎：追加 loop）再定 **拍板（2026-10-28）**：追加 loop 可组合（如 review-critic 续解问题） **落地（2026-01-29 v3-S3）**：Compose 组合（深拷贝防别名）+ tower 包附 ReviewCriticSegment 追加段 | ✅ |
| W5 | 子代理类型 | `agent_types[]`：每项声明 `prompt` / `tools[]` / `permission_request` / `model`——**用户可自定义工具列表、提示词、权限范围**。v0 未做——v3（工作流引擎：子代理类型声明）再定；v0 agent_type 契约（contracts.AgentType）已支持 prompt/tools/permission_request/model，但无工作流层加载 | ★ |
| W6 | 上下文策略 | `context_strategy` 是工作流的一部分（可区别于默认追加式）；内置枚举 + 参数，复杂的才做成扩展。v0 未做——v3（工作流引擎：上下文策略）再定 | ★ |
| W7 | 验收目标与部件 | 工作流可自带**验收目标**（自写 CI、写屏障、合并前检查……）；里程碑只是其中一例。分两类：**同步门禁类**（写屏障/权限门）→ **数据驱动的策略规则**，进程内同步生效；**异步任务类**（CI/验收/构建）→ **进程级组件**，可热插拔、可替换。**冲突检测放在这里（CI 门禁），不在通信层解决**。v0 未做——v3（工作流引擎：验收目标与部件）再定 | ★ |
| W8 | **门禁正确形态** | ✅（依据 tower 回函）**不透明命令 + 一次性快照 + 事后按 receipt 校验**。三层切分保留：① 通用组件（执行/判定，不绑工作流）② 默认工作流片段（检测到 git 仓库自动叠加，可关）③ 工作流可引用/覆盖。**门禁只认事实（exitCode / commit），不认意图**（筛选、baseline 都是后话） | ✅ |
| W9 | **命令靠注入，不靠探测** | ✅（tower 已用同样做法：`.tower/ci.json` 手工维护，**仓库级唯一**，无探测无默认无分层）。引擎把命令当**不透明 shell 字符串**执行，不区分 typecheck/lint/测试。**命令对引擎是黑盒，准确性的责任在写命令的人** | ✅ |
| W9b | **门禁是轻量的** | 不是 GitHub Actions 那种重 CI —— 耗时短，因此**所有工作流默认启用**、每次合并前都跑，耗时不是问题。且它本质是**长时程行为**（时长影响不大）。v0 未做——v3（工作流引擎：门禁轻量、默认启用）再定 | ★ |
| W10 | **冲突由协调人协商** | ✅ 文本冲突**不自动解**，交回 worker（rebase → 解冲突 → push → 重新 review）。前提：协调人有全局观感（可见所有分支 summary、可 `Inquiry` 拉原文核验）。⚠️ **必须先 `git merge --abort`**（tower 的最大坑：主 checkout 卡在 MERGE_HEAD 状态最难人工收拾）→ 我们的形态是 abort + escalate + checkpoint 挂起 | ✅ |
| W9c | **CI baseline** | ✅ **优先级高**（tower 回函：这是他们的真空地带；**红仓是长时程自主的常态**，没有 baseline 门禁就永远卡死）。要「**已知失败指纹 + 豁免**」的口子；baseline 作用域建议**仓库级**（分支级会碎片化） | ✅ |
| W10b | **CI 失败 → 自动 escalate** | ✅ 门禁失败自动 escalate（checkpoint + 挂起）。**不阻塞其它 agent**——只阻塞本分支的 merge，依赖它的 mission 由 deps 检查挡住。failed 是**终态**（无自动重试）；重跑 = 重新发起一次 run | ✅ |
| W7b | 插件可行性 | **Go plugin 在 Windows 不可用**（仅 linux/darwin/freebsd）→ 组件一律走进程级或解释型。写屏障**不能**做成进程外插件（TOCTOU + 可绕过），必须是进程内策略规则 | ✅（技术结论） |
| W21 | 编排主体 | 主 agent 自行决定：开几个 subagent、用哪个 agent_type、并行与否；框架不校验。v0 未做——v3（工作流引擎：编排主体）再定 **拍板（2026-10-28）**：编排主体=主 agent 自决，框架不校验（非强制） **落地（2026-01-29 v3-S3）**：无编排校验；agent_types 校验只查配置合法性（重名/悬空/环 fail-close，陷阱 16） | ✅ |
| W11 | 动态性 | 偏离（跳过里程碑、自造流程、多开）**记入事件流**；工作流文档变更走事件追加。v0 未做——v3（工作流引擎：动态性）再定 | ★ |
| W12 | 归属 | 绑定在 session 上的一份配置，不是 core 全局状态。v0 未做——v3（工作流引擎：归属 session 配置）再定 | ★ |
| W13 | 契约形态 | `Workflow` 是跨边界 JSON Schema 契约（纯数据，可被任何语言写/组合/追加）。v0 未做——v3（工作流引擎：Workflow 跨边界契约）再定 | ★ |
| W14 | 代码扩展点 | 只有两处是代码：**里程碑部件**（`MilestoneHandler`）与**自定义上下文策略**。v0 未做——v3（工作流引擎：代码扩展点）再定 **拍板（2026-10-28）**：代码扩展点只有两处——MilestoneHandler + 自定义上下文策略 **落地（2026-01-29 v3-S3）**：MilestoneHandler（里程碑部件）+ ContextStrategy 挂载（core/compaction.AssemblerFactory），未发明第三个 | ✅ |
| W15 | **预算策略** | **默认不设上限**（长时程自主是卖点，预算不可控）；改为**风险闸门**：主 agent 判断涉及**大范围架构改动/既有内容**时主动停下告知用户。v0 未做——v3（工作流引擎：预算策略改风险闸门）再定 | ★ |
| W16 | 闸门实现 | 框架提供**变更影响面**客观依据（diff 规模、触及的核心接口）供 agent 判断，而非纯靠模型感觉 **落地（2026-01-29 v2-S2）**：change_impact 报告面（core/graph AssembleImpact + host API）；diff 由调用方喂（框架不算 diff） | ✅（做） |
| W17 | escalate 语义 | **checkpoint + 挂起**（不 deny、不丢进度）；恢复后由用户决定继续 / 改方向 / 放弃。无人值守同样挂起而非失败 | ✅ |
| W17b | escalate vs ask | 两者都走 Approver 但**触发源不同**：ask=策略链拦截，escalate=agent 主动升级。日志必须可区分触发源。落地：契约层区分触发源——`ApprovalRequest.Trigger` = `policy_ask`（策略链拦截，无人应答→deny+拒绝话术）/ `escalate`（agent 主动升级→checkpoint 挂起）；审批事件与日志据此可区分（实现：contracts/approval.go ApprovalTrigger） | ✅ |
| W20 | 升级 vs 审批 | `ask`（策略拦截）无人应答 → deny + 拒绝话术；`escalate`（agent 主动）→ checkpoint 挂起 | ✅ |
| W18 | 哲学分类 | 构建类（plan-coder-critic-review）与探索/资产探查类各有不同哲学 → 工作流是可分发的包。v0 未做——v3（工作流引擎：哲学分类）再定 | ★ |
| W19 | 现成验证用例 | **tower 就是一个工作流实例**（worktree 生成 + 合并前仿 CI 检查作为里程碑部件）→ 可作首个实现验证。v0 未做——v3（工作流引擎：tower 作首个验证用例）再定 **拍板（2026-10-28）**：tower 作首个验证用例（critic→reviewer 循环做成第一个 workflow 包） **落地（2026-01-29 v3-S3）**：core/workflow/tower 首包——plan-coder-critic-review 哲学 + validationMilestone（critic 收敛+reviewer approved 才过）；角色池声明未接线（诚实措辞：角色由协调人提供） | ✅ |
## V. CI 门禁实现细节（依据 tower 回函 `ho_d0df74d42709`）

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| V1 | **快照隔离** | CI 跑在**一次性快照 worktree**（把分支 tip 检出到独立 detached worktree），**不是 live worktree** → 未提交改动、写了一半的文件**结构性不参与**。比 dirty 检查干净。并行跑 N 个 CI = N 个独立快照。落地：`gateexec.Snapshot` 用 `git worktree add --detach` 把分支 tip 检出到独立 detached worktree，未提交改动结构性不进快照；并行跑 N 个门禁 = N 个独立快照互不干扰（实现：runtime/gate/git.go） | ✅ |
| V2 | **receipt 校验** | 门禁只认四件事：① `exitCode === 0` ② `receipt.commit === 分支当前 tip` ③ `observedCommit === commit` ④ `command === 当前配置`。**防止"跑的是旧 commit"或"配置已改"被误判为通过**。落地：`core/gate.Verify` 四查，评估序先可信度(②③④)后成败(①)——过期/错配 receipt 硬拒绝，只有"可信但失败"才转 baseline（实现：core/gate/receipt.go） | ✅ |
| V3 | **外部资源隔离 = 留白 + 约定注入** | 快照只隔离**文件系统**；端口 / 临时目录 / 全局缓存（pnpm store、go mod cache、venv）/ 构建产物**不在问题域内** → 给命令注入约定环境变量（`$CI_WORKTREE`、`$CI_PORT_BASE` 等）让命令自隔离。**tower 明确说这是留白、他们没解**。落地：`gateexec.Run` 注入约定 env `CI_WORKTREE`/`CI_COMMIT`/`CI_PORT_BASE`（端口基址随机派生避免并行撞端口）让命令自隔离，诚认这是留白（实现：runtime/gate/exec.go） | ✅ |
| V4 | 依赖缓存策略 | tower 选**每快照独立**（干净但每次全量安装，慢）。我们可做**内容寻址的共享只读缓存**（快 + 无锁竞争 + 无污染）——但这是他们重做清单的第 1 条，说明有真实收益。v0 未做——v2（依赖缓存策略：内容寻址共享只读缓存）再定 **定案（2026-10-28）**：内容寻址共享只读缓存（无锁竞争、无污染）；v2 门禁增强实现 **落地（2026-01-29 v2-S3）**：core/gate/cache.go（键=仓库作用域+清单内容，26 字符 Crockford）+ runtime/gate 注入 CI_CACHE_KEY/CI_CACHE_DIR；框架只派生键与目录（不建目录/不加锁/不装依赖，命令自暖）——"命中不重复构建"是命令侧行为，框架保证键的内容寻址与跨键隔离（版本+仓库双隔离） | ✅ |
| V5 | **日志截断 + 耗时埋点** | CI 日志文件**必须设上限**（tower 现在无限增长，是他们重做清单第 1 条）。run 记录加 `elapsedMs`（tower 没埋，复盘全靠猜）。落地：CI 日志 `LogTail` 只留尾部 ≤64KB（`tailBuffer` 上限，防无限输出吃内存），`Receipt.ElapsedMS` 必记耗时毫秒，`gate.checked` 事件带 elapsed_ms（实现：runtime/gate/exec.go + core/gate/receipt.go） | ✅ |
| V6 | **review-tip 失效检测** | 「review 后又推了一行」是 tower 最常见的 merge block。我们的事件流天然能检测：分支 tip 变了 → 已有 review 结论失效 → 要求重审。v0 未做——v2（review-tip 失效检测）再定 照主张（2026-10-28）：review 后分支 tip 变 → 结论失效 → 要求重审（事件流天然可检测）；v2 实现 **落地（2026-01-29 v2-S3）**：core/gate/review.go（review.recorded 事件 + Stale/ReReviewRequired/Approved + 分支名归一）；无生产发射方——v3 合并门禁接线（与 gate.checked 同款口径） **接线（2026-01-29 v3-S3）**：host/mergegate.go（RecordReview 发射 + CheckMerge 消费 Approved/ReReviewRequired，缓存精确失效）——"review 后又推一行"端到端拦下 | ✅ |
| V7 | **合并后冲突预测** | tower 在合并**成功后**做「本次改动 vs 未合并 mission 的三点 diff 求交集」提前预警撞车。**我们能用依赖图做得更准**（G 区 change_impact 的可达性查询）。v0 未做——v2（合并后冲突预测，用依赖图可达性）再定 照主张（2026-10-28）：依赖图可达性做合并冲突预测；v2 实现 **落地（2026-01-29 v2-S3）**：core/graph/conflict.go（conflict.predicted 事件 + 非对称判据 direct/one-side + 聚合诚实化 unevaluated + 图不可用诚实降级）；判据代价已声明（同一消费者同时依赖两个被改的库不报，weak 信号留消费端）；接线留 v3 **接线（2026-01-29 v3-S3）**：PredictPostMergeConflicts 发射 conflict.predicted（五态诚实降级不报假无冲突） | ✅ |
| V8 | **write guard 的诚实边界** | ✅（已定，方案 a）tower 的 guard 是 **fail-open** 且**只拦 Write/Edit 工具**——Bash 重定向 / `rm` / `git checkout --` 都不拦。他们的教训：要做就 **fail-close + 覆盖 shell 写入**。我们 V0 做不到覆盖 shell 写 → **UI 上明确声明边界**（写保护覆盖文件工具；Bash 写入受策略链但非 OS 强制），**不假装覆盖了**。真正的解法是 V1 的 OS 沙箱（Windows 上原生沙箱极少：AppContainer 形状不对、MIC 不适用、Windows Sandbox 需 Hyper-V 且无法访问真实工作区） **落地（2026-01-29 v2-S4）**：OS 沙箱（H9 三件套）落地——bash 写目录外被内核拒绝（用户私有文件读也被挡）；诚实边界：BUILTIN\Users 授权位置默认仍可写可读（strict 模式可关，代价 overlapped I/O）；进程内 read/write 工具仍是软检查非 OS 边界 | ✅ |
| V9 | 可搬运资产（**语义参照，产物重写**） | **不搬代码**（与 omkc 的 mission / roster / agentId 强耦合），但**可搬 schema 与机制**：CI run 记录 schema、**receipt 四要素校验**、merge gate **步骤骨架**、gitOps 的 junction-safe rmTree 与 worktree registry、guard 的「声明 accesses + 子路径判定」形态。**但要挑着搬**——tower 的链是 mission→deps→survey→review→tip→scope→CI→base→merge，我们**没有 mission / deps / 独立 reviewer**，对应物是 agent 树 + 协调人 → **只搬 CI 那几段与步骤骨架，其余不要**；「合并后冲突预测」也不能照搬（三点 diff 交集 → 我们用依赖图可达性，更准）。落地：按"只搬 CI 那几段"选择性实现——receipt 记录 schema + 四要素校验（core/gate）、一次性快照 worktree（gateexec.Snapshot）、junction-safe rmTree（removeTreeSafe，Lstat 不跟随链接）；mission/deps/review 链未搬（实现：core/gate + runtime/gate） | ✅ |
| V10 | Windows 具体坑 | git 走**绝对路径 execFile `git.exe`**（不走 shell，防 cwd 劫持）；`windowsHide:true`；kill 用 `taskkill /T /F /PID`；**WER 弹窗压制**（否则桌面炸弹窗）；**junction 防御**（rmTree 先解 junction 再删）；路径身份比较要 **realpath + 小写**；worktree 被 CI 子进程锁住**无自动解锁**（只能人杀进程）。落地：git 绝对路径 execFile git.exe（不经 shell）、`windowsHide:true`、kill 用 taskkill /T /F /PID、junction 防御（removeTreeSafe Lstat 不跟穿）；WER 崩溃弹窗完整压制属沙箱层（v2）、realpath 路径身份比较属 S13（v1）——诚实标注未做（实现：runtime/gate + runtime/cap/local） **补账（2026-01-29 v2-S4）**：WER 完整压制落地（SetErrorMode 按位或+复原、WerAddExcludedApplication 真实映像名、只删自己新增项）——V10 沙箱层挂账清零 | ✅ |


## S. 身份与 ID（详见 `08-identity-model.md`）

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| S1 | 框架 id 格式 | **ULID**（26 字符 Crockford base32）：全局唯一、同秒不撞、字典序=时间序、自带创建时间、文件名/URL 安全、大小写不敏感 | ✅ |
| S2 | 序号 vs id | `turn_id` / `step_id` 用 **session 内 int64 序号**（天然可比较可排序），不用 ULID | ✅ |
| S3 | 哈希的用途 | **只做路径分组**（`project_id` = hash(canonical_cwd)、`repo_id` = hash(canonical_git_root)），**不作身份**；**短形落定（2026-10-28 v1-S1）**：SHA-256 截短 128bit → Crockford base32 26 字符（复用 ULID 编码器；实现：core/hash.go + runtime/identity） | ✅ |
| S4 | provider id | **黑盒**：原样存、原样回传；不解析 / 不改写 / 不折叠大小写 / 不做规范化 | ✅ |
| S5 | 字段隔离 | `Event.id`（框架 ULID）与 `payload.tool_call_id`（provider 不透明串）**永远不同字段**；MCP 的 `sessionId` / `requestId` / `progressToken` **不进我们的命名空间** | ✅ |
| S6 | JSON 类型 | 所有 id 在 JSON 里**一律 string**（int64/ULID 转 number 会丢精度） | ✅ |
| S7 | 事件去重 | **只看 `Event.id`**，绝不看 payload 里的任何 id | ✅ |
| S8 | 字符集纪律 | 框架 id 限 `[0-9A-HJKMNP-TV-Z]`；禁标准 base64；禁只靠大小写区分；禁 id 做文件名（provider id 可能含 `/` `:`） | ✅ |
| S9 | 失效即拒 | 字符集/长度不合则**拒绝并记事件**，不截断、不转义、不补零 | ✅ |
| S10 | 变更规则 | resume 保持同 id；fork/clone 生成**新 id + `parent_id`**；归档不改 id | ✅ |
| S11 | 并发唯一性 | ULID 实现线程安全（时间戳单调 + 随机源加锁 / atomic 计数器）。落地：`core.NewUlid` 线程安全——48bit 毫秒时间戳 + 80bit 随机取自 `crypto/rand`（并发安全无需加锁），降级 `math/rand/v2`（顶层函数同样并发安全）；80bit 随机保证同毫秒内唯一（实现：core/ulid.go） | ✅ |
| S12 | 校验与契约 | id 入 JSON 前统一校验（字符集/长度/非空/来源标记）；schema 里给每个 id 字段标 `pattern` + `x-id-kind`。落地：`core.ValidUlid` 校验 26 字符 Crockford base32（S9 不合则拒绝不修复），loop/agents/spawn 等入口调用它；schema 侧 id 字段标 `pattern` + `x-id-kind`（实现：core/ulid.go ValidUlid + contracts/schema） | ✅ |
| S13 | 路径规范化 | ✅ **口径拍板（2026-10-28 用户）**：`hash(cwd)` 前必须 **解析符号链接（EvalSymlinks）→ Windows 转小写 → 统一分隔符 → 去尾斜杠**；`project_id = hash(canonical_cwd)`。**落地（2026-10-28 v1-S1）**：runtime/identity；Windows 的 EvalSymlinks 不解析 junction 且遇 junction 报错 → 落地为逐组件 `os.Readlink`（junction/symlink 一视同仁 + 环路保护），语义等同口径；入口先 `filepath.Abs`（相对路径写法不漂移） | ✅ |
| S14 | repo_id 独立 | ✅ **口径拍板（2026-10-28 用户）**：`repo_id = hash(canonical_git_root)`，worktree 隔离下多个 cwd **按 git root 聚合**（资产图按 repo 级）。**落地（2026-10-28 v1-S1）**：worktree 的 `.git` 文件读 `gitdir:` 回溯主仓根，实测多 worktree 同 repo_id、异 project_id | ✅ |
| S16 | **命名空间补全** | ✅ 08 的命名空间表补入：`causation_id`（F3）、`gate_id`（K12）、`workflow:<run_id>`、`sum:` 引用（ContextSnapshot）、`parent_agent_id`（K26） | ✅ |
| S15 | 展示前缀 | 日志/UI 用 `session_id[:8]`，**比较一律用完整值**。**注（2026-10-28 v1-S4）**：在线表短 id 取**随机尾段** 8 字符——ULID 前 8 字符整个落在毫秒时间戳段内，同批铸造逐字节相同（展示不可区分，评审 p2）；[:8] 前缀仅适用于日志类容错场景 | · |

## Q. Prompt 组装与注入源

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| Q1 | 编排时机 | **最后做**——先确定"所有需要注入的内容"及其**稳定性分级**，再定 prompt 顺序。实现与提案有出入：v0 注入源只落 system/tools/history 三源（`context.assembled` 的 base/tools/history），编排时机与稳定性分级排序未做（在线表注入属 v1），待确认 → 剩余部分**随深度压缩定分级（挪 v2）**（2026-10-28 重新标注）。**落地（2026-01-29 v2-S1）**：分级与 prompt 顺序落定（13 §6 布局：[rules][ledger 稳定半][L2→L1][常识区][ledger 易变半][当前]），编排时机=装配期按稳定性分级 | ✅ |
| Q2 | 注入源清单 | system/基础提示 · 工具 schema · agent_type 与权限声明 · 项目规则（AGENTS.md）· goal ledger · 历史事件 · **在线表/在线状态** · 当前阶段与 role。实现与提案有出入：v0 只落 system/工具 schema/历史事件三源（context.go 的 base/tools/history），项目规则/ledger/在线表/阶段与 role 未注入，待确认 → 同上（挪 v2，随深度压缩补全注入源分级）（2026-10-28 重新标注）。**落地（2026-01-29 v2-S1）**：注入源补齐——base/tools/rules/ledger（稳定半+易变半）/common_sense/stage:N/history/online_table 全部入 context.assembled parts；"当前阶段与 role"随 stage:N 与 base（agent_type 既有注入）落位 | ✅ |
| Q3 | 稳定 → 变动排序 | **极稳定 → 半稳定 → 变动 → 极变动**。system/工具/schema 在前；项目规则/ledger 居中；历史次之；**在线表放最末尾**。实现与提案有出入：v0 无多源注入的稳定性分级排序（只有 base/tools/history 三项粗粒度，顺序即注册序），待确认 → 完整分级随深度压缩落地（挪 v2）（2026-10-28 重新标注）。**落地（2026-01-29 v2-S1）**：稳定→变动排序落地（13 §6 缓存定律：稳定在最前、易变贴生长前沿；在线表恒末尾） | ✅ |
| Q4 | 缓存友好 | **在线表 / 在线状态必须放 prompt 末尾**——它秒级变动，放前面会让整个前缀的 prompt cache 失效。**落地（2026-10-28 v1-S4）**：在线表恒为 provider 消息序列最后一段（含工具调用轮之后），只进本轮请求、不入持久历史（防 fold 污染与累积） | ✅ |
| Q5 | 暴露等级收敛 | ✅ 随 K14 拍板（2026-10-28 用户）：默认 `summary`，按需升 `n`，`full` 需显式 | ✅ |
| Q6 | Inquiry 返回 | **结构化摘要 + 事件窗口**（两者都给）：摘要给模型直接消费，窗口给模型按需翻。**落地（2026-10-28 v1-S3）**：`InquiryResult` = SummaryEntry[] + EventWindow + RefCheck[]；同 session 默认投影到调用方可见集（K11），跨 session 整 session（对端视角，K24 核验依赖）；HTTP 默认 limit=200 + truncated 信号 | ✅ |
| Q7 | **summary 污点继承** | ✅ subagent 的 summary 是回主 agent 的**唯一通道** → 若 subagent 读到被注入的内容，summary 就是攻击载荷出口。**summary 强制带 `taint`**（继承来源事件的最高污点级），主 agent 对 tainted summary **按不可信数据处理**（与 H13 同源） | ✅ |

## X. Durable 与恢复语义（承重墙，2026-10-27 补）

> 长时程自主是产品存在理由，而"进程半夜挂掉后从哪续"此前**整块缺席**。本区补齐。参照 pi-durable 的设计（`03` 已预警这是最难自己长出来的一块）。

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| X1 | **副作用提交顺序** | ① **意图事件 durable 落盘**（`tool.call.intent`：tool_call_id / 参数 / idempotent / replay 标记 / 外部可核验锚点如 commit sha）→ ② 执行 → ③ **结果事件 durable 落盘**（`tool.call.completed` / `.failed`）。崩溃后「有意图无结果」= **未知结果**，**不盲目重跑**。落地：三段写路径——intent（含 idempotent/replay/anchors/policy_snapshot）durable 落盘 → 执行 → completed/failed；intent 落盘失败则拒绝执行；"有意图无结果"= in-flight 未知结果交 resume 按 X2 处置（实现：core/loop/toolcall.go） **补注（2026-01-30 v3-S5）**：审计留痕与执行守约分离——Run ctx 已死时审计事件（intent/failed/loop.error）走 settle ctx（WithoutCancel+5s）落盘留痕，执行/派生一律显式查 ctx.Err() fail-close（工具执行与 spawn 均有护栏+钉子）；审批超时=deny 且事件流真留痕 | ✅ |
| X2 | **工具重放语义** | 两个字段合一套体系：`idempotent: bool`（能否自动重试：429/5xx 类抖动）+ `replay: "safe" \| "verify" \| "unsafe"`（崩溃续跑时能否重放）。**safe**=重复执行无副作用；**verify**=可重放但须先核验外部状态（git push → 先看远端有没有该 commit）；**unsafe**=不可重放（发邮件、删除）→ 未知结果必须由 agent 或人工判定。落地：`idempotent` + `replay: safe/verify/unsafe` 两字段在 Tool 契约与 intent；resume 按三类处置（safe 自动重放 / verify 先核验外部状态[Verifier 接口 + git anchor 核验器]再决定 / unsafe 转人工），未知 replay 归一为 unsafe（实现：core/durable + core/loop/replay.go） | ✅ |
| X3 | **checkpoint 内容** | 四项：① 事件流 **seq 水位** ② **ledger 快照版本**（ledger 事件 seq）③ **in-flight 清单**（intent 已落 / result 未落的操作 id）④ **会话与 agent 树的状态投影版本**。checkpoint 本身也是一条 durable 事件（`checkpoint.created`），内容是上述四项的引用。落地：`checkpoint.created` durable 事件含四项（`seq_watermark`/`ledger_seq`/`in_flight`/`tree_version`），本身是 durable 事件，内容存引用不存全量副本（实现：core/durable/checkpoint.go） | ✅ |
| X4 | **resume 流程** | ① 读最后一个 checkpoint ② **fold 事件到水位**重建状态 ③ **处置 in-flight**：按 X2 的 replay 语义分三类（safe 自动重放 / verify 先核验 / unsafe 转人工或 agent 判定）④ **恢复 agent 树**：已完成的 agent 不重启，未完成的从其最后一个 turn 继续 ⑤ 写入 `session.resumed` 事件后继续跑。落地：`durable.Resume`——读最后 checkpoint（无则全量重建）→ fold 重建 ledger/树/ownership → 按 X2 三类处置 in-flight → fold 出 agent 树（running 节点交调用方 loop 续跑 turn）→ 落 `session.resumed`；处置前原子认领防并发重放、连跑两次幂等（实现：core/durable/resume.go） | ✅ |
| X5 | **ownership / 幂等键** | 每次 spawn 产生一条 ownership 记录 `{parent_agent_id, spawn_request_id, child_agent_id}`；**续跑时按 `(parent_agent_id, spawn_request_id)` 找回同一个 child，不新建**。需要新增 id 命名空间 `spawn_request_id`（ULID）。落地：ownership 记录落地——`agent.spawned` 事件带 `spawn_request_id`（ULID），`agents.Tree.Spawn` 内存按 (parent, request_id) 幂等返回同一 child，`durable` 从事件流 fold 出 ownership 供续跑 `ResolveChild` 找回同一 child 不新建（换类型报冲突）（实现：core/agents/tree.go + core/durable/ownership.go） | ✅ |
| X6 | **ledger 写放大控制** | **关键字段立即落盘**（目标 / 约束变更 → 崩溃不丢）；**过程性字段 turn 末合并**（已完成项 / 待解项 → 同一 turn 内多次更新合并为一条 `ledger.updated`）。落地：`durable.LedgerWriter`——`UpdateCritical`（目标/约束）立即落一条 ledger.updated（崩溃不丢关键字段），`UpdateProcedural`（已完成/待解）只改内存缓冲、`Flush`（turn 末）合并为一条；每条仍是全文快照（LWW 语义不变），只控制写频率（实现：core/durable/ledger.go） | ✅ |


## P. 工程与交付

| ID | 决策项 | 我的主张 | 状态 |
|---|---|---|---|
| P1 | 仓库结构 | ✅ **monorepo 单仓多包**：主模块（contracts / core / runtime / host / ui）+ **extensions 各自独立 go.mod**（可独立发版）。通用组件（agentlink / gate / retry）放 `core/` 子包**且对外可导出**（不能放 internal）；`workflows/` 独立目录放数据 | 多仓 | ✅ |
| P2 | 版本演进 | 契约版本与实现版本独立演进 | · |
| P3 | 开源 | ✅ **先私有开发、契约冻结后再公开**（API 稳定前开源会让 module path / 目录 / 契约的改动成本立刻升高）。最终是否公开取决于商业意图 | 内部闭源 | ✅ |
| P4 | 模块路径 / 仓库名 / 命令名 | ✅ **产品名 `alasol`**（拉丁 ala「翼」+ solum「地基」半段拼接；2026-10-27 经 gh 仓库名/用户名双查定案）；**module path = `github.com/Yorha9e/alasol`**，仓库名 `alasol`；**命令名 `alasol`**（原建议 `hx` 作废：与 Helix 编辑器二进制同名、PATH 冲突；「harness」作为品类词保留在文档里） | — | ✅ |
| P5 | 许可证 | ✅ **Apache-2.0**（有明确的专利授权与不主张条款——我们要集成大量第三方模型 SDK 与协议实现，MIT 无此条款；兼容性等同 MIT）。依赖许可核对：pi=MIT、MCP/ACP=Apache-2.0 → 全部兼容。**不选 BUSL**（带商业限制，与要生态的定位冲突） | MIT | ✅ |
| P6 | CI | schema 校验 + 酸测试矩阵（每种扩展点换实现） | · |

---

## 需要你定的条目汇总（★）

> **本表由各区自动汇总**（D21 纪律：数字与清单只在源头定义一次）。改各区的状态后请重跑汇总脚本。**脚本不在仓内**（全仓检索无）；本表为 2026-10-28 v0 回填后**手工生成**（按各区剩余 ★ 逐区重排）。

| ID | 决策项 | 状态 |
|---|---|---|
| E2 | 分配 | ★ |
| G2d | 向量引擎 | ★ |
| G2e | blob 引擎 | ★ |
| H14 | env 分层 | ★ |
| H20 | 注入作用域 | ★ |
| H22 | 权限语义（工作流侧） | ★ |
| H23 | 命中即放行（工作流侧） | ★ |
| K19 | 跨机 | ★ |
| K20 | 层级 3 = 只读型 | ★ |
| M6 | 通用重试组件 | ★ |
| N4b | 公网暴露 / 远程访问 | ★ |
| N5 | 客户端鉴权与降权 | ★ |
| R8 | 能力池叠加 | ★ |
| W5 | 子代理类型 | ★ |
| W6 | 上下文策略 | ★ |
| W7 | 验收目标与部件 | ★ |
| W9b | 门禁是轻量的 | ★ |
| W11 | 动态性 | ★ |
| W12 | 归属 | ★ |
| W13 | 契约形态 | ★ |
| W15 | 预算策略 | ★ |
| W18 | 哲学分类 | ★ |

**合计**：✅ 已定 **175** 条 · ★ 待议 **22** 条 · · 默认通过（我的主张，未逐条确认）**83** 条 · 共 **280** 条

> 2026-10-28 v0 回填（施工 session，§4.5.2）：★ 108 → **64**（44 条转 ✅；其余 ★ 已补版本归属/出入注记，见各区主张列）。✅ 82 → **126**，· 仍 **90**，总数 **280** 不变（无新增/删除条目，编号不复用）。**汇总脚本不在仓内**（全仓检索无），本表按各区剩余 ★ 手工重排——后人若找到脚本请补回。2026-10-28 议程拍板：★ 64 → 56（S13/S14/K14/K15/K29/Q5/H15/J21 转 ✅）。2026-10-28 v1-S1 落地回填：**F1 ·→✅**（user.prompt/model.turn 入流补缺口），✅ 134 → **135**、· 90 → **89**；S3/S13/S14/J21 补落地注记（S13 的 EvalSymlinks 字面在 Windows 不解析 junction，落地为逐组件 Readlink，语义等同——详见行内）。2026-10-28 v1-S2 落地回填：**K10/K12 ★→✅**（发现表落 core/discovery——原名 Registry 与扩展注册表撞名；gate.joined + 活成员语义），✅ 135 → **137**、★ 56 → **54**；K11 补落地注记；`contracts/INVENTORY.md` §2 kind 表补全至 x-kind-payload 全集（16 项，此前 v0 各期漏账）。2026-10-28 v1-S3 落地回填：**K18/Q6 ★→✅**（agentlink 五件套 + Inquiry 摘要/窗口/refs 核验），✅ 137 → **139**、★ 54 → **52**；subtree 口径收窄为「目标子树 ∩ 一跳可见集」（K27 逐跳，详见 K18 行内）；INVENTORY §2 +agentlink.broadcast/agentlink.handoff（共 18 项）。2026-10-28 v1-S4 落地回填：**Q4 ★→✅**（在线表恒在 prompt 末尾），✅ 139 → **140**、★ 52 → **51**；K14/K15 补落地注记；S15 补短 id 尾段注（前缀同毫秒碰撞）。评审循环 2 轮（p2 短 id 碰撞 + 3 p3 → clean）；修复由主 agent 亲手完成并自证（reviewer 模型账户余额耗尽 402 期间），critic 复查确认 clean。2026-10-28 v1-S5 落地回填：**K9 ★→✅、I5/B2 ·→✅**（hub 监控三档告警、模型目录、bubbletea TUI 选型），✅ 140 → **143**、★ 51 → **50**、· 89 → **87**；H15/J21/O6 补落地注记（TOML env>文件、cost_tokens 成本统计、TUI 三面）。**v1 五段全部收口**。评审循环 2 轮（p2 worktree 文档言过其实 + p3 model 断言缺口 → 修复落盘后主 agent 变异自证，critic 复查轮因 402 未跑完——如实挂账）。**S5 挂账后续项**：worktree 工具级隔离（工具仍跑进程 cwd，改 core/loop 执行上下文与 X1 冲突）、K9 告警入事件流、worktree 登记表持久化、TUI 自动化交互测试 2026-10-28 v2/v3 议程拍板：25 条 ★→✅（J9/J10/J17/J18/J20 压缩两级锚定 L1≤10%/L2≤1% + 级联 C 案；G1/G2/G2f/G18-G21 图投影简版；V4/V6/V7 门禁增强；K8 沙箱务实组合+埋点；B4 保留不选型；N1 MCP client 先；W1/W2/W3/W4/W14/W19/W21 工作流骨架非强制）+ H9/I4 两条 ·→✅（I4 降级随压缩自然可得），✅ 143→**170**、★ 50→**25**、· 87→**85**；串台重标注（H14/H20→v2、K20/R8→v3、Q1-Q3 剩余随压缩）；K29 补 /btw UX（不回灌+生成新 session 预埋）；B3 补第三方 UI 外包验收。2026-01-29 v2-S1 落地回填（深度压缩）：**F6 ·→✅、Q1/Q2/Q3 ★→✅**（历史不可重写经压缩验证；注入源/稳定性分级/编排时机随深度压缩落定），✅ 170→**174**、★ 25→**22**、· 85→**84**；J8/J9/J10/J11/J13/J15/J17/J18/J20/J21/E2 补落地注记（context_strategy 预备单位+两级压缩+recall+缓存定律，core/compaction）；**J19 挂账**：durable.LedgerWriter 无调用方（账本恒空，级联以空账本降级）。评审循环 critic 4 轮 / reviewer 3 轮（探查→修正→复查→终验 clean，41 条问题闭环，其中台账回填 1 条归主 agent）。2026-01-29 v2-S2 落地回填（图投影简版）：G18/G19/G20/G21/G2f/G22/G17/G1/G2/W16 补落地注记（core/graph+runtime/graph+索引器建边+change_impact 报告），无状态翻转（✅ 174/★ 22/· 84 不变）；评审循环 critic 3 轮/reviewer 3 轮（1 p1+4 p2+24 p3 闭环，终验 clean）；**挂账留 v3**：N1 邻居截断信号、N5 Tx 类型化收尾、N2 重建窗口口径（喂 escalate 前必补）。2026-01-29 v2-S3 落地回填（门禁增强）：V4/V6/V7 补落地注记（内容寻址缓存+review-tip 失效+冲突预测），无状态翻转（✅ 174/★ 22/· 84 不变）；评审循环 critic 3 轮/reviewer 3 轮（1 p1+5 p2+13 p3 闭环，终验 clean）；**挂账留 v3**：V6/V7 无生产发射方（合并门禁接线）、V4 命中语义归命令侧。2026-01-29 v2-S4 落地回填（沙箱）：H9/K8/V8/V10 补落地注记（runtime/cap/sandbox：job object+受限 token+目录 ACL+WER+埋点接口），无状态翻转（✅ 174/★ 22/· 84 不变）；评审循环 critic 3 轮/reviewer 3 轮（3 p1+9 p2+18 p3 闭环，终验 clean）；**已知边界三条**（默认模式 BUILTIN\Users 授权位置可写/读、strict 模式代价 overlapped I/O、允许根 DACL 追加传播不还原）；Net/Proc 维度与 DACL 还原选项留挂账。2026-01-29 v2-S5 落地回填（存储收口）：G1/G2 收口 + G2d/G2e 接口先行（core/storage 五族可组合接口 + 五族双实现酸测试）；挂账清理 ②③⑤⑥（hub.alert 入流/worktree Rebuild/models 读 TOML/agentlink 空格路径判据），①④ 如实留挂（涉 X1 的 per-session 执行上下文、TUI 真 TTY）；评审循环 critic 3 轮/reviewer 3 轮（6 p2+14 p3 闭环，终验 clean）。2026-01-29 v3-S1 落地回填（MCP client）：N1 补落地注记、**N2 ·→✅**（不广告+拒+留痕），✅ 174→**175**、· 84→**83**；评审循环 critic 3 轮/reviewer 3 轮（1 p1+7 p2+6 p3 闭环，6 项挂账转 v3-S2，终验 clean）。2026-01-29 v3-S2 落地回填（fork+/btw）：K29 补落地注记（含 v3-S1 挂账 5 项一并清：工具列表快照/stderr 条数闸/装配预算口径/关停文案/tab 变体），无状态翻转（✅ 175/★ 22/· 83 不变）；评审循环 critic 3 轮/reviewer 3 轮（2 p2+16 p3 全闭环，终验 clean）。2026-01-29 v3-S3 落地回填（工作流引擎+tower 验证+v2 挂账清）：W1/W2/W3/W4/W14/W19/W21 补落地注记（core/workflow 能力包+tower 首包+扩展点仅两处）；V6/V7/J19/G21/J11 挂账清（合并门禁/冲突预测/账本生产者/重建窗口防护/硬边界接线），无状态翻转（✅ 175/★ 22/· 83 不变）；评审循环 critic 2 轮/reviewer 2 轮（1 p1+3 p2+7 p3 闭环，终验 clean）；**402 余额断供如实挂账**：reviewer/critic 通道间歇打死（code-reviewer/code-critic/critic 三通道），修复与终验换 code-coder 通道完成并变异自证。2026-01-30 v3-S4 落地回填（协议面补全，来源：第三方 GUI 验收 ho_a83f7df3ef9e）：G1 对话/审批四面 + G3 回放 + G4 hub.alert 过 bus + G6/G7/G8/G9 修复、G2/G5/A1/A3/A5 口径文档化，D14/H8/N4/F8/G25 补注记，无状态翻转（✅ 175/★ 22/· 83 不变）；评审循环 critic 2 轮/reviewer 2 轮（4 p2+12 p3 闭环 + 07 回填 1 件归主 agent，终验 clean）；**记 v4**：POST /api/sessions 开新会话入口、审批应答鉴权收紧、跨进程托管注册。2026-01-30 v3-S5 落地回填（框架整备+UI 联调）：POST /api/sessions 开新会话 REST 入口（从 v4 提前）；真机联调全通（真模型 gpt-4o-mini：开 session→prompt→三面板→审批 allow/deny 闭环→merge V6→回放）；联调暴露并修复"审批超时审计断链"（settle ctx 纪律）+ 取消护栏回退（执行/spawn 前显式 ctx 检查），评审 2 轮至 clean；无状态翻转（✅ 175/★ 22/· 83 不变）

---

## 相关文档

- `00-overview.md`（横切面缺口）· `01-permissions-and-sandbox.md`（权限三层）· `05-architecture-pivot.md`（语言定案）
- `06-skeleton-design.md`（骨架规格）· `08-identity-model.md`（ID 设计）· `09-package-boundaries.md`（包边界）
- `10-design-review-report.md`（外部审阅）· `11-construction-plan.md`（施工计划）
