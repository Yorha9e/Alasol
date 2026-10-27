# 10 · 设计审阅报告（包边界 + 130 条决策 + 盲区）

> 审阅请求：`ho_4d5ee65b2192`（C1 包边界 / C2 整体一致性 / C3 盲区 + D1–D10 + 交付四件套）
> 审阅对象：`00`–`09` 全部 10 份文档；spike 结论按 G2b/G14/G16 采信，未复跑。
> 分档：**阻断级**（施工期必返工或埋结构性错误）/ **应修**（可带病开工，v0 内消化）/ **建议**（不改也能活）。
> 本报告只做判断，不含实现。D 组（你们自己的怀疑点）之外的部分是审阅重点。

---

## 0. 总评

骨架方向成立且明显优于 omkc 现状：contract-first、一切皆事件、接口由消费方定义、权限三层 fail-close、可见性双维——这些是对的，不用推翻。但收敛出的 130 条决策里有 **4 处结构性问题**：

1. **状态源三分**（F5 / G8 / K22 互斥）——事件流唯一事实源的承诺在 goal ledger 和黑板上失守；
2. **durable 崩溃恢复语义整块缺席**——"长时程自主"的承重墙没有决策（00 §1C 自己列为必须现在定）；
3. **垂直可见性口径自相矛盾**（K11 同一格两种说法）；
4. **包边界一处真实错误**（门禁落点 = 你们的 D4，实锤成立），外加会话隔离默认值两份文档打架（K1 vs 05）。

另一个危险信号：跨边界契约的计数在四份文档里是 7 / 8 / 9 三个数，且"契约清单"本体缺项——**"决策总表 + 契约清单"已经不是唯一真相源**，这在 contract-first 纪律里是坏味道。

三档统计：**阻断级 7 · 应修 16 · 建议 9**。

---

## 1. 阻断级（P0）

### P0-1 状态源矛盾：F5「不另设可变状态源」 vs G8「Document 接口 CAS」 vs K22「黑板 LWW」

- **依据**：F5（"状态来源由事件流 fold 得出，不另设可变状态源"）；G8 ✅（goal ledger "落在 Document 接口（结构化、需要 CAS 更新、量级小）"）；K22（共享黑板：键级 last-write-wins + 墓碑删除）；06 §7.1（持久投影含 ledger，"维护者：索引器（单写者消费事件流）"）。
- **问题**：四条对"ledger/黑板算不算状态源"给出互斥答案。06 §7.1 说 ledger 当前态是索引器从事件流物化的投影；G8 说它是 CAS 直写的 Document（可变存储）；F5 说事件流之外不存在可变状态源；K22 黑板同样游离在外。
- **建议改法**：**统一成事件化**——`ledger.updated` / `board.written` 作为 durable 事件追加（用事件版本号实现 CAS 语义），"当前态"一律由索引器物化为投影（与 06 §7.1 对齐），F5 原文成立。若坚持 Document 直写，必须把 F5 改口为"**核心执行状态**不另设可变源"，并明示 ledger/黑板不参与回放一致性——不推荐。
- **不改会怎样**：v0 验收"事件流能完整回放一场 session"、J18 方案 C（ledger 切断压缩级联）全部建立在"事件流是唯一事实源"上。状态源三分后，崩溃恢复时"事件流说的"与"Document 里说的"必然漂移——而 goal ledger 恰恰是你们根治目标偏移的承重墙。

### P0-2 durable 崩溃恢复语义整块缺席

- **依据**：00 §1C（checkpoint / resume / replay、"工具副作用必须先落盘再执行"、watchdog 列为**必须现在定**）；03 §3（P2 durable 语义："提交先于可见、崩溃续跑、tool 级 replay: safe、ownership index"，标注"整个项目最难自己长出来的部分"）；而 07 决策表里 M2 只有取消分层、W17 只有 escalate 的 checkpoint 挂起——**没有任何一条决策定义 checkpoint 内容、resume 流程、副作用落盘顺序、续跑防重复**。
- **问题**：长时程自主是产品存在理由，"进程半夜挂掉后从哪续"却没有决策。F6/G11 只覆盖事件侧；工具副作用侧（执行到一半的 bash、写了一半的文件、已 push 的 commit）完全空白。
- **建议改法**：新增一节决策（建议 07 新开 X 区或并入 M 区）：
  1. 副作用提交顺序：**意图事件 durable 落盘 → 执行 → 结果事件落盘**；崩溃后"有意图无结果"判定为**未知结果**，不盲目重跑；
  2. 工具 `replay` 语义（对照 pi-durable `replay: "safe"`），与 M7 `idempotent` 合并成一个字段体系（见 D22）；
  3. checkpoint = 事件流 seq 水位 + ledger 快照版本；
  4. resume = fold 到水位 + 处置 in-flight 清单（逐个按"未知结果"语义处理）；
  5. ownership/幂等键（对照 pi-durable ownership index），防止续跑重复 spawn / 重复副作用。
- **不改会怎样**：v0"真机最小回路"在第一次 `kill -9` 后原形毕露；每个工具各自发明恢复语义，审计链与重放断裂。这是 Go 路线下最贵的一块自研（03 已预警），现在不设计，施工时只能边写边发明。

### P0-3 垂直可见性口径自相矛盾（K11）

- **依据**：K11 同一格内："祖先 ↔ 后代可见——每个 agent 可见自己的全部**直系**子 agent + 自己的**父** agent"；06 §7.1："祖先 ↔ 后代可见"（全祖先/全后代）；handoff B#5 用的也是 06 口径。
- **问题**：祖父能否看见孙子？两种口径两个答案。若只可见父+子，则"祖先↔后代可见"是错误表述，孙子卡死只能靠父级 summary 冒泡（K23）间接暴露——恰是 K16 自己批评的 summary 链失真路径；若全可见，则 Inquiry/在线表成本模型（K15）要按整棵子树算。
- **建议改法**：二选一写死。**推荐"父 ↔ 子直系可见 + Inquiry 沿父链逐级上查（每跳一次显式调用）"**：孤儿防护（K11 的初衷）靠"父可见子 + 异常冒泡（K23）"已经闭环；全祖先可见省一跳，代价是可见性矩阵、审计面、在线表成本全面变大。同步修订 06 §7.1、K11、handoff B#5 三处。
- **不改会怎样**：agentlink 的 Discover/Inquiry、escalate 链、审计追溯会各按各的理解实现，联调时对不上。

### P0-4 包边界真实错误：门禁/重试/能力接口的落点（D4，实锤）

- **依据**：09 §1 core 行："loop、事件总线、策略链、上下文装配、扩展注册、发现表、**门禁、重试**；**不含任何 I/O 实现**"；W7："异步任务类（CI、验收、构建）→ **进程级组件**，可热插拔"；E8（gate 是通用组件，放 core/ 还是独立包待定）；W8/V1（快照 worktree + 执行不透明命令）。
- **问题**：09 的 core 一句话内部就矛盾——门禁要跑真实命令（V1/V2），怎么"不含任何 I/O 实现"？三份文档给出三个落点（core 一部分 / 进程级组件 / 待定通用组件区）。同类未声明的还有两处：① extensions（builtin.tools 的 bash）需要 exec/fs 能力，**能力接口**（core 定义、runtime 实现）在 09 里没有家；② 进程级扩展的**加载器**要起进程（属 runtime），但注册表在 core。
- **建议改法**：09 core 一句话改为"……扩展注册、发现表、**门禁判定**、**重试控制流**；只定义能力接口（exec/fs/net/proc），不含任何 I/O 实现"。门禁的**执行**（快照 worktree、命令运行）落 runtime 或进程级扩展（与 W7 对齐）；receipt 四要素校验（V2）是纯判定，留 core。E8 的 gate 拆两半：判定在 core（或独立顶层包），执行器在 runtime/extensions。
- **不改会怎样**：酸测试"core 无 runtime 跑完单测"（09 全局检查第 2 条）直接失败；施工时要么 core import runtime（边界消失），要么把 receipt 判定复制到 runtime（两份判定，必然漂移）。

### P0-5 Tool 契约字段缺位：G22 的 graph_edges 与 M7 的 idempotent 都不在 D12

- **依据**：D12（Tool = `schema / side_effect / cancelable / approvable`）；06 §3 契约 5（加"执行器"）；G22 ✅（"工具返回里带 `graph_edges: [{src, dst, kind}]`"）；M7（"工具契约加 `idempotent: bool`"）。
- **问题**：G21 说图是 change_impact 的底座、change_impact 是 W16 闸门的客观依据——"三者是一条链"，而整条链的**入口字段**（graph_edges）不在 Tool 契约里。M7 的幂等字段同样。两个决策各自"顺手"改契约，契约清单没有跟上。
- **建议改法**：D12 扩为 `schema / side_effect / cancelable / approvable / idempotent / replay / returns.graph_edges`；**工具返回值契约**单独列入 contracts 清单（它跨边界：MCP/进程级扩展都要传）。
- **不改会怎样**：change_impact 链在第一环断掉；重试安全性（M7）没有契约载体，退化为口头约定。

### P0-6 工作流规则不在权限优先级链（H5 缺一环）

- **依据**：H5（managed > CLI > project > local > user > session）；06 §4.1 workflow JSON 的 `gates[].rules`（`deny:write(**/*.lock)` 等）与 `grants[]`；H21/H22（实际 = 请求 ∩ 策略上限 ∩ 父级授予）。
- **问题**：workflow 是用户/agent 可写的跨边界数据（W13），能携带策略规则与授权，但这些规则在 H5 六层优先级里没有位置。它与 project/local 规则谁赢？能不能用 deny 顶掉 managed？H21 只管"授予"不管"规则"。
- **建议改法**：H5 插入一层，建议：managed > CLI > project > **workflow** > local > user > session，且 workflow 规则**只允许收窄、不允许放宽**（与 H11 同构）；明确 `gates[].kind:"policy"` 的规则进 H2 求值链的哪个位置。
- **不改会怎样**：权限模型出现一条未审计的注入通道——一份 workflow 文件就能改写屏障行为，与 fail-close 承诺冲突。

### P0-7 会话隔离默认值两份文档打架（K1 vs 05 §4.2）

- **依据**：K1（"✅ 默认 hub 内 goroutine……**不默认独立进程**"，状态栏又标 ★）；05 §4.2（"建议**默认独立进程可选、按需启用 goroutine 模式（安全默认优先）**"）。
- **问题**：一个"goroutine 默认、进程按需"，一个"进程默认、goroutine 按需"。K8/K9（ExecutionEnv 抽象、goroutine 代价对冲）全部按 K1 假设设计；05 的建议若成立，K9 的对冲清单优先级骤降、K8 的 v0 实现换位。
- **建议改法**：采纳 K1（goroutine 默认）并把 K1 的 ★ 拍掉、05 §4.2 加"已被 K1 取代"标记。理由：subagent 是自 spawn 的自家代码（K1 原话），OS 级隔离是工具/ExecutionEnv 的事；v0 无沙箱时进程隔离也拦不住写盘（E 区已认），"安全默认"名不副实。若坚持 05，则 K8 v0 默认 `SandboxedExecutionEnv`，K9 降级，成本另算。
- **不改会怎样**：K8/K9 的设计前提悬空，进程模型实现到一半返工。

---

## 2. 应修（P1）

- **P1-1 跨边界契约计数与清单失同步**（D21）：06 §3.4 说"7 个"、§3.2 表列 8 个、§4.2 说"8 → 9"（加 ContextSnapshot）、D1 与 handoff B#2 仍说 8 个；06 §3 的 12 条契约清单里**没有 Workflow、没有 ContextSnapshot**。→ contracts 目录维护唯一清单（含 Workflow、ContextSnapshot、工具返回值），其余位置一律引用不重抄；计数由清单生成。
- **P1-2 durable=false 流事件语义未定**（F8，顺手把 D8 ★ 拍掉）：model.delta "高频、可丢、只给 UI"但"同一条流"——拿不拿 seq？进不进 SQLite？索引器消费吗？→ 建议：非 durable 事件**不入库、不占 seq**（总线直发订阅者），回放对象只有事实事件；一句话即可消除大半歧义。
- **P1-3 压缩触发未计入 prompt cache 成本**（J20）：Q4 说在线表放末尾以保前缀缓存；但压缩重写历史 = 前缀缓存全失效。50% 触发（J13）可能比"扛到溢出再压"更贵。且 Q1"编排最后做"不成立——prompt 排布被压缩策略（重写哪段）反向依赖。→ 压缩触发决策输入加"缓存失效成本"；Q1 改为"与压缩策略联动设计"。另：J5（三水位）与 J13（50% 单阈值）两套触发模型并存未分场合，合并定口径。
- **P1-4 tiered 百分比绑定模型窗口**（J21）：J9"压缩等级 = 占用窗口百分比档位"——哪个窗口？I6 模型 per-agent 绑定、可换；同一上下文在 200k/128k 窗口下档位漂移。→ 档位锚定"装配时生效模型的窗口"，换模型时重算并记事件。
- **P1-5 token 计量口径未定**（J21 附）：ContextSnapshot 的 tokens_used 是自算还是 provider 报告（各家 tokenizer 不同）？→ 声明"provider usage 事件优先，自算兜底"。
- **P1-6 summary 污点不继承**（Q7）：H13 要求工具输出标不可信来源；K24 说 summary 是 subagent 回主 agent 的**唯一通道**。被注入的 subagent 的 summary 就是攻击载荷出口，但 summary 契约（结构化 + refs）没有污点字段。→ summary 强制带 `taint`（继承来源事件的最高污点级），主 agent 对 tainted summary 按不可信数据处理。
- **P1-7 契约演进策略缺席**（F7）：D7 只管"未知 kind 宽容"；已知 kind 的字段删除/改义怎么兼容？事件 envelope（F3）无版本字段，外部消费者会碎。→ 铁律"只增不删不改义"+ `x-deprecated` 标记；envelope 加 `v`。
- **P1-8 索引器恢复语义与水位**（G24，即 D5 的落决策）：G17 ★ 只说"唯一写派生表"。补四条：① 持久化"已消费至 seq N"，崩溃续跑；② 重建 = 清空派生表重放；③ UI 显示"索引已追至 seq N"**水位**（最终一致要可见，不能静默旧）；④ 单例归属：随 hub 进程（K1 goroutine 模式下天然单例）。索引器本身**不是**多写者。
- **P1-9 Policy 规则表达式语言未定义**：H2/01 §3.1 只有语法素描（`Bash(git push *)`）；参数匹配（glob？前缀？正则？）是跨边界契约（D1 已列"Policy 规则表达式"）但无决策。→ v0 定死"工具名 + 参数 glob + 路径段向后匹配"（01 §3.2 路径匹配的教训）；别开放正则（ReDoS + 不可分析）。
- **P1-10 06 §7.1 深度强制方式过时**：06 写"depth 标签 + 数字检查"，K16 定"类型可达性为主、数字兜底"。→ 06 §7.1 改为引用 K16/K17，删除旧表述，防实现者抄错。
- **P1-11 W15"预算不设上限" vs 05"token 预算、配额"**：05 §2.1 hub 职责含"背压与配额：并发上限、token 预算"；W15 说默认不设上限。→ 区分**并发/资源配额**（K7，保留）与**任务预算**（W15，不设），一句话写进 W15。
- **P1-12 turn_id/step_id 与事件的关系未写**（S2 vs F3）：事件字段里没有它们；"第 N 轮"用 `(session_id, seq)` 索引（G10）——turn 边界事件是什么？→ `turn.started`/`turn.finished` 事件携带 turn_id，step 同理；否则"第 N 轮"永远靠启发式。
- **P1-13 模型目录（I5）归属空洞**：静态快照 + 在线拉取 + 刷新——放哪？→ 快照数据进 contracts/内置资源，拉取刷新进 runtime，目录查询接口在 core（I7 RoutingPolicy 的输入）。
- **P1-14 审批 UI 的落点在依赖规则下无解**：Approver 是进程内接口（06 §3.2），TUI 审批在 extensions/builtin.approver（06 §6），但渲染在 ui/tui——extensions 不许 import ui（09 规则 5），ui 不 import core。→ 明确"Approver 的 UI 实现 = 走 Transport 的桥接扩展"，ApprovalRequest/Response 升为跨边界消息（契约数 +1，并入 P1-1 清单）。
- **P1-15 06 §8-5 与 G2b 矛盾**：06 §8 待拍板 5 说"存储引擎首选 `modernc.org/sqlite`"，G2b ✅ 定案是 `ncruces/go-sqlite3`（modernc 为备选）。→ 06 §8 标注"已由 G2b 取代"。
- **P1-16 06 §8-2 与 E1/E2 矛盾**：06 §8-2 说"v0 只做进程级 + 声明式配置"，E1（★）说"A 接口实现为主 + B 进程级为边界"、E2 把 12 个扩展点分给 A 类。→ E1 拍板时二选一，并同步 06 §8-2。注意与 P0-4 联动：A 类接口实现模式下，能力接口的家必须先定。

---

## 3. 建议（P2）

- **P2-1 N5 编号重复**（N6）：07 N 区有两个 N5（客户端鉴权 / NDJSON 惯例），后者重编为 N6。
- **P2-2 08 命名空间表补全**（S16）：`causation_id`（F3）、`gate_id`（K12）、`workflow:<run_id>`、`sum:` 引用（ContextSnapshot）、`parent_agent_id`（K26）都不在 08 的命名空间表里。08 是最强的文档之一，别让边角字段漏网。
- **P2-3 V9"可搬运资产"与 A2"全部不复用"的语义对齐**：V9 列的"CI run 记录 schema、guard 形态"像代码级复用。明确"搬运 = 语义参照、产物重写"还是"允许抄 schema"。
- **P2-4 G19 内存预算再评估 + 图降级声明**：10 万节点/100 万边邻接表 100–200MB 常驻在 hub 多 session 下奢侈，LRU/冷加载应是默认。G2f（图不进 core 必经路径）与 G21（图→change_impact→escalate）之间补一句降级语义：图缺席时 W16 闸门退化为 diff 规模启发式。
- **P2-5 依赖图补装配关系**：09 §2 没画谁实例化 TUI（cmd→ui? host→ui?）、host→runtime 的箭头含糊。补两行。
- **P2-6 fork/clone 复制语义**（K29）：S10 只说 fork 生成新 id + parent_id；agent 树、ledger、gate 成员、worktree 怎么复制/不复制没写。
- **P2-7 计数口径统一**：06 §3.2"进程内 8 个"实列 9 项、E2"A 类 12 个"、扩展点 16 个——各表注明口径。
- **P2-8 长工具 TOCTOU 声明边界**（M9）：策略 T0 判定、副作用持续到 Tn。至少把"判定快照"记进调用事件，并声明"判定只约束发起时刻"为已知边界（与 V8 同款诚实处理）。
- **P2-9 ledger 写者与并发语义**：谁可写 ledger（只主 agent？全部 agent？）；K22 黑板有 LWW，ledger 的冲突语义补一句。

---

## 4. C1 包边界逐包核对

### 4.1 三列核对

| 包 | 三列是否成立 | 备注 |
|---|---|---|
| contracts | ✅ | 清单失同步（P1-1）；缺演进策略（P1-7） |
| core | ⚠️ | 一句话自相矛盾（P0-4）；能力接口无家 |
| runtime | ✅ | 建议把索引器归属写进本行 |
| host | ✅ | 依赖图漏 host→runtime、cmd→ui（P2-5） |
| ui/tui | ✅ | 审批 UI 落点无解（P1-14） |
| extensions/* | ✅ | 依赖的能力接口不在任何包（P0-4） |
| workflows/ | ⚠️ | 定位对（数据），但 code/data 分界（MilestoneHandler 归 extensions）与规则进链（P0-6）没写 |
| sdk / cmd / internal | ✅ | sdk 待决合理 |

### 4.2 假想改动抽查（验证第三列）

| 假想改动 | 应落包 | 第三列承诺"不动"的包 | 判定 |
|---|---|---|---|
| 加一个网络抓取工具 | extensions + core 加 net 能力接口 | core 声称"加一个工具"不动 | ❌ 需新能力接口时 core 要动 → 第三列应加注"已声明能力内的工具" |
| 压缩换滑动窗口 | extensions.Compactor | core 只因"触发条件"动 | ✅ |
| Tool 契约加 graph_edges | contracts | — | ✅（但被 G22 隐含要求，P0-5） |
| 开 MCP server 模式 | **无包认领** | — | ❌ 归属空洞（N1 未定） |
| 索引器重建 | runtime | core 不动 | ✅（需 P1-8 支撑） |
| 审批改远程手机端 | **无包认领** | ui"不判断语义"不动 | ❌ 落点空洞（P1-14） |
| SQLite 换 bbolt | runtime | core/contracts 不动 | ✅（G2b"不泄漏 SQLite 特性"约束下） |

### 4.3 五条全局检查

1. **依赖图有环吗？** 无 import 环（规则 1–6 自洽）。但存在**规则无法表达的合法装配**：TUI 审批（extensions→ui？）、进程级扩展加载（core 注册表 → runtime 进程管理）——按现行规则都违规，只能靠宿主注入绕行。图没环，图盖不住真实装配。
2. **core 无 runtime 跑单测？** 现状**不成立**（门禁执行 + 能力接口缺失）；按 P0-4 改造后成立。
3. **只靠 contracts + JSON Schema 消费事件流？** **基本成立**。补：payload 子 schema 全导出、演进策略（P1-7）、binary_ref 取法。注意 Tool.schema 的值本身是 JSON Schema（元嵌套）——codegen 受限子集应把它当原始 JSON 处理，写进 D2。
4. **extensions 单独发版不动 core？** 成立（前提：含能力接口的 core 接口稳定）。建议 Extension 元数据加 `api_version`。
5. **有空一句话吗？** 无。但 core 的一句话是病句（P0-4）；workflows 的一句话只说了"是数据"，没说代码部件在哪（W14 的 MilestoneHandler 归 extensions——写进 09）。

### 4.4 边界重叠与空洞

- **重叠**：门禁行为（core 判定 / runtime 执行 / workflows 数据 / extensions 组件）——联动正常，但判定与执行没切开就是重叠；工具能力（contracts 字段 / core 接口 / extensions 实现）。
- **空洞**（无包认领）：能力接口（exec/fs/net/proc）；模型目录；索引器角色；MCP 适配层；契约演进策略；审批桥接。

---

## 5. D1–D10 逐条结论

**D1 全局长事件流的规模化 —— 部分同意，可收敛为 3 条待补决策。**
G3/G5/G10 已给基础答案（全局 seq、分段偏移、`(session_id, seq)` 索引）。真正没答案的是：① durable=false 流事件与全局 seq 的关系（P1-2）；② 归档/保留（冷存）策略；③ UI 订阅扇出必须在 hub 侧做**服务端过滤**（按 session_id/gate 订阅），不能把全局流直推每个客户端。读放大靠 `(session_id, seq)` 索引可控。结论：担心成立，但不是架构级风险。

**D2 goal ledger 更新纪律 —— 同意，这是 D 组里最实的风险。**
只靠自觉必漏。建议三件套：① 框架在**固定机械点**（turn 结束 / checkpoint / escalate / 阶段切换）强制发起 ledger 更新（可拒绝，但要记事件）；② **陈旧检测**：装配时对比"ledger 版本 vs 其后事件数"，超阈值注入提醒；③ **一致性校验**：定期用轻量校验对照 ledger 与近期事件，偏差记事件。另补并发语义（P2-9）。

**D3 LLM 切阶段 —— 同意，缓解靠"机械锚点 + 框架算数"。**
阶段边界优先锚定机械标记（milestone/advance 事件、工具调用簇、时间窗），LLM 只在机械边界内细分；tier% 换算从 LLM 手里拿走（J11 的取放机制已经对了，框架算百分比、LLM 只报"压哪个区间到哪档"）；切错可修——合并/重切作为事件追加，不重写。另注意 D3×D6 叠加：多 agent 交错事件的阶段切分比单 agent 难一档。

**D4 core/runtime 边界 —— 推翻"可能"的措辞：这就是真实边界错误。**
见 P0-4。重试是纯控制流（core 没问题）；门禁拆判定（core）/执行（runtime）。你们把三份文档（09 / W7 / E8）的三个落点并存了。

**D5 索引器崩溃/落后 —— 同意，且"旧"必须可见。**
UI 看到的目录可以旧，但必须显示水位（"索引已追至 seq N"），不能静默。答案就是 G 区没写完的那几条：seq 检查点续跑、重建路径、水位、单例归属（P1-8）。索引器**不是**多写者——它是派生表的唯一写者，本身单实例。

**D6 因果顺序 —— 部分推翻：span 模型不必上，但有两个真风险你们没列。**
在 K1（hub 内 goroutine）前提下，同一 hub 的 append 提交序 ≈ 因果序，`causation_id` + 全局 seq 够用。但：① G9 明确支持**多写者跨进程**时，seq = 提交序 ≠ 因果序——需要装配时按 causation_id 做拓扑修正，或规定 **session 亲和写**（一个 session 的事件只由其宿主进程 append，写进 G9）；② 压缩/投影对多 agent 交错流切阶段（D3 叠加）才是真难点。span 模型留给遥测（L2），不上主链。

**D7 can_spawn 配错/环 —— 同意，补两点。**
① workflow 是用户/agent 可写数据，can_spawn 图来自**不可信配置**——环检测必须在 workflow **加载时**做（不只类型注册时）；② 每次 spawn 判定记事件（哪个闸门放行/拒绝），审计面才能回答"这棵树怎么长出来的"。数字兜底（K17 ①）保留，双保险正确。

**D8 v0 垂直切片 —— 建议顺序：② → ③ → ①（① 归 v1）。**
②（spawn + 结构化 summary）最便宜地验证差异化核心（多 agent + 上下文隔离器），不需要 hub；③（真实仓库改 bug + 过 CI 门禁）验证真机回路 + 门禁链——注意 W9c 说 baseline 是长时程自主的承重墙，**v0 完全不碰门禁会掏空 ③ 的意义**，建议把"最小 receipt 门禁"塞进 06 §5 的"做"清单；① 依赖 hub/多进程（K5 v0 不做），排 v1。另：06 §5 的 v0 范围只有单 agent 真机回路，与候选②冲突——要么把 spawn+summary 提前进 v0，要么承认 v0 是单 agent 骨架、② 是 v0.5。

**D9 v0 安全诚实降级 —— 可接受，但"UI 明说"不够机器可读。**
建议：能力声明带 `sandbox: none|partial|full` **结构化标志**（API/事件可查），让脚本、CI 等自动化调用方能感知降级，不只靠人眼看横幅。fail-close + deny 默认在无 OS 沙箱时依然有意义（管意图层），保留。

**D10 MCP 位置 —— 建议 v0 = MCP client，server 不做。**
你们的"发现表做进本体"= agentlink，与 MCP 正交；server 的价值依赖 hub/多 session（v1）；client 能立刻吃到工具生态（作为 Tool 的一种适配器）。N1 ★ 据此可拍"client first"。注意 01 §3.4 的 `mcp__` 规则前缀有实现差异，接入时实测。

---

## 6. 新问题清单（编号延续 07 的体系）

| 新 ID | 内容 | 对应条目 | 档 |
|---|---|---|---|
| D21 | 跨边界契约唯一清单源（7/8/9 计数失同步；§3 清单缺 Workflow/ContextSnapshot） | P1-1 | P1 |
| D22 | Tool 契约补 `idempotent` / `replay` / 返回值 `graph_edges`（工具返回值契约入 contracts） | P0-5 | P0 |
| F7 | 事件 envelope 版本字段 + 演进铁律（只增不删不改义） | P1-7 | P1 |
| F8 | durable=false 流事件的 seq/持久化/订阅语义 | P1-2 | P1 |
| G23 | 状态源收口：ledger/黑板事件化（F5/G8/K22/06§7.1 四方对齐） | P0-1 | P0 |
| G24 | 索引器：seq 检查点续跑 / 重建 / UI 水位 / 单例归属 | P1-8 | P1 |
| H23 | workflow 规则进 H5 优先级链（且只可收窄） | P0-6 | P0 |
| J20 | 压缩触发计入 prompt cache 失效成本；Q 编排与压缩联动；J5/J13 触发口径合并 | P1-3 | P1 |
| J21 | tiered% 锚定生效模型窗口 + token 计量口径 | P1-4/5 | P1 |
| K27 | 垂直可见性口径定死（父+子 vs 全祖先/后代） | P0-3 | P0 |
| K28 | checkpoint / resume / 副作用先落盘 / ownership——durable 语义补决策 | P0-2 | P0 |
| K29 | fork/clone 对树/ledger/gate/worktree 的复制语义 | P2-6 | P2 |
| M9 | 长工具判定时点 vs 副作用时点（TOCTOU 声明边界） | P2-8 | P2 |
| N6 | N5 重复编号，"NDJSON 惯例"重编 | P2-1 | P2 |
| Q7 | summary 污点继承（H13 × K24） | P1-6 | P1 |
| S16 | 08 命名空间补 causation_id / gate_id / run_id / sum: 等 | P2-2 | P2 |

**决策间隐含的顺序依赖（C2 第三问，未写出来的）**：
1. 契约冻结必须早于一切实现（contract-first），但没有里程碑排序；
2. K1（隔离粒度）是 K8/K9 的前提——必须先拍 K1；
3. G22 要求工具返回带 graph_edges → **先改 Tool 契约**（D22）才有图，图才有 change_impact；
4. I4（跨 provider 交接）依赖 03 P0 的 transform-messages 复刻，未列入任何阶段计划；
5. W9c（baseline）依赖 V2 receipt 校验 + 事件流 run 记录 schema（G 区）；
6. Q1"编排最后做"被压缩策略反向依赖（P1-3）——排布与压缩必须联动设计。

---

## 7. 如果只允许改三处

1. **G23 状态源收口**——ledger/黑板更新一律事件化（`ledger.updated` / `board.written`，事件版本号做 CAS），当前态 = 索引器投影。一处改动保住 F5/F6/回放/压缩级联（J18-C）四条上层推理。
2. **K28 durable 恢复语义补全**——意图事件先落盘 → 执行 → 结果事件；`idempotent`/`replay` 进 Tool 契约（D22 并入）；checkpoint = seq 水位 + ledger 版本；resume = fold + in-flight 处置。这是"长时程自主"能否兑现的分水岭，也是 03 预警"最难自己长出来"的部分。
3. **K27 垂直可见性定口径**（推荐父 ↔ 子直系 + Inquiry 沿父链上查）——决定 agentlink / escalate / 审计三件套的形状，是多 agent 卖点的地基。

> 落选说明：P0-4（门禁边界）是最"像改动"的真错误，但它机械可控（移接口、拆判定/执行），列第 4；P0-7（隔离默认）是拍板问题不是设计矛盾，拍了就好；P0-5/P0-6 可并入上述三处的实施顺带完成。

---

## 8. 附：矛盾速查表

| # | A 说 | B 说 | 处理 |
|---|---|---|---|
| 1 | F5 不另设可变状态源 | G8 Document CAS / K22 黑板 LWW | P0-1 |
| 2 | K11 祖先↔后代可见 | K11 同格"父+直系子" | P0-3 |
| 3 | 09 core 含门禁、无 I/O | W7 门禁=进程级组件；W8=跑命令 | P0-4 |
| 4 | K1 goroutine 默认（✅） | 05 §4.2 进程默认（安全优先） | P0-7 |
| 5 | G4 hub 唯一写者（·） | G9 多写者+单索引（✅） | G4 作废；05 §2.3.4 同步 |
| 6 | 06 §3.4 跨边界 7 个 | §3.2 八个 / §4.2 九个 / D1 八个 | P1-1 |
| 7 | D12 Tool 四字段 | G22 graph_edges / M7 idempotent | P0-5 |
| 8 | H5 六层规则源 | workflow gates.rules 无位置 | P0-6 |
| 9 | 06 §7.1 数字深度检查 | K16 类型可达性为主 | P1-10 |
| 10 | 05 token 预算/配额 | W15 默认不设上限 | P1-11 |
| 11 | J5 三水位触发 | J13 50% 单阈值触发 | P1-3 合并定口径 |
| 12 | 06 §8-5 存储首选 modernc | G2b 定案 ncruces（modernc 备选） | P1-15 |
| 13 | 06 §8-2 v0 只做进程级扩展 | E1/E2 A 类接口为主（12 个） | P1-16 |
| 14 | N5 客户端鉴权 | N5 NDJSON 惯例 | P2-1 |

---

*审阅方法：10 份文档全文通读、决策逐条交叉引用；未运行代码。本报告不修改 `00`–`09` 任何原文。*
