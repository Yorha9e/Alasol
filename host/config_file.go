package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/runtime/mcp"
)

// 本文件实现 H15 的**用户级 TOML 配置**：凭据来源 = 进程 env + TOML 配置文件
// （Windows %APPDATA%\alasol\config.toml，其他平台 XDG），**优先级 env > 文件**，
// 凭据永不进 prompt/事件/日志（H18/H21 脱敏纪律）。
//
// 依赖取舍（BurntSushi/toml）：选它而非自写最小解析——TOML 表面简单实则暗坑繁多
// （转义/多行串/日期时间/点分键/数组表），而本文件**携带凭据**，解析错误 = 静默读错
// 默认值或凭据，是正确性+安全性双重隐患；BurntSushi/toml 是 Go 生态事实标准、
// 零传递依赖（纯标准库）、久经审计。自写解析省一个依赖却引入一整类难测的 bug 面，不値。

// FileConfig 是用户级 TOML 配置的形状（H15）。所有字段可缺省（缺省走 env / 框架默认）。
//
// 脱敏纪律：本结构含 **APIKey（凭据）**——绝不整体日志化/序列化进事件。需要打印时用
// Redacted()（凭据打码）。host.Config 也不得被整体日志化（同理）。
type FileConfig struct {
	// BaseURL provider 端点（空 = 官方）。
	BaseURL string `toml:"base_url"`
	// Model 模型名（端点相关，无万能默认；flag/env 优先，双空 fail-close）。
	Model string `toml:"model"`
	// APIKey 凭据直填值（**env 优先**：同名 env 有值则 env 赢，本字段被忽略）。
	// 永不进 prompt/事件/日志。
	APIKey string `toml:"api_key"`
	// APIKeyEnv 持有 API key 的**环境变量名**（缺省 OPENAI_API_KEY）。只传名字。
	APIKeyEnv string `toml:"api_key_env"`

	// System 系统提示。
	System string `toml:"system"`
	// Approval 审批策略：deny-all | always-allow | terminal。
	Approval string `toml:"approval"`
	// MaxTurns 单次 Run 最大轮数（0 = 框架默认）。
	MaxTurns int `toml:"max_turns"`
	// ContextBudget 上下文窗口预算 token（0 = 框架默认；只用于占比分母）。
	ContextBudget int64 `toml:"context_budget"`
	// Allow 允许的工具名（空 = 一切 deny，fail-close）。
	Allow []string `toml:"allow"`
	// Ask 需要人审的工具名。
	Ask []string `toml:"ask"`
	// ReasoningEffort 模型思维链强度档位（G14；空 = 不传，端点用默认档）。
	// 取值 low/medium/high/xhigh（host.Config.validate 白名单校验）。
	ReasoningEffort string `toml:"reasoning_effort"`

	// OnlineTable 在线表注入（K14/K15）默认参数。
	OnlineTable OnlineTableFileConfig `toml:"online_table"`
	// Monitor hub 监控（K9）阈值。
	Monitor MonitorFileConfig `toml:"monitor"`
	// ContextStrategy 上下文装配策略（v2-S1）：kind="" = append（默认）| "tiered" = 级联压缩；
	// window/l1_pct/l2_pct 是 tiered 的可配参数（13 §8/J9：数字是**可配初值**，不写死）。
	ContextStrategy ContextStrategyFileConfig `toml:"context_strategy"`
	// ContextRules 项目规则文本（AGENTS.md 等；稳定前缀的一部分，Q2/Q3）。空 = 不注入。
	ContextRules string `toml:"context_rules"`
	// Graph 图投影（v2-S2，G2f 可选投影；默认关 = 零图开销）。
	Graph GraphFileConfig `toml:"graph"`
	// Sandbox 沙箱执行环境（v2-S4，K8/H9；默认关 = 走 local，与 v1 等价）。
	Sandbox SandboxFileConfig `toml:"sandbox"`
	// MCP MCP client（v3-S1，N1：client 先；默认关 = 不连任何外部 server）。
	MCP MCPFileConfig `toml:"mcp"`
	// MCPServer MCP **server** 面（v4-5，B 类：把 harness 暴露成 MCP server；
	// `alasol mcp-serve` 读它）。默认关 = 白名单空 = 零工具暴露（fail-close 酸测试）。
	MCPServer MCPExposeFileConfig `toml:"mcp_server"`
	// WorkflowDir 工作流能力包目录（v3-S3，W13：纯数据 JSON；空 = 只用内建 tower）。
	WorkflowDir string `toml:"workflow_dir"`
	// GoalLedger 是否给 session 注入 goal ledger 更新工具（J19/X6；默认 false = 零行为变化）。
	GoalLedger bool `toml:"goal_ledger"`

	// MultiAgent 是否给每个 session 装配 agent 树 + loop.Agents（v3-S6-B，W5；默认 false =
	// 单 agent 模式零行为变化）。开了才建树/传 Agents/spawn 对模型可见（并自动补 spawn/complete
	// 的 allow 规则）。语义与 host.Config.MultiAgent 一致（这里是它的文件配置面）。
	MultiAgent bool `toml:"multi_agent"`
	// AgentTypes 用户自定义 agent 类型（v3-S6-B，W5 扩展点；TOML 数组表 [[agent_types]]）。
	// 与内建五类（planner/coder/critic/reviewer/observer）合并进同一类型注册表，**同名覆盖内建**
	// （用户自定义优先）。**只在 MultiAgent=true 时生效**（关 = 不建注册表、**不校验** =
	// 零行为变化）。装载期 fail-close（仅 MultiAgent 开时）：id 空/重名/**保留名(root)**、
	// can_spawn 悬空/自环/成环、permission 级别非法、context_strategy.kind 越枚举 → 启动即拒并点名。
	AgentTypes []AgentTypeFileConfig `toml:"agent_types"`
	// Blob 大 payload 外置 blob（v4-3-A，G2e/D4）：[blob] 段。默认关（dir 空）= 不外置，
	// completed 载荷与现状逐字节等价（酸测试）。配了 dir 且 max_inline>0 才启用外置。
	Blob BlobFileConfig `toml:"blob"`
	// Vectors 向量引擎 + RAG 检索（v4-3-B，G2d）：[vectors] 段。默认关（enabled=false）=
	// 不建向量库、`vector_search` 工具不注册、/api/vectors* 端点 404——零行为变化（酸测试）。
	Vectors VectorsFileConfig `toml:"vectors"`
	// Embedding embeddings 提供者的文件级配置（v4-3-B 装配面）：[embedding] 段。
	// **model 空 = 不接提供者** = Config.EmbeddingProvider 保持 nil（零行为变化）；
	// 非空才在 cmd 装配面构造 openai.Embedder 填进槽位。与 [vectors] 分工：本段管
	// "用哪个端点/模型/密钥名"，[vectors] 管库与工具开关——两者正交（开了库没配
	// 提供者仍是 503/不注册的零行为变化）。
	Embedding EmbeddingFileConfig `toml:"embedding"`
	// Documents 文档族投影（G2 第 5 族）：[documents] 段。默认关（enabled=false）=
	// 不建 DocStore、/api/docs* 四个端点 404——零行为变化（酸测试）。
	Documents DocumentsFileConfig `toml:"documents"`
	// Wal WAL 预写日志（v4-4-B）：[wal] 段。默认关（dir 空）= 一个文件都不建、一次追加
	// 都不做、行为与现状**逐事件等价**（酸测试）。
	Wal WalFileConfig `toml:"wal"`
	// Telemetry 观测管道（L2/D19）：[telemetry] 段。默认关（mode 空/none）= 一条 record
	// 都不造 = 零行为变化（酸测试）。
	Telemetry TelemetryFileConfig `toml:"telemetry"`
	// Budget 成本预算闸门（W15，v4-4-B）：[budget] 段。**默认不设上限**（max_tokens 0）
	// ——W15 拍板"长时程自主是卖点，预算不可控"；配了才启用 per-Run 粒度的检查点。
	Budget BudgetFileConfig `toml:"budget"`
}

// BlobFileConfig 是大 payload 外置 blob 的文件级配置（v4-3-A，G2e/D4）。
//
// 形如：
//
//	[blob]
//	dir = "C:\\Users\\me\\alasol\\blobs"   # 外置仓库根目录（空 = 关 = 零行为变化）
//	max_inline = 65536                      # 阈值字节（0 = 不外置，即使配了 dir）
//
// 口径（与 host.Config.BlobDir / BlobMaxInline 一致）：
//   - **dir 空 = 关**：不建存储、loop 不外置、/api/blobs/{id} 恒 404——与现状完全等价（酸测试）；
//   - **max_inline 0 = 不外置**（即使配了 dir）：目录非空时仍建存储供读侧，但没有 result 会外置；
//   - 只有"dir 非空 + max_inline>0"才真正启用：超阈值的 tool.call.completed result 整体进 blob，
//     事件里留 binary_ref 引用 + 摘要 text（X1 三段结构不动，零契约改动——ToolResult 形状不变）；
//   - **max_inline 负值 = 非法配置**（启动即拒，报错文案与 serve 装配面同款——唯一事实源 =
//     host.ValidateBlobMaxInline；run / resume 装配面同样拒，不静默当成 0）。
type BlobFileConfig struct {
	// Dir blob 外置仓库根目录（runtime/store/blobfs 落地处；空 = 关）。
	Dir string `toml:"dir"`
	// MaxInline result 内联阈值（字节；超过才外置；0 = 不外置；负值非法 = 启动即拒）。
	//
	// F4：实际判定用的是**有效阈值** = max(本值, 摘要预算)（见 core/loop/blob.go）——
	// 保证"外置 = 瘦身"是结构性事实（摘要不会比原文还大）。
	MaxInline int64 `toml:"max_inline"`
}

// VectorsFileConfig 是向量引擎 + RAG 检索的文件级配置（v4-3-B，G2d）。
// 形如：
//
//	[vectors]
//	enabled = true      # 是否装配向量引擎（默认 false = 关 = 零行为变化）
//	top_k = 10          # 检索默认 top-k（K=0 时生效；0 = vecmem 默认 10；负值非法 = 启动即拒）
//
// 口径（与 host.Config.VectorsEnabled / VectorsTopK 一致）：
//   - **enabled 默认 false = 关**：不建向量库、`vector_search` 工具不注册、
//     GET /api/vectors/search 与 POST /api/vectors 都不注册（404）——与现状完全等价（酸测试）；
//   - **top_k** 只是 K=0 时的默认上限（调用方显式给 K 仍以 K 为准）；
//   - **没有 dir**：vecmem 是**内存实现**（可整体重建的缓存，丢了重建，见包 doc）——
//     要持久化是换实现的事（SQLite / 向量数据库），消费方零改动（G2b）；
//   - **Filter 口径**（实现支持过滤，不报 ErrUnsupported）：对条目 Payload JSON 的
//     **顶层字符串字段**做字节精确匹配（AND 语义；键缺失/值非字符串 → 不命中）。
//
// 另有一条**与配置无关**的硬口径（写在代码常量里，不给配置面开洞）：**单批 ingest ≤512 条**
// （一条文本 = 一次 embedding 提供者调用、批内串行）——超出 → 400 并点名上限（见
// host/vectors.go 的 maxVectorTexts）。
//
// embedding 提供者**不在本段**——那是独立的 [embedding] 段（model/base_url/api_key_env）。
// 框架不算 embedding（文本转向量是 provider 层的事，换模型就要重算）；[embedding] 段提供
// 模型/端点/密钥**变量名**，由 cmd 装配面构造成 openai.Embedder 填进 Config.EmbeddingProvider
// （不是框架在算）。生产未接提供者时：库建了但检索/ingest 端点回 503 说明、工具不注册
// （零行为变化）。
type VectorsFileConfig struct {
	// Enabled 是否装配向量引擎（默认 false = 关 = 零行为变化）。
	Enabled bool `toml:"enabled"`
	// TopK 检索默认 top-k（0 = vecmem 默认 10；负值非法 = 启动即拒）。
	TopK int `toml:"top_k"`
}

// EmbeddingFileConfig 是 embeddings 提供者的文件级配置（v4-3-B 装配面）。
// 形如：
//
//	[embedding]
//	model = "text-embedding-3-small"   # embedding 模型名（空 = 不接提供者 = 零行为变化）
//	base_url = "https://api.example/v1" # embeddings 端点（空 = 回退 chat 的 base_url；再空 = openai 缺省官方端点）
//	api_key_env = "MY_EMBEDDING_KEY"    # 持有 embedding key 的**环境变量名**（空 = 回退 chat 的 api_key_env）
//
// 口径（与 cmd 装配面一致；框架不算 embedding——文本转向量由 provider 层的 openai.Embedder 做）：
//   - **model 空 = 不接 EmbeddingProvider**（Config.EmbeddingProvider 保持 nil =
//     vector_search 工具不注册、/api/vectors* 回 503 说明——零行为变化）。
//     **注意 503 的前提**：那个 503 只在 **[vectors].enabled = true** 时成立（库开着、
//     路由注册了，只是没提供者）；[vectors].enabled = false（默认）时 **/api/vectors* 的
//     路由压根不注册 → 404**，与有没有 embedding 提供者无关（酸测试面）。
//   - **model / base_url 也有 env 面**（cmd 装配面）：ALASOL_EMBEDDING_MODEL（模型名）
//     与 ALASOL_EMBEDDING_BASE_URL（端点 base URL）——优先级同 chat 的
//     ALASOL_MODEL / ALASOL_BASE_URL：**显式 flag > env > 本文件**。env 名固定这两个，
//     不另设变量名（端点与模型名不是凭据，不需要"换一台机器就换变量名"的能力）。
//   - **base_url** 可与 chat 不同端点（embedding 与 chat 分开部署时用）；它是 **base
//     URL**——端点会自己拼 "/embeddings"（base_url = "https://api.example/v1" →
//     POST https://api.example/v1/embeddings）。
//   - **api_key_env 只收变量名**（H15 凭据纪律）：值在 cmd 装配时从那个变量取，
//     永不进 argv/事件/日志；embedding 与 chat 可以是不同 key。
//     **口径（F1：不静默跨边界回退）**：
//     · **显式指定**（本段给了非空变量名，且它 ≠ chat 的 api_key_env）→ 值**只**从那个
//     变量取；取不到 → **启动即拒**（fail-close，报错点名变量名、绝不含值）。**不**
//     静默回退到 chat 的凭据——显式"分家"就是要 chat 的 key 不被发到第三方 embedding
//     端点；
//     · **未指定**（空）→ **沿用 chat 的凭据**：**变量名与值都沿用 chat**（同一凭据是
//     意图，不只是名字沿用）。
//   - 三字段映射进 host.Config.EmbeddingProvider（经 openai.NewEmbedder），不在
//     host.Config 上再挂 model/base_url/api_key_env 的重复槽位——装配面只认 provider。
type EmbeddingFileConfig struct {
	// Model embedding 模型名（空 = 不接提供者 = 零行为变化；env 面 = ALASOL_EMBEDDING_MODEL）。
	Model string `toml:"model"`
	// BaseURL embeddings 端点的 **base URL**（端点会拼 "/embeddings"；空 = 回退 chat 的
	// base_url；再空 = openai 缺省官方端点；env 面 = ALASOL_EMBEDDING_BASE_URL）。
	BaseURL string `toml:"base_url"`
	// APIKeyEnv 持有 embedding key 的环境变量名（只传名字，不传值，H15）。
	//
	// 口径（F1）：**未指定**（空）→ 沿用 chat 的凭据（变量名与值都沿用）；
	// **显式指定**（且 ≠ chat 的 api_key_env）→ 值只从该变量取，取不到 → 启动即拒。
	APIKeyEnv string `toml:"api_key_env"`
}

// DocumentsFileConfig 是文档族投影的文件级配置（G2 第 5 族）。
// 形如：
//
//	[documents]
//	enabled = true      # 是否装配文档族存储（默认 false = 关 = 零行为变化）
//
// 口径（与 host.Config.DocumentsEnabled 一致）：
//   - **enabled 默认 false = 关**：不建 DocStore、GET /api/docs/{collection}/{key}、
//     GET /api/docs、POST /api/docs/upsert、POST /api/docs/delete 都不注册（404）——
//     与现状完全等价（酸测试）；
//   - **没有路径/dir**：DocStore 是**事件库的同库单表**（SQLite docs 表，(collection,key)
//     主键），表由 EnsureSchema 建——要换实现是换存储的事（G2b），消费方零改动；
//   - **写只经宿主 ingest 入口**（/api/docs/upsert、/api/docs/delete，经 Transact 收口
//     事务；G17 单写者：与 IngestVectors 同口径）。文档是投影，事件流才是事实源（F5）。
type DocumentsFileConfig struct {
	// Enabled 是否装配文档族存储（默认 false = 关 = 零行为变化）。
	Enabled bool `toml:"enabled"`
}

// WalFileConfig 是 WAL 预写日志的文件级配置（v4-4-B）。
//
// 形如：
//
//	[wal]
//	dir = "C:\\Users\\me\\alasol\\wal"    # WAL 根目录（空 = 关 = 零行为变化，酸测试）
//
// 口径（与 host.Config.WalDir 一致）：
//   - **dir 空 = 关**（默认）：不建目录、不建任何文件、loop.Config.WalSink 恒 nil →
//     一个字节都不多写，行为与现状**逐事件等价**（酸测试）；
//   - **WAL 不是事实源**（F5 不受影响）：事件流（core.Store）仍是唯一事实源，WAL 记录
//     "正式写之前"的中间态（turn.begin / turn.delta / turn.end / turn.failed / blob.plan），崩溃后至少
//     知道"当时在干什么、吐过什么"；
//   - **可丢弃 / 可截断 / 可 GC**：删掉整个目录不丢任何事实；恢复报告是**观察面不是写回面**
//     （WAL 里的 delta 可供展示，绝不自动补进事件流）；
//   - **写失败不阻断主流程**（尽力而为，一条 Warn 留痕）；
//   - **没有别的开关**：fsync 策略（逐条不 fsync、轮次边界 fsync）是实现决策，不给配置面
//     开洞；GC 面走 `alasol wal-gc` 的 **-retention 显式龄期**（缺省即拒、不猜默认，
//     见 host.Config.WalDir 的 GC 注记）。
type WalFileConfig struct {
	// Dir WAL 根目录（runtime/store/walfile 落地处；per-session 一个 `<session>.wal`）。
	// 空 = 关 = 零行为变化。
	Dir string `toml:"dir"`
}

// TelemetryFileConfig 是 telemetry 观测管道的文件级配置（L2/D19）。
//
// 形如：
//
//	[telemetry]
//	mode = "file"                            # "" | none（关，默认）| file | otlp（留位）
//	dir = "C:\\Users\\me\\alasol\\telemetry" # mode=file 时必填；单文件 telemetry.ndjson
//
// 口径（与 host.Config.Telemetry / TelemetryDir 一致）：
//   - **mode 空 / "none" = 关**（默认）：loop.Config.TelemetrySink 恒 nil → 一条 record
//     都不造、一次 Emit 都不调 = **零行为变化**（酸测试）；
//   - **mode "file"**：dir 必填（启动即拒，不静默关）；落盘是可丢弃的观测出站口
//     （不是事实源，F5 不受影响；emit 前自由文本已过 core.Redact）；
//   - **mode "otlp" = 留位**（尚未实现）：装配期拒绝——不静默降级，否则使用者以为在送；
//   - **只走文件配置面**（与 [wal] / [blob] 同款：结构化数据 + 默认关，不给 flag）；
//   - **只收 mode/dir 两个键**（反射派生的合法键清单自动含本表，未知键 fail-close 点名）。
type TelemetryFileConfig struct {
	// Mode 模式："" | "none" = 关（默认）| "file" = NDJSON 落盘 | "otlp" = 留位（拒）。
	Mode string `toml:"mode"`
	// Dir mode=file 时的落盘目录（单文件 `<dir>/telemetry.ndjson`）。空 = 非法（拒）。
	Dir string `toml:"dir"`
}

// BudgetFileConfig 是成本预算闸门的文件级配置（W15，v4-4-B）。
//
// 形如：
//
//	[budget]
//	max_tokens = 1000000     # 单 session 的 token 成本上限（0 = 不设上限 = 默认）
//
// 口径（与 host.Config.BudgetMaxTokens 一致）：
//   - **默认不设上限**（max_tokens=0）：W15 拍板"长时程自主是卖点，预算不可控"；
//   - 非 0 时 host 在**每个 prompt 之前**检查该 session 的累计 token 成本（input+output，
//     数据源 = 索引器 cost_tokens，J21）：≥ 上限 → 拒绝发起新一轮；跑完越界 → 告警；
//   - **per-Run 粒度**（一个 prompt = 一次 Run = 若干 turn）：一轮跑到一半越界时本轮跑完，
//     下一轮才被拒——turn 内逐轮刹车是另一笔（挂账见 11 施工计划）；
//   - **负值非法**（启动即拒，与 [blob].max_inline 同口径）。
type BudgetFileConfig struct {
	// MaxTokens 单 session 的 token 成本上限（0 = 不设上限；负值非法 = 启动即拒）。
	MaxTokens int64 `toml:"max_tokens"`
}

// AgentTypeFileConfig 是一个**用户自定义** agent 类型的文件级声明（[[agent_types]] 的元素，
// v3-S6-B / W5）。
//
// 形如：
//
//	[[agent_types]]
//	id = "auditor"                         # 必填；类型 id（唯一；同名覆盖内建五类）
//	prompt = "你是 auditor：……"             # 该类型的 system 提示
//	tools = ["read", "write", "bash"]      # 工具白名单（R5：不列即不可见）；框架工具 recall/stage/ledger 亦可列
//	model = "default"                       # 模型绑定（default = 用主模型）
//	can_spawn = ["observer"]               # 可派生的子类型 id（空 = 叶子；K20）
//	share_project_assets = false            # W6-A：true = 该类型子 agent 装配 tiered 时注入项目规则（rules）；**顶层字段**
//
//	[agent_types.permission_request]        # 权限请求（H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予）
//	read = "workspace"                      # none | workspace | full
//	write = "none"
//	exec = "none"
//
//	[agent_types.context_strategy]          # W6-A：本类型子 agent 的上下文策略（显式声明，不继承主 agent）
//	kind = "tiered"                         # append | stage_scoped | sliding_window | tiered
//
// 注意（段位别写错）：**share_project_assets 是 [[agent_types]] 的顶层字段**，不是
// [agent_types.context_strategy] 下的键（那张表只有 kind）——写错段位时 TOML 解析器会
// **静默忽略**它（配了 true 却被无声吞掉）。故装载期对**未知键** fail-close 点名
// （见 LoadFileConfig）。
//
// 口径：
//  1. **只走文件配置面**（结构化数据，不给 flag——与 [[mcp.servers]] 同款纪律）；
//  2. **deny-all 默认 + R5 白名单**：声明了类型只是"可被派生"，工具能不能调还要策略链放行
//     + 类型 tools 白名单双重同意；
//  3. **宿主只实现 tiered**：context_strategy.kind = stage_scoped / sliding_window 可装载，
//     但 spawn 时宿主的 ChildStrategy 钩子对未实现的 kind 返回 nil 工厂 → core fail-close
//     收口成 agent.failed（不静默降级成 append）；
//  4. **context_strategy 不带 params**：tiered 的 window/l1_pct/l2_pct 走宿主配置面
//     （[context_strategy] / host.Config），类型级 params 契约字段宿主尚未逐字段接线。
type AgentTypeFileConfig struct {
	// ID 类型 id（必填；唯一；同名覆盖内建五类）。
	ID string `toml:"id"`
	// Prompt 该类型的 system 提示（W5 用户可自定义）。
	Prompt string `toml:"prompt"`
	// Tools 工具白名单（R5：不列即不可见）。注册表工具名 read/write/bash + MCP 前缀工具，
	// 框架工具 recall/stage/ledger 亦可列。
	Tools []string `toml:"tools"`
	// Model 模型绑定（default = 用主模型；空 = 继承主模型，见 core/loop buildChildLoop 兜底）。
	Model string `toml:"model"`
	// CanSpawn 可派生的子类型 id（空 = 叶子；K20 只读型叶子更深层在类型层面不可达）。
	CanSpawn []string `toml:"can_spawn"`
	// PermissionRequest 权限请求（H22：实际 = 请求 ∩ 策略上限 ∩ 父级授予；nil = 未请求 = 全 none）。
	PermissionRequest *PermissionRequestFileConfig `toml:"permission_request"`
	// ContextStrategy 本类型子 agent 的上下文策略（W6-A：显式声明，不继承主 agent；nil = append 默认）。
	ContextStrategy *ContextStrategyDeclFileConfig `toml:"context_strategy"`
	// ShareProjectAssets 项目级资产（rules）共享开关（W6-A；nil = false = 不共享）。
	// true = 该类型子 agent 装配 tiered 时注入项目规则（host.Config.ContextRules）。
	ShareProjectAssets *bool `toml:"share_project_assets"`
}

// PermissionRequestFileConfig 是权限请求的文件级声明（[agent_types.permission_request]）。
type PermissionRequestFileConfig struct {
	// Read 读维度：none | workspace | full。
	Read string `toml:"read"`
	// Write 写维度：none | workspace | full。
	Write string `toml:"write"`
	// Exec 执行维度：none | allowlist | full。
	Exec string `toml:"exec"`
}

// ContextStrategyDeclFileConfig 是上下文策略声明的文件级形状（[agent_types.context_strategy]）。
type ContextStrategyDeclFileConfig struct {
	// Kind 策略种类：append | stage_scoped | sliding_window | tiered（装载期枚举校验）。
	Kind string `toml:"kind"`
}

// MCPFileConfig 是 MCP client 的文件级配置（v3-S1）。
//
// 形如：
//
//	[[mcp.servers]]
//	name = "fs"                          # 必填；工具名前缀 mcp__fs__<tool>
//	transport = "stdio"                  # stdio（默认）| http
//	command = "npx"
//	args = ["-y", "@scope/server-fs", "."]
//	enabled = true
//	env = { TOKEN = "..." }              # 子进程 env 直填（凭据；永不进日志）
//	env_from = ["MY_TOKEN"]              # 从父进程 env 继承这些变量名
//
//	[[mcp.servers]]
//	name = "remote"
//	transport = "http"
//	url = "http://127.0.0.1:1234/mcp"
//	headers = { Authorization = "Bearer ..." }
//
// 口径（三条，与 host.Config.MCP 的注释一致，不重抄数字）：
//
//  1. **默认关**：`[[mcp.servers]]` 一条都没有 = 不装 MCP（零外部进程、零工具，
//     与 v2 行为完全一致）。
//  2. **deny-all 默认**：声明了 server 也只是"工具可见/可注册"，能不能调还要
//     策略链放行（`allow` 里列 `mcp__<server>__<tool>`）与 agent 类型白名单
//     （R5）双重同意。
//  3. **凭据纪律**：`env` / `headers` 的值可能含 server 凭据，**永不进事件/日志**。
//     本结构含凭据 → 绝不整体日志化，要打印用 Redacted()。
type MCPFileConfig struct {
	// Servers 是要连接的 server 列表（TOML 的数组表 [[mcp.servers]]）。
	//
	// enabled 的缺省口径：TOML 的 bool 零值是 false，所以"没写 enabled"会解析成
	// false = 不连。这与"默认关"看似一致，实则误导——使用者写了一条 server 却
	// 忘了 enabled，会得到"静默不连"。因此 host 侧对 enabled 未显式写的条目
	// **按 true 处理**（见 host/mcp.go 的 normalizeEnabled），让"写了就要连"
	// 成为默认，`enabled = false` 才是显式关闭。
	Servers []MCPServerFileConfig `toml:"servers"`
}

// MCPServerFileConfig 是一个 MCP server 的文件级声明（[[mcp.servers]] 的元素）。
//
// 字段与 runtime/mcp.ServerConfig 一一对应；Dir 不暴露（文件级配置不该能改
// 子进程工作目录——那会让"连一个工具 server"变成"在任意目录跑任意命令"）。
type MCPServerFileConfig struct {
	// Name server 逻辑名（工具名前缀 mcp__<server>__<tool> 的来源；全局唯一）。
	Name string `toml:"name"`
	// Transport 传输类型：stdio（缺省）| http。
	Transport string `toml:"transport"`
	// Enabled 是否启用。
	//
	// **指针**而不是 bool：TOML 的 bool 零值是 false，用值类型就无法区分
	// "没写 enabled" 与 "写了 enabled = false"。没写 = 使用者声明了这条 server
	// 却忘了开关 → 按 **true** 处理（"写了就要连"比"静默不连"好排查得多）；
	// 只有显式 `enabled = false` 才是关闭。
	Enabled *bool `toml:"enabled"`
	// Command / Args 是 stdio 传输的进程声明（不走 shell）。
	Command string   `toml:"command"`
	Args    []string `toml:"args"`
	// Env 是显式注入子进程的键值（**可能含凭据**；永不进事件/日志）。
	Env map[string]string `toml:"env"`
	// EnvFrom 是要从父进程 env 继承的**变量名**（只传名字，值在装配期取出）。
	EnvFrom []string `toml:"env_from"`
	// URL / Headers 是 http 传输的端点与请求头（Headers 可能含凭据）。
	URL     string            `toml:"url"`
	Headers map[string]string `toml:"headers"`
	// TimeoutMs 连接/发现超时（毫秒；0 = 默认）。
	TimeoutMs int `toml:"timeout_ms"`
}

// Redacted 返回凭据已打码的副本（**只有它输出的结构可安全落日志**，H12/H19）。
func (c MCPFileConfig) Redacted() MCPFileConfig {
	out := c
	out.Servers = make([]MCPServerFileConfig, len(c.Servers))
	for i, s := range c.Servers {
		out.Servers[i] = s.Redacted()
	}
	return out
}

// Redacted 返回单个 server 的凭据打码副本（Env/Headers 的值 → "***"）。
func (s MCPServerFileConfig) Redacted() MCPServerFileConfig {
	out := s
	out.Args = append([]string(nil), s.Args...)
	out.EnvFrom = append([]string(nil), s.EnvFrom...)
	if len(s.Env) > 0 {
		out.Env = make(map[string]string, len(s.Env))
		for k := range s.Env {
			out.Env[k] = "***"
		}
	}
	if len(s.Headers) > 0 {
		out.Headers = make(map[string]string, len(s.Headers))
		for k := range s.Headers {
			out.Headers[k] = "***"
		}
	}
	return out
}

// MCPConfig 把文件级配置折成 runtime/mcp.Config（host.Config.MCP 的**唯一来源**）。
//
// 只做搬运 + 两件事：
//  1. `enabled` 未显式写的条目按 **true** 处理（理由见 MCPServerFileConfig.Enabled：
//     "写了 server 却静默不连"比"多连一个"更难排查）；
//  2. 没写 name 的条目照样搬过去——由 runtime/mcp.Config.Validate 给出
//     "server 名不能为空"的明确错误（fail-close：不在这里静默丢弃，
//     否则使用者以为连上了）。
func (c MCPFileConfig) MCPConfig() mcp.Config {
	out := mcp.Config{Servers: make([]mcp.ServerConfig, 0, len(c.Servers))}
	for _, s := range c.Servers {
		enabled := true
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		out.Servers = append(out.Servers, mcp.ServerConfig{
			Name:      s.Name,
			Transport: s.Transport,
			Enabled:   enabled,
			Command:   s.Command,
			Args:      append([]string(nil), s.Args...),
			Env:       copyEnvMap(s.Env),
			EnvFrom:   append([]string(nil), s.EnvFrom...),
			URL:       s.URL,
			Headers:   copyEnvMap(s.Headers),
			TimeoutMs: s.TimeoutMs,
		})
	}
	return out
}

// MCPExposeFileConfig 是 MCP **server** 面的文件级配置（v4-5，B 类；TOML `[mcp_server]`）。
//
// 形如：
//
//	[mcp_server]
//	expose_tools = ["read"]     # 只暴露 read；空/省略 = 一个都不暴露（fail-close）
//
// 口径（与 host.Config.MCPExposeTools 一致，不重抄）：
//   - **fail-close**：`expose_tools` 空 = 零工具暴露（tools/list 空集、任何 tools/call
//     命中 unknown tool）。这是刻意的最小暴露面，防 harness 写面（write/bash）被任意
//     MCP client 摸到；
//   - **只收非 mcp__ 名**：`mcp__*` 是 MCP **client** 侧的内部命名空间（外部 server 的
//     工具已经由 client 聚合过一次），不上公网面——白名单点名也拒（F2，同 fail-close 精神）；
//   - **只走文件配置面**：暴露名单是安全敏感的（与 [[mcp.servers]] 的凭据同性质），
//     不进命令行（命令行泄进 shell 历史 / ps，H12/H19）；
//   - 白名单里点名但注册表没有的工具名被**忽略**（fail-safe，不因悬空名让 server 残缺）。
//
// 命名注记：客户端每-server 元素已占用 MCPServerFileConfig（[[mcp.servers]] 的元素），
// 故本表按"暴露面"命名，TOML 段仍是 [mcp_server]。
type MCPExposeFileConfig struct {
	// ExposeTools 是暴露成 MCP tools 的工具名白名单（注册表工具名，如 read / write）。
	// 空 = 零暴露（fail-close）。**不收 mcp__ 开头的名字**（内部命名空间，见上）。
	ExposeTools []string `toml:"expose_tools"`
}

// ExposeToolsCopy 返回白名单的一份拷贝（nil 安全；不共享底层数组，防调用方改到配置内部）。
func (c MCPExposeFileConfig) ExposeToolsCopy() []string {
	if len(c.ExposeTools) == 0 {
		return nil
	}
	return append([]string(nil), c.ExposeTools...)
}

// copyEnvMap 拷一份键值表（nil 安全；不共享底层 map，防调用方改到配置内部）。
func copyEnvMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// AgentTypeConfigs 把文件级 [[agent_types]] 声明折成 []contracts.AgentType（host.Config.AgentTypes
// 的**唯一来源**，v3-S6-B / W5）。
//
// 只做搬运 + 两件归一化（不校验——校验在 host/agenttypes.go 的注册表装配期 fail-close）：
//  1. **权限空字段 → none**：使用者写了 [agent_types.permission_request] 却只填 read 时，
//     write/exec 留空 = "没授予" → 归一成 none（least privilege）。若原样留空串，permSubset
//     会按"未知级别 = 最严（rank 3）"处理，反而让该类型永远派不出来——那不是使用者的本意；
//  2. **context_strategy / share_project_assets 指针**：nil = 未声明（append 默认 / 不共享），
//     显式声明才造指针（区分"没写"与"写了 false"）。
//
// 深拷贝切片（Tools / CanSpawn）：不共享底层数组，防调用方改到配置内部（与 MCPConfig 同纪律）。
func (c FileConfig) AgentTypeConfigs() []contracts.AgentType {
	if len(c.AgentTypes) == 0 {
		return nil
	}
	out := make([]contracts.AgentType, 0, len(c.AgentTypes))
	for _, at := range c.AgentTypes {
		ct := contracts.AgentType{
			ID:       at.ID,
			Prompt:   at.Prompt,
			Tools:    append([]string(nil), at.Tools...),
			Model:    at.Model,
			CanSpawn: append([]string(nil), at.CanSpawn...),
		}
		if at.PermissionRequest != nil {
			ct.PermissionRequest = &contracts.PermissionRequest{
				Read:  contracts.ReadWriteLevel(permLevelOrNone(at.PermissionRequest.Read)),
				Write: contracts.ReadWriteLevel(permLevelOrNone(at.PermissionRequest.Write)),
				Exec:  contracts.ExecLevel(permLevelOrNone(at.PermissionRequest.Exec)),
			}
		}
		if at.ContextStrategy != nil {
			ct.ContextStrategy = &contracts.ContextStrategy{
				Kind: contracts.ContextStrategyKind(strings.TrimSpace(at.ContextStrategy.Kind)),
			}
		}
		if at.ShareProjectAssets != nil {
			v := *at.ShareProjectAssets
			ct.ShareProjectAssets = &v
		}
		out = append(out, ct)
	}
	return out
}

// permLevelOrNone 把权限级别归一化：空（trim 后）= none（least privilege）；其余原样返回
// （合法性由 host/agenttypes.go 的装载期校验判——这里不静默改写用户写的值）。
func permLevelOrNone(v string) string {
	if s := strings.TrimSpace(v); s != "" {
		return s
	}
	return string(contracts.ReadWriteLevelNone)
}

// GraphFileConfig 是图投影的文件级配置（v2-S2）。
//
// 形如：
//
//	[graph]
//	enabled = true
//	max_nodes = 100000
//	max_edges = 1000000
//	max_depth = 8
//	max_results = 5000
//	core = ["core/*", "contracts/*"]
//	escalate_reachable = 50
//
// 口径：**enabled 为 false（默认）= 图投影未启用**——不建图表、不建引擎、索引器连
// tool.call.completed 的载荷都不解析（G2f：图不进 core 必经路径，零图开销）。
// 上界 0 = 框架默认（G19 量级口径：10 万节点/100 万边）。core 里 `*` 结尾的是
// **字节前缀**（大小写敏感，陷阱 17）。
type GraphFileConfig struct {
	// Enabled 是否启用图投影（可选投影，G2f）。
	Enabled bool `toml:"enabled"`
	// MaxNodes / MaxEdges 内存图上界（0 = 框架默认 10 万/100 万）。
	MaxNodes int `toml:"max_nodes"`
	MaxEdges int `toml:"max_edges"`
	// MaxDepth / MaxResults 多跳遍历上限（0 = 框架默认 8 / 5000）。
	MaxDepth   int `toml:"max_depth"`
	MaxResults int `toml:"max_results"`
	// Core "核心接口"节点声明（`*` 结尾 = 前缀；W16 第二个客观依据）。
	Core []string `toml:"core"`
	// EscalateReachable change_impact 的 escalate 阈值（可达节点数；0 = 默认 50）。
	EscalateReachable int `toml:"escalate_reachable"`
}

// SandboxFileConfig 是沙箱执行环境的文件级配置（v2-S4，K8/H9）。
//
// 形如：
//
//	[sandbox]
//	enabled = true
//	write_dirs = ["D:\\repo"]
//	max_active_processes = 64
//	acl_max_objects = 200000
//
// 口径：**enabled 为 false（默认）= 未启用沙箱**，工具执行环境走 runtime/cap/local
// （与 v1 等价，零沙箱开销）。开了之后：
//
//   - bash / cmd 起在**受限 token** 下（用户 SID 转 deny-only + 删危险特权）；
//   - 子进程纳入 **job object**（KILL_ON_JOB_CLOSE 收容整棵进程树）；
//   - `write_dirs` 里的根被设 DACL，**根之外的写被内核拒绝**（OS 强制，不是路径守卫；
//     例外：BUILTIN\Users 有写权限的位置如 C:\ProgramData 在默认模式下仍可写）；
//   - 根之外的**用户私有文件**（.env/.ssh）读不到；系统位置默认仍可读
//     （严格模式 strict_groups = true 才连读一起收紧）。
//
// `write_dirs` 空 = 只用 CWD + 沙箱自管临时根（子进程的 TMP/TEMP 会指到后者）。
// 上界 0 = 框架默认（活动进程 64 / ACL 对象 20 万）。
type SandboxFileConfig struct {
	// Enabled 是否启用沙箱执行环境（默认 false = 走 local）。
	Enabled bool `toml:"enabled"`
	// WriteDirs 允许写根（绝对路径；空 = CWD + 沙箱临时根）。
	WriteDirs []string `toml:"write_dirs"`
	// MaxActiveProcesses job object 的活动进程上限（0 = 默认 64）。
	MaxActiveProcesses int `toml:"max_active_processes"`
	// ACLMaxObjects 施加 ACL 前数子树对象的上限（0 = 默认 20 万；超限即拒）。
	ACLMaxObjects int `toml:"acl_max_objects"`
	// StrictGroups 严格组 SID 模式（默认 false = 保留 BUILTIN 的 Users SID）。
	//
	// 严格模式把"只授予 Users 写权限的目录"（典型是 C:\ProgramData）也堵上，
	// 代价是用 overlapped I/O 写文件的程序（Go/.NET/Rust/Node）会写不进去。
	StrictGroups bool `toml:"strict_groups"`
}

// ContextStrategyFileConfig 是上下文装配策略的文件级配置（v2-S1）。
//
// 形如：
//
//	[context_strategy]
//	kind = "tiered"
//	window = 1000000
//	l1_pct = 10.0
//	l2_pct = 1.0
//
// 口径：**kind 为空 = append**（默认，v1 等价）；tiered 时 window 须 ≥ 256k（13 §8 硬约束，
// 不满足拒绝装配并说明）。l1_pct/l2_pct 是目标残留占窗口的百分比（可配初值，J9）。
type ContextStrategyFileConfig struct {
	// Kind 策略种类："" = append（默认）| "tiered" = 级联压缩。
	Kind string `toml:"kind"`
	// Window tiered 的窗口预算 W（token；0 = 默认 1M；须 ≥ 256k）。
	Window int64 `toml:"window"`
	// L1Pct L1（浅摘要）目标残留占窗口百分比（0 = 默认 10）。
	L1Pct float64 `toml:"l1_pct"`
	// L2Pct L2（深压缩）目标残留占窗口百分比（0 = 默认 1）。
	L2Pct float64 `toml:"l2_pct"`
	// RulesBudget 项目规则注入的 token 上限（0 = 布局默认 15k）。
	RulesBudget int64 `toml:"rules_budget"`
	// CommonSenseBudget 常识区注入的 token 上限（0 = 布局默认 30k）。
	CommonSenseBudget int64 `toml:"common_sense_budget"`
}

// OnlineTableFileConfig 是在线表注入的文件级默认（K14/K15）。
type OnlineTableFileConfig struct {
	Rows       int    `toml:"rows"`
	LineTokens int    `toml:"line_tokens"`
	Level      string `toml:"level"`
	Window     int    `toml:"window"`
}

// MonitorFileConfig 是 hub 监控（K9）的文件级阈值。0 = 用框架默认。
type MonitorFileConfig struct {
	// CPUPercent 进程 CPU 百分比阈值（单核等价，可 >100）。0 = 默认。
	CPUPercent float64 `toml:"cpu_percent"`
	// MemMB 进程内存阈值（MB，runtime 口径）。0 = 默认。
	MemMB float64 `toml:"mem_mb"`
	// Goroutines goroutine 数阈值（K9：死循环 goroutine 抢占）。0 = 默认。
	Goroutines int `toml:"goroutines"`
	// IntervalSec 采样周期（秒）。0 = 默认。
	IntervalSec int `toml:"interval_sec"`
}

// Redacted 返回一个凭据已打码的副本（供日志/诊断打印：**只有这个方法输出的结构可安全
// 落日志**）。APIKey 非空 → "***"（保留"有没有设"的信息，不泄露值）；APIKeyEnv 是变量名
// （非值）可原样保留。
//
// **MCP 段同样要打码**（v3-S1）：[[mcp.servers]] 的 env / headers 里放的就是
// server 凭据——只打码 APIKey 而放过 MCP 段，等于让"可安全落日志"的承诺在最
// 容易被忽略的那一段上失效（凭据纪律 H12/H19）。
func (c FileConfig) Redacted() FileConfig {
	if strings.TrimSpace(c.APIKey) != "" {
		c.APIKey = "***"
	}
	c.MCP = c.MCP.Redacted()
	return c
}

// DefaultConfigPath 返回用户级配置文件的默认路径（H15）：
//   - Windows：%APPDATA%\alasol\config.toml；
//   - 其他平台：$XDG_CONFIG_HOME/alasol/config.toml，缺省 $HOME/.config/alasol/config.toml。
func DefaultConfigPath() (string, error) {
	if runtime.GOOS == "windows" {
		appdata := strings.TrimSpace(os.Getenv("APPDATA"))
		if appdata == "" {
			return "", errors.New("host: APPDATA 未设置，无法定位用户配置目录（Windows）")
		}
		return filepath.Join(appdata, "alasol", "config.toml"), nil
	}
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "alasol", "config.toml"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("host: 取 HOME 失败: %w", err)
	}
	return filepath.Join(home, ".config", "alasol", "config.toml"), nil
}

// LoadFileConfig 从 path 读 TOML 配置。path 为空 → 用 DefaultConfigPath()。
//
// 文件**不存在** → (nil, nil)（"没有配置文件"是合法状态，不是错误：新用户本就没有）。
// 文件存在但解析失败 → error（fail-close：损坏的配置可能正是用户指望的默认值/凭据来源，
// 静默忽略会让行为与预期不符，宁可明确报错）。
//
// **未知键 fail-close（F3）**：TOML 解析器对没被结构体认领的键是**静默忽略**——用户把
// share_project_assets 写进 [agent_types.context_strategy]（那张表只有 kind）就是这样被
// 无声吞掉的：配了 true 却毫无效果，用户以为配了。故装载期点名报错（与"坏配置当场拒"
// 同一纪律）：错误信息只含**键名**，不回显键值（凭据安全）。
//
// **错误里带合法键清单（F3 补）**：闸门本身保留（"老配置升级即拒"是既定口径），但报错要
// 给排查留路——只列坏键的话，用户还得自己去翻文档才知道**能写什么**。故错误里附：
// ① 顶层合法键清单（反射派生，永不与结构体漂移）；② 坏键**所在那张表**的合法键
// （点出"这张表只能写这些"——正是"写错段位"这类误配最需要的提示）；③ 嵌套表的位置指引。
func LoadFileConfig(path string) (*FileConfig, error) {
	p := strings.TrimSpace(path)
	if p == "" {
		var err error
		p, err = DefaultConfigPath()
		if err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("host: 读配置文件 %s: %w", p, err)
	}
	var fc FileConfig
	md, err := toml.Decode(string(data), &fc)
	if err != nil {
		// 错误里**不含**配置内容（toml 的行号/键名足够定位，不回显值——凭据安全）。
		return nil, fmt.Errorf("host: 解析配置文件 %s: %w", p, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, 0, len(undecoded))
		tableHints := make([]string, 0, len(undecoded))
		for _, k := range undecoded {
			keys = append(keys, k.String())
			if sub := legalKeysUnderUnknownKey([]string(k)); len(sub) > 0 {
				tableHints = append(tableHints, fmt.Sprintf("%s 所在那张表的合法键：%s",
					k.String(), strings.Join(sub, ", ")))
			}
		}
		msg := fmt.Sprintf("host: 配置文件 %s 有未知键：%s（fail-close：TOML 会**静默忽略**没被认领的键"+
			"——配了不生效比当场报错难排查得多；键名可能有拼写错误或写错了段位）",
			p, strings.Join(keys, ", "))
		msg += fmt.Sprintf("。合法键（顶层）：%s", strings.Join(legalFileConfigKeys(), ", "))
		for _, hint := range tableHints {
			msg += "。" + hint
		}
		msg += "（嵌套表键见 FileConfig 各表注释，如 [context_strategy]/[graph]/[sandbox]/[mcp]/[[agent_types]]）"
		// errors.New（不是 fmt.Errorf）：msg 是拼出来的常量文本，里面可能含用户键名里的 %
		// （TOML 允许带引号的键名含 %）——当格式串用会被 fmt 吃掉。
		return nil, errors.New(msg)
	}
	return &fc, nil
}

// legalFileConfigKeys 返回 FileConfig 的**顶层合法键**清单（按结构体字段序）。
//
// **反射派生，不手抄**：手抄的清单迟早与结构体漂移（加了字段忘了改清单 = 又一个"配了
// 不生效"的坑——正是这个闸门要治的病）。反射派生 = 永远与结构体一致。
func legalFileConfigKeys() []string {
	return tomlKeysOf(reflect.TypeOf(FileConfig{}))
}

// tomlKeysOf 列一个结构体类型的 toml 键（按字段序；跳过无 tag / "-" 的字段）。
func tomlKeysOf(t reflect.Type) []string {
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	out := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("toml")
		if tag == "" || tag == "-" {
			continue
		}
		if name := strings.Split(tag, ",")[0]; name != "" {
			out = append(out, name)
		}
	}
	return out
}

// tomlFieldByName 按 toml 键名找结构体字段（找不到 → false）。
func tomlFieldByName(t reflect.Type, name string) (reflect.StructField, bool) {
	if t == nil || t.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("toml")
		if tag == "" || tag == "-" {
			continue
		}
		if strings.Split(tag, ",")[0] == name {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

// legalKeysUnderUnknownKey 沿未知键的路径找到**它所在那一层**的合法键清单（找不到 → nil）。
//
// 路径 = TOML 的点分键（如 agent_types.context_strategy.share_project_assets）。逐段下钻
// （数组表 → 元素类型；指针 → 元素类型）到**最后一段的父层**：
//   - 最后一段**不在**那一层里 → 那层的合法键就是提示（"这张表只能写这些"）；
//   - 最后一段**在**那一层里（写深了一层）→ 给**子表**的合法键（更贴近现场）。
//
// 顶层键（只有一段）→ nil：顶层清单错误信息里已给全，不重复。
func legalKeysUnderUnknownKey(parts []string) []string {
	if len(parts) < 2 {
		return nil
	}
	cur := reflect.TypeOf(FileConfig{})
	for _, part := range parts[:len(parts)-1] {
		f, ok := tomlFieldByName(cur, part)
		if !ok {
			return nil
		}
		cur = f.Type
		for cur.Kind() == reflect.Slice || cur.Kind() == reflect.Ptr {
			cur = cur.Elem()
		}
		if cur.Kind() != reflect.Struct {
			return nil
		}
	}
	last := parts[len(parts)-1]
	f, ok := tomlFieldByName(cur, last)
	if !ok {
		return tomlKeysOf(cur) // 键不在这一层 → 这一层的合法键
	}
	sub := f.Type
	for sub.Kind() == reflect.Slice || sub.Kind() == reflect.Ptr {
		sub = sub.Elem()
	}
	if sub.Kind() != reflect.Struct {
		return nil
	}
	return tomlKeysOf(sub)
}

// Precedence 实现 H15 的取值优先级：**显式值（flag）> 环境变量 > 配置文件**。
// 返回选中值与来源标签（"flag" / "env" / "file" / ""=都空）。
//
// 这是优先级矩阵的**唯一实现处**（D21：口径只定义一次）——model/base_url/凭据名/
// 在线表参数等全部经它收敛，测试在此钉死 env>file。
func Precedence(flagVal, envVal, fileVal string) (value, source string) {
	switch {
	case strings.TrimSpace(flagVal) != "":
		return flagVal, sourceFlag
	case strings.TrimSpace(envVal) != "":
		return envVal, sourceEnv
	case strings.TrimSpace(fileVal) != "":
		return fileVal, sourceFile
	default:
		return "", ""
	}
}

const (
	sourceFlag = "flag"
	sourceEnv  = "env"
	sourceFile = "file"
)

// ResolveCredential 解析凭据值：**env > 文件**（H15）。envName 是持有 key 的变量名；
// fileKey 是配置文件里直填的 key。env 有值 → env 赢；否则用文件值。
//
// 凭据只在这里被取出并交给 provider，**调用方不得把它写入任何日志/事件/argv**。
func ResolveCredential(envName, fileKey string) string {
	if envName != "" {
		if v := os.Getenv(envName); strings.TrimSpace(v) != "" {
			return v
		}
	}
	return fileKey
}

// ResolveAPIKeyEnv 解析"持有 API key 的环境变量名"：flag > 文件 > 默认（OPENAI_API_KEY）。
// 只解析**名字**（不是值）——名字可安全日志化。
func ResolveAPIKeyEnv(flagVal, fileVal string) string {
	if v, _ := Precedence(flagVal, "", fileVal); v != "" {
		return v
	}
	return DefaultAPIKeyEnv
}
