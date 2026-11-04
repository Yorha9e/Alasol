package host

// 本文件是缺口 G-c「per-prompt 配置体系」+ G-d「模型面字段」的 host 装配与写路径
// （设计 15 §3，定稿禁止重讨论）：
//
//	生效链：hub 含动默认（Config）
//	          → session 配置（config.changed 折叠；本文件的 configFeed 喂 sessioncfg.Table）
//	            → 本轮快照（loop.RunOverrides 钩子在 Run 开始冻结，见 runOverridesHook）
//
// 三件事都在这里：
//
//	① configFeed：事件流 → fold 表的**拉式扫描**（照 discoveryFeed 样板：扫描水位 +
//	   重启从 0 全量重放；不用 bus 订阅——订阅会和扫描重复计数，见 host/discovery.go 头）；
//	② 写路径：PromptSpec（HTTP body 与 WS CommandSubmit 的归一形状）→ 校验（写时
//	   fail-close）→ 经 **bus.Publish** 落 config.changed（绝不直插 store，照
//	   hubAlertSink 的口径）→ 起跑；
//	③ runOverridesHook：每个 prompt 的 Run 开头重取 fold + 叠 hub 默认 → loop.RunOverrides。
//
// 依赖方向：host 引用 core/sessioncfg（core 边界：sessioncfg 不 import host）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
	"github.com/Yorha9e/alasol/core/sessioncfg"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
)

// PromptSpec 是**对话发起的归一形状**（设计 15 §3.4 钉名）：
//
//	HTTP  POST /api/sessions/{id}/prompt body  {"prompt":"…","config":{…}}
//	WS    command.submit                      {"prompt":"…","config":{…}}
//
// 两处入口共用这一个类型（同源同型，G2/P3-3：不各写一套字段）。Config 缺省 =
// 不修改配置；出现 = 修改**该 session** 的配置（本轮生效 + 持久 + 重启生效——
// 修改本身落 config.changed 事件，见 recordConfigChanged）。
type PromptSpec struct {
	// Prompt 用户 prompt 原文（空 = 拒绝）。
	Prompt string `json:"prompt"`
	// Config 是可选的 session 配置变更集（G-c/G-d 首批字段：model / reasoning_effort /
	// provider{base_url, api_key_env}；G-e 第二批：tools{only, except}）。语义见
	// contracts.ConfigChangedPayload：字段出现（含显式空串）= 覆盖、缺失 = 不变、
	// 空串 = 清除回 hub 默认；tools 组是**整组 LWW**（出现 = 整组覆盖，缺失 = 不变，
	// tools:{} = 清除回默认）。
	//
	// **挂账（G-e）**：/btw fork **不继承**父 session 的 tools 白名单——fork 出来的是
	// **新 session**，它的配置继承 **hub 默认**（与 model / reasoning_effort 同口径：
	// fork 不继承父 session 的 per-prompt 配置）。要继承得另开一刀（fork 时显式带上
	// 父 session 的当前 fold 值）。
	Config *contracts.ConfigChangedPayload `json:"config,omitempty"`
}

// ---------------------------------------------------------------------------
// ① 拉式扫描喂 fold 表（照 discoveryFeed 样板）
// ---------------------------------------------------------------------------

// configFeed 把事件流**拉式扫**进配置 fold 表（G-c；见文件头"为什么不是 bus 订阅"）。
//
// 并发：一把互斥锁保护扫描水位与表（"读水位→读事件→Apply→推水位"要整段原子——
// 表自身 Apply 幂等所以并发也不会算错，但没必要白扫）。
type configFeed struct {
	table *sessioncfg.Table
	store core.Store

	mu      sync.Mutex
	scanSeq int64
}

func newConfigFeed(store core.Store, table *sessioncfg.Table) *configFeed {
	return &configFeed{table: table, store: store}
}

// catchUp 增量扫描事件流并喂进配置 fold 表，返回本批处理条数。
//
// 与 discoveryFeed.catchUp 同构：水位只前进不后退；重启后从 0 起扫即全量重建
// （启动播种与每次 Run 的钩子都走它——写路径落完事件后也扫一次让 fold 立刻可见）。
func (f *configFeed) catchUp(ctx context.Context) (int, error) {
	if f == nil || f.table == nil || f.store == nil {
		return 0, nil // 字面量构造的 Hub（内部测试）：没有 fold 面 = 零行为
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for {
		evs, err := f.store.ReadFrom(ctx, nil, f.scanSeq+1, discoveryScanBatch)
		if err != nil {
			return total, err
		}
		if len(evs) == 0 {
			return total, nil
		}
		for i := range evs {
			f.table.Apply(&evs[i])
			if evs[i].Seq != nil && *evs[i].Seq > f.scanSeq {
				f.scanSeq = *evs[i].Seq
			}
		}
		total += len(evs)
		if len(evs) < discoveryScanBatch {
			return total, nil
		}
	}
}

// rebuild 清空 fold 表 + 水位归零，再从 seq 0 全量重放（重建入口；启动播种用
// catchUp 即可——它从 0 起扫，效果相同）。
func (f *configFeed) rebuild(ctx context.Context) error {
	if f == nil || f.table == nil || f.store == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.table.Reset()
	f.scanSeq = 0
	for {
		evs, err := f.store.ReadFrom(ctx, nil, 1, discoveryScanBatch)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			return nil
		}
		for i := range evs {
			f.table.Apply(&evs[i])
			if evs[i].Seq != nil && *evs[i].Seq > f.scanSeq {
				f.scanSeq = *evs[i].Seq
			}
		}
		if len(evs) < discoveryScanBatch {
			return nil
		}
	}
}

// ---------------------------------------------------------------------------
// ② 写路径：校验 + 落 config.changed（经 bus）
// ---------------------------------------------------------------------------

// validatePromptConfig 校验 per-prompt 配置变更集（**写时 fail-close**，G-c/G-d/G-e）。
//
// 三道闸（契约层只定形状，合法性归 host——契约注释同口径）：
//
//  1. reasoning_effort 非空必须过 ValidateReasoningEffort（G14 白名单，**唯一事实源**）
//     → 不合法返回错误，调用方回 400 / WS error 帧 **bad_config**（校验在落事件**之前**
//     → 非法配置零事件）；
//  2. provider 凭据纪律（gap2/G-d）：base_url 出现且非空 → api_key_env 必须**同时显式
//     给且非空**——不静默把 hub key 发给第三方端点。provider 两字段都空串 = 清除覆盖
//     （合法，本函数不拦）；
//  3. tools 白名单的元素纪律（G-e）：元素非空串 + 数量上限（见 validateToolList）。
func validatePromptConfig(cfg *contracts.ConfigChangedPayload) error {
	if cfg == nil {
		return nil
	}
	if cfg.ReasoningEffort != nil {
		if err := ValidateReasoningEffort(*cfg.ReasoningEffort); err != nil {
			return fmt.Errorf("config.reasoning_effort 非法（G14 白名单 low/medium/high/xhigh）: %w", err)
		}
	}
	if cfg.Provider != nil && cfg.Provider.BaseURL != nil &&
		strings.TrimSpace(*cfg.Provider.BaseURL) != "" {
		if cfg.Provider.APIKeyEnv == nil || strings.TrimSpace(*cfg.Provider.APIKeyEnv) == "" {
			return errors.New("config.provider.base_url 非空时必须同时给出非空的 api_key_env" +
				"（gap2/G-d 凭据纪律：不静默把 hub 的 key 发给第三方端点；api_key_env 只传" +
				"环境变量名，凭据值不进请求体/事件/日志）")
		}
	}
	if cfg.Tools != nil {
		if cfg.Tools.Only != nil {
			if err := validateToolList("only", *cfg.Tools.Only); err != nil {
				return err
			}
		}
		if cfg.Tools.Except != nil {
			if err := validateToolList("except", *cfg.Tools.Except); err != nil {
				return err
			}
		}
	}
	return nil
}

// maxToolWhitelistEntries 是 config.tools **单个**数组（only / except）的元素数上限
// （防滥用：白名单是"点名"语义，点名几千个工具没有意义，只会把事件流灌大）。
const maxToolWhitelistEntries = 256

// validateToolList 校验一个工具名列表（G-e；写时 fail-close）。
//
// 三条口径：
//
//  1. 元素必须是**非空串**（去首尾空白后非空）——空名是笔误（点名"什么都没有"）；
//  2. 数量上限 maxToolWhitelistEntries（防滥用）；
//  3. **重复可容忍**（不报错）：名单语义是集合，重复项无副作用（fold 侧按集合用）。
//
// **不校验工具名是否已注册**：注册表是运行期可变的（工具可注册/注销），且白名单语义
// 天然容错——点名一个当前不存在的工具 = 该工具不存在（不暴露即不存在），不是错误。
func validateToolList(field string, list []string) error {
	if len(list) > maxToolWhitelistEntries {
		return fmt.Errorf("config.tools.%s 元素数 = %d，超上限 %d（防滥用）",
			field, len(list), maxToolWhitelistEntries)
	}
	for i, name := range list {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("config.tools.%s[%d] 不能是空串（工具名必须非空）", field, i)
		}
	}
	return nil
}

// recordConfigChanged 落一条 config.changed durable 事件（G-c 写路径；HTTP/WS 两面共用）。
//
// 经 **bus.Publish**（照 hubAlertSink 的口径：**绝不直插 store**——直插会绕过 WS 广播，
// 订阅者永远收不到，入库与广播必须同源一致，G4）。envelope.session_id = 被修改配置的
// session，agent_id = 该 session 的主 agent（调用方在 ensureHosted / loopFor 之后取）；
// payload = Config 原样（只含变量名不含凭据值，H15）。
//
// **事件 id 内容寻址 + 届数**（照 alertEventID / newGateJoinedEvent 的确定性 id 样板）：
//
//	id = ShortHash("config-changed:<sid>:<gen>:<payload>")
//
// gen = fold 表里该 session 已折叠的 config.changed 条数（sessioncfg.Generation）。
// 届数必须编进 id：纯 payload 寻址下，A→B→A 的第三次提交与第一次 payload 相同 →
// 撞 id 被存储层去重吃掉 → "回到旧值"永远写不进去（fold 停在 B）。编进 gen 后，
// 后续变更是新的一届 → 新 id，A→B→A 不丢更新。
//
// **诚实的幂等边界（F2，别读宽了）**：客户端在**首次已落库之后**重试同一 payload，
// 重试时届数已前进（写前 catchUp 把新事件折进来）→ 新 gen → 新 id → **落成第二条
// 等价事件**（不被去重；fold 结果不变——同 payload LWW 同值，纯审计噪声）。幂等
// 去重只覆盖**同届同 payload** 的撞击（并发提交/存储层重试拿到同一个 id → 存储层
// ON CONFLICT 只留一条）。写前扫平是为了让 gen 按最新 fold 算：gen 落后会让"回到
// 旧值"的再提交撞上第一次的 id（A→B→A 丢更新）；扫不平就 fail-close——拿一个
// 不确定的届数造 id 比不写更糟。
//
// 落库失败 → 调用方按 500 / error 帧 **config_write_failed** 回（fail-close：不带
// 未持久化的配置开跑）。
func (h *Hub) recordConfigChanged(ctx context.Context, sid, agentID contracts.Ulid, cfg *contracts.ConfigChangedPayload) error {
	if h == nil {
		return errors.New("host: Hub 为 nil")
	}
	if cfg == nil {
		return nil
	}
	// F8：**全空变更集**（model / reasoning_effort / provider / tools 全缺失）= 不修改
	// 配置 → **不落事件**、照常受理 prompt（202）。贴"缺失 = 不变"语义：`{"config":{}}`
	// 是无操作，落一条 payload `{}` 的 durable 事件只会往唯一事实源里灌噪声（fold 结果
	// 也确实不变）。HTTP/WS 两面共用本函数 → 一处判，两面同口径。
	//
	// **tools 出现即是有变更**（G-e）：`{"config":{"tools":{}}}` = 清除回默认（全部已
	// 注册工具可见）——这是一次**实打实的配置变更**（清除动作要可审计），必须落事件。
	// 判据是"tools 对象出现"，不是"tools 里有东西"（tools:{} 也要落）。
	if cfg.Model == nil && cfg.ReasoningEffort == nil && cfg.Provider == nil && cfg.Tools == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if h.bus == nil {
		return errors.New("host: 事件总线未装配（config.changed 无处发布）")
	}
	// 写前扫平：届数（确定性 id 的判别位）要按最新 fold 算——不扫平的话，连续两次
	// 配置变更之间若没有 Run（钩子才会扫），第二次会拿到陈旧 gen。
	if _, err := h.cfgFeed.catchUp(ctx); err != nil {
		return fmt.Errorf("host: 落 config.changed 前扫事件流失败（fail-close：不拿不确定的届数造幂等 id）: %w", err)
	}
	// G-e 写路径**回填**（堵死"静默放宽"）：tools 组出现且**至少一个子项**在 →
	// 把缺失的那个子项**从当前 fold 值回填**进事件——事件于是**自含整组**。
	//
	// 为什么必须回填（静默放宽的具体形态）：tools 组是**整组 LWW**（组出现 = 整组
	// 覆盖）→ 第一次提交 `{"only":["read"]}`、第二次提交 `{"except":["bash"]}`（没带
	// only）→ 第二次的组 = {except:[bash]}（only 缺失 = 未设置）→ 可见集**静默放宽**
	// 成"全部工具减 bash"（第一轮的 only:[read] 被整组覆盖掉了）。回填后第二次的
	// 组 = {only:[read], except:[bash]} → 可见 = {read} 减 bash（与"逐项修改"的
	// 直觉一致），且**整组 LWW 的折叠语义一字不变**（事件自含整组 → fold 照旧整组
	// 覆盖）。
	//
	// 例外：`tools:{}`（**两子项都缺**）= **清除整组**（回默认）——回填不适用
	// （"清除"就是"两项都未设置"，不能拿旧值填回去）。
	cfg = backfillToolsFromFold(cfg, h.cfgTable.Current(sid).Tools)
	raw, err := json.Marshal(normalizeConfigPayload(cfg))
	if err != nil {
		return fmt.Errorf("host: 序列化 config.changed payload: %w", err)
	}
	gen := h.cfgTable.Generation(sid)
	ev := contracts.Event{
		V: 1,
		// 内容寻址（届数 + payload）：见函数头的 id 口径与**幂等边界**（重试会落第二条等价事件）。
		ID:        configChangedEventID(sid, gen, raw),
		Kind:      sessioncfg.KindConfigChanged,
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		SessionID: sid,
		AgentID:   agentID,
		Durable:   true, // 事实事件：入库、占 seq、可回放（F8）
		Payload:   raw,
	}
	if err := h.bus.Publish(ctx, ev); err != nil {
		return fmt.Errorf("host: 落 config.changed 事件: %w", err)
	}
	// 落完立刻扫平（best-effort）：让 fold 立刻可见（即便下一个 Run 的钩子也会再扫，
	// 这里先追平可让"落完立刻读"的路径拿到新配置；扫失败不致命——事件已 durable）。
	if _, err := h.cfgFeed.catchUp(ctx); err != nil {
		hubLog.Warn("host: config.changed 落库后配置 fold catch-up 失败（下一次 Run 会重试）",
			"session_id", string(sid), "err", err.Error())
	}
	return nil
}

// backfillToolsFromFold 把 tools 组里**缺失**的子项从当前 fold 值回填进变更集
// （返回副本；不改调用方的结构体）。
//
// 触发条件（G-e 静默放宽的修法）：tools 组出现且**至少一个子项**在。
// 例外：两子项都缺（`tools:{}`）= 清除整组 → 不回填。
//
// fold 侧的口径（core/sessioncfg.ToolsState）：Only/Except 是 StringListField——
// IsSet() 区分"未设置"与"显式清空（空数组）"；List() 返回副本（未设置 → nil）。
func backfillToolsFromFold(cfg *contracts.ConfigChangedPayload, cur sessioncfg.ToolsState) *contracts.ConfigChangedPayload {
	if cfg == nil || cfg.Tools == nil {
		return cfg
	}
	only, except := cfg.Tools.Only, cfg.Tools.Except
	if only == nil && except == nil {
		return cfg // tools:{} = 清除整组（回填不适用）
	}
	out := *cfg
	tools := *cfg.Tools
	if tools.Only == nil && cur.Only.IsSet() {
		v := contracts.ToolNameList(cur.Only.List()) // 显式清空（空数组）也原样带过去——"清空"≠"未设置"
		tools.Only = &v
	}
	if tools.Except == nil && cur.Except.IsSet() {
		v := contracts.ToolNameList(cur.Except.List())
		tools.Except = &v
	}
	out.Tools = &tools
	return &out
}

// normalizeConfigPayload 规整要落事件的变更集（**不改调用方的结构体**，返回副本）：
//
//	tools 的列表项若是 **nil 切片**，规整成**空切片**——json.Marshal 把 nil 切片写成
//	`null`（不是 `[]`），而 fold 侧把 `null` 读成"该项缺失"。"清空该项"（空数组）与
//	"该项未设置"（缺失）必须可区分，故这里把 nil 切片落成 `[]`（= 显式清空）。
//
// 其余字段原样透传（变更集语义：字段出现 = 覆盖、缺失 = 不变）。
func normalizeConfigPayload(cfg *contracts.ConfigChangedPayload) *contracts.ConfigChangedPayload {
	if cfg == nil || cfg.Tools == nil {
		return cfg
	}
	out := *cfg
	tools := *cfg.Tools
	out.Tools = &tools
	if tools.Only != nil && *tools.Only == nil {
		empty := contracts.ToolNameList{}
		tools.Only = &empty
	}
	if tools.Except != nil && *tools.Except == nil {
		empty := contracts.ToolNameList{}
		tools.Except = &empty
	}
	return &out
}

// configChangedEventID 派生 config.changed 事件的确定性 id（内容寻址：session +
// 届数 + payload；口径见 recordConfigChanged 的注释）。26 字符 Crockford base32
// （ULID 形态，core.ValidUlid 校验通过）。
func configChangedEventID(sid contracts.Ulid, gen int, payload []byte) contracts.Ulid {
	return contracts.Ulid(core.ShortHash(
		"config-changed:" + string(sid) + ":" + strconv.Itoa(gen) + ":" + string(payload)))
}

// ---------------------------------------------------------------------------
// ③ 本轮快照钩子（生效链第三层）+ G-d provider 覆盖构造
// ---------------------------------------------------------------------------

// runOverridesHook 返回绑在某 session 上的 loop.RunOverrides 钩子（loopFor 装配时传入）。
//
// 每次 Run 开头被调一次（**轮 = 一个 prompt 的 Run**）：
//
//	① 先扫平事件流（写路径经 bus 落的 config.changed 到此进 fold）；
//	② 取 Current(sid) 折叠态 → RunOverridesFromConfig 叠 hub 默认 + 构造 provider 覆盖
//	   （**与 cmd/alasol resume 共用的唯一取值口**：fold 字段无生效覆盖 → 返回空值，
//	   loop 侧回退到它的基值——loop 基值就是 loopFor 用 h.cfg 装配的那份）。
//
// 闭包是**无状态**的（只读 fold 表与 hub 配置）→ 并发 Run 只读共享，不破坏 loop 的
// 并发不变式。**运行中修改配置不影响已冻结的本轮**：钩子只在 Run 开头调一次，
// 之后的轮次全部读 runState 里的快照——这就是设计 §3.1"当前轮结束后下一轮生效"
// （轮 = 一个 prompt 的 Run）。
func (h *Hub) runOverridesHook(sid contracts.Ulid) func(context.Context) (loop.RunOverrides, error) {
	return func(ctx context.Context) (loop.RunOverrides, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		// ① 扫平（写路径落的事件在此进 fold；失败 fail-close——扫不平就不知道当前
		// 配置是什么，带一个不确定的配置开跑可能把请求打到错的端点/用错的模型）。
		if _, err := h.cfgFeed.catchUp(ctx); err != nil {
			return loop.RunOverrides{}, fmt.Errorf("host: 扫配置事件流（G-c 生效链）: %w", err)
		}
		if h.cfgTable == nil {
			return loop.RunOverrides{}, nil // 字面量构造的 Hub：没有 fold 面 = 无覆盖
		}
		// ② 取值口径收敛在 RunOverridesFromConfig（host/resume 共用）；provider 构造
		// 走本 hub 的缓存口（同参数复用实例）。错误带上 session 定位。
		out, err := RunOverridesFromConfig(h.cfgTable.Current(sid), h.cachedProviderOverride)
		if err != nil {
			return loop.RunOverrides{}, fmt.Errorf("host: session %s 的配置覆盖（G-c/G-d 本轮快照）: %w", sid, err)
		}
		return out, nil
	}
}

// ResolveProviderCredential 解析 G-d provider 覆盖的凭据值：**只从环境变量取**
// （H15；envName 传空也直接拒——base_url 与 api_key_env 的成对纪律）。环境变量无值
// → fail-close 报错——**绝不回退 hub 的 key**（gap2/G-d：不静默把 hub 凭据发第三方
// 端点）。
//
// **host 的 Run 钩子与 cmd/alasol resume 共用本函数**（凭据值只在此取出交给 provider，
// 不进事件/日志/argv，H12/H19）。
func ResolveProviderCredential(keyEnv string) (string, error) {
	keyEnv = strings.TrimSpace(keyEnv)
	if keyEnv == "" {
		return "", errors.New("api_key_env 为空（gap2/G-d 凭据纪律：base_url 非空必须同时给出非空的 api_key_env）")
	}
	key := ResolveCredential(keyEnv, "")
	if strings.TrimSpace(key) == "" {
		return "", fmt.Errorf("环境变量 %s 无值（provider 覆盖凭据）——fail-close，绝不回退 hub 的 key（gap2/G-d）", keyEnv)
	}
	return key, nil
}

// NewProviderOverride 按 (base_url, 已解析的 key) 构造 G-d 覆盖 provider（照 hub.go
// 装配样板：openai.New + Stream: true）。key 必须先经 ResolveProviderCredential 解析
// ——两步分开是为了 host 的构造缓存能把 key 缓进键里（env 值变了不复用旧实例）。
func NewProviderOverride(baseURL, key string) core.Provider {
	return openai.New(openai.Options{BaseURL: strings.TrimSpace(baseURL), APIKey: key, Stream: true})
}

// RunOverridesFromConfig 把 fold 出的 sessioncfg.SessionConfig 叠成 loop.RunOverrides
// （G-c/G-d/G-e 生效链第二层 → 第三层的**唯一取值口**）：
//
//   - model / reasoning_effort：取**生效覆盖**（HasValue）；空 = 返回空值，loop 侧
//     回退基值（hub 默认 / CLI -model flag——session fold 在其上，与 serve 面同一条
//     生效链）；
//   - provider：生效 base_url 非空才算有覆盖（只设了 api_key_env 而没端点 = 没有可
//     切换的端点 → 维持基值 provider，不猜）；成对纪律与凭据解析经
//     ResolveProviderCredential（fail-close，绝不回退 hub 的 key）；
//   - tools：session 工具白名单（G-e）——**整组 LWW** 的折叠态折成本轮可见性过滤
//     （见 toolVisibilityFromFold）。
//
// newProvider 是 provider 覆盖的构造口（nil = 直造 NewProviderOverride；host 传带
// 缓存的实现）。**host 的 Run 钩子与 cmd/alasol resume 共用本函数**——三面一条口径，
// 不各写一套。
func RunOverridesFromConfig(sc sessioncfg.SessionConfig, newProvider func(baseURL, keyEnv, key string) core.Provider) (loop.RunOverrides, error) {
	var out loop.RunOverrides
	if sc.Model.HasValue() {
		out.Model = sc.Model.Value
	}
	if sc.ReasoningEffort.HasValue() {
		out.ReasoningEffort = sc.ReasoningEffort.Value
	}
	// tools（G-e）：组出现过 = 本轮生效（哪怕组内两项都未设置 = 清除回默认，
	// 那也是"全部已注册工具可见"——与零值同效果，但语义上是一次显式清除）。
	if sc.Tools.Touched {
		out.Tools = toolVisibilityFromFold(sc.Tools)
	}
	if !sc.Provider.BaseURL.HasValue() {
		return out, nil
	}
	baseURL := strings.TrimSpace(sc.Provider.BaseURL.Value)
	if baseURL == "" {
		return out, nil // 空/纯空白 base_url = 无端点覆盖（含"显式清除"记录）
	}
	// 成对纪律 + 凭据解析（空 keyEnv → 成对错误；env 无值 → fail-close）。
	key, err := ResolveProviderCredential(strings.TrimSpace(sc.Provider.APIKeyEnv.Value))
	if err != nil {
		return loop.RunOverrides{}, err
	}
	keyEnv := strings.TrimSpace(sc.Provider.APIKeyEnv.Value)
	if newProvider != nil {
		out.Provider = newProvider(baseURL, keyEnv, key)
	} else {
		out.Provider = NewProviderOverride(baseURL, key)
	}
	return out, nil
}

// toolVisibilityFromFold 把 fold 出的 tools 折叠态折成本轮的工具可见性白名单（G-e）。
//
// 三态口径（与 core/sessioncfg 的 ToolsState 一致，一处口径三面同）：
//
//	组未出现过（Touched=false） → 零值（不限：全部已注册工具可见）；
//	only 未设置                 → Only=nil（不限：可见集 = 全部已注册工具）；
//	only 显式空数组             → Only=[]（**非 nil**！可见集为空 = 一个工具都不暴露）；
//	except 未设置               → Except=nil（不排除）；except 显式空数组 → Except=[]（不排除）。
//
// 工具名去首尾空白（写时校验只拦"全空白"，不静默改写用户写的名字——取值时归一，
// 与 provider.base_url 的 TrimSpace 口径同款）。
func toolVisibilityFromFold(ts sessioncfg.ToolsState) loop.ToolVisibility {
	var vis loop.ToolVisibility
	if ts.Only.Written {
		vis.Only = trimToolNames(ts.Only.Value)
	}
	if ts.Except.Written {
		vis.Except = trimToolNames(ts.Except.Value)
	}
	return vis
}

// trimToolNames 去掉工具名首尾空白，**保持非 nil**（空数组 = 显式清空该项，
// 与"该项未设置"必须可区分——nil vs 空切片）。
func trimToolNames(list []string) []string {
	out := make([]string, 0, len(list))
	for _, n := range list {
		out = append(out, strings.TrimSpace(n))
	}
	return out
}

// cachedProviderOverride 是 RunOverridesFromConfig 的 newProvider 口（host 侧）：
// 按 (base_url, key) 取/造覆盖 provider（简单 map 缓存；provMu 独立于 h.mu——钩子跑
// 在 Run 路径上，不和 h.mu 的 map 抢锁）。
//
// 缓存键含 key 的 ShortHash：环境变量值在进程生命周期内变了 → 键变 → 造新 provider
// （不复用拿着旧 key 的实例）；provider 语义只由 (base_url, key) 决定，keyEnv 名字
// 不进键。key 原值只活在 provider 内存与 env 里，不进事件/日志（H12/H19）。
// 挂账：进程内不主动失效（env 变更不热加载，重启生效）——当前无此需求，不预留接口。
func (h *Hub) cachedProviderOverride(baseURL, keyEnv, key string) core.Provider {
	cacheKey := baseURL + "\x00" + core.ShortHash(key)
	h.provMu.Lock()
	defer h.provMu.Unlock()
	if h.provCache == nil {
		h.provCache = make(map[string]core.Provider)
	}
	if p, ok := h.provCache[cacheKey]; ok {
		return p
	}
	p := NewProviderOverride(baseURL, key)
	// 只记端点与**变量名**（不记 key 值；base_url 不是凭据）。
	hubLog.Info("host: 本轮启用 provider 覆盖（G-d）", "base_url", baseURL, "api_key_env", keyEnv)
	h.provCache[cacheKey] = p
	return p
}
