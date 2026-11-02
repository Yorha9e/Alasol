package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// Approver 是 core.Approver 的 **ACP 协议面**实现（v4-2-B，K19）：把 loop 的 ask 审批
// 接到 ACP 连接的 session/request_permission 上——agent 发请求（带 options 清单）、
// client 回 outcome（ACP 官方 schema：{outcome:{outcome:"selected",optionId:"…"}}），
// 无人应答/超时/出错/未放行一切落 **deny**（fail-close，D14/H8）。
//
// 与 host 的 protocolApprover 同哲学（"等人来决定"，不自己猜），但"人"在 **ACP 客户端**
// （经 session/request_permission）而非 WS。为什么不在 host 复用 protocolApprover：它经
// transport.Broadcaster 广播给 WS 连接，而 ACP 模式下没有 WS 客户端——审批必须回到这条
// stdio 连接上。故本包自实现一个 core.Approver（task 口径：host 有 protocol approver 但它
// 的出口是 WS，ACP 场景接不上，就在 acp 包里实现）。
//
// # 绑定时序（conn 后置）
//
// Approver 必须在 host.New **之前**造好并塞进 cfg.Approver（hub 装配 loop 时捕获审批器），
// 而承载它的 conn 要到 Serve 才建。故 conn 用 bind 后置注入：Serve 建好 conn 后调
// approver.bind(conn)。Approve 只在 session/prompt（Serve 已建 conn）之后才可能被调，
// 时序安全；bind 之前被调 = conn 未就绪 = 无人可应答 → deny（fail-close）。
//
// # 超时口径（与 host 审批超时同源）
//
// Approve 的 ctx 由 loop 带下来 = session/prompt 那一轮的整体预算（endpoint 的
// turnTimeout，默认与 host promptRunTimeout 同值 10min）。本实现**不另设**审批超时——
// 整轮 ctx 到期即无人应答 → deny，与 host protocolApprover 完全同口径。
type Approver struct {
	mu   sync.Mutex
	conn *conn
}

// 编译期断言：*Approver 满足 core.Approver。
var _ core.Approver = (*Approver)(nil)

// NewApprover 造 ACP 审批器（conn 后置，见类型 doc 的"绑定时序"）。
func NewApprover() *Approver { return &Approver{} }

// bind 后置注入承载连接（Serve 调；幂等，后绑覆盖前绑）。
func (a *Approver) bind(c *conn) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.conn = c
	a.mu.Unlock()
}

// boundConn 取当前绑定连接（nil = 未就绪）。
func (a *Approver) boundConn() *conn {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conn
}

// Approve implements core.Approver。返回 error 恒为 nil：拒绝是**正常结论**（fail-close
// 的 deny），不是异常——与 terminal / protocolApprover / policy.Decide 同口径。
func (a *Approver) Approve(ctx context.Context, req contracts.ApprovalRequest) (contracts.ApprovalResponse, error) {
	if a == nil {
		return denyApproval(req, "fail-close: ACP Approver 为 nil（无人可应答）"), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// ctx 已结束：这次审批注定无人应答，连请求都不必发（fail-close）。
	if err := ctx.Err(); err != nil {
		return denyApproval(req, fmt.Sprintf("fail-close: 审批前上下文已结束（%v）", err)), nil
	}
	c := a.boundConn()
	if c == nil {
		// conn 未就绪（bind 之前就被调，不该发生）：无人可应答 → deny。
		return denyApproval(req, "fail-close: ACP 连接未建立（无人可应答）"), nil
	}

	result, rpcErr, err := c.call(ctx, methodRequestPermission, permissionParams(req))
	switch {
	case err != nil:
		// 写失败 / ctx 结束。区分"无人应答"与"已投递但未放行"（应答与 ctx 结束竞态到达时
		// call 会把载荷带回：result/rpcErr 非空即"有人答了但没赶上"）。
		if result != nil || rpcErr != nil {
			return denyApproval(req, fmt.Sprintf(
				"fail-close: 已收到应答但上下文已结束（%v）：应答已投递，未放行", err)), nil
		}
		return denyApproval(req, fmt.Sprintf("fail-close: 无人应答（ctx 超时/取消：%v）", err)), nil
	case rpcErr != nil:
		// 客户端回了 JSON-RPC 错误（而非 allow/deny outcome）：按未放行处理（fail-close）。
		return denyApproval(req, fmt.Sprintf(
			"fail-close: 客户端对 request_permission 回错误（code=%d）：%s", rpcErr.Code, rpcErr.Message)), nil
	}
	// 客户端给了成功响应：解析 outcome。只有显式 allow 才放行，其余（deny / 缺省 /
	// 无法解析 / 认不出的 optionId）一律 deny（fail-close）。
	outcome, note := parsePermissionOutcomeDetail(result)
	if outcome == outcomeAllow {
		return contracts.ApprovalResponse{
			V:         1,
			RequestID: req.ID,
			TS:        req.TS,
			Decision:  contracts.ApprovalDecisionAllow,
			Note:      strPtr(note),
			DecidedBy: strPtr("user:acp"),
		}, nil
	}
	return denyApproval(req, "fail-close: "+note), nil
}

// ---------------------------------------------------------------------------
// session/request_permission 的参数形状 + H18 脱敏
// ---------------------------------------------------------------------------

// methodRequestPermission 是审批请求的 ACP 方法名。
const methodRequestPermission = "session/request_permission"

// permissionParams 把 ApprovalRequest 折成 session/request_permission 的 params。
//
// # H18 脱敏（不发凭据值）
//
// 只带**工具名 + 人读摘要 + 副作用等级 + 关联 tool_call_id**；工具参数经 redactJSON
// 脱敏（敏感键值打码 + **值级凭据打码** + 超长截断）后才进 rawInput。**绝不原样转发**
// ApprovalRequest.Detail 里的 args——那可能含凭据（bash 命令、API 调用参数等）。
//
// 脱敏是**两道**（都是启发式，诚实边界见下）：
//
//	① 键名形状：键名命中敏感词（token/secret/password/api-key/…）→ 整个值打码；
//	② 值级形状：值**本身**像凭据（core.IsCredentialValue 同口径：sk- / ghp_ / JWT），
//	   或出现在 Authorization/Bearer 头、URL userinfo（含连接串口令段）、api-key 赋值、
//	   PEM 私钥块、AWS AKIA 长串里 → 打码。
//
// ② 是必需的：bash 的 `command` 参数键名完全不敏感，凭据却明晃晃写在值里
// （`curl -H "Authorization: Bearer sk-xxxx"`）——只按键名脱敏等于没脱敏（F9）。
//
// 诚实边界：**变形过的凭据认不出**（base64 / 拼接 / 拆片）——
// 这一层是启发式，不是 DLP。（自定义 scheme 的鉴权头**能**遮住值：F1 修后按"词的
// 个数"定位值，不再押在 scheme 白名单上。）故外发面仍是**参数摘要**（不是完整回放）：
// 审批人看到的是"在调什么工具 + 脱敏后的关键参数"。
func permissionParams(req contracts.ApprovalRequest) permissionRequest {
	toolName, args := extractToolInfo(req.Detail)
	pr := permissionRequest{
		SessionID: string(req.SessionID),
		Trigger:   string(req.Trigger),
		ToolCall: permissionToolCall{
			ToolCallID: derefProviderID(req.ToolCallID),
			Title:      toolName,
			Summary:    req.Summary,
			RawInput:   redactJSON(args),
		},
		// 选项列表（ACP schema）：客户端**照着这份清单回** optionId（F2）。
		Options: permissionOptions(),
	}
	if req.SideEffect != nil {
		pr.ToolCall.SideEffect = string(*req.SideEffect)
	}
	if toolName == "" {
		// 拿不到工具名时用摘要兜底（审批人至少看到"要审什么"的一句话）。
		pr.ToolCall.Title = req.Summary
	}
	return pr
}

// permissionRequest 是 session/request_permission 的 params 形状（ACP v1）。
type permissionRequest struct {
	SessionID string             `json:"sessionId"`
	Trigger   string             `json:"trigger,omitempty"`
	ToolCall  permissionToolCall `json:"toolCall"`
	// Options 是给客户端选的**选项清单**（ACP schema 必有字段）。
	//
	// 为什么必须有（F2）：规范客户端拿到 request_permission 后**照着 options 回**
	// {outcome:{outcome:"selected", optionId:"…"}}——请求侧不带 options，它就没有
	// optionId 可回，审批闭环直接断掉（此前的私有词表 allow/deny 只有自写客户端认）。
	Options []permissionOption `json:"options"`
}

// permissionOption 是一个可选项（ACP schema：{optionId, name, kind}）。
type permissionOption struct {
	// OptionID 是**回指用的标识**（客户端应答时原样带回）。
	OptionID string `json:"optionId"`
	// Name 是给人看的名字（客户端 UI 展示）。
	Name string `json:"name"`
	// Kind 是语义分类（ACP 惯例：allow_once / allow_always / reject_once）。
	Kind string `json:"kind"`
}

// 选项词表（optionId 是**本实现与客户端的约定**——客户端原样回带即可；kind 走 ACP 惯例）。
const (
	optionIDAllowOnce   = "allow-once"
	optionIDAllowAlways = "allow-always"
	optionIDRejectOnce  = "reject-once"

	kindAllowOnce   = "allow_once"
	kindAllowAlways = "allow_always"
	kindRejectOnce  = "reject_once"
)

// permissionOptions 造选项清单（每次请求都给同一份——固定三选一够闭环了）。
//
// "始终允许"的诚实边界：**本次请求有效、不落策略**——本实现**不记住**"以后都放行"
// （没有持久化的放行名单，每次请求照旧问客户端；客户端一直回 allow-always 就一直放行）。
// 哪些工具会走到审批面，仍由策略链（allow/ask 白名单）决定。
func permissionOptions() []permissionOption {
	return []permissionOption{
		{OptionID: optionIDAllowOnce, Name: "允许一次", Kind: kindAllowOnce},
		{OptionID: optionIDAllowAlways, Name: "始终允许", Kind: kindAllowAlways},
		{OptionID: optionIDRejectOnce, Name: "拒绝", Kind: kindRejectOnce},
	}
}

type permissionToolCall struct {
	ToolCallID string          `json:"toolCallId,omitempty"`
	Title      string          `json:"title"`
	Summary    string          `json:"summary,omitempty"`
	SideEffect string          `json:"sideEffect,omitempty"`
	RawInput   json.RawMessage `json:"rawInput,omitempty"`
}

// extractToolInfo 从 ApprovalRequest.Detail（policy.approvalDetail 造的形状
// {tool, rule_id, args, suggested_rules}）里取工具名与参数。解不开 → 空工具名 + 空参数
// （调用方用摘要兜底，不阻塞审批）。
func extractToolInfo(detail json.RawMessage) (string, json.RawMessage) {
	if len(detail) == 0 {
		return "", nil
	}
	var d struct {
		Tool string          `json:"tool"`
		Args json.RawMessage `json:"args"`
	}
	if err := json.Unmarshal(detail, &d); err != nil {
		return "", nil
	}
	return d.Tool, d.Args
}

// 审批响应 outcome 的取值（只有 allow 放行，其余一律 deny——fail-close）。
const (
	outcomeAllow = "allow"
	outcomeDeny  = "deny"
)

// permissionResponse 是客户端对 session/request_permission 的响应形状。
//
// 两套写法都吃（F2）：
//
//	① ACP 官方 schema：{"outcome":{"outcome":"selected","optionId":"allow-once"}}
//	   （另一支：{"outcome":{"outcome":"cancelled"}} = 用户撤了）
//	② 旧私有词表（本仓老测试客户端）：{"outcome":"allow"} / {"decision":"allow"}
//
// outcome 用 RawMessage：两种写法的形状差一个层级，只能原样解一遍再判。
type permissionResponse struct {
	Outcome  json.RawMessage `json:"outcome"`
	Decision json.RawMessage `json:"decision"`
}

// acpOutcome 是 ACP 官方的 outcome 对象（"outcome" 里的 tagged union）。
type acpOutcome struct {
	// Outcome 是 "selected"（用户选了某个 optionId）或 "cancelled"（用户撤了）。
	Outcome string `json:"outcome"`
	// OptionID 是被选中的选项（selected 时必填）。
	OptionID string `json:"optionId"`
}

// parsePermissionOutcome 解析客户端响应，返回规范化 outcome（非 allow 即 deny，fail-close）。
func parsePermissionOutcome(result json.RawMessage) string {
	outcome, _ := parsePermissionOutcomeDetail(result)
	return outcome
}

// parsePermissionOutcomeDetail 解析客户端响应，返回 (规范化 outcome, 人读说明)。
//
// 说明进审批审计（contracts.ApprovalResponse.Note）——"为什么被放行/拒绝"要能查。
func parsePermissionOutcomeDetail(result json.RawMessage) (string, string) {
	if len(result) == 0 {
		return outcomeDeny, "客户端回空结果"
	}
	var r permissionResponse
	if err := json.Unmarshal(result, &r); err != nil {
		return outcomeDeny, "客户端结果解不开 JSON"
	}
	// ① ACP 官方 schema：{"outcome":{"outcome":"selected","optionId":"…"}}。
	if len(r.Outcome) > 0 {
		var o acpOutcome
		if err := json.Unmarshal(r.Outcome, &o); err == nil && o.Outcome != "" {
			switch normOutcome(o.Outcome) {
			case "selected":
				if outcomeDecision(o.OptionID) == outcomeAllow {
					return outcomeAllow, "客户端选了 " + o.OptionID
				}
				return outcomeDeny, "客户端选了非放行选项 " + o.OptionID
			case "cancelled":
				return outcomeDeny, "客户端撤回了审批"
			case outcomeAllow:
				// 旧私有词表的嵌套写法（兼容老客户端）：{"outcome":{"outcome":"allow"}}。
				return outcomeAllow, "客户端回了 allow（嵌套写法）"
			default:
				return outcomeDeny, "客户端回了未知 outcome " + o.Outcome
			}
		}
	}
	// ② 旧私有词表：{"outcome":"allow"} / {"decision":"allow"}。
	if normOutcome(rawString(r.Outcome)) == outcomeAllow || normOutcome(rawString(r.Decision)) == outcomeAllow {
		return outcomeAllow, "客户端回了 allow"
	}
	return outcomeDeny, "客户端未放行（outcome 非 allow）"
}

// outcomeDecision 把 optionId 映射成 allow / deny。
//
// 只有两个显式放行项放行（allow-once / allow-always）；**认不出的 optionId 一律 deny**
// （fail-close：客户端回了个我们没给过的选项 = 不认识 = 不放行）。
func outcomeDecision(optionID string) string {
	switch normOutcome(optionID) {
	case optionIDAllowOnce, optionIDAllowAlways:
		return outcomeAllow
	default:
		return outcomeDeny
	}
}

// rawString 取 RawMessage 里的字符串值（解不出 → 空串）。
func rawString(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func normOutcome(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// ---------------------------------------------------------------------------
// H18 脱敏 + fail-close deny 助手
// ---------------------------------------------------------------------------

// sensitiveKeySubstrings 是触发脱敏的键名子串（大小写不敏感）。
//
// 保守取向：宁可多打码（把非凭据的值也藏了）也不漏放凭据值。都是高特异词，误伤面小。
// "api-key"（连字符）与 "api_key"（下划线）都要：JSON 键 `api-key` 两个下划线词都套不中
// （加固补的漏网场景之一），子串命中才遮得住。
var sensitiveKeySubstrings = []string{
	"secret", "token", "password", "passwd", "pwd",
	"credential", "apikey", "api_key", "api-key", "auth", "bearer", "private",
}

// maxRedactedValueLen 是脱敏后单个字符串值的长度上限（防一条巨值把帧撑爆）。
const maxRedactedValueLen = 512

// 值级脱敏的正则（F9：脱敏不能只看键名）。
//
//	① Authorization: <scheme> <value> / authorization=<value> → **值**打码（scheme 留着）
//	② 参数化鉴权头 response= / Credential= / Signature= / token= / api-key: … → **值**打码
//	③ Bearer <token>                                        → token 打码
//	④ URL userinfo https://user:pass@host                    → 口令打码（用户名留着）
//	⑤ PEM 私钥块（-----BEGIN … -----…-----END … -----）       → 整块打码
//	⑥ AWS 风格访问 key id（AKIA/ASIA/… + 16 位大写长串）      → 打码
//
// 凭据**形状**（sk- / ghp_ / JWT）不重复写正则：复用 core.IsCredentialValue（同一事实源），
// 由 maskSecretTokens 按"词元"扫（core 那个是整串匹配，扫不出嵌在命令里的凭据）。
//
// # ① 的写法为什么是"scheme 可选 + 打码值"（别写反）
//
// 头形状是 `Authorization: <scheme> <value>`（scheme = Bearer/Basic/Token/…，**值在
// scheme 后面**）。若把 scheme 当成"值"打码（`Authorization: *** abc123`），真凭据
// **明文外发**——而且因为 scheme 被打掉，后面的 Bearer 规则再也匹配不到（连带把
// `Bearer <token>` 那条也废了）。故：scheme 单独一组（**可选**），**打码第三组（值）**，
// 替换串 `${1}${2}***`——scheme 留着（审批人看得懂是哪种鉴权），值没了。
//
// # ① 的第二组为什么**不靠 scheme 白名单**（F1 修）
//
// 旧实现把"哪一词是 scheme"押在白名单（bearer/basic/token/digest/negotiate/oauth）上：
// 白名单**外**的 scheme（`Authorization: Custom abc123def456`、
// `Authorization: AWS4-HMAC-SHA256 Credential=AKIA…`）会让第三组吃掉 **scheme 本身**，
// 真值留明文外发——白名单既是"漏出口"又是"维护负担"（每加一种 scheme 就漏一次）。
//
// 故第二组改成**看词的个数、不看 scheme 是什么**：
//
//	冒号后**有第二个词** → 第二个词是值（遮它），第一个词当 scheme 留着给人看；
//	冒号后**只有第一个词** → 它就是值（遮它）。
//
// 诚实边界：这是**启发式**——"只有一个词"与"有个怪 scheme"分不开，故
// `Authorization: <值> <其它词>` 这种（值后面还有词、且值本身不是 scheme 形状）仍可能
// 认不出；变形凭据同样认不出（见"诚实边界"一节）。
var (
	// reAuthzValue 匹配 `Authorization: <scheme> <value>`（scheme 可省）。
	//
	// 三组：① `authorization:` / `authorization=` 及其空白 ② scheme（含尾随空白，可选）
	// ③ **值**（非空白、非引号/逗号/分号的一段；引号包住的那一整段也算值）。
	//
	// ② **不认白名单**：任何"词"都能当 scheme（见上面的"为什么"）。
	reAuthzValue = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)((?:[^\s"',;]+\s+)?)("[^"]*"|[^\s"',;]+(?:"[^"]*")?)`)
	// reAuthzParam 匹配**参数化**鉴权头里的敏感参数（response= / Credential= /
	// **收窄记录（critic F4/F5，如实）**：①B 档通用词（response/signature/nonce/cnonce/opaque/pwd）
	// 只认**紧贴** name=value——`response = "abc"`（= 两侧带空白）与 `pwd: xxx`（冒号+空格）**不遮**
	// （旧单档曾遮 = 两侧空白那档，此处是刻意收窄换误伤面）；②截断 PEM（缺 END）的 body 行认定
	// 有 >=16 字符门槛——尾行短残余（<=15 字符 ≈ 11 字节密钥料）不遮。
	// Signature= / token= / api-key: …）。
	//
	// 为什么单加一条：`Authorization: Digest username="u", response="abc…"` 这种头
	// **没有**"scheme 值"那半——凭据藏在参数里，reAuthzValue 只能遮住第一个词
	// （`username="u"`），真正的 `response="abc…"` 会留明文。
	//
	// 参数名取高特异的一小撮（与 JSON 键名脱敏同哲学：宁可多打码）。**刻意不含**
	// authorization/auth/bearer——那是**头名/方案名**不是凭据（遮它会把 scheme 一起
	// 打掉，审批人就看不出是哪种鉴权了）。
	//
	// 名字**不锚词首**（无 \b）：`auth_token=` / `x_token=` 这类带前缀的参数名也要遮
	// （带前缀才是常见写法）；锚了词首反而漏。
	//
	// # 两档名单（收紧误伤面：散文不该被整段打码）
	//
	// 旧实现一张名单 + `[:=]` 分隔 + 值前允许空白 → **普通散文被误伤**：
	// "The response: 200 OK" 里的 "response: 200" 被当成凭据参数打码（response 本是
	// 通用词）。故拆成两档：
	//
	//	**A 档（高特异凭据名）**：secret/password/passwd/credential/apikey/api_key/
	//		api-key/token/access_token/refresh_token/id_token —— 分隔符 `[:=]`、值前
	//		**允许空白**（`api-key: hunter2` 这类冒号赋值是常见真凭据形态，加固时专门
	//		补的覆盖，不能因收紧误伤而丢）。
	//	**B 档（通用词）**：response/signature/nonce/cnonce/opaque/pwd —— 只认**紧贴**的
	//		`name=value` 形态（分隔符前后**都没有空白**）。Digest 鉴权的
	//		`response="abc…"` 正是这个形态（照旧遮住）；散文里的 "The response: 200 OK"
	//		（冒号 + 空格）**不再**被当成凭据。
	//
	// 诚实边界：B 档的 `pwd: xxx`（冒号 + 空格）不再遮——这是收紧误伤的**代价**，
	// 如实记在这里（要遮就写 `pwd=xxx`；`password:` 仍在 A 档里照遮）。
	reAuthzParam = regexp.MustCompile(`(?i)((?:secret|password|passwd|credential|apikey|api_key|api-key|token|access_token|refresh_token|id_token)\s*[:=]\s*)("[^"]*"|[^\s"',;]+)`)
	// reAuthzParamTight 是 B 档（通用词）的**紧贴**形态：名字与分隔符之间、分隔符与值
	// 之间都**不许有空白**（`response="abc"` 遮；`The response: 200 OK` 不遮）。
	reAuthzParamTight = regexp.MustCompile(`(?i)\b(response|signature|nonce|cnonce|opaque|pwd)([=])("[^"]*"|[^\s"',;]+)`)
	reBearerValue     = regexp.MustCompile(`(?i)(\bbearer\s+)([A-Za-z0-9._~+/=-]{8,})`)
	reURLUserinfo     = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://[^/\s:@]+:)([^/\s:@]*)(@)`)
	// rePEMBlockHead 匹配 PEM 块的**头**（"-----BEGIN <类型>-----"）。
	// rePEMBlockTail 匹配 PEM 块的**尾**（"-----END <类型>-----"）。
	//
	// 完整块（头尾都有）→ 整块打码（body 是密钥，必须遮）。
	// **缺 END**（被截断的块）→ 旧实现用 `$` 兜底**吞掉整段余文**（误伤面极大：一行
	// 散文里的 "-----BEGIN" 能把后面所有内容都吃掉）。新口径见 redactPEMBlocks：
	// 截断块只吞**到行尾**（+ 其后连续的 base64 行 = 真正的密钥体），且**限长 8KB**。
	rePEMBlockHead = regexp.MustCompile(`-----BEGIN [^-]+-----`)
	rePEMBlockTail = regexp.MustCompile(`-----END [^-]+-----`)
	// reAWSKeyID 匹配 AWS 风格访问 key id：4 字母前缀（AKIA 长期/ASIA 临时/…）+ 16 位
	// 大写字母数字 = 20 位整串（AWS 文档示例 AKIAIOSFODNN7EXAMPLE 的形状）。词边界锚定：
	// 无 \b 会把 `XAKIA…` 这类词中串也吃掉（没必要的误伤）。
	reAWSKeyID    = regexp.MustCompile(`\b(?:AKIA|ASIA|AIDA|AROA|AGPA|AIPA|ANPA|ANVA)[0-9A-Z]{16}\b`)
	reSecretToken = regexp.MustCompile("[^\\s\"'`,;()\\[\\]{}<>]+")
)

// maxPEMFallbackBytes 是**截断 PEM 块**（缺 END）的兜底遮罩长度上限（8KB）。
//
// 为什么限长：旧实现用 `$` 兜底（吞到串尾）——一行散文里的 "-----BEGIN" 能把后面
// 几十 KB 的余文全吃掉（误伤面失控）。限长 = 最多吞 8KB，其余原样保留。
const maxPEMFallbackBytes = 8 * 1024

// redactPEMBlocks 把 PEM 块（-----BEGIN … -----…-----END … -----）整块打码。
//
// 两档口径：
//
//	**完整块**（有 END）→ 从 BEGIN 头到 END 尾**整块**打码（body 是密钥，必须遮）。
//	**截断块**（缺 END）→ 只吞**到行尾**（BEGIN 头所在行）+ 其后**连续的 base64 行**
//	  （那才是真正的密钥体），并**限长 maxPEMFallbackBytes（8KB）**。
//
// 为什么截断块不再"吞到串尾"：那是误伤面（散文里一个 "-----BEGIN" 能吃掉整段余文）。
// 诚实边界：截断块里**不是** base64 形态的密钥体（少见）认不出来——宁可少遮，也不
// 吞掉整段余文。
func redactPEMBlocks(s string) string {
	if !strings.Contains(s, "-----BEGIN ") {
		return s
	}
	var b strings.Builder
	rest := s
	for {
		loc := rePEMBlockHead.FindStringIndex(rest)
		if loc == nil {
			b.WriteString(rest)
			return b.String()
		}
		b.WriteString(rest[:loc[0]])
		rest = rest[loc[0]:]
		if tail := rePEMBlockTail.FindStringIndex(rest); tail != nil {
			// 完整块：头 → 尾整块打码（含 END 标记本身）。
			b.WriteString("***")
			rest = rest[tail[1]:]
			continue
		}
		// 截断块：吞到行尾 + 其后连续的 base64 行，限长 8KB。
		n := truncatedPEMSpan(rest)
		b.WriteString("***")
		rest = rest[n:]
	}
}

// truncatedPEMSpan 算"截断 PEM 块"要吞掉多长：从块首起，吞掉 BEGIN 头所在行，
// 再吞其后**连续的 base64 行**（真密钥体），总长限 maxPEMFallbackBytes。
func truncatedPEMSpan(block string) int {
	consumed := len(block)
	if i := strings.IndexByte(block, '\n'); i >= 0 {
		consumed = i + 1 // 含换行
	}
	rest := block[consumed:]
	for consumed < maxPEMFallbackBytes && rest != "" {
		i := strings.IndexByte(rest, '\n')
		line := rest
		if i >= 0 {
			line = rest[:i]
		}
		if !isPEMBodyLine(line) {
			break // 不是 base64 行 = 块体结束（后面的余文**不吞**）
		}
		if i < 0 {
			consumed += len(line)
			break
		}
		consumed += i + 1
		rest = rest[i+1:]
	}
	if consumed > maxPEMFallbackBytes {
		consumed = maxPEMFallbackBytes
	}
	return consumed
}

// isPEMBodyLine 报告一行是否是 PEM 体（base64 行）：非空、全是 base64 字符、长度 ≥ 16。
func isPEMBodyLine(line string) bool {
	if len(line) < 16 {
		return false
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '+', c == '/', c == '=':
		default:
			return false
		}
	}
	return true
}

// redactJSON 脱敏一段参数 JSON：递归走查，敏感键的值打码 "***"，**值里的凭据**也打码，
// 超长字符串截断。
//
// 输入不是合法 JSON → 返回 nil（**不转发**无法解析的原始参数——宁缺毋滥）。
// 这是**参数摘要**，不是完整回放：审批人要看清"在调什么工具、关键参数是什么"，
// 但不该看到凭据值（H18）。
func redactJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	out, err := json.Marshal(redactValue(v))
	if err != nil {
		return nil
	}
	return out
}

func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if isSensitiveKey(k) {
				out[k] = "***"
			} else {
				out[k] = redactValue(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactValue(e)
		}
		return out
	case string:
		// 值级脱敏**先于**截断（先打码再截断：反过来的话截断可能把凭据切一半放出去）。
		return truncateStr(redactSecretText(t), maxRedactedValueLen)
	default:
		return v
	}
}

// redactSecretText 把自由文本（命令行 / URL / 请求头）里的凭据打码。
//
// 七步（顺序有意义）：
//
//	① Authorization 头**值**打码（scheme 留着：`Authorization: Bearer ***`）；② 参数化
//	   鉴权头的敏感参数打码（`response=***` / `api-key: ***`——分隔符 = 或 :；通用词走
//	   **紧贴**形态 `name=value`，见 reAuthzParam 的两档名单）；③ Bearer
//	   token 打码；④ URL userinfo 的口令打码（含连接串 `postgres://user:***@`——同一
//	   规则吃任意 scheme）；⑤ PEM 私钥块打码（**完整块整块遮；截断块只遮到行尾 + 限长 8KB**——
//	   见 redactPEMBlocks）；⑥ AWS 风格
//	   访问 key id 打码（AKIA… 等 20 位整串）；⑦ 词元扫描：core.IsCredentialValue 同
//	   口径的形状（sk- / ghp_ / JWT），外加 "key=value" 形态里=value 那半（同
//	   runtime/cap/remote 的 isCredentialArg 口径）。
//
// 诚实边界：**尽力而为**的启发式——变形过的凭据（base64 / 拼接 / 拆片）与**没见过的
// 新形态**都认不出；真正防线是凭据不进日志/事件（H15），本函数只是外发面前的兜底。
//
// **误伤面也如实说**：② 的 B 档（通用词 response/signature/nonce/opaque/pwd）只认紧贴
// 形态，故散文里的 "The response: 200 OK" 不再被整段打码；代价是 `pwd: xxx`（冒号 +
// 空格）不再遮（写 `pwd=xxx` 才遮，`password:` 照遮）。
func redactSecretText(s string) string {
	if s == "" {
		return s
	}
	// ① Authorization：**打码值**（第三组），scheme（第二组，可选）留着。
	//    替换串不能写成 "${1}***"——那会把 scheme 当值打掉，真凭据反而明文外发。
	out := reAuthzValue.ReplaceAllString(s, "${1}${2}***")
	// ② 参数化鉴权头：敏感参数（response= / Credential= / api-key: …）的值打码
	//    ——这种头里没有"scheme 值"那半（① 只能遮住第一个词），值藏在参数里。
	out = reAuthzParam.ReplaceAllString(out, "${1}***")
	out = reAuthzParamTight.ReplaceAllString(out, "${1}${2}***")
	out = reBearerValue.ReplaceAllString(out, "${1}***")
	out = reURLUserinfo.ReplaceAllString(out, "${1}***${3}")
	// ⑤⑥ 加固（缺口 7 脱敏补漏）：PEM 私钥块 / AWS 访问 key id——两者的 body/keyid
	//    都不具 sk-/ghp_ 形状，词元扫描兜不住，必须立规则。
	//    PEM：完整块整块遮；**截断块（缺 END）只遮到行尾 + 限长 8KB**（旧实现用 `$`
	//    兜底吞掉整段余文——误伤面失控）。
	out = redactPEMBlocks(out)
	out = reAWSKeyID.ReplaceAllString(out, "***")
	return maskSecretTokens(out)
}

// maskSecretTokens 按"词元"扫自由文本，把凭据形状的词元换成 "***"。
//
// 词元 = 不含空白/引号/括号的连续字符（命令行的最小单元）。判定口径与
// core.IsCredentialValue 一致（**整串**匹配），另加一条："key=value" 形态取 value
// 那半单独判（与 runtime/cap/remote 的 isCredentialArg 同款）。
func maskSecretTokens(s string) string {
	return reSecretToken.ReplaceAllStringFunc(s, func(tok string) string {
		if core.IsCredentialValue(tok) {
			return "***"
		}
		if i := strings.IndexAny(tok, "=:"); i >= 0 {
			if core.IsCredentialValue(strings.TrimSpace(tok[i+1:])) {
				return tok[:i+1] + "***"
			}
		}
		return tok
	})
}

// sanitizeErrSummary 把错误压成一行、可外发的摘要（进 JSON-RPC 错误帧的 message）。
//
// 三件事：① 凭据形状打码（复用 redactSecretText）；② 截断（防一条巨错误撑爆帧）；
// ③ 控制字符/换行折成空格（一行一帧的纪律：错误消息里带换行会让人眼读错帧边界）。
const maxErrSummaryLen = 200

func sanitizeErrSummary(err error) string {
	if err == nil {
		return ""
	}
	msg := redactSecretText(err.Error())
	msg = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 {
			return -1
		}
		return r
	}, msg)
	if len(msg) > maxErrSummaryLen {
		msg = msg[:maxErrSummaryLen] + "…"
	}
	return msg
}

func isSensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	for _, s := range sensitiveKeySubstrings {
		if strings.Contains(lk, s) {
			return true
		}
	}
	return false
}

func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…[已截断]"
}

// denyApproval 造一条 fail-close 拒绝响应（形状对齐 core.DenyAll / terminal）。
func denyApproval(req contracts.ApprovalRequest, note string) contracts.ApprovalResponse {
	return contracts.ApprovalResponse{
		V:         1,
		RequestID: req.ID,
		TS:        req.TS,
		Decision:  contracts.ApprovalDecisionDeny,
		Note:      strPtr(note),
		DecidedBy: strPtr("auto:acp-fail-close"),
	}
}

func strPtr(s string) *string { return &s }

func derefProviderID(p *contracts.ProviderId) string {
	if p == nil {
		return ""
	}
	return string(*p)
}
