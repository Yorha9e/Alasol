package openai

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/retry"
)

// DefaultBaseURL 是 OpenAI 官方 chat completions 端点。
// 接本地网关 / Azure 兼容端点时由装配方覆盖（Options.BaseURL）。
const DefaultBaseURL = "https://api.openai.com/v1"

// chatCompletionsPath 是 chat completions 的相对路径（拼在 BaseURL 之后）。
const chatCompletionsPath = "/chat/completions"

// maxErrorBody 是 HTTP 错误响应体进入错误文案前的截断上限（1KB）：
// 网关的 5xx 页面可能几百 KB，全塞进事件/日志会把它们撑爆。
const maxErrorBody = 1024

// maxDrainBody 是排空（drain）响应体的上限（64KB）。
//
// 为什么不是 maxErrorBody（1KB）：排空的目的**不是**取内容，是让**连接能复用**——
// Go 的 transport 只有在响应体被**读完**时才把连接放回空闲池。错误体常有几十 KB
// （网关的 5xx 页面），按 1KB 截断排空 = 排不干净 = 连接用不上 → 每次重试都新建一条
// TCP+TLS（重试反而更慢），句柄也越用越多。64KB 是"绝大多数错误体都能排干净"与
// "别为一个巨型错误体白读几百 MB"之间的折中：超过 64KB 的体排不干净 → 该连接
// 不复用（直接关掉，不泄漏）。
const maxDrainBody = 64 * 1024

// 流式事件的 kind（**core 是唯一事实源**：core.ProviderEvent.Kind 的 v0 子集，I3 的
// 开放字符串）。这里只做**别名**——loop / UI 侧对照 core.Kind* 常量或本包的同名常量，
// 不再各写一份字面量（改值时只改 core 一处）。
const (
	// KindTextDelta 增量文本（Text=片段）。
	KindTextDelta = core.KindTextDelta
	// KindReasoningDelta 增量思维链（Reasoning=片段；G14）。与 KindTextDelta 同层级：
	// 都是"边收边发"的增量，消费端按 Kind 分流到正文/思维链两个显示区。
	KindReasoningDelta = core.KindReasoningDelta
	// KindToolCallStart 一个工具调用开始（ToolCall.ID / ToolCall.Name 已知，Args 可能还空）。
	KindToolCallStart = core.KindToolCallStart
	// KindToolCallDelta 工具调用参数片段（ToolCall.ID 已锁定，Args=**这一个片段**的原始字节）。
	KindToolCallDelta = core.KindToolCallDelta
	// KindDone 正常结束（Text=最终全文，ToolCall=nil）。
	KindDone = core.KindDone
	// KindError 失败（Text=错误文案）；返回前必发，发完照常返回 error。
	KindError = core.KindError
)

// Options 是 Provider / Embedder 的装配参数（零值可用：BaseURL 落默认值、无 APIKey、
// 无 Model、无流式）。各字段对两个装配体的含义见字段注释（Model 只对 Embedder 有意义，
// Stream 只对 Provider 有意义）。
type Options struct {
	// BaseURL 是 OpenAI 兼容端点根路径（默认 DefaultBaseURL）。
	// 尾部的 "/" 会被去掉，避免拼出 "//chat/completions"。
	BaseURL string

	// APIKey 是 Bearer 凭据。**由装配方从 env 取出后传入**——本包不读 env（见包 doc）。
	// 为空时不下发 Authorization 头（本地网关常不需要）。
	APIKey string

	// Model 是 **embeddings** 的模型名（仅 Embedder 使用：POST /embeddings 请求体的
	// model 字段）。chat completions 的模型名走 core.ModelRequest.Model（按请求传），
	// 与本字段互不相干——Provider.Run 不读它。为空时 Embedder 照发请求，让端点去拒
	//（与"缺参数后兜底 / 让端点拒绝"的包口径一致）。
	Model string

	// HTTPClient 可注入（测试用 httptest.Server，或调用方想统一设置超时/代理/传输层）。
	//
	// **两个装配体的 nil 语义刻意不同**：
	//   - Provider（chat）：nil → 用**不带超时**的 *http.Client，超时归调用方 ctx 管
	//     （见包 doc「超时」）；
	//   - Embedder（embeddings）：nil → 新建一个带 **30s 超时**的 client（见
	//     embed.go 的 defaultEmbedTimeout）——embeddings 是单次短调用，而宿主 RAG 面的
	//     调用方 ctx（HTTP 请求的 ctx）没有 deadline 兜底。
	HTTPClient *http.Client

	// Stream 是否走 SSE 流式（默认 false=非流式）。两种模式返回的 ModelResult 形状一致。
	Stream bool
}

// Provider 是 core.Provider 的 OpenAI 兼容实现。
//
// 并发安全：无共享可变状态，可多 goroutine 同时 Run。
type Provider struct {
	baseURL string
	apiKey  string
	client  *http.Client
	stream  bool
}

// 编译期断言：Provider 必须是 core.Provider（改签名立刻编译失败，而不是到 loop 层才炸）。
var _ core.Provider = (*Provider)(nil)

// New 构造 Provider。零值 Options 合法（默认官方 BaseURL、无凭据、非流式）。
//
// nil 语义：不 panic——缺参数后用更安全的默认值顶上（BaseURL 兜底、无凭据时不下发
// Authorization 头）。这与 core/doc.go 的「兜底」一档同口径：缺参数后系统仍能安全地
// 做事（发请求，让端点去拒绝），不存在"不放行任何东西"的问题。
func New(o Options) *Provider {
	base := normalizeBaseURL(o.BaseURL)
	if base == "" {
		base = DefaultBaseURL
	}
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	return &Provider{
		baseURL: base,
		apiKey:  strings.TrimSpace(o.APIKey),
		client:  client,
		stream:  o.Stream,
	}
}

// BaseURL 返回实际使用的端点根路径（测试/诊断用）。
func (p *Provider) BaseURL() string {
	if p == nil {
		return ""
	}
	return p.baseURL
}

// Run implements core.Provider：一次 Run = 一次 chat completions 调用。
//
// 返回的 ModelResult 与 emit 的事件描述**同一次**调用：流式下事件是增量、Result 是
// 组装后的全量；非流式下事件是"一次性补发"、Result 直接来自响应体。
//
// # 重试（v4-4-A，07 M6/M7）
//
// 可重试的传输层失败（HTTP 429/5xx、网络瞬断）在**本方法内部**经 core/retry 小重试
// （3 次尝试，指数退避 + 抖动；口径见包 doc「重试」）。因此：
//
//   - 重试期间**不发** error 事件——只有最终失败才 emit 一条（UI 不该看到"抖了一下又
//     好了"的红色闪烁）；
//   - 重试只覆盖"发请求 → 拿到响应状态行"这一段。响应体一旦开始解析（流式已发出第一条
//     delta、或非流式已在解码）就**绝不重试**：半截流重试会双发 delta，非流式重发则可能
//     让端点把同一轮补全算两次钱；
//   - ctx 取消不重试（取消是调用方的意图，不是瞬断）；
//   - **服务端建议的等待被采信**：429/503 的 Retry-After 头会被解析并参与退避计算
//     （Retry-After ≤ MaxBackoff 时实际等待 = max(计算退避, RetryAfter)；Retry-After
//     > MaxBackoff 时**停重试**——服务端要的窗口比我们愿意等的上限还长，再试也是撞在
//     同一个限流窗口里（还会加剧限流），窗口随错误抛给调用方；口径见包 doc「等待节奏」）；
//   - 重试过仍失败、或重试后撞上不可重试的错误 → 返回 core/retry 的**聚合错误**
//     （含每一次尝试；errors.As 取到的是 Unwrap 序列中**最近一次匹配**的那个——
//     完整尝试史见 Aggregated.Attempts）。
func (p *Provider) Run(ctx context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	if p == nil {
		return core.ModelResult{}, errors.New("openai: provider 为 nil（New 未被调用）")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if emit == nil {
		// 允许不传 emit（纯结果调用）：内部统一走 nil 安全的 emitEvent。
		emit = func(core.ProviderEvent) {}
	}
	if strings.TrimSpace(req.Model) == "" {
		return fail(emit, errors.New("openai: ModelRequest.Model 不能为空（chat completions 必须指定模型）"))
	}
	// ctx 已结束就别发请求（core/retry 对"一次都没跑"的取消原样返回 ctx 错误；
	// 这里包上 provider 前缀，保持"中止"类错误的文案一贯：日志里能看出是取消不是失败）。
	if err := ctx.Err(); err != nil {
		return fail(emit, fmt.Errorf("openai: 请求中止（%w）", err))
	}

	payload, err := p.buildRequest(req)
	if err != nil {
		return fail(emit, err)
	}

	// 可重试的传输层失败在这里小重试（M6/M7）；resp 非 nil = 拿到一个**不再重试**的
	// 2xx 响应（非 2xx 一律在重试循环**内**成错——见 doWithRetry 的说明）。
	resp, err := p.doWithRetry(ctx, chatCompletionsPath, payload)
	if err != nil {
		return fail(emit, err)
	}
	// 无论走哪条路都要把 body 关掉（成功路径的 body 已读完，排空只是兜底）。
	defer drainClose(resp.Body)

	if p.stream {
		return p.runStream(ctx, resp.Body, emit)
	}
	return p.runOnce(resp.Body, emit)
}

// statusErrorOf 读一点响应体并折成 *StatusError（重试路径与"不重试的非 2xx"路径共用
// 同一形状：状态码 + 截断到 1KB 的 body + Retry-After 建议等待）。
func statusErrorOf(resp *http.Response) *StatusError {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
	return &StatusError{
		StatusCode: resp.StatusCode,
		Status:     strings.TrimSpace(resp.Status),
		Body:       truncateString(string(raw), maxErrorBody),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
}

// parseRetryAfter 解析 Retry-After 头（RFC 9110 的两种形态）：
//
//   - **延迟秒数**（"120"）→ 120s；
//   - **HTTP-date**（"Wed, 21 Oct 2015 07:28:00 GMT"）→ 距该时刻的时长（已过去 → 0）。
//
// 解析不出（空 / 畸形 / 负数）→ 0（= 没有建议，按常规退避走）。不猜、不报错：端点的
// 建议是"参考"，解析不出来就当没给——比猜一个错的等待时长安全。
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d <= 0 {
			return 0 // 日期在过去 = "现在就可以再来"
		}
		return d
	}
	return 0
}

// providerMaxAttempts 是 provider 级小重试的尝试次数上限（**含第一次**）。
//
// 为什么是 3：07 M6 拍板"provider 级**小**重试"。网关 429/5xx 绝大多数在 1-2 次退避后
// 就恢复；真故障（挂死 / 持续 5xx）该由调用方或上层决定，provider 无限重发只会把一次
// 失败放大成一次拥塞。
const providerMaxAttempts = 3

// providerRetryPolicy 是 provider 级小重试的节奏（**只读**，包级共享：Policy 是值类型，
// 无共享可变状态，可被多个 goroutine 并发 Run 使用）。
//
// 指数退避 100ms→200ms + 20% 抖动：总睡眠量级几百 ms，远小于一次模型调用的正常耗时，
// 又足以让过载的网关喘口气。抖动用来打散多 agent 同步重试（雷鸣群）。
//
// MaxBackoff=2s 是**硬上界**，两处生效：
//
//   - 计算退避的封顶（本包的自造节奏只到 200ms，实际碰不到）；
//   - **服务端建议等待（Retry-After）的上界**：端点说"过 2s 再来"就照 2s 等；说
//     "过 30s 再来"——那**不重试**（我们最多等 2s，再试也是撞在同一个限流窗口里，
//     还会加剧限流），窗口（Retry-After）留在错误里抛给调用方，由它用 ctx deadline
//     / 上层重试去等那个窗口。provider 级**小**重试不该被单个端点的指示撑爆。
var providerRetryPolicy = retry.Policy{
	MaxAttempts:    providerMaxAttempts,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	Multiplier:     2,
	Jitter:         0.2,
	RetryIf:        retryableErr,
}

// doWithRetry 经 core/retry 做一次（可能重试的）HTTP 往返。
//
// path 是拼在 baseURL 后的相对路径（chat 传 chatCompletionsPath、embeddings 传
// embeddingsPath）：重试/错误口径两条端点完全同款，只有 URL 不同——参数化后共用
// 同一套白名单与退避，不必复制一份重试逻辑。
//
// fn 的**幂等性自证**（M7）：chat completions 是只读推理请求——同一个 payload 重发一次
// 得到的仍是同一轮补全（最坏多付一次 token 费），不写任何系统状态。这正是 07 拍板
// "模型 API 429/5xx 可重试"的那一类面。embeddings 同理：同一文本重发得到同一向量，
// 不写任何系统状态（见 embed.go）。
//
// **非 2xx 一律在循环内成错**（不是把响应带出去再判）：本次响应体被排空 + 关闭，折成
// 一个 *StatusError 交给 core/retry 判重试——
//   - 状态码可重试（429/5xx）→ core/retry 重试；三次都那样 → 聚合错误（每条尝试一个
//     条目，errors.As 仍能取出 *StatusError）；
//   - 状态码不可重试（4xx 等请求侧错误）→ 当场返回那个 *StatusError（第一次就失败，
//     原样返回，调用方 errors.As 直接命中）；
//   - 429 且 Retry-After 超出等待上限（MaxBackoff）→ core/retry **停重试**（注定撞窗），
//     窗口留在 *StatusError.RetryAfter 里抛给调用方（口径见包 doc「等待节奏」）。
//
// 这么做的额外好处："429 重试后拿到 400"这类序列会落进**聚合错误**（M6：聚合含各次
// 尝试）——前一次的可重试失败不会被吞掉；而把 4xx 响应带出循环就永远只有最后那个错误。
//
// 返回的 resp 只在**成功**时非 nil（2xx）。
func (p *Provider) doWithRetry(ctx context.Context, path string, payload []byte) (*http.Response, error) {
	var resp *http.Response
	err := retry.Do(ctx, providerRetryPolicy, func(rctx context.Context) error {
		// 每次尝试都造一个**新**请求（新的 body 读取器）：共用一个 *http.Request 跨尝试
		// 要靠 GetBody 回退，那是标准库给重定向用的内部机制，不适合在这里依赖。
		// URL 走 endpointURL 拼接（保 base 的 query——见 list.go 的说明）。
		httpReq, err := http.NewRequestWithContext(rctx, http.MethodPost, endpointURL(p.baseURL, path), bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("openai: 构造 HTTP 请求失败：%w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if p.stream {
			httpReq.Header.Set("Accept", "text/event-stream")
		} else {
			httpReq.Header.Set("Accept", "application/json")
		}
		if p.apiKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
		}

		r, err := p.client.Do(httpReq)
		if err != nil {
			// ctx 取消要能被 errors.Is 认出来：包一层但不吞掉原错误。
			if ctxErr := rctx.Err(); ctxErr != nil {
				return fmt.Errorf("openai: 请求中止（%w）", ctxErr)
			}
			return fmt.Errorf("openai: 请求失败：%w", err)
		}
		if r.StatusCode < 200 || r.StatusCode > 299 {
			// 非 2xx：读一点 body 进错误文案（排空 + 关闭，让连接能复用），折成
			// *StatusError 交回 retry.Do —— 可重试状态码（429/5xx）由它重试，
			// 不可重试的（4xx 等请求侧错误）当场原样返回。
			se := statusErrorOf(r)
			drainClose(r.Body)
			return se
		}
		resp = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// retryableStatus 判定 HTTP 状态码是否值得 provider 级小重试（M7 拍板：模型 API
// 429/5xx 可重试）。
//
// 集合刻意收窄：只有 429 与 5xx。408（请求超时）等"可能已处理"的状态**不入列**——
// fail-close：宁可少重试一次，也不重发一个端点可能已经执行过的请求（v0 的保守口径，
// 后续有实测需求再放宽）。
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || (code >= 500 && code <= 599)
}

// retryableErr 是 provider 级小重试的 RetryIf（M7：只有声明可重试的面才自动重试）。
//
// 可重试（白名单，其余一律不重试）：
//
//   - *StatusError 且状态码在 retryableStatus 集合（429 / 5xx）；
//   - 传输层网络错误（net.Error）——网关瞬断、连接被重置、DNS 抖动这一类，
//     **但永久传输错除外**（见 isPermanentTransportErr）。
//
// 不可重试：ctx 取消/超时（**永不重试**：取消是调用方的意图，不是瞬断；注意
// context.DeadlineExceeded 自己就满足 net.Error，所以它必须排在前面挡掉）、
// 4xx（非 429）等请求侧错误、构造请求失败、**永久传输错**（TLS/证书错误、DNS 查无此域、
// https 打明文端口 / 协议不支持这类配置错）、畸形响应。
func retryableErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// 永久传输错（TLS 证书错、DNS 查无此域、https 打明文端口这类配置错）：**重发同一个
	// 请求还是同样的错**，且 net.Error 的兜底会把它们误判成瞬断（*url.Error 自己就实现了
	// net.Error，不管内层是什么）→ 必须排在 net.Error 判定之前挡掉。
	if isPermanentTransportErr(err) {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return retryableStatus(se.StatusCode)
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// isPermanentTransportErr 判定"**永久**传输错"：重发同一个请求还是同样的错的那一族。
//
// 这一族错误全都会被 net.Error 的兜底误判成可重试（*url.Error 实现了 net.Error，
// 内层是什么它不管；TLS 握手失败正是这样被重试了三遍——三遍都失败，白等三个退避
// 周期，错误文案还误导人："网络瞬断"其实是自己配错了 CA）。
//
// 判据（写明以便复查）：
//
//   - **x509 证书类错误**：证书不受信（UnknownAuthorityError）、证书无效
//     （CertificateInvalidError：过期 / 用途不对）、主机名不匹配（HostnameError）、
//     系统根证书池加载失败（SystemRootsError）——配错 CA / 端点换了证书，重试无用；
//   - **TLS 握手层**：证书校验错（*tls.CertificateVerificationError：它 unwrap 到上面
//     的 x509 错，独立判一次更保险）与记录头错（tls.RecordHeaderError：端口说错了）；
//   - **DNS 永久错**（*net.DNSError 且 IsNotFound：域名不存在）——再重试也解析不出来。
//   - **http 层改写后的形态**（v4-4-A 终查 F2）：net/http 把"TLS 记录头错"改写成了
//     一个**不带类型信息**的哨兵（http.ErrSchemeMismatch："http: server gave HTTP
//     response to HTTPS client"——https 打明文端口），把"协议不支持"折成纯文案错误
//     （unsupported protocol scheme "xxx"）——两者都是**配置错**，重发还是同样的错，
//     但它们都被 *url.Error 包着（url.Error 自己实现 net.Error）→ 会落进可重试兜底。
//   - ***url.Error 的内层再判一次**：外层的 net.Error 兜底不管内层是什么，内层若仍是
//     永久错（类型判据或文案判据），外层也判永久。
//
// **不包括**：连接被拒 / 连接被重置 / 超时 / DNS 抖动（IsTimeout）——那些是**瞬时**面，
// 仍在可重试白名单里（网关重启、连接池打满都表现为它们）。
func isPermanentTransportErr(err error) bool {
	if err == nil {
		return false
	}
	if isPermanentTransportCause(err) {
		return true
	}
	// *url.Error 的内层再判一次（外层 url.Error 自己像 net.Error → 兜底会误判）。
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil && urlErr.Err != err {
		return isPermanentTransportCause(urlErr.Err)
	}
	return false
}

// isPermanentTransportCause 判"永久传输错"的**成因**判据（不含 *url.Error 的再入，
// 免得自成一圈）。
func isPermanentTransportCause(err error) bool {
	if err == nil {
		return false
	}
	// HTTP 状态码错误**不是**传输错：端点已经给了明确答复（该按状态码分类，见
	// retryableErr），它的响应体里碰巧出现下面的特征串也不算传输错的证据。
	var se *StatusError
	if errors.As(err, &se) {
		return false
	}
	var (
		unknownAuthority x509.UnknownAuthorityError
		certInvalid      x509.CertificateInvalidError
		hostnameErr      x509.HostnameError
		systemRoots      x509.SystemRootsError
	)
	if errors.As(err, &unknownAuthority) || errors.As(err, &certInvalid) ||
		errors.As(err, &hostnameErr) || errors.As(err, &systemRoots) {
		return true
	}
	var certVerify *tls.CertificateVerificationError
	if errors.As(err, &certVerify) {
		return true
	}
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &recordHeader) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	// http 层改写后的形态（见 isPermanentTransportErr 的说明）。
	if errors.Is(err, http.ErrSchemeMismatch) {
		return true
	}
	return hasPermanentTransportText(err.Error())
}

// permanentTransportTexts 是"永久配置错"在**错误文案**里的特征串。
//
// 为什么按文案判：这两类错 net/http 没给类型（"unsupported protocol scheme" 只是
// fmt.Errorf 出来的纯文案；ErrSchemeMismatch 虽是哨兵，但人工构造/包装后的同款文案
// 未必是那个哨兵本身）——而它们都是**配置错**（https 打明文端口 / 端点协议不对），
// 重发同一个请求还是同样的错。
var permanentTransportTexts = []string{
	"HTTP response to HTTPS client", // http.ErrSchemeMismatch 的文案（https 打明文端口）
	"unsupported protocol scheme",   // 协议不支持（如 ftp://）
}

// hasPermanentTransportText 判错误文案里是否命中"永久配置错"的特征串。
func hasPermanentTransportText(msg string) bool {
	for _, s := range permanentTransportTexts {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// drainClose 排空并关掉一个响应体。
//
// 重试路径上**必须**两个都做：不关 = 连接泄漏；不排空 = 连接无法被复用（每次都新建
// TCP + TLS，重试反而更慢）。
//
// 排空上限是 maxDrainBody（64KB，见其处的理由）：错误体比这更大时排不干净 → 该连接
// 不用了（关掉，不泄漏），但绝大多数错误体（几 KB ~ 几十 KB）都能排干净 → 连接复用。
func drainClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrainBody))
	_ = body.Close()
}

// fail 统一「先 emit error 事件、再返回错误」的出口（保证 UI 看得到失败原因）。
func fail(emit func(core.ProviderEvent), err error) (core.ModelResult, error) {
	emitEvent(emit, core.ProviderEvent{Kind: KindError, Text: err.Error()})
	return core.ModelResult{}, err
}

// emitEvent 是 nil 安全的 emit（调用方可以不传回调）。
func emitEvent(emit func(core.ProviderEvent), ev core.ProviderEvent) {
	if emit == nil {
		return
	}
	emit(ev)
}

// ---- 请求体 ----

// chatRequest 是 chat completions 的请求体（只写用得到的字段，保持零依赖）。
type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Tools    []chatTool    `json:"tools,omitempty"`
	Stream   bool          `json:"stream"`
	// StreamOptions 仅流式下发：让最后一个 chunk 带 usage（否则流式拿不到 token 计量）。
	StreamOptions *chatStreamOptions `json:"stream_options,omitempty"`
	// ReasoningEffort 思维链强度档位（G14；**仅当调用方设置才下发**——nil/空 = 不发该字段，
	// 让端点用自己的默认档）。取值由网关解释（low/medium/high/xhigh…），本包原样透传。
	ReasoningEffort *string `json:"reasoning_effort,omitempty"`
}

type chatStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatFunction struct {
	Name      string   `json:"name"`
	Arguments flexArgs `json:"arguments"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

// defaultParameters 是工具未提供 Schema 时的兜底参数模式（OpenAI 要求 function 带 parameters）。
var defaultParameters = json.RawMessage(`{"type":"object","properties":{}}`)

// buildRequest 把 provider 无关的 ModelRequest 映射成 chat completions 请求体。
//
// 消息映射（角色一一对应，不重排、不合并——重排历史是 loop/上下文层的职责，v0 不做）：
//
//	system            → role=system（req.System 非空时置顶）
//	user              → role=user
//	assistant         → role=assistant（带 ToolCalls 时附 tool_calls 数组）
//	tool              → role=tool + tool_call_id（**provider 原样 id**，S4）
//
// 工具映射：contracts.Tool → {"type":"function","function":{name,description,parameters}}，
// parameters 用 Tool.Schema **原样字节**（元嵌套：值本身就是 JSON Schema，不重新编码）。
func (p *Provider) buildRequest(req core.ModelRequest) ([]byte, error) {
	msgs := make([]chatMessage, 0, len(req.Messages)+1)
	if strings.TrimSpace(req.System) != "" {
		msgs = append(msgs, chatMessage{Role: "system", Content: req.System})
	}
	for i, m := range req.Messages {
		cm, err := mapMessage(i, m)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, cm)
	}

	tools, err := mapTools(req.Tools)
	if err != nil {
		return nil, err
	}

	cr := chatRequest{
		Model:    req.Model,
		Messages: msgs,
		Tools:    tools,
		Stream:   p.stream,
	}
	// 思维链档位（G14）：trim 后非空才下发（"   " 这种空白串等于没设——发了反而让
	// 端点收到一个它不认识的档位）。nil = 不传，端点用默认档。
	if req.ReasoningEffort != nil {
		if eff := strings.TrimSpace(*req.ReasoningEffort); eff != "" {
			cr.ReasoningEffort = &eff
		}
	}
	if p.stream {
		cr.StreamOptions = &chatStreamOptions{IncludeUsage: true}
	}
	return json.Marshal(cr)
}

// mapMessage 映射单条消息。角色不认识 / tool 消息缺 id / assistant 工具调用缺 id 一律报错：
// 这些是调用方 bug，静默丢字段会让模型拿到错位的历史，比当场失败难查得多。
func mapMessage(i int, m core.Message) (chatMessage, error) {
	switch m.Role {
	case "system":
		return chatMessage{Role: "system", Content: m.Content}, nil
	case "user":
		return chatMessage{Role: "user", Content: m.Content}, nil
	case "assistant":
		cm := chatMessage{Role: "assistant", Content: m.Content}
		if len(m.ToolCalls) > 0 {
			cm.ToolCalls = make([]chatToolCall, 0, len(m.ToolCalls))
			for j, tc := range m.ToolCalls {
				if tc.ID == "" {
					return chatMessage{}, fmt.Errorf("openai: Messages[%d].ToolCalls[%d].ID 为空（provider 的 tool_call_id 必须原样回传，S4）", i, j)
				}
				if strings.TrimSpace(tc.Name) == "" {
					return chatMessage{}, fmt.Errorf("openai: Messages[%d].ToolCalls[%d].Name 为空", i, j)
				}
				cm.ToolCalls = append(cm.ToolCalls, chatToolCall{
					ID:       string(tc.ID), // 黑盒原样（S4/陷阱 17）
					Type:     "function",
					Function: chatFunction{Name: tc.Name, Arguments: flexArgs(tc.Args)},
				})
			}
		}
		return cm, nil
	case "tool":
		if m.ToolCallID == "" {
			return chatMessage{}, fmt.Errorf("openai: Messages[%d] 角色为 tool 但 ToolCallID 为空（必须回填对应的 provider tool_call_id，S4）", i)
		}
		return chatMessage{
			Role:       "tool",
			Content:    m.Content,
			ToolCallID: string(m.ToolCallID), // 黑盒原样（S4/陷阱 17）
		}, nil
	default:
		return chatMessage{}, fmt.Errorf("openai: Messages[%d].Role=%q 不是受支持的角色（v0：system/user/assistant/tool）", i, m.Role)
	}
}

// mapTools 映射工具描述。Schema 为空时落 defaultParameters（否则 OpenAI 侧会拒请求）。
func mapTools(tools []contracts.Tool) ([]chatTool, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	out := make([]chatTool, 0, len(tools))
	for i, t := range tools {
		if strings.TrimSpace(t.Name) == "" {
			return nil, fmt.Errorf("openai: Tools[%d].Name 为空", i)
		}
		params := json.RawMessage(t.Schema)
		if len(bytes.TrimSpace(params)) == 0 {
			params = defaultParameters
		}
		ct := chatTool{
			Type: "function",
			Function: chatToolFunction{
				Name:       t.Name,
				Parameters: params,
			},
		}
		if t.Description != nil {
			ct.Function.Description = *t.Description
		}
		out = append(out, ct)
	}
	return out, nil
}

// ---- 响应体（非流式） ----

// runOnce 解析非流式响应体并组装 ModelResult。
func (p *Provider) runOnce(body io.Reader, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	var cr chatResponse
	if err := json.NewDecoder(body).Decode(&cr); err != nil {
		return fail(emit, fmt.Errorf("openai: 解析响应体失败：%w", err))
	}
	if cr.Error != nil && strings.TrimSpace(cr.Error.Message) != "" {
		return fail(emit, fmt.Errorf("openai: 端点返回错误对象（type=%s code=%v）：%s",
			cr.Error.Type, cr.Error.Code, truncateString(cr.Error.Message, maxErrorBody)))
	}
	if len(cr.Choices) == 0 {
		return fail(emit, errors.New("openai: 响应缺少 choices（既无消息也无 usage，视为畸形响应）"))
	}

	ch := cr.Choices[0]
	res := core.ModelResult{
		Text:      string(ch.Message.Content),
		Reasoning: reasoningOf(ch.Message.ReasoningContent, ch.Message.Reasoning),
	}
	for i, tc := range ch.Message.ToolCalls {
		if strings.TrimSpace(tc.Function.Name) == "" {
			return fail(emit, fmt.Errorf("openai: choices[0].tool_calls[%d]（id=%q）缺少 function.name", i, tc.ID))
		}
		res.ToolCalls = append(res.ToolCalls, core.ToolCallReq{
			ID:   contracts.ProviderId(tc.ID), // 黑盒原样（S4/陷阱 17）
			Name: tc.Function.Name,
			Args: normalizeArgs(tc.Function.Arguments),
		})
	}
	if cr.Usage != nil {
		res.Usage = core.Usage{InputTokens: cr.Usage.PromptTokens, OutputTokens: cr.Usage.CompletionTokens}
	}

	// 非流式也按流式的形状补发事件：一条全文 delta + 每个工具一条 start + done。
	// 消费端因此不必区分两种模式。
	//
	// G14：思维链同样补发一条 reasoning.delta（在正文之前——思维链在时间上先于正文），
	// 消费端把两类片段分流到正文/思维链两个显示区即可。
	if res.Reasoning != "" {
		emitEvent(emit, core.ProviderEvent{Kind: KindReasoningDelta, Reasoning: res.Reasoning})
	}
	if res.Text != "" {
		emitEvent(emit, core.ProviderEvent{Kind: KindTextDelta, Text: res.Text})
	}
	for i := range res.ToolCalls {
		tc := res.ToolCalls[i]
		emitEvent(emit, core.ProviderEvent{Kind: KindToolCallStart, ToolCall: &tc})
	}
	emitEvent(emit, core.ProviderEvent{Kind: KindDone, Text: res.Text})
	return res, nil
}

// ---- 响应体（流式） ----

// runStream 按 SSE 解析流式响应体。
//
// SSE 形状（一行一条）：
//
//	data: {"id":"...","choices":[{"index":0,"delta":{"content":"你"}}]}
//	data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_x","function":{"name":"read","arguments":"{\"pa"}}]}}]}
//	data: [DONE]
//
// 以 ":" 开头的行是注释/心跳，直接忽略；空行是分帧，忽略；"[DONE]" 是结束哨兵。
// 流自然 EOF 但没有 [DONE] 时**容忍**（用已收到的内容组装结果）——部分网关不发送哨兵。
// 空流（只有 [DONE]）→ 返回空 ModelResult 且不报错：部分网关对"无内容可返回"这么回，
// 交给上层（loop）判断要不要重试，provider 不猜。
func (p *Provider) runStream(ctx context.Context, body io.Reader, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	acc := newStreamAccumulator(emit)
	br := newLineReader(body)

	for {
		if err := ctx.Err(); err != nil {
			return fail(emit, fmt.Errorf("openai: 流式中止（%w）", err))
		}
		line, err := br.readLine()
		if line != "" {
			if perr := acc.handleLine(line); perr != nil {
				return fail(emit, perr)
			}
			if acc.done {
				break
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // 流结束（[DONE] 或自然 EOF 都走这里）
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fail(emit, fmt.Errorf("openai: 读取流失败（%w）", ctxErr))
			}
			return fail(emit, fmt.Errorf("openai: 读取流失败：%w", err))
		}
	}

	res := acc.result()
	if err := acc.validate(); err != nil {
		return fail(emit, err)
	}
	emitEvent(emit, core.ProviderEvent{Kind: KindDone, Text: res.Text})
	return res, nil
}

// toolAccum 是一个 tool_call 在流过程中的累积态（按 chunk 里的 index 归并）。
type toolAccum struct {
	id      string
	name    string
	args    strings.Builder
	started bool
}

// streamAccumulator 累积一次流式调用的全部增量。
type streamAccumulator struct {
	emit      func(core.ProviderEvent)
	text      strings.Builder
	reasoning strings.Builder // 思维链整段（G14：逐段组装，与 text 同口径）
	order     []int           // tool_call 的 index 出现顺序（保序：模型并行调用的顺序有意义）
	byIndex   map[int]*toolAccum
	usage     *core.Usage
	done      bool
	// indexlessFallback 记录"曾用过缺 index 的位置兜底"——一旦流里出现第二个
	// 调用，兜底那次归属就是猜的 → 必须 fail-loud（见 applyToolCalls）。
	indexlessFallback bool
}

func newStreamAccumulator(emit func(core.ProviderEvent)) *streamAccumulator {
	return &streamAccumulator{emit: emit, byIndex: map[int]*toolAccum{}}
}

// handleLine 处理一条 SSE 行。
func (a *streamAccumulator) handleLine(line string) error {
	line = strings.TrimRight(line, "\r\n")
	if line == "" || strings.HasPrefix(line, ":") {
		return nil // 空行分帧 / 注释心跳
	}
	if !strings.HasPrefix(line, "data:") {
		return nil // event: / id: / retry: 等行 v0 不用
	}
	payload := strings.TrimSpace(line[len("data:"):])
	if payload == "" {
		return nil
	}
	if payload == "[DONE]" {
		a.done = true
		return nil
	}

	var ch chatChunk
	if err := json.Unmarshal([]byte(payload), &ch); err != nil {
		return fmt.Errorf("openai: 解析 SSE 数据块失败：%w（块已截断：%s）", err, truncateString(payload, maxErrorBody))
	}
	if ch.Error != nil && strings.TrimSpace(ch.Error.Message) != "" {
		return fmt.Errorf("openai: 流中返回错误对象（type=%s code=%v）：%s",
			ch.Error.Type, ch.Error.Code, truncateString(ch.Error.Message, maxErrorBody))
	}
	if ch.Usage != nil {
		a.usage = &core.Usage{InputTokens: ch.Usage.PromptTokens, OutputTokens: ch.Usage.CompletionTokens}
	}
	for _, choice := range ch.Choices {
		if delta := choice.Delta.Content; delta != "" {
			a.text.WriteString(string(delta))
			emitEvent(a.emit, core.ProviderEvent{Kind: KindTextDelta, Text: string(delta)})
		}
		// 思维链增量（G14）：边收边发（与正文同口径），整段留在 accumulator 里
		// 供 result() 组装。两个字段名都看（reasoning_content 优先，见 reasoningOf）。
		if rd := reasoningOf(choice.Delta.ReasoningContent, choice.Delta.Reasoning); rd != "" {
			a.reasoning.WriteString(rd)
			emitEvent(a.emit, core.ProviderEvent{Kind: KindReasoningDelta, Reasoning: rd})
		}
		if err := a.applyToolCalls(choice.Delta.ToolCalls); err != nil {
			return err // fail-loud：缺 index 且无法安全归并（见 applyToolCalls）
		}
	}
	return nil
}

// errMissingToolCallIndex 是"chunk 缺 tool_calls[].index 且无法安全归并"的错误。
//
// 多调用流里缺 index = 这个增量可能属于**别的**调用（按位置兜底会把参数并到
// 别的调用上 → 污染参数）。宁可当场报错（fail-loud），不猜。
var errMissingToolCallIndex = errors.New("openai: chunk 缺 tool_calls[].index，无法安全归并")

// applyToolCalls 归并一个 chunk 里的 tool_call 增量。
//
// 顺序很重要：先把整个数组归并完，再对"本轮首次出现"的 tool_call 发 start——
// 这样 id 和 name 同在首个 chunk 到达时（常见形态），start 事件就一次带全，
// 消费端不必拼多个事件才知道"这个调用叫什么"。
//
// **缺 index 的处理**（fail-loud）：OpenAI 官方总带 index。缺 index 时按位置兜底
// 只在"整条流只有这一个调用"时安全；一旦流里已有/将有多个调用，兜底就会把参数
// 并到别的调用上（污染参数）→ 当场报错（emit error + 返回错误），不猜。
func (a *streamAccumulator) applyToolCalls(deltas []chunkToolCall) error {
	if len(deltas) == 0 {
		return nil
	}
	// 一个 chunk 里多个 delta 时，缺 index 的那个无法归属（不能按位置猜）。
	if len(deltas) > 1 {
		for _, d := range deltas {
			if d.Index == nil {
				return errMissingToolCallIndex
			}
		}
	}
	type frag struct {
		acc  *toolAccum
		args string
	}
	frags := make([]frag, 0, len(deltas))
	touched := make([]*toolAccum, 0, len(deltas))

	for i, d := range deltas {
		idx := i
		if d.Index != nil {
			idx = *d.Index // OpenAI 官方总带 index
			if a.indexlessFallback {
				// 之前用过"缺 index 的位置兜底"，现在又出现带 index 的调用 →
				// 流里不止一个调用，而兜底那次归属是猜的 → 不可信（fail-loud）。
				return errMissingToolCallIndex
			}
		} else {
			switch {
			case len(a.byIndex) == 0:
				// 第一个调用：按位置兜底（若后面出现第二个调用 → 立即报错）。
				idx = 0
				a.indexlessFallback = true
			case len(a.byIndex) == 1 && a.indexlessFallback:
				// 仍是那唯一一个调用（续传）→ 同一个槽位。
				idx = 0
			default:
				// 已经有别的调用 → 这个增量属于谁无法判断 → 不安全。
				return errMissingToolCallIndex
			}
		}
		acc, ok := a.byIndex[idx]
		if !ok {
			acc = &toolAccum{}
			a.byIndex[idx] = acc
			a.order = append(a.order, idx)
		}
		touched = append(touched, acc)

		// id：首个非空即锁定，后续 chunk 即使再带同名 id 也不改（S4 黑盒，不改写）。
		if acc.id == "" && d.ID != "" {
			acc.id = d.ID
		}
		// name：绝大多数端点只在首 chunk 给一次；少数网关会把 name 拆成多个片段，
		// 也有网关每个 chunk 重复完整 name。这里用"追加 + 重复检测"两种形态都兼容：
		// 片段等于已累积的前缀时判定为重复发送，跳过。
		if n := d.Function.Name; n != "" {
			if acc.name == "" || !strings.HasPrefix(acc.name, n) {
				acc.name += n
			}
		}
		if args := string(d.Function.Arguments); args != "" {
			acc.args.WriteString(args)
			frags = append(frags, frag{acc: acc, args: args})
		}
	}

	for _, acc := range touched {
		if acc.started {
			continue
		}
		if acc.id == "" && acc.name == "" {
			continue // 还没有任何可识别信息，等下一个 chunk
		}
		acc.started = true
		emitEvent(a.emit, core.ProviderEvent{Kind: KindToolCallStart, ToolCall: &core.ToolCallReq{
			ID:   contracts.ProviderId(acc.id), // 黑盒原样（S4/陷阱 17）
			Name: acc.name,
		}})
	}
	for _, f := range frags {
		emitEvent(a.emit, core.ProviderEvent{Kind: KindToolCallDelta, ToolCall: &core.ToolCallReq{
			ID:   contracts.ProviderId(f.acc.id), // 黑盒原样
			Name: f.acc.name,
			Args: json.RawMessage(f.args), // 只是一个片段，不是完整 JSON
		}})
	}
	return nil
}

// result 按 index 出现顺序组装 ModelResult（流已结束、尚未校验）。
func (a *streamAccumulator) result() core.ModelResult {
	res := core.ModelResult{Text: a.text.String(), Reasoning: a.reasoning.String()}
	if len(a.order) > 0 {
		res.ToolCalls = make([]core.ToolCallReq, 0, len(a.order))
		for _, idx := range a.order {
			acc := a.byIndex[idx]
			res.ToolCalls = append(res.ToolCalls, core.ToolCallReq{
				ID:   contracts.ProviderId(acc.id), // 黑盒原样（S4/陷阱 17）
				Name: acc.name,
				Args: normalizeArgs([]byte(acc.args.String())),
			})
		}
	}
	if a.usage != nil {
		res.Usage = *a.usage
	}
	return res
}

// validate 校验组装结果：缺 name 的 tool_call 无法分发，必须当场失败（fail-loud），
// 否则 loop 会拿着一个空名字去查工具注册表，报一个离现场很远的错。
func (a *streamAccumulator) validate() error {
	for i, idx := range a.order {
		if strings.TrimSpace(a.byIndex[idx].name) == "" {
			return fmt.Errorf("openai: 流式 tool_calls[%d]（id=%q）始终没有 function.name", i, a.byIndex[idx].id)
		}
	}
	return nil
}

// normalizeArgs 把 arguments 规范化为 json.RawMessage：
// 空/纯空白 → "{}"（下游要 unmarshal 进参数结构体，空 RawMessage 会直接报错）；
// 非空 → **原样字节**（不重新编码，保留 provider 原文，S4 精神）。
func normalizeArgs(raw []byte) json.RawMessage {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(b)
}

// ---- 错误类型 ----

// StatusError 是 HTTP 非 2xx 的错误（调用方可 errors.As 取出状态码做分类，如 429 重试）。
//
// v4-4-A：它同时满足 core/retry.StatusCarrier（见下面的 HTTPStatus）——白名单口径
// retry.RetryIfStatus(...) 因此能直接吃它，不必 import 本包。
type StatusError struct {
	StatusCode int
	Status     string // 形如 "400 Bad Request"
	Body       string // 已截断到 maxErrorBody
	// RetryAfter 是**服务端建议的等待时长**（来自 Retry-After 头；解析不出 = 0）。
	//
	// core/retry 采信它：Retry-After ≤ MaxBackoff 时实际等待 = max(计算退避, RetryAfter)；
	// Retry-After > MaxBackoff 时**停重试**（注定撞窗），窗口留在本字段里供调用方读。
	// 没有它的话，429 + Retry-After: 30 会被按 100ms/200ms 的自造节奏重试——端点的限流
	// 窗口不会因为我们重试得快而缩短，那是**白烧**重试（07 M6：重试要有意义）。
	RetryAfter time.Duration
}

// Error implements error。
//
// 文案带**服务端建议的等待窗口**（Retry-After > 0 时，如 "(retry after 60s)"）：限流
// 窗口是关键事实（"要等 60s"），只躺在字段里等于对用户/日志读者不可见。
func (e *StatusError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "openai: HTTP %d", e.StatusCode)
	if e.RetryAfter > 0 {
		fmt.Fprintf(&b, " (retry after %s)", formatRetryAfter(e.RetryAfter))
	}
	if e.Status != "" {
		b.WriteString(" " + e.Status)
	}
	if e.Body != "" {
		b.WriteString(": " + e.Body)
	}
	return b.String()
}

// formatRetryAfter 把服务端建议的等待时长格式化成**秒级**文案（"60s"）。
//
// 为什么不用 d.String()：Duration.String() 把 60s 写成 "1m0s"——限流窗口是给人看的，
// "60s" 更贴近服务端 Retry-After 头的原话（"60"）。亚秒级保留原样（"500ms"）。
func formatRetryAfter(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return d.String()
	}
	return fmt.Sprintf("%ds", int64(d/time.Second))
}

// HTTPStatus 返回 HTTP 状态码（core/retry.StatusCarrier：让通用重试组件按状态码分类，
// 而不用 import 本包——core 不得 import runtime，09）。
func (e *StatusError) HTTPStatus() int { return e.StatusCode }

// RetryAfterDelay 暴露 Retry-After 建议（core/retry 的结构形状接口
// retry.RetryAfterDelayCarrier：core 不 import runtime，只按形状取）。
//
// 方法名带 Delay 后缀的原因：字段名已经是 RetryAfter，Go 不允许字段与方法同名。
func (e *StatusError) RetryAfterDelay() time.Duration {
	if e == nil {
		return 0
	}
	return e.RetryAfter
}

// truncateString 按字节截断并标注（截断后不超过 limit 字节 + 标注）。
func truncateString(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + fmt.Sprintf("…[truncated %d bytes]", len(s)-limit)
}
