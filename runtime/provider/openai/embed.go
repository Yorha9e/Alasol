package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// 这一文件是 OpenAI 兼容 **embeddings** 客户端（RAG 嵌入端点）。与 chat completions
// 同一选型理由（包 doc「选型」）：标准库 raw net/http、零新依赖、换端点只改 BaseURL。
// 它不实现 core.Provider（那是 chat 的事）——它满足的是宿主 RAG 面（host.EmbeddingProvider）
// 的窄接口形状：Embed(ctx, text) ([]float32, error)。

// embeddingsPath 是 embeddings 的相对路径（拼在 BaseURL 之后，与 chatCompletionsPath
// 同口径：走 endpointURL 拼接——base 的 **query 原样保留**，路径用 path.Join 接到
// base 路径后面，见 list.go 的 endpointURL 说明）。
const embeddingsPath = "/embeddings"

// 编译期断言：Embedder 必须满足宿主槽位（host.EmbeddingProvider）的**结构形状**——
// 宿主槽位的形状在这儿被钉住，方向上 runtime 不 import host（依赖方向：host → runtime，
// 反向 import 会成环）。改方法签名立刻编译失败，而不是到宿主装配时才发现不兼容。
var _ interface {
	Embed(context.Context, string) ([]float32, error)
} = (*Embedder)(nil)

// Embedder 是 OpenAI 兼容 /embeddings 端点的客户端（RAG 嵌入：文本 → 向量）。
//
// 装配复用 chat 的 Options：BaseURL / APIKey 语义与 Provider 完全一致（零值可用：默认
// 官方端点、无凭据）；**Model 在这里是 embedding 模型名**（chat 的模型名走
// ModelRequest.Model 按请求传，两者互不相干）；**Stream 对 embeddings 无意义**
// （协议里没有流式 /embeddings），构造时忽略。
//
// **HTTPClient 与 chat 刻意不同**：chat 的超时归调用方 ctx（Options.HTTPClient 为 nil
// 时用不带超时的 client）；embeddings 是单次短调用，而宿主 RAG 面的调用方 ctx（来自
// HTTP 请求）**没有 deadline 兜底** → 这里给一个带 30s 超时的默认 client（见
// defaultEmbedTimeout），要自己的超时/代理/传输层就注入 Options.HTTPClient。
//
// 并发安全：无共享可变状态（与 Provider 同款），可多 goroutine 同时 Embed。
type Embedder struct {
	// p 复用 Provider 的全部装配与传输层：BaseURL 归一化、APIKey → Authorization 头、
	// HTTP client、core/retry 小重试（doWithRetry）、*StatusError 折错、drainClose。
	p     *Provider
	model string
}

// defaultEmbedTimeout 是 embeddings 单次调用的**兜底时钟**（30s）。
//
// # 为什么 embeddings 有兜底时钟、chat 没有（**刻意不同**，不是同款口径）
//
// chat 的超时**归调用方 ctx**（包 doc「超时」：provider 不猜超时值）。embeddings 不同：
// 宿主 RAG 面（host.IngestVectors / SearchVectors）拿的是**HTTP 请求的 ctx**，而那个 ctx
// **没有 deadline 兜底**——挂死的 embedding 端点会把那个 HTTP 请求**永久挂住**（连接、
// goroutine 都占着，调用方也拿不到任何结论）。
//
// embeddings 是**单次短调用**（一段文本 → 一个向量），30s 足够；超时 = 失败快、可见、
// 可排查。调用方仍可用 Options.HTTPClient 注入自己的 client（超时/代理/传输层都归它）。
const defaultEmbedTimeout = 30 * time.Second

// NewEmbedder 构造 Embedder。零值 Options 合法（默认官方 BaseURL、无凭据、空模型——
// 空模型照发请求，让端点去拒）。
//
// # 空模型的口径（**刻意不同于 chat**，两者不是同款）
//
// chat 对空 model 是**本地 fail-close**（provider.go 的 Run：ModelRequest.Model 为空
// → 本地报错，一个请求都不发）；embeddings 这里**刻意不同**：空模型照发请求，让**端点**
// 去拒。两者不是同款口径——embeddings 的模型名是**装配期**定的（Options.Model，宿主从
// 配置面给），不是按请求传的；宿主没配模型名时，让权威的端点去拒比本地猜一个更贴近真实。
func NewEmbedder(opts Options) *Embedder {
	// Stream 对 embeddings 无意义：强制 false，免得复用的 Provider 误发
	// Accept: text/event-stream（见类型注释）。
	opts.Stream = false
	// 兜底时钟（见 defaultEmbedTimeout）：**只在这里**补默认 client——chat 的 Provider
	// 仍用"不带超时的 client"（超时归调用方 ctx），两者刻意不同。
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: defaultEmbedTimeout}
	}
	return &Embedder{
		p:     New(opts),
		model: strings.TrimSpace(opts.Model),
	}
}

// BaseURL 返回实际使用的端点根路径（测试/诊断用，与 Provider.BaseURL 同口径）。
func (e *Embedder) BaseURL() string {
	if e == nil {
		return ""
	}
	return e.p.BaseURL()
}

// Embed 把一段文本转成嵌入向量：POST {BaseURL}/embeddings，请求体
// {"model": <Model>, "input": <text>}，取响应 data[0].embedding。
//
// # 输入校验（本实现的选择，不是契约要求）
//
// text 为空串 → 显式拒绝（不发请求）：一次无意义的外部调用能省则省——虽然端点
// 自己也会拒（400），但为"空"白付一次网络往返不值得。宿主契约（host.EmbeddingProvider）
// 只要求"同一文本同维度"，没要求实现拒空——这条是本客户端的输入校验。
//
// # 响应形状（单输入 → 单输出）
//
// data 为空 / 多于一条 / data[0].index 不为 0 / 缺 embedding / embedding 非数值 →
// 明文错误（与 chat「响应体不可解析 → 明文错误」同口径）。**不静默挑一条**：单输入
// 就应单输出，多条 = 响应形状异常（批响应混进了单输入调用，挑哪条都是猜）。
//
// # 数值精度
//
// JSON number → float32 由 encoding/json 按 float32 精度直接解析（不是先 float64
// 再截断）：解出来的值就是该 JSON 数字的**最近 float32 表示**；超出 float32 表示
// 范围（如 1e300）会解码报错而不是悄悄变成 ±Inf。向量按原样返回 []float32——
// 宿主要的就是 float32（相似度计算够用），精度损失是 float32 契约本身的一部分，
// 本层不做任何缩放/归一化（同一文本同一模型永远走同一条解析路径 → 维度与数值稳定）。
//
// # 错误与重试
//
// 与 chat completions **同款**（共用 doWithRetry / StatusError）：非 2xx →
// *StatusError（状态码 + 1KB 截断响应体 + Retry-After）；ctx 取消/超时 → 包装
// ctx.Err()（errors.Is 可取）；重试白名单同款（429/5xx/传输错，ctx 取消不重试，
// Retry-After 采信，见包 doc「重试」）——embeddings 是只读计算（同一文本重发得到
// 同一向量、不写系统状态），幂等性自证与 chat 同款。
//
// # 凭据与错误文案（**如实声明，不打码**）
//
// 本层**不主动**把凭据写进错误文案（Authorization 头不会进任何错误文案）。但**端点若在
// 响应体里回显凭据**（有些网关把收到的 Authorization 头原样回在错误 JSON 里），那段
// 文本会被抄进 *StatusError.Body —— 与 chat completions **同款既有风险**（同一个错误
// 形状）。本层不做打码（模糊 regex 既会漏又会误伤），边界如实声明：要把错误体回给终端
// 用户的一方自行按需脱敏。
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if e == nil || e.p == nil {
		return nil, errors.New("openai: embedder 为 nil（NewEmbedder 未被调用）")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if text == "" {
		// 本实现的输入校验（见方法注释）：省掉一次注定被拒的外部调用。
		return nil, errors.New("openai: embeddings 输入文本为空")
	}
	// ctx 已结束就别发请求（与 Run 同款：包上 provider 前缀，日志里能看出是取消不是失败）。
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("openai: 请求中止（%w）", err)
	}

	payload, err := json.Marshal(embeddingsRequest{Model: e.model, Input: text})
	if err != nil {
		return nil, fmt.Errorf("openai: 构造 embeddings 请求体失败：%w", err)
	}

	// 与 chat 共用的可重试传输层小重试（白名单/退避/Retry-After 口径见包 doc「重试」）；
	// resp 非 nil = 拿到一个不再重试的 2xx 响应。
	resp, err := e.p.doWithRetry(ctx, embeddingsPath, payload)
	if err != nil {
		return nil, err
	}
	defer drainClose(resp.Body)

	var er embeddingsResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		// 非数值 / 结构非法（如 embedding 是字符串）都落这里 → 明文错误。
		return nil, fmt.Errorf("openai: 解析 embeddings 响应体失败：%w", err)
	}
	// 部分网关在 200 里回错误对象（与 chat runOnce 同款处理）。
	if er.Error != nil && strings.TrimSpace(er.Error.Message) != "" {
		return nil, fmt.Errorf("openai: 端点返回错误对象（type=%s code=%v）：%s",
			er.Error.Type, er.Error.Code, truncateString(er.Error.Message, maxErrorBody))
	}
	// 单输入 → 单输出：多于一条**不静默挑**（挑哪条都是猜——见方法注释）。
	if len(er.Data) == 0 {
		return nil, errors.New("openai: embeddings 响应缺少 data（既无向量也无错误对象，视为畸形响应）")
	}
	if len(er.Data) > 1 {
		return nil, fmt.Errorf("openai: embeddings 响应含 %d 条 data（单输入应单输出，视为畸形响应）", len(er.Data))
	}
	d := er.Data[0]
	if d.Index != 0 {
		return nil, fmt.Errorf("openai: embeddings data[0].index=%d（单输入的 index 应为 0，视为畸形响应）", d.Index)
	}
	if len(d.Embedding) == 0 {
		return nil, errors.New("openai: embeddings data[0] 缺少 embedding（视为畸形响应）")
	}
	return d.Embedding, nil
}

// ---- /embeddings 线上形状（wire format）----
//
// 与 wire.go（chat 的线上形状）分开：改 embeddings 协议细节不用翻 chat 代码。

// embeddingsRequest 是 /embeddings 的请求体（单输入：input 是一个字符串；
// 协议还允许字符串数组，本客户端一次只嵌一段文本，不发批）。
type embeddingsRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// embeddingsResponse 是 /embeddings 的响应体。
type embeddingsResponse struct {
	Data  []embeddingsDatum `json:"data"`
	Error *errorWire        `json:"error"` // 部分网关 200 里回错误对象（与 chat 同款宽容）
}

// embeddingsDatum 是一条向量。Index 按 OpenAI 形状总是 0（单输入）；非 0 = 形状异常。
type embeddingsDatum struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}
