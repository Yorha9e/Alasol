package mcp

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
)

// 本文件把**外部 MCP server 的 stderr**接到本包的告警出口（mcpLog）。
//
// # 为什么不能丢（p3）
//
// 外部 server 是第三方进程：它启动失败的原因、依赖缺失、崩溃栈**只在 stderr 上**。
// 把 stderr 接到 /dev/null 等于"server 挂了但看不出为什么"——装配期能看到的
// 只有一句"tools/list 失败"，排查的人拿不到任何线索。
//
// # 为什么必须脱敏（凭据纪律 H12/H19）
//
// server 完全可能把收到的 env / 凭据打进 stderr（"连不上时打印一下配置"是
// 很常见的写法）。凭据值不进日志是不可让渡的纪律，所以这里把**这个 server 的
// 已知凭据值**（配置里的 env / headers 值 + 显式继承的父 env 值）替换成
// "***"——与 core/loop 的 redactSecrets 同口径（按值替换，不猜形态）。
//
// 每个值登记**两个形态**：原值，以及**剥掉 scheme 前缀**后的值本体
// （"Bearer sk-x" → "sk-x"）——server 常把凭据单独打出来（"auth=sk-x"），
// 只认整串会漏掉这一形态。
//
// 剩余风险要说清楚：server 若把凭据**变形**后打印（base64/切片/拼接），这里
// 认不出来。兜底是"别把凭据交给会乱打印的 server"——env_from 让凭据只活在
// 进程 env 里（不落配置文件）是更保守的选择。
//
// # 为什么要总量闸
//
// stderr 是**外部进程**的输出，长度不受我们控制：1 万行 × 1KB = 11MB 足以
// 刷爆日志、把真正有用的诊断信息淹掉。所以单行有上限（maxStderrLine），
// **单个 server 的总量也有上限**（maxStderrTotal）——超出后停止逐行记录，
// 只留一条"stderr 已截断"的说明。

// maxStderrLine 是单行 stderr 的长度上限（超出截断；防无界日志）。
const maxStderrLine = 4 << 10

// maxStderrTotal 是**单个 server** 的 stderr 总量上限（64KB）。
//
// 为什么要有总量闸：单行上限挡不住"很多行"——1 万行 × 1KB = 11MB，能把
// 日志刷爆（还可能把有用的诊断信息淹掉）。超过总量就**停止逐行记录**，
// 并补一条"stderr 已截断"（让人知道后面的输出没被记录）。
const maxStderrTotal = 64 << 10

// maxStderrLines 是**单个 server** 的 stderr 记录**条数**上限（1000 行）。
//
// 为什么字节闸之外还要条数闸（v3-S2 清账）：字节闸挡的是"大"，挡不住"多"——
// 一个话痨 server 可以用很多**短**行刷日志（每行都合法、都不超单行/总量字节上限），
// 1000 行 × 40 字节的噪声足以把真正有用的启动错误淹掉。所以记录条数也封顶：
// 超过 maxStderrLines 就**停止逐行记录**并补一条"stderr 已截断"（与字节闸同口径）。
// 两个闸是 OR 关系——谁先触发谁截断，之后都不再逐条记。
const maxStderrLines = 1000

// stderrSink 把子进程的 stderr 按行接到 mcpLog（脱敏后）。
//
// 为什么要按行：server 的诊断信息通常是"一行一件事"（栈 trace 是多行，
// 但每行单独看也有意义）；按行切分让日志可读，且能及时落盘（不等进程退出）。
type stderrSink struct {
	// server 是 server 名（日志的归属，不含任何配置值）。
	server string
	// secrets 是要打码的凭据值（见 secretValues）。
	secrets []string

	mu sync.Mutex
	// buf 是未凑满一行的残余字节（下次 Write 接着拼）。
	buf []byte
	// total 是已落日志的字节数（对照 maxStderrTotal）。
	total int
	// lines 是已落日志的条数（对照 maxStderrLines）。
	lines int
	// truncated 标记总量闸（字节或条数）已触发（之后的行不再逐条记录）。
	truncated bool
}

// newStderrSink 造一个 stderr 出口（server 名 + 该 server 的已知凭据值）。
func newStderrSink(cfg *ServerConfig) *stderrSink {
	s := &stderrSink{}
	if cfg != nil {
		s.server = cfg.Name
		s.secrets = secretValues(cfg)
	}
	return s
}

// Write implements io.Writer（exec 的 stderr 拷贝 goroutine 调它）。
//
// 永不返回错误：stderr 写失败不该让 server 进程崩掉（它只是诊断通道）。
func (s *stderrSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			break
		}
		line := string(s.buf[:i])
		s.buf = s.buf[i+1:]
		s.log(line)
	}
	// 没有换行的超长行 → 截断后丢弃（防无界缓冲：server 可以一直不换行）。
	if len(s.buf) > maxStderrLine {
		s.log(string(s.buf[:maxStderrLine]))
		s.buf = nil
	}
	return len(p), nil
}

// log 落一行（脱敏 + 截断 + 走 mcpLog）。
//
// 两道总量闸（OR，谁先触发谁截断，之后都不再逐条记）：
//   - **条数闸**（maxStderrLines）：挡"很多短行"——字节都合法但条数能刷爆日志；
//   - **字节闸**（maxStderrTotal）：挡"大"——少数几行就顶到字节上限。
//
// 不这么做的话，一个话痨 server 能刷爆日志（1 万行 × 1KB = 11MB），把真正有用的
// 诊断信息淹掉。
func (s *stderrSink) log(line string) {
	line = strings.TrimRight(line, "\r")
	if strings.TrimSpace(line) == "" {
		return
	}
	if s.truncated {
		return // 总量闸已触发：不再逐条记录
	}
	line = redactSecrets(line, s.secrets)
	if len(line) > maxStderrLine {
		line = line[:maxStderrLine] + "…[truncated]"
	}
	// 条数闸：已记录行数达到上限 → 补一条说明后停止逐条记录。
	if s.lines >= maxStderrLines {
		note := fmt.Sprintf("stderr 已截断（超过 %d 行上限，后续输出不再记录）", maxStderrLines)
		mcpLog.Warn("mcp: server stderr", "server", s.server, "line", note)
		s.truncated = true
		return
	}
	// 字节闸：本条会把总量顶过上限 → 补一条说明后停止逐条记录（说明自己也字节封顶）。
	if s.total+len(line) > maxStderrTotal {
		note := fmt.Sprintf("stderr 已截断（超过 %d 字节上限，后续输出不再记录）", maxStderrTotal)
		mcpLog.Warn("mcp: server stderr", "server", s.server, "line", note)
		s.total += len(note)
		s.truncated = true
		return
	}
	mcpLog.Warn("mcp: server stderr", "server", s.server, "line", line)
	s.total += len(line)
	s.lines++
}

// secretValues 列出这个 server 的**已知凭据值**（脱敏用）。
//
// 覆盖三处：配置里直填的 env、http 传输的 headers、显式从父 env 继承的
// env_from 的**值**（继承的是名字，值在这里才取得到）。
//
// **每个值登记两个形态**（见 add）：
//   - 原值（去首尾空白）；
//   - **剥掉 scheme 前缀**后的剩余部分（"Bearer sk-xxx" → "sk-xxx"）——
//     server 常把凭据单独打出来（"auth=sk-xxx"），只登记整串会漏掉这一形态。
//
// 太短的值不脱敏：1~3 个字符的"凭据"不是凭据，而把它当凭据会把整行日志
// 变成 "***"（诊断价值全失）。这是有意识的取舍，写在这里免得后人当成 bug。
func secretValues(cfg *ServerConfig) []string {
	if cfg == nil {
		return nil
	}
	var out []string
	seen := make(map[string]bool)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if len(v) < 4 {
			return
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
		// 剥掉 scheme 前缀后的**值本体**也要登记（"Bearer sk-x" → "sk-x"）。
		if body := stripScheme(v); body != v && len(body) >= 4 && !seen[body] {
			seen[body] = true
			out = append(out, body)
		}
	}
	for _, v := range cfg.Env {
		add(v)
	}
	for _, v := range cfg.Headers {
		add(v)
	}
	for _, name := range cfg.EnvFrom {
		if v, ok := os.LookupEnv(name); ok {
			add(v)
		}
	}
	return out
}

// credentialSchemes 是常见的凭据 scheme 词（小写比较；**不含分隔符**）。
//
// 为什么只认这几个：scheme 前缀是**协议形状**（HTTP 的认证 scheme），不是
// 凭据内容本身；剥掉它之后剩下的才是值本体。认不认得准不要紧——
// 脱敏是"多认一点总比漏好一点"的方向，而这里认错了最多是把一段普通文本
// 也打码（不会泄密）。
var credentialSchemes = []string{"bearer", "basic", "token", "apikey", "api-key"}

// credentialSeps 是 scheme 词与值本体之间的分隔符（v3-S2 清账：补 tab 变体）。
//
// 除了常规空格外，还要认 **tab**（"Bearer\tsk-x"）：server 打配置时用 tab 对齐很常见
// （`printf "%s\t%s"` / 日志表格），只认空格会漏掉这一形态的凭据值本体。
var credentialSeps = []string{" ", "\t"}

// stripScheme 剥掉凭据的 scheme 前缀（"Bearer sk-x" / "Bearer\tsk-x" → "sk-x"）。
//
// 只在前缀**恰好**是 "已知 scheme 词 + 一个空白分隔符（空格或 tab）"时才剥；
// 其余原样返回。
func stripScheme(v string) string {
	lower := strings.ToLower(v)
	for _, scheme := range credentialSchemes {
		for _, sep := range credentialSeps {
			prefix := scheme + sep
			if strings.HasPrefix(lower, prefix) {
				return strings.TrimSpace(v[len(prefix):])
			}
		}
	}
	return v
}

// redactSecrets 按值替换秘密（空值跳过——空串会替换掉一切）。
//
// 与 core/loop 的 redactSecrets 同口径：只认**确切的值**，不做形态猜测
// （猜形态会误伤正常内容，反而让日志没法看）。
func redactSecrets(text string, secrets []string) string {
	if text == "" || len(secrets) == 0 {
		return text
	}
	for _, sec := range secrets {
		if sec == "" {
			continue
		}
		text = strings.ReplaceAll(text, sec, "***")
	}
	return text
}
