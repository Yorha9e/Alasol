package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// 审批桥钉子：allow 放行 / deny 拒绝 / 客户端错误拒绝 / 超时 fail-close deny / 无连接 deny。
// 以及 H18 脱敏与 outcome 解析。

func sampleApprovalReq() contracts.ApprovalRequest {
	detail, _ := json.Marshal(map[string]any{
		"tool": "bash",
		"args": map[string]any{"command": "echo hi", "token": "sk-SECRET"},
	})
	return contracts.ApprovalRequest{
		V: 1, ID: contracts.Ulid("01HAAAAAAA"), TS: "2026-10-31T00:00:00Z",
		SessionID: contracts.Ulid("01SES000000"), AgentID: contracts.Ulid("01AGNT00000"),
		Trigger: contracts.ApprovalTriggerPolicyAsk, Summary: "策略拦截：工具 bash 需要人审",
		Detail: detail,
	}
}

// newApproverWithConn 造一个审批器 + 承载连接（Out 接 io.Pipe），返回连接输出侧的 reader
// （测试从中读 request_permission 帧并按需回响应）。
func newApproverWithConn(t *testing.T) (*Approver, *conn, *bufio.Reader) {
	t.Helper()
	pr, pw := io.Pipe()
	c := newConn(bufio.NewReader(bytes.NewReader(nil)), pw)
	a := NewApprover()
	a.bind(c)
	return a, c, bufio.NewReader(pr)
}

func TestApprover_Allow(t *testing.T) {
	a, c, cr := newApproverWithConn(t)
	go respondResult(cr, c, `{"outcome":"allow"}`)
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("allow 未放行：decision=%q note=%v", resp.Decision, derefStr(resp.Note))
	}
}

func TestApprover_Deny(t *testing.T) {
	a, c, cr := newApproverWithConn(t)
	go respondResult(cr, c, `{"outcome":"deny"}`)
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("deny 未拒绝：decision=%q", resp.Decision)
	}
}

// 无人应答（超时）→ fail-close deny。
func TestApprover_TimeoutFailClosed(t *testing.T) {
	a, _, cr := newApproverWithConn(t)
	// 起一个"只读不应答"的 drain：让 request_permission 能写出（io.Pipe 写阻塞到有人读），
	// 但永远不回应 → 模拟客户端超时不应答。
	go func() {
		for {
			if _, err := cr.ReadBytes('\n'); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	resp, err := a.Approve(ctx, sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("超时未 fail-close deny：decision=%q", resp.Decision)
	}
	if resp.Note == nil || !bytes.Contains([]byte(*resp.Note), []byte("无人应答")) {
		t.Errorf("超时 note 应写明无人应答：%v", derefStr(resp.Note))
	}
}

// 客户端回 JSON-RPC 错误 → fail-close deny。
func TestApprover_ClientErrorFailClosed(t *testing.T) {
	a, c, cr := newApproverWithConn(t)
	go respondError(cr, c, codeInternalError, "client blew up")
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("客户端错误未 fail-close deny：decision=%q", resp.Decision)
	}
}

// conn 未绑定（bind 前被调）→ fail-close deny。
func TestApprover_NoConnFailClosed(t *testing.T) {
	a := NewApprover() // 不 bind
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("无连接未 fail-close deny：decision=%q", resp.Decision)
	}
}

// ctx 已结束 → 连请求都不发，直接 deny。
func TestApprover_CtxAlreadyDone(t *testing.T) {
	a, _, _ := newApproverWithConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := a.Approve(ctx, sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("ctx 已结束未 deny：decision=%q", resp.Decision)
	}
}

// request_permission 的载荷：工具名带上、args 脱敏（token 打码）、不发原始 Detail。
func TestPermissionParams_Redaction(t *testing.T) {
	pr := permissionParams(sampleApprovalReq())
	if pr.ToolCall.Title != "bash" {
		t.Errorf("工具名应解出 bash，得到 %q", pr.ToolCall.Title)
	}
	if pr.ToolCall.RawInput == nil {
		t.Fatal("rawInput 不该为空（有脱敏摘要）")
	}
	if bytes.Contains(pr.ToolCall.RawInput, []byte("sk-SECRET")) {
		t.Errorf("凭据值泄漏进 rawInput：%s", pr.ToolCall.RawInput)
	}
	if !bytes.Contains(pr.ToolCall.RawInput, []byte("echo hi")) {
		t.Errorf("非敏感参数应保留：%s", pr.ToolCall.RawInput)
	}
}

func TestRedactJSON(t *testing.T) {
	if got := redactJSON(json.RawMessage(`not json`)); got != nil {
		t.Errorf("非 JSON 应返 nil，得到 %s", got)
	}
	out := redactJSON(json.RawMessage(`{"password":"p","nested":{"api_key":"k","keep":"v"},"arr":["a","b"]}`))
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["password"] != "***" {
		t.Errorf("password 应打码：%v", m["password"])
	}
	nested := m["nested"].(map[string]any)
	if nested["api_key"] != "***" {
		t.Errorf("嵌套 api_key 应打码：%v", nested["api_key"])
	}
	if nested["keep"] != "v" {
		t.Errorf("非敏感值应保留：%v", nested["keep"])
	}
}

func TestParsePermissionOutcome(t *testing.T) {
	cases := map[string]string{
		`{"outcome":"allow"}`:             outcomeAllow,
		`{"outcome":"deny"}`:              outcomeDeny,
		`{"outcome":"ALLOW"}`:             outcomeAllow, // 大小写宽容
		`{"decision":"allow"}`:            outcomeAllow, // 别名
		`{}`:                              outcomeDeny,  // 缺省 = deny
		`{"outcome":"cancel"}`:            outcomeDeny,  // 非 allow 一律 deny
		`{"outcome":{"outcome":"allow"}}`: outcomeAllow, // 嵌套旧写法（兼容老客户端）
		// ACP 官方 schema（F2）：selected + optionId。
		`{"outcome":{"outcome":"selected","optionId":"allow-once"}}`:   outcomeAllow,
		`{"outcome":{"outcome":"selected","optionId":"allow-always"}}`: outcomeAllow,
		`{"outcome":{"outcome":"selected","optionId":"reject-once"}}`:  outcomeDeny,
		`{"outcome":{"outcome":"selected","optionId":"unknown-opt"}}`:  outcomeDeny, // 认不出 = deny
		`{"outcome":{"outcome":"selected"}}`:                           outcomeDeny, // 没 optionId = deny
		`{"outcome":{"outcome":"cancelled"}}`:                          outcomeDeny, // 用户撤了 = deny
	}
	for in, want := range cases {
		if got := parsePermissionOutcome(json.RawMessage(in)); got != want {
			t.Errorf("parsePermissionOutcome(%s)=%q want %q", in, got, want)
		}
	}
}

// F2 钉子：ACP 官方 schema 的应答（selected + optionId）→ 放行。
//
// 这是"用现成协议接入"的审批闭环：现成 ACP 客户端按规范回
// {outcome:{outcome:"selected",optionId:"allow-once"}}——旧实现只认 allow/deny 词表，
// 规范应答会被判 deny（审批闭环不通）。
func TestApprover_ACPSchemaSelected(t *testing.T) {
	a, c, cr := newApproverWithConn(t)
	go respondResult(cr, c, `{"outcome":{"outcome":"selected","optionId":"allow-once"}}`)
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionAllow {
		t.Fatalf("官方 schema 的 selected 应放行：decision=%q note=%v", resp.Decision, derefStr(resp.Note))
	}
}

// F2 钉子：cancelled → 拒绝（用户撤了审批）。
func TestApprover_ACPSchemaCancelled(t *testing.T) {
	a, c, cr := newApproverWithConn(t)
	go respondResult(cr, c, `{"outcome":{"outcome":"cancelled"}}`)
	resp, err := a.Approve(context.Background(), sampleApprovalReq())
	if err != nil {
		t.Fatalf("Approve 不应返 error: %v", err)
	}
	if resp.Decision != contracts.ApprovalDecisionDeny {
		t.Fatalf("cancelled 应拒绝：decision=%q", resp.Decision)
	}
}

// F2 钉子：请求侧必须带 options 清单（客户端照着它回 optionId）。
func TestPermissionParams_HasOptions(t *testing.T) {
	pr := permissionParams(sampleApprovalReq())
	got := map[string]permissionOption{}
	for _, o := range pr.Options {
		got[o.OptionID] = o
		if o.Name == "" {
			t.Errorf("选项 %q 缺人读名字", o.OptionID)
		}
		if o.Kind == "" {
			t.Errorf("选项 %q 缺 kind", o.OptionID)
		}
	}
	for _, want := range []string{optionIDAllowOnce, optionIDAllowAlways, optionIDRejectOnce} {
		if _, ok := got[want]; !ok {
			t.Errorf("请求缺选项 %q（现成 ACP 客户端要照着它回）", want)
		}
	}
}

// F9 钉子：命令**值**里的凭据不能外发（键名脱敏挡不住——"command" 键名不敏感）。
func TestPermissionParams_RedactsCredentialInsideCommand(t *testing.T) {
	const secret = "sk-abcdefgh12345678"
	detail, _ := json.Marshal(map[string]any{
		"tool": "bash",
		"args": map[string]any{
			"command": `curl -H "Authorization: Bearer ` + secret + `" https://api.example.com/v1`,
		},
	})
	req := sampleApprovalReq()
	req.Detail = detail
	pr := permissionParams(req)
	if bytes.Contains(pr.ToolCall.RawInput, []byte(secret)) {
		t.Errorf("命令里的凭据泄漏进 rawInput：%s", pr.ToolCall.RawInput)
	}
	if !bytes.Contains(pr.ToolCall.RawInput, []byte("curl")) {
		t.Errorf("命令轮廓应保留（审批人要看得懂在干什么）：%s", pr.ToolCall.RawInput)
	}
}

// F9 钉子：URL userinfo 的口令也不能外发。
func TestPermissionParams_RedactsURLUserinfo(t *testing.T) {
	const secret = "hunter2-passphrase"
	detail, _ := json.Marshal(map[string]any{
		"tool": "bash",
		"args": map[string]any{
			"command": "curl https://alice:" + secret + "@example.com/v1",
		},
	})
	req := sampleApprovalReq()
	req.Detail = detail
	pr := permissionParams(req)
	if bytes.Contains(pr.ToolCall.RawInput, []byte(secret)) {
		t.Errorf("URL 口令泄漏进 rawInput：%s", pr.ToolCall.RawInput)
	}
}

// H18 钉子（N1）：`Authorization: <scheme> <value>` 的**值**必须打码——
// **scheme 留着**（审批人看得懂是哪种鉴权），**值**没了。
//
// 旧实现的病：把冒号后第一个词（= scheme）当成"值"打码 → 输出
// `Authorization: *** abc123def456`：**真凭据明文外发**，而且 scheme 被打掉后
// Bearer 规则再也匹配不到（连带把 `Bearer <token>` 那条也废了）。
//
// 注意用**不具特征形状**的 token（abc123def456）：sk-/ghp_ 这类形状会被词元扫描
// 兜住，掩盖掉"Authorization 规则打错对象"这个病（旧的 F9 钉子就是这么漏过去的）。
func TestRedactSecretText_AuthorizationMasksValueNotScheme(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		secret string
	}{
		{"Bearer 头", `curl -H "Authorization: Bearer abc123def456"`, "abc123def456"},
		{"Basic 头", "Authorization=Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"},
		{"无 scheme", "Authorization: abc123def456", "abc123def456"},
		{"小写", "authorization: bearer abc123def456", "abc123def456"},
	}
	for _, c := range cases {
		got := redactSecretText(c.in)
		if strings.Contains(got, c.secret) {
			t.Errorf("%s：凭据值泄漏：%q → %q", c.name, c.in, got)
		}
	}
	// scheme 留着（审批人要看得懂是哪种鉴权）：Bearer / Basic 不该被打掉。
	if got := redactSecretText(`Authorization: Bearer abc123def456`); !strings.Contains(got, "Bearer") {
		t.Errorf("scheme 应留着（只打码值）：%q", got)
	}
	if got := redactSecretText("Authorization=Basic dXNlcjpwYXNz"); !strings.Contains(got, "Basic") {
		t.Errorf("scheme 应留着（只打码值）：%q", got)
	}
}

// H18 钉子（N1 的另一半）：**孤立**的 `Bearer <token>` 仍被遮（既有行为不回归）。
func TestRedactSecretText_StandaloneBearerStillMasked(t *testing.T) {
	got := redactSecretText("Bearer abc123def456")
	if strings.Contains(got, "abc123def456") {
		t.Errorf("孤立 Bearer token 泄漏：%q", got)
	}
	if !strings.Contains(got, "Bearer") {
		t.Errorf("孤立 Bearer 的 scheme 应留着：%q", got)
	}
}

// F1 钉子：Authorization 的**值**定位**不靠 scheme 白名单**。
//
// 旧实现的病：白名单（bearer/basic/token/…）外的 scheme 会让规则吃掉 **scheme 本身**，
// 真值留明文——`Authorization: Custom abc123def456` 与
// `Authorization: AWS4-HMAC-SHA256 Credential=AKIA…` 两类都漏（白名单既是漏出口，
// 又是"每加一种 scheme 就漏一次"的维护负担）。
//
// 修法：**看词的个数，不看 scheme 是什么**——冒号后有第二个词就遮第二个词（值），
// 只有第一个词就遮它；参数化头（`response="…"`）的敏感参数值另立一条规则遮。
//
// 同样注意用**不具特征形状**的 token（abc123def456 / AKIAEXAMPLE）：sk-/ghp_ 这类形状
// 会被词元扫描兜住，掩盖掉"规则打错对象"这个病。
func TestRedactSecretText_AuthorizationMasksValueWhateverTheScheme(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		secret string
	}{
		{"白名单外的 scheme", "Authorization: Custom abc123def456", "abc123def456"},
		{"AWS4 签名头", "Authorization: AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE", "AKIAEXAMPLE"},
		{"参数化 Digest 头", `Authorization: Digest username="u", response="abc123def456"`, "abc123def456"},
		{"参数化 Signature 头", `Authorization: Signature credential="AKIAEXAMPLE", signed="x"`, "AKIAEXAMPLE"},
	}
	for _, c := range cases {
		got := redactSecretText(c.in)
		if strings.Contains(got, c.secret) {
			t.Errorf("%s：凭据值泄漏：%q → %q", c.name, c.in, got)
		}
	}
	// scheme 仍留着（审批人要看得懂是哪种鉴权）：白名单外的 scheme 也不例外。
	if got := redactSecretText("Authorization: Custom abc123def456"); !strings.Contains(got, "Custom") {
		t.Errorf("scheme 应留着（只打码值）：%q", got)
	}
}

// F1 钉子（端到端）：白名单外的 scheme 也不能把凭据带进 rawInput（外发面）。
func TestPermissionParams_RedactsCredentialInUnknownAuthzScheme(t *testing.T) {
	const secret = "abc123def456"
	detail, _ := json.Marshal(map[string]any{
		"tool": "bash",
		"args": map[string]any{
			"command": `curl -H "Authorization: Custom ` + secret + `" https://api.example.com/v1`,
		},
	})
	req := sampleApprovalReq()
	req.Detail = detail
	pr := permissionParams(req)
	if bytes.Contains(pr.ToolCall.RawInput, []byte(secret)) {
		t.Errorf("命令里的凭据泄漏进 rawInput：%s", pr.ToolCall.RawInput)
	}
	if !bytes.Contains(pr.ToolCall.RawInput, []byte("curl")) {
		t.Errorf("命令轮廓应保留（审批人要看得懂在干什么）：%s", pr.ToolCall.RawInput)
	}
}

// --- 小助手：模拟客户端回响应 ---

func respondResult(cr *bufio.Reader, c *conn, resultJSON string) {
	line, err := cr.ReadBytes('\n')
	if err != nil {
		return
	}
	var f inbound
	if json.Unmarshal(bytes.TrimSpace(line), &f) != nil || f.ID == nil {
		return
	}
	key, ok := idString(*f.ID)
	if !ok {
		return
	}
	c.deliverResponse(inboundResponse{result: json.RawMessage(resultJSON)}, key)
}

func respondError(cr *bufio.Reader, c *conn, code int, msg string) {
	line, err := cr.ReadBytes('\n')
	if err != nil {
		return
	}
	var f inbound
	if json.Unmarshal(bytes.TrimSpace(line), &f) != nil || f.ID == nil {
		return
	}
	key, ok := idString(*f.ID)
	if !ok {
		return
	}
	c.deliverResponse(inboundResponse{err: &rpcError{Code: code, Message: msg}}, key)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---------------------------------------------------------------------------
// 缺口 7 脱敏加固：漏网场景逐格钉住
// ---------------------------------------------------------------------------

// 六类漏网场景逐格（脱敏后**不含原凭据**；每类至少一格）。
//
// 格的性质：
//   - Authorization/Bearer、URL userinfo、连接串口令段是**既有覆盖的回归格**（本刀不动
//     其语义——既有脱敏用例全部原样保绿）；
//   - api-key 连字符赋值、AWS AKIA 长串、PEM 私钥块是**本刀补上的漏**（加固前实测全漏，
//     见 redactSecretText 的七步口径）。
//
// 变异自证：去掉 reAuthzValue（Authorization 模式）→ "Authorization/Bearer token" 格红。
func TestRedactSecretText_HardenedLeakScenarios(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		secrets []string // 脱敏后不允许出现的原凭据片段
		keep    string   // 轮廓保留（空 = 不查；审批人要看得懂在干什么）
	}{
		{
			name:    "Authorization/Bearer token",
			in:      `curl -H "Authorization: Bearer abc123def456secret99" https://api.example.com`,
			secrets: []string{"abc123def456secret99"},
			keep:    "Bearer",
		},
		{
			name:    "api_key 赋值(=)",
			in:      `export api_key=zzzqqqxxxwwwvvv111`,
			secrets: []string{"zzzqqqxxxwwwvvv111"},
		},
		{
			name:    "api-key 赋值(: 连字符键)",
			in:      `curl -H "api-key: zzzqqqxxxwwwvvv111" https://api.example.com`,
			secrets: []string{"zzzqqqxxxwwwvvv111"},
			keep:    "curl",
		},
		{
			name:    "URL userinfo",
			in:      `http://alice:hunter2secret@host.example/x`,
			secrets: []string{"hunter2secret"},
			keep:    "alice",
		},
		{
			name:    "AWS AKIA 长串(裸)",
			in:      `aws sts get-caller-identity --access-key-id AKIAIOSFODNN7EXAMPLE`,
			secrets: []string{"AKIAIOSFODNN7EXAMPLE"},
			keep:    "aws",
		},
		{
			name:    "AWS AKIA 长串(赋值)",
			in:      `AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE`,
			secrets: []string{"AKIAIOSFODNN7EXAMPLE"},
		},
		{
			name: "PEM 私钥块",
			in: "-----BEGIN RSA PRIVATE KEY-----\n" +
				"MIIEpAIBAAKCAQEAyabc123SECRETbody\n" +
				"-----END RSA PRIVATE KEY-----",
			secrets: []string{"MIIEpAIBAAKCAQEAyabc123SECRETbody", "BEGIN RSA PRIVATE KEY"},
		},
		{
			name:    "postgres 连接串口令段",
			in:      `psql postgres://dbuser:dbpassSECRET@db.example:5432/prod`,
			secrets: []string{"dbpassSECRET"},
			keep:    "postgres://dbuser:",
		},
	}
	for _, c := range cases {
		got := redactSecretText(c.in)
		for _, s := range c.secrets {
			if strings.Contains(got, s) {
				t.Errorf("%s：原凭据泄漏：%q → %q", c.name, c.in, got)
			}
		}
		if c.keep != "" && !strings.Contains(got, c.keep) {
			t.Errorf("%s：轮廓应保留 %q：%q", c.name, c.keep, got)
		}
	}
}

// JSON 键面的加固格：api-key（**连字符**键名——apikey / api_key 两个子串都套不中它）
// 与 AWS_ACCESS_KEY_ID（键名不在敏感词表 → 靠**值级** AKIA 形状兜）脱敏后值必须打码。
// Authorization 是既有键的回归格（敏感词 "auth" 命中，不许因加固改语义）。
func TestRedactJSON_HardenedKeyScenarios(t *testing.T) {
	cases := []struct {
		key    string
		value  string
		secret string
	}{
		{"api-key", "zzzqqqxxxwwwvvv111", "zzzqqqxxxwwwvvv111"},
		{"AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"},
		{"Authorization", "Custom abc123def456", "abc123def456"},
	}
	for _, c := range cases {
		raw, _ := json.Marshal(map[string]string{c.key: c.value})
		out := redactJSON(raw)
		if out == nil {
			t.Errorf("%s：redactJSON 返 nil（合法 JSON 不该解不开）", c.key)
			continue
		}
		if bytes.Contains(out, []byte(c.secret)) {
			t.Errorf("JSON 键 %s：凭据值泄漏：%s", c.key, out)
		}
	}
}
