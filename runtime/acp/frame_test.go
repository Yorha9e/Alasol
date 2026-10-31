package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// 帧编解码钉子：请求/响应/通知/错误/坏 JSON/换行边界/大帧/id 配对。

func TestDecodeFrame_ThreeForms(t *testing.T) {
	cases := []struct {
		name       string
		line       string
		wantResp   bool
		wantReq    bool
		wantNotif  bool
		wantMethod string
		wantErr    bool
	}{
		{name: "request", line: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`, wantReq: true, wantMethod: "initialize"},
		{name: "request string id", line: `{"jsonrpc":"2.0","id":"abc","method":"session/new"}`, wantReq: true, wantMethod: "session/new"},
		{name: "notification", line: `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"x"}}`, wantNotif: true, wantMethod: "session/cancel"},
		{name: "response success", line: `{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}`, wantResp: true},
		{name: "response error", line: `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`, wantResp: true},
		{name: "response null result", line: `{"jsonrpc":"2.0","id":1,"result":null}`, wantResp: true},
		{name: "empty line", line: `   `, wantErr: false}, // → (nil,nil) skip; 单独处理
		{name: "bad json", line: `{bad`, wantErr: true},
		{name: "not an object", line: `[1,2,3]`, wantErr: true},
		{name: "scalar", line: `42`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := decodeFrame([]byte(tc.line))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望解帧失败，得到 f=%+v", f)
				}
				return
			}
			if err != nil {
				t.Fatalf("解帧出错: %v", err)
			}
			if f == nil {
				// 只有空行用例会走到这（skip）。
				if strings.TrimSpace(tc.line) != "" {
					t.Fatalf("非空行却解成 skip（nil）: %q", tc.line)
				}
				return
			}
			if f.isResponse() != tc.wantResp {
				t.Errorf("isResponse=%v want %v", f.isResponse(), tc.wantResp)
			}
			if f.isRequest() != tc.wantReq {
				t.Errorf("isRequest=%v want %v", f.isRequest(), tc.wantReq)
			}
			if f.isNotification() != tc.wantNotif {
				t.Errorf("isNotification=%v want %v", f.isNotification(), tc.wantNotif)
			}
			if tc.wantMethod != "" && (f.Method == nil || *f.Method != tc.wantMethod) {
				t.Errorf("method=%v want %q", f.Method, tc.wantMethod)
			}
		})
	}
}

func TestEncodeFrame_RoundTrip(t *testing.T) {
	// 请求 → 编一行 → 解回来，字段一致。
	id := json.RawMessage(`7`)
	req, err := newRequest(id, "session/prompt", map[string]any{"sessionId": "s1"})
	if err != nil {
		t.Fatal(err)
	}
	line, err := encodeFrameForTest(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(line, []byte("\n")) {
		t.Errorf("帧必须以 \\n 结尾: %q", line)
	}
	if bytes.Count(line, []byte("\n")) != 1 {
		t.Errorf("一帧只能有一个换行: %q", line)
	}
	back, err := decodeFrame(line)
	if err != nil {
		t.Fatal(err)
	}
	if !back.isRequest() || *back.Method != "session/prompt" {
		t.Fatalf("round-trip 丢了 request 形状: %+v", back)
	}
	if string(*back.ID) != "7" {
		t.Errorf("id 未逐字节保留: %q", string(*back.ID))
	}
}

func TestErrorObject_Shape(t *testing.T) {
	o, err := newError(json.RawMessage(`1`), codeMethodNotFound, "Method not found", map[string]any{"m": "x"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		JSONRPC string   `json:"jsonrpc"`
		ID      int      `json:"id"`
		Error   rpcError `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.JSONRPC != "2.0" || got.ID != 1 {
		t.Errorf("envelope 不对: %+v", got)
	}
	if got.Error.Code != -32601 || got.Error.Message != "Method not found" {
		t.Errorf("error 对象不对: %+v", got.Error)
	}
	if string(got.Error.Data) == "" {
		t.Errorf("error.data 丢了")
	}
}

// 换行边界：一个流里多条帧，逐条读；空行跳过。
func TestFrameScanner_NewlineBoundaries(t *testing.T) {
	stream := "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"a\"}\n" +
		"\n" + // 空行：跳过
		"   \n" + // 全空白：跳过
		"{\"jsonrpc\":\"2.0\",\"method\":\"b\"}\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n"
	fs := newFrameScanner(bufio.NewReader(strings.NewReader(stream)))

	f1, err := fs.next()
	if err != nil || f1 == nil || !f1.isRequest() || *f1.Method != "a" {
		t.Fatalf("第 1 帧不对: f=%+v err=%v", f1, err)
	}
	f2, err := fs.next()
	if err != nil || f2 == nil || !f2.isNotification() || *f2.Method != "b" {
		t.Fatalf("第 2 帧（空行被跳过）不对: f=%+v err=%v", f2, err)
	}
	f3, err := fs.next()
	if err != nil || f3 == nil || !f3.isResponse() {
		t.Fatalf("第 3 帧不对: f=%+v err=%v", f3, err)
	}
	// EOF：(nil, nil)。
	f4, err := fs.next()
	if err != nil || f4 != nil {
		t.Fatalf("EOF 应返 (nil,nil)，得到 f=%+v err=%v", f4, err)
	}
}

// 坏 JSON：坏帧后仍能继续读后续好帧（D7 宽容——一条坏帧不打死连接）。
func TestFrameScanner_BadFrameThenRecover(t *testing.T) {
	stream := "{bad json\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"ok\"}\n"
	fs := newFrameScanner(bufio.NewReader(strings.NewReader(stream)))
	if _, err := fs.next(); err == nil {
		t.Fatal("坏帧应报错")
	}
	f, err := fs.next()
	if err != nil || f == nil || *f.Method != "ok" {
		t.Fatalf("坏帧后应能继续读好帧: f=%+v err=%v", f, err)
	}
}

// 大帧：远超默认 64KB 的一帧（< 8MB 上限）能正确 round-trip。
func TestFrameScanner_LargeFrame(t *testing.T) {
	big := strings.Repeat("x", 1<<20) // 1MB
	line, err := encodeFrameForTest(mustNotif(t, "session/update", map[string]any{
		"sessionId": "s",
		"update":    map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": big}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	fs := newFrameScanner(bufio.NewReader(bytes.NewReader(line)))
	f, err := fs.next()
	if err != nil || f == nil {
		t.Fatalf("大帧读取失败: err=%v", err)
	}
	var p updateParams
	if err := json.Unmarshal(f.Params, &p); err != nil {
		t.Fatal(err)
	}
	u := p.Update
	if u.Content == nil {
		t.Fatal("大帧 content 丢空")
	}
	// 逐字节校验载荷确实完整到达（1MB 没被截断）。
	raw, _ := json.Marshal(u.Content)
	if !bytes.Contains(raw, []byte(big)) {
		t.Fatalf("大帧载荷不完整（长度 %d，期望含 %d 字节）", len(raw), len(big))
	}
}

// F1 钉子（帧层）：超上限的帧被**跳过**，后续帧仍能读出来。
//
// 事故原委：旧实现用 bufio.Scanner，它的错误是粘性的（ErrTooLong 之后永远读不出下一行），
// 读循环把终态错当"可恢复坏帧"无限重试 → 8 秒吐 118 万条错误帧、进程永不退出。
// 现在：超限行被跳到行尾丢弃（内存有界），连接继续读后续帧。
func TestFrameScanner_OversizedFrameSkipped(t *testing.T) {
	huge := strings.Repeat("y", maxFrameBytes+1024)
	stream := `{"jsonrpc":"2.0","id":1,"method":"a","params":{"x":"` + huge + `"}}` + "\n" +
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ok\"}\n"
	fs := newFrameScanner(bufio.NewReader(strings.NewReader(stream)))

	// 第一条：超限 → 报"超限"（**可恢复**：该行已被跳过）。
	_, err := fs.next()
	if !errors.Is(err, errFrameTooLong) {
		t.Fatalf("超限帧应报 errFrameTooLong，得到 %v", err)
	}
	// 第二条：后续帧照常读得到（连接没死）。
	f, err := fs.next()
	if err != nil || f == nil || f.Method == nil || *f.Method != "ok" {
		t.Fatalf("超限帧之后应能继续读好帧: f=%+v err=%v", f, err)
	}
}

// N4 钉子（帧层）：额度按**帧体**字节算——行分隔符（"\n" / CRLF 的 "\r"）**不算**进
// 额度，故**恰好** maxFrameBytes 的帧合法（+1 字节才拒）。
//
// 病根（off-by-one）：额度本意是"一帧最多 8MiB"，但旧实现把行尾的 "\n" 也算进去 →
// 客户端发一个**合法**的 8MiB 帧会被回 -32600（"帧超限"）——白挨一枪。
func TestFrameScanner_FrameSizeBoundaryExcludesLineSeparator(t *testing.T) {
	body := strings.Repeat("y", maxFrameBytes)

	// 恰好 maxFrameBytes 字节的帧体 → 接受。
	fs := newFrameScanner(bufio.NewReader(strings.NewReader(body + "\n")))
	line, err := fs.readLine()
	if err != nil {
		t.Fatalf("恰好 %d 字节的帧应被接受，得到 %v", maxFrameBytes, err)
	}
	if len(line) != maxFrameBytes {
		t.Errorf("帧体长度=%d want %d", len(line), maxFrameBytes)
	}

	// +1 字节 → 超限（该行被跳过，连接可继续）。
	fs2 := newFrameScanner(bufio.NewReader(strings.NewReader(body + "y\n")))
	if _, err := fs2.readLine(); !errors.Is(err, errFrameTooLong) {
		t.Fatalf("超 1 字节的帧应报 errFrameTooLong，得到 %v", err)
	}

	// CRLF 客户端：分隔符是 "\r\n"（两字节都不算进额度）→ 同样接受。
	fs3 := newFrameScanner(bufio.NewReader(strings.NewReader(body + "\r\n")))
	if _, err := fs3.readLine(); err != nil {
		t.Fatalf("CRLF 行恰好 %d 字节应被接受，得到 %v", maxFrameBytes, err)
	}
}

// F1 钉子（帧层）：底层读错误是**终态**——包成 terminalReadError（调用方据此结束连接）。
func TestFrameScanner_TerminalReadError(t *testing.T) {
	boom := errors.New("底层流坏了")
	r := &failingReader{data: []byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"a\"}\n"), err: boom}
	fs := newFrameScanner(bufio.NewReader(r))
	f, err := fs.next()
	if err != nil || f == nil {
		t.Fatalf("第一帧应正常读出: f=%+v err=%v", f, err)
	}
	_, err = fs.next()
	var te *terminalReadError
	if !errors.As(err, &te) {
		t.Fatalf("底层读错误应包成 terminalReadError，得到 %v", err)
	}
	if !errors.Is(err, boom) {
		t.Errorf("终态错应保留原始错误链: %v", err)
	}
}

// failingReader 读完一段数据后返回一个**非 EOF** 的错误（模拟底层流坏了）。
type failingReader struct {
	data []byte
	off  int
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}

// --- 小助手 ---

// encodeFrameForTest 编一帧（raw + "\n"），与 conn.writeFrame 的字节形状一致。
func encodeFrameForTest(o outbound) ([]byte, error) {
	raw, err := json.Marshal(o)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func mustNotif(t *testing.T, method string, params any) outbound {
	t.Helper()
	o, err := newNotification(method, params)
	if err != nil {
		t.Fatal(err)
	}
	return o
}
