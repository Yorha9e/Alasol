package transport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
	"github.com/Yorha9e/alasol/runtime/transport"
	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// newHub 建一条真的事件流（真 SQLite 文件库 + bus），返回 bus 与关闭函数。
//
// 用临时**文件**库而不是 :memory:：内存库 + 连接池在并发写下会各自看到空 schema
// （实测 "no such table: events"），文件库与真实部署一致。
func newHub(t *testing.T) (*bus.Bus, func()) {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"), sqlite.Options{})
	if err != nil {
		t.Fatalf("打开事件库: %v", err)
	}
	return bus.New(st), func() { _ = st.Close() }
}

// newWSServer 起一个只挂 /ws 的 httptest 服务端。
func newWSServer(t *testing.T, s *transport.Server) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.ServeWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

// mustDial 连上去（失败即中止当前测试 goroutine）。
func mustDial(t *testing.T, srv *httptest.Server) *transport.Client {
	t.Helper()
	c, err := dial(srv)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// dial 连上去（返回 error，供并发 goroutine 用）。
func dial(srv *httptest.Server) (*transport.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return transport.Dial(ctx, wsURL(srv))
}

// recvTimeout 带超时地收一条消息（避免测试挂死；失败即中止当前 goroutine）。
func recvTimeout(t *testing.T, c *transport.Client, d time.Duration) contracts.TransportMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	m, err := c.RecvContext(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	return m
}

// recvErr 带超时地收一条消息（返回 error，供并发 goroutine 用）。
func recvErr(c *transport.Client, d time.Duration) (contracts.TransportMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.RecvContext(ctx)
}

// recvAck 收一条订阅回执并断言 kind=subscribed（G8：订阅后服务端先回执再推事件）。
func recvAck(t *testing.T, c *transport.Client) contracts.TransportMessage {
	t.Helper()
	m := recvTimeout(t, c, 5*time.Second)
	if m.Kind != transport.KindSubscribed {
		t.Fatalf("订阅后首条应是 kind=subscribed 回执，得到 kind=%q", m.Kind)
	}
	return m
}

// publish 发一批事件（durable=true，走真 store → 带 seq）。
func publish(t *testing.T, b *bus.Bus, sid contracts.Ulid, kinds ...string) error {
	t.Helper()
	evs := make([]contracts.Event, 0, len(kinds))
	for _, k := range kinds {
		evs = append(evs, contracts.Event{
			V: 1, ID: core.NewUlid(), Kind: k, TS: "2026-01-01T00:00:00Z",
			SessionID: sid, AgentID: core.NewUlid(), Durable: true,
			Payload: json.RawMessage(`{"k":"` + k + `"}`),
		})
	}
	if err := b.Publish(context.Background(), evs...); err != nil {
		return err
	}
	return nil
}

func decodeEvent(t *testing.T, m contracts.TransportMessage) contracts.Event {
	t.Helper()
	if m.Kind != transport.KindEvent {
		t.Fatalf("kind = %q，want %q", m.Kind, transport.KindEvent)
	}
	var ev contracts.Event
	if err := json.Unmarshal(m.Payload, &ev); err != nil {
		t.Fatalf("event payload 解不开: %v", err)
	}
	return ev
}

// ---------------------------------------------------------------------------
// ping/pong + NDJSON 行式 framing
// ---------------------------------------------------------------------------

func TestPingPongRoundTrip(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ping, err := transport.NewMessage(transport.KindPing, nil)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}

	pong := recvTimeout(t, c, 5*time.Second)
	if pong.Kind != transport.KindPong {
		t.Fatalf("kind = %q，want pong", pong.Kind)
	}
	if pong.ReplyTo == nil || *pong.ReplyTo != ping.ID {
		t.Fatalf("pong.reply_to = %v，want ping.id %s", pong.ReplyTo, ping.ID)
	}
	if pong.V != transport.ContractVersion {
		t.Fatalf("pong.v = %d，want %d", pong.V, transport.ContractVersion)
	}
}

// TestOpaqueCorrelationIDIsEchoedVerbatim（G9 钉子）：消息 id 是**客户端关联 id**——
// 客户端可以造**任意不透明字符串**（不是 ULID），服务端**原样回指**（不校验、不拒绝、
// 不去重）。这钉住 transport.json 的 id/reply_to 口径（"客户端关联 id，服务端只回指不校验"）。
func TestOpaqueCorrelationIDIsEchoedVerbatim(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ① 正常 kind（ping）：pong.reply_to 原样回指不透明 id。
	const opaqueID = "client-req-0001/自定义 id"
	ping := contracts.TransportMessage{V: transport.ContractVersion, ID: opaqueID, Kind: transport.KindPing}
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	pong := recvTimeout(t, c, 5*time.Second)
	if pong.Kind != transport.KindPong {
		t.Fatalf("kind = %q，want pong", pong.Kind)
	}
	if pong.ReplyTo == nil || *pong.ReplyTo != opaqueID {
		t.Fatalf("pong.reply_to = %v，want %q（原样回指）", pong.ReplyTo, opaqueID)
	}

	// ② 协议违规（不认识的 kind）：error.reply_to 同样原样回指。
	bad := contracts.TransportMessage{V: transport.ContractVersion, ID: opaqueID + "#2", Kind: "bogus.kind"}
	if err := c.SendContext(ctx, bad); err != nil {
		t.Fatalf("send bad: %v", err)
	}
	m := recvTimeout(t, c, 5*time.Second)
	if m.Kind != transport.KindError {
		t.Fatalf("kind = %q，want error", m.Kind)
	}
	if m.ReplyTo == nil || *m.ReplyTo != opaqueID+"#2" {
		t.Fatalf("error.reply_to = %v，want %q（原样回指）", m.ReplyTo, opaqueID+"#2")
	}
}

// TestNDJSONLineFraming：出站消息是"一行一个 TransportMessage"——结尾换行、无内嵌换行。
// 用裸 WS 客户端读原始字节验证 framing（我们的 Client 会解析 JSON，看不出 framing）。
func TestNDJSONLineFraming(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL(srv), nil)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer conn.CloseNow()

	// 订阅（同一连接上先 subscribe，再触发一条事件）。
	line, err := json.Marshal(contracts.TransportMessage{
		V: transport.ContractVersion, ID: string(core.NewUlid()), Kind: transport.KindSubscribe,
	})
	if err != nil {
		t.Fatalf("marshal subscribe: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, append(line, '\n')); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if err := publish(t, b, core.NewUlid(), "context.assembled"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 订阅后服务端先回一条 kind=subscribed（G8），再推事件——两条都必须是"一行一消息"。
	_, ackData, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("raw read (ack): %v", err)
	}
	if n := strings.Count(string(ackData), "\n"); n != 1 {
		t.Fatalf("回执行应只含一个换行（行尾），得到 %d 个: %q", n, ackData)
	}
	var ack contracts.TransportMessage
	if err := json.Unmarshal(bytes.TrimSpace(ackData), &ack); err != nil {
		t.Fatalf("回发行解不成 TransportMessage: %v", err)
	}
	if ack.Kind != transport.KindSubscribed {
		t.Fatalf("订阅回执 kind = %q，want subscribed", ack.Kind)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("raw read: %v", err)
	}
	if n := strings.Count(string(data), "\n"); n != 1 {
		t.Fatalf("一行应只含一个换行（行尾），得到 %d 个: %q", n, data)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Fatalf("行尾应是换行符（NDJSON）: %q", data)
	}
	var m contracts.TransportMessage
	if err := json.Unmarshal(bytes.TrimSpace(data), &m); err != nil {
		t.Fatalf("一行解不成 TransportMessage: %v", err)
	}
	if m.Kind != transport.KindEvent {
		t.Fatalf("kind = %q，want event", m.Kind)
	}
}

// TestSubscribeKindPrefixes：按 kind 前缀过滤推送（K4）。
func TestSubscribeKindPrefixes(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{
		KindPrefixes: []string{"tool.call."},
	})
	if err := c.SendContext(ctx, sub); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	recvAck(t, c) // G8：订阅先回执

	sid := core.NewUlid()
	if err := publish(t, b, sid, "context.assembled", "tool.call.intent", "loop.error", "tool.call.completed"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var got []string
	for i := 0; i < 2; i++ {
		got = append(got, decodeEvent(t, recvTimeout(t, c, 5*time.Second)).Kind)
	}
	if len(got) != 2 || got[0] != "tool.call.intent" || got[1] != "tool.call.completed" {
		t.Fatalf("推送 kind = %v，want [tool.call.intent tool.call.completed]", got)
	}

	// 再多发一条不该收到的：确认过滤器持续生效（不会"收到一次就全放"）。
	if err := publish(t, b, sid, "context.assembled"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := recvErr(c, 300*time.Millisecond); err == nil {
		t.Fatal("不该再收到任何推送（前缀过滤应持续生效）")
	}
}

// TestSubscribeSessionFilter：session 过滤（K4）。
func TestSubscribeSessionFilter(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	want := core.NewUlid()
	other := core.NewUlid()
	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{SessionID: &want})
	if err := c.SendContext(ctx, sub); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	recvAck(t, c) // G8：订阅先回执

	if err := publish(t, b, other, "tool.call.intent"); err != nil {
		t.Fatalf("publish other: %v", err)
	}
	if err := publish(t, b, want, "tool.call.intent", "tool.call.failed"); err != nil {
		t.Fatalf("publish want: %v", err)
	}

	ev := decodeEvent(t, recvTimeout(t, c, 5*time.Second))
	if ev.SessionID != want {
		t.Fatalf("session_id = %q，want %q（别的 session 不该收到）", ev.SessionID, want)
	}
	if got := decodeEvent(t, recvTimeout(t, c, 5*time.Second)).Kind; got != "tool.call.failed" {
		t.Fatalf("第二条 = %q，want tool.call.failed", got)
	}
}

// TestSubscribeDurableOnly：durable_only=true 时流事件（durable=false）不推送（F8）。
func TestSubscribeDurableOnly(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	yes := true
	sub, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{DurableOnly: &yes})
	if err := c.SendContext(ctx, sub); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	recvAck(t, c) // G8：订阅先回执

	// 流事件（durable=false）不入库也不该推给 durable_only 订阅者。
	if err := b.Publish(context.Background(), contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "model.delta", TS: "2026-01-01T00:00:00Z",
		SessionID: core.NewUlid(), AgentID: core.NewUlid(), Durable: false,
	}); err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	if _, err := recvErr(c, 300*time.Millisecond); err == nil {
		t.Fatal("durable_only 订阅者不该收到流事件")
	}

	// 事实事件照常推送。
	if err := publish(t, b, core.NewUlid(), "tool.call.intent"); err != nil {
		t.Fatalf("publish durable: %v", err)
	}
	if got := decodeEvent(t, recvTimeout(t, c, 5*time.Second)).Kind; got != "tool.call.intent" {
		t.Fatalf("kind = %q，want tool.call.intent", got)
	}
}

// TestOriginRejected：跨源 Origin 一律 403（N5b）。
func TestOriginRejected(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://evil.example.com"}},
	})
	if err == nil {
		conn.CloseNow()
		t.Fatal("跨源 Origin 应该被拒绝")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("状态码 = %v，want 403（err=%v）", resp, err)
	}
	var te contracts.TransportError
	if jerr := json.NewDecoder(resp.Body).Decode(&te); jerr != nil {
		t.Fatalf("错误体不是 JSON: %v", jerr)
	}
	defer resp.Body.Close()
	if te.Code != transport.CodeOriginRejected {
		t.Fatalf("错误码 = %q，want %q", te.Code, transport.CodeOriginRejected)
	}
}

// TestSameOriginAccepted：同源 Origin（hub 自己的 Web UI）放行。
func TestSameOriginAccepted(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	host := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://" + host}},
	})
	if err != nil {
		t.Fatalf("同源 Origin 应放行: %v（status=%v）", err, resp)
	}
	defer conn.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("状态码 = %d，want 101", resp.StatusCode)
	}
}

// TestNoOriginAccepted：无 Origin 头的非浏览器客户端（CLI / curl）放行。
func TestNoOriginAccepted(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ping, _ := transport.NewMessage(transport.KindPing, nil)
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if got := recvTimeout(t, c, 5*time.Second).Kind; got != transport.KindPong {
		t.Fatalf("kind = %q，want pong", got)
	}
}

// TestLoopbackOriginCrossSpellingAccepted：SSH -L 隧道后的 loopback 拼写差异应放行（v4-2-A）。
//
// httptest 服务端的请求 Host 是 127.0.0.1:<port>；用户经 ssh -L 转发后用浏览器开
// http://localhost:<port>，页面 Origin=http://localhost:<port>——两端同端口、都 loopback，
// 旧判据（字符串相等）会误拒，v4-2-A 起 loopback 互认放行（101 握手成功）。
func TestLoopbackOriginCrossSpellingAccepted(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatalf("split host: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://localhost:" + port}},
	})
	if err != nil {
		t.Fatalf("loopback 拼写差异应放行: %v（status=%v）", err, resp)
	}
	defer conn.CloseNow()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("状态码 = %d，want 101", resp.StatusCode)
	}
}

// TestLoopbackDifferentPortRejected：loopback 但**不同端口** = 浏览器视角的不同源，仍 403。
// 钉死"loopback 互认只同端口"——否则本机另一端口上的恶意页面又能连 hub 了。
func TestLoopbackDifferentPortRejected(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL(srv), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://localhost:9999"}},
	})
	if err == nil {
		conn.CloseNow()
		t.Fatal("loopback 不同端口应被拒（浏览器视角的不同源）")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("状态码 = %v，want 403", resp)
	}
}
func TestUnknownKindGetsErrorReply(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bad, _ := transport.NewMessage("bogus.kind", nil)
	if err := c.SendContext(ctx, bad); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := recvTimeout(t, c, 5*time.Second)
	if m.Kind != transport.KindError {
		t.Fatalf("kind = %q，want error", m.Kind)
	}
	if m.ReplyTo == nil || *m.ReplyTo != bad.ID {
		t.Fatalf("reply_to = %v，want %s", m.ReplyTo, bad.ID)
	}
	var te contracts.TransportError
	if err := json.Unmarshal(m.Payload, &te); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if te.Code != transport.CodeUnknownKind {
		t.Fatalf("code = %q，want %q", te.Code, transport.CodeUnknownKind)
	}
}

// TestClientSendingEventIsRejected：客户端擅自发 kind=event → forbidden_kind。
func TestClientSendingEventIsRejected(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev := contracts.Event{V: 1, ID: core.NewUlid(), Kind: "tool.call.intent", TS: "2026-01-01T00:00:00Z",
		SessionID: core.NewUlid(), AgentID: core.NewUlid(), Durable: true}
	m, _ := transport.NewMessage(transport.KindEvent, ev)
	if err := c.SendContext(ctx, m); err != nil {
		t.Fatalf("send: %v", err)
	}
	got := recvTimeout(t, c, 5*time.Second)
	if got.Kind != transport.KindError {
		t.Fatalf("kind = %q，want error", got.Kind)
	}
	var te contracts.TransportError
	_ = json.Unmarshal(got.Payload, &te)
	if te.Code != transport.CodeForbiddenKind {
		t.Fatalf("code = %q，want %q", te.Code, transport.CodeForbiddenKind)
	}
}

// TestConcurrentSubscribersEachGetsOwnFilter：并发订阅互不串扰（每个连接独立过滤）。
func TestConcurrentSubscribersEachGetsOwnFilter(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := dial(srv)
			if err != nil {
				errs <- "dial: " + err.Error()
				return
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			// 第 i 个客户端只订阅自己的前缀（互相不重叠）。
			prefix := "kind" + string(rune('a'+i)) + "."
			sub, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{
				KindPrefixes: []string{prefix},
			})
			if err := c.SendContext(ctx, sub); err != nil {
				errs <- "subscribe: " + err.Error()
				return
			}
			// G8：先收订阅回执（各连接独立）。
			ack := recvTimeout(t, c, 5*time.Second)
			if ack.Kind != transport.KindSubscribed {
				errs <- "订阅回执 kind = " + ack.Kind
				return
			}

			// 让所有订阅都就位后再统一发事件。
			time.Sleep(300 * time.Millisecond)
			if err := publish(t, b, core.NewUlid(), prefix+"x"); err != nil {
				errs <- "publish: " + err.Error()
				return
			}
			ev, err := recvErr(c, 10*time.Second)
			if err != nil {
				errs <- "recv: " + err.Error()
				return
			}
			if !strings.HasPrefix(decodeEvent(t, ev).Kind, prefix) {
				errs <- "收到不该收到的: " + decodeEvent(t, ev).Kind
				return
			}
			// 再发一条别人的：不该收到（无串扰）。
			if err := publish(t, b, core.NewUlid(), "other.y"); err != nil {
				errs <- "publish other: " + err.Error()
				return
			}
			if _, err := recvErr(c, 300*time.Millisecond); err == nil {
				errs <- "串扰：收到了别人的事件"
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatal(msg)
	}
}

// TestReplaceSubscriptionOnResubscribe：同连接重复 subscribe = 替换（新过滤生效）。
func TestReplaceSubscriptionOnResubscribe(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{KindPrefixes: []string{"a."}})
	if err := c.SendContext(ctx, first); err != nil {
		t.Fatalf("subscribe1: %v", err)
	}
	ack1 := recvAck(t, c) // 第一条订阅：replaced=false
	if ack1.Payload != nil {
		var p contracts.SubscribeAck
		if err := json.Unmarshal(ack1.Payload, &p); err != nil {
			t.Fatalf("ack1 payload: %v", err)
		}
		if p.Replaced {
			t.Fatalf("首条订阅 replaced 应为 false")
		}
	}
	second, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{KindPrefixes: []string{"b."}})
	if err := c.SendContext(ctx, second); err != nil {
		t.Fatalf("subscribe2: %v", err)
	}
	ack2 := recvAck(t, c) // 重复订阅 = 替换：replaced=true
	var p2 contracts.SubscribeAck
	if err := json.Unmarshal(ack2.Payload, &p2); err != nil {
		t.Fatalf("ack2 payload: %v", err)
	}
	if !p2.Replaced {
		t.Fatalf("重复订阅 replaced 应为 true（G8）")
	}
	if len(p2.KindPrefixes) != 1 || p2.KindPrefixes[0] != "b." {
		t.Fatalf("回执应回显生效的 kind_prefixes=[b.]，得到 %v", p2.KindPrefixes)
	}

	if err := publish(t, b, core.NewUlid(), "a.x"); err != nil {
		t.Fatalf("publish a: %v", err)
	}
	if _, err := recvErr(c, 300*time.Millisecond); err == nil {
		t.Fatal("换订阅后不该再收到旧前缀的事件")
	}

	if err := publish(t, b, core.NewUlid(), "b.y"); err != nil {
		t.Fatalf("publish b: %v", err)
	}
	if got := decodeEvent(t, recvTimeout(t, c, 5*time.Second)).Kind; got != "b.y" {
		t.Fatalf("新订阅应生效，得到 %q", got)
	}
}

// TestBadLineDoesNotKillConnection：一条缺 kind 的消息 → error 回复，连接继续可用。
func TestBadLineDoesNotKillConnection(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 合法 JSON 但缺 kind → 服务端回 bad_json，连接不死。
	bad, _ := transport.NewMessage("", nil)
	if err := c.SendContext(ctx, bad); err != nil {
		t.Fatalf("send bad: %v", err)
	}
	ping, _ := transport.NewMessage(transport.KindPing, nil)
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		m := recvTimeout(t, c, 5*time.Second)
		if m.Kind == transport.KindError {
			continue
		}
		if m.Kind == transport.KindPong {
			return // 连接在坏行之后仍然可用
		}
		if time.Now().After(deadline) {
			t.Fatalf("坏行之后连接不可用（收到 kind=%q）", m.Kind)
		}
	}
}

// TestSubscribeAckEchoesEffectiveFilter（G8）：subscribed 回执回显**生效的**过滤条件
// （session_id / kind_prefixes / durable_only），让客户端不必靠"事件到不到"反推订阅。
func TestSubscribeAckEchoesEffectiveFilter(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	srv := newWSServer(t, transport.NewServer(b))
	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := core.NewUlid()
	yes := true
	sub, _ := transport.NewMessage(transport.KindSubscribe, contracts.SubscribeRequest{
		SessionID:    &want,
		KindPrefixes: []string{"tool.call.", "context.assembled"},
		DurableOnly:  &yes,
	})
	if err := c.SendContext(ctx, sub); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ack := recvAck(t, c)
	if ack.ReplyTo == nil || *ack.ReplyTo != sub.ID {
		t.Fatalf("回执 reply_to 应指向原 subscribe: %v", ack.ReplyTo)
	}
	var p contracts.SubscribeAck
	if err := json.Unmarshal(ack.Payload, &p); err != nil {
		t.Fatalf("ack payload: %v", err)
	}
	if p.SessionID == nil || *p.SessionID != want {
		t.Fatalf("回执 session_id = %v，want %s", p.SessionID, want)
	}
	if len(p.KindPrefixes) != 2 || p.KindPrefixes[0] != "tool.call." || p.KindPrefixes[1] != "context.assembled" {
		t.Fatalf("回执 kind_prefixes = %v", p.KindPrefixes)
	}
	if p.DurableOnly == nil || !*p.DurableOnly {
		t.Fatalf("回执 durable_only = %v，want true", p.DurableOnly)
	}
	if p.Replaced {
		t.Fatalf("首条订阅 replaced 应为 false")
	}
}

// fakeInbound 是测试用的 InboundHandler（记录收到的 kind，按 reply 回调产出应答）。
type fakeInbound struct {
	mu    sync.Mutex
	seen  []string
	reply func(contracts.TransportMessage) (*contracts.TransportMessage, error)
}

func (f *fakeInbound) HandleInbound(_ context.Context, m contracts.TransportMessage) (*contracts.TransportMessage, error) {
	f.mu.Lock()
	f.seen = append(f.seen, m.Kind)
	f.mu.Unlock()
	return f.reply(m)
}

// TestInboundCommandAndApprovalRouting（G1）：command.submit / approval.respond 经
// InboundHandler 分发；command.submit 回 command.accepted（reply_to 指向原消息），
// approval.respond 的 handler 返回 nil = 只收不回（效果经事件流），连接继续可用。
func TestInboundCommandAndApprovalRouting(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	s := transport.NewServer(b)
	fi := &fakeInbound{reply: func(m contracts.TransportMessage) (*contracts.TransportMessage, error) {
		if m.Kind == transport.KindCommandSubmit {
			r, _ := transport.NewMessage(transport.KindCommandAccepted,
				contracts.CommandAccepted{SessionID: core.NewUlid(), Status: "running"})
			return &r, nil
		}
		return nil, nil // approval.respond：受理即可，无回执
	}}
	s.SetInboundHandler(fi)
	srv := newWSServer(t, s)
	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// command.submit → command.accepted（reply_to 指向原 submit）。
	cs, _ := transport.NewMessage(transport.KindCommandSubmit, contracts.CommandSubmit{Prompt: "读一下"})
	if err := c.SendContext(ctx, cs); err != nil {
		t.Fatalf("send command.submit: %v", err)
	}
	rep := recvTimeout(t, c, 5*time.Second)
	if rep.Kind != transport.KindCommandAccepted {
		t.Fatalf("kind = %q，want command.accepted", rep.Kind)
	}
	if rep.ReplyTo == nil || *rep.ReplyTo != cs.ID {
		t.Fatalf("command.accepted reply_to = %v，want %s", rep.ReplyTo, cs.ID)
	}

	// approval.respond（handler 返回 nil）→ 无回执；随后 ping → pong 证明连接仍活着、
	// 且中间没有夹杂别的消息（若 approval.respond 产出了回复，这里收到的就不是 pong）。
	ar, _ := transport.NewMessage(transport.KindApprovalRespond, contracts.ApprovalResponse{
		RequestID: core.NewUlid(), Decision: contracts.ApprovalDecisionAllow,
	})
	if err := c.SendContext(ctx, ar); err != nil {
		t.Fatalf("send approval.respond: %v", err)
	}
	ping, _ := transport.NewMessage(transport.KindPing, nil)
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if got := recvTimeout(t, c, 5*time.Second).Kind; got != transport.KindPong {
		t.Fatalf("approval.respond 后应直接收到 pong（无回执），得到 %q", got)
	}
}

// TestInboundErrorKeepsConnectionAlive（D7）：InboundHandler 返回 error → kind=error
// （reply_to 指向原消息）但**不关连接**。
func TestInboundErrorKeepsConnectionAlive(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	s := transport.NewServer(b)
	s.SetInboundHandler(&fakeInbound{reply: func(contracts.TransportMessage) (*contracts.TransportMessage, error) {
		return nil, errors.New("没有挂起的审批")
	}})
	srv := newWSServer(t, s)
	c := mustDial(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ar, _ := transport.NewMessage(transport.KindApprovalRespond, contracts.ApprovalResponse{RequestID: core.NewUlid()})
	if err := c.SendContext(ctx, ar); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := recvTimeout(t, c, 5*time.Second)
	if m.Kind != transport.KindError {
		t.Fatalf("kind = %q，want error", m.Kind)
	}
	if m.ReplyTo == nil || *m.ReplyTo != ar.ID {
		t.Fatalf("error reply_to = %v，want %s", m.ReplyTo, ar.ID)
	}
	// 连接仍活：ping → pong。
	ping, _ := transport.NewMessage(transport.KindPing, nil)
	if err := c.SendContext(ctx, ping); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if got := recvTimeout(t, c, 5*time.Second).Kind; got != transport.KindPong {
		t.Fatalf("错误后连接应仍可用，得到 %q", got)
	}
}

// TestBroadcastReachesAllConnections（G1）：Broadcast 不经订阅过滤，推到所有活动连接
// （approval.request 就是这样推的——任何窗口的客户端都能应答）。
func TestBroadcastReachesAllConnections(t *testing.T) {
	b, closeHub := newHub(t)
	defer closeHub()
	s := transport.NewServer(b)
	srv := newWSServer(t, s)

	c1 := mustDial(t, srv)
	c2 := mustDial(t, srv)
	// 故意**不订阅**：广播不该依赖订阅。
	time.Sleep(100 * time.Millisecond) // 等两条连接都注册进 conns

	s.Broadcast(transport.KindApprovalRequest, contracts.ApprovalRequest{
		V: 1, ID: core.NewUlid(), TS: "2026-01-01T00:00:00Z",
		SessionID: core.NewUlid(), AgentID: core.NewUlid(),
		Trigger: contracts.ApprovalTriggerPolicyAsk, Summary: "策略拦截：工具 bash 需要人审",
	})
	for i, c := range []*transport.Client{c1, c2} {
		m := recvTimeout(t, c, 5*time.Second)
		if m.Kind != transport.KindApprovalRequest {
			t.Fatalf("连接 %d 收到 kind=%q，want approval.request", i+1, m.Kind)
		}
		var req contracts.ApprovalRequest
		if err := json.Unmarshal(m.Payload, &req); err != nil {
			t.Fatalf("连接 %d approval.request payload: %v", i+1, err)
		}
		if req.Trigger != contracts.ApprovalTriggerPolicyAsk || req.Summary == "" {
			t.Fatalf("连接 %d 审批请求要素不齐: %+v", i+1, req)
		}
	}
}
