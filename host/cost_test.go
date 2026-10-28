package host_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// TestTokenCostEndToEnd 钉死 J21 硬验收"成本统计可查"：喂带 usage 的 model.turn
// （脚本 provider 两轮：in=10/out=5 + in=20/out=3）→ hub 查询返回正确聚合。
func TestTokenCostEndToEnd(t *testing.T) {
	target := filepath.Join(t.TempDir(), "cost-target.txt")
	if err := os.WriteFile(target, []byte("hello"), 0o600); err != nil {
		t.Fatalf("写目标: %v", err)
	}
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))

	sid := host.NewSession()
	ctx := context.Background()
	if _, err := h.RunPrompt(ctx, sid, "读一下目标文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// per-session 查询。
	view, ok, err := h.TokenCost(ctx, sid)
	if err != nil || !ok {
		t.Fatalf("TokenCost: ok=%v err=%v", ok, err)
	}
	if view.InputTokens != 30 || view.OutputTokens != 8 {
		t.Fatalf("token 总量错: in=%d out=%d，want in=30 out=8", view.InputTokens, view.OutputTokens)
	}
	if view.TotalTokens != 38 {
		t.Fatalf("total = %d，want 38", view.TotalTokens)
	}
	if view.ModelTurns != 2 {
		t.Fatalf("metered turns = %d，want 2", view.ModelTurns)
	}
	// 按模型分组（hub 的 Model = "scripted"）。
	mu, ok := view.ByModel["scripted"]
	if !ok || mu.InputTokens != 30 || mu.OutputTokens != 8 || mu.Turns != 2 {
		t.Fatalf("按模型分组错: %+v (byModel=%+v)", mu, view.ByModel)
	}

	// 总额查询。
	total, err := h.TokenCostTotal(ctx)
	if err != nil {
		t.Fatalf("TokenCostTotal: %v", err)
	}
	if total.InputTokens != 30 || total.OutputTokens != 8 || total.ModelTurns != 2 {
		t.Fatalf("总额错: %+v", total)
	}
	if total.Sessions != 1 {
		t.Fatalf("sessions = %d，want 1", total.Sessions)
	}
}

// TestTokenCostHTTP 验证成本统计经 HTTP 可查（/api/cost 与 /api/cost/{session_id}）。
func TestTokenCostHTTP(t *testing.T) {
	target := filepath.Join(t.TempDir(), "cost-http.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatalf("写目标: %v", err)
	}
	h := newTestHub(t, withProvider(`{"path":"`+filepath.ToSlash(target)+`"}`))
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	sid := host.NewSession()
	if _, err := h.RunPrompt(context.Background(), sid, "读文件"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}

	// /api/cost/{session_id}
	resp, err := http.Get(srv.URL + "/api/cost/" + string(sid))
	if err != nil {
		t.Fatalf("GET cost: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("per-session cost 状态码 = %d", resp.StatusCode)
	}
	var view host.TokenCostView
	if err := decodeJSON(resp, &view); err != nil {
		t.Fatalf("解 per-session cost: %v", err)
	}
	if view.InputTokens != 30 || view.OutputTokens != 8 {
		t.Fatalf("HTTP per-session cost 错: %+v", view)
	}

	// /api/cost（总额）
	resp2, err := http.Get(srv.URL + "/api/cost")
	if err != nil {
		t.Fatalf("GET total cost: %v", err)
	}
	defer resp2.Body.Close()
	var total host.TokenTotalView
	if err := decodeJSON(resp2, &total); err != nil {
		t.Fatalf("解 total cost: %v", err)
	}
	if total.InputTokens != 30 || total.OutputTokens != 8 {
		t.Fatalf("HTTP total cost 错: %+v", total)
	}

	// 坏 session id → 400。
	respBad, err := http.Get(srv.URL + "/api/cost/not-a-ulid")
	if err != nil {
		t.Fatalf("GET bad cost: %v", err)
	}
	respBad.Body.Close()
	if respBad.StatusCode != http.StatusBadRequest {
		t.Fatalf("坏 session id 应 400，得到 %d", respBad.StatusCode)
	}
}

// TestTokenCostEmpty 验证无 usage 数据时查询返回 ok=false（/api/cost/{id} → 404）。
func TestTokenCostEmpty(t *testing.T) {
	h := newTestHub(t)
	sid := host.NewSession()
	_, ok, err := h.TokenCost(context.Background(), sid)
	if err != nil {
		t.Fatalf("TokenCost: %v", err)
	}
	if ok {
		t.Fatal("没有 usage 数据时应 ok=false")
	}
}

// decodeJSON 是小辅助：解 HTTP 响应体 JSON。
func decodeJSON(resp *http.Response, out any) error {
	return json.NewDecoder(resp.Body).Decode(out)
}
