package loop_test

// G-a（session 在线三态）：online_table 配置面的 activity_window——
//   - View / Update 的时长字符串往返（"15m" / "30m" 形态，time.ParseDuration）；
//   - 非法字符串 / 负时长 → Update 报错（先校验后应用，原配置不动）；
//   - 零值 = core.DefaultActivityWindow（15m，全仓唯一权威常量）；
//   - ActivityWindow() 零值 / nil 归一。
//
// 边界纪律：core/loop 不 import core/discovery；缺省值只从 core 根包取。

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/loop"
)

// strPtr 取字符串指针（activity_window 的 Update 夹具）。
func strPtrAW(s string) *string { return &s }

// TestOnlineTableActivityWindowRoundTrip：View/Update 往返 + 零值默认 15m + JSON 键名。
func TestOnlineTableActivityWindowRoundTrip(t *testing.T) {
	if core.DefaultActivityWindow != 15*time.Minute {
		t.Fatalf("缺省活跃窗口应是 15m，得到 %v", core.DefaultActivityWindow)
	}

	cfg := loop.NewOnlineTableConfig()
	// 零值默认：View 是 "15m" 字符串，ActivityWindow() 归一到权威常量。
	if got := cfg.View().ActivityWindow; got != "15m" {
		t.Fatalf("默认 activity_window = %q，want \"15m\"", got)
	}
	if got := cfg.ActivityWindow(); got != core.DefaultActivityWindow {
		t.Fatalf("ActivityWindow() = %v，want %v", got, core.DefaultActivityWindow)
	}

	// Update("30m") → View / ActivityWindow 同步反映。
	if err := cfg.Update(loop.OnlineTableUpdate{ActivityWindow: strPtrAW("30m")}); err != nil {
		t.Fatalf("合法 activity_window 被拒: %v", err)
	}
	if got := cfg.View().ActivityWindow; got != "30m" {
		t.Fatalf("Update 后 View = %q，want \"30m\"", got)
	}
	if got := cfg.ActivityWindow(); got != 30*time.Minute {
		t.Fatalf("Update 后 ActivityWindow() = %v，want 30m", got)
	}

	// JSON 往返：View 序列化出 activity_window 键（时长字符串）→ 解回 Update → 再应用。
	raw, err := json.Marshal(cfg.View())
	if err != nil {
		t.Fatalf("marshal view: %v", err)
	}
	if !strings.Contains(string(raw), `"activity_window":"30m"`) {
		t.Fatalf("View JSON 应含 activity_window 键: %s", raw)
	}
	var u loop.OnlineTableUpdate
	if err := json.Unmarshal(raw, &u); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if u.ActivityWindow == nil || *u.ActivityWindow != "30m" {
		t.Fatalf("往返后的 activity_window = %+v，want 30m", u.ActivityWindow)
	}
	cfg2 := loop.NewOnlineTableConfig()
	if err := cfg2.Update(u); err != nil {
		t.Fatalf("往返 Update: %v", err)
	}
	if got := cfg2.View().ActivityWindow; got != "30m" {
		t.Fatalf("往返后 View = %q，want \"30m\"", got)
	}

	// "" = 用默认（与数值参数 0 = 用默认同口径）；nil = 不变。
	if err := cfg.Update(loop.OnlineTableUpdate{ActivityWindow: strPtrAW("")}); err != nil {
		t.Fatalf("空串应被接受（= 用默认）: %v", err)
	}
	if got := cfg.ActivityWindow(); got != core.DefaultActivityWindow {
		t.Fatalf("空串后应回默认 %v，得到 %v", core.DefaultActivityWindow, got)
	}
	if err := cfg.Update(loop.OnlineTableUpdate{MaxRows: intPtr(7)}); err != nil {
		t.Fatalf("部分更新: %v", err)
	}
	if got := cfg.View().ActivityWindow; got != "15m" {
		t.Fatalf("nil activity_window 应保持默认，得到 %q", got)
	}
}

// TestOnlineTableActivityWindowInvalid：非法字符串报错 + 零值 / nil 配置归一。
func TestOnlineTableActivityWindowInvalid(t *testing.T) {
	cfg := loop.NewOnlineTableConfig()
	for _, bad := range []string{"bogus", "15", "-5m", "15x", "m"} {
		if err := cfg.Update(loop.OnlineTableUpdate{ActivityWindow: strPtrAW(bad)}); err == nil {
			t.Fatalf("非法 activity_window %q 应被拒", bad)
		}
		// 先校验后应用：原配置一动不动。
		if got := cfg.View().ActivityWindow; got != "15m" {
			t.Fatalf("非法更新后配置被改动: %q", got)
		}
	}

	// 零值配置（不走 New）：ActivityWindow() / View() 都归一到缺省。
	var zero loop.OnlineTableConfig
	if got := zero.ActivityWindow(); got != core.DefaultActivityWindow {
		t.Fatalf("零值配置 ActivityWindow() = %v，want %v", got, core.DefaultActivityWindow)
	}
	if got := zero.View().ActivityWindow; got != "15m" {
		t.Fatalf("零值配置 View = %q，want \"15m\"", got)
	}

	// nil 配置：同样归一，不 panic。
	var nilCfg *loop.OnlineTableConfig
	if got := nilCfg.ActivityWindow(); got != core.DefaultActivityWindow {
		t.Fatalf("nil 配置 ActivityWindow() = %v，want %v", got, core.DefaultActivityWindow)
	}
	if got := nilCfg.View().ActivityWindow; got != "15m" {
		t.Fatalf("nil 配置 View = %q，want \"15m\"", got)
	}
	if err := nilCfg.Update(loop.OnlineTableUpdate{}); err == nil {
		t.Fatal("nil 配置的 Update 应报错")
	}
}
