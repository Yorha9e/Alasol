package host_test

// GET /api/profiles（GUI profile 选择器的数据源）的验收面：
//
//	① 空表 → {"profiles":[]}（**不是 null**——前端 forEach 不必判空）；
//	② [[profiles]] 配两个 → 两个都出、**按 name 升序**（确定性）、字段映射对
//	   （配置面 / 执行面分组 + 空组省略）；
//	③ **凭据纪律（H15）**：响应体里只有 api_key_env **变量名**，**绝不含 key 值**。

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// writeProfilesConfig 把一段 TOML 写到临时文件并返回路径（[[profiles]] 装载面用）。
func writeProfilesConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return p
}

// getRaw 发一个 GET 并回**原始响应体**（形状断言用：空数组 / 字段省略 / 不含敏感字样）。
func getRaw(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读 %s 的响应体: %v", url, err)
	}
	return resp.StatusCode, string(raw)
}

// TestProfilesEndpointEmptyTable：没有 profile → 200 + {"profiles":[]}（不是 null）。
func TestProfilesEndpointEmptyTable(t *testing.T) {
	h := newTestHub(t)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	code, body := getRaw(t, srv.URL+"/api/profiles")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（体=%s）", code, body)
	}
	if !strings.Contains(body, `"profiles":[]`) {
		t.Fatalf("空表应回 {\"profiles\":[]}（不是 null）: %s", body)
	}
	var view struct {
		Profiles []map[string]any `json:"profiles"`
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("解 JSON: %v（体=%s）", err, body)
	}
	if view.Profiles == nil {
		t.Fatal("profiles 应是空数组，不是 null")
	}
	if len(view.Profiles) != 0 {
		t.Fatalf("空表应回 0 条，实际 %d 条", len(view.Profiles))
	}
}

// TestProfilesEndpointListsFileProfiles：[[profiles]] 配两张 → 两个都出、按 name 升序、
// 字段映射对（配置面 / 执行面分组；空组省略）。
func TestProfilesEndpointListsFileProfiles(t *testing.T) {
	// 凭据纪律的对照组：环境变量里有值，但 profile 只存**变量名**。
	t.Setenv("PROFILE_KEY_ENV", "super-secret-key-value")

	path := writeProfilesConfig(t, `
model = "m"

[[profiles]]
name = "deep-work"
description = "深度工作模板"
model = "gpt-4o"
reasoning_effort = "high"
cwd = "D:/work"
worktree = "wt-deep"
run_id = "run-9"
remote_host = "build-01"

[profiles.provider]
base_url = "https://api.example.com/v1"
api_key_env = "PROFILE_KEY_ENV"

[profiles.tools]
only = ["read"]
except = ["bash"]

[profiles.context]
kind = "tiered"
window = 1000000
l1_pct = 10
l2_pct = 1
rules_budget = 20000
common_sense_budget = 30000

[[profiles]]
name = "exec-only"
cwd = "D:/other"
`)
	fc, err := host.LoadFileConfig(path)
	if err != nil {
		t.Fatalf("LoadFileConfig: %v", err)
	}
	profiles, err := fc.ProfileConfigs()
	if err != nil {
		t.Fatalf("ProfileConfigs: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("应解析出 2 张 profile，实际 %d 张", len(profiles))
	}

	h := newTestHub(t, func(c *host.Config) { c.Profiles = profiles })
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	code, body := getRaw(t, srv.URL+"/api/profiles")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，want 200（体=%s）", code, body)
	}
	var view host.ProfilesView
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("解 JSON: %v（体=%s）", err, body)
	}
	if len(view.Profiles) != 2 {
		t.Fatalf("应回 2 张 profile，实际 %d 张: %s", len(view.Profiles), body)
	}
	// 排序：按 name 升序（确定性——map 遍历顺序随机，不能直接吐给前端）。
	if view.Profiles[0].Name != "deep-work" || view.Profiles[1].Name != "exec-only" {
		t.Fatalf("应按 name 升序，实际 %q, %q", view.Profiles[0].Name, view.Profiles[1].Name)
	}

	deep := view.Profiles[0]
	if deep.Description != "深度工作模板" {
		t.Errorf("description = %q，want 深度工作模板", deep.Description)
	}
	if deep.Config.Model != "gpt-4o" || deep.Config.ReasoningEffort != "high" {
		t.Errorf("配置面映射不对: %+v", deep.Config)
	}
	if deep.Config.Provider == nil {
		t.Fatal("provider 组应出现（base_url + api_key_env 都配了）")
	}
	if deep.Config.Provider.BaseURL != "https://api.example.com/v1" ||
		deep.Config.Provider.APIKeyEnv != "PROFILE_KEY_ENV" {
		t.Errorf("provider 面映射不对: %+v", *deep.Config.Provider)
	}
	if deep.Config.Tools == nil || len(deep.Config.Tools.Only) != 1 || deep.Config.Tools.Only[0] != "read" ||
		len(deep.Config.Tools.Except) != 1 || deep.Config.Tools.Except[0] != "bash" {
		t.Errorf("工具白名单映射不对: %+v", deep.Config.Tools)
	}
	if deep.Config.Context == nil {
		t.Fatal("context 组应出现（六项都配了）")
	}
	if deep.Config.Context.Kind != "tiered" || deep.Config.Context.Window != 1000000 ||
		deep.Config.Context.L1Pct != 10 || deep.Config.Context.L2Pct != 1 ||
		deep.Config.Context.RulesBudget != 20000 || deep.Config.Context.CommonSenseBudget != 30000 {
		t.Errorf("上下文面映射不对: %+v", *deep.Config.Context)
	}
	if deep.Exec.CWD != "D:/work" || deep.Exec.Worktree != "wt-deep" ||
		deep.Exec.RunID != "run-9" || deep.Exec.RemoteHost != "build-01" {
		t.Errorf("执行面映射不对: %+v", deep.Exec)
	}

	// 只有执行面的 profile：配置面**整组为空**（各项都不出现）。
	exec := view.Profiles[1]
	if exec.Config.Model != "" || exec.Config.ReasoningEffort != "" {
		t.Errorf("exec-only 的配置面应为空: %+v", exec.Config)
	}
	if exec.Exec.CWD != "D:/other" {
		t.Errorf("exec-only 的 cwd = %q，want D:/other", exec.Exec.CWD)
	}

	// **空组在 JSON 里省略**（provider / tools / context 三组都没配 → 不出现）。
	var raw struct {
		Profiles []map[string]json.RawMessage `json:"profiles"`
	}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("解 JSON: %v", err)
	}
	var execCfg map[string]json.RawMessage
	if err := json.Unmarshal(raw.Profiles[1]["config"], &execCfg); err != nil {
		t.Fatalf("解 config 组: %v", err)
	}
	for _, key := range []string{"provider", "tools", "context"} {
		if _, ok := execCfg[key]; ok {
			t.Errorf("没有覆盖的 %q 组应省略（空 = 不覆盖）: %s", key, body)
		}
	}

	// **凭据纪律**：响应体里有变量名，**没有 key 值**。
	if !strings.Contains(body, "PROFILE_KEY_ENV") {
		t.Errorf("响应体应含凭据**变量名**: %s", body)
	}
	if strings.Contains(body, "super-secret-key-value") {
		t.Fatalf("响应体**绝不能**含 key 值（H15）: %s", body)
	}
}
