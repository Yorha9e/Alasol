package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestResolveModel：模型名解析口径——flag 优先、env 兜底、**无错误默认**（fail-close）。
func TestResolveModel(t *testing.T) {
	t.Setenv("ALASOL_MODEL", "")
	if _, err := resolveModel(""); err == nil {
		t.Fatal("flag 与 env 都空时应报错（不做错误默认）")
	}

	t.Setenv("ALASOL_MODEL", "env-model")
	got, err := resolveModel("")
	if err != nil || got != "env-model" {
		t.Fatalf("env 兜底: got %q err %v, want env-model", got, err)
	}

	got, err = resolveModel("flag-model")
	if err != nil || got != "flag-model" {
		t.Fatalf("flag 优先: got %q err %v, want flag-model", got, err)
	}
}

// ---------------------------------------------------------------------------
// v2-S5 清账⑤：`alasol models` 读 TOML 配置（H15 优先级：显式 flag > env > 文件）
// ---------------------------------------------------------------------------

// modelsTestServer 起一个假的 OpenAI 兼容 /models 端点，返回给定模型 id，
// 并**记下收到的 Authorization 头**（用来验证凭据来源走了 env 还是文件）。
func modelsTestServer(t *testing.T, ids ...string) (baseURL string, gotAuth func() string) {
	t.Helper()
	var mu sync.Mutex
	auth := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		auth = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":[`)
		for i, id := range ids {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"id":%q,"object":"model"}`, id)
		}
		fmt.Fprint(w, `]}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() string {
		mu.Lock()
		defer mu.Unlock()
		return auth
	}
}

// writeConfig 写一份 TOML 配置并返回其路径。
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("写配置: %v", err)
	}
	return p
}

// captureStdoutStderr 捕获一次调用里的 stdout/stderr（models 用 stdout 打表、stderr 打摘要）。
func captureStdoutStderr(t *testing.T, fn func()) (string, string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout, os.Stderr = wOut, wErr
	defer func() { os.Stdout, os.Stderr = oldOut, oldErr }()

	done := make(chan string, 2)
	go func() { b, _ := io.ReadAll(rOut); done <- string(b) }()
	go func() { b, _ := io.ReadAll(rErr); done <- string(b) }()

	fn()
	_ = wOut.Close()
	_ = wErr.Close()
	first, second := <-done, <-done
	// 两个 goroutine 完成顺序不定：按"哪个是 stdout"分辨——stdout 里一定有模型表行。
	if strings.Contains(first, "[online]") || strings.Contains(first, "[builtin]") {
		return first, second
	}
	return second, first
}

// TestModelsCommandReadsTOML：`alasol models` 从 TOML 取 base_url / 凭据（清账⑤硬验收）。
func TestModelsCommandReadsTOML(t *testing.T) {
	base, gotAuth := modelsTestServer(t, "gateway-model-a", "gateway-model-b")
	cfg := writeConfig(t, "base_url = \""+base+"\"\napi_key = \"file-secret-key\"\nmodel = \"cfg-model\"\n")

	// 不传 -base-url、不设 env → 必须走配置文件（此前这个命令完全不读 TOML）。
	t.Setenv("ALASOL_BASE_URL", "")
	t.Setenv("ALASOL_API_KEY_ENV", "")
	code := modelsMain([]string{"-config", cfg})
	if code != 0 {
		t.Fatalf("models -config 退出码 = %d，want 0", code)
	}
	if auth := gotAuth(); auth != "Bearer file-secret-key" {
		t.Fatalf("端点收到的 Authorization = %q，want 文件里的凭据（env 优先但此处未设）", auth)
	}
}

// TestModelsCommandPrecedence：显式 flag > env > 文件（与 run/serve 同一条优先级）。
func TestModelsCommandPrecedence(t *testing.T) {
	fileBase, _ := modelsTestServer(t, "from-file")
	envBase, _ := modelsTestServer(t, "from-env")
	flagBase, flagAuth := modelsTestServer(t, "from-flag")
	cfg := writeConfig(t, "base_url = \""+fileBase+"\"\napi_key_env = \"MY_KEY_ENV\"\n")

	// ① env 覆盖文件。
	t.Setenv("ALASOL_BASE_URL", envBase)
	t.Setenv("MY_KEY_ENV", "")
	code := modelsMain([]string{"-config", cfg})
	if code != 0 {
		t.Fatalf("env 优先级: 退出码 = %d", code)
	}

	// ② 显式 flag 覆盖 env（凭据变量名也显式给）。
	t.Setenv("ALASOL_BASE_URL", envBase)
	t.Setenv("FLAG_KEY_ENV", "flag-secret")
	code = modelsMain([]string{"-config", cfg, "-base-url", flagBase, "-api-key-env", "FLAG_KEY_ENV"})
	if code != 0 {
		t.Fatalf("flag 优先级: 退出码 = %d", code)
	}
	if auth := flagAuth(); auth != "Bearer flag-secret" {
		t.Fatalf("flag 场景端点收到的 Authorization = %q，want flag-secret（凭据变量名经 flag 显式指定）", auth)
	}
}

// TestModelsCommandOfflineStillReportsConfiguredModel：-offline 不碰网络，但仍把
// 配置文件里的 model 名提示出来（用户不必猜"我配的是哪个"）。
func TestModelsCommandOfflineStillReportsConfiguredModel(t *testing.T) {
	cfg := writeConfig(t, "model = \"cfg-model\"\n")
	out, errOut := captureStdoutStderr(t, func() {
		code := modelsMain([]string{"-config", cfg, "-offline"})
		if code != 0 {
			t.Errorf("offline 退出码 = %d，want 0", code)
		}
	})
	if !strings.Contains(out, "[builtin]") {
		t.Fatalf("offline 应呈内置快照: %q", out)
	}
	if !strings.Contains(errOut, "cfg-model") {
		t.Fatalf("应提示配置文件里的 model: %q", errOut)
	}
}

// TestModelsCommandBrokenConfigFailsClosed：配置解析失败 → fail-close 报错（与 run/serve 同口径）。
func TestModelsCommandBrokenConfigFailsClosed(t *testing.T) {
	cfg := writeConfig(t, "this is not = = toml\n")
	if code := modelsMain([]string{"-config", cfg}); code == 0 {
		t.Fatal("损坏的配置应报错退出（fail-close）")
	}
}

// TestRedactEndpointMasksCredentials（F4 回归）：端点回显**不许带出凭据**——
// userinfo（`user:pass@`）与 query（`?api_key=…`）里常藏着密钥，原样打到终端即泄密。
func TestRedactEndpointMasksCredentials(t *testing.T) {
	cases := []struct {
		in     string
		absent []string // 回显里不该出现的片段
		kept   string   // 回显里该保留的片段（端点主体）
	}{
		{
			in:     "https://user:hunter2@api.example.com/v1",
			absent: []string{"user", "hunter2"},
			kept:   "api.example.com",
		},
		{
			in:     "https://api.example.com/v1?api_key=sk-secret&x=1",
			absent: []string{"sk-secret", "api_key"},
			kept:   "api.example.com",
		},
		{
			in:     "https://user:hunter2@api.example.com/v1?token=t0ken#frag",
			absent: []string{"hunter2", "t0ken", "frag"},
			kept:   "api.example.com",
		},
	}
	for _, tc := range cases {
		got := redactEndpoint(tc.in)
		for _, bad := range tc.absent {
			if strings.Contains(got, bad) {
				t.Fatalf("脱敏后不该出现 %q: %s（输入 %q）", bad, got, tc.in)
			}
		}
		if !strings.Contains(got, tc.kept) {
			t.Fatalf("脱敏后该保留端点主体 %q: %s（输入 %q）", tc.kept, got, tc.in)
		}
	}
	// 解析不了的怪形态：保守脱敏（去掉 query + 掩掉 userinfo），不许原样吐出。
	weird := redactEndpoint("not a url?api_key=sk-secret")
	if strings.Contains(weird, "sk-secret") {
		t.Fatalf("怪 URL 也要脱敏: %s", weird)
	}
	if redactEndpoint("") != "" {
		t.Fatal("空端点应回显空串")
	}
}

// TestModelsCommandDoesNotEchoEndpointCredentials（F4 回归，端到端）：`alasol models`
// 的摘要行里不许出现端点 URL 里的凭据。
func TestModelsCommandDoesNotEchoEndpointCredentials(t *testing.T) {
	cfg := writeConfig(t, "base_url = \"https://user:hunter2@api.example.com/v1?api_key=sk-secret\"\n")
	_, errOut := captureStdoutStderr(t, func() {
		code := modelsMain([]string{"-config", cfg, "-offline"})
		if code != 0 {
			t.Errorf("offline 退出码 = %d，want 0", code)
		}
	})
	for _, leak := range []string{"hunter2", "sk-secret"} {
		if strings.Contains(errOut, leak) {
			t.Fatalf("回显里不该出现凭据 %q: %s", leak, errOut)
		}
	}
	if !strings.Contains(errOut, "api.example.com") {
		t.Fatalf("回显里该保留端点主体（用户要知道自己连的是哪台）: %s", errOut)
	}
}
