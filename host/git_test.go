package host_test

// G17 git 观测面钉子（v4-7 后半）：GET /api/git 对真仓 / 非仓各给一个如实答案。
//
//   - 真仓（现建带一次提交的临时仓库）：repo=true + branch 非空 + dirty=false；
//     写脏一个文件再采 → dirty=true（观测面真的在看工作区，不是写死的常量）；
//   - 非仓（普通临时目录）：**200 + repo=false + error 点名原因**（hub 起在非 git
//     目录是合法状态，观测面要把"这里不是仓库"当成答案回——不是 404 也不是 500）；
//   - ?path= 覆盖采样目录：默认目录非仓、path 指向真仓 → repo=true。
//
// 变异自证：serveGit 恒回 repo=true → 非仓钉子红；恒回 repo=false → 真仓钉子红。

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/host"
)

// gitViewShape 是 GET /api/git 响应的解码形状（与 host.gitView 字段对齐；
// host_test 是外部包，按 JSON 键解码）。
type gitViewShape struct {
	Repo       bool   `json:"repo"`
	Path       string `json:"path"`
	Branch     string `json:"branch,omitempty"`
	Dirty      *bool  `json:"dirty,omitempty"`
	LastCommit string `json:"last_commit,omitempty"`
	Error      string `json:"error,omitempty"`
	SampledAt  string `json:"sampled_at"`
}

// newGitHub 起一个以 cwd 为默认采样目录的 hub（只读观测面测试，不跑 session）。
func newGitHub(t *testing.T, cwd string) *httptest.Server {
	t.Helper()
	h, err := host.New(host.Config{
		DBPath: filepath.Join(t.TempDir(), "gitview.db"),
		Model:  "scripted", System: "gitview", MaxTurns: 1,
		CWD: cwd,
	})
	if err != nil {
		t.Fatalf("host.New: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// TestGitEndpointRealRepo：真仓 → repo=true + branch + dirty 可见（写脏后翻 true）。
func TestGitEndpointRealRepo(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "gitrepo"))
	srv := newGitHub(t, repo)

	var view gitViewShape
	if code := getJSON(t, srv.URL+"/api/git", &view); code != http.StatusOK {
		t.Fatalf("GET /api/git status = %d, want 200", code)
	}
	if !view.Repo {
		t.Fatalf("真仓应 repo=true，得到 %+v", view)
	}
	if view.Branch == "" {
		t.Fatalf("真仓 branch 不应为空: %+v", view)
	}
	if view.Dirty == nil || *view.Dirty {
		t.Fatalf("刚建好的干净仓库应 dirty=false，得到 %+v", view)
	}
	if view.LastCommit == "" {
		t.Fatalf("有提交的仓库 last_commit 不应为空: %+v", view)
	}
	if view.SampledAt == "" {
		t.Fatal("sampled_at 应非空（观测面快照时刻）")
	}
	if view.Path != repo {
		t.Fatalf("path 回显 = %q，want %q", view.Path, repo)
	}

	// 写脏一个文件 → dirty 翻 true（观测面真在看工作区）。
	if err := os.WriteFile(filepath.Join(repo, "scratch.txt"), []byte("dirty"), 0o644); err != nil {
		t.Fatalf("写脏文件: %v", err)
	}
	var dirty gitViewShape
	if code := getJSON(t, srv.URL+"/api/git", &dirty); code != http.StatusOK {
		t.Fatalf("第二次 GET status = %d", code)
	}
	if dirty.Dirty == nil || !*dirty.Dirty {
		t.Fatalf("有未提交改动应 dirty=true，得到 %+v", dirty)
	}
}

// TestGitEndpointNotARepo：非仓目录 → 200 + repo=false + error 点名原因（如实报，不是 404/500）。
func TestGitEndpointNotARepo(t *testing.T) {
	notRepo := plainDir(t, "not-a-repo")
	srv := newGitHub(t, notRepo)

	var view gitViewShape
	code := getJSON(t, srv.URL+"/api/git", &view)
	if code != http.StatusOK {
		t.Fatalf("非仓也应回 200（观测面如实报，不是错误），得到 %d", code)
	}
	if view.Repo {
		t.Fatalf("非仓应 repo=false，得到 %+v", view)
	}
	if view.Error == "" {
		t.Fatalf("非仓 error 应点名原因: %+v", view)
	}
	if !strings.Contains(view.Error, "git") {
		t.Fatalf("error 文案应点名 git: %q", view.Error)
	}
}

// TestGitEndpointPathOverride：?path= 覆盖采样目录——默认目录非仓、path 指向真仓 → repo=true。
func TestGitEndpointPathOverride(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "override-repo"))
	srv := newGitHub(t, plainDir(t, "default-not-repo"))

	var view gitViewShape
	if code := getJSON(t, srv.URL+"/api/git?path="+filepath.ToSlash(repo), &view); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if !view.Repo || view.Branch == "" {
		t.Fatalf("?path= 指向真仓应 repo=true+branch，得到 %+v", view)
	}
	// query 里用 ToSlash 传路径，回显是 URL 形态（正斜杠）。
	if view.Path != filepath.ToSlash(repo) {
		t.Fatalf("path 回显 = %q，want %q", view.Path, filepath.ToSlash(repo))
	}
}
