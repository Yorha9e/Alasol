package tools

// 本文件是 L2 per-session 执行上下文（件 1）的工具侧钉子：路径解析 + Dir 填充。
//
// 口径（三条，见 workspace.go）：
//
//  1. 相对路径 → 锚到会话工作区根；越出根 → **拒**（错误文案点名"相对路径越出工作区根"）；
//  2. 绝对路径 → **原样透传**（这刀不是安全刀，真隔离归 sandbox）；
//  3. 工作区为空 → **零行为变化**（相对路径按进程 cwd；bash 继承进程 cwd）。
//
// 钉子（各自对应一处删改）：
//
//	去掉相对路径锚定        → TestReadWriteRelativePathAnchoredToWorkspace 红；
//	去掉越界判定            → TestReadWriteRejectOutsideWorkspace 红；
//	给绝对路径也加限制      → TestReadWriteAbsolutePathPassthrough 红；
//	工作区为空时也去解析     → TestToolsWithoutWorkspaceUnchanged 红；
//	bash 不填 Dir           → TestBashDirFollowsWorkspace 红；
//	git_status 不用工作区    → TestGitStatusDirFollowsWorkspace 红。

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// withWorkspace 造一个带会话工作区的 ctx（工具侧只认这一个入口）。
func withWorkspace(ws string) context.Context {
	return core.WithExecContext(context.Background(), core.ExecContext{Workspace: ws})
}

// ---------------------------------------------------------------------------
// read / write：相对路径锚定
// ---------------------------------------------------------------------------

// TestReadWriteRelativePathAnchoredToWorkspace 相对路径落在**会话工作区**里
// （真 tmp 目录 + 真本地 FS：看文件到底落在哪儿）。
func TestReadWriteRelativePathAnchoredToWorkspace(t *testing.T) {
	ws := t.TempDir()
	fs := newLocalFS(t)
	ctx := withWorkspace(ws)

	if _, err := NewWrite(fs).Execute(ctx, intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("工作区内容")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := filepath.Join(ws, "out.txt")
	data, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("文件应落在工作区 %s：%v", want, err)
	}
	if string(data) != "工作区内容" {
		t.Errorf("文件内容 = %q", data)
	}

	// read 走同一条解析：相对路径读的是工作区里的那个文件。
	res, err := NewRead(fs).Execute(ctx, intent(toolNameRead, readArgs{Path: "out.txt"}))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := textOf(t, res); got != "工作区内容" {
		t.Errorf("读回 = %q", got)
	}
}

// TestReadWriteSubdirectoryRelativePath 相对路径带子目录：同样锚到工作区下。
func TestReadWriteSubdirectoryRelativePath(t *testing.T) {
	ws := t.TempDir()
	sub := filepath.Join(ws, "pkg")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	fs := newLocalFS(t)
	ctx := withWorkspace(ws)

	if _, err := NewWrite(fs).Execute(ctx, intent(toolNameWrite, writeArgs{Path: filepath.Join("pkg", "a.txt"), Content: strPtr("x")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sub, "a.txt")); err != nil {
		t.Fatalf("文件应落在工作区的子目录下：%v", err)
	}
}

// TestReadWriteRejectOutsideWorkspace 越界（../）→ **拒**：错误文案点名"相对路径越出
// 工作区根"（模型要能据此自我修正，M5），且**不得**在越界处留下文件。
func TestReadWriteRejectOutsideWorkspace(t *testing.T) {
	ws := t.TempDir()
	parent := filepath.Dir(ws)
	fs := newLocalFS(t)
	ctx := withWorkspace(ws)

	// write 越界。
	_, err := NewWrite(fs).Execute(ctx, intent(toolNameWrite, writeArgs{Path: filepath.Join("..", "escape.txt"), Content: strPtr("x")}))
	if err == nil {
		t.Fatal("越界的相对路径应被拒绝")
	}
	if !strings.Contains(err.Error(), "相对路径越出工作区根") {
		t.Errorf("错误文案 = %q，期望含 %q", err, "相对路径越出工作区根")
	}
	if !strings.Contains(err.Error(), toolNameWrite) {
		t.Errorf("错误文案未带工具名：%q", err)
	}
	if _, statErr := os.Stat(filepath.Join(parent, "escape.txt")); statErr == nil {
		t.Error("越界写竟然留下了文件")
	}

	// read 越界：同样拒（读侧也守同一条口径——否则"写不进但读得出"是更怪的组合）。
	_, err = NewRead(fs).Execute(ctx, intent(toolNameRead, readArgs{Path: filepath.Join("..", "escape.txt")}))
	if err == nil {
		t.Fatal("越界的相对路径应被拒绝")
	}
	if !strings.Contains(err.Error(), "相对路径越出工作区根") {
		t.Errorf("错误文案 = %q，期望含 %q", err, "相对路径越出工作区根")
	}
}

// TestReadWriteAbsolutePathPassthrough 绝对路径**原样透传**（不受工作区影响）。
//
// 这一刀不是安全刀：绝对路径不新增限制——真正的隔离归沙箱（内核强制）。
// 这里钉住的是"别顺手把绝对路径也拦了"（那会打断既有用法，且是**假的**安全感）。
func TestReadWriteAbsolutePathPassthrough(t *testing.T) {
	ws := t.TempDir()
	outside := t.TempDir()
	fs := newLocalFS(t)
	ctx := withWorkspace(ws)

	abs := filepath.Join(outside, "abs.txt")
	if _, err := NewWrite(fs).Execute(ctx, intent(toolNameWrite, writeArgs{Path: abs, Content: strPtr("绝对")})); err != nil {
		t.Fatalf("绝对路径应原样透传：%v", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("文件应落在绝对路径指向处：%v", err)
	}
	if string(data) != "绝对" {
		t.Errorf("内容 = %q", data)
	}
	// 工作区里**不该**多出同名文件。
	if _, err := os.Stat(filepath.Join(ws, "abs.txt")); err == nil {
		t.Error("绝对路径不该被搬进工作区")
	}
}

// TestReadWriteResolvedPathIsAbsolute 解析后的路径是**绝对路径**下沉给能力面
// （能力面拿到的不是"out.txt"这种还要自己解释的相对路径）。
func TestReadWriteResolvedPathIsAbsolute(t *testing.T) {
	ws := t.TempDir()
	fake := newFakeFS()
	if _, err := NewWrite(fake).Execute(withWorkspace(ws), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("x")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := filepath.Join(ws, "out.txt")
	if !filepath.IsAbs(want) {
		t.Fatalf("测试前提不成立：%q 不是绝对路径", want)
	}
	// 假 FS 收到的键 = 解析后的绝对路径。
	if got := string(fake.writtenBytes(t, want)); got != "x" {
		t.Errorf("写入内容 = %q", got)
	}
}

// TestToolsWithoutWorkspaceUnchanged 零行为变化酸测：工作区为空 → 路径**原样**下沉
// （相对路径按进程 cwd）。
//
// 两个断言：
//   - 假 FS 看到的路径**逐字不变**（"out.txt" 不是 "D:\\...\\out.txt"）；
//   - 真 FS + 换 cwd：文件落在**进程 cwd**（现状行为）。
func TestToolsWithoutWorkspaceUnchanged(t *testing.T) {
	// ① 路径原样下沉（没注入工作区 = 没解析）。
	fake := newFakeFS()
	if _, err := NewWrite(fake).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("x")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := string(fake.writtenBytes(t, "out.txt")); got != "x" {
		t.Errorf("写入内容 = %q", got)
	}
	// ② 相对路径按进程 cwd（真 FS）。
	cwd := t.TempDir()
	t.Chdir(cwd)
	realFS := newLocalFS(t)
	if _, err := NewWrite(realFS).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "cwd.txt", Content: strPtr("y")})); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cwd, "cwd.txt")); err != nil {
		t.Fatalf("文件应落在进程 cwd %s：%v", cwd, err)
	}
}

// TestWriteReceiptReportsResolvedPath 有工作区 → write 的回执报**解析后的绝对路径**
// （critic P2-6：回执是模型"东西落在哪儿"的唯一线索——报相对路径等于让模型猜）。
func TestWriteReceiptReportsResolvedPath(t *testing.T) {
	ws := t.TempDir()
	realFS := newLocalFS(t)
	res, err := NewWrite(realFS).Execute(withWorkspace(ws), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("内容")}))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	got := textOf(t, res)
	want := filepath.Join(ws, "out.txt")
	if !strings.Contains(got, want) {
		t.Fatalf("回执应含解析后的绝对路径 %q，实际 = %q", want, got)
	}
	// 回执仍不回显内容（纯浪费 token）。
	if strings.Contains(got, "内容") {
		t.Fatalf("回执不该回显内容: %q", got)
	}
}

// TestWriteReceiptWithoutWorkspaceUnchanged 无工作区 → 回执**逐字不变**
// （critic P2-6：零行为变化——不锚定时报原样的相对路径，不多报一个绝对路径）。
func TestWriteReceiptWithoutWorkspaceUnchanged(t *testing.T) {
	fake := newFakeFS()
	res, err := NewWrite(fake).Execute(context.Background(), intent(toolNameWrite, writeArgs{Path: "out.txt", Content: strPtr("123456789")}))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := textOf(t, res); got != "wrote 9 bytes to out.txt" {
		t.Fatalf("回执 = %q，want %q（逐字不变）", got, "wrote 9 bytes to out.txt")
	}
}

// ---------------------------------------------------------------------------
// bash：工作目录
// ---------------------------------------------------------------------------

// TestBashDirFollowsWorkspace bash 的子进程工作目录 = 会话工作区。
func TestBashDirFollowsWorkspace(t *testing.T) {
	ws := t.TempDir()
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0}}
	if _, err := NewBash(fe).Execute(withWorkspace(ws), intent(toolNameBash, bashArgs{Command: "ls"})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := fe.lastSpec().Dir; got != ws {
		t.Errorf("ExecSpec.Dir = %q，期望 %q", got, ws)
	}
}

// TestBashWithoutWorkspaceKeepsProcessCwd 工作区为空 → Dir 空 = 继承进程 cwd（现状）。
func TestBashWithoutWorkspaceKeepsProcessCwd(t *testing.T) {
	fe := &fakeExec{res: core.ExecResult{ExitCode: 0}}
	if _, err := NewBash(fe).Execute(context.Background(), intent(toolNameBash, bashArgs{Command: "ls"})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := fe.lastSpec().Dir; got != "" {
		t.Errorf("ExecSpec.Dir = %q，期望空（继承进程 cwd）", got)
	}
}

// TestBashRealCommandRunsInWorkspace 真机联跑：在工作区里执行"打印当前目录"的命令，
// 断言子进程**真的**在那里（不是只看 ExecSpec 的字段）。
func TestBashRealCommandRunsInWorkspace(t *testing.T) {
	ws := t.TempDir()
	exec := newLocalExec()
	res, err := NewBash(exec).Execute(withWorkspace(ws), intent(toolNameBash, bashArgs{Command: pwdCommandForTest()}))
	if err != nil {
		t.Fatalf("bash: %v", err)
	}
	out := textOf(t, res)
	if !strings.Contains(out, "exit_code: 0") {
		t.Fatalf("命令未成功：\n%s", out)
	}
	got := extractPwd(t, out)
	if !samePath(got, ws) {
		t.Errorf("子进程 cwd = %q，期望 %q", got, ws)
	}
}

// ---------------------------------------------------------------------------
// git_status：探测目录
// ---------------------------------------------------------------------------

// TestGitStatusDirFollowsWorkspace path 空 → 探测目录 = 会话工作区（与 bash 同源）。
func TestGitStatusDirFollowsWorkspace(t *testing.T) {
	var gotDir string
	e := &GitStatusExecutor{
		lookPath: func(string) (string, error) { return "git", nil },
		run: func(_ context.Context, dir string, args ...string) ([]byte, error) {
			gotDir = dir
			switch args[0] {
			case "rev-parse":
				return []byte("main\n"), nil
			case "status":
				return []byte(""), nil
			default:
				return []byte("abc1234 提交 (1 minute ago, Bot)\n"), nil
			}
		},
	}
	ws := t.TempDir()
	if _, err := e.Execute(withWorkspace(ws), intent(toolNameGitStatus, gitStatusArgs{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotDir != ws {
		t.Errorf("git 探测目录 = %q，期望 %q（path 空时用工作区）", gotDir, ws)
	}

	// 显式给了 path → 用它（工作区不夺权）。
	explicit := t.TempDir()
	if _, err := e.Execute(withWorkspace(ws), intent(toolNameGitStatus, gitStatusArgs{Path: explicit})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotDir != explicit {
		t.Errorf("git 探测目录 = %q，期望 %q（显式 path 优先）", gotDir, explicit)
	}
}

// TestGitStatusWithoutWorkspaceKeepsProcessCwd 工作区为空 → 探测目录空 = 进程 cwd（现状）。
func TestGitStatusWithoutWorkspaceKeepsProcessCwd(t *testing.T) {
	var gotDir string
	e := &GitStatusExecutor{
		lookPath: func(string) (string, error) { return "git", nil },
		run: func(_ context.Context, dir string, args ...string) ([]byte, error) {
			gotDir = dir
			switch args[0] {
			case "rev-parse":
				return []byte("main\n"), nil
			case "status":
				return []byte(""), nil
			default:
				return []byte("abc1234 提交 (1 minute ago, Bot)\n"), nil
			}
		},
	}
	if _, err := e.Execute(context.Background(), intent(toolNameGitStatus, gitStatusArgs{})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotDir != "" {
		t.Errorf("git 探测目录 = %q，期望空（进程 cwd）", gotDir)
	}
}

// ---------------------------------------------------------------------------
// 描述文案：描述随行为改（别留谎言）
// ---------------------------------------------------------------------------

// TestToolSchemasDescribeWorkspaceRule 三个工具的描述/Schema 要说清"相对路径按工作区
// 解析、绝对路径原样"——描述是模型唯一的行为依据，留旧话（"路径守卫由策略层负责"）
// 就是让模型按过时规则行事。
func TestToolSchemasDescribeWorkspaceRule(t *testing.T) {
	cases := []struct {
		name string
		tool contracts.Tool
	}{
		{toolNameRead, ReadDescriptor()},
		{toolNameWrite, WriteDescriptor(true)},
		{toolNameGitStatus, GitStatusDescriptor()},
	}
	for _, c := range cases {
		schema := string(c.tool.Schema)
		if strings.Contains(schema, "路径守卫由策略层负责") {
			t.Errorf("%s: Schema 还留着旧话（路径守卫由策略层负责）", c.name)
		}
	}
	// read / write：明确说出"相对路径按会话工作区根解析 + 越界即拒 + 绝对路径原样"。
	for _, c := range []contracts.Tool{ReadDescriptor(), WriteDescriptor(true)} {
		schema := string(c.Schema)
		for _, want := range []string{"相对路径按会话工作区根解析", "越出工作区根即拒", "绝对路径原样"} {
			if !strings.Contains(schema, want) {
				t.Errorf("%s: Schema 缺 %q：%s", c.Name, want, schema)
			}
		}
	}
	// git_status：path 空 = 会话工作区。
	if s := string(GitStatusDescriptor().Schema); !strings.Contains(s, "会话工作区") {
		t.Errorf("git_status 的 Schema 应说明 path 空 = 会话工作区：%s", s)
	}
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

// pwdCommandForTest 返回一条"打印当前目录"的跨平台命令。
//
// Windows 的 bash 工具走 cmd /c（没有 pwd，cd 无参才打印当前目录）；Unix 走 bash -c。
func pwdCommandForTest() string {
	if runtime.GOOS == "windows" {
		return "cd"
	}
	return "pwd"
}

// extractPwd 从工具返回文本里取出命令打印的当前目录。
func extractPwd(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "exit_code:") || strings.HasPrefix(line, "---") {
			continue
		}
		return line
	}
	t.Fatalf("输出里没有当前目录：\n%s", out)
	return ""
}

// samePath 比较两个路径是否指向同一处（清尾分隔符 + 解符号链接 + 大小写不敏感）。
func samePath(a, b string) bool {
	na, nb := resolveForCompare(a), resolveForCompare(b)
	return strings.EqualFold(na, nb)
}

// resolveForCompare 把路径归一（解符号链接，失败则原样）。
func resolveForCompare(p string) string {
	p = strings.TrimSpace(p)
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(p)
}
