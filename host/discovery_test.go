package host_test

// host 的 v1-S2 验收测试：K10 发现表 + K12 gate + K11 水平可见性 + hub 宿管接线。
//
// 硬验收（11 §2 v1-S2 行）：
//  ① 两个 session 经 gate 互见主 agent；
//  ② 发现表崩溃重启后由事件流重建。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/identity"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// ---------------------------------------------------------------------------
// 测试辅助（git 仓库：真 repo_id，与 runtime/identity 同约定——git 不可用则跳过）
// ---------------------------------------------------------------------------

func gitAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skipf("git 不可用，跳过：%v", err)
	}
}

// gitExe 返回 git 可执行文件的**绝对路径**（V10：不经 shell、不靠 PATH 的相对解析）。
//
// 找不到返回 ""（调用方据此决定跳过或降级）。用绝对路径调用是为了不让子进程继承/依赖
// 当前工作目录下的同名文件，也免得 shell 参与参数拼接。
func gitExe() string {
	p, err := exec.LookPath("git")
	if err != nil {
		return ""
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	exe := gitExe()
	if exe == "" {
		t.Skipf("git 不可用，跳过")
	}
	cmd := exec.Command(exe, args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v (dir=%s): %v\n%s", args, dir, err, out)
	}
}

// initGitRepo 建一个带一次提交的 git 仓库（.git 为目录），返回其路径。
func initGitRepo(t *testing.T, dir string) string {
	t.Helper()
	gitAvailable(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	runGit(t, dir, "init", "-q")
	runGit(t, dir, "config", "user.email", "test@example.com")
	runGit(t, dir, "config", "user.name", "test")
	runGit(t, dir, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("v1-S2"), 0o644); err != nil {
		t.Fatalf("写 README: %v", err)
	}
	runGit(t, dir, "add", ".")
	runGit(t, dir, "commit", "-q", "-m", "init")
	return dir
}

// plainDir 建一个**普通**临时目录（不是 git 仓库）当工作目录。
//
// 为什么 join 护栏用例要用它：join 的幂等与并发去重跟 repo 语义**无关**（用的是
// `team:...` 这种普通 gate id）。若拿 git 仓库当 cwd，环境里没有 git 时整条用例会被
// Skip——双防线（host 侧 per-session 锁 + 存储层确定性 id 去重）的回归护栏在无 git
// 环境就形同不存在（虚假信心）。**只有真需要 repo 语义的用例才该用 initGitRepo。**
func plainDir(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	return dir
}

// tempRepoOrDir 返回一个**能算出 repo_id** 的临时工作目录：git 可用就现建一个带一次
// 提交的仓库（.git 是目录）；git 不可用就退回普通临时目录（此时没有 repo gate）。
//
// 为什么不用进程 cwd 当默认：hub 级默认工作目录为空时会退回**进程 cwd**，而进程 cwd
// 是否在 git 仓库内取决于「跑测试的地方」——把仓库拷到没有 .git 的目录里、或从别处
// 跑 `go test`，「自动 gate → 多一条 gate.joined → 事件数 8」这类断言就随机红。
// 自建仓库把这件事变成测试自洽的（hub 事件数不再依赖环境）。
func tempRepoOrDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "repo")
	if gitExe() == "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		return dir
	}
	return initGitRepo(t, dir)
}

// wantEventCount 算「跑一轮真回路」应有的事件条数：
// 7 = user.prompt + 两轮各 context.assembled/model.turn + tool.call.intent +
// tool.call.completed；再 +1 = 自动 gate.joined（**仅当**工作目录能算出 repo_id 时
// 才有——K12 的 repo gate 是 fail-close 的，算不出就不加）。
//
// 断言用这个而不是写死 8：断言必须只依赖"测试自己造出来的目录"，不能依赖环境
// （有没有 .git、跑在哪个目录）。
func wantEventCount(t *testing.T, cwd string) int {
	t.Helper()
	n := 7
	if cwd != "" {
		if repoID, err := identity.RepoID(cwd); err == nil && repoID != "" {
			n++
		}
	}
	return n
}

// newRepoHub 装配一个 Hub 并把 hub 级默认工作目录指到 repo（自动 gate 按它算）。
func newRepoHub(t *testing.T, repo string) *host.Hub {
	t.Helper()
	return newTestHub(t, func(c *host.Config) {
		c.CWD = repo
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
}

// runPromptIn 在指定工作目录起一个 session 跑一轮，返回 session id。
func runPromptIn(t *testing.T, h *host.Hub, cwd, prompt string) contracts.Ulid {
	t.Helper()
	sid, err := h.CreateSession(host.SessionSpec{CWD: cwd})
	if err != nil {
		t.Fatalf("CreateSession(%s): %v", cwd, err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, prompt); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	return sid
}

// peerSessions 取可见对端的 session id 集合。
func peerSessions(t *testing.T, h *host.Hub, sid contracts.Ulid) []string {
	t.Helper()
	view, err := h.Peers(context.Background(), sid)
	if err != nil {
		t.Fatalf("Peers(%s): %v", sid, err)
	}
	out := make([]string, 0, len(view.Peers))
	for _, p := range view.Peers {
		out = append(out, p.SessionID)
	}
	return out
}

// ---------------------------------------------------------------------------
// 验收①：两个 session 经 gate 互见主 agent
// ---------------------------------------------------------------------------

// TestTwoSessionsSameRepoSeeEachOtherMainAgent：同 repo（同 git root → 同 repo:<id>）
// 两个 session 各跑一轮 prompt → 经发现表**互见对方的主 agent**（K11① + K12）。
func TestTwoSessionsSameRepoSeeEachOtherMainAgent(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newRepoHub(t, repo)

	a := runPromptIn(t, h, repo, "A 的任务")
	b := runPromptIn(t, h, repo, "B 的任务")

	// 互见（双向）。
	got := peerSessions(t, h, a)
	if len(got) != 1 || got[0] != string(b) {
		t.Fatalf("a 的可见对端 = %v，want [b]", got)
	}
	got = peerSessions(t, h, b)
	if len(got) != 1 || got[0] != string(a) {
		t.Fatalf("b 的可见对端 = %v，want [a]", got)
	}

	// 对端行带的是**主 agent**（不是 subagent；K11 水平可见性的粒度是 session 的主 agent）。
	view, err := h.Peers(context.Background(), a)
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	mainB, ok := h.Discovery().PrimaryAgent(b)
	if !ok {
		t.Fatal("发现表推不出 b 的主 agent")
	}
	if view.Peers[0].MainAgentID != string(mainB) {
		t.Fatalf("对端 main_agent_id = %q，want %q", view.Peers[0].MainAgentID, mainB)
	}
	if !reflect.DeepEqual(view.Peers[0].Gates, []string{"repo:" + repoIDOf(t, repo)}) {
		t.Fatalf("相交 gate = %v，want 该 repo 的自动 gate", view.Peers[0].Gates)
	}
	if view.Gates == nil || len(view.Gates) != 1 || !strings.HasPrefix(view.Gates[0], "repo:") {
		t.Fatalf("本 session 的 gate 成员关系 = %v", view.Gates)
	}
	// 发现表的活跃成员 = 该 gate 下两个存活 session。
	members, err := h.GateMembers(context.Background(), "repo:"+repoIDOf(t, repo))
	if err != nil {
		t.Fatalf("GateMembers: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("gate 活跃成员 = %d，want 2: %+v", len(members), members)
	}
}

// repoIDOf 算 repo 的 repo_id（与 host 用同一个 runtime/identity 口径）。
func repoIDOf(t *testing.T, repo string) string {
	t.Helper()
	id, err := identity.RepoID(repo)
	if err != nil {
		t.Fatalf("RepoID(%s): %v", repo, err)
	}
	return id
}

// TestTwoSessionsDifferentRepoInvisible：异 repo 两个 session 互不可见（负例）。
func TestTwoSessionsDifferentRepoInvisible(t *testing.T) {
	root := t.TempDir()
	repoA := initGitRepo(t, filepath.Join(root, "a"))
	repoB := initGitRepo(t, filepath.Join(root, "b"))
	h := newRepoHub(t, repoA)

	a := runPromptIn(t, h, repoA, "A 的任务")
	b := runPromptIn(t, h, repoB, "B 的任务")

	if got := peerSessions(t, h, a); len(got) != 0 {
		t.Fatalf("异 repo 的 a 不该看得见 b，得到 %+v", got)
	}
	if got := peerSessions(t, h, b); len(got) != 0 {
		t.Fatalf("异 repo 的 b 不该看得见 a，得到 %+v", got)
	}
	// 两边各自仍然是存活的、只是不相交。
	if !h.Discovery().IsLive(a) || !h.Discovery().IsLive(b) {
		t.Fatal("两个 session 都该由本 hub 托管")
	}
	if ga, gb := h.Discovery().GatesOf(a), h.Discovery().GatesOf(b); reflect.DeepEqual(ga, gb) {
		t.Fatalf("两个 repo 的 gate 不该相同: %v vs %v", ga, gb)
	}
}

// TestSameRepoDifferentWorktreesShareGate：同 repo 的两个 worktree（S14：按 git root
// 聚合）拿到同一个 repo:<id> → 互见。这是"多 worktree 并行"场景的可见性基础。
func TestSameRepoDifferentWorktreesShareGate(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	wt := filepath.Join(t.TempDir(), "wt")
	runGit(t, repo, "worktree", "add", "-q", "-b", "feature", wt)
	h := newRepoHub(t, repo)

	a := runPromptIn(t, h, repo, "主工作区")
	b := runPromptIn(t, h, wt, "worktree")

	if got := peerSessions(t, h, a); len(got) != 1 || got[0] != string(b) {
		t.Fatalf("同 repo 的 worktree 该互见，得到 %v", got)
	}
}

// TestNonGitCwdGetsNoRepoGate：非 git 目录 → 算不出 repo_id → 不加 repo gate
// （fail-close：宁不可见也不乱聚），session 仍然正常跑。
func TestNonGitCwdGetsNoRepoGate(t *testing.T) {
	dir := t.TempDir() // 临时目录，不在任何 git 仓库内
	h := newRepoHub(t, dir)
	sid := runPromptIn(t, h, dir, "无 git 的任务")
	if got := h.Discovery().GatesOf(sid); len(got) != 0 {
		t.Fatalf("非 git 目录不该有 gate，得到 %v", got)
	}
	if !h.Discovery().IsLive(sid) {
		t.Fatal("没有 gate 不影响 session 存活")
	}
}

// TestExplicitJoinGateMakesDisjointSessionsVisible：显式 join_gate 令原本不相交的
// 两个 session 互见（K12 的 join_gate）。
func TestExplicitJoinGateMakesDisjointSessionsVisible(t *testing.T) {
	root := t.TempDir()
	repoA := initGitRepo(t, filepath.Join(root, "a"))
	repoB := initGitRepo(t, filepath.Join(root, "b"))
	h := newRepoHub(t, repoA)

	a := runPromptIn(t, h, repoA, "A 的任务")
	b := runPromptIn(t, h, repoB, "B 的任务")
	if got := peerSessions(t, h, a); len(got) != 0 {
		t.Fatalf("join 前不该可见: %+v", got)
	}

	const shared = "team:backend"
	if _, err := h.JoinGate(context.Background(), a, shared); err != nil {
		t.Fatalf("JoinGate(a): %v", err)
	}
	if _, err := h.JoinGate(context.Background(), b, shared); err != nil {
		t.Fatalf("JoinGate(b): %v", err)
	}

	got := peerSessions(t, h, a)
	if len(got) != 1 || got[0] != string(b) {
		t.Fatalf("join 后 a 的可见对端 = %v，want [b]", got)
	}
	if got := peerSessions(t, h, b); len(got) != 1 || got[0] != string(a) {
		t.Fatalf("join 后 b 的可见对端 = %v，want [a]", got)
	}
	// 两个 gate 都在成员关系里（自动的 repo: + 显式的 team:）。
	if gates := h.Discovery().GatesOf(a); len(gates) != 2 {
		t.Fatalf("a 的 gate = %v，want 2 个", gates)
	}
	// 显式 join 的来源记在 join 历史里（可审计）。
	var sawExplicit bool
	for _, j := range h.Discovery().JoinHistory() {
		if j.GateID == shared && j.SessionID == string(a) && j.Source == "explicit" {
			sawExplicit = true
		}
	}
	if !sawExplicit {
		t.Fatalf("join 历史里没有 source=explicit 的记录: %+v", h.Discovery().JoinHistory())
	}
}

// TestJoinGateIdempotentAndValidated：重复 join 不落重复事件；非法 gate_id / 野 session
// 一律拒绝（S9 + 不放野 session 进事件流）。
func TestJoinGateIdempotentAndValidated(t *testing.T) {
	// **不依赖 git**：本用例守的是 join 的幂等与入参校验，与 repo 语义无关。
	dir := plainDir(t, "work")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")

	if _, err := h.JoinGate(context.Background(), a, "team:x"); err != nil {
		t.Fatalf("JoinGate: %v", err)
	}
	joinsAfterFirst := len(h.Discovery().JoinHistory())
	for i := 0; i < 3; i++ {
		if _, err := h.JoinGate(context.Background(), a, "team:x"); err != nil {
			t.Fatalf("重复 JoinGate #%d: %v", i, err)
		}
	}
	if got := len(h.Discovery().JoinHistory()); got != joinsAfterFirst {
		t.Fatalf("重复 join 落了重复事件：%d → %d", joinsAfterFirst, got)
	}

	// 非法 gate_id（S9：不合则拒）。
	for _, bad := range []string{"", "   ", "team:with space", "team:tab\there"} {
		if _, err := h.JoinGate(context.Background(), a, bad); err == nil {
			t.Errorf("gate_id %q 该被拒", bad)
		}
	}
	// 野 session（不是本 hub 托管的）→ 拒，且不进事件流。
	wild := host.NewSession()
	if _, err := h.JoinGate(context.Background(), wild, "team:x"); err == nil {
		t.Fatal("野 session 的 join 该被拒")
	}
	for _, j := range h.Discovery().JoinHistory() {
		if j.SessionID == string(wild) {
			t.Fatal("野 session 不该进 join 历史")
		}
	}
}

// TestPeersExposeOnlyMainAgents：可见对端里只有**主 agent**——本 hub 的 loop 都是单 agent
// （v0 未装配多 agent 环境），所以对端数必须等于相交 gate 的存活 session 数，
// 且每个对端行的 main_agent_id 都确实是该 session 的主 agent。
func TestPeersExposeOnlyMainAgents(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newRepoHub(t, repo)

	sessions := make([]contracts.Ulid, 0, 3)
	for i := 0; i < 3; i++ {
		sessions = append(sessions, runPromptIn(t, h, repo, "并行任务"))
	}
	for _, sid := range sessions {
		view, err := h.Peers(context.Background(), sid)
		if err != nil {
			t.Fatalf("Peers: %v", err)
		}
		if len(view.Peers) != len(sessions)-1 {
			t.Fatalf("session %s 的可见对端 = %d，want %d", sid, len(view.Peers), len(sessions)-1)
		}
		for _, p := range view.Peers {
			main, ok := h.Discovery().PrimaryAgent(contracts.Ulid(p.SessionID))
			if !ok || string(main) != p.MainAgentID {
				t.Fatalf("对端 %s 的 main_agent_id = %q，want %q", p.SessionID, p.MainAgentID, main)
			}
		}
	}

	// 别 session 的 subagent 完全不可见（负例）：往表里喂一条**别的 session** 的
	// agent.spawned（hub 的 loop 都是单 agent——v0 没装配多 agent 环境，故用表的公开
	// 喂入口 Apply 模拟共享流里的子 agent），可见对端必须毫无变化。
	peer := sessions[1]
	sub := core.NewUlid()
	main, _ := h.Discovery().PrimaryAgent(peer)
	seq := int64(9999)
	h.Discovery().Apply(&contracts.Event{
		V: 1, ID: core.NewUlid(), Kind: "agent.spawned", TS: "2026-10-28T00:00:00Z",
		Seq: &seq, SessionID: peer, AgentID: sub, Durable: true,
		Payload: mustJSON(t, contracts.AgentSpawnedPayload{
			ParentAgentID: main, ChildAgentID: sub, AgentType: "scout", Role: "backend",
			Depth: 2, SpawnRequestID: core.NewUlid(),
		}),
	})
	if got := h.Discovery().Agents(peer); len(got) != 2 {
		t.Fatalf("该 session 该有 2 个 agent（主 + 子），得到 %d", len(got))
	}
	view, err := h.Peers(context.Background(), sessions[0])
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(view.Peers) != len(sessions)-1 {
		t.Fatalf("别 session 的 subagent 不该改变对端数量: %+v", view.Peers)
	}
	for _, p := range view.Peers {
		if p.MainAgentID == string(sub) || p.SessionID == string(sub) {
			t.Fatalf("别 session 的 subagent 泄漏进可见对端: %+v", p)
		}
	}
}

// mustJSON 序列化测试载荷（失败即中止）。
func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// ---------------------------------------------------------------------------
// 验收②：发现表崩溃重启后由事件流重建
// ---------------------------------------------------------------------------

// TestDiscoveryRebuildFromEventStreamAfterCrash：真事件流落库 → 关掉 hub（模拟崩溃）
// → 新 hub 从 seq 0 全量重放 → 折叠态与崩溃前一致（含 gate join 历史）；
// 老成员全部失活（G17b）；宿主重新激活（resume）后可见性恢复。
func TestDiscoveryRebuildFromEventStreamAfterCrash(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("rebuild"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	cfg := func() host.Config {
		return host.Config{
			DBPath:   db,
			Model:    "scripted",
			System:   "test-system",
			MaxTurns: 4,
			Allow:    []string{"read"},
			CWD:      repo,
			Provider: &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`},
		}
	}

	h1, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #1: %v", err)
	}
	a := runPromptIn(t, h1, repo, "A 的任务")
	b := runPromptIn(t, h1, repo, "B 的任务")
	// 崩前：互见 + join 历史有两条（两个自动 gate）。
	if got := peerSessions(t, h1, a); len(got) != 1 || got[0] != string(b) {
		t.Fatalf("崩前 a 该看得见 b: %v", got)
	}
	before := h1.Discovery().Snapshot()
	if len(before.Sessions) != 2 || len(before.Joins) != 2 || len(before.Agents) != 2 {
		t.Fatalf("崩前快照形状不对: %+v", before)
	}
	mainA, _ := h1.Discovery().PrimaryAgent(a)
	mainB, _ := h1.Discovery().PrimaryAgent(b)
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// 崩溃重启：同一事件库，新 hub 全量重放。
	h2, err := host.New(cfg())
	if err != nil {
		t.Fatalf("host.New #2: %v", err)
	}
	defer h2.Close()

	after := h2.Discovery().Snapshot()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("重放后的折叠态与崩溃前不一致:\n got %+v\nwant %+v", after, before)
	}
	// 主 agent 身份也重建出来了（事件流口径，与 durable.PrimaryAgentID 一致）。
	if got, _ := h2.Discovery().PrimaryAgent(a); got != mainA {
		t.Fatalf("重建后 a 的主 agent = %q，want %q", got, mainA)
	}
	if got, _ := h2.Discovery().PrimaryAgent(b); got != mainB {
		t.Fatalf("重建后 b 的主 agent = %q，want %q", got, mainB)
	}

	// 老成员全部失活（G17b：session 生命周期 = 进程生命周期）。
	if h2.Discovery().IsLive(a) || h2.Discovery().IsLive(b) {
		t.Fatal("重建后老 session 不该还是存活的")
	}
	// 直接问发现表（不走 Hub.Peers——它要求 session 由本 hub 托管，重建后正好没有）。
	if got := h2.Discovery().VisiblePeers(a); len(got) != 0 {
		t.Fatalf("重建后不该还有可见对端: %+v", got)
	}
	members, err := h2.GateMembers(context.Background(), "repo:"+repoIDOf(t, repo))
	if err != nil {
		t.Fatalf("GateMembers: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("重建后 gate 活跃成员该为空: %+v", members)
	}
	// join 历史仍在（可审计、可重建）。
	if len(h2.Discovery().JoinHistory()) != 2 {
		t.Fatalf("重建后 join 历史 = %d 条，want 2", len(h2.Discovery().JoinHistory()))
	}

	// resume：宿主重新激活（自动 gate 重算 + gate.joined 历史重放）→ 可见性恢复，
	// 且主 agent id 不变（复用原来的，不造幽灵 agent）。
	if _, err := h2.RunPrompt(context.Background(), a, "A 续跑"); err != nil {
		t.Fatalf("resume a: %v", err)
	}
	if _, err := h2.RunPrompt(context.Background(), b, "B 续跑"); err != nil {
		t.Fatalf("resume b: %v", err)
	}
	if got := peerSessions(t, h2, a); len(got) != 1 || got[0] != string(b) {
		t.Fatalf("重新激活后 a 该重新看得见 b: %v", got)
	}
	if got, _ := h2.Discovery().PrimaryAgent(a); got != mainA {
		t.Fatalf("resume 后主 agent 该保持不变: %q → %q", mainA, got)
	}
}

// TestDiscoveryFeedRebuildIsIdempotent：对同一事件库反复"重建"结果一致（重建入口可重入）。
func TestDiscoveryFeedRebuildIsIdempotent(t *testing.T) {
	db := filepath.Join(t.TempDir(), "events.db")
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	newHub := func() *host.Hub {
		return newTestHub(t, func(c *host.Config) {
			c.DBPath = db
			c.CWD = repo
			c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
		})
	}
	h := newHub()
	defer h.Close()

	for i := 0; i < 2; i++ {
		if _, err := h.RunPrompt(context.Background(), runPromptIn(t, h, repo, "任务"), "任务"); err != nil {
			t.Fatalf("RunPrompt: %v", err)
		}
	}
	want := h.Discovery().Snapshot()
	for i := 0; i < 3; i++ {
		// "关掉再开一个同库 hub" = 崩溃重启后从事件流全量重放；反复做结果必须一致。
		h2 := newHub()
		got := h2.Discovery().Snapshot()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("第 %d 次重建不一致:\n got %+v\nwant %+v", i, got, want)
		}
		if err := h2.Close(); err != nil {
			t.Fatalf("close #%d: %v", i, err)
		}
	}
}

// TestSecondHubResumesSessionKeepsMainAgent：跨进程 resume——session 原属另一个 hub
// 进程（共享事件库），新 hub 接管时**复用原来的主 agent id**（先扫事件流再取，
// durable.PrimaryAgentID 口径），并重新激活 gate 成员关系。
func TestSecondHubResumesSessionKeepsMainAgent(t *testing.T) {
	db := filepath.Join(t.TempDir(), "shared.db")
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("resume"), 0o600); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	newHub := func() *host.Hub {
		return newTestHub(t, func(c *host.Config) {
			c.DBPath = db
			c.CWD = repo
			c.Provider = &scriptedProvider{args: `{"path":"` + filepath.ToSlash(target) + `"}`}
		})
	}

	h1 := newHub()
	sid, err := h1.CreateSession(host.SessionSpec{CWD: repo})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h1.RunPrompt(context.Background(), sid, "第一个进程跑"); err != nil {
		t.Fatalf("RunPrompt #1: %v", err)
	}
	main1, ok := h1.Discovery().PrimaryAgent(sid)
	if !ok {
		t.Fatal("h1 推不出主 agent")
	}

	// 第二个 hub 进程接管同一个 session（同一事件库）。
	h2 := newHub()
	if _, err := h2.RunPrompt(context.Background(), sid, "第二个进程续跑"); err != nil {
		t.Fatalf("RunPrompt #2: %v", err)
	}
	main2, ok := h2.Discovery().PrimaryAgent(sid)
	if !ok {
		t.Fatal("h2 推不出主 agent")
	}
	if main2 != main1 {
		t.Fatalf("跨进程 resume 该复用主 agent id: %q → %q", main1, main2)
	}
	// 重新激活 → gate 成员关系恢复（自动 gate 重算）。
	if !h2.Discovery().IsLive(sid) {
		t.Fatal("h2 该把 session 激活")
	}
	if got := h2.Discovery().GatesOf(sid); len(got) != 1 || !strings.HasPrefix(got[0], "repo:") {
		t.Fatalf("接管后 gate 成员关系 = %v", got)
	}
}

// ---------------------------------------------------------------------------
// hub 宿管接线：并发多 session + HTTP 端点
// ---------------------------------------------------------------------------

// TestConcurrentPromptsTwoSessionsOneHub：两个 session 在**同一个 hub** 上并发跑 prompt
// （验收④：Hub 已有多 session loop，这里验证并发下 gate/发现表不串味）。
func TestConcurrentPromptsTwoSessionsOneHub(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = repo
		c.Provider = &concurrencyProvider{args: `{"path":"go.mod"}`}
	})

	const n = 4
	sids := make([]contracts.Ulid, n)
	for i := range sids {
		sid, err := h.CreateSession(host.SessionSpec{CWD: repo})
		if err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		sids[i] = sid
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range sids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = h.RunPrompt(context.Background(), sids[i], "并发任务")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("RunPrompt #%d: %v", i, err)
		}
	}

	// 每个 session 都看得见其余 n-1 个（同 repo 同一 gate）。
	for _, sid := range sids {
		got := peerSessions(t, h, sid)
		if len(got) != n-1 {
			t.Fatalf("session %s 的可见对端 = %d，want %d", sid, len(got), n-1)
		}
		for _, other := range got {
			if other == string(sid) {
				t.Fatal("自己不该出现在自己的对端里")
			}
		}
	}
	// 在线表 n 行，事件数一致（每 session = gate.joined + 7 条 loop 事件）。
	rows, err := h.Sessions(context.Background())
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	if len(rows) != n {
		t.Fatalf("在线表 = %d 行，want %d", len(rows), n)
	}
	for _, r := range rows {
		if r.EventCount != 8 {
			t.Fatalf("session %s event_count = %d，want 8", r.SessionID, r.EventCount)
		}
	}
}

// concurrencyProvider 是并发测试用的脚本 provider：**无共享可变状态**（按请求内容判轮次，
// 不按调用计数器）→ 多 goroutine 并发调用安全（tool_call_id 用原子计数器保证唯一）。
type concurrencyProvider struct {
	args string
	seq  uint64
}

func (p *concurrencyProvider) Run(_ context.Context, req core.ModelRequest, emit func(core.ProviderEvent)) (core.ModelResult, error) {
	hasToolResult := false
	for _, m := range req.Messages {
		if m.Role == "tool" {
			hasToolResult = true
			break
		}
	}
	if !hasToolResult {
		id := contracts.ProviderId(fmt.Sprintf("toolu_CONC_%d", atomic.AddUint64(&p.seq, 1)))
		tc := core.ToolCallReq{ID: id, Name: "read", Args: json.RawMessage(p.args)}
		emit(core.ProviderEvent{Kind: "toolcall.start", ToolCall: &tc})
		emit(core.ProviderEvent{Kind: "done"})
		return core.ModelResult{ToolCalls: []core.ToolCallReq{tc}, Usage: core.Usage{InputTokens: 7, OutputTokens: 3}}, nil
	}
	emit(core.ProviderEvent{Kind: "text.delta", Text: "并发读完了"})
	emit(core.ProviderEvent{Kind: "done"})
	return core.ModelResult{Text: "并发读完了", Usage: core.Usage{InputTokens: 9, OutputTokens: 4}}, nil
}

// TestPeersAndGateJoinHTTPEndpoints：GET /api/peers 与 POST /api/gates/join 的
// 状态码与载荷形状（Web 观察面）。
func TestPeersAndGateJoinHTTPEndpoints(t *testing.T) {
	root := t.TempDir()
	repoA := initGitRepo(t, filepath.Join(root, "a"))
	repoB := initGitRepo(t, filepath.Join(root, "b"))
	h := newRepoHub(t, repoA)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	a := runPromptIn(t, h, repoA, "A 的任务")
	b := runPromptIn(t, h, repoB, "B 的任务")

	// ① 异 repo → 空对端（200 + 空数组，不是 null）。
	var view host.PeersView
	if code := getJSON(t, srv.URL+"/api/peers?session_id="+string(a), &view); code != http.StatusOK {
		t.Fatalf("peers 状态码 = %d", code)
	}
	if view.SessionID != string(a) || len(view.Peers) != 0 || view.Peers == nil {
		t.Fatalf("peers 视图不符: %+v", view)
	}
	if len(view.Gates) != 1 || !strings.HasPrefix(view.Gates[0], "repo:") {
		t.Fatalf("gates = %v", view.Gates)
	}

	// ② 非法 session_id → 400。
	var te contracts.TransportError
	if code := getJSON(t, srv.URL+"/api/peers?session_id=not-a-ulid", &te); code != http.StatusBadRequest {
		t.Fatalf("非法 session_id 状态码 = %d", code)
	}
	// ③ 野 session（本 hub 不托管）→ 404。
	if code := getJSON(t, srv.URL+"/api/peers?session_id="+string(host.NewSession()), &te); code != http.StatusNotFound {
		t.Fatalf("野 session 状态码 = %d，want 404", code)
	}
	if te.Code != "unknown_session" {
		t.Fatalf("错误码 = %q", te.Code)
	}

	// ④ POST /api/gates/join：非法体 / 非法 gate_id → 400。
	for _, body := range []string{`not json`, `{"session_id":"x","gate_id":"g"}`, `{"session_id":"` + string(a) + `","gate_id":"bad id"}`} {
		resp, err := http.Post(srv.URL+"/api/gates/join", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q 状态码 = %d，want 400", body, resp.StatusCode)
		}
	}
	// ⑤ 野 session 的 join → 404（且不进事件流）。
	resp, err := http.Post(srv.URL+"/api/gates/join", "application/json",
		strings.NewReader(`{"session_id":"`+string(host.NewSession())+`","gate_id":"team:x"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("野 session join 状态码 = %d，want 404", resp.StatusCode)
	}

	// ⑥ 正常 join：两个 session 进同一个显式 gate → 立即可见。
	postJoin := func(sid contracts.Ulid, gate string) host.PeersView {
		t.Helper()
		resp, err := http.Post(srv.URL+"/api/gates/join", "application/json",
			strings.NewReader(`{"session_id":"`+string(sid)+`","gate_id":"`+gate+`"}`))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("join %s 状态码 = %d", sid, resp.StatusCode)
		}
		var v host.PeersView
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			t.Fatalf("解 join 响应: %v", err)
		}
		return v
	}
	v1 := postJoin(a, "team:x")
	if len(v1.Peers) != 0 {
		t.Fatalf("只有一个 session 在 gate 里时不该有对端: %+v", v1.Peers)
	}
	v2 := postJoin(b, "team:x")
	if len(v2.Peers) != 1 || v2.Peers[0].SessionID != string(a) {
		t.Fatalf("b join 后该看得见 a: %+v", v2.Peers)
	}
	if !reflect.DeepEqual(v2.Peers[0].Gates, []string{"team:x"}) {
		t.Fatalf("相交 gate = %v，want [team:x]", v2.Peers[0].Gates)
	}
	// ⑦ 重复 join 幂等（200，且不新增 join 事件）。
	joins := len(h.Discovery().JoinHistory())
	if v := postJoin(b, "team:x"); len(v.Peers) != 1 {
		t.Fatalf("重复 join 后对端该不变: %+v", v.Peers)
	}
	if got := len(h.Discovery().JoinHistory()); got != joins {
		t.Fatalf("重复 join 落了重复事件: %d → %d", joins, got)
	}
	// ⑧ 方法路由：join 只认 POST。GET 落到 /api/ 回退面 → 405（带 Allow），
	// 不再掉进静态通配报"没有这个静态资源"（G7）——关键是**不会**触发 join 处理器（不加 gate）。
	if resp, err := http.Get(srv.URL + "/api/gates/join"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("GET /api/gates/join 状态码 = %d，want 405（路径认得、方法不符，G7）", resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != "POST" {
			t.Fatalf("405 应带 Allow: POST，得到 %q", allow)
		}
	}
	// 反向：GET-only 端点用 POST → 405（/api/ 回退面同样处理）。
	if resp, err := http.Post(srv.URL+"/api/sessions", "application/json", nil); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST /api/sessions 状态码 = %d，want 405", resp.StatusCode)
		}
	}
	// 未知 /api 路径 → 404，且错误体说"没有这个 API 端点"（不是"静态资源"，G7）。
	if resp, err := http.Get(srv.URL + "/api/nope"); err == nil {
		var te contracts.TransportError
		_ = json.NewDecoder(resp.Body).Decode(&te)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound || strings.Contains(te.Message, "静态资源") {
			t.Fatalf("GET /api/nope 状态码 = %d message=%q，want 404 且不提静态资源", resp.StatusCode, te.Message)
		}
	}
	// ⑧' **带路径参数**的端点：方法不符也要 405（不是 404）——路由表存的是**模板**
	// （/api/sessions/{session_id}/prompt），拿字面路径查表永远查不到（那会让客户端
	// 以为"端点不存在"，而不是"方法用错了"）。
	if resp, err := http.Get(srv.URL + "/api/sessions/" + string(host.NewSession()) + "/prompt"); err == nil {
		var te contracts.TransportError
		_ = json.NewDecoder(resp.Body).Decode(&te)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("GET /api/sessions/{id}/prompt 状态码 = %d，want 405（路径认得、方法不符）", resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != "POST" {
			t.Fatalf("405 应带 Allow: POST，得到 %q", allow)
		}
		if te.Code != "method_not_allowed" {
			t.Fatalf("405 错误码 = %q，want method_not_allowed", te.Code)
		}
	}
	// 另一条带参数的端点（DELETE 专用）：GET → 405，Allow: DELETE。
	if resp, err := http.Get(srv.URL + "/api/worktrees/some-name"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("GET /api/worktrees/{name} 状态码 = %d，want 405", resp.StatusCode)
		}
		if allow := resp.Header.Get("Allow"); allow != "DELETE" {
			t.Fatalf("405 应带 Allow: DELETE，得到 %q", allow)
		}
	}
}

// TestCreateSessionValidation：CreateSession 的入参校验（S9：不合则拒）。
func TestCreateSessionValidation(t *testing.T) {
	repo := initGitRepo(t, filepath.Join(t.TempDir(), "repo"))
	h := newRepoHub(t, repo)

	if _, err := h.CreateSession(host.SessionSpec{CWD: filepath.Join(t.TempDir(), "nope")}); err == nil {
		t.Error("不存在的工作目录该被拒")
	}
	if _, err := h.CreateSession(host.SessionSpec{RunID: "bad run id"}); err == nil {
		t.Error("含空白的 run_id 该被拒")
	}
	if _, err := h.CreateSession(host.SessionSpec{RunID: strings.Repeat("x", 200)}); err == nil {
		t.Error("过长的 run_id 该被拒")
	}
	// 合法：带 run_id → 自动加入 workflow gate。
	sid, err := h.CreateSession(host.SessionSpec{CWD: repo, RunID: "run-42"})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := h.RunPrompt(context.Background(), sid, "带 run_id 的任务"); err != nil {
		t.Fatalf("RunPrompt: %v", err)
	}
	gates := h.Discovery().GatesOf(sid)
	if len(gates) != 2 {
		t.Fatalf("gates = %v，want repo: + workflow:", gates)
	}
	var hasWorkflow bool
	for _, g := range gates {
		if g == "workflow:run-42" {
			hasWorkflow = true
		}
	}
	if !hasWorkflow {
		t.Fatalf("缺 workflow:run-42: %v", gates)
	}
}

// TestConcurrentJoinGateFallsToSingleEvent：并发 join 同一 (session, gate) → **事件流**
// 恰 1 条 gate.joined。
//
// 「判重 + 发布」之间不是原子的：并发请求都过判重 → 各落一条重复事件（文档宣称的
// 幂等只对**顺序**调用成立）。修法：① host 侧把「判重 + 发布」收进一把 per-session
// 锁；② gate.joined 的 Event.ID 由 (session, gate, source) 确定性导出 → store 的
// ON CONFLICT(id) DO NOTHING 在存储层兜底（与 X5 spawn_request_id 同款手法）。
//
// 断言看**事件流**（不是发现表折出的 join 历史——后者是按 gate 建索引的 map，
// 重复事件会互相覆盖，看不出来）。
func TestConcurrentJoinGateFallsToSingleEvent(t *testing.T) {
	// **不依赖 git**：本用例守的是 join 的双防线（判重+发布的原子性、确定性 id 去重），
	// 用的是 `team:...` 普通 gate id，跟 repo 语义无关。拿 git 仓库当 cwd 的话，
	// 环境里没有 git 时整条用例被 Skip → 护栏形同不存在（虚假信心）。
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")

	// 多轮 × 多并发：判重与发布之间的窗口很窄（判重过 → 发布前被抢占），
	// 单轮不一定撞上；多轮把"撞上"的概率堆起来（竞态回归测试：修复前会红）。
	const (
		rounds = 10
		n      = 16
	)
	for round := 0; round < rounds; round++ {
		gateID := fmt.Sprintf("team:race-%d", round)
		var wg sync.WaitGroup
		errs := make(chan error, n)
		start := make(chan struct{}) // 起跑闸：让 N 个 join 真正同时进入判重窗口
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := h.JoinGate(context.Background(), a, gateID); err != nil {
					errs <- err
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("并发 JoinGate: %v", err)
		}
	}

	// 事件流里：每个 gate 恰 1 条 gate.joined（并发 join 只落一条）。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	evs, err := st.ReadFrom(context.Background(), &a, 1, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	perGate := map[string]int{}
	for _, ev := range evs {
		if ev.Kind != "gate.joined" {
			continue
		}
		// S1：Event.ID 必须是 26 字符 Crockford ULID 形态（确定性导出的 id 也不能
		// 破契约——契约 schema 的 id 正则是 ^[0-9A-HJKMNP-TV-Z]{26}$）。
		if !core.ValidUlid(ev.ID) {
			t.Fatalf("gate.joined 的 event id = %q，不是合法 ULID", ev.ID)
		}
		var p contracts.GateJoinedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 gate.joined payload: %v", err)
		}
		perGate[p.GateID]++
	}
	for round := 0; round < rounds; round++ {
		gateID := fmt.Sprintf("team:race-%d", round)
		if n := perGate[gateID]; n != 1 {
			t.Fatalf("gate %s 落了 %d 条 gate.joined（want 1）", gateID, n)
		}
	}
}

// TestJoinGateDedupSurvivesDiscoveryTableLoss：同一个 (session, gate) 的 join 事件，
// **即使发现表丢了**也只落一条（存储层不变量）。
//
// 场景：发现表是**可重建的派生视图**（崩溃后由事件流重放），它一丢，host 侧的
// 「判重（InGate）」就判不出来 → 会再发一次 join。host 的 per-session 锁挡不住这种
// 情况（它只保证"并发时不多发"，不保证"重复发时不多落"）。真正兜底的是 gate.joined
// 的**确定性 Event.ID** + store 的 ON CONFLICT(id) DO NOTHING：同一 (session, gate,
// source) 恒是同一个 id → 第二次发布在存储层被吃掉。
//
// 这也是**跨进程**成立的原因：两个 hub 各发一次，落进同一个库也只有一条。
func TestJoinGateDedupSurvivesDiscoveryTableLoss(t *testing.T) {
	// **不依赖 git**：同上——守的是存储层去重不变量，与 repo 语义无关。
	dir := plainDir(t, "work")
	dbPath := filepath.Join(t.TempDir(), "events.db")
	h := newTestHub(t, func(c *host.Config) {
		c.CWD = dir
		c.DBPath = dbPath
		c.Provider = &scriptedProvider{args: `{"path":"go.mod"}`}
	})
	a := runPromptIn(t, h, dir, "A 的任务")

	const gateID = "team:dedup"
	if _, err := h.JoinGate(context.Background(), a, gateID); err != nil {
		t.Fatalf("JoinGate #1: %v", err)
	}
	// 模拟"发现表丢了"（可重建视图被清空）：判重必然落空 → host 会再发一次。
	h.Discovery().Reset()
	if h.Discovery().InGate(a, gateID) {
		t.Fatalf("Reset 后 InGate 仍为 true，场景没造出来")
	}
	if _, err := h.JoinGate(context.Background(), a, gateID); err != nil {
		t.Fatalf("JoinGate #2: %v", err)
	}

	// 事件流里：仍只有 1 条 gate.joined（第二次被 store 的 ON CONFLICT 去重吃掉）。
	st, err := sqlite.Open(dbPath, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	evs, err := st.ReadFrom(context.Background(), &a, 1, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	n := 0
	for _, ev := range evs {
		if ev.Kind != "gate.joined" {
			continue
		}
		var p contracts.GateJoinedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			t.Fatalf("解 gate.joined payload: %v", err)
		}
		if p.GateID == gateID {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("gate %s 落了 %d 条 gate.joined（want 1：确定性 id 应让 store 去重）", gateID, n)
	}
}
