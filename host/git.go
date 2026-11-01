package host

// G17 git 观测面（v4-7 后半，GUI 反馈轮挂账）：host 侧暴露 `GET /api/git`——
// 对 hub 级默认工作目录（Config.CWD，缺省进程 cwd）做一次 git 状态快照
// （branch / dirty / last_commit），复用 runtime/tools 的 git_status 探测逻辑
// （SnapshotGitStatus，与模型工具**同源**——两面看到的状态恒一致）。
//
// # 口径（诚实边界，逐条对应）
//
//   - **只做 HTTP 面，不进事件流**：git.state 事件走 F7 全链（schema → codegen →
//     INVENTORY → 样本 → TS）成本高，且 git 状态是**进程本地派生观测**（不是事实流
//     里的事实——同一时刻两个 worktree 各有各的 dirty），入 durable 事件反而混淆
//     "事实"与"采样"。拉式快照（请求时现采）就是最小可用口径。
//   - **请求时现采，不做后台周期采样**：一次 git status 是毫秒级只读调用，按需采样
//     即可；周期采样需要缓存失效与后台 goroutine 生命周期，对观测面不值得。
//   - **非仓库如实报 repo=false**（不是 404 也不是 500：hub 起在非 git 目录是合法状态，
//     观测面要把"这里不是仓库"当成答案回）。
//   - 只读：rev-parse / status --porcelain / log -1，不碰工作区。

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/runtime/tools"
)

// gitView 是 GET /api/git 的 JSON 形状（字段口径见 tools.GitStatus）。
type gitView struct {
	// Repo = 采样目录在 git 仓库里（false → Error 点名原因，其余字段缺省）。
	Repo bool `json:"repo"`
	// Path 实际采样的目录（hub 级默认工作目录；回显便于前端对账）。
	Path string `json:"path"`
	// Branch 当前分支名（Repo=true 时非空）。
	Branch string `json:"branch,omitempty"`
	// Dirty 工作区是否有未提交改动（Repo=true 时才有；缺省 = 没探测到）。
	Dirty *bool `json:"dirty,omitempty"`
	// LastCommit 最近一次提交摘要（空仓库 = "（无提交）"）。
	LastCommit string `json:"last_commit,omitempty"`
	// Error 探测失败原因（Repo=false 时非空）。
	Error string `json:"error,omitempty"`
	// SampledAt 采样时刻（RFC3339；观测面是"此刻"的快照，不是事件流事实）。
	SampledAt string `json:"sampled_at"`
}

// serveGit 服务 GET /api/git（G17 git 观测面；只读、HTTP-only、不进事件流）。
//
// 可选 query：`path=<dir>` 覆盖采样目录（默认 hub 级 cwd）——前端有时要看某个
// session 的工作区；path 不做存在性预检，探测失败如实回 repo=false + error。
func (h *Hub) serveGit(w http.ResponseWriter, r *http.Request) {
	dir := h.cwd
	if p := strings.TrimSpace(r.URL.Query().Get("path")); p != "" {
		dir = p
	}
	// 短超时：观测面不能被一个挂死的 git（如等待锁）拖住 HTTP handler。
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	snap := tools.SnapshotGitStatus(ctx, dir)
	writeJSONBody(w, http.StatusOK, gitView{
		Repo:       snap.Repo,
		Path:       dir,
		Branch:     snap.Branch,
		Dirty:      snap.Dirty,
		LastCommit: snap.LastCommit,
		Error:      snap.Error,
		SampledAt:  time.Now().UTC().Format(time.RFC3339),
	})
}
