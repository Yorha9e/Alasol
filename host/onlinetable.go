package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/discovery"
	"github.com/Yorha9e/alasol/core/loop"
)

// 本文件是 hub 与在线表注入（v1-S4：K14/K15）之间的装配层：**数据源**（发现表 → loop 视图）
// + **配置面**（hub API + HTTP 端点，运行期可调）。core/loop 是在线表的渲染与配置持有者，
// 本包只做两件事——把 core/discovery 的可见面折成 loop.OnlineTableSource，以及绑 HTTP。
//
// 与 host/discovery.go 的分工：那个文件管"事件流 → 发现表"的扫描与 gate；本文件管
// "发现表 → 注入给主 agent 的在线表"。K11 的水平可见性过滤**不在这里重写**——直接复用
// discovery.VisiblePeers（gate 相交 ∩ **活跃**（G-a：running ∨ 窗口内）∩ 只给对端主 agent）。

// discoveryOnlineSource 把发现表折成 loop.OnlineTableSource（self + gate 相交的对端主 agent）。
type discoveryOnlineSource struct {
	table *discovery.Table
	feed  *discoveryFeed
}

// OnlineRows 返回 sessionID 的在线表行（K15/K11：self 行 + gate 相交的对端**主 agent**）。
//
// 先扫平发现表：在线表秒级变动，gate 成员 / 对端要反映最新事件（与 Peers/Presence 同口径）。
// 扫失败不致命——用当前表渲染（下次轮次会重试），只记一条告警。
//
//	self 行     = 本 session 的主 agent（恒在表首；主 agent 未知时跳过，不塞空壳行）；
//	             **恒在**：self 不做活跃过滤（自己永远在自己的在线表里，"我在不在线"由
//	             调用方自己的状态回答——G-a 只滤对端）；
//	对端行     = VisiblePeers(sessionID)——gate 相交 ∩ 活跃 ∩ 只含对端主 agent；
//	             别 session 的 subagent **完全不出现**（VisiblePeers 的既有语义，K11①）。
//	             活跃过滤是件 1 落在 core/discovery.visiblePeersLocked 里的（G-a），
//	             本层不重复判定（一处判定，四消费方同刀）。
func (s *discoveryOnlineSource) OnlineRows(ctx context.Context, sessionID contracts.Ulid) []loop.OnlineTableRow {
	if s == nil || s.table == nil {
		return nil
	}
	if s.feed != nil {
		if _, err := s.feed.catchUp(ctx); err != nil {
			hubLog.Warn("host: 在线表扫事件流失败（用当前发现表渲染，下次轮次重试）",
				"session_id", string(sessionID), "err", err.Error())
		}
	}
	out := make([]loop.OnlineTableRow, 0, 4)
	if self, ok := s.table.Session(sessionID); ok && self.MainAgentID != "" {
		out = append(out, loop.OnlineTableRow{
			Self:        true,
			SessionID:   self.SessionID,
			MainAgentID: self.MainAgentID,
			Gates:       self.Gates,
			LastKind:    self.LastKind,
			LastSeq:     self.LastSeq,
			UpdatedAt:   self.UpdatedAt,
		})
	}
	for _, p := range s.table.VisiblePeers(sessionID) {
		// 剔掉 /btw 旁支 fork（v3-S2 K29）：旁支不是一等会话——注入给主 agent 的
		// 在线表与 presence 面必须同一个口径（core/discovery 不改，过滤在 host 层）。
		if s.feed != nil && s.feed.isFork(contracts.Ulid(p.SessionID)) {
			continue
		}
		out = append(out, loop.OnlineTableRow{
			SessionID:   p.SessionID,
			MainAgentID: p.MainAgentID,
			Gates:       p.Gates, // 与查询方相交的 gate（K11：不泄漏对端全部 gate）
			LastKind:    p.LastKind,
			LastSeq:     p.LastSeq,
			UpdatedAt:   p.UpdatedAt,
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// 配置面（K15：hub 起动时可配 + 运行期动态管理，不重启）
// ---------------------------------------------------------------------------

// OnlineTableConfig 返回 hub 共享的在线表活配置（loop 每轮读它；HTTP 端点调它调整）。
//
// 返回的是**同一个指针**：运行期改它 → 所有 loop 下一轮 request 时读 View() 立即生效。
func (h *Hub) OnlineTableConfig() *loop.OnlineTableConfig {
	if h == nil {
		return nil
	}
	return h.onlineConfig
}

// OnlineTableView 读当前生效的在线表配置。
func (h *Hub) OnlineTableView() loop.OnlineTableView {
	if h == nil {
		return loop.OnlineTableView{}
	}
	return h.onlineConfig.View()
}

// UpdateOnlineTable 运行期调整在线表配置（K15：不重启，下一轮生效）。校验失败 → 配置不动 + error。
func (h *Hub) UpdateOnlineTable(u loop.OnlineTableUpdate) (loop.OnlineTableView, error) {
	if h == nil {
		return loop.OnlineTableView{}, errors.New("host: Hub 为 nil")
	}
	if err := h.onlineConfig.Update(u); err != nil {
		return loop.OnlineTableView{}, err
	}
	// G-a：activity_window 改了要同步进发现表（唯一旋钮，见 syncActivityWindow）——
	// 运行期把窗口调到极小，下一拍 GET /api/sessions / peers / presence 就该判超窗。
	h.syncActivityWindow()
	return h.onlineConfig.View(), nil
}

// syncActivityWindow 把在线表配置里的活跃窗口同步进发现表（G-a 单源同步）。
//
// 为什么必须有一个显式同步动作：在线表注入（K14/K15，loop 每轮读 onlineConfig）与
// K11 peers / agentlink presence / GET /api/sessions 的活跃判定（发现表 activeLocked）
// 是**四个消费方共用同一个 N**——窗口值只活在配置面里，发现表不被告知就会出现
// "注入面改了、判定面没改"（同一个 N 两个值 = G-a 口径漂移）。构造路径（host.New）与
// 运行期路径（UpdateOnlineTable ← POST /api/online-table/config）都调本方法，免得两处
// 各写一行后漂移。
func (h *Hub) syncActivityWindow() {
	if h == nil || h.disc == nil || h.onlineConfig == nil {
		return
	}
	// ActivityWindow() 已把零值归一到 core.DefaultActivityWindow；SetActivityWindow 对
	// d<=0 也回缺省——双保险，缺省值的唯一权威仍是 core.DefaultActivityWindow。
	h.disc.SetActivityWindow(h.onlineConfig.ActivityWindow())
}

// maxOnlineTableBody 限制 POST /api/online-table/config 请求体大小（loopback 上也别裸奔）。
const maxOnlineTableBody = 4 << 10

// serveOnlineTableConfig 服务 GET/POST /api/online-table/config（K15 运行期动态管理）。
//
//	GET  → 当前生效配置（OnlineTableView JSON）；
//	POST → 部分更新（{max_rows,line_token_budget,level,window_lines,activity_window}，
//	       缺省字段不变），200 回更新后的配置；400 体不是 JSON / 参数非法（level 不在
//	       summary|n|full、activity_window 不是非负时长字符串等）。
//	       activity_window 是 G-a 活跃窗口的**运行期旋钮**：更新成功即同步进发现表
//	       （UpdateOnlineTable → syncActivityWindow），在线表注入与判定面同时生效。
func (h *Hub) serveOnlineTableConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSONBody(w, http.StatusOK, h.OnlineTableView())
	case http.MethodPost:
		var u loop.OnlineTableUpdate
		if err := json.NewDecoder(io.LimitReader(r.Body, maxOnlineTableBody)).Decode(&u); err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_body", "请求体不是合法 JSON 对象: "+err.Error())
			return
		}
		v, err := h.UpdateOnlineTable(u)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_online_table_config", err.Error())
			return
		}
		writeJSONBody(w, http.StatusOK, v)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "只支持 GET / POST")
	}
}
