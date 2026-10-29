package host

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// 本文件把 K9 告警接进**事件流**（v2-S5 清账②）：host.Monitor 的第三个出口。
//
// 为什么补：告警此前只进 slog 日志 + 内存环——hub 一重启什么都没了，"昨天是不是
// runaway 过"无从复盘，UI 也看不到。告警是**事实**（K9：hub 资源超阈值），事实就该进
// 唯一事实源（F5）：durable 事件 `hub.alert`（契约见 contracts/schema/event.json 的
// HubAlertPayload；INVENTORY §2）。
//
// # envelope 归属：hub 伪 session
//
// Event envelope 强制要 session_id / agent_id（ULID），而 hub 级告警不属于任何 agent
// session。取**确定性派生**的一对伪 id（core.ShortHash(规范化后的库路径)）而不是每次随机：
//
//   - 跨重启稳定 → 同一库的告警聚成一条可查的流（随机发号会让每次重启多出一个"新 session"）；
//   - 派生自**规范化后的事件库路径** → 同一库 = 同一 hub 实例（多 hub 各自一个库，
//     G25 session 亲和写）；路径写法不同（相对/绝对/大小写/软链）归一到同一个 id；
//   - 128 bit 派生与真 session ULID 碰撞的概率可忽略（~2^-64 生日界）。
//
// 伪 session **不是 agent session**：它没有 presence / agent / gate，因此
//   - 不进在线表（/api/sessions，host/projection.go 的 rows 按 **hub.* 事件族整族**过滤
//     ——不是"登记某个 id"：派生口径变了、老库里残留的伪 id 也照样挡得住）；
//   - 不进成本统计的 per-session（没有 usage）；
//   - 只在事件流里按 kind=hub.alert 可查（/ws 订阅者同样看得到）。

// hubEventFamily 是 hub 级基础设施事件的 **kind 族前缀**（`hub.alert` 等）。
//
// # 为什么按"族"判，而不是按"某个派生出来的 id"判
//
// 在线表（/api/sessions）是 **agent session 面板**——只承载 hub.* 事件的伪 session
// 不该出现在里面。旧口径登记"某一个派生 id"（skipSession），一旦派生口径变了
// （库路径写法不同、规范化口径升级、旧版本库里的老 id），那个 id 就**漏进**在线表：
// 面板里冒出一条谁也没建过的"会话"。
//
// 按**事件族**判则一劳永逸：凡是只承载 `hub.*` 事件的 session 一律不进在线表，
// 与 id 怎么派生无关（老库里的伪 session 也照样被挡住）。
const hubEventFamily = "hub."

// isHubEventKind 报告这个 kind 是不是 hub 级基础设施事件族的一员。
func isHubEventKind(kind string) bool {
	return strings.HasPrefix(kind, hubEventFamily)
}

// hubAlertSink 实现 host.AlertSink：把一条告警追加成 durable 事件 hub.alert。
type hubAlertSink struct {
	store core.Store
	// sessionID / agentID 是 hub 级事件的伪归属（按库路径确定性派生，见文件头）。
	sessionID contracts.Ulid
	agentID   contracts.Ulid
}

// newHubAlertSink 建告警事件流出流（dbPath = 事件库路径，用于确定性派生伪 id）。
func newHubAlertSink(store core.Store, dbPath string) *hubAlertSink {
	sessionID, agentID := hubIdentityIDs(dbPath)
	return &hubAlertSink{store: store, sessionID: sessionID, agentID: agentID}
}

// hubIdentityIDs 派生 hub 级事件的伪 (session_id, agent_id)。
//
// 两个 id 用不同命名空间前缀派生（"hub:" / "hub-agent:"）→ 保证不相同：
// session 与 agent 在语义上是两个角色，共用一个 id 会让"这是哪个 agent 的告警"变得含糊。
//
// **派生源是规范化后的库路径**（normalizeDBPath）：同一份库可能被写成
// `alasol.db` / `./alasol.db` / `D:\x\alasol.db` / `d:/x/alasol.db`——未规范化时
// 它们派生出**不同的**伪 id：告警流被切成多条（看着像多个 hub），旧口径留下的
// 伪 session 还会漏进在线表。规范化后"同一个库 = 同一个伪 id"才成立。
func hubIdentityIDs(dbPath string) (sessionID, agentID contracts.Ulid) {
	norm := normalizeDBPath(dbPath)
	return contracts.Ulid(core.ShortHash("hub:" + norm)),
		contracts.Ulid(core.ShortHash("hub-agent:" + norm))
}

// normalizeDBPath 规范化事件库路径（伪 id 的派生源）。
//
// 口径（与 runtime/identity 的路径规范化同族，**纯变换 + 尽力解链**）：
//   - 绝对化（相对路径按进程 cwd 解析；"同一个库"不能因写法不同而裂成两个）；
//   - 解符号链接（能取则取：软链/junction 指向同一处 → 同一个 id；取不到不失败，
//     退回绝对路径——库文件还没建出来时正是这种情况）；
//   - 统一分隔符（ToSlash）+ Windows 下小写（NTFS 大小写不敏感）。
//
// **小写那一步服务的是"库还没建"那一刻**（p3-A）：文件**存在**时 EvalSymlinks 已经
// 给出磁盘上的规范大小写（Windows 实测：查 `events.db` 也回 `Events.db`），小写是
// 冗余的；但库**不存在**时 EvalSymlinks 直接失败（拿不到规范形式），归一就只剩
// "Abs + 小写"——没有它，同一个库的两种大小写写法会派生出两个伪 id（告警被切成两条流）。
//
// **不做 IO 之外的判断**：路径不存在不是错误（首次启动时库还没建）。
func normalizeDBPath(p string) string {
	s := strings.TrimSpace(p)
	if s == "" {
		return ""
	}
	if abs, err := filepath.Abs(s); err == nil {
		s = abs
	}
	if resolved, err := filepath.EvalSymlinks(s); err == nil && resolved != "" {
		s = resolved
	}
	s = filepath.ToSlash(filepath.Clean(s))
	if runtime.GOOS == "windows" {
		s = strings.ToLower(s) // Windows 路径大小写不敏感（同一份库的大小写写法要归一）
	}
	return s
}

// RecordAlert 落一条 hub.alert 事件（实现 host.AlertSink）。
//
// payload 只含契约里的四要素（metric/value/threshold/message）——**不含**任何会话内容、
// 路径或凭据（告警是资源结论，不需要业务上下文；H12/H19 脱敏纪律因此天然满足）。
func (s *hubAlertSink) RecordAlert(ctx context.Context, a Alert) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("host: 告警事件流出口未装配（store 为 nil）")
	}
	payload, err := json.Marshal(contracts.HubAlertPayload{
		Metric:    a.Metric,
		Value:     a.Value,
		Threshold: a.Threshold,
		Message:   a.Message,
	})
	if err != nil {
		return fmt.Errorf("host: 编 hub.alert 载荷: %w", err)
	}
	ts := a.At
	if ts.IsZero() {
		ts = time.Now()
	}
	ev := contracts.Event{
		V: 1,
		// **确定性 id**（内容寻址）：同一采样点的同一条告警恒得同一 id →
		// 重试天然幂等（事件库按 Event.ID 去重，G11）——写库失败后重试（monitor 的
		// F1 释放槽位重试）不会落成"两条一样的告警"。
		ID:        alertEventID(a.Metric, a.At),
		Kind:      KindHubAlert,
		TS:        ts.UTC().Format(time.RFC3339Nano),
		SessionID: s.sessionID,
		AgentID:   s.agentID,
		Durable:   true, // 事实事件：入库、占 seq、可回放（F8）
		Payload:   payload,
	}
	if _, err := s.store.Append(ctx, []contracts.Event{ev}); err != nil {
		return fmt.Errorf("host: 追加 hub.alert 事件: %w", err)
	}
	return nil
}

// alertEventID 派生 hub.alert 事件的 id（**内容寻址**：metric + 采样时刻）。
//
// 为什么不用随机 ULID：告警入流失败会**重试**（F1：释放抑制槽位后下个周期重试），
// 重试必须**幂等**——同一采样点的同一条告警只能落**一条**事件。随机 id 会让重试
// 落成两条（同指标、同时刻、两条事实 = 复盘时数出"两条告警"）。
//
// 派生自 (metric, 采样时刻) → 同一采样点恒得同一 id → 事件库的 Event.ID 幂等去重
// （G11）天然生效：重试落库时被去重，不产生第二条。
func alertEventID(metric string, at time.Time) contracts.Ulid {
	return contracts.Ulid(core.ShortHash("hub.alert:" + metric + "@" + at.UTC().Format(time.RFC3339Nano)))
}
