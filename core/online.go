package core

import "time"

// DefaultActivityWindow 是"活跃窗口"的缺省值（G-a 在线三态：活跃 active = 本轮在跑
// running ∨ 最后事件 ts 落在本窗口内）。**全仓唯一权威常量**——core/discovery 的表配置、
// core/loop 的 online_table 配置面（activity_window）零值都归一到它，免得两处各写一个
// "15m" 悄悄漂移（四消费方必须同一判定：K14/K15 在线表注入、K11 peers、agentlink
// presence、GET /api/sessions）。
const DefaultActivityWindow = 15 * time.Minute

// WithinActivityWindow 判定"最后事件 ts"是否仍在活跃窗口内（G-a 判定原语，纯函数）。
//
// 语义（钉死，调用方不得各自改口径）：
//
//   - hasTS=false（该 session 还没有任何带 ts 的事件）→ false：没有活动证据就不算活跃
//     （live 但无事件的 session 不进对端可见面——窗口判定的源是事件流，不是进程状态）；
//   - window<=0 → false：**防御分支**——装配层已把 0/负归一为 core.DefaultActivityWindow
//     （discovery.Table.SetActivityWindow / loop.OnlineTableConfig.ActivityWindow），
//     正常装配**不会**传 0进来。**0 不是"关窗"语义**：系统里表达不出"关窗"（"只认
//     running"是 running 短路实现的，见 discovery.activeLocked，与窗口值无关）；
//   - 否则 now.Sub(lastTS) <= window；**未来时间戳按活跃算**（now.Sub 为负，恒 ≤ window
//     ——保守：时钟偏移不该把刚发生的事件判成过期，宁可多活跃不可误杀）。
func WithinActivityWindow(lastTS time.Time, hasTS bool, now time.Time, window time.Duration) bool {
	if !hasTS || window <= 0 {
		return false
	}
	return now.Sub(lastTS) <= window
}
