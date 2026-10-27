package durable

// 会话级 **resume 认领**（并发互斥，X4）。
//
// 威胁：两个 resume 并发处置同一个 session → 同一个 in-flight 被**重放两次**
//（verify 类工具有副作用：写文件、发请求……）。顺序幂等挡不住这个——两个 resume
// 都在"已处置集合"落盘之前扫到同一个 in-flight。
//
// 认领用 store 的**幂等写**实现（与现有架构最贴：core.Store 是本包唯一的 I/O 面，
// 它的 Append 以 Event.ID 幂等——ON CONFLICT(id) DO NOTHING）：
//
//   - 认领事件的 Event.ID 由 (session, 轮次) **确定性地**导出 → 所有并发者算出同一个
//     id → store 的幂等保证**只有一行**存在（"INSERT ... ON CONFLICT 只成功一次"）；
//   - 认领载荷里带 attempt id → 读回**存下来的**那份，谁的 attempt id 在里面谁就是
//     赢家；其余并发者直接报「已有 resume 在进行」并退出，**不进处置循环**；
//   - 轮次 = 已有认领条数 → 崩溃后的接管（ForceResume）用下一轮，不会撞已存在的行；
//   - 认领的**释放** = 之后有 session.resumed（本次完成时落的那条）→ 顺序跑第二次
//     resume 不会被挡（幂等：已处置的 op 扫不到）。
//
// 崩溃恢复：崩在处置中途 → 认领没有跟着释放 → 下一次 resume 会报「已有 resume 在进行」，
// 需显式 ForceResume 接管（宁可要人拍板，也不自动抢一个可能还在跑的 resume 的活）。

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// KindSessionResumeClaimed 是 resume 认领事件（本包产生；契约形状见 contracts.ResumeClaimedPayload）。
const KindSessionResumeClaimed = "session.resume_claimed"

// ErrResumeInProgress 表示该 session 已有 resume 正在处置 in-flight（并发互斥）。
//
// 调用方可用 errors.Is 判定；默认处置是**退出**（不重放），除非显式 ForceResume 接管。
var ErrResumeInProgress = errors.New("durable: 该 session 已有 resume 在进行")

// claimEvent 是一条认领事件（认领载荷 + 落盘 seq）。
type claimEvent struct {
	seq       int64
	claimedAt string
	payload   contracts.ResumeClaimedPayload
}

// claimResume 认领本 session 的 resume 权（并发互斥）。
//
// 返回本次的 attempt id（认领成功）；拿不到 → ErrResumeInProgress（调用方直接退出，
// 不进处置循环）。force=true 时无视"未释放的旧认领"（崩溃后的接管）。
func claimResume(ctx context.Context, store core.Store, sessionID contracts.Ulid, events []contracts.Event, force bool) (string, error) {
	claims := claimEvents(events)
	round := int64(len(claims))

	// 旧认领还没被释放（之后没有 session.resumed）→ 上一次 resume 可能还在跑。
	if len(claims) > 0 {
		last := claims[len(claims)-1]
		if !claimReleased(events, last) && !force {
			return "", fmt.Errorf("%w（认领于 %s，attempt=%s；若上一次已崩溃，可用 ForceResume 接管）",
				ErrResumeInProgress, last.claimedAt, last.payload.AttemptID)
		}
	}

	attempt := core.NewUlid()
	ev, err := newDurableEvent(sessionID, primaryAgentID(events), KindSessionResumeClaimed, nil, contracts.ResumeClaimedPayload{
		SessionID: sessionID,
		AttemptID: attempt,
		Round:     round,
		ClaimedAt: strPtr(nowRFC3339()),
	})
	if err != nil {
		return "", err
	}
	// 确定性 id：并发者撞同一行 → store 的幂等写兜住竞争（只有一行）。
	ev.ID = deriveClaimULID(sessionID, round)

	stored, err := store.Append(ctx, []contracts.Event{ev})
	if err != nil {
		return "", fmt.Errorf("durable: 写 resume 认领事件: %w", err)
	}
	if len(stored) == 0 {
		return "", errors.New("durable: 写 resume 认领事件后没有拿回事件")
	}
	// 赢家判定：看**存下来的**那份认领（并发时是先到者的）。
	var got contracts.ResumeClaimedPayload
	if err := json.Unmarshal(stored[0].Payload, &got); err != nil {
		return "", fmt.Errorf("durable: 解析 resume 认领事件: %w", err)
	}
	if got.AttemptID != attempt {
		return "", fmt.Errorf("%w（attempt=%s）", ErrResumeInProgress, got.AttemptID)
	}
	return string(attempt), nil
}

// claimEvents 从事件流里取出全部认领事件（按 seq 升序）。
func claimEvents(events []contracts.Event) []claimEvent {
	var out []claimEvent
	for i := range events {
		if events[i].Kind != KindSessionResumeClaimed {
			continue
		}
		var p contracts.ResumeClaimedPayload
		if json.Unmarshal(events[i].Payload, &p) != nil {
			continue // 宽容（D7）：坏载荷跳过
		}
		var seq int64
		if events[i].Seq != nil {
			seq = *events[i].Seq
		}
		claimedAt := ""
		if p.ClaimedAt != nil {
			claimedAt = *p.ClaimedAt
		}
		out = append(out, claimEvent{seq: seq, claimedAt: claimedAt, payload: p})
	}
	return out
}

// claimReleased 报告认领是否已被释放（之后有 session.resumed → 那次 resume 完成了）。
func claimReleased(events []contracts.Event, claim claimEvent) bool {
	for i := range events {
		if events[i].Kind != KindSessionResumed || events[i].Seq == nil {
			continue
		}
		if *events[i].Seq > claim.seq {
			return true
		}
	}
	return false
}

// deriveClaimULID 由 (session, 轮次) 确定性地导出一个 ULID 形态的 id。
//
// 为什么要确定性：认领靠 store 的**幂等写**去重（Event.ID 冲突 → 只成功一次），
// 所以并发者必须算出**同一个** id 才能让 store 兜住竞争。而 Event.id 必须是合法
// ULID（契约 common.json#/$defs/Ulid 的 26 字符 Crockford base32 正则）。
func deriveClaimULID(sessionID contracts.Ulid, round int64) contracts.Ulid {
	sum := sha256.Sum256([]byte("alasol:resume-claim:" + string(sessionID) + ":" + strconv.FormatInt(round, 10)))
	return contracts.Ulid(encodeCrockford26(sum[:16]))
}

// crockfordAlphabet 是 Crockford base32 字符集（S1/S8：排除易混淆的 I/L/O/U）。
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// encodeCrockford26 把 128bit 编成 26 字符 Crockford base32（标准 ULID 编码）。
//
// 与 core 包的同名实现同款（那里未导出）：本包需要**确定性**的 id（认领靠 store 的
// 幂等写去重），core 又不导出编码器，故在本地复刻一份。
//
// 位累积方向：从最低字节开始，**新字节比已累积的位更显著** → 必须左移 nbits 后 OR 进来。
func encodeCrockford26(b []byte) string {
	out := make([]byte, 26)
	var bits uint64
	var nbits uint
	idx := 25
	for i := len(b) - 1; i >= 0; i-- {
		bits |= uint64(b[i]) << nbits
		nbits += 8
		for nbits >= 5 {
			out[idx] = crockfordAlphabet[bits&0x1f]
			idx--
			bits >>= 5
			nbits -= 5
		}
	}
	if nbits > 0 { // 128 = 25×5 + 3 → 最后 3bit 进首字符
		out[idx] = crockfordAlphabet[bits&0x1f]
	}
	return string(out)
}
