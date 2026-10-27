package durable

import (
	"context"

	"github.com/Yorha9e/alasol/contracts"
)

// Verifier 核验一个 replay=verify 的 in-flight 操作的**外部状态**（X2 第二类）。
//
// 按 op 携带的 anchors（外部可核验锚点，如 commit sha）查外部事实：
//   - 返回 (true, nil)  = 外部事实表明该操作**已完成**（如目标 commit 已在远端）→ resume 不重放，
//     直接补一条 completed（disposition=verified）；
//   - 返回 (false, nil) = 核验不通过（外部查无此事实）→ 当作未完成，走重放（disposition=replayed）；
//   - 返回 (_, err)     = 核验过程本身出错（查不了）→ fail-safe 不盲目重放，转 manual。
//
// 接口在 core（C2/D15）；基于 anchors 的默认实现（anchor kind=git-commit 用 git 查存在性/祖先性）
// 在 runtime 侧提供并经此接口注入——core 不碰 I/O。
type Verifier interface {
	Verify(ctx context.Context, op contracts.InFlightOp) (bool, error)
}
