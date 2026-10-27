package loop

import (
	"context"
	"os"
	"strings"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/policy"
)

// 执行 env 的组装（H21：v0 只做 session + 单次调用两级作用域；H17：凭据按需注入；
// H12/H19：值只经 ctx 传给执行器，永不进事件/日志）。
//
//	执行 env = ScrubEnv(进程env ∪ SessionEnv)      ← 基础层：凭据类键与凭据形态值一律剥离
//	         ∪ { RequiredEnv 键 : 进程 env 取值 }   ← 单次调用层：每个键先过策略裁决
//
// 注意顺序：显式注入发生在 ScrubEnv **之后**——否则刚获批的凭据会被脱敏规则剥掉
// （H16/H18 的剥离只针对"继承来的环境噪声"，不针对"本次获批的显式注入"）。

// buildExecEnv 组装一次工具调用的执行 env，并返回**本次注入的秘密值**（脱敏用）。
//
// RequiredEnv 里每个键都当能力过同一套策略求值（E3）：Query{Tool: "env:"+key}。
// allow 才从进程 env 取值注入；deny / ask 未放行 → 该键不出现（fail-close）。
// 键在进程 env 里不存在时无可注入值，静默跳过（不注入空值占位）。
//
// 返回的 secrets = **框架自己注入、且属于凭据的值**：
//
//   - **经策略获批的 required_env 注入值**：一律算秘密（那是工具描述里**显式声明**
//     的凭据通道，声明了就当凭据处理）；
//   - **session 级值里凭据分类命中的**（键名凭据类或值凭据形态，见
//     core.IsCredentialKey / core.IsCredentialValue）。
//
// 普通 session 值（LOG_LEVEL=debug、MODE=ok 之类）**不进 secrets**——它们是数据不是
// 秘密，抹掉就是误伤（`echo build ok debug 1` 不该变成 `build *** *** 1`）。
//
// 刻意**不含**继承来的父 env（净化后的普通环境，如 PATH/HOME）：那些不是注入，
// redact 它们同样会误伤输出里的正常数据（零猜测零误伤）。
func (l *Loop) buildExecEnv(ctx context.Context, desc *contracts.Tool) (core.EnvScope, []string) {
	proc := processEnv()
	base := core.ScrubEnv(core.MergeEnv(proc, l.sessionEnv))

	// session 级注入：只有**凭据分类命中**的值才算秘密。
	secrets := make([]string, 0, len(l.sessionEnv))
	for k, v := range l.sessionEnv {
		if core.IsCredentialKey(k) || core.IsCredentialValue(v) {
			secrets = append(secrets, v)
		}
	}

	if desc == nil || len(desc.RequiredEnv) == 0 {
		return base, secrets
	}

	call := make(core.EnvScope, len(desc.RequiredEnv))
	seen := make(map[string]struct{}, len(desc.RequiredEnv))
	for _, raw := range desc.RequiredEnv {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue // 描述里重复声明同一键：只裁决/注入一次
		}
		seen[key] = struct{}{}

		dec := policy.Decide(ctx, l.policy, l.approver, policy.Query{
			Tool:      envCapabilityPrefix + key,
			SessionID: &l.sessionID,
			AgentID:   &l.agentID,
		})
		if dec.Verdict != contracts.EffectAllow {
			continue // 未获准 = 不注入（fail-close；不记值也不记键，H19）
		}
		if v, ok := lookupEnv(proc, key); ok {
			call[key] = v
			secrets = append(secrets, v) // 注入的值 → 必须能在输出里被收口
		}
	}
	return core.MergeEnv(base, call), secrets
}

// processEnv 取当前进程 env（键 → 值）。
//
// **值只在本函数与 buildExecEnv 之间流动**，不进事件/Message/日志（H12/H19）。
func processEnv() core.EnvScope {
	out := make(core.EnvScope)
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			continue // 畸形条目（无 '=' 或空键）忽略
		}
		out[k] = v
	}
	return out
}

// lookupEnv 在执行 env 来源里查键：先精确匹配，再大小写不敏感匹配
// （Windows 的 env 名大小写不敏感；精确优先保证跨平台行为一致）。
func lookupEnv(env core.EnvScope, key string) (string, bool) {
	if v, ok := env[key]; ok {
		return v, true
	}
	for k, v := range env {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}
