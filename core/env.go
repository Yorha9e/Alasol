package core

import (
	"context"
	"regexp"
	"strings"
)

// Env 注入 v0 子集（H21）：**session + 单次调用**两级作用域；**不给模型看任何 env**。
// global / agent / task 级作用域与上下文 env 留 v1。

// EnvScope 是一次注入作用域的键值集（值是凭据或普通配置，永不进 prompt / 事件 / 日志）。
type EnvScope map[string]string

// MergeEnv 合并注入作用域：call 覆盖 session（单次调用级优先）。
func MergeEnv(session, call EnvScope) EnvScope {
	out := make(EnvScope, len(session)+len(call))
	for k, v := range session {
		out[k] = v
	}
	for k, v := range call {
		out[k] = v
	}
	return out
}

// ScrubEnv 净化执行 env（H16/H18）：剥离凭据类键与凭据形态的值，普通 env（PATH/HOME 等）保留。
// 工具默认继承净化后的父 env；显式注入（required_env 经策略裁决）的键值不在此处剥离。
func ScrubEnv(env EnvScope) EnvScope {
	out := make(EnvScope, len(env))
	for k, v := range env {
		if IsCredentialKey(k) || IsCredentialValue(v) {
			continue
		}
		out[k] = v
	}
	return out
}

// EnvKeys 只返回键名（H19：事件记录只记 env 键名，不记值）。
func EnvKeys(env EnvScope) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	return keys
}

var credentialKeyRe = regexp.MustCompile(`(?i)(.*_(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)S?|.*)`)

// IsCredentialKey 判定凭据类键名（H18 deny 名单：*_KEY / *_TOKEN / *_SECRET / *_PASSWORD / *_CREDENTIAL*）。
//
// 导出给需要"按凭据分类"的地方复用（如输出侧的脱敏集合：只有凭据类的 session 值
// 才算秘密，普通值如 LOG_LEVEL=debug 是数据，抹掉就是误伤）。
func IsCredentialKey(k string) bool {
	u := strings.ToUpper(k)
	for _, suf := range []string{"_KEY", "_TOKEN", "_SECRET", "_PASSWORD", "_CREDENTIAL", "_CREDENTIALS"} {
		if strings.HasSuffix(u, suf) || strings.Contains(u, suf) {
			return true
		}
	}
	return false
}

// 凭据值形态的**整值锚定**判定（H18）：与 Redact（子串脱敏，见 redact.go）共用同一组
// shape* 形状核——这边加首尾锚定（"整个值就是凭据"），那边不加（"文本里混有凭据"）。
// 形状核只在 redact.go 定义一次，避免判定与脱敏各写一遍字面量而静默漂移。
var (
	valueSkRe  = regexp.MustCompile(`^(?:` + shapeSk + `)$`)
	valueGhpRe = regexp.MustCompile(`^(?:` + shapeGhp + `)$`)
	valueJwtRe = regexp.MustCompile(`^(?:` + shapeJwt + `)$`)
)

// IsCredentialValue 判定凭据形态的值（H18：sk- / ghp_ / gho_ / JWT 形态）。
//
// 导出给需要"按凭据分类"的地方复用（理由同 IsCredentialKey）。
func IsCredentialValue(v string) bool {
	v = strings.TrimSpace(v)
	return valueSkRe.MatchString(v) || valueGhpRe.MatchString(v) || valueJwtRe.MatchString(v)
}

type envCtxKey struct{}

// WithEnv 把执行 env 注入作用域放进 ctx（H12/H19：只经 ctx 传给工具进程，
// **永不进 prompt / 事件 payload / 日志**；事件只记键名）。
func WithEnv(ctx context.Context, env EnvScope) context.Context {
	return context.WithValue(ctx, envCtxKey{}, env)
}

// EnvFrom 从 ctx 取出注入作用域（工具执行侧用）。
func EnvFrom(ctx context.Context) (EnvScope, bool) {
	env, ok := ctx.Value(envCtxKey{}).(EnvScope)
	return env, ok
}
