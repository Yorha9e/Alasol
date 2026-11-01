package core

import "regexp"

// 本文件是**脱敏口径（H18）的可复用出口**：把"凭据形态的值"从自由文本里 mask 成 "***"。
//
// # 为什么住在 core
//
// 与 ScrubEnv / IsCredentialValue 同源（H16/H18 的 deny 名单 + 值形态匹配）：凭据形态
// 的**形状定义只有一份**（下面的 shape* 常量），IsCredentialValue 拿它做**整值锚定**判定
// （"这个 env 值是不是凭据"），Redact 拿它做**子串扫描**替换（"这段自由文本里有没有混进
// 凭据"）。两件事共用同一组形状 → 改形状只改一处，判定与脱敏不会静默漂移。
//
// # 复用，不新造
//
// telemetry（L2/D19）在 emit 前跑本函数脱敏：它**不引入新的凭据识别规则**，用的就是既有
// 的 sk- / ghp_|gho_|ghu_|ghs_ / JWT 三形态。观测管道与执行管道认同一套"什么是凭据"。

// 凭据值形态的**形状核**（H18：与 IsCredentialValue 完全同源，只是这里不做首尾锚定）。
//
//   - sk-…：OpenAI/Anthropic 一类的 secret key；
//   - gh[pous]_…：GitHub token（PAT / OAuth / user-to-server / server-to-server）；
//   - JWT：ey…<header>.<payload>.<sig> 三段点分。
const (
	shapeSk  = `sk-[A-Za-z0-9_-]{8,}`
	shapeGhp = `gh[pous]_[A-Za-z0-9]{8,}`
	shapeJwt = `ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*`
)

// redactCredRe 匹配文本中**任意位置**的凭据形态 token（三形状取并集）。
//
// 与 IsCredentialValue 的差别只在锚定：那边是 ^(?:shape)$（整值判定），这里是裸形状
// （子串扫描）——同一组 shape* 常量，别在两处各写一遍字面量。
var redactCredRe = regexp.MustCompile(shapeSk + `|` + shapeGhp + `|` + shapeJwt)

// redactMask 是脱敏替换串（与 core/loop 的 redactSecrets 一致：***）。
const redactMask = "***"

// Redact 把自由文本里**混入的凭据形态 token** 逐个替换成 "***"（H18 脱敏口径）。
//
// 定位：给"可能夹带凭据的自由文本"（如观测管道的错误摘要）做最后一道收口。它是**形态
// 匹配**（认 shape 不认具体值）——框架若知道注入了哪些秘密值，精确替换仍应走 core/loop
// 的 redactSecrets（零误伤）；Redact 兜的是"不知道具体值、但认得形态"的那类文本。
//
// 空串原样返回；不含任何凭据形态的文本**一字不改**（不误伤正常数据）。
func Redact(s string) string {
	if s == "" {
		return s
	}
	return redactCredRe.ReplaceAllString(s, redactMask)
}
