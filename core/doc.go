// Package core 是 agent 的骨架逻辑：loop、事件总线、策略链、上下文装配、扩展注册等。
//
// 只定义能力接口（exec / fs / net / proc），不含任何 I/O 实现；
// 不得 import runtime / extensions / host / ui（见 docs/research/09）。
package core
