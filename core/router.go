package core

// Router 是模型路由扩展点（I7；进程内接口）：按 agent 类型字符串给出该类型应使用的模型名。
//
// # 语义与边界
//
//   - **输入**是 agent 类型 id（如 "planner" / "coder" / "root"，大小写敏感，与
//     core/agents 的类型注册表一致）；
//   - **输出**是模型名（原样透传给 provider，非空才算"有意见"）；
//   - 返回**空串** = 对该类型"无意见"，宿主回落到既有解析口径（类型声明的 model 字段 /
//     主模型——见 host.resolveAgentModel）；
//   - **nil = 不路由**：宿主在装配处判 nil，完全走既有解析口径（生产默认 nil =
//     零行为变化）。
//
// # 为什么是接口而不是函数字段
//
// 与 Provider / Approver 同形态（C2/E1：消费方定义接口、host/runtime 提供实现、测试注入
// fake）——便于生产换实现、测试精确控制"某个类型路由到哪个模型"，且不给 core 引入任何
// 对 runtime 的依赖。
//
// # 谁在咨询它
//
// 宿主是唯一装配处：host.Config.ModelRouter 持有实现，host.resolveAgentModel 在装配
// agent 类型注册表（内建五类 + TOML 自定义）与树 root 类型时按类型 id 咨询它，
// **router 非 nil 时优先用其结果**。
type Router interface {
	// RouteModel 给出 agentType 对应的模型名；空串 = 该类型无意见（回落既有口径）。
	RouteModel(agentType string) string
}
