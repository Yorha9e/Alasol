package models

// BuiltinSnapshot 返回编译期内置的常见模型清单（I5 的"内置静态快照"）。
//
// **这是快照、非权威**：模型元数据（上下文窗口等）随各家发版变动，可能过时；权威来源是
// 在线拉取（{base}/models / models.dev）。内置快照的价值是**离线也能给个起点**，且为已知
// 模型附带人类可读名/厂商，免得界面上只冒一串裸 id。
//
// 收录口径：跨厂商挑**广为人知、命名稳定**的几档（不追求全——全量交在线刷新）。上下文窗口
// 取公开文档的约数，标 best-effort。新增/修订只需改本函数（快照与在线合并逻辑在 catalog.go）。
func BuiltinSnapshot() []Model {
	return []Model{
		// OpenAI
		{ID: "gpt-4o", Name: "GPT-4o", Provider: "openai", ContextWindow: 128000},
		{ID: "gpt-4o-mini", Name: "GPT-4o mini", Provider: "openai", ContextWindow: 128000},
		{ID: "gpt-4-turbo", Name: "GPT-4 Turbo", Provider: "openai", ContextWindow: 128000},
		{ID: "gpt-4", Name: "GPT-4", Provider: "openai", ContextWindow: 8192},
		{ID: "gpt-3.5-turbo", Name: "GPT-3.5 Turbo", Provider: "openai", ContextWindow: 16385},
		{ID: "o1", Name: "o1", Provider: "openai", ContextWindow: 200000},
		{ID: "o1-mini", Name: "o1-mini", Provider: "openai", ContextWindow: 128000},
		// Anthropic
		{ID: "claude-3-5-sonnet-20241022", Name: "Claude 3.5 Sonnet", Provider: "anthropic", ContextWindow: 200000},
		{ID: "claude-3-5-haiku-20241022", Name: "Claude 3.5 Haiku", Provider: "anthropic", ContextWindow: 200000},
		{ID: "claude-3-opus-20240229", Name: "Claude 3 Opus", Provider: "anthropic", ContextWindow: 200000},
		{ID: "claude-3-sonnet-20240229", Name: "Claude 3 Sonnet", Provider: "anthropic", ContextWindow: 200000},
		{ID: "claude-3-haiku-20240307", Name: "Claude 3 Haiku", Provider: "anthropic", ContextWindow: 200000},
	}
}
