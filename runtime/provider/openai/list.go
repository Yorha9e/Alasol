package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// normalizeBaseURL 归一化端点根路径：去首尾空白、去尾部 "/"。
// New 与 ListModels 共用同一口径——同一个 -base-url 值在两个命令上必须行为一致。
func normalizeBaseURL(u string) string {
	return strings.TrimRight(strings.TrimSpace(u), "/")
}

// ListModels 查询 OpenAI 兼容端点的可用模型列表（GET {BaseURL}/models）。
//
// 模型名是**端点相关**的：不同网关各说各话，v0 不做内置目录（I5 的内置快照留 v1），
// 由调用方/用户在线发现。Options 与 Run 同口径（BaseURL 归一化、APIKey 可空）。
func ListModels(ctx context.Context, o Options) ([]string, error) {
	base := normalizeBaseURL(o.BaseURL)
	if base == "" {
		base = DefaultBaseURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/models", nil)
	if err != nil {
		return nil, fmt.Errorf("openai: 构造 /models 请求: %w", err)
	}
	if o.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.APIKey)
	}
	client := o.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai: 请求 /models: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("openai: /models 返回 %s", resp.Status)
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("openai: 解析 /models 响应: %w", err)
	}
	ids := make([]string, 0, len(body.Data))
	for _, m := range body.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids, nil
}
