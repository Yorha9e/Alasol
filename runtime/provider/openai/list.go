package openai

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// normalizeBaseURL 归一化端点根路径：去首尾空白、去尾部 "/"。
// New 与 ListModels 共用同一口径——同一个 -base-url 值在两个命令上必须行为一致。
func normalizeBaseURL(u string) string {
	return strings.TrimRight(strings.TrimSpace(u), "/")
}

// endpointURL 把端点根路径与相对路径拼成完整 URL（**保留 base 的 query**）。
//
// # 为什么不用 base + rel 直接拼串（老坑）
//
// base 形如 "http://h/v1?api-key=xxx" 时，直接拼会得到
// "http://h/v1?api-key=xxx/embeddings"——**路径落进了 query**：端点收到的是
// 路径 "/v1" + query "api-key=xxx/embeddings"，请求打到了错的路径上，query 也被改了
// 形状（key 被改写）。有些网关（one-api / azure 兼容层之类）就是靠 query 传 key 或
// 版本号——直接拼会把它们打坏，而且**错得看不出来**（端点回 404，排查半天是本地拼错）。
//
// 口径：**路径用 path.Join 接到 base 的路径后面，query 原样保留**。fragment 被丢弃
// （fragment 不会发给服务端，留着只会误导人）。
//
// 解析不了 base（畸形 URL）→ 退回老口径的拼串：让 HTTP 请求本身去报那个错，
// 而不是在这里造一个更含糊的错。
func endpointURL(base, rel string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base + rel
	}
	u.Path = path.Join(u.Path, rel)
	u.RawPath = "" // 路径变了 → 让 URL 重新转义（别用与新 Path 不匹配的旧 RawPath）
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL(base, "/models"), nil)
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
