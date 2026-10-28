package main

import (
	"fmt"
	"os"
)

// resolveModel 解析模型名：flag 值优先，缺省回落 env `ALASOL_MODEL`。
//
// **不做错误默认**（fail-close）：模型名是端点相关的（gpt-4o-mini 只对 OpenAI 官方有意义），
// 写死一个默认值换网关就是废请求。两处都空 → 明确报错并给出发现手段（`alasol models`）。
func resolveModel(flagVal string) (string, error) {
	if v := firstNonEmpty(flagVal, os.Getenv("ALASOL_MODEL")); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("缺模型名：用 -model 指定，或设环境变量 ALASOL_MODEL；" +
		"可用 `alasol models` 列出端点支持的模型")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
