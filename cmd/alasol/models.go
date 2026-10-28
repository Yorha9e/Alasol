package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/Yorha9e/alasol/runtime/provider/openai"
)

// modelsMain 是 `alasol models`：向 OpenAI 兼容端点查询可用模型列表。
//
// 模型名是端点相关的——不同网关各说各话，硬编码默认必然踩空（I5 的 v0 最小形态：
// 在线发现；内置目录留 v1）。凭证照旧只经 env 取，不进 argv。
func modelsMain(args []string) int {
	fs := flag.NewFlagSet("models", flag.ExitOnError)
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（空=官方）")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名")
	_ = fs.Parse(args)

	ctx := context.Background()
	ids, err := openai.ListModels(ctx, openai.Options{
		BaseURL: *baseURL,
		APIKey:  os.Getenv(*apiKeyEnv),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "查询模型列表失败:", err)
		return 1
	}
	if len(ids) == 0 {
		fmt.Fprintln(os.Stderr, "端点未返回任何模型")
		return 1
	}
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Println(id)
	}
	fmt.Fprintf(os.Stderr, "[alasol] 共 %d 个模型；用 -model <名字> 或 ALASOL_MODEL 选用\n", len(ids))
	return 0
}
