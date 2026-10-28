package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/Yorha9e/alasol/runtime/models"
	"github.com/Yorha9e/alasol/runtime/provider/openai"
)

// modelsMain 是 `alasol models`：列出可用模型——内置静态快照 + 在线拉取（I5 的 v1 完整形态）。
//
// 模型名是端点相关的（I9：无万能默认），故权威来源是**在线查端点**（GET {base}/models）；
// 内置快照给离线起点 + 已知模型的人类可读元数据。两者合并并呈：
//   - 默认先在线刷新，失败则**保留内置快照**并告警（端点抖动不该让命令变空）；
//   - -offline 跳过在线，只呈内置快照（无网/CI 用）。
//
// 凭证照旧只经 env 取（-api-key-env 只传变量名），不进 argv。
func modelsMain(args []string) int {
	fs := flag.NewFlagSet("models", flag.ExitOnError)
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（空=官方）")
	apiKeyEnv := fs.String("api-key-env", "OPENAI_API_KEY", "持有 API key 的环境变量名")
	offline := fs.Bool("offline", false, "跳过在线拉取，只呈内置静态快照")
	_ = fs.Parse(args)

	ctx := context.Background()

	var fetch models.FetchFunc
	if !*offline {
		fetch = func(ctx context.Context) ([]string, error) {
			return openai.ListModels(ctx, openai.Options{
				BaseURL: *baseURL,
				APIKey:  os.Getenv(*apiKeyEnv),
			})
		}
	}
	cat := models.New(nil, fetch)

	onlineOK := false
	if !*offline {
		if err := cat.Refresh(ctx); err != nil {
			// 在线失败不清空目录（Catalog.Refresh 已保留内置）；此处只告警。
			fmt.Fprintln(os.Stderr, "[alasol] 在线拉取失败，仅呈内置快照:", err)
		} else {
			onlineOK = true
		}
	}

	list := cat.Models()
	if len(list) == 0 {
		fmt.Fprintln(os.Stderr, "没有任何模型（内置快照为空且未拉取到在线模型）")
		return 1
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	for _, m := range list {
		tag := "online"
		if m.Builtin {
			tag = "builtin"
		}
		meta := ""
		if m.Provider != "" {
			meta += " " + m.Provider
		}
		if m.ContextWindow > 0 {
			meta += fmt.Sprintf(" ctx=%d", m.ContextWindow)
		}
		fmt.Printf("%-40s [%s]%s\n", m.ID, tag, meta)
	}
	builtin, online := 0, 0
	for _, m := range list {
		if m.Builtin {
			builtin++
		} else {
			online++
		}
	}
	src := "内置快照"
	if onlineOK {
		src = "内置快照 + 在线"
	}
	fmt.Fprintf(os.Stderr, "[alasol] 共 %d 个模型（builtin=%d online=%d，来源：%s）；"+
		"用 -model <名字> 或 ALASOL_MODEL 选用\n", len(list), builtin, online, src)
	return 0
}
