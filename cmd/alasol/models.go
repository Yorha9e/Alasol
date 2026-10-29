package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/host"
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
// # 配置面（v2-S5 清账⑤）
//
// 此前这个命令**不读 TOML**：用户配了 base_url / api_key 的配置文件，`alasol models`
// 却还去打官方端点（或因为没 key 而失败）——"配了却像没配"。现在与 run/serve/resume
// 走同一条优先级（H15）：**显式 flag > 进程 env > 配置文件**。凭据仍只经 env/文件收敛
// （-api-key-env 只传变量名），不进 argv/日志。
//
// 配置文件解析失败 → fail-close 报错（与 run/serve 同口径：损坏的配置可能正是用户
// 指望的端点/凭据来源，静默忽略会让命令悄悄打向别处）。
func modelsMain(args []string) int {
	fs := flag.NewFlagSet("models", flag.ExitOnError)
	baseURL := fs.String("base-url", "", "OpenAI 兼容端点（空=官方；缺省走 env ALASOL_BASE_URL，再走配置文件）")
	apiKeyEnv := fs.String("api-key-env", "", "持有 API key 的环境变量名（缺省走配置文件，再 OPENAI_API_KEY）")
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	offline := fs.Bool("offline", false, "跳过在线拉取，只呈内置静态快照")
	_ = fs.Parse(args)

	ctx := context.Background()

	// H15：读用户级配置（不存在 = 合法状态；解析失败 → fail-close）。
	fc, err := host.LoadFileConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}

	// 优先级与 run/serve/resume 一字不差（D21：口径只定义一次，host.Precedence 是唯一实现）。
	baseURLVal, _ := host.Precedence(*baseURL, os.Getenv("ALASOL_BASE_URL"), fc.BaseURL)
	// 凭据变量名：显式 flag > 文件 > 默认（OPENAI_API_KEY）——"显式"用 flagSet 判定
	// （models 有自己的 FlagSet，不是 hubFlags；口径与 config.go 的 flagIfSet 一致）。
	keyEnvFlag := ""
	if flagSet(fs, "api-key-env") {
		keyEnvFlag = *apiKeyEnv
	}
	keyEnv := host.ResolveAPIKeyEnv(keyEnvFlag, fc.APIKeyEnv)
	apiKey := host.ResolveCredential(keyEnv, fc.APIKey) // env 有值则 env 赢；永不进日志

	var fetch models.FetchFunc
	if !*offline {
		fetch = func(ctx context.Context) ([]string, error) {
			return openai.ListModels(ctx, openai.Options{
				BaseURL: baseURLVal,
				APIKey:  apiKey,
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
	// 端点与凭据来源只说"从哪来"，**不回显任何凭据值**（H12/H19）。
	// 端点 URL 也要脱敏：userinfo（user:pass@）与 query（?api_key=…）里常藏着密钥，
	// 原样打到终端就是泄密（终端会进日志/截图/录屏）。
	endpoint := baseURLVal
	if endpoint == "" {
		endpoint = "官方端点（未配 base_url）"
	} else {
		endpoint = redactEndpoint(endpoint)
	}
	fmt.Fprintf(os.Stderr, "[alasol] 共 %d 个模型（builtin=%d online=%d，来源：%s）；"+
		"端点：%s；凭据变量：%s\n", len(list), builtin, online, src, endpoint, keyEnv)
	// 配置文件里选定的模型名（若有）一并提示：用户不必猜"我配的是哪个"。
	if m := strings.TrimSpace(fc.Model); m != "" {
		fmt.Fprintf(os.Stderr, "[alasol] 配置文件里的 model = %s（flag/ALASOL_MODEL 优先）\n", m)
	}
	fmt.Fprintln(os.Stderr, "[alasol] 用 -model <名字> 或 ALASOL_MODEL 选用")
	return 0
}

// redactEndpoint 把端点 URL 脱敏后回显（只说"从哪来"，不回显凭据）。
//
// 脱敏口径（H12/H19）：
//   - **userinfo**（`https://user:pass@host/...`）→ 整段替换成 `***@`；
//   - **query**（`?api_key=...` / `?token=...`）→ 整段去掉（查询串里常带凭据）；
//   - **fragment**（`#...`）→ 同样去掉。
//
// 解析失败（不是合法 URL）→ 退回保守形态：去掉 query/fragment，并把 "@" 之前的部分
// 整段掩掉。宁可少说，也不把凭据原样打出来。
func redactEndpoint(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return maskBeforeAt(stripQuery(s))
	}
	if u.User != nil {
		u.User = url.User("***")
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// stripQuery 去掉 URL 里的 query 与 fragment（`?…` / `#…` 之后的部分）。
func stripQuery(s string) string {
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		return s[:i]
	}
	return s
}

// maskBeforeAt 把 "@" 之前的部分（userinfo）整段掩掉（`user:pass@host` → `***@host`）。
func maskBeforeAt(s string) string {
	i := strings.LastIndex(s, "@")
	if i < 0 {
		return s
	}
	return "***" + s[i:]
}
