package host

// 本文件实现 **opt-in 的 CORS 来源白名单**（GUI 联调缺口：dsh 前端在
// http://127.0.0.1:3080 用浏览器**直连**协调四面 —— /api/gates*、/api/presence、
// /api/merge/check —— 需要响应带 Access-Control-Allow-Origin 才能读）。
//
// # 缺省 = 零行为变化（酸测试）
//
// 白名单空（没配 cors_origins）→ **一个 CORS 头都不发**，OPTIONS 照旧 405/404。
// host/doc.go「前端接入方式（G5）」那条口径（"跨源页面不该能驱动本地 hub"）
// **继续成立**——那是默认。白名单是**显式例外通道**，不是新常态。
//
// # 为什么是"精确 origin 白名单"而不是别的形状
//
//   - **精确 origin**（scheme+host+port 齐全）：origin 是**安全边界**（浏览器把它当
//     "这个响应可以被哪个页面读到"），一个 origin 就是一份点名授权。带路径/带通配的
//     写法会让人以为"放行了某个子路径/某个域"，实际语义完全不同——宁可启动即拒。
//   - **不支持通配 `*`**：`Access-Control-Allow-Origin: *` 等于"任何页面都能读本地
//     hub 的响应"，与"无 cookie、无 token"的 v0 安全模型直接冲突（读到了就是拿到了
//     别人的会话数据）。写了 `*` → **启动即拒**（fail-close），不让它悄悄变成"全开"。
//   - **默认关 + 逐条列举**：白名单是 opt-in 的旋钮，不配就零行为变化；配了就是
//     使用者**点名**放行谁。
//
// # 作用范围（钉死）
//
//   - **`/api/*` 全部**（不只协调四面——统一口径，免得"哪个端点放行"变成第二份
//     路由表；前端 preflight 还会打到任意路径，含未命中路由的 404，故 404 也带头）；
//   - **静态面（GET / 与 /index.html）不动**：那是同源托管的页面，不该被跨源页面读；
//   - **/ws 不动**：WS 的 Origin 校验维持现状（runtime/transport.checkOrigin：跨源页面
//     403 origin_rejected）——事件流走 driver，不走浏览器直连。
//
// # 方法口径
//
// 放行 **GET / POST / OPTIONS**（preflight）+ 请求头限 **Content-Type**。
// 其它方法不给 ACAO（fail-close）：浏览器的 preflight 会因 Allow-Methods 不含该方法
// 而拦下，跨源页面读不到响应。

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	// corsAllowMethods 是 CORS 白名单**放行的方法**（preflight 应答回给浏览器）。
	//
	// 只放行 GET/POST/OPTIONS：这三类覆盖了 hub 的读面（GET）与协调写面（POST）；
	// 其余方法（PUT/DELETE/PATCH…）不放行——跨源页面驱动本地 hub 的写面已经够用了。
	corsAllowMethods = "GET, POST, OPTIONS"
	// corsAllowHeaders 是 CORS 白名单**放行的请求头**（preflight 应答回给浏览器）。
	//
	// 只放行 Content-Type：GUI 前端的 JSON 体（application/json）需要它；其余头
	// （Authorization 等）不放行——本仓的凭据纪律是"凭证只走 header/query 且不进
	// cookie"，放开自定义头等于给跨源页面递刀。
	corsAllowHeaders = "Content-Type"
)

// corsAllowlist 是 CORS 来源白名单（**精确 origin** 集合；空 = 关 = 零行为变化）。
//
// 不可变：装配期建一次（host.New），运行期只读。
type corsAllowlist struct {
	// origins 是白名单集合（键 = 精确 origin 串）。
	origins map[string]struct{}
}

// newCORSAllowlist 造白名单。空表 → **nil**（= 关；调用方据此"原样返回 handler"，
// 一个 CORS 头都不发——零行为变化的实现方式就是**根本不装**这段逻辑）。
func newCORSAllowlist(list []string) *corsAllowlist {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(list))
	for _, o := range list {
		m[strings.TrimSpace(o)] = struct{}{}
	}
	return &corsAllowlist{origins: m}
}

// allowed 报告 origin 是否**精确命中**白名单（字节精确，大小写敏感）。
//
// 精确匹配而不是"前缀/通配"：origin 是安全边界，点名到字节才谈得上"放行谁"。
// 命中失败 = **什么都不加**（fail-close：没在名单里的页面读不到响应）。
func (a *corsAllowlist) allowed(origin string) bool {
	if a == nil || len(a.origins) == 0 {
		return false
	}
	_, ok := a.origins[origin]
	return ok
}

// ValidateCORSOrigin 校验一个 CORS 白名单条目（**精确 origin** 形态）。
//
// 合法的形态：`http(s)://<host>:<port>` —— scheme + host + port 齐全，**无路径**
// （末尾斜杠也不行）、无查询串、无 fragment、无用户信息。
//
// 非法 → 报错（调用方 = host.Config.validate → **启动即拒**，fail-close：
// 配了个不生效的白名单比没配更难排查——浏览器只看到"被 CORS 拦了"，看不出是白名单
// 写错了；启动期拒掉，报错文案直接点名是哪一条）。
//
// `*` 单独点名（通配 = 全开 = 与 v0 安全模型冲突，不接受）。
func ValidateCORSOrigin(raw string) error {
	s := strings.TrimSpace(raw)
	if s == "" {
		return errors.New("条目为空（CORS 白名单只收精确 origin，如 \"http://127.0.0.1:3080\"）")
	}
	if s == "*" {
		return errors.New("不支持通配 `*`（那等于任何页面都能读本地 hub 的响应）" +
			"——请逐条列出精确 origin，如 \"http://127.0.0.1:3080\"")
	}
	if strings.Contains(s, "*") {
		return errors.New("不支持通配（条目里含 `*`）——请逐条列出精确 origin")
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("不是合法的 origin（须形如 \"http://127.0.0.1:3080\"）: %w", err)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("scheme 须是 http 或 https（得到 %q）", u.Scheme)
	}
	if u.User != nil {
		return errors.New("不得含用户信息（origin 里没有 user:pass@ 这种形态）")
	}
	if u.Path != "" {
		return fmt.Errorf("不得含路径（精确 origin 只有 scheme://host:port，得到路径 %q）", u.Path)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return errors.New("不得含查询串")
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return errors.New("不得含 fragment")
	}
	if u.Hostname() == "" {
		return errors.New("缺 host（须形如 \"http://127.0.0.1:3080\"）")
	}
	port := u.Port()
	if port == "" {
		return errors.New("缺端口（精确 origin 须是 http(s)://host:port 形态——" +
			"省端口的 origin 落不到具体监听实例上）")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("端口不合法: %q", port)
	}
	return nil
}

// ValidateCORSOrigins 校验整张 CORS 白名单（逐条；任一条不合法 → 整张拒）。
//
// **启动即拒**（fail-close）：半张有效的白名单 = 使用者以为放行了、浏览器却拒，
// 两边都不好排查。
func ValidateCORSOrigins(list []string) error {
	for _, raw := range list {
		if err := ValidateCORSOrigin(raw); err != nil {
			return fmt.Errorf("host: Config.CORSOrigins 条目 %q 不合法: %w", strings.TrimSpace(raw), err)
		}
	}
	return nil
}

// corsScopePath 报告路径是否在 CORS 白名单的**作用范围**内（只 `/api/*`）。
//
// 范围外的两类（**都不动**）：
//   - **静态面**（GET / 与 /index.html）：同源托管的页面，不该被跨源页面读；
//   - **/ws**：WS 的 Origin 校验维持现状（跨源页面 403 origin_rejected）。
//
// "/api"（不带斜杠）与 "/api/…" 同属 API 面（mux 里两者都注册了回退面）。
func corsScopePath(p string) bool {
	return p == "/api" || strings.HasPrefix(p, "/api/")
}

// withCORS 给 HTTP 出口挂 CORS 白名单中间件。
//
// **白名单为空 → 原样返回 handler**（零行为变化的实现方式：根本不装这段逻辑，
// 一个 CORS 头都不发、OPTIONS 照旧 405/404）。
//
// 白名单非空时（opt-in）：
//
//	Origin 精确命中 + GET/POST  → 加 Access-Control-Allow-Origin（**原样回显**该
//	                              Origin）+ Vary: Origin，再透传给路由
//	Origin 精确命中 + OPTIONS   → **204** + 放行头（Allow-Origin / Allow-Methods /
//	                              Allow-Headers / Vary），**不落到路由**（路由不认
//	                              OPTIONS；前端 preflight 会打到任意路径，含不存在的）
//	未命中 / 无 Origin           → **什么都不加**（现状口径；fail-close）
//	其它方法（PUT/DELETE/…）     → 不加 ACAO（只放行 GET/POST/OPTIONS）
//
// 命中白名单时头是在**透传之前**写下的，故**未命中路由的 404**（/api/ 回退面）也带
// 这些头——前端要的就是"任何路径的响应都可读"。
//
// 诚实边界：`Vary: Origin` **只在命中时加**（未命中时"什么都不加"是契约钉死的口径）。
// hub 只监听 loopback、面前没有共享缓存，故无碍；若把 hub 放到共享反向代理后面，
// 未命中的响应可能被缓存下来再喂给命中方（浏览器会因缺 ACAO 拒读——功能性问题，
// 不是安全问题），那种部署要在代理层自行补 `Vary: Origin`。
func (h *Hub) withCORS(next http.Handler) http.Handler {
	allow := h.corsAllow
	if allow == nil {
		return next // 关：原样返回（零行为变化）
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !corsScopePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" || !allow.allowed(origin) {
			// 无 Origin（curl/CLI）或未命中 → 什么都不加（现状）。
			next.ServeHTTP(w, r)
			return
		}
		hdr := w.Header()
		switch r.Method {
		case http.MethodOptions:
			// preflight：命中白名单 → 204 + 放行头。
			hdr.Set("Access-Control-Allow-Origin", origin)
			hdr.Set("Access-Control-Allow-Methods", corsAllowMethods)
			hdr.Set("Access-Control-Allow-Headers", corsAllowHeaders)
			hdr.Set("Vary", "Origin")
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet, http.MethodPost:
			hdr.Set("Access-Control-Allow-Origin", origin)
			hdr.Set("Vary", "Origin")
			next.ServeHTTP(w, r)
		default:
			// 只放行 GET/POST/OPTIONS：其它方法不给 ACAO（fail-close）。
			next.ServeHTTP(w, r)
		}
	})
}
