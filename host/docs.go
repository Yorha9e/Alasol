package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/storage"
)

// 本文件是文档族（G2 第 5 族）在宿主的 **HTTP 面**（v4 装配）：四个 /api/docs* 端点
// + 错误映射 writeDocsHubError。存储**槽位**是接口 docStore（见下方定义；当前实现 =
// runtime/store/sqlite.DocStore——换实现宿主零改动）。
//
// # 零行为变化口径
//
// **仅 h.docs != nil 时注册**（Config.DocumentsEnabled=true 才建）：关 = 四个端点
// 路由不注册 → 路径认不得 → 404 not_found（与 vectors 端点同口径，酸测试）。
//
// # 单写者纪律（G17）
//
// 写路径（upsert / delete）经 DocStore.Transact 开真实事务执行——**写只经宿主这一个
// ingest 入口**（与 IngestVectors 同口径）：HTTP 调用方拿不到裸 Tx，事务边界由宿主收口。
// 文档是投影、事件流才是事实源（F5）：这里的手工写面是"种子/运维注入"入口，不绕过
// 版本冲突口径（Upsert 的 checkBatch：整批拒绝，ErrConflict → 409）。
//
// 请求/响应直接用 storage.Doc（自带 JSON tag），不造多余 view。

// maxDocsBody 限制 POST 请求体大小（一批文档 + 正文，1MB 够正常种子/运维批）。
const maxDocsBody = 1 << 20

// docStore 是宿主对文档族存储的**依赖面**（Hub 的槽位类型）。
//
// 形态照 blob 的 storage.Blob（接口槽位，不是具体实现）：宿主只说"要一个能读能写、
// 还能开事务的文档存储"，**不认实现**——当前实现是 runtime/store/sqlite.DocStore，
// 换实现（内存 / 别的 DB / 远端服务）时**宿主一行不改**（G2b：接口在 core，实现在 runtime；
// 消费方拿接口说话，换实现零改动）。
//
// 三个方法集全部由 *sqlite.DocStore 直接提供（零适配）：
//   - storage.Documents：读面（Get / List）；
//   - storage.DocumentProjection：写面（EnsureSchema / Upsert / Delete / Clear）；
//   - Transact：宿主 ingest 面（/api/docs/upsert、/api/docs/delete）的事务跑道
//     （HTTP 调用方拿不到裸 Tx，事务边界由宿主收口——与 IngestVectors 同口径）。
//
// 为什么不把 Transact 塞进 core/storage 的契约：它是**宿主装配面**的便利方法（给没有
// 事务跑道的调用方一条），不是文档族契约的一部分（契约里的写面一律带 graph.Tx）。
type docStore interface {
	storage.Documents
	storage.DocumentProjection
	Transact(ctx context.Context, fn func(coregraph.Tx) error) error
}

// serveDocGet 服务 GET /api/docs/{collection}/{key}（单条读面）。
//
//	200 → storage.Doc JSON（Body 是原始 JSON 字节，原样回）
//	404 → 未启用（路由本就不注册；这里是纵深防御）或文档不存在
//	500 → 读取失败
func (h *Hub) serveDocGet(w http.ResponseWriter, r *http.Request) {
	if h.docs == nil {
		writeJSONError(w, http.StatusNotFound, "documents_disabled",
			"文档族存储未启用（Config.DocumentsEnabled=false；/api/docs 不注册）")
		return
	}
	doc, err := h.docs.Get(r.Context(), r.PathValue("collection"), r.PathValue("key"))
	if err != nil {
		writeDocsHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, doc)
}

// serveDocList 服务 GET /api/docs?collection=&prefix=&limit=（列举读面）。
//
//	200 → {"docs":[storage.Doc, ...]}（按 (collection, key) 字节序；空 = 空数组不是 null）
//	400 → limit 不是非负整数 / limit 负值（ErrBadQuery）
//	404 → 未启用（路由本就不注册；这里是纵深防御）
//	500 → 列举失败
//
// collection / prefix 是可选过滤（空 = 不限）；prefix 是**字节前缀、大小写敏感**
// （陷阱 17，DocStore.List 的口径）。limit 空/0 = 不截断（实现默认）。
//
// **collection 与 prefix 都不 trim**（字节精确）：集合名/键名是**字节精确**的标识
// （陷阱 17），" rules" 与 "rules" 是两个不同的集合名——悄悄 trim 掉空白等于把
// "查不到的集合"变成"查另一个集合"，答案看着对、其实查错了对象（与 prefix 的处理
// 保持对称：prefix 也不 trim，空白是合法前缀字节）。
func (h *Hub) serveDocList(w http.ResponseWriter, r *http.Request) {
	if h.docs == nil {
		writeJSONError(w, http.StatusNotFound, "documents_disabled",
			"文档族存储未启用（Config.DocumentsEnabled=false；/api/docs 不注册）")
		return
	}
	q := storage.DocQuery{
		Collection: r.URL.Query().Get("collection"), // 字节精确：不 trim
		Prefix:     r.URL.Query().Get("prefix"),     // 字节精确：不 trim
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "bad_limit",
				fmt.Sprintf("limit 必须是整数（0 = 不截断；得到 %q）", raw))
			return
		}
		q.Limit = n // 负值交给 List 报 ErrBadQuery（契约口径，这里不重复判）
	}
	docs, err := h.docs.List(r.Context(), q)
	if err != nil {
		writeDocsHubError(w, err)
		return
	}
	// 空结果回空数组不是 null（前端 docs.forEach 不需要判 null；与 /api/sessions 同口径）。
	if docs == nil {
		docs = []storage.Doc{}
	}
	writeJSONBody(w, http.StatusOK, map[string]any{"docs": docs})
}

// docsUpsertRequest 是 POST /api/docs/upsert 的请求形状。
type docsUpsertRequest struct {
	Docs []storage.Doc `json:"docs"`
}

// serveDocUpsert 服务 POST /api/docs/upsert（写面 ingest 入口）。
//
//	请求体：{"docs":[storage.Doc, ...]}（Doc 自带 JSON tag：collection/key/body/revision/…）
//	200 → {"ok":true}（整批提交；DocStore.Upsert 的整批拒绝口径：冲突批次零写入）
//	400 → 体不是合法 JSON
//	404 → 未启用（路由本就不注册；这里是纵深防御）
//	409 → 版本冲突（storage.ErrConflict：Doc.Revision>0 且与库内现存不一致——先 Get 拿最新版本再写回）
//	500 → 写入失败
//
// 写经 DocStore.Transact 收口事务（G17 单写者：写只经宿主这一个 ingest 入口，
// 与 IngestVectors 同口径）。Revision 语义照契约：不自动分配、不自动盖时间戳。
func (h *Hub) serveDocUpsert(w http.ResponseWriter, r *http.Request) {
	if h.docs == nil {
		writeJSONError(w, http.StatusNotFound, "documents_disabled",
			"文档族存储未启用（Config.DocumentsEnabled=false；/api/docs/upsert 不注册）")
		return
	}
	var body docsUpsertRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxDocsBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body",
			"请求体不是合法 JSON 对象（应形如 {\"docs\":[{\"collection\":\"…\",\"key\":\"…\",…}]}）: "+err.Error())
		return
	}
	// G17 写面单写者：写只经宿主这一个 ingest 入口（Transact = BeginTx → fn → Commit/Rollback
	// 收口；与 IngestVectors 同口径——HTTP 调用方拿不到裸 Tx）。
	err := h.docs.Transact(r.Context(), func(tx coregraph.Tx) error {
		return h.docs.Upsert(r.Context(), tx, body.Docs)
	})
	if err != nil {
		writeDocsHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]bool{"ok": true})
}

// docsDeleteRequest 是 POST /api/docs/delete 的请求形状（小 struct，不造多余 view）。
type docsDeleteRequest struct {
	Collection string   `json:"collection"`
	Keys       []string `json:"keys"`
}

// serveDocDelete 服务 POST /api/docs/delete（写面删除入口；幂等）。
//
//	请求体：{"collection":"…","keys":["k1","k2"]}
//	200 → {"ok":true}（幂等：不存在的键天然跳过，不算冲突）
//	400 → 体不是合法 JSON
//	404 → 未启用（路由本就不注册；这里是纵深防御）
//	500 → 删除失败
//
// 写经 DocStore.Transact 收口事务（G17 单写者，与 upsert 同口径）。
func (h *Hub) serveDocDelete(w http.ResponseWriter, r *http.Request) {
	if h.docs == nil {
		writeJSONError(w, http.StatusNotFound, "documents_disabled",
			"文档族存储未启用（Config.DocumentsEnabled=false；/api/docs/delete 不注册）")
		return
	}
	var body docsDeleteRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxDocsBody)).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_body",
			"请求体不是合法 JSON 对象（应形如 {\"collection\":\"…\",\"keys\":[\"k1\"]}）: "+err.Error())
		return
	}
	// G17 写面单写者：写只经宿主这一个 ingest 入口（同 upsert）。
	err := h.docs.Transact(r.Context(), func(tx coregraph.Tx) error {
		return h.docs.Delete(r.Context(), tx, body.Collection, body.Keys)
	})
	if err != nil {
		writeDocsHubError(w, err)
		return
	}
	writeJSONBody(w, http.StatusOK, map[string]bool{"ok": true})
}

// writeDocsHubError 把文档面（Upsert / Delete / Get / List）的错误映射成 HTTP 状态码。
//
// 口径：**按错误种类分**，不一律 500——照 writeVectorHubError 形态：
//   - 版本冲突（storage.ErrConflict：先 Get 拿最新版本再写回）→ 409（客户端能改：刷新版本重试）；
//   - 不存在（storage.ErrNotFound）→ 404；
//   - 查询非法（storage.ErrBadQuery：负 limit）→ 400（客户端能改）；
//   - 其余 → 500（真出事了）。
func writeDocsHubError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrConflict):
		writeJSONError(w, http.StatusConflict, "doc_conflict", err.Error())
		return
	case errors.Is(err, storage.ErrNotFound):
		writeJSONError(w, http.StatusNotFound, "doc_not_found", err.Error())
		return
	case errors.Is(err, storage.ErrBadQuery):
		writeJSONError(w, http.StatusBadRequest, "bad_doc_query", err.Error())
		return
	}
	writeJSONError(w, http.StatusInternalServerError, "doc_failed", err.Error())
}
