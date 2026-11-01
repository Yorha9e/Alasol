package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	coregraph "github.com/Yorha9e/alasol/core/graph"
	"github.com/Yorha9e/alasol/core/storage"
	"github.com/Yorha9e/alasol/runtime/graph"
)

// 编译期断言：*DocStore 同时实现文档族的读面与写面（G2：接口在 core，实现在 runtime；
// 消费方拿 storage.Documents / storage.DocumentProjection 说话，换实现零改动）。
var (
	_ storage.Documents          = (*DocStore)(nil)
	_ storage.DocumentProjection = (*DocStore)(nil)
)

// DocStore 是文档族（G2 文档投影）的 SQLite 实现：单表 docs，(collection, key) 主键寻址。
//
// # 为什么是一张表而不是五张
//
// 文档形状归消费方（框架不解释 Body），存储侧只提供"按 (集合, 键) 存取 + 可列举"。
// 一张表 + 复合主键就是全部寻址模型；按集合分表只会把"新增一个集合"变成一次 DDL
// （索引器的写面不该有建表副作用，F6/G2b：DDL 只在 EnsureSchema）。
//
// # 原样存取纪律
//
// Revision / UpdatedSeq / UpdatedAt **不自动分配、不自动盖时间戳**——调用方给什么存什么
// （core/storage/document.go 的字段语义：Revision 归冲突判定、UpdatedSeq 是"落后事件流多少"
// 的诚实信号、UpdatedAt 是诊断）。实现若偷偷分配版本，"调用方先 Get 再按最新版本写回"的
// 冲突口径就无从谈起（LWW 只发生在 revision 相同或为 0 时，契约明文）。
//
// # 并发安全（真实不变量，不是约定）
//
// 结构体只持一个建表/读句柄（dbQuerier 包 *sql.DB），**无可变状态**——并发读由
// database/sql 连接池保证（契约：宿主会从多个 HTTP handler 并发读）。
//
// 写面的串行化**不靠"单写者纪律"这类约定**，而靠两件结构事实：
//   - 版本检查（库内现存版本）与写语句**都在调用方那一个事务里**（同一连接、同一快照：
//     检查看得见本事务未提交的写）；
//   - DSN 带 `_txlock=immediate`（见 runtime/store/sqlite 的 buildDSN）→ **BeginTx 即拿写锁**
//     → "检查 + 写"全程串行：并发多客户端 upsert 同一键时，后拿锁的那个读到的已经是前一个
//     事务提交后的版本 → 冲突被如实判出（而不是两个都"检查通过"后互相覆盖）。
//
// 这也是"并发多客户端 upsert 同键 = 1×200 + 5×409"这类结果的来源：第一个成功，
// 其余版本不符 → 409（不是"最后写赢"）。
type DocStore struct {
	// q 是读与建表句柄（同包复用 dbQuerier：外部包始终拿不到裸 *sql.DB，G17 同款纪律）。
	//
	// 注意它**不是**调用方的写事务：**Upsert 的库内版本检查不走这里**（走调用方 Tx ——
	// 见 Upsert 的可见性声明）；q 只服务读面（Get/List）与 DDL（EnsureSchema）。
	q dbQuerier
	// db 是本存储自持的连接池句柄，只给 Transact 开事务用（宿主 ingest 面没有
	// 自己的事务跑道——见 Transact）。写语句仍只经 core/graph.Tx 执行（G17 同款纪律：
	// 外部包拿不到裸句柄）。
	db *sql.DB
}

// NewDocStore 建文档族存储（读面 + 写面）。st 为 nil 报错（与 NewGraphProjection 口径一致）。
//
// 表不会自动建：调用方（索引器/宿主）显式调 EnsureSchema（F6/G2b：DDL 归实现侧、
// 也由实现侧决定事务边界——不塞进任何调用方的投影事务）。
func NewDocStore(st *Store) (*DocStore, error) {
	if st == nil {
		return nil, errors.New("sqlite: NewDocStore 需要非 nil 的 Store")
	}
	return &DocStore{q: dbQuerier{db: st.db}, db: st.db}, nil
}

// ---------------------------------------------------------------------------
// SQL（唯一出处：写面与读面共用，不许各写一份）
// ---------------------------------------------------------------------------

// docSelectCols 是读面的列清单（与 Doc 字段一一对应）。
//
// body 是 BLOB：json.RawMessage 的**原始字节**原样存取——框架不解释正文形状
// （document.go：文档形状归消费方），所以不做 TEXT 往返（TEXT 会过 UTF-8 校验/改写路径，
// BLOB 才是"字节进字节出"）。NULL 读回 nil（缺省正文）。
//
// collection/key 走 TEXT 默认的 BINARY 排序规则 = 逐字节比较 = Go string 的 < 语义
// （陷阱 17 纪律：大小写敏感、字节精确，"Ab" 与 "ab" 是两个键——不许 COLLATE NOCASE）。
//
// ORDER BY collection, key 与主键 (collection, key) 的索引序一致 → **带 collection 过滤的
// 列举走主键区间扫**（无额外排序）；**prefix-only 的列举走 key 索引**（idx_docs_key，见
// docSchemaSQL）区间扫，但 key 索引序 ≠ (collection, key) 序 → 会 USE TEMP B-TREE
// FOR ORDER BY（EXPLAIN QUERY PLAN 实测）——定位仍是索引区间、不是全表扫，代价多一层
// O(m log m) 临时排序（m = 命中行数）。
const docSelectCols = `SELECT collection, key, body, revision, updated_seq, updated_at FROM docs`

// docSchemaSQL 建文档投影表（幂等；列语义见 docSelectCols 与 Doc 字段注释）。
//
// idx_docs_key 是**单列 key 索引**：给"只按前缀列举"（List 不带 collection 过滤）用——
// 主键 (collection, key) 的左前缀是 collection，前缀过滤落在第二列上时主键区间扫用不上，
// 有了这个单列索引就能直接按 key 做区间扫（`key >= ? AND key < ?`）。
//
// 幂等：CREATE INDEX IF NOT EXISTS（老库重跑 EnsureSchema 自动补上，不用写迁移脚本）。
const docSchemaSQL = `
CREATE TABLE IF NOT EXISTS docs (
  collection  TEXT    NOT NULL,
  key         TEXT    NOT NULL,
  body        BLOB,
  revision    INTEGER NOT NULL DEFAULT 0,
  updated_seq INTEGER NOT NULL DEFAULT 0,
  updated_at  TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY (collection, key)
);
CREATE INDEX IF NOT EXISTS idx_docs_key ON docs(key);
`

// docUpsertSQL：按主键 upsert。Revision 冲突已在调用方事务**之前**整批校验完
// （见 Upsert），这里不再判断——SQL 层是纯覆盖（后者覆盖前者，含批内同键多条的写序）。
const docUpsertSQL = `INSERT INTO docs (collection, key, body, revision, updated_seq, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(collection, key) DO UPDATE SET
    body = excluded.body, revision = excluded.revision,
    updated_seq = excluded.updated_seq, updated_at = excluded.updated_at`

const docDeleteSQL = `DELETE FROM docs WHERE collection = ? AND key = ?`

const docClearSQL = `DELETE FROM docs`

// ---------------------------------------------------------------------------
// 读面（Documents）
// ---------------------------------------------------------------------------

// Get 取一条文档；不存在 → ErrNotFound（错误形态与契约替身一致：`doc <collection>/<key>`）。
func (d *DocStore) Get(ctx context.Context, collection, key string) (storage.Doc, error) {
	var (
		doc  storage.Doc
		body []byte
	)
	err := d.q.QueryRowContext(ctx, docSelectCols+` WHERE collection = ? AND key = ?`, collection, key).
		Scan(&doc.Collection, &doc.Key, &body, &doc.Revision, &doc.UpdatedSeq, &doc.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.Doc{}, fmt.Errorf("%w: doc %s/%s", storage.ErrNotFound, collection, key)
	}
	if err != nil {
		return storage.Doc{}, fmt.Errorf("sqlite: 读文档 %s/%s: %w", collection, key, err)
	}
	doc.Body = body // BLOB 原样；NULL → nil（保留缺省正文语义）
	return doc, nil
}

// List 列举文档：按 (collection, key) 升序（SQLite BINARY = 字节序 = Go string 比较，确定性输出）。
//
// Limit 语义（契约 + 替身对齐）：
//   - 负值 → ErrBadQuery（契约明文；替身没查这条是替身宽松，实现守契约）；
//   - 0 = 不截断（契约"0 = 实现默认"，本实现取"全返回"——与 mapDocs/logDocs 一致）。
//
// Prefix 是**字节前缀、大小写敏感**（陷阱 17）。不许用 SQL LIKE：SQLite 的 LIKE 对 ASCII
// 大小写不敏感，"Ab" 会串到 "ab…"——字节语义就毁了。做法：Go 里算前缀的字节上界
// （prefixUpperBound），SQL 用 `key >= ? AND key < ?` 范围扫——逐字节精确。
//
// 走的哪条索引：**带 collection 过滤**时按主键 (collection, key) 区间扫（排序免费）；
// **prefix-only**（不带 collection）时按单列 key 索引（idx_docs_key）区间扫 + 临时
// B-tree 按 (collection, key) 排序（见 docSelectCols 注释的实测口径）。
func (d *DocStore) List(ctx context.Context, q storage.DocQuery) ([]storage.Doc, error) {
	if q.Limit < 0 {
		return nil, fmt.Errorf("%w: 负 Limit %d", storage.ErrBadQuery, q.Limit)
	}
	var sb strings.Builder
	sb.WriteString(docSelectCols)
	var args []any
	if q.Collection != "" {
		sb.WriteString(" WHERE collection = ?")
		args = append(args, q.Collection)
	}
	if q.Prefix != "" {
		if len(args) == 0 {
			sb.WriteString(" WHERE ")
		} else {
			sb.WriteString(" AND ")
		}
		sb.WriteString("key >= ?")
		args = append(args, q.Prefix)
		if upper, ok := prefixUpperBound(q.Prefix); ok {
			// 有上界才加 `<`：全 0xFF 前缀没有后继字节串，只有下界。
			sb.WriteString(" AND key < ?")
			args = append(args, upper)
		}
	}
	sb.WriteString(" ORDER BY collection, key")
	if q.Limit > 0 {
		sb.WriteString(" LIMIT ?")
		args = append(args, q.Limit)
	}
	rows, err := d.q.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: 列举文档: %w", err)
	}
	defer rows.Close()
	var out []storage.Doc
	for rows.Next() {
		var (
			doc  storage.Doc
			body []byte
		)
		if err := rows.Scan(&doc.Collection, &doc.Key, &body, &doc.Revision, &doc.UpdatedSeq, &doc.UpdatedAt); err != nil {
			return nil, fmt.Errorf("sqlite: 扫文档行: %w", err)
		}
		doc.Body = body
		out = append(out, doc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: 读文档行: %w", err)
	}
	return out, nil
}

// prefixUpperBound 算字节前缀的**独占上界**：把最后一个非 0xFF 字节 +1、截断其后。
//
// 语义：`prefix <= key < upper` 恰好圈住"以 prefix 开头"的全体字节串
// （BINARY 排序下与 strings.HasPrefix 等价）。全 0xFF 前缀没有后继字串 → ok=false，
// 调用方只写下界（key >= prefix 本身已是"前缀匹配"的充要条件）。
//
// 例："Ab" → "Ac"；"k\xff" → "l"；"\xff\xff" → 无上界。
func prefixUpperBound(prefix string) (upper string, ok bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			out := make([]byte, i+1)
			copy(out, b[:i+1])
			out[i]++
			return string(out), true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// 写面（DocumentProjection）
// ---------------------------------------------------------------------------

// EnsureSchema 建文档投影表（幂等，IF NOT EXISTS）。
//
// DDL 自建、不走调用方事务（F6/G2b：core 契约里没有 SQL 文本，也不替调用方决定
// 建表落在哪个事务里）——与图投影写面（runtime/graph.Projection.EnsureSchema）同款口径。
func (d *DocStore) EnsureSchema(ctx context.Context) error {
	if _, err := d.q.ExecContext(ctx, docSchemaSQL); err != nil {
		return fmt.Errorf("sqlite: 建文档表: %w", err)
	}
	return nil
}

// Upsert 在调用方事务里写入/更新一批文档。口径：**整批拒绝**（契约 + mapDocs/logDocs 基准）。
//
// 步骤（检查全在写之前完成 → 冲突批次零写入，K24"宁可全拒不可半成"）：
//
//  1. 批内检查（纯 Go）：同键条目按出现顺序维护 pending（后者覆盖前者）。微妙的不对称
//     （照抄 mapDocs.checkBatch，不要"修正"成对称）：
//     - [rev0, rev2] → **冲突**：rev0 先入 pending，rev2>0 且 ≠0；
//     - [rev2, rev0] → **不冲突**：rev0 那条 doc.Revision>0 不成立，pending 被 rev0 覆盖，
//     整批按 LWW（rev0 = 免检）放行，写序后者覆盖前者。
//  2. 库内版本检查：对 pending 每个 key 查现存行；doc.Revision>0 且现存 ≠ → ErrConflict。
//     **不存在 = 无冲突**（删除后写回允许——logDocs 墓碑同口径：删除 = 没有现存版本）；
//     doc.Revision==0 = LWW 免检。
//  3. 按 docs 入参**原顺序**逐条 upsert（同键多条后者覆盖前者，与替身写序一致）。
//     任何一步出错立即返回（调用方回滚整个事务——写面不替调用方吞错）。
//
// # 步骤 2 走**调用方事务**（可见性口径）
//
// 库内版本检查**经传入的 tx 读**（txQuerier.QueryContext），**不走实现自持的读句柄**——
// 同一连接、同一事务快照 → **看得见本事务里尚未提交的写**。
//
// 为什么必须这样：检查若走自持读句柄（读已提交态），"同一事务里先写 rev1、再写 rev2
// 同键"这条路径**看不见未提交的 rev1** → 版本检查被架空 → rev2 静默落库（冲突被吞）。
// 走 tx 之后这条路径如实判 ErrConflict（回归测试见 docs_test.go 的
// TestDocStoreUpsertSameTxVersionConflict）。
//
// # 能力要求（fail-close，不静默退回旧读法）
//
// 检查需要 tx 具备 **QueryContext 能力**（txQuerier）：
//   - `graph.SQLTx(tx)`（真 *sql.Tx）满足 → 正常路径；
//   - 纯 Exec 的 Tx（如 graph.SQLExec）**不满足** → **显式报错**（不是"检查不了就当没冲突"）。
//
// 只有当这一批**全是 revision=0**（LWW 免检，根本不需要查库）时，任何 Tx 都能写——
// 那条路径不做版本检查，不需要 QueryContext。
func (d *DocStore) Upsert(ctx context.Context, tx coregraph.Tx, docs []storage.Doc) error {
	if len(docs) == 0 {
		return nil
	}
	// ① 先整批校验（批内 + 与库里现存版本），② 再整批写。
	if err := d.checkBatch(ctx, tx, docs); err != nil {
		return err
	}
	for _, doc := range docs {
		// Body 原样：非 nil 走 BLOB 字节；**nil 要显式落 NULL**——driver 会把
		// nil []byte 存成零长 BLOB（spike 实测），读回 []byte{} 而不是 nil，
		// 破坏"NULL 读回 nil"的缺省正文语义。untyped nil 才被 database/sql 绑成 NULL。
		var body any
		if doc.Body != nil {
			body = []byte(doc.Body)
		}
		if err := tx.Exec(ctx, docUpsertSQL,
			doc.Collection, doc.Key, body, doc.Revision, doc.UpdatedSeq, doc.UpdatedAt,
		); err != nil {
			return fmt.Errorf("sqlite: 写文档 %s/%s: %w", doc.Collection, doc.Key, err)
		}
	}
	return nil
}

// docKey 是文档的**复合主键**（collection + key）。
//
// **为什么是结构体而不是拼字符串**（F3）：`collection + "\x00" + key` 这种拼法**会撞键**——
// ("a\x00b", "c") 与 ("a", "b\x00c") 拼出来是同一个字符串 → 两条**不同的文档**被当成同一条：
// 版本检查互相顶掉、pending 里后者覆盖前者 → 一条文档的版本冲突被静默吞掉。
// 结构体键没有这种歧义（两段分别比较，NUL 只是普通字节）。
type docKey struct {
	collection string
	key        string
}

// txQuerier 是"能在**调用方事务里**跑查询"的最小能力面（**可选能力**，不是扩 core 契约）。
//
// core/graph.Tx 刻意只留 Exec（G2b：不泄漏引擎特性；N5 拍板保留 raw-exec 形状）——
// 本地接口让实现侧能对传入的 Tx 做**类型断言**：断言过 → 检查走事务（同连接、看得见未提交写）；
// 断言不过 → **显式报错**（fail-close），绝不静默退回"看不见未提交写"的读法。
//
// 真 SQL 事务（runtime/graph.SQLTx 包 *sql.Tx）天然满足（*sql.Tx 有 QueryContext）。
type txQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// errNoQueryCap 是"调用方事务读不回自己"的错误文案（fail-close）。
// 错误包 storage.ErrUnsupported 哨兵——调用方可 errors.Is 区分"能力缺失"（换 Tx 形态
// 可解）与真故障（SQL/IO 错），而不是一律当 500。
const errNoQueryCap = "版本检查需要能读回调用方事务的 Tx（QueryContext 能力）；" +
	"纯 Exec Tx 只支持 revision=0 的 LWW 批次；宿主写路径请用 DocStore.Transact"

// checkBatch 校验一批文档的写入口径（批内 + 与库现存版本；语义与 mapDocs.checkBatch 逐条同）。
//
// 库内检查**经传入的 tx 读**（见 Upsert 的可见性口径）：同一事务快照 → 看得见本事务未提交的写。
//
// 库内检查按 (collection, key) 排序遍历 pending：冲突**有无**与遍历序无关（整体性判定），
// 排序只是让"多处同时冲突"时的报错可复现（mapDocs 遍历 map，序不定——那是替身的松，
// 不是语义）。
func (d *DocStore) checkBatch(ctx context.Context, tx coregraph.Tx, docs []storage.Doc) error {
	pending := make(map[docKey]storage.Doc, len(docs))
	for _, doc := range docs {
		k := docKey{collection: doc.Collection, key: doc.Key}
		if prev, ok := pending[k]; ok && doc.Revision > 0 && prev.Revision != doc.Revision {
			return fmt.Errorf("%w: 批内 doc %s/%s 版本 %d ≠ %d",
				storage.ErrConflict, doc.Collection, doc.Key, doc.Revision, prev.Revision)
		}
		pending[k] = doc
	}
	// 库里要查哪些键：**只有 revision>0 的那几条**（revision=0 = LWW 免检，压根不查库）。
	keys := make([]docKey, 0, len(pending))
	needCheck := false
	for k, doc := range pending {
		if doc.Revision == 0 {
			continue
		}
		needCheck = true
		keys = append(keys, k)
	}
	if !needCheck {
		return nil // 整批 revision=0：LWW 免检，任何 Tx 都能写（不查库 → 不需要 QueryContext）
	}
	// 查库必须经**调用方事务**（看得见本事务未提交的写）；Tx 没有这个能力 → 显式报错
	// （包 ErrUnsupported 哨兵：能力缺失可程序化区分于真故障）。
	q, ok := tx.(txQuerier)
	if !ok {
		return fmt.Errorf("sqlite: %w: %s", storage.ErrUnsupported, errNoQueryCap)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].collection != keys[j].collection {
			return keys[i].collection < keys[j].collection
		}
		return keys[i].key < keys[j].key
	})
	for _, k := range keys {
		doc := pending[k]
		cur, found, err := queryRevision(ctx, q, k.collection, k.key)
		if err != nil {
			return fmt.Errorf("sqlite: 读文档 %s/%s 现存版本: %w", k.collection, k.key, err)
		}
		if !found {
			continue // 不存在 = 无冲突（删除后写回允许）
		}
		if cur != doc.Revision {
			return fmt.Errorf("%w: doc %s/%s 版本 %d ≠ %d",
				storage.ErrConflict, k.collection, k.key, doc.Revision, cur)
		}
	}
	return nil
}

// queryRevision 在调用方事务里读一条文档的现存版本：found=false 表示**不存在**
// （不存在 = 没有现存版本 = 无冲突；删除后写回允许）。
func queryRevision(ctx context.Context, q txQuerier, collection, key string) (rev int64, found bool, err error) {
	rows, err := q.QueryContext(ctx,
		`SELECT revision FROM docs WHERE collection = ? AND key = ?`,
		collection, key,
	)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, err
		}
		return 0, false, nil // 无行 = 不存在
	}
	if err := rows.Scan(&rev); err != nil {
		return 0, false, err
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	return rev, true, nil
}

// Delete 在调用方事务里删除一批文档。不存在 → 天然幂等跳过（DELETE 无行可删，不算冲突）。
func (d *DocStore) Delete(ctx context.Context, tx coregraph.Tx, collection string, keys []string) error {
	for _, key := range keys {
		if err := tx.Exec(ctx, docDeleteSQL, collection, key); err != nil {
			return fmt.Errorf("sqlite: 删文档 %s/%s: %w", collection, key, err)
		}
	}
	return nil
}

// Clear 清空文档投影（索引器 Rebuild 用，G24②）。在调用方事务里执行（与重放原子）。
func (d *DocStore) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, docClearSQL); err != nil {
		return fmt.Errorf("sqlite: 清空文档表: %w", err)
	}
	return nil
}

// Transact 在本存储自持的句柄上开一个真实事务执行 fn（提交/回滚由本方法收口）。
//
// 为什么需要它：写面（Upsert/Delete/Clear）的契约签名带**调用方的** graph.Tx——索引器
// 路径天然有自己的投影事务；宿主 ingest 面（/api/docs/*）没有。本方法给那类调用方一条
// 事务跑道：BeginTx → graph.SQLTx 适配 → fn → fn 报错则 Rollback、否则 Commit。
//
// fn 内仍只经传入的 Tx 写（写语句不走 d.q）；版本检查（Upsert 的 checkBatch）**也经这个
// Tx**（同连接、看得见本事务未提交的写——见 Upsert 的可见性口径）。fn 返回的错误原样交还
// （含 storage.ErrConflict 等哨兵，errors.Is 可判）；BeginTx/Commit 失败包 sqlite 前缀返回。
//
// # 事务收口的三条纪律
//
//  1. **defer Rollback 兜底**（F4）：BeginTx 之后立刻 `defer tx.Rollback()`——fn **panic**
//     时 defer 仍会跑 → 事务回滚 → **写锁释放**。没有这条，panic 会把事务悬着：
//     `_txlock=immediate` 下 BeginTx 已拿写锁 → 悬着的事务把写锁一直占着 → 后续所有写
//     卡到 busy_timeout 超时才失败（"一把锁卡死整库"）。Commit 成功后 Rollback 返
//     ErrTxDone，是 database/sql 的常态，无害（与 runtime/store/sqlite 的 Append 同款惯例）。
//  2. **Rollback 错误吞掉**（F11）：回滚失败（连接已断等）没有可恢复的动作——事务已经
//     结束或已失效，错误吞掉、让原始错误（fn 的错 / Commit 的错）原样冒泡才是有用信息。
//  3. **Commit 失败路径无法注入测试**（F11）：本包没有故障注入钩子，Commit 失败只能靠
//     真实的 IO/锁故障触发 → 单元测试覆盖不到；那一条路径靠 defer Rollback 兜底清理
//     （写锁不会悬着），错误文案包 sqlite 前缀便于定位。
func (d *DocStore) Transact(ctx context.Context, fn func(tx coregraph.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: 开文档事务: %w", err)
	}
	// 兜底回滚：fn panic 时也会执行（释放写锁，不把库卡死）。
	defer func() { _ = tx.Rollback() }()
	if err := fn(graph.SQLTx(tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: 提交文档事务: %w", err)
	}
	return nil
}
