// Package graph 是图投影的 SQLite 实现（G2c：**SQLite 的 node/edge 表做持久化 +
// 自己写内存图做多跳**）+ 索引器的图投影写面（G17/G22）。
//
// # 分层（G18）
//
//   - 一跳/两跳 → 直查 SQLite **双向倒排**（`edges(src,kind)` / `edges(dst,kind)`：
//     主键 (src,kind,dst) 管出向，(dst,kind) 索引管入向）。恒查库、不走内存图 →
//     跨进程写入立即可见，也不依赖内存图是否已加载。口径是**恰好 N 跳**（不含一跳邻居），
//     结果有上限（NeighborQuery.Limit，截断有标记，F12）。
//   - 多跳 → 内存图 BFS（core/graph.MemGraph：邻接表 + visited set + 深度/结果上限 +
//     context 取消）。传递闭包在内存图上是 O(V+E)，不用 SQL 递归 CTE。
//
// # 内存图与持久层的关系
//
// 内存图是持久层的**进程内缓存**：冷启动/增量都走 Sync（按 (seq,rowid) 元组流式加载，
// 见 Engine.Sync——单 seq 水位会把"同一个事件的边组"切两半、尾巴永久丢，F1）。
// 写侧只归索引器（G17：派生表的唯一写者）——本包只提供写面 `Projection`（SQL 都在本包），
// 由索引器在它自己的投影事务里调用（投影+水位同事务原子，G24）。
//
// # 一致性口径（诚实声明）
//
//   - 一跳/两跳：**直查库** → 任何进程的写入都立即可见；
//   - 多跳：读内存图 → **同进程**的写入经 `Projection.Committed()` 立即失效闭包缓存，
//     下一次查询增量同步取回；**跨进程**的写入要等一次 Sync（宿主在自然的 catch-up
//     点调用它，如一轮 Run 之后）对多跳可见。这与 G9"派生索引最终一致、毫秒级延迟"一致。
//
// # 依赖方向
//
// 本包 import core/graph（接口 + 纯逻辑）与 contracts；**不 import runtime/indexer**
// （索引器只认 core/graph 的形状，不 import 本包——依赖单向，无环）。
package graph

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Yorha9e/alasol/contracts"
	coregraph "github.com/Yorha9e/alasol/core/graph"
)

// 编译期断言：*Projection 实现 core/graph.GraphProjection（写面），
// *Engine 实现 core/graph.Graph（读面）与 core/graph.Invalidator（失效通知）。
var (
	_ coregraph.GraphProjection = (*Projection)(nil)
	_ coregraph.Graph           = (*Engine)(nil)
	_ coregraph.Invalidator     = (*Engine)(nil)
)

// ErrRebuilding 是图投影**正在重建**的哨兵（N2，v3-S3 补）：索引器 Rebuild 的清空与
// 重放不是原子的，窗口内投影可能"干净地为空"。读侧引擎据此 fail-close 报错，让消费端
// （escalate 闸门 / V7 冲突预测）明确看到"预测不可用"，**绝不把"图不可用"当成"无冲突"**。
var ErrRebuilding = errors.New("graph: 图投影正在重建（N2：窗口内影响面可能干净为空）→ 影响面/冲突预测不可用，拒绝作答")

// ---------------------------------------------------------------------------
// 类型别名：消费方（host / v3）只 import 本包就能用全链路，不必同时 import core/graph
// ---------------------------------------------------------------------------

type (
	Limits         = coregraph.Limits
	Direction      = coregraph.Direction
	NeighborQuery  = coregraph.NeighborQuery
	NeighborResult = coregraph.NeighborResult
	ReachQuery     = coregraph.ReachQuery
	Closure        = coregraph.Closure
	ImpactRequest  = coregraph.ImpactRequest
	ImpactReport   = coregraph.ImpactReport
	CoreSet        = coregraph.CoreSet
	Thresholds     = coregraph.Thresholds
	DiffStat       = coregraph.DiffStat
	Scale          = coregraph.Scale
	Stats          = coregraph.Stats
)

// 方向常量（core/graph 的别名出口）。
const (
	DirectionOut = coregraph.DirectionOut
	DirectionIn  = coregraph.DirectionIn
)

// Scale 常量。
const (
	ScaleSmall  = coregraph.ScaleSmall
	ScaleMedium = coregraph.ScaleMedium
	ScaleLarge  = coregraph.ScaleLarge
)

// 构造函数/解析器的别名出口（消费方只 import 本包）。
//
// **用函数包装而不是 var 别名**（F15）：`var X = coregraph.X` 导出的是**可变全局**——
// 任何包都能 `graph.ParseCoreSet = nil` 把它改掉（运行时才炸，且难查）。
// 函数包装是不可改的（只能重新赋值整个包级标识符，但那是显式动作）。
func ParseCoreSet(specs []string) coregraph.CoreSet { return coregraph.ParseCoreSet(specs) }

// DefaultLimits 返回默认上界（G19 量级口径）。
func DefaultLimits() coregraph.Limits { return coregraph.DefaultLimits() }

// DefaultThresholds 返回默认 escalate 阈值（W15/W16）。
func DefaultThresholds() coregraph.Thresholds { return coregraph.DefaultThresholds() }

// ---------------------------------------------------------------------------
// 图投影 schema 与 SQL（**唯一出处**：索引器写、引擎读，两边不许各写一份）
// ---------------------------------------------------------------------------

// SchemaSQL 建图投影表（G2c：node/edge 两张表）。
//
// 三处口径：
//   - **COLLATE BINARY**：node id / kind 大小写敏感、字节精确（陷阱 17：不折叠大小写——
//     "Core" 与 "core" 是两个节点，折叠会让影响面判定悄悄放过/错杀）；
//   - 主键 (src, kind, dst)：出向倒排（`WHERE src=? AND kind=?` 命中左前缀）；
//   - (dst, kind) 索引：入向倒排（`WHERE dst=? AND kind=?` 与两跳 join 都靠它）；
//   - **idx_graph_edges_seq**：增量加载的水位扫描（F4）。没有它，每次 Sync 都是
//     全表扫 + TEMP B-TREE 排序（20 万边冷启动 ~1.4s）；有了它是索引区间扫。
//     **老库迁移**：CREATE INDEX IF NOT EXISTS 是幂等的——EnsureSchema 每次启动都跑，
//     老库自动补上这个索引（不用写迁移脚本）。
//
// graph_meta 的 generation 是**重建代**：索引器 Rebuild 清空投影时 +1，读侧引擎据此
// 发现"我加载的是上一代"→ 丢掉内存图从 0 重载。没有它的话，Rebuild 后重放出来的边
// seq 与旧值相同，增量水位 `seq > ?` 永远等不到它们（静默用旧图答新问题）。
//
// graph_meta 的 rebuilding 是**重建中标记**（N2 挂账，v3-S3 补）：索引器 Rebuild 的
// 清空与重放**不是原子的**——窗口内投影可能"干净地为空"（Direct=[]、无错误无告警），
// 消费端分不清"真没影响"与"正在重建"（少报影响面 → escalate 闸门 / V7 冲突预测被蒙）。
// rebuilding=1 时读侧引擎 fail-close 报错（ErrRebuilding），**绝不把"图不可用"当成
// "无冲突/无影响"**。老库经 EnsureSchema 的幂等 ALTER 补上这一列。
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS graph_nodes (
  id TEXT PRIMARY KEY COLLATE BINARY
);
CREATE TABLE IF NOT EXISTS graph_edges (
  src  TEXT    NOT NULL COLLATE BINARY,
  dst  TEXT    NOT NULL COLLATE BINARY,
  kind TEXT    NOT NULL COLLATE BINARY,
  seq  INTEGER NOT NULL,
  PRIMARY KEY (src, kind, dst)
);
CREATE INDEX IF NOT EXISTS idx_graph_edges_dst ON graph_edges(dst, kind);
CREATE INDEX IF NOT EXISTS idx_graph_edges_seq ON graph_edges(seq);
CREATE TABLE IF NOT EXISTS graph_meta (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  generation INTEGER NOT NULL,
  rebuilding INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO graph_meta (id, generation) VALUES (1, 0);
`

// addRebuildingColumnSQL：老库迁移（N2）——给已存在的 graph_meta 补 rebuilding 列。
// 幂等靠"忽略 duplicate column 错"：新库的 SchemaSQL 已含该列 → ALTER 报 duplicate → 忽略。
const addRebuildingColumnSQL = `ALTER TABLE graph_meta ADD COLUMN rebuilding INTEGER NOT NULL DEFAULT 0`

// setRebuildingSQL：设置重建中标记（Rebuild 清空时置 1、重放完成后置 0）。
const setRebuildingSQL = `UPDATE graph_meta SET rebuilding = ? WHERE id = 1`

// bumpGenerationSQL：清空投影时把重建代 +1（Rebuild 用）。
const bumpGenerationSQL = `UPDATE graph_meta SET generation = generation + 1 WHERE id = 1`

// readGenerationSQL：读当前重建代 + 重建中标记（Sync 开头的对齐检查 + N2 fail-close 依据）。
const readGenerationSQL = `SELECT generation, rebuilding FROM graph_meta WHERE id = 1`

// upsertNodeSQL：端点节点随之建立（幂等；节点表是 node/edge 持久化形态的一半，G2c）。
const upsertNodeSQL = `INSERT OR IGNORE INTO graph_nodes (id) VALUES (?)`

// upsertEdgeSQL：边**以首次出现为准**（ON CONFLICT DO NOTHING）。
//
// 为什么是"首次出现"而不是"最新 seq 覆盖"：读侧按 `seq > 水位` 做增量加载，
// 一条边必须在库里**只有一个 seq**，否则同一条边会被重复加载（去重能挡住邻接膨胀，
// 但 EdgeCount 与水位语义会乱）。首次出现的 seq = 这条边进入投影的时刻，语义更干净。
const upsertEdgeSQL = `INSERT INTO graph_edges (src, dst, kind, seq)
VALUES (?, ?, ?, ?)
ON CONFLICT(src, kind, dst) DO NOTHING`

// loadEdgesSQL：增量加载（流式：调用方逐行 Scan，不整表读进内存，G19 冷启动口径）。
//
// **水位是 (seq, rowid) 二元组**，不是单个 seq（F1）：
//
// 单个 seq 做水位有个致命缺口——**同一个事件**的所有边共用一个 seq（写面按事件
// 落 seq）。一个事件产出 6000 条边时，`WHERE seq > ? LIMIT 5000` 会把这个 seq 组
// **切成两半**：第一批取走 5000 条，水位推到该 seq，下一批 `seq > 水位` 就把这组
// 剩下的 1000 条**永久丢掉**（静默：查询看起来正常，影响面悄悄少报）。
//
// 二元组把"同一个 seq 内部"的顺序也定下来（rowid 单调递增，等价于写入顺序）：
// 水位 = (最后一行 seq, 最后一行 rowid)，下一批从它**之后**继续，同 seq 的尾巴
// 不会丢。rowid 隐含在每个索引里 → `idx_graph_edges_seq`（seq → (seq,rowid)）
// 正好覆盖这个范围扫描（实测走索引、无 TEMP B-TREE）。
const loadEdgesSQL = `SELECT seq, src, dst, kind, rowid FROM graph_edges
WHERE (seq, rowid) > (?, ?) ORDER BY seq ASC, rowid ASC LIMIT ?`

// loadBatch 是增量加载的单批行数（流式加载的批大小）。
const loadBatch = 5000

// Projection 是图投影的**写面**（G17：只由索引器调用；G22：边来自工具产出）。
//
// 写边与推水位必须原子（G24），所以**写语句由索引器在它的事务里执行**
// （通过 core/graph.Tx 传进来）；但**建表不塞进调用方的事务**（F6/G2b）——
// DDL 文本与它的事务边界都归实现侧，core 契约里不出现 SQL 文本。
//
// 唯一的状态是失效通知目标（读侧引擎）与建表用的执行句柄，提交成功后由索引器调
// Committed 触发失效。
type Projection struct {
	inv coregraph.Invalidator // 可为 nil（没人接失效通知 = 闭包缓存不会因写入失效）
	// exec 是**建表用**的语句执行面（DDL 自建，不占调用方的事务）。
	exec SQLExecTx
}

// 编译期断言见文件头。

// NewProjection 建写面。inv 是写入后要通知的读侧（通常是 *Engine）；nil = 不通知。
//
// exec 是建表用的执行句柄（*sql.DB / *sql.Conn 都行）；nil = 没有建表能力
// （EnsureSchema 会显式报错——不静默跳过，免得"以为建好了其实没建"）。
func NewProjection(inv coregraph.Invalidator, exec SQLExecTx) *Projection {
	return &Projection{inv: inv, exec: exec}
}

// EnsureSchema 建图投影表（幂等）。由索引器在确保自身 schema 时调用**一次**。
//
// **实现自建表**（F6/G2b）：不收 Tx——DDL 不进调用方的事务（core 契约不含 SQL 文本，
// 也不替别人决定建表落在哪个事务里）。语句是幂等的（CREATE ... IF NOT EXISTS），
// 老库重跑自动补上缺的索引/表。
func (p *Projection) EnsureSchema(ctx context.Context) error {
	if p.exec == nil {
		return errors.New("graph: Projection 缺少建表句柄（NewProjection 的 exec 为 nil）")
	}
	if _, err := p.exec.ExecContext(ctx, SchemaSQL); err != nil {
		return fmt.Errorf("graph: 建图投影表: %w", err)
	}
	// 老库迁移（N2）：给已存在的 graph_meta 补 rebuilding 列。幂等靠忽略 "duplicate
	// column" 错——新库的 SchemaSQL 已含该列，ALTER 会报 duplicate → 忽略即可。
	if _, err := p.exec.ExecContext(ctx, addRebuildingColumnSQL); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("graph: 迁移 graph_meta.rebuilding 列（N2 重建窗口标记）: %w", err)
		}
	}
	return nil
}

// ApplyEdges 在一个事务里写入一批边（端点节点随之建立）。
//
// 空 src/dst/kind 的边被**丢弃**（契约里三个字段都 required；空值没有意义，
// 且不能猜默认 kind——猜了会让查询语义悄悄变形）。
func (p *Projection) ApplyEdges(ctx context.Context, tx coregraph.Tx, seq int64, edges []contracts.GraphEdge) error {
	for _, e := range edges {
		if e.Src == "" || e.Dst == "" || e.Kind == "" {
			continue
		}
		if err := tx.Exec(ctx, upsertNodeSQL, e.Src); err != nil {
			return fmt.Errorf("graph: 写节点 %q: %w", e.Src, err)
		}
		if err := tx.Exec(ctx, upsertNodeSQL, e.Dst); err != nil {
			return fmt.Errorf("graph: 写节点 %q: %w", e.Dst, err)
		}
		if err := tx.Exec(ctx, upsertEdgeSQL, e.Src, e.Dst, e.Kind, seq); err != nil {
			return fmt.Errorf("graph: 写边 %s -%s-> %s: %w", e.Src, e.Kind, e.Dst, err)
		}
	}
	return nil
}

// Clear 清空图投影（索引器 Rebuild 用，G24②）。
//
// 顺带把**重建代 +1**：读侧引擎下次 Sync 发现代数变了就丢掉内存图从 0 重载
// （否则 Rebuild 后重放出的边 seq 与旧值相同，增量水位永远等不到它们）。
//
// **并置 rebuilding=1**（N2）：Rebuild 的清空与重放不是原子的——从这一刻到重放完成，
// 投影可能"干净地为空"。置位让读侧引擎在此期间 fail-close 报错（ErrRebuilding），
// 绝不把"图不可用"当成"无冲突/无影响"。重放完成后由索引器调 MarkRebuilt 置 0。
func (p *Projection) Clear(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, `DELETE FROM graph_edges`); err != nil {
		return fmt.Errorf("graph: 清空边表: %w", err)
	}
	if err := tx.Exec(ctx, `DELETE FROM graph_nodes`); err != nil {
		return fmt.Errorf("graph: 清空节点表: %w", err)
	}
	if err := tx.Exec(ctx, bumpGenerationSQL); err != nil {
		return fmt.Errorf("graph: 推进重建代: %w", err)
	}
	if err := tx.Exec(ctx, setRebuildingSQL, 1); err != nil {
		return fmt.Errorf("graph: 置重建中标记（N2）: %w", err)
	}
	return nil
}

// MarkRebuilt 清**重建中标记**（rebuilding=0；索引器 Rebuild 重放完成后调用，N2）。
//
// 必须在重放追平后调——早调（还在半满投影时）会让读侧把"部分图"当"完整图"答题。
// 崩溃遗留的 rebuilding=1 是 fail-close（安全侧）：恢复靠重跑一次成功的 Rebuild。
func (p *Projection) MarkRebuilt(ctx context.Context, tx coregraph.Tx) error {
	if err := tx.Exec(ctx, setRebuildingSQL, 0); err != nil {
		return fmt.Errorf("graph: 清重建中标记（N2）: %w", err)
	}
	return nil
}

// Committed 在索引器**提交成功**一个写了边的批次后调用 → 通知读侧失效闭包缓存（G20）。
func (p *Projection) Committed() {
	if p.inv != nil {
		p.inv.Invalidate()
	}
}

// ---------------------------------------------------------------------------
// SQL 事务适配（*sql.Tx / 语句执行面 → core/graph.Tx）
// ---------------------------------------------------------------------------

// SQLTx 把 *sql.Tx 适配成 core/graph.Tx（写面在调用方事务里写边时用）。
//
// core/graph.Tx 不泄漏引擎特性（G2b）：换一个图存储时 core 侧一行不改，
// 只需要在这里换一个适配器。
func SQLTx(tx *sql.Tx) coregraph.Tx { return sqlTx{tx: tx} }

type sqlTx struct{ tx *sql.Tx }

func (t sqlTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, query, args...)
	return err
}

// QueryContext 在**调用方事务里**跑一条查询（读回本事务未提交的写）。
//
// # 为什么加这个方法（additive，不扩 core/graph.Tx）
//
// core/graph.Tx 刻意只留 `Exec`（G2b：不泄漏引擎特性，N5 拍板保留 raw-exec 形状）——
// 但有些写面需要**在同一个事务里读回自己刚写的行**做版本/冲突检查
// （runtime/store/sqlite.DocStore 的 Upsert：库内现存版本检查必须看得见本事务的未提交写，
// 否则"同事务二次 Upsert 不同版本"的冲突会被静默吞掉）。
//
// 实现侧的做法是**可选能力面**：写面自己声明一个局部接口
// （`QueryContext(ctx, query, args...) (*sql.Rows, error)`）并对传入的 Tx 做类型断言——
// 断言不过就**显式报错**（fail-close），绝不静默退回"看不见未提交写"的旧读法。
// 这样 core 契约零改动（core/graph.Tx 形状不动），而真实 SQL 事务（*sql.Tx）天然满足。
func (t sqlTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

// SQLExecTx 是"能执行语句的面"（建表用：DDL 幂等，无需事务边界）。
//
// 它也是 Projection 自建表的句柄类型（F6：DDL 不塞进调用方事务）。
type SQLExecTx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// SQLExec 返回把 e 适配成 core/graph.Tx 的包装（给"只有执行面、没有事务"的调用方用）。
func SQLExec(e SQLExecTx) coregraph.Tx { return sqlExecTx{e: e} }

type sqlExecTx struct{ e SQLExecTx }

func (t sqlExecTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := t.e.ExecContext(ctx, query, args...)
	return err
}

// ---------------------------------------------------------------------------
// 一跳/两跳 SQL（G18：双向倒排）
// ---------------------------------------------------------------------------

// neighborQuery 组装一跳/两跳的倒排查询。
//
// 起点本身**不出现在结果里**（自环与"绕回起点"的环都被 `<> ?` 滤掉）——
// 查询语义是"除我之外还有谁"（与内存图 BFS 同口径，两边答案才一致）。
//
// **口径：恰好 N 跳**（F8）——hops=2 的结果是"正好隔两层"的节点，**不含**一跳邻居
// （除非它同时也是两跳邻居）。这与 Reachable 的"≤ MaxDepth"口径**不同**：
// 要"depth 以内"用 Reachable（见 core/graph.ReachQuery.MaxDepth 的注释）。
//
// fetch 是**实际要取的行数**（= 结果上限 + 1，多取一行用来判截断；上限取到
// maxInt 时不 +1，避免溢出成"无上限"，见 neighborFetchLimit）。
func neighborQuery(hops int, dir Direction, node, kind string, fetch int) (string, []any) {
	var sb strings.Builder
	var args []any
	// 结果列与"起点落在哪个列上"由方向决定（出向看 src→dst，入向看 dst→src）。
	col, startCol := "dst", "src"
	if dir == DirectionIn {
		col, startCol = "src", "dst"
	}
	// 两跳要给第二张表起别名 e2；一跳直接查本表（无别名）。
	tbl, res := "graph_edges", col
	if hops == 2 {
		tbl, res = "graph_edges e2", "e2."+col
	}
	switch hops {
	case 1:
		sb.WriteString("SELECT DISTINCT ")
		sb.WriteString(res)
		sb.WriteString(" FROM ")
		sb.WriteString(tbl)
		sb.WriteString(" WHERE ")
		sb.WriteString(startCol)
		sb.WriteString(" = ?")
		args = append(args, node)
		if kind != "" {
			sb.WriteString(" AND kind = ?")
			args = append(args, kind)
		}
	case 2:
		sb.WriteString("SELECT DISTINCT ")
		sb.WriteString(res)
		sb.WriteString(" FROM graph_edges e1 JOIN ")
		sb.WriteString(tbl)
		sb.WriteString(" ON e2.")
		sb.WriteString(startCol)
		sb.WriteString(" = e1.")
		sb.WriteString(col)
		if kind != "" {
			sb.WriteString(" AND e2.kind = e1.kind")
		}
		sb.WriteString(" WHERE e1.")
		sb.WriteString(startCol)
		sb.WriteString(" = ?")
		args = append(args, node)
		if kind != "" {
			sb.WriteString(" AND e1.kind = ?")
			args = append(args, kind)
		}
	}
	// 滤掉起点自身（自环 / 回到起点的环）。
	sb.WriteString(" AND ")
	sb.WriteString(res)
	sb.WriteString(" <> ?")
	args = append(args, node)
	sb.WriteString(" ORDER BY ")
	sb.WriteString(res)
	// 结果上限：多取一行 → 调用方据此置"截断"标记（F12）。
	sb.WriteString(" LIMIT ?")
	args = append(args, fetch)
	return sb.String(), args
}

// maxInt 是本平台 int 的最大值（邻居结果上限的溢出判据，N4）。
const maxInt = int(^uint(0) >> 1)

// neighborFetchLimit 算"实际要取多少行"。
//
// 正常做法是**多取一行**（limit+1）：多出来的那行就是"确实被上限截断了"的证据
// （F12）。但 limit == maxInt 时 limit+1 会**溢出成负数**——SQLite 把负 LIMIT
// 当成"**无上限**"（N4）：超级节点护栏被悄悄绕过。
//
// 溢出分支：直接按 limit 取（不溢出、不变成"无上限"），并用"取满 = 可能还有更多"
// 保守判截断（拿不到第 limit+1 行就无法精确区分"刚好取满"与"被截断"——保守报
// 截断，对 escalate 闸门是偏安全的一侧）。
//
// 返回 (要取的行数, 是否走了溢出分支)。
func neighborFetchLimit(limit int) (fetch int, overflow bool) {
	if limit < maxInt {
		return limit + 1, false
	}
	return limit, true
}

// errNoTable 是"图投影表不存在"的判定串（图未启用 / 索引器还没建表）。
//
// 为什么不当成空图：静默把"没建表"当成"图是空的"会让调用方以为"没有影响面"——
// 对 escalate 闸门那是危险答案。所以这里显式报错，让调用方看见。
const errNoTable = "no such table"

// isNoTableErr 判定错误是否是"表不存在"。
func isNoTableErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNoTable)
}

// Querier 是引擎读持久层所需的最小面（G17 同款纪律：**不暴露裸 *sql.DB**）。
//
// 生产装配由 runtime/store/sqlite 提供（见 sqlite.NewGraphEngine）；测试可以包一层
// 计数的 Querier 来断言"缓存命中时不落库"。
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Engine 是 core/graph.Graph 的 SQLite 实现：倒排直查 + 内存图多跳 + 闭包缓存。
//
// 并发：Sync 与内存图变更串行（syncMu + MemGraph 自身锁）；查询走 MemGraph 读锁，
// 多个 BFS 可并发。缓存自身持锁。
type Engine struct {
	q     Querier
	mem   *coregraph.MemGraph
	cache *coregraph.ClosureCache

	limits coregraph.Limits
	syncMu sync.Mutex

	// generation 是本引擎已加载的重建代（与持久层 graph_meta 对齐；不等就整体重载）。
	generation int64
	// applyGen 是**内存图**的代号：每次往内存图灌进新边就 +1。
	//
	// 用途（F3 的另一半护栏）：读侧算闭包时记下 applyGen，算完比对——不一致说明
	// "算的期间内存图又变了"（另一个并发查询同步进了新边）→ 这份闭包可能过期，
	// 不写回缓存。缓存条目的代号（epoch）管的是"**外部**写入"，两者合起来
	// 才盖住"读旧值 → 写新值"的竞态窗口。
	applyGen atomic.Uint64
	// loadedSeq / loadedRow 是增量加载的水位**二元组**（seq, rowid）：F1——
	// 单个 seq 做水位会把"同一个事件的边组"切两半，尾巴永久丢。
	// 两个分量都用原子：Stats 会在别的 goroutine 里读（-race 干净）。
	loadedSeq atomic.Int64
	loadedRow atomic.Int64
	syncs     atomic.Int64

	// rebuilding 是持久层 graph_meta 的"重建中"标记（N2，v3-S3 补）：索引器 Rebuild
	// 清空投影时置 1、重放完成置 0。读侧每次 Sync 刷新它；为 true 时 Reachable fail-close
	// 报 ErrRebuilding（绝不把"图不可用/半图"当成"无冲突/无影响"）。
	rebuilding atomic.Bool
}

// NewEngine 建引擎（limits 0 = 默认上界）。
//
// **构造时不加载**（懒加载）：未启用图的工作流压根不会建引擎；构造这一步只备好
// 内存图骨架与空缓存，不碰持久层。
//
// **第一次真正加载的时机（N3）**：由第一次 Sync 触发——
//   - 第一次**多跳查询**（Reachable/Impact）：缓存未命中 → 增量同步 → 冷启动全量加载；
//   - 或宿主在**一轮 Run 结束后**的 catch-up（host/hub.go 的图投影 catch-up）：
//     那一步也会 Sync → **第一轮 Run 就会把整张图灌进内存**（冷启动成本前移到
//     第一次 Run，而不是第一次查询）。
//
// 即"没人查询"不等于"不加载"：宿主只要跑过一轮 Run，图就已经是热的。启用图投影
// 就意味着接受这份内存与加载成本（图是**可选投影**，不启用则零开销，G2f）。
func NewEngine(q Querier, limits coregraph.Limits) (*Engine, error) {
	if q == nil {
		return nil, errors.New("graph: NewEngine 需要非 nil 的读句柄（Querier）")
	}
	mem, err := coregraph.NewMemGraph(limits)
	if err != nil {
		return nil, err
	}
	return &Engine{q: q, mem: mem, cache: coregraph.NewClosureCache(), limits: mem.Limits()}, nil
}

// Invalidate 失效闭包缓存（G20：图写入后缓存必须失效）。
//
// 只清缓存、不动内存图：下一次多跳查询自然走增量同步把新边取回来
// （Sync 的 delta 查询很便宜，命中 0 行就返回）。
//
// 同时**推进缓存代号**（F3）：正在算（Get 之后、Put 之前）的闭包会因代号对不上
// 而写不回去——免得"写者清完缓存又被读侧塞回一条旧的"。
func (e *Engine) Invalidate() { e.cache.Invalidate() }

// Sync 把持久层的增量边并进内存图（冷启动流式加载 + 跨进程写入可见）。
//
// 流式：按 **(seq, rowid) 水位**分批拉，逐行 Scan 进内存图，不整表读进内存（G19）。
// 触到内存上界 → 停止加载并返回 ErrGraphFull（fail-close：不返回可能不完整的影响面）；
// 水位只推进到"已成功应用的最后一行"，重试不会重复应用。
//
// **水位语义依赖单写者纪律（G17/G24④）**：图投影的写者只有索引器一个，且它按
// **seq 升序、按批原子**提交（applyBatch 在一个事务里写边 + 推水位）→ 库里可见的边集
// 永远是 seq 的**前缀**，"seq > 水位" 才不会漏。Rebuild 会打破这个前缀（清空后重放出的
// 边 seq 与旧值相同），所以清空时推进 graph_meta 的**重建代**，Sync 开头对齐代数、
// 不等就整体重载。多写者各带任意 seq 并发提交会让提交序 ≠ seq 序 → 水位跳过后提交的
// 小 seq：那种部署本就不支持（一个库一个索引器）。
//
// **同一事件的多条边不会被切两半**（F1）：水位是 (seq, rowid) 二元组，同 seq 组内
// 按 rowid（写入顺序）继续推进——一个事件产出上万条边也不会丢尾巴。
func (e *Engine) Sync(ctx context.Context) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return e.syncLocked(ctx)
}

func (e *Engine) syncLocked(ctx context.Context) error {
	e.syncs.Add(1)
	// ① 对齐重建代 + 读重建中标记：索引器 Rebuild 过 → 丢掉内存图从 0 重载（否则旧图会
	// 一直答新问题）。rebuilding 标记同步进内存（N2：读侧据此 fail-close）。
	gen, rebuilding, err := e.readGeneration(ctx)
	if err != nil {
		return err
	}
	e.rebuilding.Store(rebuilding)
	if gen != e.generation {
		e.mem.Reset()
		e.loadedSeq.Store(0)
		e.loadedRow.Store(0)
		e.generation = gen
		// 重建代变了 → 内存图整体重来 → 缓存里的闭包**全部**过期（且外部写入
		// 语义也变了）→ 走 Invalidate（清条目 + 推进代号，F3 护栏）。
		e.applyGen.Add(1)
		e.cache.Invalidate()
		// **重建窗口的 fail-close 口径**（N2，v3-S3 补齐）：索引器 Rebuild 的清空与重放
		// **不是原子的**——窗口内投影可能是空的或半满的（重放还没跑完）→ 本次加载出来的
		// 闭包可能"干净地为空"（Direct=[]、Escalate=false、无错误无告警），消费端分不清
		// "真没影响"与"正在重建"（少报影响面 → escalate 闸门 / V7 冲突预测被蒙）。
		// 处置：Projection.Clear 置 rebuilding=1、重放完成 MarkRebuilt 置 0；读侧引擎
		// 据 rebuilding **fail-close 报错**（ErrRebuilding，见 Reachable），绝不把"图不可用"
		// 当成"无冲突/无影响"。同进程重建靠 Committed() 失效缓存，跨进程靠这里的代数校验
		// 整体重载 + rebuilding 标记 fail-close，两头都不拿旧图/半图答题。
	}
	// ② 增量加载（流式：按 (seq,rowid) 水位分批拉，逐行 Scan 进内存图，G19 冷启动口径）。
	applied := 0
	for {
		rows, err := e.q.QueryContext(ctx, loadEdgesSQL, e.loadedSeq.Load(), e.loadedRow.Load(), loadBatch)
		if err != nil {
			if isNoTableErr(err) {
				// 表不存在 = 图投影未启用/未建表 → 显式报错（不静默当空图）。
				return fmt.Errorf("graph: 图投影表不存在（图投影未启用？G2f 可选投影）: %w", err)
			}
			return fmt.Errorf("graph: 增量加载边: %w", err)
		}
		res, rerr := e.drainRows(rows)
		if rerr != nil {
			return rerr
		}
		// 水位只推进到"成功应用的最后一行"（二元组一起推进，F1）。
		if res.applied > 0 {
			e.loadedSeq.Store(res.lastSeq)
			e.loadedRow.Store(res.lastRow)
		}
		applied += res.applied
		if res.full {
			// 触界即停并报错：**不返回可能不完整的影响面**（对 escalate 闸门，
			// 少报比多报危险）。水位只推进到"成功应用的最后一行" → 这个错是**粘的**：
			// 不调大上界就会一直报，不会悄悄放行几条边。
			return fmt.Errorf("%w: 已加载 %d 条边 / %d 个节点（上限 edges=%d nodes=%d）",
				coregraph.ErrGraphFull, e.mem.EdgeCount(), e.mem.NodeCount(),
				e.limits.MaxEdges, e.limits.MaxNodes)
		}
		if res.rows < loadBatch {
			break // 这批没拉满 → 追平了
		}
	}
	if applied > 0 {
		// **同步到了新边就必须失效闭包缓存**（G20）：否则跨进程写入后，一个还温着的
		// 缓存条目会把旧答案当新答案返回（影响面少报 → escalate 闸门被蒙）。
		//
		// 用 Clear（清条目、**不推进代号**）：这次同步是**读侧自己**干的，后面算出的
		// 闭包正是按同步后的内存图算的 → 不该被自己的同步挡住写回缓存。
		// 内存图变了 → applyGen +1（并发查询据此判断"你算的期间图变了"）。
		e.applyGen.Add(1)
		e.cache.Clear()
	}
	return nil
}

// readGeneration 读持久层的重建代 + 重建中标记（表不存在 → 显式报错，与增量加载同口径）。
func (e *Engine) readGeneration(ctx context.Context) (gen int64, rebuilding bool, err error) {
	if err := e.q.QueryRowContext(ctx, readGenerationSQL).Scan(&gen, &rebuilding); err != nil {
		if isNoTableErr(err) {
			return 0, false, fmt.Errorf("graph: 图投影表不存在（图投影未启用？G2f 可选投影）: %w", err)
		}
		return 0, false, fmt.Errorf("graph: 读图投影重建代: %w", err)
	}
	return gen, rebuilding, nil
}

// loadBatchResult 是一批增量加载的结果（水位推进的依据）。
type loadBatchResult struct {
	// rows 是扫过的行数。
	rows int
	// applied 是成功灌进内存图的行数。
	applied int
	// lastSeq / lastRow 是**最后成功应用**那行的 (seq,rowid)：水位二元组。
	lastSeq int64
	lastRow int64
	// full 表示触到了内存上界（加载中断）。
	full bool
}

// drainRows 把一批行流式灌进内存图。
func (e *Engine) drainRows(rows *sql.Rows) (loadBatchResult, error) {
	defer rows.Close()
	var res loadBatchResult
	for rows.Next() {
		var (
			seq      int64
			row      int64
			src, dst string
			kind     string
		)
		if err := rows.Scan(&seq, &src, &dst, &kind, &row); err != nil {
			return res, fmt.Errorf("graph: 扫边行: %w", err)
		}
		res.rows++
		if err := e.mem.AddEdge(src, dst, kind); err != nil {
			if errors.Is(err, coregraph.ErrGraphFull) {
				res.full = true
				break
			}
			return res, err
		}
		res.applied++
		// **只有成功应用的行才推进水位**：触界中断时未应用的行必须留给重试，
		// 否则水位跳过它们 = 永久丢几条边，而查询还在"看起来正常"地返回不完整结果。
		res.lastSeq, res.lastRow = seq, row
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("graph: 读边行: %w", err)
	}
	return res, nil
}

// Neighbors 一跳/两跳邻居（G18：直查 SQLite 双向倒排，**不走内存图**）。
//
// 结果有上限（q.Limit，0 = 默认 DefaultNeighborLimit）：超级节点一次能连出十万条边，
// 不限就会把调用方拉爆（F12）。想知道"是否被截断"用 NeighborsResult。
func (e *Engine) Neighbors(ctx context.Context, q coregraph.NeighborQuery) ([]string, error) {
	res, err := e.NeighborsResult(ctx, q)
	if err != nil {
		return nil, err
	}
	return res.Nodes, nil
}

// NeighborsResult 一跳/两跳邻居 + **截断标记**（F12）。
//
// 口径：恰好 q.Hops 跳（1|2）；不含起点自身；结果按节点 id 排序。
// Truncated=true 表示命中结果上限被截断（**答案不完整**——调用方要如实告知）。
func (e *Engine) NeighborsResult(ctx context.Context, q coregraph.NeighborQuery) (coregraph.NeighborResult, error) {
	if q.Node == "" {
		return coregraph.NeighborResult{}, errors.New("graph: Neighbors 需要非空节点 id")
	}
	if q.Hops < 1 || q.Hops > coregraph.MaxHopsSQL {
		return coregraph.NeighborResult{}, fmt.Errorf("%w（得到 hops=%d）", coregraph.ErrBadHops, q.Hops)
	}
	limit := q.Limit
	if limit < 0 {
		return coregraph.NeighborResult{}, fmt.Errorf("%w（邻居结果上限不能为负，0 = 默认 %d）",
			coregraph.ErrBadLimits, coregraph.DefaultNeighborLimit)
	}
	if limit == 0 {
		limit = coregraph.DefaultNeighborLimit
	}
	// 上限取到 maxInt 时不能 +1（会溢出成负数 → SQLite 当"无上限"，护栏失效，N4）。
	fetch, overflow := neighborFetchLimit(limit)
	query, args := neighborQuery(q.Hops, q.Direction, q.Node, q.Kind, fetch)
	rows, err := e.q.QueryContext(ctx, query, args...)
	if err != nil {
		if isNoTableErr(err) {
			return coregraph.NeighborResult{}, fmt.Errorf("graph: 图投影表不存在（图投影未启用？G2f 可选投影）: %w", err)
		}
		return coregraph.NeighborResult{}, fmt.Errorf("graph: 查 %d 跳邻居: %w", q.Hops, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return coregraph.NeighborResult{}, fmt.Errorf("graph: 扫邻居行: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return coregraph.NeighborResult{}, fmt.Errorf("graph: 读邻居行: %w", err)
	}
	// 多取的那一行 = "确实被上限截断了"的证据（SQL 层 LIMIT limit+1）。
	truncated := false
	if len(out) > limit {
		out = out[:limit]
		truncated = true
	} else if overflow && len(out) >= limit {
		// 溢出分支：取不到第 limit+1 行 → "取满"就保守当截断（偏安全的一侧）。
		truncated = true
	}
	return coregraph.NeighborResult{Nodes: out, Truncated: truncated, Limit: limit}, nil
}

// Reachable 多跳可达闭包（G18：内存图 BFS + G20 闭包缓存）。
//
// 缓存**命中即返回、零 SQL**（不碰持久层）；未命中才增量同步 + BFS + 写缓存。
// 图写入后索引器调 Committed → Invalidate → 缓存清空 → 下次查询重新同步。
//
// **代号护栏**（F3）：写回缓存前要比对两道代号——
//   - 缓存代号 epoch（**外部**写入次数）：在同步**之前**取。期间若有外部写入
//     （含"提交了但还没通知"的那一拍），代号变了 → 这份闭包可能缺那条边 → 不写回；
//   - 内存图代号 applyGen（读侧自己灌进新边的次数）：在同步**之后**取。算的期间
//     另一个并发查询又同步进了新边 → 闭包可能过期 → 不写回。
//
// 宁可多算一次 BFS，也不能给 escalate 闸门一份过期答案（少报影响面 = 蒙住闸门）。
func (e *Engine) Reachable(ctx context.Context, q coregraph.ReachQuery) (coregraph.Closure, error) {
	nq, err := coregraph.NormalizeReach(q)
	if err != nil {
		return coregraph.Closure{}, err
	}
	// N2 fail-close：上一次 Sync 已知投影正在重建 → 直接拒绝作答（不拿可能"干净为空"
	// 的影响面答题）。缓存在重建开始时已被 Invalidate 清掉，这里 Mostly 是兜底。
	if e.rebuilding.Load() {
		return coregraph.Closure{}, ErrRebuilding
	}
	key := coregraph.ClosureKeyOf(nq)
	if c, ok := e.cache.Get(key); ok {
		return c, nil
	}
	// 缓存代号在同步**之前**取：同步期间的任何外部写入都会让它作废这份闭包。
	epoch := e.cache.Epoch()
	if err := e.Sync(ctx); err != nil {
		return coregraph.Closure{}, err
	}
	// Sync 刷新了 rebuilding 标记：若重建刚开始（上次 Sync 时还是 false）→ 这里抓到，
	// 仍 fail-close（N2）。
	if e.rebuilding.Load() {
		return coregraph.Closure{}, ErrRebuilding
	}
	// 内存图代号在同步**之后**取（自己同步进来的边已计入）。
	applyGen := e.applyGen.Load()
	c, err := e.mem.Reach(ctx, nq.Sources, nq.Kind, nq.Direction, nq.MaxDepth, nq.MaxResults)
	if err != nil {
		return coregraph.Closure{}, err
	}
	if e.applyGen.Load() == applyGen {
		// Put 内部还会比对 epoch（不一致 = 期间有外部写入 → 不写）。
		e.cache.Put(key, c, epoch)
	}
	return c, nil
}

// Impact 生成 change_impact 影响面报告（G21/W16）。
//
// 影响面 = **谁依赖改动面**（DirectionIn）：改了 X，受影响的是依赖 X 的那些。
// 报告是纯数据（见 core/graph.ImpactReport），可直接喂给 v3 的 escalate 闸门。
func (e *Engine) Impact(ctx context.Context, req coregraph.ImpactRequest) (coregraph.ImpactReport, error) {
	roots := make([]string, 0, len(req.Roots))
	for _, r := range req.Roots {
		if r != "" {
			roots = append(roots, r)
		}
	}
	if len(roots) == 0 {
		return coregraph.ImpactReport{}, errors.New("graph: change_impact 需要至少一个起点（roots）")
	}
	c, err := e.Reachable(ctx, coregraph.ReachQuery{
		Sources:    roots,
		Kind:       req.Kind,
		Direction:  coregraph.DirectionIn,
		MaxDepth:   req.MaxDepth,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return coregraph.ImpactReport{}, err
	}
	return coregraph.AssembleImpact(req, c), nil
}

// Stats 返回运行时统计（诊断 + 测试断言）。
func (e *Engine) Stats() coregraph.Stats {
	return coregraph.Stats{
		Nodes:        e.mem.NodeCount(),
		Edges:        e.mem.EdgeCount(),
		LoadedSeq:    e.loadedSeq.Load(),
		Saturated:    e.mem.Saturated(),
		CacheEntries: e.cache.Len(),
		CacheHits:    e.cache.Hits(),
		CacheMisses:  e.cache.Misses(),
		Syncs:        e.syncs.Load(),
	}
}

// Limits 返回生效的内存上界（0 值已归一成默认）。
func (e *Engine) Limits() coregraph.Limits { return e.limits }

// Rebuilding 报告图投影是否**正在重建**（N2，v3-S3 补）：true = 索引器 Rebuild 的清空与
// 重放窗口未闭合，此刻的影响面可能"干净地为空"。宿主/escalate 消费端可据此判断"预测不可用"；
// Reachable/Impact 在 rebuilding=true 时已直接 fail-close 报 ErrRebuilding。
func (e *Engine) Rebuilding() bool { return e.rebuilding.Load() }

// RebuildingState 读持久层的"重建中"标记（**只读 graph_meta 一行，不触发加载**）。
//
// # 为什么需要它（v3-S3 修 N2 的砖化）
//
// rebuilding=1 是索引器 Rebuild **清空之后、重放完成之前**的窗口标记。进程在窗口里被
// 杀掉（崩溃 / kill -9）→ 标记**永久留在库里**：此后读侧引擎每次 Sync 都读到 rebuilding=1
// → Reachable/Impact 一直 fail-close 报 ErrRebuilding → 影响面/冲突预测**永久不可用**
// （砖化）。而 Rebuild 当时没有生产调用方 → 没人恢复它。
//
// 宿主（组合根）据此在**启动期**检测并自动重跑一次重建（见 host.New 的恢复逻辑）；
// 本方法只负责"便宜地读出真相"——不建内存图、不加载边（那才是 Sync 的活）。
//
// 表不存在（这个库里从没建过图投影）→ 返回 (false, nil)：没有图投影就没有重建窗口。
func (e *Engine) RebuildingState(ctx context.Context) (bool, error) {
	_, rebuilding, err := e.readGeneration(ctx)
	if err != nil {
		if isNoTableErr(err) {
			return false, nil // 没有图投影 → 没有重建窗口
		}
		return false, err
	}
	return rebuilding, nil
}
