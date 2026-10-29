package storage

import (
	"context"
	"io"

	"github.com/Yorha9e/alasol/contracts"
)

// blob 族（G2e）：**大对象的外置仓库**。接口先立，实现留 v4。
//
// # 它装什么（G2e 原文口径）
//
// 大工具输出、原始条目——即"**不该进事件流**"的那类字节：
// 事件 envelope 里只留一个引用 `Event.binary_ref`（D4），内容本身放 blob。
//
// 为什么必须外置：事件流要被回放、要被订阅者逐条转发、要被索引器逐条读——
// 一条几十 MB 的工具输出塞进 payload 会把这三件事全拖死（F12 同款理由：
// 上限不是优化，是正确性）。
//
// # 与事件日志的分工
//
//	事件日志 = 事实源（F5）：引用 + 元数据 + 小载荷；
//	blob    = 大字节的仓库：内容寻址/引用寻址，可换实现（文件系统 → 对象存储 → DB 大字段）。
//
// 所以 blob **不是第二状态源**：它可以在引用还在的情况下被整体重建（实现侧的事），
// 事件流才是"有没有这条引用"的唯一事实。
//
// # 挂接点：Event.binary_ref
//
//	meta, _ := blob.Put(ctx, storage.BlobMeta{MediaType: "text/plain"}, content)
//	ev := contracts.Event{...}
//	ev.BinaryRef = meta.Ref()   // ← 引用 id（ULID，26 字符；JSON 里是 string，陷阱 2/3）
//
// 读侧按 `*ev.BinaryRef` 调 `blob.Get` 取回内容。**引用与内容最终一致**（写 blob 成功
// 才落引用；落库失败不留引用）——顺序反了就会留下指向空处的引用。
//
// # 实现留 v4
//
// 本刀只立接口与口径（v0/v1/v2 的大输出仍走 ToolResult 的行内内容——没有实现就不会被
// 悄悄依赖）。文件系统 / 对象存储 / DB 大字段三类实现都照这个面写，消费方零改动。

// BlobMeta 是 blob 的元数据（**不含内容**）。
type BlobMeta struct {
	// ID 是引用 id（ULID，26 字符）。Put 时留空 = 由实现分配；调用方也可以自己给
	// （内容寻址 / 幂等重试用——同 id 同内容重复 Put 不产生第二份）。
	ID contracts.Ulid `json:"id"`
	// Size 是字节数（Put 时可由实现回填；调用方预填的值仅作提示）。
	Size int64 `json:"size"`
	// MediaType 是 MIME（开放字符串，可空）。
	MediaType string `json:"media_type,omitempty"`
	// Name 是原始名（诊断用，如工具输出的文件名；可空）。
	Name string `json:"name,omitempty"`
	// CreatedAt 是 RFC 3339 创建时刻（诊断用）。
	CreatedAt string `json:"created_at,omitempty"`
}

// Ref 折出 `Event.binary_ref` 的取值（id 为空 → nil，即"不外置"）。
//
// 这是 blob 族与事件契约的**唯一挂接点**：引用类型就是契约里的 `*contracts.Ulid`，
// 不引入第二套 id 表示（S 系纪律：一个 id 一种表示）。
func (m BlobMeta) Ref() *contracts.Ulid {
	if m.ID == "" {
		return nil
	}
	id := m.ID
	return &id
}

// Blob 是大对象存储（G2e；v4 保留位，本刀只立接口）。
//
// 实现必须并发安全；必须**不泄漏存储位置**（接口里没有路径/桶名/连接串——
// 换实现时消费方一行不改，G2b）。
type Blob interface {
	// Put 写入一个大对象，返回落库后的元数据（ID 一定非空）。
	//
	// content 由实现负责读完/关闭语义：本接口不替调用方关 reader（调用方持有它）。
	// 幂等口径：meta.ID 非空且**内容一致** → 原样返回已有元数据（不写第二份）；
	// 内容不一致 → ErrConflict（调用方必须换新 id，不许静默覆盖——
	// 覆盖会让已落库的引用指向别的内容）。
	Put(ctx context.Context, meta BlobMeta, content io.Reader) (BlobMeta, error)

	// Get 打开一个大对象供读取（不存在 → ErrNotFound）。
	//
	// 返回的 ReadCloser **由调用方关闭**（实现不得在返回前吞掉它——大对象要能流式读，
	// 不能整个塞进内存，F12）。
	Get(ctx context.Context, id contracts.Ulid) (io.ReadCloser, error)

	// Stat 取元数据（不含内容；不存在 → ErrNotFound）。
	//
	// 给"只想看大小/类型，不想读内容"的消费方（UI 列表、预算估算）。
	Stat(ctx context.Context, id contracts.Ulid) (BlobMeta, error)

	// Delete 删除一个大对象（不存在 → 幂等成功）。
	//
	// **实现不得级联删事件**：删 blob 只是删内容仓库，事件流里的引用原样留着
	// （历史不可重写，F6）——读侧按引用取不到内容时报"内容已回收"，而不是假装事件不存在。
	Delete(ctx context.Context, id contracts.Ulid) error
}
