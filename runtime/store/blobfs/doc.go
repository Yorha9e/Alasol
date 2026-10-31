// Package blobfs 是 core/storage.Blob 的**文件系统实现**（G2e/D4，v4-3-A）。
//
// 定位（与 runtime/store/sqlite 并列）：sqlite 落 EventLog（唯一事实源 F5），blobfs 落
// **大对象的外置仓库**（G2e）——事件 envelope 里只留一个引用 `Event.binary_ref`，内容本身
// 放这儿。blob **不是第二状态源**：它可以在引用还在的情况下被整体重建，事件流才是"有没有
// 这条引用"的唯一事实（见 core/storage/blob.go 的 doc）。
//
// # 磁盘布局（引用寻址，id=ULID 一目录）
//
//	<root>/
//	  <ulid>/            ← 一个 blob 一个目录（目录名 = BlobMeta.ID，26 字符 Crockford base32）
//	    meta.json        ← BlobMeta（id/size/media_type/name/created_at；**权威存在标记**）
//	    content          ← 原始字节（临时文件 + rename 原子落位，防半截文件被读到）
//
// 为什么 meta 与 content **分两个文件**：Stat 要能只报大小/类型而不读内容（接口 doc：
// "只想看大小/类型，不想读内容"）——若内容和 meta 混在一个文件里，Stat 就得解析/扫内容。
//
// # 存在性以 meta.json 为权威
//
// Put 的落盘序是 **content 先、meta.json 后**（rename 原子）：
//   - 正常：content rename 完成 → meta.json rename 完成 → blob 可见；
//   - 崩溃在两者之间：content 在、meta.json 不在 → **视为不存在**（Get/Stat 都报
//     ErrNotFound），下次同 id Put 当作新建覆盖（幂等重试，见下）。
//
// 所以 Get 与 Stat **同口径**：都先认 meta.json。Get 在 meta 存在后还会**打开 content 并
// 校验大小**——meta 在而 content 缺失/大小不符 = 存储损坏，返回**明确错误**（不静默返回
// 空/半截内容；这是"读到非预期内容 → 明确 error 不静默"的口径）。
//
// # 幂等口径（接口原文，core/storage.Blob.Put）
//
//   - meta.ID 非空且**内容一致**（同字节）→ 原样返回已有元数据，**不写第二份**；
//   - meta.ID 非空且**内容不一致** → storage.ErrConflict（调用方必须换新 id，不许静默覆盖
//     ——覆盖会让已落库的引用指向别的内容）；
//   - meta.ID 空 → 实现发号（core.NewUlid，框架 id 的唯一生成处）。
//
// 崩溃恢复也走这条：上次 Put 写了 content 没写 meta.json 就崩了 → 重试时 meta.json 不在
// → 当新建处理（覆盖孤儿 content + 落 meta.json），不误报冲突。
//
// # Delete 不级联删事件
//
// Delete 只删内容仓库（<root>/<ulid>/ 整个目录），**不碰事件流**（接口 doc/历史不可重写 F6）：
// 事件里的 binary_ref 原样留着，读侧按引用取不到内容时报"内容已回收"，而不是假装事件不存在。
// Delete **幂等**（不存在 → 成功）。
//
// # 并发安全
//
// 写锁按 id **固定分片**（256 片，id 哈希选片）：同一 id 恒落同一片 → 互斥语义与 per-id 锁
// 一致；不同 id 大概率落不同片（可并发写，偶发碰撞只是多串行一次，不影响正确性）。
// **不用 per-id 的 map**：那张表只增不减（每个新 id 一把锁，进程越跑越久越大 = 内存单调增长），
// 固定分片把锁的总数钉死在 256 把（F8）。
// Get/Stat 无锁（content/meta.json 落位后不可变，rename 原子 → 不会读到半截）。多 goroutine
// 同 id Put 被写锁串行化 → 第二个看到第一个已落的 meta.json，走幂等/冲突判定。
//
// # 依赖方向
//
// 本包 import core/storage（接口）/ contracts（Ulid）/ core（NewUlid/ValidUlid）——runtime
// 依赖 core，方向正确（09）。换实现（文件系统 → 对象存储 → DB 大字段）消费方零改动（G2b）。
package blobfs
