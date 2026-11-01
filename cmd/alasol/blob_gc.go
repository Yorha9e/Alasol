package main

// 本文件是 `alasol blob-gc` 子命令（缺口 4 后半）：扫 [blob].dir 的外置仓库，回收崩溃
// 残留（.tmp / 孤儿目录），可选按 mtime 回收超期完整 blob，打印**单行 JSON 报告**
// （照 `alasol wal` 的输出口径：JSON 到 stdout，人话/错误到 stderr）。
//
// # 默认安全（Retention=0 = 只做两扫）
//
// mtime 清扫会删事件流里 binary_ref 还指向的完整 blob——读侧取不到时按契约报"内容已回收"
// （Delete 不级联删事件的既有口径）。默认 -retention 0 = **不清扫**，只回收 .tmp 与孤儿
// 目录（崩溃残留，没有任何引用指向——删掉安全）。想要磁盘回收就显式给 -retention，
// 把握由调用方自己拿。
//
// # dir 未配置 = 明确报错（不静默成功）
//
// 与 `alasol wal` 不同（WAL 未配 = wal_exists=false 合法状态）：blob-gc 没有"目标仓库"
// 就没有可 GC 的对象——静默"成功扫了 0 个"会让人以为磁盘已经清过。故 [blob].dir 空 →
// 退出码 2 + 点名。
//
// # 负值 = 非法配置即拒（仓库纪律：host.ValidateBlobMaxInline 同款）
//
// -retention < 0 / -grace < 0 → **退出码 2 + 点名 flag**。负值没有合理语义（"保留期 -1h"
// = 保留负一天？），静默改写成 0/默认值 = **悄悄换了一套回收策略**（用户以为在按 0 跑
// "只做安全两扫"，实际跑的是别的口径）。fail-close：启动即拒。
//
// -grace 0 的语义**写死**：= **用默认 1h**（blobfs.GC 的 Grace ≤ 0 口径）——安全网不给关：
// 零宽限 = "立刻全删" = 会误删在途写（.tmp / 正在写的目录）；"立即回收"不是一个有意义的
// 配置项，不该由一个 0 悄悄表达。
//
// # 跨进程零互斥（如实标注，不假装安全）
//
// blobfs.GC 的 id 分片锁**只在同进程内**互斥（sync.Mutex 挂在 Store 实例上）：本命令与
// 常驻 hub **并行跑时没有共享锁**，唯一的防线是 **Grace 宽限**（在途写的目录年轻）。
// 跨进程锁**不做**（文件锁的跨平台语义碎，收益配不上复杂度）。

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Yorha9e/alasol/host"
	"github.com/Yorha9e/alasol/runtime/store/blobfs"
)

// blobGCMain 是 `alasol blob-gc`：blob 外置仓库的 GC 报告（单行 JSON 到 stdout）。
//
// flags：
//
//	-config <path>    用户级配置文件路径（TOML；空=平台默认位，H15）——读 [blob].dir
//	-retention <dur>  完整 blob 的 mtime 保留期（**默认 0 = 不清扫 = 只安全两扫**；
//	                  >0 时超期整目录删——**龄期 = 最后外置时间**（幂等引用会刷新）；
//	                  事件流 binary_ref 不动，读侧按契约报"内容已回收"；
//	                  想要安全模式就保持默认；**负值非法** → 退出码 2）
//	-grace <dur>      孤儿/.tmp 的宽限期（默认 1h；年轻残留可能是在途写，不动；
//	                  **显式 0 = 用默认 1h**（安全网不给关）；**负值非法** → 退出码 2）
//	-dry-run          只报不删（报告如实算 Removed*/FreedBytes）
func blobGCMain(args []string) int {
	fs := flag.NewFlagSet("blob-gc", flag.ExitOnError)
	configPath := fs.String("config", "", "用户级配置文件路径（TOML；空=平台默认位，H15）")
	retention := fs.Duration("retention", 0, "完整 blob 的 mtime 保留期（默认 0 = 不清扫 = 只安全两扫；>0 时超期整目录删——龄期 = 最后外置时间，幂等引用会刷新；事件流 binary_ref 不动，读侧按契约报\"内容已回收\"；负值非法 → 退出码 2）")
	grace := fs.Duration("grace", time.Hour, "孤儿/.tmp 的宽限期（年轻残留可能是在途写，不动；显式 0 = 用默认 1h——安全网不给关；负值非法 → 退出码 2）")
	dryRun := fs.Bool("dry-run", false, "只报不删（报告如实算，磁盘不动）")
	_ = fs.Parse(args)

	// 负值即拒（fail-close；口径同 host.ValidateBlobMaxInline）：负值没有合理语义，
	// 静默改写成 0/默认值 = 悄悄换了一套回收策略。退出码 2 + 点名 flag。
	if *retention < 0 {
		fmt.Fprintf(os.Stderr, "blob-gc: -retention 不能为负（得到 %v）；0 = 不清扫 = 只安全两扫\n", *retention)
		return 2
	}
	if *grace < 0 {
		fmt.Fprintf(os.Stderr, "blob-gc: -grace 不能为负（得到 %v）；0 = 用默认 1h\n", *grace)
		return 2
	}

	// 目录读配置文件 [blob].dir（与 run/serve/resume 的 blob 装配同一条配置面）。
	fc, err := host.LoadFileConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if fc == nil {
		fc = &host.FileConfig{}
	}
	dir := strings.TrimSpace(fc.Blob.Dir)
	if dir == "" {
		fmt.Fprintln(os.Stderr, "blob-gc: blob 外置仓库未配置（[blob].dir 为空）——没有可 GC 的目录；先在配置文件的 [blob] 段设置 dir")
		return 2
	}
	// 目录不存在 → 明确报错（GC 不替用户建目录：建出一个空仓库再报"扫了 0 个"是假成功）。
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		if err == nil {
			err = fmt.Errorf("不是目录")
		}
		fmt.Fprintf(os.Stderr, "blob-gc: blob 仓库目录不可用 %q: %v\n", dir, err)
		return 2
	}

	st, err := blobfs.New(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "打开 blob 仓库:", err)
		return 1
	}

	rep, gerr := st.GC(context.Background(), blobfs.GCOptions{
		Retention: *retention,
		Grace:     *grace,
		DryRun:    *dryRun,
	})
	// 报告**打到 stdout**（机器可消费，单行 JSON）；中途出错也打印部分报告 + 退出码 1。
	out, merr := json.Marshal(rep)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "序列化报告失败: %v\n", merr)
		return 1
	}
	fmt.Println(string(out))
	if gerr != nil {
		fmt.Fprintln(os.Stderr, "blob-gc 中途出错（以上为部分报告）:", gerr)
		return 1
	}
	return 0
}
