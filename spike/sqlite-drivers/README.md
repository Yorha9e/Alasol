# SQLite 驱动选型 spike

一次性选型工具，用可执行代码回答"纯 Go 嵌入式 SQLite 能不能撑住我们的存储需求"。

**这是 spike，不是库。** 结论确定后可以删掉；但 `common/conformance.go` 里的断言值得保留，将来升级驱动版本时重跑一遍即可。

## 跑法

```bash
cd spike/sqlite-drivers
bash run.sh              # 全部驱动
bash run.sh modernc      # 单个驱动
```

环境：Go 1.26.5 / Windows amd64 / `CGO_ENABLED=0` / `GOPROXY=https://goproxy.cn,direct`

## 测什么

| # | 检查项 | 为什么测 |
|---|---|---|
| 1 | `journal_mode=WAL` 是否真的生效 | 网上说法互相矛盾（"modernc 不支持 WAL"）——实测 |
| 2 | `busy_timeout` 是否真的生效 | 决定"锁冲突时等待"还是"立即失败" |
| 3 | 幂等追加（`id` 唯一约束） | 事件日志去重的基础 |
| 4 | `seq` 单调 + `TIMESTAMP` 往返类型 | 队列序号的分配方式；驱动对时间列的类型转换（跨驱动迁移陷阱） |
| 5 | 并发追加 50 goroutine × 20（默认事务模式） | 高并发写是否出现 `SQLITE_BUSY` |
| 6 | 同上，`_txlock=immediate` | 事务模式是否影响并发表现 |
| 7 | 写期间并发读 | 读是否被写阻塞 |
| 8 | 锁冲突时是否等待 | busy handler 是否真的被调用 |
| 9 | **跨进程**：两个进程开同一个库同时写 | **真实场景（hub + CLI）**——单进程并发完全说明不了磁盘锁对不对 |
| 10 | `windows/amd64` + `CGO_ENABLED=0` 交叉编译 | 纯 Go、Windows 一等目标 |

## 实测结果（2026-10-26）

| 检查项 | modernc.org/sqlite v1.61.0 | ncruces/go-sqlite3 v0.35.6 |
|---|---|---|
| driver 名 | `sqlite` | `sqlite3` |
| `journal_mode=WAL` | ✅ `wal` | ✅ `wal` |
| `busy_timeout` 生效 | ✅ 等待 1.509s | ✅ 等待 1.501s |
| 幂等追加 | ✅ 行数=1 | ✅ 行数=1 |
| `seq` 单调 | ✅ | ✅ |
| `TIMESTAMP` 往返类型 | `time.Time` | `time.Time` |
| 并发追加（默认事务） | ❌ **966/1000，34 次 SQLITE_BUSY**，274 tps | ❌ 999/1000，1 次 SQLITE_LOCKED，4328 tps |
| 并发追加（`_txlock=immediate`） | ❌ **969/1000，31 次 SQLITE_BUSY**，294 tps | ✅ **1000/1000 零失败**，5852 tps |
| 写期间并发读 | ✅ 0 错 | ✅ 0 错 |
| 跨进程写 | ✅ 探针看到对方 83 行，自己的写成功 | ✅ 探针看到对方 100 行，自己的写成功 |
| 纯 Go 交叉编译 | ✅ | ✅ |

## 结论

**选 `github.com/ncruces/go-sqlite3` v0.35.6，配 `_txlock=immediate` + `busy_timeout`。**

理由：
1. **并发写零失败**（1000/1000），而 modernc 有 3% 失败
2. **吞吐高一个数量级**（5852 vs 294 tps）
3. **跨进程已验证**——它虽然是 WASM + 纯 Go VFS，但文件锁是对的（这点原本是最大疑问）
4. **它就是真 SQLite**（wasm2go），语义最接近未来换 Postgres 时的行为

**但要记住代价**：它经 WASM 沙箱执行，理论上应有性能损耗；这里反而更快，是因为 modernc 的 busy 重试把吞吐拖垮了。**单驱动横向对比不等于跨驱动绝对性能**，真实负载下要复测。

**必须配的两项**：
- `_txlock=immediate`（两个驱动的并发表现都因此变好，ncruces 从 1 次失败变 0 次）
- `busy_timeout`（否则锁冲突立即失败）

**单写者模型仍是主防线**：上面的并发测试是 50 goroutine 抢写的极端情况；我们正常只有一个 writer。

## 顺带否掉的两条

| 候选 | 否掉的原因（实测证据） |
|---|---|
| `github.com/tursodatabase/limbo` v0.8.3 | **Go module 里 0 个 `.go` 文件**（bindings 只有 c / dotnet / java / javascript / python / react-native / rust / tcl）。Go 侧无 API，只能走 C binding → cgo |
| `github.com/surrealdb/surrealdb.go` v1.7.0 | 嵌入模式在（`pkg/connection/embedded.go`），但是 **cgo**（仓库含 `libsrc/` Rust 源码），Windows 需 Rust + MSVC 工具链；且嵌入式把整个查询引擎常驻进程，不是"轻" |

## 网络说法纠正

网上流传"modernc.org/sqlite 纯 Go 实现不支持 WAL / busy_timeout，生产环境踩过坑的人都退回去了"——**实测 v1.61.0 不成立**：WAL 生效、busy_timeout 生效（等待 1.509s）、TIMESTAMP 往返正常。问题不在 WAL，在**高并发写下的 busy 重试**。

## 目录

```
spike/sqlite-drivers/
├── README.md
├── run.sh                  # 编排：进程内套件 → 跨进程检查 → 交叉编译
├── common/
│   ├── go.mod
│   └── conformance.go       # 驱动无关的断言体 + hold/probe 跨进程子命令
├── modernc/                # 独立 module，main.go 是薄壳
└── ncruces/
```
