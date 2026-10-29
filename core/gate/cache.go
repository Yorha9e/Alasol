package gate

import (
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Yorha9e/alasol/core"
)

// V4 依赖缓存策略（07 V 区 V4 定案 2026-10-28）：**内容寻址的共享只读缓存**，
// 替代"每快照独立全量安装"（干净但慢）。门禁在快照执行时注入约定 env（对齐 V3 的
// CI_WORKTREE/CI_COMMIT/CI_PORT_BASE 风格）：
//
//   - CI_CACHE_KEY：缓存键（**仓库作用域 + 清单内容**的内容寻址键，26 字符 Crockford）；
//   - CI_CACHE_DIR：该键对应的共享缓存目录（= <root>/<key>）。
//
// # 框架的边界：只管键与目录，不管安装
//
// 框架**只派生键与目录路径**并注入 env，**不创建目录、不加锁、不清缓存、不装依赖**——
// 依赖安装是命令（不透明 shell 字符串，W9）自己的事，命令自暖（首次跑把依赖填进
// CI_CACHE_DIR，之后命中复用）。这条边界是刻意的：
//
//   - **内容寻址 = 版本隔离 + 仓库隔离**：键由"仓库作用域 + 清单内容"派生 → 同仓库同内容
//     同键同目录；改内容 → 变键 → 换目录（两个版本的依赖集**永不共享目录**，命令写进自己
//     CI_CACHE_DIR 的东西**不会污染**另一版本）；不同仓库即使清单内容相同也**不同键不同目录**
//     （否则 A 仓的依赖会漏进 B 仓的缓存目录——那不是"污染"那么简单，是**串仓**）。
//     仓库作用域默认 = 主仓库根路径的哈希（**跨机器不共享**：路径不同 → 哈希不同）；
//     **要跨机共享同一个缓存目录就配 cache.scope**（团队约定同一个字符串）。
//   - **目录里只放依赖类产物**：CI_CACHE_DIR 是**依赖缓存**目录，只放包管理器装出来的东西
//     （node_modules / GOPATH 缓存 / venv……），**不放构建产物、不放源码、不放密钥**——
//     它是共享只读目录，跨快照、跨仓库可见（按上面的键隔离），放别的东西等于把它泄出去。
//   - **只读 = 无锁竞争**：框架对缓存**只读**（从不写/锁/清）。同键的多个并发快照读同一份
//     已暖好的稳定目录，框架自身不引入任何锁。取舍（写清免得后人误加锁）：若两个并发快照
//     对**同一冷键**都要**首写**，写写竞争归包管理器自己的锁管——框架的保证是**跨键隔离**，
//     不是**同键写串行化**（后者要么加锁破坏"无锁竞争"，要么超出"框架只管键与目录"的边界）。
//
// 本文件是**纯函数**（无 I/O）：清单文件的**读取**在执行侧（runtime/gate），本包只把
// "文件名 → 内容"折成键与目录。命令无关（不做包管理器特判）。
const (
	// cacheKeyBytes 是内容哈希截短到的字节数：16 字节 = 128 bit（对齐 08 S3 的短哈希口径）。
	cacheKeyBytes = 16
	// cacheKeyDomain 是缓存键的**域分隔前缀**（版本号进前缀 → 换编码口径时键整体换代，
	// 不会与旧口径的目录撞上）。
	cacheKeyDomain = "alasol:gate-cache:v1"
)

// DefaultManifestFiles 是依赖清单的默认集合（跨生态常见 lockfile / manifest）。
//
// 内容寻址缓存键由这些文件的内容哈希派生：改了其中任何一个 → 键变 → 换缓存目录 →
// 命令重新暖（不串旧版本的缓存）。可经 CacheConfig.ManifestFiles 覆盖。
func DefaultManifestFiles() []string {
	return []string{
		// Go
		"go.mod", "go.sum",
		// Node
		"package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock",
		// Python
		"requirements.txt", "Pipfile.lock", "poetry.lock",
		// Rust
		"Cargo.toml", "Cargo.lock",
		// Ruby
		"Gemfile", "Gemfile.lock",
		// PHP
		"composer.json", "composer.lock",
	}
}

// CacheConfig 是依赖缓存的仓库级配置（V4）。零值 = 用默认（DefaultManifestFiles +
// 由执行侧解析的默认根目录）。
//
// 它是 Config 的可选字段（F7 只增，json:"cache"）：缺省时老配置行为不变。
type CacheConfig struct {
	// ManifestFiles 覆盖默认清单集（空 / 全空白 = 用 DefaultManifestFiles）。
	// 只列**参与内容寻址键**的文件；执行侧只读这些（存在即读、不存在跳过）。
	//
	// **只填仓库内的相对路径**（如 "go.mod"）：执行侧拒绝绝对路径、".." 穿越与盘符路径
	// （清单名越界 = 读快照外的文件，键与目录都不可信）。
	ManifestFiles []string `json:"manifest_files,omitempty"`
	// Dir 是共享缓存根（空 = 执行侧用本机默认根）。缓存目录 = Dir/<key>。
	// 生产应配置一个**持久**根（默认落临时目录，跨会话可能被清）。
	//
	// 相对路径按**主仓库根**（稳定落点）解析成绝对路径——**不是**一次性快照目录
	// （快照跑完就删：拿它当基准 = 缓存目录每次换地方、随快照消失，缓存永不命中）。
	// 相对路径的落点也不能随调用方的 cwd 漂移（那会让同一份配置在不同机器上落到不同地方）。
	// 注意副作用：相对路径会落在**仓库工作树内部**（如 <repo>/persistent/cache）——
	// 请自行加 .gitignore，或直接配**仓库外的绝对路径**（推荐，免得缓存被误提交）。
	Dir string `json:"dir,omitempty"`
	// Scope 是**仓库作用域**的可配覆盖（空 = 执行侧按主仓库根派生）。
	//
	// 键 = (仓库作用域, 清单内容)：作用域用来把**不同仓库**隔开（否则同内容的两个仓库
	// 共用一个缓存目录 = 串仓）。默认作用域 = 主仓库根路径的哈希——**跨机器不共享**
	// （路径不同 → 哈希不同）。**要跨机共享同一个缓存目录，就配 scope**（配成同一个
	// 字符串即可，例如团队约定的名字）：这是显式的选择，不是默认行为。
	Scope string `json:"scope,omitempty"`
}

// ManifestSet 返回生效的清单集：显式覆盖（去重、去空白、保序）或默认集。
//
// 覆盖列了但全是空白项 = 视为误配置 → 退回默认（不静默变成"无清单 = 空键"——
// 那会让所有仓库共享同一个空键目录，反而不安全）。
func (c CacheConfig) ManifestSet() []string {
	if len(c.ManifestFiles) == 0 {
		return DefaultManifestFiles()
	}
	seen := make(map[string]bool, len(c.ManifestFiles))
	out := make([]string, 0, len(c.ManifestFiles))
	for _, f := range c.ManifestFiles {
		f = strings.TrimSpace(f)
		if f == "" || seen[f] {
			continue
		}
		seen[f] = true
		out = append(out, f)
	}
	if len(out) == 0 {
		return DefaultManifestFiles()
	}
	return out
}

// CacheKey 派生**内容寻址键**（V4）：键 = (仓库作用域, 清单文件内容)。
//
// repoScope 是仓库作用域标识（生产用 core.ShortHash(主仓库根) 这类**规范化后**的短哈希——
// 同一仓库的多个快照/工作树必须算出同一个作用域，否则缓存永远命中不了；要**跨机器**共享
// 同一个缓存目录就配一个团队约定的字符串（CacheConfig.Scope））。
//
// files：文件名 → 内容。键对**集合**确定（与传入顺序无关）：按文件名排序后，把每个
// (name, content) 以**长度前缀**编码进 SHA-256，截短 128 bit → **26 字符 Crockford base32**
// （与框架 id 同字符集/同编码器，core.EncodeCrockford26；不用 hex——键要进目录名，
// Crockford 排除 I/L/O/U 等易混淆字符，人读目录时不会看错）。
//
//   - **同内容 → 同键**（确定性，命中复用同目录）；
//   - **任一字节变 → 变键**（内容寻址：不同版本不串缓存）；
//   - **长度前缀**（F10 同款）：分隔符拼接会撞键——`{a:"b\x00c"}` 与 `{a:"b", "":"c"}` 之类
//     可能折出同一串；8 字节大端长度前缀让编码无歧义；
//   - **仓库作用域**并进键 → 不同仓库的同内容清单**不同键不同目录**（否则跨仓库串缓存）。
//
// 空 files（无任何清单）→ 空输入的稳定键（不代表"禁用缓存"，代表"该仓库无依赖清单"；
// 注意**无清单仓库之间也不再共用目录**——有仓库作用域在）。
func CacheKey(repoScope string, files map[string][]byte) string {
	return cacheKey(repoScope, files)
}

// CacheKeyScoped 是 CacheKey 的**同义别名**（语义直白时用：作用域 → 内容的顺序一眼可见）。
//
// 同义而非另一套口径：两者算出**同一个**键。
func CacheKeyScoped(repoScope string, files map[string][]byte) string {
	return CacheKey(repoScope, files)
}

// CacheKeyUnscoped 派生**只有清单内容**的键（**无仓库隔离**）。
//
// **仅测试 / 诊断用**：它不含仓库作用域 → 两个不同仓库的同内容清单会折出**同一个**键、
// 共用同一个缓存目录（串仓）。生产路径请用 CacheKey（带作用域）或 CacheKeyScoped。
func CacheKeyUnscoped(files map[string][]byte) string {
	return cacheKey("", files)
}

// cacheKey 是 CacheKey / CacheKeyScoped / CacheKeyUnscoped 的共同实现
// （键 = (域前缀, 作用域?, 清单内容)）。
func cacheKey(repoScope string, files map[string][]byte) string {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)

	h := sha256.New()
	var lenBuf [8]byte
	writeLP := func(b []byte) {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write(b)
	}
	// 域分隔前缀：键只在本用途内成立（换编码口径 → 整体换代，不与旧目录撞上）。
	writeLP([]byte(cacheKeyDomain))
	if repoScope != "" {
		writeLP([]byte(repoScope))
	}
	for _, n := range names {
		writeLP([]byte(n))
		writeLP(files[n])
	}
	sum := h.Sum(nil)
	return core.EncodeCrockford26(sum[:cacheKeyBytes])
}

// CacheDir 由缓存根与键派生缓存目录（V4）：<root>/<key>。
//
// 同键同目录（命中复用）、异键异目录（版本隔离 + 仓库隔离）。纯字符串拼接，无 I/O、
// 不创建目录（框架只派生路径，创建/写入归命令）。
//
// 目录里**只放依赖类产物**（见包头说明）：它是共享只读目录，跨快照可见。
func CacheDir(root, key string) string {
	return filepath.Join(root, key)
}
