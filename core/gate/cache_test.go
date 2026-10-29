package gate

import (
	"path/filepath"
	"strings"
	"testing"
)

// testScope 是测试用的仓库作用域（26 字符 Crockford 形态的短哈希）。
const testScope = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

// TestCacheKeyContentAddressed：内容寻址——同内容同键、改一个字节变键、与传入顺序无关。
//
// 变异自证：若 CacheKey 不把**内容**折进哈希（例如只哈希文件名），下面"改内容应变键"
// 与"换顺序应同键"至少一条会红。
func TestCacheKeyContentAddressed(t *testing.T) {
	base := map[string][]byte{"go.sum": []byte("h1:abc\nh2:def\n")}
	k1 := CacheKey(testScope, base)
	k2 := CacheKey(testScope, map[string][]byte{"go.sum": []byte("h1:abc\nh2:def\n")})
	if k1 != k2 {
		t.Errorf("同内容应同键：%q != %q", k1, k2)
	}
	// 键形：26 字符 Crockford base32（128bit 截短 + 框架同款编码器）。
	if len(k1) != 26 {
		t.Errorf("键长度 = %d，期望 26（Crockford base32）", len(k1))
	}
	// 改一个字节 → 变键（内容寻址：不同版本不串缓存）。
	changed := CacheKey(testScope, map[string][]byte{"go.sum": []byte("h1:abc\nh2:deg\n")})
	if changed == k1 {
		t.Error("改一个字节应变键（内容寻址）")
	}
	// 多个文件时只改其中一个 → 变键。
	multi := CacheKey(testScope, map[string][]byte{"go.mod": []byte("module x\n"), "go.sum": []byte("h1:abc\n")})
	multiChanged := CacheKey(testScope, map[string][]byte{"go.mod": []byte("module y\n"), "go.sum": []byte("h1:abc\n")})
	if multi == multiChanged {
		t.Error("多文件集里改一个文件应变键")
	}
}

// TestCacheKeyCrockford26：键是 26 字符 Crockford base32（不是 hex）——键要进目录名，
// Crockford 排除 I/L/O/U 等易混淆字符，人读目录时不会看错（p3-3）。
//
// 变异自证：若退回 hex 编码，长度（32）与字符集两条都会红。
func TestCacheKeyCrockford26(t *testing.T) {
	k := CacheKey(testScope, map[string][]byte{"go.mod": []byte("module x\n")})
	if len(k) != 26 {
		t.Fatalf("键长度 = %d，期望 26（Crockford base32）：%q", len(k), k)
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z')) {
			t.Errorf("键含非 Crockford 字符 %q（位置 %d）：%q", c, i, k)
		}
		switch c {
		case 'I', 'L', 'O', 'U':
			t.Errorf("键含易混淆字符 %q（位置 %d）：%q", c, i, k)
		}
	}
}

// TestCacheKeyRepoScope（p2-3）：键 = (仓库作用域, 清单内容)——**不同仓库**的同内容清单
// 必须不同键（否则跨仓库共用缓存目录 = 串仓）。
//
// 变异自证：若键里不含仓库作用域（只哈希清单内容），本测试会红。
func TestCacheKeyRepoScope(t *testing.T) {
	files := map[string][]byte{"go.mod": []byte("module x\n")}
	scopeA := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	scopeB := "01ARZ3NDEKTSV4RRFFQ69G5FAW"

	kA := CacheKey(scopeA, files)
	kB := CacheKey(scopeB, files)
	if kA == kB {
		t.Error("不同仓库作用域应得不同键（否则跨仓库共用缓存目录 = 串仓）")
	}
	// 同作用域 + 同内容 → 同键（命中复用）。
	if CacheKey(scopeA, files) != kA {
		t.Error("同作用域同内容应同键（命中复用）")
	}
	// CacheKeyScoped 是同义别名（同一口径、同一键）。
	if CacheKeyScoped(scopeA, files) != kA {
		t.Error("CacheKeyScoped 应与 CacheKey 同键")
	}
	// 无作用域形态（仅测试/诊断用）不含仓库隔离 → 与带作用域的键不同。
	if CacheKeyUnscoped(files) == kA {
		t.Error("无作用域的内容键不应与带作用域的键相同")
	}
	// 作用域不改变"内容寻址"性质：同作用域下改内容仍变键。
	changed := map[string][]byte{"go.mod": []byte("module y\n")}
	if CacheKey(scopeA, changed) == kA {
		t.Error("改内容应变键（内容寻址）")
	}
}

// TestCacheKeyOrderIndependent：键对集合确定——map 无序，但同集合两次算必须同键。
func TestCacheKeyOrderIndependent(t *testing.T) {
	a := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "c": []byte("3")}
	// 多次计算（map 迭代顺序随机）必须稳定。
	first := CacheKey(testScope, a)
	for i := 0; i < 20; i++ {
		if got := CacheKey(testScope, a); got != first {
			t.Fatalf("第 %d 次键 = %q，期望稳定 %q（map 无序不该影响键）", i, got, first)
		}
	}
	// 文件名也参与键：同内容不同文件名 → 不同键。
	renamed := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "z": []byte("3")}
	if CacheKey(testScope, a) == CacheKey(testScope, renamed) {
		t.Error("文件名应参与键（改名应变键）")
	}
}

// TestCacheKeyLengthPrefixed：长度前缀防撞键（F10 同款）——不同 (name,content) 切分不撞。
func TestCacheKeyLengthPrefixed(t *testing.T) {
	// 一个是"名字 ab + 内容 c"，另一个是"名字 a + 内容 bc"：无长度前缀可能撞。
	x := CacheKey(testScope, map[string][]byte{"ab": []byte("c")})
	y := CacheKey(testScope, map[string][]byte{"a": []byte("bc")})
	if x == y {
		t.Error("不同 (name, content) 切分不应撞键（长度前缀）")
	}
}

// TestCacheKeyEmptySet：无清单 → 空输入的稳定键（非随机、可复现）。
func TestCacheKeyEmptySet(t *testing.T) {
	k1 := CacheKey(testScope, nil)
	k2 := CacheKey(testScope, map[string][]byte{})
	if k1 != k2 {
		t.Errorf("空集键应稳定：%q != %q", k1, k2)
	}
	if k1 == "" {
		t.Error("空集也应有一个确定键（不是空串）")
	}
	// 无作用域形态同样稳定（仅测试/诊断用）。
	if CacheKeyUnscoped(nil) != CacheKeyUnscoped(map[string][]byte{}) {
		t.Error("无作用域形态的空集键也应稳定")
	}
}

// TestCacheDirDerivation：目录派生——同键同目录（命中复用）、异键异目录（版本隔离）。
func TestCacheDirDerivation(t *testing.T) {
	root := filepath.Join("var", "cache", "alasol")
	key := CacheKey(testScope, map[string][]byte{"go.sum": []byte("x")})
	dir := CacheDir(root, key)
	if !strings.HasPrefix(dir, root+string(filepath.Separator)) || !strings.HasSuffix(dir, key) {
		t.Errorf("CacheDir = %q，期望 %s/<key>", dir, root)
	}
	// 同键 → 同目录（命中不重复构建）。
	if CacheDir(root, key) != dir {
		t.Error("同键应派生同目录")
	}
	// 不同键 → 不同目录（只读无污染：不同版本互不干扰）。
	other := CacheKey(testScope, map[string][]byte{"go.sum": []byte("y")})
	if CacheDir(root, other) == dir {
		t.Error("异键应派生异目录（无污染）")
	}
}

// TestManifestSet：清单集——默认集、覆盖、去重去空白、全空白退回默认。
func TestManifestSet(t *testing.T) {
	// 零值 → 默认集。
	if got := (CacheConfig{}).ManifestSet(); len(got) == 0 || got[0] != "go.mod" {
		t.Errorf("零值应得默认集，got %v", got)
	}
	// 覆盖（去重、去空白、保序）。
	got := CacheConfig{ManifestFiles: []string{" pnpm-lock.yaml ", "", "go.mod", "go.mod"}}.ManifestSet()
	want := []string{"pnpm-lock.yaml", "go.mod"}
	if len(got) != len(want) {
		t.Fatalf("覆盖集 = %v，want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("覆盖集[%d] = %q，want %q（去空白/去重/保序）", i, got[i], want[i])
		}
	}
	// 全空白覆盖 = 误配置 → 退回默认（不退化成"空键 = 所有仓库共享一个目录"）。
	if got := (CacheConfig{ManifestFiles: []string{"", "  "}}).ManifestSet(); len(got) == 0 || got[0] != "go.mod" {
		t.Errorf("全空白覆盖应退回默认集，got %v", got)
	}
}

// TestDefaultManifestFilesCoverage：默认集覆盖常见生态（不为空、含 go/node/python）。
func TestDefaultManifestFilesCoverage(t *testing.T) {
	def := DefaultManifestFiles()
	if len(def) == 0 {
		t.Fatal("默认清单集不应为空")
	}
	joined := strings.Join(def, ",")
	for _, must := range []string{"go.mod", "go.sum", "package-lock.json", "requirements.txt"} {
		if !strings.Contains(joined, must) {
			t.Errorf("默认集应含 %q，got %v", must, def)
		}
	}
	// 调用方改了返回值不应影响下次（防御性拷贝语义由返回新切片保证）。
	def[0] = "MUTATED"
	if DefaultManifestFiles()[0] == "MUTATED" {
		t.Error("DefaultManifestFiles 应每次返回新切片（不被调用方改动污染）")
	}
}

// TestParseConfigWithCache：F7 只增——gate.json 带 cache 字段能解析，缺省时为 nil。
func TestParseConfigWithCache(t *testing.T) {
	// 带 cache：覆盖清单集 + 自定义根 + 作用域（跨机共享用）。
	c, err := ParseConfig([]byte(`{"command":"make ci","cache":{"manifest_files":["go.mod","go.sum"],"dir":"/data/cache","scope":"team-shared"}}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if c.Cache == nil {
		t.Fatal("带 cache 字段应解析出 Cache")
	}
	if len(c.Cache.ManifestFiles) != 2 || c.Cache.Dir != "/data/cache" || c.Cache.Scope != "team-shared" {
		t.Errorf("Cache = %+v", c.Cache)
	}
	// 缺 cache：老配置行为不变（nil，执行侧用默认）。
	c2, err := ParseConfig([]byte(`{"command":"make ci"}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if c2.Cache != nil {
		t.Errorf("缺 cache 字段应为 nil，got %+v", c2.Cache)
	}
}
