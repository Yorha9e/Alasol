package core

import (
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// ulidPattern 与契约 common.json 的 pattern 一致（S1）。
var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

func TestNewUlidFormat(t *testing.T) {
	for i := 0; i < 500; i++ {
		u := NewUlid()
		if !ulidPattern.MatchString(string(u)) {
			t.Fatalf("第 %d 个 ULID %q 不合字符集/长度（^[0-9A-HJKMNP-TV-Z]{26}$）", i, u)
		}
		if !ValidUlid(u) {
			t.Fatalf("ValidUlid(%q)=false", u)
		}
		// 首字符只用到 3bit（标准 ULID 编码）→ 取值 0–7。
		if u[0] > '7' {
			t.Fatalf("ULID %q 首字符 %c 超范围（应 ≤ 7）", u, u[0])
		}
	}
}

func TestValidUlidRejectsBadCharset(t *testing.T) {
	bad := []string{
		"",
		"01BX5ZZKBKACTAV9WEVGEMMVR",   // 25 字符
		"01BX5ZZKBKACTAV9WEVGEMMVRZZ", // 27 字符
		"01BX5ZZKBKACTAV9WEVGEMMVRI",  // 含 I
		"01BX5ZZKBKACTAV9WEVGEMMVRL",  // 含 L
		"01BX5ZZKBKACTAV9WEVGEMMVRO",  // 含 O
		"01BX5ZZKBKACTAV9WEVGEMMVRU",  // 含 U
		"01bx5zzkbkactav9wevgemmvrz",  // 小写
		"01BX5ZZKBKACTAV9WEVGEMMV-",   // 非法符号
	}
	for _, s := range bad {
		if ValidUlid(contracts.Ulid(s)) {
			t.Errorf("ValidUlid(%q)=true, want false", s)
		}
	}
}

func TestNewUlidTimestampPrefix(t *testing.T) {
	before := time.Now().UTC().UnixMilli()
	u := NewUlid()
	after := time.Now().UTC().UnixMilli()

	// 前 10 字符恰好编满 48bit 毫秒时间戳（首字符只占 3bit，后 9 字符各 5bit）。
	var ms uint64
	for i := 0; i < 10; i++ {
		ms = ms<<5 | uint64(crockfordIndex(u[i]))
	}
	if ms < uint64(before) || ms > uint64(after) {
		t.Fatalf("ULID 时间戳前缀解出 %d, 期望在 [%d, %d] 内（ulid=%s）", ms, before, after, u)
	}
}

func crockfordIndex(c byte) int {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for i := 0; i < len(alphabet); i++ {
		if alphabet[i] == c {
			return i
		}
	}
	return -1
}

// S11：ULID 生成必须线程安全（并发唯一）。
func TestNewUlidConcurrentUnique(t *testing.T) {
	const (
		goroutines = 8
		perG       = 2000
	)
	var wg sync.WaitGroup
	ch := make(chan string, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ch <- string(NewUlid())
			}
		}()
	}
	wg.Wait()
	close(ch)

	seen := make(map[string]struct{}, goroutines*perG)
	total := 0
	for s := range ch {
		total++
		if !ulidPattern.MatchString(s) {
			t.Fatalf("并发产出的 %q 不合法", s)
		}
		if _, dup := seen[s]; dup {
			t.Fatalf("并发产出重复 ULID: %s", s)
		}
		seen[s] = struct{}{}
	}
	if total != goroutines*perG {
		t.Fatalf("总数=%d, want %d", total, goroutines*perG)
	}
}

// 同毫秒内连续生成也必须唯一（80bit 随机位）。
func TestNewUlidSameMillisecondUnique(t *testing.T) {
	seen := make(map[string]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		s := string(NewUlid())
		if _, dup := seen[s]; dup {
			t.Fatalf("第 %d 个 ULID 重复: %s", i, s)
		}
	}
}

func TestNewUlidSortableByTime(t *testing.T) {
	// ULID 前缀即毫秒时间戳 → 字典序随时间单调（同毫秒内靠随机位，不保证）。
	a := string(NewUlid())
	var b string
	for i := 0; i < 1000; i++ {
		time.Sleep(2 * time.Millisecond)
		b = string(NewUlid())
		if ulidPrefixValue(a) != ulidPrefixValue(b) {
			break
		}
	}
	if ulidPrefixValue(a) == ulidPrefixValue(b) {
		t.Skip("未能取到不同毫秒的样本，跳过单调性断言")
	}
	if !(a < b) {
		t.Fatalf("时间上前者应先于后者: %s vs %s", a, b)
	}
	// 前 10 字符（时间戳部分）解出来应满足 a < b。
	ta, _ := strconv.ParseUint(ulidPrefixValue(a), 10, 64)
	tb, _ := strconv.ParseUint(ulidPrefixValue(b), 10, 64)
	if !(ta < tb) {
		t.Fatalf("时间戳部分应递增: %d vs %d", ta, tb)
	}
}

func ulidPrefixValue(u string) string {
	var v uint64
	for i := 0; i < 10; i++ {
		v = v<<5 | uint64(crockfordIndex(u[i]))
	}
	return strconv.FormatUint(v, 10)
}
