package core

import (
	crand "crypto/rand"
	mrand "math/rand/v2"
	"time"

	"github.com/Yorha9e/alasol/contracts"
)

// crockfordAlphabet 是 Crockford base32 字符集（S1/S8：排除易混淆的 I/L/O/U）。
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewUlid 生成一个新的框架 id：26 字符 Crockford base32
// （48bit 毫秒时间戳 + 80bit 随机，S1）。
//
// 这是**框架 id 的唯一生成处**（S 系统一前不扩散）：core 根包提供，policy /
// registry / bus 等包一律调用它，不各自实现（避免两套编码漂移）。
//
// 线程安全（S11）：随机位取自 crypto/rand（并发安全、无需加锁）；
// 仅当 crypto/rand 不可用时降级到 math/rand/v2（其顶层函数同样并发安全且自动播种）。
// 80bit 随机位保证同毫秒内的唯一性（进程内 10^6 个 id 的碰撞概率 ~1e-12）。
func NewUlid() contracts.Ulid {
	var b [16]byte
	// 高 48bit = 毫秒时间戳（大端，落在 b[0:6] = 128bit 数的最高 48 位）。
	ms := uint64(time.Now().UTC().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	// 低 80bit = 随机（b[6:16]）。
	copy(b[6:16], randomBytes(10))
	return contracts.Ulid(encodeCrockford(b[:]))
}

// ValidUlid 校验 ULID 格式（S1/S9：26 字符 Crockford base32；不合则拒绝，不修复）。
func ValidUlid(u contracts.Ulid) bool {
	if len(u) != 26 {
		return false
	}
	for i := 0; i < len(u); i++ {
		c := u[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
		if c == 'I' || c == 'L' || c == 'O' || c == 'U' {
			return false
		}
	}
	return true
}

// randomBytes 取 n 字节随机（crypto/rand 优先，失败降级 math/rand/v2）。
func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := crand.Read(b); err == nil {
		return b
	}
	for i := range b {
		b[i] = byte(mrand.Uint64())
	}
	return b
}

// encodeCrockford 把 128bit 大端整数编成 26 字符 Crockford base32（标准 ULID 编码）：
// 26×5=130bit ≥ 128bit，最高字符只用到 3bit（取值 0–7，与标准 ULID 的首字符约束一致）。
//
// 位累积方向：从最低字节开始，**新字节比已累积的位更显著** → 必须左移 nbits 后 OR 进来
// （写成 (bits<<8)|b[i] 会把字节显著性倒置，编码仍是 26 字符但位序全乱——曾由此踩过坑）。
func encodeCrockford(b []byte) string {
	out := make([]byte, 26)
	var bits uint64
	var nbits uint
	idx := 25
	for i := len(b) - 1; i >= 0; i-- {
		bits |= uint64(b[i]) << nbits
		nbits += 8
		for nbits >= 5 {
			out[idx] = crockfordAlphabet[bits&0x1f]
			idx--
			bits >>= 5
			nbits -= 5
		}
	}
	if nbits > 0 { // 128 = 25×5 + 3 → 最后 3bit 进首字符
		out[idx] = crockfordAlphabet[bits&0x1f]
	}
	return string(out)
}
