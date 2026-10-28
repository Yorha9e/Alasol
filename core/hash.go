package core

import "crypto/sha256"

// ShortHashLen 是 project_id / repo_id 的 Crockford base32 长度：26 字符（= 128 bit，
// 与 ULID 同字符集/同长，S1）。文件系统遍历（找 .git）在 runtime/identity；本函数只是
// **纯变换**（hash + base32），放 core 根包供 runtime/identity 复用 encodeCrockford。
const ShortHashLen = 26

// shortHashBytes 是 SHA-256 截短后参与 base32 编码的字节数：16 字节 = 128 bit。
//
// 128 bit 的生日碰撞界约 2^64，对任何现实规模的仓库/项目数（路径分组用途）都可忽略；
// 选 128 bit 而非更短，是为了直接复用 ULID 的 26 字符 Crockford 编码（少一套编码实现、
// 少一处 bug 面），并与框架 id 保持同一字符集（排除 I/L/O/U，S1）。
const shortHashBytes = 16

// ShortHash 把一个字符串规范成 project_id / repo_id 用的短哈希（S 系口径：哈希**只用于
// 路径分组**，不用作身份——08 §1/§3）：SHA-256 → 截短到前 128 bit → Crockford base32
// （26 字符，与 core.NewUlid 同字符集/同长/同编码器）。
//
// 确定性（同一输入恒得同一输出）、纯函数（无 IO、无随机）：project_id = ShortHash(canonical_cwd)、
// repo_id = ShortHash(canonical_git_root)——输入必须是已规范化的路径（runtime/identity 负责
// EvalSymlinks→小写→统一分隔符→去尾斜杠后再调本函数）。
func ShortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return encodeCrockford(sum[:shortHashBytes])
}
