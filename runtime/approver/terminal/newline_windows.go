//go:build windows

package terminal

// lineEnd 是 Windows 的行尾（\r\n）。
//
// 为什么分平台：往 Windows 控制台写 "\n" 只会下移一行、不回到行首，
// 提示会呈阶梯状。build tag 比运行时判断更直白，也不会漏掉一边。
const lineEnd = "\r\n"
