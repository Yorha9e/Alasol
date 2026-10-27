package gateexec

import (
	"os"
	"path/filepath"
)

// removeTreeSafe 删除目录树，带 junction / 符号链接防御（V10 / 陷阱 6：rmTree 先解 junction 再删，
// 防递归跟穿到仓库外）。
//
// 机制：对每个条目先 os.Lstat（**不跟随**链接）。lstat 返回链接**自身**的 FileInfo——
// 符号链接与 junction 都不被跟随，因而 IsDir() 一律为 false。于是：
//
//   - IsDir()==false：文件、符号链接或 junction → os.Remove 删掉条目本身。
//     os.Remove 对链接只删链接、不碰目标（Windows junction 实测：删链接后目标完好），
//     因而**绝不会递归跟穿到仓库外**；
//   - IsDir()==true：是**真实目录**（链接不会被 lstat 报成目录）→ 才递归进去。
//
// 这比"判 ModeSymlink"更稳：Windows 上 junction 的 lstat 并不置 ModeSymlink（实测为 false），
// 但 IsDir() 同样为 false，靠 IsDir 分流两者都能挡住。
func removeTreeSafe(root string) error {
	fi, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 本来就不存在：无需删
		}
		return err
	}
	// 非目录 = 文件 / 符号链接 / junction：直接删条目（删链接不跟随、不递归）。
	if !fi.IsDir() {
		if err := os.Remove(root); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		// 读不了内容（权限 / 竞争删除）：退回 os.RemoveAll。此时 root 已由 Lstat 确认是
		// 普通目录（非链接），RemoveAll 不会跟穿到仓库外。
		if rmErr := os.RemoveAll(root); rmErr != nil && !os.IsNotExist(rmErr) {
			return rmErr
		}
		return nil
	}
	for _, e := range ents {
		if err := removeTreeSafe(filepath.Join(root, e.Name())); err != nil {
			return err
		}
	}
	if err := os.Remove(root); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
