package worktree

import (
	"os"
	"path/filepath"
)

// removeTreeSafe 删除目录树，带 junction / 符号链接防御（陷阱 6：rmTree 先解 junction 再删，
// 防递归跟穿到仓库外）。逻辑与 runtime/gate/remove.go 同款（此处独立一份：worktree 是与 gate
// 不同的关注点，不引入跨包耦合；该逻辑经 gate 的实测-junction 用例验证过，逐字沿用其判据）。
//
// 机制：对每个条目先 os.Lstat（**不跟随**链接）。lstat 对符号链接与 junction 都返回链接
// **自身**的 FileInfo——IsDir() 一律为 false。于是：
//   - IsDir()==false：文件 / 符号链接 / junction → os.Remove 删条目本身（删链接不跟随、
//     不递归 → 绝不会跟穿到仓库外）；
//   - IsDir()==true：是**真实目录** → 才递归进去。
func removeTreeSafe(root string) error {
	fi, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 本来就不存在：无需删
		}
		return err
	}
	if !fi.IsDir() {
		if err := os.Remove(root); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		// 读不了内容（权限 / 竞争删除）：退回 os.RemoveAll。root 已由 Lstat 确认是普通目录
		//（非链接），RemoveAll 不会跟穿到仓库外。
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
