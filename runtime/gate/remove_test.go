package gateexec

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRemoveTreeSafeBasic：普通目录树被完整删除。
func TestRemoveTreeSafeBasic(t *testing.T) {
	base := t.TempDir()
	tree := filepath.Join(base, "tree")
	mustMkdir(t, filepath.Join(tree, "sub"))
	writeFile(t, filepath.Join(tree, "top.txt"), "x")
	writeFile(t, filepath.Join(tree, "sub", "deep.txt"), "y")

	if err := removeTreeSafe(tree); err != nil {
		t.Fatalf("removeTreeSafe: %v", err)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Errorf("tree 应被删净，stat err = %v", err)
	}
}

// TestRemoveTreeSafeJunctionDefense 是 junction/符号链接防御（V10/陷阱6）的核心证据：
// 树里放一个指向"珍贵"目录的链接，删树时**绝不跟穿**——链接被删，目标完好。
func TestRemoveTreeSafeJunctionDefense(t *testing.T) {
	base := t.TempDir()

	// 珍贵目标（删树后必须存活）。
	precious := filepath.Join(base, "precious")
	mustMkdir(t, precious)
	marker := filepath.Join(precious, "marker.txt")
	writeFile(t, marker, "do-not-delete")

	// 待删的树：一个普通文件 + 一个指向 precious 的链接。
	tree := filepath.Join(base, "tree")
	mustMkdir(t, tree)
	writeFile(t, filepath.Join(tree, "plain.txt"), "x")
	makeLink(t, filepath.Join(tree, "link"), precious)

	if err := removeTreeSafe(tree); err != nil {
		t.Fatalf("removeTreeSafe: %v", err)
	}
	// 树应消失。
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Errorf("tree 应被删除，stat err = %v", err)
	}
	// 链接应被删（只删链接本身）。
	if _, err := os.Lstat(filepath.Join(tree, "link")); !os.IsNotExist(err) {
		t.Errorf("链接应被删除，lstat err = %v", err)
	}
	// 关键：珍贵目标未被跟穿删除。
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("junction/符号链接被跟穿了！marker 应仍在：%v", err)
	}
	if got := readFile(t, marker); got != "do-not-delete" {
		t.Errorf("marker 内容被改：%q", got)
	}
}

// TestRemoveTreeSafeNotExist：删不存在的路径不报错（幂等）。
func TestRemoveTreeSafeNotExist(t *testing.T) {
	if err := removeTreeSafe(filepath.Join(t.TempDir(), "nope")); err != nil {
		t.Errorf("删不存在路径不应报错：%v", err)
	}
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s：%v", dir, err)
	}
}
