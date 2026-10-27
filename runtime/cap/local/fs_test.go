package local

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFSWriteReadRoundTrip(t *testing.T) {
	fs := FS{}
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "sub", "a.txt")

	const content = "你好 local"
	if err := fs.WriteFile(ctx, path, []byte(content)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := fs.ReadFile(ctx, path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != content {
		t.Errorf("读回内容 = %q，期望 %q", got, content)
	}
	// 覆盖写（幂等：同内容重复写结果一致）。
	if err := fs.WriteFile(ctx, path, []byte(content)); err != nil {
		t.Fatalf("二次 WriteFile: %v", err)
	}
	again, err := fs.ReadFile(ctx, path)
	if err != nil || string(again) != content {
		t.Errorf("覆盖读 = %q err=%v", again, err)
	}
}

func TestFSWriteReadStatList(t *testing.T) {
	fs := FS{}
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "sub", "a.txt")

	const content = "你好，local 能力"
	if err := fs.WriteFile(ctx, path, []byte(content)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := fs.ReadFile(ctx, path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != content {
		t.Errorf("读回内容 = %q，期望 %q", got, content)
	}

	// 覆盖写（idempotent 语义：同内容重复写结果一致）。
	if err := fs.WriteFile(ctx, path, []byte(content)); err != nil {
		t.Fatalf("二次 WriteFile: %v", err)
	}
	again, err := fs.ReadFile(ctx, path)
	if err != nil || string(again) != content {
		t.Errorf("覆盖读 = %q err=%v", again, err)
	}

	fi, err := fs.Stat(ctx, path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.IsDir {
		t.Error("文件被 Stat 成目录")
	}
	if want := int64(len(content)); fi.Size != want {
		t.Errorf("Size = %d，期望 %d（按字节，不是 rune）", fi.Size, want)
	}
	if fi.ModTime.IsZero() {
		t.Error("ModTime 为零值")
	}

	di, err := fs.Stat(ctx, filepath.Join(dir, "sub"))
	if err != nil {
		t.Fatalf("Stat 目录: %v", err)
	}
	if !di.IsDir {
		t.Error("目录未被标为 IsDir")
	}

	// ListDir：只回条目名、按名排序（确定性）。
	names, err := fs.ListDir(ctx, dir)
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(names) != 1 || names[0] != "sub" {
		t.Errorf("ListDir = %v，期望 [sub]（条目名而非完整路径）", names)
	}
	if err := os.WriteFile(filepath.Join(dir, "z.txt"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	names, err = fs.ListDir(ctx, dir)
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	want := []string{"a.txt", "sub", "z.txt"}
	if len(names) != len(want) {
		t.Fatalf("ListDir = %v，期望 %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("ListDir 顺序 = %v，期望 %v（排序保证确定性）", names, want)
		}
	}
}

func TestFSErrors(t *testing.T) {
	fs := FS{}
	ctx := context.Background()
	dir := t.TempDir()

	if _, err := fs.ReadFile(ctx, filepath.Join(dir, "nope.txt")); err == nil {
		t.Error("读不存在的文件应报错")
	} else if !strings.Contains(err.Error(), "nope.txt") {
		t.Errorf("错误文案未带路径（模型要靠它自我修正，M5）：%v", err)
	}

	if err := fs.WriteFile(ctx, filepath.Join(dir, "no", "such", "dir", "a.txt"), []byte("x")); err == nil {
		t.Error("写进不存在的目录应报错")
	}
	if _, err := fs.Stat(ctx, filepath.Join(dir, "nope")); err == nil {
		t.Error("stat 不存在的路径应报错")
	}
	if _, err := fs.ListDir(ctx, filepath.Join(dir, "nope")); err == nil {
		t.Error("列举不存在的目录应报错")
	}
	// 对文件列目录也应报错（不是空列表）。
	f := filepath.Join(dir, "f.txt")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ListDir(ctx, f); err == nil {
		t.Error("对文件 ListDir 应报错")
	}
}

func TestFSContextCancelled(t *testing.T) {
	fs := FS{}
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fs.ReadFile(ctx, path); err == nil {
		t.Error("已取消的 ctx 应报错（不碰盘）")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v，期望 errors.Is Canceled", err)
	}
	if err := fs.WriteFile(ctx, filepath.Join(dir, "b.txt"), []byte("x")); !errors.Is(err, context.Canceled) {
		t.Errorf("WriteFile err = %v", err)
	}
	if _, err := fs.Stat(ctx, path); !errors.Is(err, context.Canceled) {
		t.Errorf("Stat err = %v", err)
	}
	if _, err := fs.ListDir(ctx, dir); !errors.Is(err, context.Canceled) {
		t.Errorf("ListDir err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "b.txt")); !os.IsNotExist(err) {
		t.Error("取消后不该落下任何文件")
	}
}

func TestFSNilContext(t *testing.T) {
	// nil ctx 不允许 panic：按 Background 处理（core 的 nil 语义：兜底而非 panic）。
	fs := FS{}
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	//nolint:staticcheck // 故意传 nil 验证不 panic
	if err := fs.WriteFile(nil, path, []byte("x")); err != nil {
		t.Fatalf("nil ctx WriteFile: %v", err)
	}
	//nolint:staticcheck // 同上
	if _, err := fs.ReadFile(nil, path); err != nil {
		t.Fatalf("nil ctx ReadFile: %v", err)
	}
}

// TestFSConcurrent 并发读写不同文件：验证无共享状态（-race 下跑）。
func TestFSConcurrent(t *testing.T) {
	fs := FS{}
	ctx := context.Background()
	dir := t.TempDir()
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			p := filepath.Join(dir, "f", string(rune('a'+i))+".txt")
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				done <- err
				return
			}
			if err := fs.WriteFile(ctx, p, []byte("x")); err != nil {
				done <- err
				return
			}
			if _, err := fs.ReadFile(ctx, p); err != nil {
				done <- err
				return
			}
			if _, err := fs.Stat(ctx, p); err != nil {
				done <- err
				return
			}
			done <- nil
		}(i)
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// TestFileInfoShape 固化 core.FileInfo 的映射（改字段名会在这里炸）。
func TestFileInfoShape(t *testing.T) {
	fs := FS{}
	dir := t.TempDir()
	p := filepath.Join(dir, "s.txt")
	if err := os.WriteFile(p, []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := fs.Stat(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size != 5 {
		t.Errorf("Size = %d", fi.Size)
	}
	if time.Since(fi.ModTime) > time.Minute {
		t.Errorf("ModTime 不合理：%v", fi.ModTime)
	}
}
