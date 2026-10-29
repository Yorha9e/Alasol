package sandbox

import (
	"context"
	"fmt"
	"os"

	"github.com/Yorha9e/alasol/core"
)

// FS 是沙箱模式下的进程内文件访问（read / write 工具走这里）。
//
// # 它**不是** OS 强制的那一层
//
// read / write 工具跑在宿主进程身份下，不受子进程受限 token 的约束——OS ACL 只对
// 以受限 token 起的子进程生效。所以这里做的是 **fail-close 的便利层**：
// 把"允许集"作为 harness 内检查执行一遍，让进程内工具与沙箱语义一致。
//
// 真正的强制层是：子进程（bash / cmd / git …）以受限 token 跑 + 允许根的 DACL。
// 别把这一类检查当成边界——runtime/cap/local 的包文档已经讲清"能力层偷偷加一层
// 看起来安全的校验比没有更危险"，这里加它的**唯一**理由是"沙箱模式下进程内工具
// 也该有同一个世界观"，并且错误信息直说"这是沙箱允许集"。
type FS struct {
	env *Env
}

// softCheckNote 是所有"允许集之外"错误都要带的标注（F19）。
//
// 为什么必须标：用户看到"沙箱拒绝"时，分不清"内核拒的"（bash 子进程，真边界）与
// "我们查的"（read/write 工具，进程内便利层）就会误判安全强度——
// 这正是 runtime/cap/local 包文档里点名批评的"看起来很安全的校验"。
const softCheckNote = "（进程内软检查，非 OS 边界）"

// 编译期断言：FS 必须满足 core.FSCapability。
var _ core.FSCapability = FS{}

// ReadFile implements core.FSCapability。
//
// ctx 已结束时直接返回错误（不碰盘）——同 local 的理由：大文件读取会把取消信号压后很久。
func (f FS) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if err := f.env.checkAllowed("读取", path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sandbox: 读取文件失败（path=%s）：%w", path, err)
	}
	return data, nil
}

// WriteFile implements core.FSCapability。
//
// 权限 0o644（尊重 umask）。**不是原子写**——同 local 的已知边界：写到一半崩溃会留下
// 截断文件；要原子性请在工具层用"写临时文件 + rename"两段式。
func (f FS) WriteFile(ctx context.Context, path string, data []byte) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if err := f.env.checkAllowed("写入", path); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("sandbox: 写入文件失败（path=%s）：%w", path, err)
	}
	return nil
}

// Stat implements core.FSCapability。
func (f FS) Stat(ctx context.Context, path string) (core.FileInfo, error) {
	if err := ctxErr(ctx); err != nil {
		return core.FileInfo{}, err
	}
	if err := f.env.checkAllowed("stat", path); err != nil {
		return core.FileInfo{}, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return core.FileInfo{}, fmt.Errorf("sandbox: stat 失败（path=%s）：%w", path, err)
	}
	return core.FileInfo{Size: fi.Size(), IsDir: fi.IsDir(), ModTime: fi.ModTime()}, nil
}

// ListDir implements core.FSCapability。
//
// 返回**条目名**（不是完整路径）——同 local：让调用方自己拼，避免本包对"路径怎么拼"做假设。
func (f FS) ListDir(ctx context.Context, path string) ([]string, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if err := f.env.checkAllowed("列举", path); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("sandbox: 列举目录失败（path=%s）：%w", path, err)
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, nil
}

// FS 返回进程内文件能力（带允许集检查；见类型注释的诚实边界）。
func (e *Env) FS() FS { return FS{env: e} }

// ctxErr 把 ctx 状态转成带定位的错误（nil ctx 视为 Background，绝不 panic）。
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sandbox: 上下文已结束（%w）", err)
	}
	return nil
}
