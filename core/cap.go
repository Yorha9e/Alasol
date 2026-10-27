package core

import (
	"context"
	"net/http"
	"time"
)

// 能力接口（P0-4）：core 只定义、不含任何 IO 实现；实现由 runtime 提供、host 装配时注入。
// 工具与扩展通过能力接口接触外界——声明需要什么能力（E3），由策略引擎裁决后注入。
// 换实现（本地进程 → 沙箱 → 远端）不动 core 与工具逻辑（K8）。

// ExecCapability：执行一条命令（同步等待）。
type ExecCapability interface {
	Run(ctx context.Context, spec ExecSpec) (ExecResult, error)
}

// ExecSpec 是一次命令执行的声明（不经过 shell；name 直接是可执行文件）。
type ExecSpec struct {
	Name string
	Args []string
	Dir  string
	Env  []string // 只含获准注入的键值（H17/H21）
}

// ExecResult 是命令执行结果。
type ExecResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}

// FSCapability：文件系统访问（路径由调用方守卫；V0 写路径边界见陷阱 8）。
type FSCapability interface {
	ReadFile(ctx context.Context, path string) ([]byte, error)
	WriteFile(ctx context.Context, path string, data []byte) error
	Stat(ctx context.Context, path string) (FileInfo, error)
	ListDir(ctx context.Context, path string) ([]string, error)
}

// FileInfo 是 Stat 的最小返回。
type FileInfo struct {
	Size    int64
	IsDir   bool
	ModTime time.Time
}

// NetCapability：出站网络（默认拒绝 + 域名白名单是策略层的事，H10；接口只管发）。
type NetCapability interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// ProcCapability：长驻进程管理（启动/等待/杀树；Windows 用 taskkill /T /F，陷阱 5）。
type ProcCapability interface {
	Start(ctx context.Context, spec ExecSpec) (ProcHandle, error)
}

// ProcHandle 是已启动进程的句柄。
type ProcHandle interface {
	PID() int
	Wait(ctx context.Context) (ExecResult, error)
	Kill(ctx context.Context) error
}
