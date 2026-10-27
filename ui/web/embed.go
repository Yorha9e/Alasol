// Package web 持有 Web UI 的**静态资源**（v0 单文件 index.html，O5）。
//
// 纪律：
//   - 这里只有资源与取资源的窄接口，**不含任何渲染/判断逻辑**（09 的边界：
//     UI 只渲染 core 给出的结论，不判断语义）；
//   - 资源用 //go:embed 编译进二进制 → "单二进制即用"，hub 不需要外部文件；
//   - 前端零构建工具链、零框架、零 CDN（离线可用）——改完直接刷新页面。
//
// 谁用它：host（唯一组合根）在 GET / 上服务它；cmd/alasol 不直接碰。
package web

import (
	"embed"
	"fmt"
)

// files 是嵌入的静态资源。新增前端文件时同步加进 embed 模式。
//
//go:embed index.html
var files embed.FS

// FS 返回嵌入的静态文件系统（host 用它服务 GET / 与未来的静态子路径）。
func FS() embed.FS { return files }

// IndexHTML 返回 index.html 的字节（GET / 用它；读失败是编译期就排除了的
// embed 内容缺失，只在极端情况下返回 error）。
func IndexHTML() ([]byte, error) {
	b, err := files.ReadFile("index.html")
	if err != nil {
		return nil, fmt.Errorf("web: 读嵌入的 index.html: %w", err)
	}
	return b, nil
}
