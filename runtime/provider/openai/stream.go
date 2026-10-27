package openai

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// maxSSELineBytes 是单条 SSE 行的字节上限（8MB）。
// 作用只有一个：挂死/恶意的端点不能用"永不换行的无限长行"把我们的内存吃光。
// 正常 chunk 一行几百字节到几十 KB，8MB 有极大余量。
const maxSSELineBytes = 8 << 20

// lineReader 是按行读取 SSE 流的读取器。
//
// 为什么不用 bufio.Scanner：Scanner 有 token 上限（默认 64KB），调大 Buffer 也只是换个
// 常数；而且它把"末行无换行符 + EOF"合成一个 ErrFinalToken，语义要额外分辨。
// 这里用 ReadSlice 手工累积：长行安全、末行数据不丢、EOF 原样透出（调用方能 errors.Is）。
type lineReader struct {
	br *bufio.Reader
}

func newLineReader(r io.Reader) *lineReader {
	return &lineReader{br: bufio.NewReader(r)}
}

// readLine 读一行（含行尾换行符，由调用方 trim）。
//
// 返回值约定：
//   - ("...\n", nil)            正常一行；
//   - ("...（无换行）", nil)     流的最后一行没有换行符——**先把数据交付**，
//     下一次调用才返回 EOF（数据不丢）；
//   - ("", io.EOF)              流正常结束；
//   - ("", 其它 err)            读取失败（连接重置等），原样透出。
func (lr *lineReader) readLine() (string, error) {
	var buf []byte
	for {
		slice, err := lr.br.ReadSlice('\n')
		if len(buf)+len(slice) > maxSSELineBytes {
			return "", fmt.Errorf("openai: SSE 单行超过 %d 字节上限（端点行为异常）", maxSSELineBytes)
		}
		buf = append(buf, slice...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue // 一行还没读完，继续累积
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(buf) > 0 {
				return string(buf), nil
			}
			return "", err
		}
		return string(buf), nil
	}
}

// ssePrefix 是 SSE 数据字段前缀（规范里 "data:" 后可有一个可选空格）。
const ssePrefix = "data:"

// trimSSE 去掉行尾换行（Windows 侧可能是 \r\n）。
func trimSSE(line string) string {
	return strings.TrimRight(line, "\r\n")
}
