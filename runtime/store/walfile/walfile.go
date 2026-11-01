// Package walfile 实现 WAL 预写日志（v4-4-B）的**文件系统落地**：
// per-session 一个 `<session>.wal` 追加式 NDJSON 文件（与事件流同风格，一行一条记录）。
//
// # 定位：WAL 不是事实源（F5 不受影响）
//
// 事件流（core.Store）仍是唯一事实源。本包产出的一切都是**崩溃恢复的补充面**：
// 记录"正式写之前"的中间态，崩溃后至少知道"当时在干什么、吐过什么"。
//
//   - **可丢弃 / 可截断 / 可 GC**：删掉整个目录不丢任何事实（只丢"当时在干什么"这段
//     观察），事件流照常完整回放；
//   - **不自动补进事件流**：Reconcile 产出的报告是**观察面不是写回面**——里面的 delta
//     文本可供展示，绝不回填成 durable 事件（那就成了第二事实源，F5 与陷阱 11 一起破）；
//   - **不参与任何判定**：没有任何控制流读 WAL。
//
// # 磁盘布局
//
//	<dir>/<session_id>.wal        # NDJSON，一行一条 core.WalRecord（追加写）
//
// 与事件流同风格（一行一事实、可 tail、可 grep）；per-session 一个文件 → 单 session 的
// WAL 可以单独 truncate / 删除而不影响别的 session（GC 粒度，见下面的挂账）。
//
// # 写路径（Writer.Append）
//
// 每条记录 = json.Marshal(core.WalRecord) + "\n"，在**一把 mutex** 下一次性写出
// （一行一写 → 行与行之间不会交错；并发 Append 安全）。
//
// # fsync 策略：逐条不 fsync，轮次边界 fsync（取舍写明）
//
//   - **逐条 Append 只 write、不 fsync**。理由：delta 是逐条的，逐条 fsync 会把打字机
//     效果直接卡死（每条一次落盘 = 每秒几十次同步 IO）；而且**进程崩溃**（本刀要治的
//     那种崩溃：panic / kill -9 / OOM）根本不需要 fsync——write 一进 OS page cache，
//     进程死了数据还在，同机的下一个进程照样读得到。fsync 只对**机器掉电/OS 崩溃**有用。
//   - **轮次边界 fsync**（turn.begin / turn.end / turn.failed，以及 Close）：每轮一两次，
//     成本可忽略，换来"轮次边界在机器崩溃后也大概率在盘上"。
//   - **blob.plan 不 fsync**（critic F1）：它是**逐工具调用**的（一条工具调用一条），
//     拿它当边界等于在工具调用关键路径上插一次同步落盘；且它的内容是"预留"（体量未知），
//     精度本来就有限。计划写在 intent **之前**（X1 三段窗口之外），不占关键窗口。
//
// 代价（如实写明）：**机器掉电**时可能丢掉尾部若干条 delta（已 write 未 fsync 的那几条）。
//
//	这是有意的取舍——为掉电多保几条 delta 而让每次流式输出都同步落盘，是把补充面的重要性
//	置于主流程之上了。
//
// # 崩溃残留（.tmp 类）口径
//
// 本实现**不写临时文件**（一条记录就是一次 append，没有"写一半再 rename"的形状）→
// 不会产生自己的 .tmp 残留。目录里可能出现的是：
//
//   - **半截行**（进程在 write 中途被杀）：扫描时容忍——最后一行不完整/不可解析 → 计入
//     CorruptLines 并跳过，**不当错误**（崩溃后的 WAL 本来就该容忍半截；为它报错等于
//     崩溃恢复第一步就失败）；
//   - 别的实现/旧版本留下的 .tmp：本包**不主动清理**（不猜别人的文件是谁的），
//     但 ReadSession 只认 `<session>.wal` 一个名字，残留文件不会被误当成 WAL 读。
//
// # GC / 截断：**挂账，未做**
//
// 本包只提供 Truncate（截断成空文件）与 Remove（删整个 session 的 WAL）两个原语，
// **不提供任何自动策略**：什么时候算"这个 session 的 WAL 没用了"（事件流已有 completed？
// session 已归档？N 天前？）是产品决策，需要"孤儿 blob 清理"一起定（两者共用同一批对账
// 键）。挂账见 11 施工计划 v4-4 行。
//
// # 关 = 零行为变化（酸测试）
//
// 目录未配置（host.Config.WalDir 空）→ hub 不建本包的任何对象 → 一个文件都不建、
// 一次追加都不做、行为与配置前逐事件等价。这是默认路径。
package walfile

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core"
)

// walSuffix 是 per-session WAL 文件名后缀（布局：<session>.wal）。
const walSuffix = ".wal"

// maxLineBytes 是单行 WAL 记录的读取上限（1MB）。
//
// 为什么要有上限：防一条无界的行把内存吃满（崩溃现场可能有畸形行）。
//
// **超限不中断扫描**（critic F3）：超限的行**跳过并继续读后面的行**——
//   - 旧实现用 bufio.Scanner：它的错误是**粘性**的（ErrTooLong 之后每次 Scan 都失败，
//     Err() 不变）→ **后续记录全丢**（"崩溃后读不了自己的 WAL"）；
//   - 现在用 bufio.Reader 自己读行（与 runtime/acp 的 frame.go 同款）：读到行尾为止，
//     丢弃超限的那一行，**继续读下一行**；超限行计入 CorruptLines（如实计数，不静默）。
//
// 1MB 是给 delta 片段留的余量（一条 delta 通常是几十到几百字节）。
const maxLineBytes = 1 << 20

// walReadBufSize 是读 WAL 的行缓冲大小（bufio.Reader）。
//
// 只影响读的分块粒度（不影响单行上限 maxLineBytes）：默认 4KB 太碎——一条带长 delta 的
// 记录会被切成很多块。64KB 是个折中（内存有界 + 少 syscall）。
const walReadBufSize = 64 * 1024

// 编译期证明：*Writer 就是 core.WalSink（换实现面不变，与 storage.Blob 同款纪律）。
var _ core.WalSink = (*Writer)(nil)

// walFile 是 Writer 依赖的**最小文件面**（*os.File 满足它）。
//
// # 为什么要抽成接口（F4 的观测面）
//
// fsync 在**黑盒面不可观测**：外部测试只能证明"关完不丢"（数据完整可读），证不了
// "Close 真的调了 fsync、且在 close **之前**调"（那两件事的差别在机器掉电时才显形）。
// 抽出接口后，测试可注入一个**记账的假文件** → "fsync 调了几次、先后如何"变成可断言的
// 事实（见同包的 sync_seam_test.go）。
//
// 面只留三件事：写、fsync、关——不多不少（不引入"文件改名/截断"这类本包用不到的能力）。
type walFile interface {
	io.Writer
	Sync() error
	Close() error
}

// Writer 是一个 per-session 的 WAL 追加写入口（并发安全）。
//
// 一个 session 一个 Writer（一个文件）；多个 session 各持各的（ Store 负责分发与收口）。
type Writer struct {
	path string

	// mu 护住"一行一写"的原子性（并发 Append 时行与行不交错）与 fsync 的临界区。
	mu sync.Mutex
	f  walFile
	// closed 保证 Close 幂等（Close 之后再 Append 返回错误，不当 panic）。
	closed bool
}

// Store 管理一个 WAL 目录下的 per-session Writer（并发安全）。
//
// hub 持有一个（nil = 未启用 = 零行为变化）；Writer 按 session 懒建（第一次用到那个
// session 才开文件），Close 时统一收口。
type Store struct {
	dir string

	mu      sync.Mutex
	writers map[contracts.Ulid]*Writer
}

// New 建一个 WAL 目录管理器并确保目录存在（幂等：已存在直接用）。
//
// dir 空 → 拒绝（没有落地目录就无从谈 WAL；"空 = 关"由调用方判，不在这里静默放行）。
func New(dir string) (*Store, error) {
	d := strings.TrimSpace(dir)
	if d == "" {
		return nil, errors.New("walfile: WAL 目录必填（空 = 关闭 WAL，由调用方判）")
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		return nil, fmt.Errorf("walfile: 建 WAL 目录 %q: %w", d, err)
	}
	return &Store{dir: d, writers: map[contracts.Ulid]*Writer{}}, nil
}

// Dir 返回 WAL 根目录（诊断用）。
func (s *Store) Dir() string { return s.dir }

// Path 返回某 session 的 WAL 文件路径（**不建文件**：纯路径推导，读面/诊断用）。
func (s *Store) Path(sessionID contracts.Ulid) string {
	return filepath.Join(s.dir, string(sessionID)+walSuffix)
}

// Writer 取（或建）某 session 的 WAL 追加写入口。
//
// sessionID 必须是合法 ULID（防路径穿越：contracts.Ulid 只是字符串类型；与 blobfs 同口径
// ——不合则拒，不修复）。
func (s *Store) Writer(sessionID contracts.Ulid) (*Writer, error) {
	if !core.ValidUlid(sessionID) {
		return nil, fmt.Errorf("walfile: session id %q 不是合法 ULID（26 字符 Crockford base32）", string(sessionID))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.writers[sessionID]; ok {
		return w, nil
	}
	w, err := newWriter(s.Path(sessionID))
	if err != nil {
		return nil, err
	}
	s.writers[sessionID] = w
	return w, nil
}

// Close 收口本管理器建过的所有 Writer（幂等；逐个 fsync + close）。
//
// 错误用 errors.Join 汇总（一个 session 关失败不该让别的 session 不关）。
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	writers := s.writers
	s.writers = map[contracts.Ulid]*Writer{}
	s.mu.Unlock()
	var errs []error
	for _, w := range writers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// newWriter 开（或建）一个 WAL 文件供追加。
func newWriter(path string) (*Writer, error) {
	// O_APPEND：每条记录都追加到尾部（与"一行一写"的 mutex 配合 → 行不相交）。
	// 0o600：WAL 里有模型输出片段（可能含敏感内容）——不给同机其他用户读。
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("walfile: 开 WAL 文件 %q: %w", path, err)
	}
	return &Writer{path: path, f: f}, nil
}

// Path 返回本 Writer 的文件路径（诊断用）。
func (w *Writer) Path() string { return w.path }

// Append 追加一条 WAL 记录（core.WalSink 的实现）。
//
// 序列化失败 → 返错（调用方按尽力为之处理：一次 Warn，不阻断主流程）。
// 轮次边界 kind（turn.begin / turn.end / turn.failed）额外 fsync（见包 doc 的取舍）；
// 其余（turn.delta / blob.plan）只 write 不 fsync。
func (w *Writer) Append(kind string, payload any) error {
	if w == nil {
		return errors.New("walfile: Writer 为 nil")
	}
	rec := core.WalRecord{Kind: kind, TS: nowRFC3339()}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("walfile: 序列化 %s 记录的 payload: %w", kind, err)
		}
		rec.Payload = raw
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("walfile: 序列化 %s 记录: %w", kind, err)
	}
	line = append(line, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("walfile: WAL 文件 %q 已关闭（不再接受追加）", w.path)
	}
	if _, err := w.f.Write(line); err != nil {
		return fmt.Errorf("walfile: 追加 %s 记录到 %q: %w", kind, w.path, err)
	}
	if isBoundaryKind(kind) {
		if err := w.f.Sync(); err != nil {
			// fsync 失败**不**算这条没写下（write 已进 OS cache，进程崩溃仍可恢复）——
			// 只对"机器掉电"的保证降级。如实返错让调用方留痕，但不夸大事实。
			return fmt.Errorf("walfile: fsync %s 记录（已 write，机器掉电可能丢尾部）: %w", kind, err)
		}
	}
	return nil
}

// Sync 把已 write 的记录 fsync（供调用方在自选的时机落盘；轮次边界已自动做过）。
func (w *Writer) Sync() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil // 已 Close（里面已 fsync 过）→ 幂等成功
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("walfile: fsync %q: %w", w.path, err)
	}
	return nil
}

// Close fsync + 关闭文件（幂等；关闭后 Append 返错）。
//
// **先 fsync 再 close**（critic F4）：文档（包头 / Store.Close / 本函数旧注释）都写着
// "Close fsync"，但旧实现直接 close → 机器掉电时**已 write 未 fsync 的尾部记录可能丢**
// （进程崩溃不丢，掉电丢）。fsync 一次的成本可忽略（收口路径），换来"关完就稳"。
//
// fsync 失败**不掩盖** close 的结果：两个错误都报出来（errors.Join）。
func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	// 先 fsync（把已 write 的内容真正刷到盘上），再关 fd。
	syncErr := w.f.Sync()
	closeErr := w.f.Close()
	switch {
	case syncErr != nil && closeErr != nil:
		return fmt.Errorf("walfile: 关 WAL 文件 %q: fsync 失败: %v；close 失败: %w", w.path, syncErr, closeErr)
	case syncErr != nil:
		return fmt.Errorf("walfile: fsync WAL 文件 %q（已 close；机器掉电可能丢尾部）: %w", w.path, syncErr)
	case closeErr != nil:
		return fmt.Errorf("walfile: 关 WAL 文件 %q: %w", w.path, closeErr)
	}
	return nil
}

// isBoundaryKind 判定是否是"结构边界"kind（这些才 fsync；其余只 write，见包 doc）。
//
// 边界 = **轮次边界**（turn.begin / turn.end / turn.failed）：每轮一两次，成本可忽略，
// 换来"轮次边界在机器崩溃后也大概率在盘上"。
//
// **blob.plan 不是边界**（critic F1）：计划写在 intent **之前**（X1 三段窗口之外），但它
// 是**逐工具调用**发生的（一条工具调用一条）——拿它当 fsync 边界等于在工具调用关键路径
// 上加一次同步落盘。计划的内容是"预留"（体量未知），精度本来就有限，不值得为它付 fsync。
func isBoundaryKind(kind string) bool {
	switch kind {
	case core.KindWalTurnBegin, core.KindWalTurnEnd, core.KindWalTurnFailed:
		return true
	}
	return false
}

// Truncate 把某 session 的 WAL 截成空文件（**原语，不自动调**：GC 策略挂账，见包 doc）。
//
// 文件不存在 → 幂等成功（"已经没了"就是目标状态）。在途 Writer 的 fd 仍有效
// （O_APPEND + 截断 → 后续追加从 0 开始；调用方须自己保证此刻没有并发送入）。
func Truncate(dir string, sessionID contracts.Ulid) error {
	if !core.ValidUlid(sessionID) {
		return fmt.Errorf("walfile: session id %q 不是合法 ULID", string(sessionID))
	}
	p := filepath.Join(strings.TrimSpace(dir), string(sessionID)+walSuffix)
	if err := os.Truncate(p, 0); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("walfile: 截断 WAL %q: %w", p, err)
	}
	return nil
}

// Remove 删掉某 session 的 WAL 文件（**原语，不自动调**：GC 策略挂账，见包 doc）。
// 不存在 → 幂等成功。
func Remove(dir string, sessionID contracts.Ulid) error {
	if !core.ValidUlid(sessionID) {
		return fmt.Errorf("walfile: session id %q 不是合法 ULID", string(sessionID))
	}
	p := filepath.Join(strings.TrimSpace(dir), string(sessionID)+walSuffix)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("walfile: 删 WAL %q: %w", p, err)
	}
	return nil
}

// ScanResult 是扫描一个 WAL 文件的产物（含**诚实计数**：损坏/半截行数）。
type ScanResult struct {
	// Path 是被扫的文件路径。
	Path string `json:"path"`
	// Exists 是文件是否存在（false = 这个 session 没有 WAL，Records 为空）。
	Exists bool `json:"exists"`
	// Records 是解析成功的记录（按写入序）。
	Records []core.WalRecord `json:"records,omitempty"`
	// CorruptLines 是**不完整/不可解析**的行数（半截行 / 坏 JSON）。
	//
	// 为什么不静默跳过：崩溃后的 WAL 有半截行是**正常**的（进程在 write 中途被杀），
	// 但"有几行读不了"这件事本身要看得见——否则损坏被当成"本来就没写"。
	CorruptLines int `json:"corrupt_lines"`
}

// ReadSession 扫某 session 的 WAL 文件（dir 下 <session>.wal）。
//
// 文件不存在 → (空 ScanResult, false, nil)：不是错误（没有 WAL = 没配/没写过）。
func ReadSession(dir string, sessionID contracts.Ulid) (ScanResult, error) {
	if !core.ValidUlid(sessionID) {
		return ScanResult{}, fmt.Errorf("walfile: session id %q 不是合法 ULID", string(sessionID))
	}
	p := filepath.Join(strings.TrimSpace(dir), string(sessionID)+walSuffix)
	return ReadFile(p)
}

// ReadFile 扫一个 WAL 文件（半截/坏行计入 CorruptLines，不当错误——见包 doc）。
//
// **超长行不中断扫描**（critic F3）：一行超过 maxLineBytes → 丢弃该行、计入 CorruptLines、
// **继续读后面的行**（旧实现用 bufio.Scanner，超限后整个扫描停掉，后续记录全丢）。
func ReadFile(path string) (ScanResult, error) {
	out := ScanResult{Path: path}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil // 没有 WAL：合法状态
		}
		return out, fmt.Errorf("walfile: 开 WAL %q: %w", path, err)
	}
	defer f.Close()
	out.Exists = true

	br := bufio.NewReaderSize(f, walReadBufSize)
	for {
		line, tooLong, err := readWALLine(br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // 流结束
			}
			// 底层读错误：把已读到的部分带回去 + 报错。调用方（恢复报告）选择
			// "用部分结果 + 说明"而不是整体失败——崩溃恢复的第一步不该被一行坏数据堵死。
			return out, fmt.Errorf("walfile: 扫 WAL %q: %w", path, err)
		}
		if tooLong {
			out.CorruptLines++ // 超长行：丢弃并计数，**继续读后面的行**
			continue
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			continue // 空行：跳过（不计损坏——可能是外部工具加的尾换行）
		}
		var rec core.WalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			out.CorruptLines++
			continue
		}
		if strings.TrimSpace(rec.Kind) == "" {
			out.CorruptLines++ // 没有 kind 的行对不上账，按损坏计
			continue
		}
		out.Records = append(out.Records, rec)
	}
	return out, nil
}

// readWALLine 读**一行**（不含换行符）。
//
//	(line, false, nil)   → 一行
//	(nil, true, nil)     → 本行**超上限**（已跳到行尾丢弃；**继续读后面的行**）
//	(nil, false, io.EOF) → 流结束
//
// 为什么不用 bufio.Scanner：Scanner 的错误是**粘性**的——一旦 ErrTooLong（行超限），
// 之后每次 Scan 都直接返回 false 且 Err() 不变 → 调用方无法区分"这一行坏了"与"整个流
// 死了"，而实际后果是**后面的记录全丢**（崩溃恢复读不了自己的 WAL）。本实现与
// runtime/acp 的 frame.go 同款：自己按行读，超限就丢弃该行并继续。
func readWALLine(br *bufio.Reader) (line []byte, tooLong bool, err error) {
	var acc []byte
	for {
		chunk, rerr := br.ReadSlice('\n')
		if len(chunk) > 0 && !tooLong {
			body := len(chunk)
			if body > 0 && chunk[body-1] == '\n' {
				body-- // 行分隔符不算额度（与 acp/frame.go 同口径）
			}
			if len(acc)+body > maxLineBytes {
				// 超限：停止累积（这行的内容直接丢），但**继续读到行尾**——
				// 不能停在行首（否则后面的记录会被当成这行的尾巴一起丢）。
				tooLong = true
				acc = nil
			} else {
				acc = append(acc, chunk...)
			}
		}
		switch {
		case rerr == nil:
			// 读到换行：一行读完。
			if tooLong {
				return nil, true, nil
			}
			return trimEOL(acc), false, nil
		case errors.Is(rerr, bufio.ErrBufferFull):
			// 行比**读缓冲**还长（≠ 超上限）：继续读下一块并累积
			//（累积到 maxLineBytes 由上面的长度检查收口——内存有界）。
			continue
		case errors.Is(rerr, io.EOF):
			if len(acc) > 0 {
				// 末行没有换行（宽容：仍按一行处理）。
				if tooLong {
					return nil, true, nil
				}
				return trimEOL(acc), false, nil
			}
			if tooLong {
				return nil, true, nil
			}
			return nil, false, io.EOF // 流结束
		default:
			return nil, false, rerr // 底层读错误
		}
	}
}

// trimEOL 去掉行尾的换行符（以及它前面的回车——容忍 CRLF）。
func trimEOL(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}

// nowRFC3339 返回 UTC RFC 3339 时间戳（WalRecord.TS；诊断用，不参与判定）。
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
