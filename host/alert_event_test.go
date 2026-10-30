package host

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Yorha9e/alasol/core"
	"github.com/Yorha9e/alasol/core/bus"
	"github.com/Yorha9e/alasol/runtime/store/sqlite"
)

// ---------------------------------------------------------------------------
// v2-S5 清账②：hub 伪 session/agent id 的**确定性派生**（p2-2 回归）
// ---------------------------------------------------------------------------

// TestHubIdentityIDsStableAcrossPathSpellings（p2-2）：同一份事件库，**写法不同**
// （相对 / 绝对 / 大小写 / 分隔符 / 软链）必须派生出**同一个**伪 id。
//
// 旧口径直接拿原始路径串去哈希：`alasol.db` / `./alasol.db` / `D:\x\alasol.db` /
// `d:/x/alasol.db` 派生出四个不同的伪 id → 同一个 hub 的告警被切成四条流，
// 旧口径留下的伪 session 还漏进在线表。
func TestHubIdentityIDsStableAcrossPathSpellings(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "events.db")
	if err := os.WriteFile(db, []byte("x"), 0o600); err != nil {
		t.Fatalf("写库文件: %v", err)
	}

	baseS, baseA := hubIdentityIDs(db)
	variants := []string{
		db,
		filepath.Clean(db),
		// 分隔符写法不同（Windows 上正斜杠也算同一路径）。
		filepath.ToSlash(db),
	}
	// 相对写法（按进程 cwd 解析）：与 cwd 不同盘时 Rel 报错 → 那几种写法跳过（不硬造）。
	if rel, err := filepath.Rel(".", db); err == nil {
		variants = append(variants, rel, "."+string(filepath.Separator)+rel)
	}
	for _, v := range variants {
		s, a := hubIdentityIDs(v)
		if s != baseS || a != baseA {
			t.Fatalf("写法 %q 派生出的伪 id 与 %q 不一致: (%v,%v) vs (%v,%v)", v, db, s, a, baseS, baseA)
		}
	}
	// 软链（能取则取）：指向同一个库文件 → 同一个伪 id。
	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(db, link); err != nil {
		t.Skipf("建不了符号链接（Windows 权限），跳过软链那一步: %v", err)
	}
	if s, a := hubIdentityIDs(link); s != baseS || a != baseA {
		t.Fatalf("软链路径应派生出同一个伪 id: (%v,%v) vs (%v,%v)", s, a, baseS, baseA)
	}
	// 不同的库 → 不同的伪 id（多 hub 各自一个库，G25）。
	osS, osA := hubIdentityIDs(filepath.Join(dir, "other.db"))
	if osS == baseS || osA == baseA {
		t.Fatal("不同库路径应派生不同伪 id")
	}
}

// TestHubIdentityIDsCaseVariantWhenLibraryMissing（p3-A 回归）：库**还没建**时
// （首次启动那一瞬间），同一个库的**大小写变体**必须派生出同一个伪 id。
//
// 为什么这一条重要：文件**存在**时 EvalSymlinks 已经给出磁盘上的规范大小写
// （Windows 实测：查 `events.db` 也回 `Events.db`），小写那步是冗余的；但文件
// **不存在**时 EvalSymlinks 直接失败 → 归一只剩 "Abs + 小写"——没有它，同一个库
// 的两种大小写写法会派生出两个伪 id（告警被切成两条流，旧伪 session 还漏进在线表）。
func TestHubIdentityIDsCaseVariantWhenLibraryMissing(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "NotYetCreated.db") // 故意**不创建**
	baseS, baseA := hubIdentityIDs(missing)
	variantS, variantA := hubIdentityIDs(strings.ToLower(missing))
	if runtime.GOOS == "windows" {
		// NTFS 大小写不敏感 → 同一个库 → 同一个伪 id。
		if baseS != variantS || baseA != variantA {
			t.Fatalf("库未建时大小写变体应派生出同一伪 id（Windows）: (%v,%v) vs (%v,%v)",
				baseS, baseA, variantS, variantA)
		}
		return
	}
	// 其它平台大小写敏感 → 大小写变体是**另一个文件** → 不同伪 id（如实记账）。
	if baseS == variantS {
		t.Fatal("非 Windows 平台：大小写变体是另一个文件，应派生不同伪 id")
	}
}

// TestHubIdentityIDsAreDistinctValidUlids：伪 id 必须是**合法且互不相同**的 ULID
// （envelope 强制要 session_id/agent_id；共用一个 id 会让"这是哪个 agent 的告警"含糊）。
func TestHubIdentityIDsAreDistinctValidUlids(t *testing.T) {
	sid, aid := hubIdentityIDs(filepath.Join(t.TempDir(), "events.db"))
	if !core.ValidUlid(sid) || !core.ValidUlid(aid) {
		t.Fatalf("伪 id 必须是合法 ULID: session=%q agent=%q", sid, aid)
	}
	if sid == aid {
		t.Fatal("session 与 agent 的伪 id 必须不同")
	}
	if len(sid) != 26 || len(aid) != 26 {
		t.Fatalf("伪 id 必须是 26 字符: session=%q agent=%q", sid, aid)
	}
}

// TestHubAlertEventIDIsDeterministic（p3-C 回归）：hub.alert 的事件 id 必须是
// **内容寻址**的（同一采样点的同一条告警恒得同一 id）。
//
// 为什么：告警入流失败会**重试**（F1：释放抑制槽位后下个周期重试）。随机 id 会让
// 重试落成**两条**一样的告警（同指标、同时刻、两条事实 = 复盘时数出"两条告警"）。
// 确定性 id → 事件库按 Event.ID 幂等去重（G11）→ 重试天然幂等。
func TestHubAlertEventIDIsDeterministic(t *testing.T) {
	at := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
	id1 := alertEventID("mem", at)
	id2 := alertEventID("mem", at)
	if id1 != id2 {
		t.Fatalf("同一采样点的告警 id 必须相同（内容寻址）: %q vs %q", id1, id2)
	}
	// 不同指标 / 不同采样点 → 不同 id。
	if alertEventID("cpu", at) == id1 {
		t.Fatal("不同指标的告警 id 必须不同")
	}
	if alertEventID("mem", at.Add(time.Second)) == id1 {
		t.Fatal("不同采样点的告警 id 必须不同")
	}
	if !core.ValidUlid(id1) {
		t.Fatalf("派生 id 必须是合法 ULID: %q", id1)
	}
}

// TestHubAlertRetryIsIdempotent（p3-C 回归，端到端）：同一采样点**重试**两次
// → 事件流里只落**一条** hub.alert（Event.ID 幂等去重，G11）。
func TestHubAlertRetryIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-alert.db")
	st, err := sqlite.Open(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	// G4：告警经 bus 发布（bus 内部 store.Append 入库 + 通知 WS 订阅者）。
	sink := newHubAlertSink(bus.New(st), path)

	ctx := context.Background()
	alert := Alert{
		At:     time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC),
		Metric: "mem", Value: 3 << 30, Threshold: 2 << 30,
		Message: "hub mem 超阈值",
	}
	// 第一次成功 + 第二次重试（模拟"写库失败后重试"，这里直接重放同一条）。
	for i := 0; i < 2; i++ {
		if err := sink.RecordAlert(ctx, alert); err != nil {
			t.Fatalf("RecordAlert #%d: %v", i+1, err)
		}
	}
	evs, err := st.ReadFrom(ctx, nil, 1, 100)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("同采样点重试只该落 1 条 hub.alert，得到 %d 条", len(evs))
	}
	if evs[0].Kind != KindHubAlert {
		t.Fatalf("事件 kind 不对: %+v", evs[0])
	}
}
