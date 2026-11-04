package sessioncfg_test

// core/sessioncfg 的验收面（G-c fold 表）：
//
//	① 逐字段 LWW：乱序 / 重复 Apply 不倒退、后写覆盖先写、字段缺失不动；
//	② 三态语义：未写过 / 显式空串清除（回 hub 默认）/ 显式覆盖 可区分；
//	③ provider 子对象：字段级 LWW + "两字段都空串 = 清除覆盖" + 缺失 = 不变；
//	④ 坏载荷跳过（D7 宽容）+ 非 config.changed / 非 durable / nil 一律不吃；
//	⑤ Generation 按事件 id 去重（幂等重放不重数——确定性事件 id 的届数源）。

import (
	"encoding/json"
	"testing"

	"github.com/Yorha9e/alasol/contracts"
	"github.com/Yorha9e/alasol/core/sessioncfg"
)

// ev 造一条待折叠的 config.changed 事件（seq/ts 可控 → 乱序用例的判别位）。
func ev(t *testing.T, id string, seq int64, ts string, payload any) *contracts.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &contracts.Event{
		V:         1,
		ID:        contracts.Ulid(id),
		Kind:      sessioncfg.KindConfigChanged,
		TS:        ts,
		SessionID: "S1",
		AgentID:   "A1",
		Durable:   true,
		Payload:   raw,
		Seq:       &seq,
	}
}

func str(s string) *string { return &s }

// TestApplyFieldLevelLWW：乱序 / 重复 Apply 下逐字段 last-write-wins。
//
// seq 大的后到也盖得住 seq 小的先到（乱序容忍）；同一条事件重复投递结果不变
// （幂等）；某事件只带 model 时 effort 字段不动（缺失 = 不变）。
func TestApplyFieldLevelLWW(t *testing.T) {
	tab := sessioncfg.New()

	// 先到 seq=2（model=m2, effort=high），再倒回 seq=1（model=m1, 无 effort）。
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{
		Model: str("m2"), ReasoningEffort: str("high"),
	}))
	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{
		Model: str("m1"),
	}))

	got := tab.Current("S1")
	if got.Model.Value != "m2" || !got.Model.Written {
		t.Fatalf("乱序后 model = (%v, %v)，want (m2, true)——seq=1 的旧事件盖不掉 seq=2",
			got.Model.Written, got.Model.Value)
	}
	if got.ReasoningEffort.Value != "high" || !got.ReasoningEffort.Written {
		t.Fatalf("effort 应保留 seq=2 的覆盖（seq=1 缺失 = 不变）: %+v", got.ReasoningEffort)
	}

	// 重复投递同一条事件（同 id 同 seq 同 ts）：结果不变（幂等）。
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{
		Model: str("m2"), ReasoningEffort: str("high"),
	}))
	again := tab.Current("S1")
	if again.Model.Value != "m2" || again.ReasoningEffort.Value != "high" {
		t.Fatalf("重复 Apply 后状态漂移: %+v", again)
	}
	// 重复投递不推进届数（按事件 id 去重）。
	if n := tab.Generation("S1"); n != 2 {
		t.Fatalf("Generation = %d，want 2（E1/E2 各一，重复投递不重数）", n)
	}

	// 后写覆盖先写（正常顺序）：seq=3 把 model 改成 m3。
	tab.Apply(ev(t, "E3", 3, "2026-11-03T10:00:03Z", contracts.ConfigChangedPayload{Model: str("m3")}))
	if got := tab.Current("S1"); got.Model.Value != "m3" {
		t.Fatalf("后写未覆盖先写: model = %q, want m3", got.Model.Value)
	}
}

// TestApplyClearAndTriState：空串清除的三态语义（未写过 / 显式清除 / 显式覆盖）。
func TestApplyClearAndTriState(t *testing.T) {
	tab := sessioncfg.New()

	// 未写过：Written=false（与"显式清除"可区分）。
	if got := tab.Current("S1"); got.Model.Written {
		t.Fatalf("从未写过的字段不该标 Written: %+v", got.Model)
	}

	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{Model: str("m2")}))
	if got := tab.Current("S1"); !got.Model.HasValue() || got.Model.Value != "m2" {
		t.Fatalf("覆盖后应 HasValue: %+v", got.Model)
	}

	// 显式空串清除：Written=true、Value=""、HasValue=false（生效回 hub 默认，
	// 但审计上仍看得出"曾覆盖过、后来被清掉"——与"未覆盖"区分）。
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{Model: str("")}))
	got := tab.Current("S1")
	if !got.Model.Written || got.Model.Value != "" || got.Model.HasValue() {
		t.Fatalf("空串清除后应 (Written=true, Value=\"\", HasValue=false): %+v", got.Model)
	}
}

// TestApplyProviderSubObject：provider 子对象的字段级折叠。
//
//	① 只带 base_url（api_key_env 缺失）→ base_url 覆盖、api_key_env 不动；
//	② 两字段都空串 = 清除 provider 覆盖（Touched 仍在，字段全部回到未覆盖值）；
//	③ provider 缺失 = 整体不变；
//	④ 后续只改 api_key_env → base_url 不动（字段级 LWW 互不干扰）。
func TestApplyProviderSubObject(t *testing.T) {
	tab := sessioncfg.New()

	// ① 只带 base_url。
	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{
		Provider: &contracts.ProviderConfigChange{BaseURL: str("http://a.example")},
	}))
	got := tab.Current("S1")
	if !got.Provider.Touched || got.Provider.BaseURL.Value != "http://a.example" {
		t.Fatalf("① base_url 未折进来: %+v", got.Provider)
	}
	if got.Provider.APIKeyEnv.Written {
		t.Fatalf("① api_key_env 缺失 = 不该有记录: %+v", got.Provider.APIKeyEnv)
	}

	// ④ 后续只改 api_key_env → base_url 不动。
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{
		Provider: &contracts.ProviderConfigChange{APIKeyEnv: str("KEY_A")},
	}))
	got = tab.Current("S1")
	if got.Provider.BaseURL.Value != "http://a.example" || got.Provider.APIKeyEnv.Value != "KEY_A" {
		t.Fatalf("④ 字段级 LWW 互不干扰失败: %+v", got.Provider)
	}

	// ③ provider 缺失 = 整体不变。
	tab.Apply(ev(t, "E3", 3, "2026-11-03T10:00:03Z", contracts.ConfigChangedPayload{Model: str("m2")}))
	got = tab.Current("S1")
	if got.Provider.BaseURL.Value != "http://a.example" || got.Provider.APIKeyEnv.Value != "KEY_A" {
		t.Fatalf("③ provider 缺失时不该被改动: %+v", got.Provider)
	}

	// ② 两字段都空串 = 清除 provider 覆盖。
	tab.Apply(ev(t, "E4", 4, "2026-11-03T10:00:04Z", contracts.ConfigChangedPayload{
		Provider: &contracts.ProviderConfigChange{BaseURL: str(""), APIKeyEnv: str("")},
	}))
	got = tab.Current("S1")
	if !got.Provider.Touched {
		t.Fatalf("② 清除记录仍应 Touched=true: %+v", got.Provider)
	}
	if got.Provider.BaseURL.Written && got.Provider.BaseURL.HasValue() {
		t.Fatalf("② base_url 应回到无生效覆盖: %+v", got.Provider.BaseURL)
	}
	if got.Provider.APIKeyEnv.HasValue() {
		t.Fatalf("② api_key_env 应回到无生效覆盖: %+v", got.Provider.APIKeyEnv)
	}
	// model 不受 provider 清除影响（字段彼此独立）。
	if got.Model.Value != "m2" {
		t.Fatalf("② provider 清除不该动 model: %+v", got.Model)
	}
}

// TestApplyBadPayloadAndForeignEvents：坏载荷跳过 + 非目标事件一律不吃（D7）。
func TestApplyBadPayloadAndForeignEvents(t *testing.T) {
	tab := sessioncfg.New()
	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{Model: str("m2")}))

	// 坏载荷：跳过专属投影（状态不动），不 panic。
	bad := ev(t, "EBAD", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{})
	bad.Payload = []byte("{not json")
	tab.Apply(bad)
	if got := tab.Current("S1"); got.Model.Value != "m2" {
		t.Fatalf("坏载荷不该改动状态: %+v", got.Model)
	}

	// 其余 kind（哪怕 durable、带同型载荷）不进本表。
	foreign := ev(t, "E2", 3, "2026-11-03T10:00:03Z", contracts.ConfigChangedPayload{Model: str("m3")})
	foreign.Kind = "user.prompt"
	tab.Apply(foreign)
	if got := tab.Current("S1"); got.Model.Value != "m2" {
		t.Fatalf("非 config.changed 不该进表: %+v", got.Model)
	}

	// 非 durable 不进表（F8：流事件不是事实）。
	stream := ev(t, "E3", 4, "2026-11-03T10:00:04Z", contracts.ConfigChangedPayload{Model: str("m4")})
	stream.Durable = false
	tab.Apply(stream)
	if got := tab.Current("S1"); got.Model.Value != "m2" {
		t.Fatalf("非 durable 不该进表: %+v", got.Model)
	}

	// nil 事件 / 空 session_id：安全空转。
	tab.Apply(nil)
	noSid := ev(t, "E4", 5, "2026-11-03T10:00:05Z", contracts.ConfigChangedPayload{Model: str("m5")})
	noSid.SessionID = ""
	tab.Apply(noSid)
	if got := tab.Current("S1"); got.Model.Value != "m2" {
		t.Fatalf("nil / 空 session_id 不该改动状态: %+v", got.Model)
	}

	// 未知 session → 零值（全"未覆盖"）。
	if got := tab.Current("NOPE"); got.Model.Written || got.Provider.Touched {
		t.Fatalf("未知 session 应返回零值: %+v", got)
	}
}

// TestGenerationAndReset：届数计数（幂等 + 按 id 去重）与 Reset 重建入口。
func TestGenerationAndReset(t *testing.T) {
	tab := sessioncfg.New()
	if n := tab.Generation("S1"); n != 0 {
		t.Fatalf("空表 Generation = %d, want 0", n)
	}
	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{Model: str("m1")}))
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{Model: str("m2")}))
	// 重复投递 E2：届数不重数（确定性事件 id 的届数必须稳定）。
	tab.Apply(ev(t, "E2", 2, "2026-11-03T10:00:02Z", contracts.ConfigChangedPayload{Model: str("m2")}))
	if n := tab.Generation("S1"); n != 2 {
		t.Fatalf("Generation = %d, want 2", n)
	}
	if n := tab.Generation("OTHER"); n != 0 {
		t.Fatalf("未知 session 的 Generation = %d, want 0", n)
	}

	// Reset 后回到空表（重建入口：宿主随后从 seq 0 重放）。
	tab.Reset()
	if got := tab.Current("S1"); got.Model.Written {
		t.Fatalf("Reset 后不该有折叠态: %+v", got.Model)
	}
	if n := tab.Generation("S1"); n != 0 {
		t.Fatalf("Reset 后 Generation = %d, want 0", n)
	}
	// Reset 后照常可喂（懒建）。
	tab.Apply(ev(t, "E9", 9, "2026-11-03T10:00:09Z", contracts.ConfigChangedPayload{Model: str("m9")}))
	if got := tab.Current("S1"); got.Model.Value != "m9" {
		t.Fatalf("Reset 后 Apply 失效: %+v", got.Model)
	}
}

// TestZeroValueTableUsable：零值 Table 也能用（懒建 map；nil 接收者安全）。
func TestZeroValueTableUsable(t *testing.T) {
	var tab *sessioncfg.Table // nil：所有方法安全空转
	tab.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{Model: str("m1")}))
	if got := tab.Current("S1"); got.Model.Written {
		t.Fatalf("nil 表 Current 应为零值: %+v", got.Model)
	}
	if n := tab.Generation("S1"); n != 0 {
		t.Fatalf("nil 表 Generation = %d, want 0", n)
	}
	tab.Reset()

	zero := &sessioncfg.Table{}
	zero.Apply(ev(t, "E1", 1, "2026-11-03T10:00:01Z", contracts.ConfigChangedPayload{Model: str("m1")}))
	if got := zero.Current("S1"); got.Model.Value != "m1" {
		t.Fatalf("零值表 Apply 失效: %+v", got.Model)
	}
}
