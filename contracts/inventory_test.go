package contracts

// 对账测试：**INVENTORY §2 的 kind 集 == schema/event.json 的 x-kind-payload 键集**，
// 且 durable 列与**样本事件**（跨语言证人）同判。
//
// 为什么要有这条：INVENTORY §2（人读的清单）与 schema 的 x-kind-payload（机器读的映射）
// 是同一件事的两份抄本，此前**没有任何测试对账**——改了一边忘了另一边，消费端（TS）按
// schema 校验、审计按 INVENTORY 查，两边悄悄分叉（漏一个 kind = 消费端按"未知 kind 宽容"
// 跳过 payload 校验，而清单里还写着它有 payload 形状）。
//
// durable 列的证人 = **共享样本**（tools/ts-consumer/sample-events.ndjson）：Go 侧与 TS
// 侧读的是同一份样本，样本里每条事件的 durable 标记就是"这个 kind 是不是流事件"的事实。

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

// schemaEventPath 是事件 schema（x-kind-payload 的唯一定义处）。
const schemaEventPath = "schema/event.json"

// inventoryPath 是契约清单（§2 的 kind 表）。
const inventoryPath = "INVENTORY.md"

// readEventKindPayload 读事件 schema，取出 kind → payload $defs 名 的映射。
func readEventKindPayload(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(schemaEventPath)
	if err != nil {
		t.Fatalf("读 %s: %v", schemaEventPath, err)
	}
	var doc struct {
		Defs map[string]struct {
			KindPayload map[string]string `json:"x-kind-payload"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("解 %s: %v", schemaEventPath, err)
	}
	ev, ok := doc.Defs["Event"]
	if !ok {
		t.Fatalf("%s 缺 $defs.Event", schemaEventPath)
	}
	if len(ev.KindPayload) == 0 {
		t.Fatalf("%s 的 $defs.Event 缺 x-kind-payload", schemaEventPath)
	}
	return ev.KindPayload
}

// readInventoryKinds 解析 INVENTORY §2 的 kind 表 → kind → durable（true/false）。
//
// 只取 §2（"## 2. 事件 core kind 子集"）那一节：§2b 是 **Transport 消息 kind**（不是
// 事件 kind、不进事件流），§1 是契约清单——混进来就比错对象了。
func readInventoryKinds(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatalf("读 %s: %v", inventoryPath, err)
	}
	lines := strings.Split(string(raw), "\n")

	// 定位 §2 的行范围（下一节是 "### 2b."——三级标题，故按"任何标题行"收口）。
	start, end := -1, len(lines)
	for i, ln := range lines {
		if start < 0 {
			if strings.HasPrefix(ln, "## 2. ") {
				start = i
			}
			continue
		}
		if strings.HasPrefix(ln, "#") { // 下一节（2b / 3 …，任意层级标题）
			end = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("%s 里找不到 §2 章节", inventoryPath)
	}

	out := map[string]bool{}
	for _, ln := range lines[start:end] {
		if !strings.HasPrefix(ln, "|") {
			continue
		}
		cells := strings.Split(ln, "|")
		if len(cells) < 4 {
			continue
		}
		kind := strings.Trim(strings.TrimSpace(cells[1]), "`")
		if kind == "" || kind == "kind" { // 表头
			continue
		}
		if strings.HasPrefix(kind, "---") { // 分隔行
			continue
		}
		durCell := strings.Trim(strings.TrimSpace(cells[2]), "*")
		switch durCell {
		case "true":
			out[kind] = true
		case "false":
			out[kind] = false
		default:
			t.Fatalf("%s §2 里 kind=%q 的 durable 列 = %q（want true/false）", inventoryPath, kind, cells[2])
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s §2 里没解析出任何 kind", inventoryPath)
	}
	return out
}

// sampleDurableByKind 读共享样本，返回 kind → durable（样本里出现过的 kind）。
func sampleDurableByKind(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(sampleEventsPath)
	if err != nil {
		t.Fatalf("读 %s: %v", sampleEventsPath, err)
	}
	out := map[string]bool{}
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var ev struct {
			Kind    string `json:"kind"`
			Durable bool   `json:"durable"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("%s 第 %d 行不是合法 JSON: %v", sampleEventsPath, i+1, err)
		}
		if prev, ok := out[ev.Kind]; ok && prev != ev.Durable {
			t.Fatalf("%s 里 kind=%s 同时出现 durable=true 与 durable=false", sampleEventsPath, ev.Kind)
		}
		out[ev.Kind] = ev.Durable
	}
	return out
}

// sortedKeys 返回排序后的键集（报错信息稳定，不受 map 迭代顺序影响）。
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestInventoryKindSetMatchesSchema：INVENTORY §2 的 kind 集 == schema 的 x-kind-payload
// 键集（**两个方向**都要查：清单多一个 = 消费端按未知 kind 宽容跳过 payload 校验；
// schema 多一个 = 清单漏账，审计查不到它的形状）。
func TestInventoryKindSetMatchesSchema(t *testing.T) {
	schema := readEventKindPayload(t)
	inv := readInventoryKinds(t)

	inSchemaNotInv := []string{}
	for kind := range schema {
		if _, ok := inv[kind]; !ok {
			inSchemaNotInv = append(inSchemaNotInv, kind)
		}
	}
	inInvNotSchema := []string{}
	for kind := range inv {
		if _, ok := schema[kind]; !ok {
			inInvNotSchema = append(inInvNotSchema, kind)
		}
	}
	if len(inSchemaNotInv) > 0 {
		t.Errorf("schema 的 x-kind-payload 有、INVENTORY §2 没有的 kind: %v", inSchemaNotInv)
	}
	if len(inInvNotSchema) > 0 {
		t.Errorf("INVENTORY §2 有、schema 的 x-kind-payload 没有的 kind: %v", inInvNotSchema)
	}
	// 规模也钉一下（防止某一边的表被误删成空 → 上面的差集恰好为空）。
	if len(schema) < 20 {
		t.Errorf("schema 的 x-kind-payload 只有 %d 个 kind，疑似被清空: %v", len(schema), sortedKeys(schema))
	}
	if len(inv) != len(schema) {
		t.Errorf("kind 集大小不等：schema=%d INVENTORY=%d", len(schema), len(inv))
	}
}

// TestInventoryDurableColumnMatchesSample：durable 列与**样本事件**同判。
//
// 口径：
//   - 样本里出现过的 kind：清单的 durable 列必须与样本一致（样本是跨语言证人）；
//   - durable=false 的 kind 在样本里必须**确实**是流事件（不入库、不占 seq）；
//   - 清单里 durable=false 的 kind **有且只有** model.delta（唯一的流事件 kind）——
//     多一个就说明清单与实现分叉了（流事件不进事件流，清单写 true 会骗审计）。
func TestInventoryDurableColumnMatchesSample(t *testing.T) {
	inv := readInventoryKinds(t)
	sample := sampleDurableByKind(t)

	for kind, wantDurable := range inv {
		got, ok := sample[kind]
		if !ok {
			continue // 样本没覆盖这个 kind（样本是抽样，不是全集）
		}
		if got != wantDurable {
			t.Errorf("kind=%s：INVENTORY §2 的 durable=%v，样本里是 %v（两处对不上）",
				kind, wantDurable, got)
		}
	}

	// durable=false 的 kind 有且只有 model.delta（G13 的流事件）。
	var streamKinds []string
	for kind, durable := range inv {
		if !durable {
			streamKinds = append(streamKinds, kind)
		}
	}
	sort.Strings(streamKinds)
	if len(streamKinds) != 1 || streamKinds[0] != "model.delta" {
		t.Errorf("INVENTORY §2 里 durable=false 的 kind = %v，want [model.delta]（唯一的流事件 kind）", streamKinds)
	}
	// 样本里 model.delta 确实是非 durable 的流事件（与清单同判）。
	if d, ok := sample["model.delta"]; !ok || d {
		t.Errorf("样本里的 model.delta durable = %v（want false：流事件不入库）", d)
	}
}
