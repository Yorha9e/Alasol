package contracts

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// sampleEventsPath is the NDJSON sample shared with the TypeScript consumer
// (tools/ts-consumer/sample-events.ndjson). Both sides read the same file, so
// a change of the contract breaks the Go and the TypeScript check at once.
const sampleEventsPath = "../tools/ts-consumer/sample-events.ndjson"

// ulidPattern is the framework id rule of common.json#/$defs/Ulid
// (26 characters, Crockford base32). The Go type is a plain string, so the
// pattern is the only place where the rule can be enforced.
var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

// TestSampleEventsDecodeIntoContracts is the Go side of the cross-language
// check: every line of the shared sample must decode into the generated types
// and survive a round trip unchanged.
func TestSampleEventsDecodeIntoContracts(t *testing.T) {
	data, err := os.ReadFile(sampleEventsPath)
	if err != nil {
		t.Fatalf("read %s: %v", sampleEventsPath, err)
	}

	lines := bytes.Split(data, []byte("\n"))
	seen := 0
	kinds := map[string]int{}
	for i, raw := range lines {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			continue
		}
		seen++
		lineNo := i + 1

		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Errorf("line %d: unmarshal into Event: %v", lineNo, err)
			continue
		}
		kinds[ev.Kind]++

		// The id fields are ULIDs: the type is a string, the rule is the pattern.
		checkULID(t, lineNo, "id", string(ev.ID))
		checkULID(t, lineNo, "session_id", string(ev.SessionID))
		checkULID(t, lineNo, "agent_id", string(ev.AgentID))
		if ev.CausationID != nil {
			checkULID(t, lineNo, "causation_id", string(*ev.CausationID))
		}
		if ev.V != 1 {
			t.Errorf("line %d: v = %d, want 1", lineNo, ev.V)
		}
		if ev.TS == "" {
			t.Errorf("line %d: ts is empty", lineNo)
		}

		// The payload stays raw JSON: the bytes must not be touched.
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Errorf("line %d: unmarshal into envelope: %v", lineNo, err)
			continue
		}
		if !bytes.Equal(envelope["payload"], ev.Payload) {
			t.Errorf("line %d: payload bytes changed:\n got %s\nwant %s", lineNo, ev.Payload, envelope["payload"])
		}
		if len(ev.Payload) == 0 {
			t.Errorf("line %d: payload is empty", lineNo)
			continue
		}

		// The payload of a known kind decodes into its own type.
		switch ev.Kind {
		case "tool.call.intent":
			var p ToolCallIntentPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallIntentPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if p.ToolName == "" {
				t.Errorf("line %d: tool_name is empty", lineNo)
			}
			if len(p.Arguments) == 0 {
				t.Errorf("line %d: arguments are empty", lineNo)
			}
			if p.Replay != ReplayKindSafe {
				t.Errorf("line %d: replay = %q, want %q", lineNo, p.Replay, ReplayKindSafe)
			}
		case "tool.call.completed":
			var p ToolCallCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallCompletedPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if len(p.Result.Content) == 0 {
				t.Errorf("line %d: result.content is empty", lineNo)
				break
			}
			if p.Result.Content[0].Type != "text" {
				t.Errorf("line %d: result.content[0].type = %q, want %q", lineNo, p.Result.Content[0].Type, "text")
			}
			if len(p.Result.GraphEdges) == 0 {
				t.Errorf("line %d: result.graph_edges is empty", lineNo)
			}
		case "tool.call.failed":
			var p ToolCallFailedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ToolCallFailedPayload: %v", lineNo, err)
				break
			}
			if p.ToolCallID == "" {
				t.Errorf("line %d: tool_call_id is empty", lineNo)
			}
			if p.Error.Message == "" {
				t.Errorf("line %d: error.message is empty", lineNo)
			}
			if p.Error.Code == nil {
				t.Errorf("line %d: error.code is missing", lineNo)
			}
		case "context.assembled":
			var p ContextSnapshot
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Errorf("line %d: unmarshal into ContextSnapshot: %v", lineNo, err)
				break
			}
			if p.SessionID == "" {
				t.Errorf("line %d: context snapshot session_id is empty", lineNo)
			}
			if p.TokensBudget == 0 {
				t.Errorf("line %d: context snapshot tokens_budget is zero", lineNo)
			}
			if len(p.Parts) == 0 {
				t.Errorf("line %d: context snapshot parts are empty", lineNo)
			}
		default:
			// Unknown kinds are tolerated (D7): the payload stays raw JSON.
		}

		// Round trip: re-serializing must be semantically the same document.
		out, err := json.Marshal(ev)
		if err != nil {
			t.Errorf("line %d: marshal: %v", lineNo, err)
			continue
		}
		if !jsonSemanticallyEqual(t, lineNo, raw, out) {
			t.Errorf("line %d: round trip changed the document:\n got %s\nwant %s", lineNo, out, raw)
		}
	}

	if seen == 0 {
		t.Fatalf("%s has no events", sampleEventsPath)
	}
	// Every kind with a frozen payload mapping must be covered by the sample
	// and must decode into its type (x-kind-payload in event.json is the source).
	for _, kind := range []string{"tool.call.intent", "tool.call.completed", "tool.call.failed", "context.assembled"} {
		if kinds[kind] == 0 {
			t.Errorf("sample has no %q event", kind)
		}
	}
}

// checkULID asserts that an id field is a well-formed ULID.
func checkULID(t *testing.T, line int, field, value string) {
	t.Helper()
	if !ulidPattern.MatchString(value) {
		t.Errorf("line %d: %s = %q is not a ULID", line, field, value)
	}
}

// jsonSemanticallyEqual compares two JSON documents by value, not by bytes:
// the key order and the number formatting may differ, the meaning may not.
func jsonSemanticallyEqual(t *testing.T, line int, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Errorf("line %d: unmarshal %s: %v", line, a, err)
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Errorf("line %d: unmarshal %s: %v", line, b, err)
		return false
	}
	return reflect.DeepEqual(va, vb)
}

// TestEventPayloadIsRawJSON pins the D2 exception: the payload field keeps
// its bare json.RawMessage form (nil means "absent").
func TestEventPayloadIsRawJSON(t *testing.T) {
	var ev Event
	ev.Kind = "model.delta"
	if ev.Payload != nil {
		t.Errorf("a fresh Event has a payload: %s", ev.Payload)
	}
	out, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "payload") {
		t.Errorf("an empty payload must be omitted: %s", out)
	}
}
