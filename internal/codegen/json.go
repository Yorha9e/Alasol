package codegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// jsonKind is the kind of a decoded JSON value.
type jsonKind int

const (
	jsonObject jsonKind = iota
	jsonArray
	jsonString
	jsonNumber
	jsonBool
	jsonNull
)

// jsonMember is one member of a JSON object, kept in source order.
type jsonMember struct {
	Key   string
	Value *jsonValue
}

// jsonValue is a decoded JSON value that preserves object member order.
//
// Deterministic output (CI requires "re-generate and the diff is empty") depends
// on never re-ordering what the schema author wrote, so a plain
// map[string]any is not enough: it would sort keys alphabetically. Members are
// therefore kept as an ordered slice plus a lookup index.
type jsonValue struct {
	Kind    jsonKind
	Members []jsonMember
	lookup  map[string]*jsonValue
	Items   []*jsonValue
	Str     string
	Num     json.Number
	Bool    bool
}

// Member returns the non-null member stored under key.
func (v *jsonValue) Member(key string) (*jsonValue, bool) {
	if v == nil || v.Kind != jsonObject {
		return nil, false
	}
	child, ok := v.lookup[key]
	if !ok || child == nil || child.Kind == jsonNull {
		return nil, false
	}
	return child, true
}

// str returns the string member stored under key.
func (v *jsonValue) str(ctx, key string) (string, error) {
	child, ok := v.Member(key)
	if !ok {
		return "", fmt.Errorf("%s: missing %q", ctx, key)
	}
	return child.stringValue(ctx, key)
}

// stringValue reads v itself as a string, naming key in any error.
func (v *jsonValue) stringValue(ctx, key string) (string, error) {
	if v == nil || v.Kind != jsonString {
		return "", fmt.Errorf("%s: %q must be a string", ctx, key)
	}
	return v.Str, nil
}

// strSlice returns the array-of-strings member stored under key.
func (v *jsonValue) strSlice(ctx, key string) ([]string, error) {
	child, ok := v.Member(key)
	if !ok {
		return nil, fmt.Errorf("%s: missing %q", ctx, key)
	}
	return stringItems(child, ctx, key)
}

// stringItems reads an array value whose items must all be strings.
func stringItems(v *jsonValue, ctx, key string) ([]string, error) {
	if v.Kind != jsonArray {
		return nil, fmt.Errorf("%s: %q must be an array", ctx, key)
	}
	out := make([]string, 0, len(v.Items))
	for i, item := range v.Items {
		if item.Kind != jsonString {
			return nil, fmt.Errorf("%s: %s[%d] must be a string", ctx, key, i)
		}
		out = append(out, item.Str)
	}
	return out, nil
}

// Len reports the number of members (objects) or items (arrays).
func (v *jsonValue) Len() int {
	switch v.Kind {
	case jsonObject:
		return len(v.Members)
	case jsonArray:
		return len(v.Items)
	default:
		return 0
	}
}

// utf8BOM is stripped before decoding: editors on Windows like to add one and
// encoding/json rejects it.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// decodeJSON parses data into an order-preserving tree.
//
// It is stricter than encoding/json's Unmarshal in two ways that matter for a
// contract compiler: duplicate object keys are rejected (a silent last-wins
// merge would hide schema authoring mistakes) and trailing content after the
// top-level value is rejected.
func decodeJSON(data []byte) (*jsonValue, error) {
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimPrefix(data, utf8BOM)))
	dec.UseNumber()
	root, err := decodeJSONValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("unexpected trailing content: %w", err)
		}
		return nil, fmt.Errorf("unexpected trailing content after top-level JSON value")
	}
	return root, nil
}

func decodeJSONValue(dec *json.Decoder) (*jsonValue, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return decodeJSONObject(dec)
		case '[':
			return decodeJSONArray(dec)
		default:
			return nil, fmt.Errorf("unexpected delimiter %q", t)
		}
	case string:
		return &jsonValue{Kind: jsonString, Str: t}, nil
	case json.Number:
		return &jsonValue{Kind: jsonNumber, Num: t}, nil
	case bool:
		return &jsonValue{Kind: jsonBool, Bool: t}, nil
	case nil:
		return &jsonValue{Kind: jsonNull}, nil
	default:
		return nil, fmt.Errorf("unexpected JSON token %v", tok)
	}
}

func decodeJSONObject(dec *json.Decoder) (*jsonValue, error) {
	obj := &jsonValue{Kind: jsonObject, lookup: make(map[string]*jsonValue)}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected object key, got %v", tok)
		}
		if _, dup := obj.lookup[key]; dup {
			return nil, fmt.Errorf("duplicate object key %q", key)
		}
		val, err := decodeJSONValue(dec)
		if err != nil {
			return nil, fmt.Errorf("member %q: %w", key, err)
		}
		obj.Members = append(obj.Members, jsonMember{Key: key, Value: val})
		obj.lookup[key] = val
	}
	if _, err := dec.Token(); err != nil { // closing '}'
		return nil, err
	}
	return obj, nil
}

func decodeJSONArray(dec *json.Decoder) (*jsonValue, error) {
	arr := &jsonValue{Kind: jsonArray}
	for dec.More() {
		val, err := decodeJSONValue(dec)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", len(arr.Items), err)
		}
		arr.Items = append(arr.Items, val)
	}
	if _, err := dec.Token(); err != nil { // closing ']'
		return nil, err
	}
	return arr, nil
}
