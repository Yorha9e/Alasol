package codegen

import (
	"strings"
	"unicode"
)

// wellKnownAcronyms maps a whole snake_case word to its canonical Go spelling.
//
// The rule (docs/research/08 id discipline + Go style) is that well-known
// initialisms stay all-caps when they appear as a whole word: session_id →
// SessionID, tool_call_id → ToolCallID, binary_ref → BinaryRef, v → V.
var wellKnownAcronyms = map[string]string{
	"id":   "ID",
	"ts":   "TS",
	"url":  "URL",
	"api":  "API",
	"json": "JSON",
	"sql":  "SQL",
	"ulid": "ULID",
	"http": "HTTP",
	"ws":   "WS",
	"v":    "V",
}

// goKeywords are rejected as $defs type names (they cannot be identifiers).
var goKeywords = map[string]bool{
	"break": true, "case": true, "chan": true, "const": true, "continue": true,
	"default": true, "defer": true, "else": true, "fallthrough": true, "for": true,
	"func": true, "go": true, "goto": true, "if": true, "import": true,
	"interface": true, "map": true, "package": true, "range": true, "return": true,
	"select": true, "struct": true, "switch": true, "type": true, "var": true,
}

// splitWords splits s on every run of non-alphanumeric characters.
//
// It is used for both snake_case property names (session_id → [session id]) and
// enum values (reads-from → [reads from]), so a single word splitter covers the
// whole naming surface.
func splitWords(s string) []string {
	var (
		words []string
		cur   strings.Builder
	)
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur.WriteRune(r)
			continue
		}
		if cur.Len() > 0 {
			words = append(words, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		words = append(words, cur.String())
	}
	return words
}

// PascalCase converts a snake_case / kebab-case name to PascalCase, keeping
// well-known acronyms upper-cased as whole words.
func PascalCase(s string) string {
	var b strings.Builder
	for _, w := range splitWords(s) {
		if up, ok := wellKnownAcronyms[strings.ToLower(w)]; ok {
			b.WriteString(up)
			continue
		}
		r := []rune(w)
		r[0] = unicode.ToUpper(r[0])
		b.WriteString(string(r))
	}
	return b.String()
}

// EnumConstName builds the constant name for an enum value: the type name
// followed by the PascalCase value. A value that carries no alphanumeric
// characters at all ("" or "-") becomes "<Name>Empty".
func EnumConstName(typeName, value string) string {
	p := PascalCase(value)
	if p == "" {
		p = "Empty"
	}
	return typeName + p
}

// validGoIdent reports whether s is a usable Go identifier.
func validGoIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_' || unicode.IsLetter(r):
		case i > 0 && unicode.IsDigit(r):
		default:
			return false
		}
	}
	return true
}
