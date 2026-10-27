// Package codegen turns the hand-written JSON Schema files in contracts/schema
// into the Go types in contracts/.
//
// It is deliberately a small, dependency-free compiler for a *restricted subset*
// of JSON Schema 2020-12 (decision D2 in docs/research/07): object / array /
// string / integer / number / boolean / enum / required / optional / inline
// nested object / non-cyclic $ref / x-go-type:"json.RawMessage". Everything
// outside the subset — $ref cycles, allOf/anyOf/oneOf/not, type arrays, unknown
// types, missing $defs, keywords next to a $ref — is a hard error rather than a
// silent fallback, because a contract that compiles into something the author
// did not write is worse than a build failure.
//
// The schema files are the single source of truth; the generated Go files are
// checked in and CI re-runs the generator requiring an empty diff. To make that
// possible the output is byte-for-byte deterministic:
//
//   - files are processed in sorted name order,
//   - types keep the $defs order of their schema, fields keep property order
//     (the JSON decoder in this package preserves member order instead of
//     sorting keys),
//   - the emitted source always goes through go/format,
//   - nothing time- or machine-dependent (no timestamps, no absolute paths)
//     enters the output.
//
// Naming and shape rules (see also docs/research/06 §3 and INVENTORY.md §3):
//
//   - one schema file → one Go file in package contracts,
//   - each $defs entry → one named type,
//   - optional fields (not listed in "required") are pointers with
//     json:",omitempty"; required fields use value types; arrays stay []T;
//     json.RawMessage keeps its bare form,
//   - snake_case property names become PascalCase field names with well-known
//     acronyms upper-cased (id→ID, ts→TS, url→URL, ulid→ULID, v→V, ...),
//   - enums become a named string type plus one constant per value; an enum
//     declared on a property becomes <Struct><Field>Enum plus its constants,
//   - an inline nested object becomes <Struct><Field>; the element type of an
//     array becomes <Array>Item.
//
// A "$ref" node may only carry the keywords $ref, description and title plus
// the machine-readable "x-" annotations (x-go-type, x-id-kind, x-deprecated,
// ...); an assertion keyword (type / enum / const / items / properties /
// required / allOf / anyOf / oneOf / not / format / pattern / ...) next to a
// reference is rejected instead of being silently dropped (x-go-type wins over
// $ref).
//
// A generated file whose schema file was renamed or deleted is an orphan: the
// check mode reports it and a normal run removes it. Only files carrying the
// generated header are ever touched, so hand-written files (contracts/doc.go)
// stay untouched.
package codegen
