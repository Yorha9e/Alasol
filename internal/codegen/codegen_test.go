package codegen

import (
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Fixture directories (relative to this package, so the tests run from any
// working directory).
const (
	fixtureBasic = "testdata/basic"
	fixtureCycle = "testdata/cycle"
	fixtureSelf  = "testdata/selfcycle"
	fixtureDup   = "testdata/dup"
)

// fieldInfo is one struct field as it appears in the generated source.
type fieldInfo struct {
	Name string
	Type string
	Tag  string
}

// generateFixture compiles a fixture directory into a fresh output directory and
// returns the generated sources keyed by Go file name.
func generateFixture(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := t.TempDir()
	results, err := Run(Options{SchemaDir: dir, OutDir: out})
	if err != nil {
		t.Fatalf("Run(%s) error: %v", dir, err)
	}
	if len(results) == 0 {
		t.Fatalf("Run(%s) produced no results", dir)
	}
	files := make(map[string][]byte, len(results))
	for _, r := range results {
		src, err := os.ReadFile(r.Output)
		if err != nil {
			t.Fatalf("read generated %s: %v", r.Output, err)
		}
		files[filepath.Base(r.Output)] = src
	}
	return files
}

// parseGenerated parses generated Go source.
func parseGenerated(t *testing.T, filename string, src []byte) *ast.File {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("generated %s does not parse: %v\n---\n%s", filename, err, src)
	}
	return f
}

// structFields returns every struct of a generated file, in field order.
func structFields(t *testing.T, f *ast.File) map[string][]fieldInfo {
	t.Helper()
	out := make(map[string][]fieldInfo)
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			var fields []fieldInfo
			for _, field := range st.Fields.List {
				tag := ""
				if field.Tag != nil {
					tag = strings.Trim(field.Tag.Value, "`")
				}
				for _, name := range field.Names {
					fields = append(fields, fieldInfo{
						Name: name.Name,
						Type: types.ExprString(field.Type),
						Tag:  tag,
					})
				}
			}
			out[ts.Name.Name] = fields
		}
	}
	return out
}

// constValues returns every generated string constant as name → value.
func constValues(t *testing.T, f *ast.File) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("constant %s: %v", name.Name, err)
				}
				out[name.Name] = val
			}
		}
	}
	return out
}

// namedTypeKinds returns every named non-struct type as name → underlying type.
func namedTypeKinds(t *testing.T, f *ast.File) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			if _, isStruct := ts.Type.(*ast.StructType); isStruct {
				continue
			}
			out[ts.Name.Name] = types.ExprString(ts.Type)
		}
	}
	return out
}

// importsOf returns the import paths of a generated file.
func importsOf(f *ast.File) []string {
	var out []string
	for _, imp := range f.Imports {
		out = append(out, strings.Trim(imp.Path.Value, `"`))
	}
	sort.Strings(out)
	return out
}

// assertFields compares one struct against the expected field list.
func assertFields(t *testing.T, got map[string][]fieldInfo, structName string, want []fieldInfo) {
	t.Helper()
	fields, ok := got[structName]
	if !ok {
		t.Fatalf("struct %s was not generated (have %v)", structName, sortedKeys(got))
	}
	if !reflect.DeepEqual(fields, want) {
		var b strings.Builder
		for _, f := range fields {
			b.WriteString("\n  got  " + f.Name + " " + f.Type + " `" + f.Tag + "`")
		}
		for _, f := range want {
			b.WriteString("\n  want " + f.Name + " " + f.Type + " `" + f.Tag + "`")
		}
		t.Errorf("%s fields mismatch:%s", structName, b.String())
	}
}

func sortedKeys(m map[string][]fieldInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestGenerateBasicShapes pins the whole type mapping table of D2.
func TestGenerateBasicShapes(t *testing.T) {
	files := generateFixture(t, fixtureBasic)

	event := parseGenerated(t, "event.go", files["event.go"])
	structs := structFields(t, event)

	assertFields(t, structs, "Event", []fieldInfo{
		{Name: "V", Type: "int64", Tag: `json:"v"`},
		{Name: "ID", Type: "Ulid", Tag: `json:"id"`},
		{Name: "Kind", Type: "string", Tag: `json:"kind"`},
		{Name: "TS", Type: "string", Tag: `json:"ts"`},
		{Name: "SessionID", Type: "*Ulid", Tag: `json:"session_id,omitempty"`},
		{Name: "AgentID", Type: "*Ulid", Tag: `json:"agent_id,omitempty"`},
		{Name: "Seq", Type: "*int64", Tag: `json:"seq,omitempty"`},
		{Name: "Durable", Type: "*bool", Tag: `json:"durable,omitempty"`},
		{Name: "Payload", Type: "json.RawMessage", Tag: `json:"payload"`},
		{Name: "BinaryRef", Type: "*string", Tag: `json:"binary_ref,omitempty"`},
		{Name: "Meta", Type: "*EventMeta", Tag: `json:"meta,omitempty"`},
		{Name: "Tags", Type: "[]string", Tag: `json:"tags,omitempty"`},
		{Name: "Edges", Type: "[]GraphEdge", Tag: `json:"edges,omitempty"`},
		{Name: "Scores", Type: "[]float64", Tag: `json:"scores,omitempty"`},
		{Name: "Flags", Type: "[]bool", Tag: `json:"flags,omitempty"`},
	})

	// Inline nested object → <Outer><Field>, emitted right after its parent.
	assertFields(t, structs, "EventMeta", []fieldInfo{
		{Name: "TraceID", Type: "string", Tag: `json:"trace_id"`},
		{Name: "Retry", Type: "*int64", Tag: `json:"retry,omitempty"`},
		{Name: "Labels", Type: "map[string]any", Tag: `json:"labels,omitempty"`},
	})

	// A plain string $defs entry with a pattern becomes a named string type.
	if kinds := namedTypeKinds(t, event); kinds["Ulid"] != "string" {
		t.Errorf("Ulid: got underlying %q, want string", kinds["Ulid"])
	}

	tr := parseGenerated(t, "tool_result.go", files["tool_result.go"])
	assertFields(t, structFields(t, tr), "ToolResult", []fieldInfo{
		{Name: "Ok", Type: "bool", Tag: `json:"ok"`},
		{Name: "Output", Type: "json.RawMessage", Tag: `json:"output,omitempty"`},
		{Name: "Taint", Type: "*Taint", Tag: `json:"taint,omitempty"`},
		{Name: "SideEffect", Type: "*SideEffect", Tag: `json:"side_effect,omitempty"`},
		{Name: "GraphEdges", Type: "[]GraphEdge", Tag: `json:"graph_edges,omitempty"`},
		{Name: "Replayable", Type: "*bool", Tag: `json:"replayable,omitempty"`},
	})
	assertFields(t, structFields(t, tr), "GraphEdge", []fieldInfo{
		{Name: "Src", Type: "string", Tag: `json:"src"`},
		{Name: "Dst", Type: "string", Tag: `json:"dst"`},
		{Name: "Kind", Type: "string", Tag: `json:"kind"`},
		{Name: "Weight", Type: "*float64", Tag: `json:"weight,omitempty"`},
	})

	wantConsts := map[string]string{
		"SideEffectNone":        "none",
		"SideEffectRead":        "read",
		"SideEffectWrite":       "write",
		"SideEffectDestructive": "destructive",
		"TaintClean":            "clean",
		"TaintTainted":          "tainted",
		"TaintUnknown":          "unknown",
		"EdgeKindCalls":         "calls",
		"EdgeKindReadsFrom":     "reads-from",
		"EdgeKindWritesTo":      "writes-to",
	}
	gotConsts := constValues(t, tr)
	for name, val := range wantConsts {
		got, ok := gotConsts[name]
		if !ok {
			t.Errorf("constant %s was not generated", name)
			continue
		}
		if got != val {
			t.Errorf("constant %s = %q, want %q", name, got, val)
		}
	}
	if len(gotConsts) != len(wantConsts) {
		t.Errorf("generated %d constants, want %d (%v)", len(gotConsts), len(wantConsts), gotConsts)
	}

	// A file without any json.RawMessage must not import encoding/json.
	plain := parseGenerated(t, "plain.go", files["plain.go"])
	if imports := importsOf(plain); len(imports) != 0 {
		t.Errorf("plain.go imports %v, want none", imports)
	}
	if got := importsOf(event); !reflect.DeepEqual(got, []string{"encoding/json"}) {
		t.Errorf("event.go imports %v, want [encoding/json]", got)
	}
}

// TestHeaderAndGofmtClean checks the generated-file contract.
func TestHeaderAndGofmtClean(t *testing.T) {
	files := generateFixture(t, fixtureBasic)
	headerRE := regexp.MustCompile(`(?m)^// Code generated by cmd/codegen from .*\.json; DO NOT EDIT\.$`)
	for name, src := range files {
		if !headerRE.Match(src) {
			t.Errorf("%s: missing %q header, got:\n%s", name, "// Code generated by cmd/codegen from <schema>.json; DO NOT EDIT.", firstLines(src, 3))
		}
		// go/format must be a fixed point: re-formatting changes nothing.
		formatted, err := format.Source(src)
		if err != nil {
			t.Errorf("%s: not valid Go: %v", name, err)
			continue
		}
		if string(formatted) != string(src) {
			t.Errorf("%s: generated source is not gofmt-clean", name)
		}
		if strings.Contains(string(src), "\r\n") {
			t.Errorf("%s: generated source contains CRLF (output must be byte-identical on every platform)", name)
		}
	}
}

func firstLines(src []byte, n int) string {
	lines := strings.Split(string(src), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

// TestDeterministicOutput is the CI property: the same input must produce
// byte-identical output, run after run.
func TestDeterministicOutput(t *testing.T) {
	var first map[string][]byte
	for i := 0; i < 3; i++ {
		got := generateFixture(t, fixtureBasic)
		if first == nil {
			first = got
			continue
		}
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("run %d differs from run 1", i+1)
		}
	}
	// And a second run over an already generated tree must not rewrite anything.
	out := t.TempDir()
	if _, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	results, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	for _, r := range results {
		if r.Written {
			t.Errorf("%s was rewritten although it was already up to date", r.Output)
		}
	}
}

// TestOutDirIsCreated covers the "fresh checkout" case.
func TestOutDirIsCreated(t *testing.T) {
	out := filepath.Join(t.TempDir(), "nested", "contracts")
	results, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out})
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("no results")
	}
	if _, err := os.Stat(filepath.Join(out, "event.go")); err != nil {
		t.Errorf("event.go not written: %v", err)
	}
}

// TestRefCyclesRejected covers the hard rule: $ref cycles are rejected.
func TestRefCyclesRejected(t *testing.T) {
	cases := []struct {
		dir  string
		want string
	}{
		{fixtureCycle, "$ref cycle detected"},
		{fixtureSelf, "$ref cycle detected"},
	}
	for _, tc := range cases {
		out := t.TempDir()
		_, err := Run(Options{SchemaDir: tc.dir, OutDir: out})
		if err == nil {
			t.Errorf("%s: expected an error, got none", tc.dir)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not mention %q", tc.dir, err, tc.want)
		}
		if !strings.Contains(err.Error(), "#/$defs/") {
			t.Errorf("%s: error %q should name the cycle members", tc.dir, err)
		}
	}
}

// TestMalformedSchemasRejected covers the error surface of the CLI contract:
// every case must fail with a non-zero exit and a readable message.
func TestMalformedSchemasRejected(t *testing.T) {
	cases := []struct {
		file string
		want string
	}{
		{"no-defs.json", "missing $defs"},
		{"unknown-type.json", "unknown type"},
		{"composition.json", "allOf"},
		{"duplicate-key.json", "duplicate object key"},
		{"dangling-ref.json", "unresolved $ref"},
		{"type-array.json", "type arrays"},
	}
	for _, tc := range cases {
		dir := t.TempDir()
		src, err := os.ReadFile(filepath.Join("testdata", "bad", tc.file))
		if err != nil {
			t.Fatalf("read fixture %s: %v", tc.file, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "schema.json"), src, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err = Run(Options{SchemaDir: dir, OutDir: t.TempDir()})
		if err == nil {
			t.Errorf("%s: expected an error, got none", tc.file)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not mention %q", tc.file, err, tc.want)
		}
	}
}

// TestUnsupportedKeywordCombinations covers the remaining "reject, never guess"
// branches of the subset.
func TestUnsupportedKeywordCombinations(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"integer enum", `{"$defs":{"N":{"type":"integer","enum":[1,2]}}}`, "enum"},
		{"x-go-type with properties", `{"$defs":{"X":{"type":"object","x-go-type":"json.RawMessage","properties":{"a":{"type":"string"}}}}}`, "x-go-type"},
		{"unknown x-go-type", `{"$defs":{"X":{"type":"object","x-go-type":"time.Time"}}}`, "x-go-type"},
		{"tuple items", `{"$defs":{"X":{"type":"array","items":[{"type":"string"}]}}}`, "tuple"},
		{"required without property", `{"$defs":{"X":{"type":"object","properties":{"a":{"type":"string"}},"required":["b"]}}}`, "required"},
		{"bad $defs key", `{"$defs":{"not a name":{"type":"string"}}}`, "Go type name"},
		{"keyword $defs key", `{"$defs":{"type":{"type":"string"}}}`, "keyword"},
		{"unsupported ref form", `{"$defs":{"X":{"type":"object","properties":{"a":{"$ref":"#/definitions/Y"}}}}}`, "unsupported $ref"},
		{"array without items is fine", `{"$defs":{"X":{"type":"array"}}}`, ""},
	}
	for _, tc := range cases {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "s.json"), []byte(tc.src), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Run(Options{SchemaDir: dir, OutDir: t.TempDir()})
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: unexpected error: %v", tc.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: expected an error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.want)
		}
	}
}

// TestFailedRunWritesNothing makes sure a broken schema never leaves a
// half-generated tree behind.
func TestFailedRunWritesNothing(t *testing.T) {
	dir := t.TempDir()
	good, err := os.ReadFile(filepath.Join(fixtureBasic, "event.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a-event.json"), good, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "z-bad.json"), []byte(`{"$defs":{"Bad":{"type":"nope"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := Run(Options{SchemaDir: dir, OutDir: out}); err == nil {
		t.Fatal("expected an error")
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed run wrote %d files, want 0", len(entries))
	}
}

// TestDuplicateTypeAcrossFilesRejected: all generated files share one package,
// so a name may only be declared once.
func TestDuplicateTypeAcrossFilesRejected(t *testing.T) {
	_, err := Run(Options{SchemaDir: fixtureDup, OutDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "both declare Same") {
		t.Errorf("error %q does not mention the duplicate declaration", err)
	}
}

// TestEmptyAndMissingSchemaDir covers the "nothing to compile" cases.
func TestEmptyAndMissingSchemaDir(t *testing.T) {
	empty := t.TempDir()
	if _, err := Run(Options{SchemaDir: empty, OutDir: t.TempDir()}); err == nil {
		t.Error("empty schema dir: expected an error")
	}
	if _, err := Run(Options{SchemaDir: filepath.Join(empty, "nope"), OutDir: t.TempDir()}); err == nil {
		t.Error("missing schema dir: expected an error")
	}
	if _, err := Run(Options{OutDir: t.TempDir()}); err == nil {
		t.Error("empty SchemaDir: expected an error")
	}
}

// TestGoFileNameMapping pins the schema → Go file name rule.
func TestGoFileNameMapping(t *testing.T) {
	cases := map[string]string{
		"event.json":            "event.go",
		"tool-result.json":      "tool_result.go",
		"context-snapshot.json": "context_snapshot.go",
		"a-b-c.json":            "a_b_c.go",
	}
	for in, want := range cases {
		got, err := goFileName(in)
		if err != nil {
			t.Errorf("goFileName(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("goFileName(%q) = %q, want %q", in, got, want)
		}
	}
	for _, bad := range []string{"we ird.json", "", "9lives.json"} {
		if _, err := goFileName(bad); err == nil {
			t.Errorf("goFileName(%q): expected an error", bad)
		}
	}
}

// TestNaming pins the snake_case → PascalCase and enum constant rules.
func TestNaming(t *testing.T) {
	cases := map[string]string{
		"id":                  "ID",
		"session_id":          "SessionID",
		"tool_call_id":        "ToolCallID",
		"binary_ref":          "BinaryRef",
		"ts":                  "TS",
		"created_at":          "CreatedAt",
		"api_url":             "APIURL",
		"json_payload":        "JSONPayload",
		"sql_text":            "SQLText",
		"ulid":                "ULID",
		"http_status":         "HTTPStatus",
		"ws_frame":            "WSFrame",
		"v":                   "V",
		"idempotent":          "Idempotent",
		"a__b":                "AB",
		"already_Pascal_Case": "AlreadyPascalCase",
	}
	for in, want := range cases {
		if got := PascalCase(in); got != want {
			t.Errorf("PascalCase(%q) = %q, want %q", in, got, want)
		}
	}

	enumCases := map[[2]string]string{
		{"SideEffect", "none"}:        "SideEffectNone",
		{"SideEffect", "destructive"}: "SideEffectDestructive",
		{"EdgeKind", "reads-from"}:    "EdgeKindReadsFrom",
		{"EdgeKind", "writes-to"}:     "EdgeKindWritesTo",
		{"Kind", ""}:                  "KindEmpty",
	}
	for in, want := range enumCases {
		if got := EnumConstName(in[0], in[1]); got != want {
			t.Errorf("EnumConstName(%q, %q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

// TestOrderedJSONPreservesMemberOrder is what makes the deterministic output
// possible at all: object members must not be sorted.
func TestOrderedJSONPreservesMemberOrder(t *testing.T) {
	v, err := decodeJSON([]byte(`{"z":1,"a":2,"m":{"y":3,"b":4}}`))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, m := range v.Members {
		keys = append(keys, m.Key)
	}
	if !reflect.DeepEqual(keys, []string{"z", "a", "m"}) {
		t.Errorf("member order = %v, want [z a m]", keys)
	}
	if got := v.Members[2].Value.Members[0].Key; got != "y" {
		t.Errorf("nested first key = %q, want y", got)
	}
	if _, err := decodeJSON([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Error("duplicate keys must be rejected")
	}
	if _, err := decodeJSON([]byte(`{"a":1} trailing`)); err == nil {
		t.Error("trailing content must be rejected")
	}
	if _, err := decodeJSON([]byte(`[1,2]`)); err != nil {
		t.Errorf("top-level array should still decode: %v", err)
	}
}

// TestGeneratedCodeCompiles builds the generated output as a real Go package.
func TestGeneratedCodeCompiles(t *testing.T) {
	goTool := findGoTool(t)
	out := t.TempDir()
	if _, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out}); err != nil {
		t.Fatalf("Run error: %v", err)
	}
	modDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(modDir, "go.mod"), []byte("module generatedcontracts\n\ngo 1.26\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		src, err := os.ReadFile(filepath.Join(out, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(modDir, e.Name()), src, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(goTool, "build", "./...")
	cmd.Dir = modDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated package does not compile: %v\n%s", err, combined)
	}
}

// TestCheckModeDetectsDrift covers the CI "re-generate → empty diff" gate.
func TestCheckModeDetectsDrift(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out, Check: true}); err == nil {
		t.Fatal("check on an empty output dir must fail")
	}
	if _, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out}); err != nil {
		t.Fatalf("generate: %v", err)
	}
	if _, err := Run(Options{SchemaDir: fixtureBasic, OutDir: out, Check: true}); err != nil {
		t.Errorf("check after generate must pass, got: %v", err)
	}
	// Drift: hand-edit a generated file.
	target := filepath.Join(out, "event.go")
	src, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, append(src, []byte("\n// drift\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Run(Options{SchemaDir: fixtureBasic, OutDir: out, Check: true})
	if err == nil {
		t.Fatal("check must fail on a drifted file")
	}
	if !strings.Contains(err.Error(), "out of date") {
		t.Errorf("error %q does not report drift", err)
	}
}

// TestHeaderPathIsRepoRootRelative pins the header rule: the source path is
// relative to the repository root, so it does not depend on the working
// directory the generator was invoked from.
func TestHeaderPathIsRepoRootRelative(t *testing.T) {
	files := generateFixture(t, fixtureBasic)
	want := "// Code generated by cmd/codegen from internal/codegen/testdata/basic/event.json; DO NOT EDIT."
	if got := firstLines(files["event.go"], 1); got != want {
		t.Errorf("header = %q, want %q", got, want)
	}
	lines := strings.Split(string(files["event.go"]), "\n")
	if len(lines) < 3 || lines[2] != "package contracts" {
		t.Errorf("line 3 = %q, want %q", lines[2], "package contracts")
	}
}

// TestBOMAndTopLevelSlip: a UTF-8 BOM must not break decoding, and a schema that
// puts its type at the top level must produce a helpful error.
func TestBOMAndTopLevelSlip(t *testing.T) {
	dir := t.TempDir()
	bom := append([]byte{0xEF, 0xBB, 0xBF}, []byte(`{"$defs":{"Bom":{"type":"object","properties":{"a":{"type":"string"}}}}}`)...)
	if err := os.WriteFile(filepath.Join(dir, "bom.json"), bom, 0o644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := Run(Options{SchemaDir: dir, OutDir: out}); err != nil {
		t.Fatalf("BOM-prefixed schema rejected: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "bom.go")); err != nil {
		t.Errorf("bom.go not written: %v", err)
	}

	for _, src := range []string{
		`{"type":"object","properties":{"a":{"type":"string"}}}`,
		`{"type":"string"}`,
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "s.json"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Run(Options{SchemaDir: dir, OutDir: t.TempDir()})
		if err == nil {
			t.Errorf("%s: expected an error", src)
			continue
		}
		if !strings.Contains(err.Error(), "$defs entry") {
			t.Errorf("%s: error %q does not explain the $defs rule", src, err)
		}
	}
}

// repoRoot walks up to the directory holding go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the working directory")
		}
		dir = parent
	}
}

// fixturePath resolves a testdata directory to an absolute path.
func fixturePath(t *testing.T, dir string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(wd, filepath.FromSlash(dir))
}

// findGoTool locates the Go toolchain, skipping the test if it is unavailable.
func findGoTool(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not in PATH")
	}
	return path
}

// buildCLI builds cmd/codegen once and returns the binary path.
func buildCLI(t *testing.T) string {
	t.Helper()
	goTool := findGoTool(t)
	binDir := t.TempDir()
	cmd := exec.Command(goTool, "build", "-o", binDir, "./cmd/codegen")
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build cmd/codegen: %v\n%s", err, combined)
	}
	matches, err := filepath.Glob(filepath.Join(binDir, "codegen*"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("built binary not found in %s: %v", binDir, err)
	}
	return matches[0]
}

// TestCLIEndToEnd runs the real command: default-ish flags, output, exit codes.
func TestCLIEndToEnd(t *testing.T) {
	bin := buildCLI(t)
	root := repoRoot(t)
	schema := fixturePath(t, fixtureBasic)
	out := filepath.Join(t.TempDir(), "contracts")

	run := func(args ...string) (string, string, int) {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Dir = root
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		return stdout.String(), stderr.String(), code
	}

	stdout, stderr, code := run("-schema", schema, "-out", out)
	if code != 0 {
		t.Fatalf("generate exited %d: %s", code, stderr)
	}
	for _, want := range []string{"event.go", "tool_result.go", "plain.go"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout %q does not mention %s", stdout, want)
		}
	}
	for _, name := range []string{"event.go", "tool_result.go", "plain.go"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Errorf("%s not written: %v", name, err)
		}
	}

	// Second run: idempotent, reports "unchanged".
	stdout, _, code = run("-schema", schema, "-out", out)
	if code != 0 {
		t.Fatalf("second run exited %d", code)
	}
	if !strings.Contains(stdout, "unchanged") {
		t.Errorf("second run stdout = %q, want \"unchanged\"", stdout)
	}

	// A $ref cycle must exit non-zero with a message on stderr and write nothing.
	cycleOut := filepath.Join(t.TempDir(), "contracts")
	_, stderr, code = run("-schema", fixturePath(t, fixtureCycle), "-out", cycleOut)
	if code == 0 {
		t.Error("cycle input must exit non-zero")
	}
	if !strings.Contains(stderr, "$ref cycle detected") {
		t.Errorf("stderr %q does not report the cycle", stderr)
	}
	if _, err := os.Stat(cycleOut); !os.IsNotExist(err) {
		t.Errorf("failed run created the output dir: %v", err)
	}

	// Unknown flag and missing schema dir also fail non-zero.
	if _, _, code = run("-nope"); code == 0 {
		t.Error("unknown flag must exit non-zero")
	}
	if _, stderr, code = run("-schema", filepath.Join(root, "testdata-does-not-exist"), "-out", t.TempDir()); code == 0 {
		t.Error("missing schema dir must exit non-zero")
	} else if !strings.Contains(stderr, "codegen:") {
		t.Errorf("stderr %q does not carry the codegen prefix", stderr)
	}
}

// TestCLICheckFlag runs -check through the CLI (what CI will call).
func TestCLICheckFlag(t *testing.T) {
	bin := buildCLI(t)
	root := repoRoot(t)
	schema := fixturePath(t, fixtureBasic)
	out := t.TempDir()

	run := func(args ...string) (string, string, int) {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Dir = root
		var stdout, stderr strings.Builder
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		return stdout.String(), stderr.String(), code
	}

	if _, stderr, code := run("-schema", schema, "-out", out, "-check"); code == 0 {
		t.Error("check on an empty tree must fail")
	} else if !strings.Contains(stderr, "missing") {
		t.Errorf("stderr %q does not report the missing file", stderr)
	}
	if _, _, code := run("-schema", schema, "-out", out); code != 0 {
		t.Fatalf("generate exited %d", code)
	}
	if stdout, _, code := run("-schema", schema, "-out", out, "-check"); code != 0 {
		t.Errorf("check after generate must pass, got stderr/stdout: %q", stdout)
	}
}
