package codegen

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// nodeKind is the (small) set of schema shapes the generator understands.
//
// This is the "restricted subset" of D2: object / array / string / integer /
// number / boolean / enum / non-cyclic $ref / x-go-type raw JSON. Anything
// outside it is a hard error, never a silent fallback.
type nodeKind int

const (
	nodeObject  nodeKind = iota // object; may be bare (no properties) → map[string]any
	nodeArray                   // array of items
	nodeString                  // string, possibly with enum/pattern/format
	nodeInteger                 // → int64
	nodeNumber                  // → float64
	nodeBoolean                 // → bool
	nodeRef                     // $ref, resolved to a *def
	nodeRaw                     // x-go-type: "json.RawMessage"
	nodeAny                     // unconstrained (array without items)
)

// schemaProp is one property of an object schema, in source order.
type schemaProp struct {
	name        string
	node        *schemaNode
	description string
}

// schemaNode is the compiled form of one JSON Schema node.
type schemaNode struct {
	kind        nodeKind
	description string
	format      string
	pattern     string
	enum        []string
	properties  []*schemaProp
	propIndex   map[string]bool
	required    map[string]bool
	items       *schemaNode
	ref         string // raw $ref string, as written
	refTarget   *def   // resolved target (nodeRef only)
	xGoType     string // "json.RawMessage" (nodeRaw only)
}

// def is one $defs entry: the unit of generated Go types.
type def struct {
	name string
	node *schemaNode
	doc  string
	file *schemaFile
}

// id is the human-readable identity used in error messages and cycle reports.
func (d *def) id() string {
	return fmt.Sprintf("%s#/$defs/%s", d.file.rel, d.name)
}

// schemaFile is one parsed *.json schema file.
type schemaFile struct {
	path  string // absolute path on disk
	rel   string // display path (repo-root relative when possible)
	defs  []*def
	index map[string]*def
}

// loader holds every schema file of one directory, with $ref resolved.
type loader struct {
	dir        string // absolute directory
	displayDir string
	files      []*schemaFile
	byPath     map[string]*schemaFile
}

// schemaDoc extracts the doc comment of a schema node: description, falling
// back to title.
func schemaDoc(v *jsonValue) string {
	if s, err := v.str("schema", "description"); err == nil {
		return s
	}
	if s, err := v.str("schema", "title"); err == nil {
		return s
	}
	return ""
}

// loadSchemas reads every *.json file in dir, compiles it and resolves all
// $ref pointers (rejecting cycles).
func loadSchemas(dir string) (*loader, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve schema dir %s: %w", dir, err)
	}
	entries, err := os.ReadDir(absDir)
	if err != nil {
		return nil, fmt.Errorf("read schema dir %s: %w", displayDir(dir), err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("read schema dir %s: no *.json schema files found", displayDir(dir))
	}
	// Deterministic order: sorted by file name, so the run never depends on the
	// directory iteration order of the host OS.
	sort.Strings(names)

	l := &loader{dir: absDir, displayDir: displayDir(dir), byPath: make(map[string]*schemaFile)}
	for _, name := range names {
		path := filepath.Join(absDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read schema %s: %w", displayDir(filepath.Join(dir, name)), err)
		}
		f, err := parseSchemaFile(path, displayPath(path), data)
		if err != nil {
			return nil, err
		}
		if _, dup := l.byPath[path]; dup {
			return nil, fmt.Errorf("%s: duplicate schema file", f.rel)
		}
		l.files = append(l.files, f)
		l.byPath[path] = f
	}
	if err := l.resolve(); err != nil {
		return nil, err
	}
	return l, nil
}

// displayDir renders a directory path the way it was given (cleaned, forward
// slashes) so error messages stay stable across machines.
func displayDir(dir string) string {
	if dir == "" {
		return "."
	}
	return filepath.ToSlash(filepath.Clean(dir))
}

// displayPath renders the path of a source file relative to the repository root
// (the directory holding go.mod) when it can be found.
//
// The header of every generated file names its source schema, and the generated
// output must be byte-identical no matter where the generator was invoked from,
// so the path is anchored at the repository root instead of the working
// directory. Outside a module it degrades to a working-directory-relative path
// and finally to the path as given.
func displayPath(path string) string {
	p := filepath.Clean(path)
	if root := findRepoRoot(p); root != "" {
		if rel, err := filepath.Rel(root, p); err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.ToSlash(rel)
		}
	}
	if wd, err := os.Getwd(); err == nil {
		if rel, err := filepath.Rel(wd, p); err == nil && !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel) {
			return filepath.ToSlash(rel)
		}
	}
	return filepath.ToSlash(p)
}

// findRepoRoot walks up from path looking for the module root.
func findRepoRoot(path string) string {
	dir := path
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		dir = filepath.Dir(dir)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// parseSchemaFile compiles one schema file into its $defs entries.
func parseSchemaFile(path, rel string, data []byte) (*schemaFile, error) {
	root, err := decodeJSON(data)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid JSON: %w", rel, err)
	}
	if root.Kind != jsonObject {
		return nil, fmt.Errorf("%s: top-level JSON value must be an object", rel)
	}
	defsVal, ok := root.Member("$defs")
	if !ok {
		// A common authoring slip: writing the type at the top level instead of
		// in $defs. Say so instead of just "missing $defs".
		if _, bad := root.Member("properties"); bad {
			return nil, fmt.Errorf("%s: top-level \"properties\" is not supported: every generated type must be a $defs entry", rel)
		}
		if _, bad := root.Member("type"); bad {
			return nil, fmt.Errorf("%s: a top-level \"type\" is not supported: every generated type must be a $defs entry", rel)
		}
		return nil, fmt.Errorf("%s: missing $defs: every generated type is a $defs entry", rel)
	}
	if defsVal.Kind != jsonObject {
		return nil, fmt.Errorf("%s: $defs must be an object", rel)
	}
	if len(defsVal.Members) == 0 {
		return nil, fmt.Errorf("%s: $defs is empty: no types to generate", rel)
	}

	f := &schemaFile{path: path, rel: rel, index: make(map[string]*def, len(defsVal.Members))}
	for _, m := range defsVal.Members {
		if !validGoIdent(m.Key) {
			return nil, fmt.Errorf("%s: $defs key %q is not a valid Go type name", rel, m.Key)
		}
		if goKeywords[m.Key] {
			return nil, fmt.Errorf("%s: $defs key %q is a Go keyword", rel, m.Key)
		}
		ctx := fmt.Sprintf("%s: $defs/%s", rel, m.Key)
		node, err := parseNode(m.Value, ctx)
		if err != nil {
			return nil, err
		}
		d := &def{name: m.Key, node: node, doc: schemaDoc(m.Value), file: f}
		f.defs = append(f.defs, d)
		f.index[m.Key] = d
	}
	return f, nil
}

// parseNode compiles one schema node, rejecting everything outside the subset.
func parseNode(v *jsonValue, ctx string) (*schemaNode, error) {
	if v == nil || v.Kind != jsonObject {
		return nil, fmt.Errorf("%s: schema must be a JSON object", ctx)
	}
	n := &schemaNode{}

	if refVal, ok := v.Member("$ref"); ok {
		ref, err := refVal.stringValue(ctx, "$ref")
		if err != nil {
			return nil, err
		}
		if ref == "" {
			return nil, fmt.Errorf("%s: $ref must not be empty", ctx)
		}
		n.kind = nodeRef
		n.ref = ref
		return n, nil
	}

	// D2 forbids complex composition: allOf/anyOf/oneOf/not.
	for _, kw := range []string{"allOf", "anyOf", "oneOf", "not"} {
		if _, ok := v.Member(kw); ok {
			return nil, fmt.Errorf("%s: %q is not supported: the codegen subset forbids complex composition (D2)", ctx, kw)
		}
	}

	// x-go-type is the escape hatch for meta-nested JSON (Tool.schema's value is
	// itself a JSON Schema): keep it as raw JSON instead of modelling it.
	if xg, ok := v.Member("x-go-type"); ok {
		s, err := xg.stringValue(ctx, "x-go-type")
		if err != nil {
			return nil, err
		}
		if s != "json.RawMessage" {
			return nil, fmt.Errorf("%s: unsupported x-go-type %q (only \"json.RawMessage\")", ctx, s)
		}
		if _, ok := v.Member("properties"); ok {
			return nil, fmt.Errorf("%s: x-go-type cannot be combined with properties", ctx)
		}
		n.kind = nodeRaw
		n.xGoType = s
		n.description = schemaDoc(v)
		return n, nil
	}

	n.description = schemaDoc(v)

	typeVal, hasType := v.Member("type")
	enumVal, hasEnum := v.Member("enum")

	// A missing "type" is only tolerated when the node is still recognisable:
	// an enum (JSON Schema says string) or an object with properties.
	if !hasType {
		switch {
		case hasEnum:
			return n.compileString(v, ctx, enumVal)
		case hasProperties(v):
			return n.compileObject(v, ctx)
		default:
			return nil, fmt.Errorf("%s: missing \"type\"", ctx)
		}
	}

	typeName, err := typeVal.stringValue(ctx, "type")
	if err != nil {
		if typeVal.Kind == jsonArray {
			return nil, fmt.Errorf("%s: type arrays such as [\"string\",\"null\"] are not supported: mark optionality with \"required\" instead", ctx)
		}
		return nil, err
	}

	switch typeName {
	case "object":
		return n.compileObject(v, ctx)
	case "array":
		return n.compileArray(v, ctx)
	case "string":
		return n.compileString(v, ctx, enumVal)
	case "integer", "number", "boolean":
		if hasEnum {
			return nil, fmt.Errorf("%s: \"enum\" is only supported for the string type", ctx)
		}
		switch typeName {
		case "integer":
			n.kind = nodeInteger
		case "number":
			n.kind = nodeNumber
		default:
			n.kind = nodeBoolean
		}
		return n, nil
	default:
		return nil, fmt.Errorf("%s: unknown type %q (supported: object, array, string, integer, number, boolean)", ctx, typeName)
	}
}

func hasProperties(v *jsonValue) bool {
	p, ok := v.Member("properties")
	return ok && p.Kind == jsonObject
}

func (n *schemaNode) compileObject(v *jsonValue, ctx string) (*schemaNode, error) {
	n.kind = nodeObject
	n.required = make(map[string]bool)
	propsVal, ok := v.Member("properties")
	if !ok {
		return n, nil // bare object → map[string]any
	}
	n.propIndex = make(map[string]bool, len(propsVal.Members))
	for _, m := range propsVal.Members {
		if m.Key == "" {
			return nil, fmt.Errorf("%s: property name must not be empty", ctx)
		}
		child, err := parseNode(m.Value, ctx+"."+m.Key)
		if err != nil {
			return nil, err
		}
		n.properties = append(n.properties, &schemaProp{
			name:        m.Key,
			node:        child,
			description: schemaDoc(m.Value),
		})
		n.propIndex[m.Key] = true
	}
	if reqVal, ok := v.Member("required"); ok {
		names, err := stringItems(reqVal, ctx, "required")
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			if !n.propIndex[name] {
				return nil, fmt.Errorf("%s: \"required\" lists %q, which is not a property", ctx, name)
			}
			n.required[name] = true
		}
	}
	return n, nil
}

func (n *schemaNode) compileArray(v *jsonValue, ctx string) (*schemaNode, error) {
	n.kind = nodeArray
	itemsVal, ok := v.Member("items")
	if !ok {
		n.items = &schemaNode{kind: nodeAny} // "array of anything" → []any
		return n, nil
	}
	if itemsVal.Kind == jsonArray {
		return nil, fmt.Errorf("%s: tuple validation (\"items\" as an array) is not supported", ctx)
	}
	child, err := parseNode(itemsVal, ctx+"[]")
	if err != nil {
		return nil, err
	}
	n.items = child
	return n, nil
}

func (n *schemaNode) compileString(v *jsonValue, ctx string, enumVal *jsonValue) (*schemaNode, error) {
	n.kind = nodeString
	if enumVal != nil {
		values, err := stringItems(enumVal, ctx, "enum")
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("%s: \"enum\" must not be empty", ctx)
		}
		seen := make(map[string]bool, len(values))
		for _, val := range values {
			if seen[val] {
				return nil, fmt.Errorf("%s: duplicate enum value %q", ctx, val)
			}
			seen[val] = true
			n.enum = append(n.enum, val)
		}
	}
	if formatVal, ok := v.Member("format"); ok {
		s, err := formatVal.stringValue(ctx, "format")
		if err != nil {
			return nil, err
		}
		n.format = s
	}
	if patternVal, ok := v.Member("pattern"); ok {
		s, err := patternVal.stringValue(ctx, "pattern")
		if err != nil {
			return nil, err
		}
		n.pattern = s
	}
	return n, nil
}

// resolve wires every $ref to its target definition and rejects cycles.
func (l *loader) resolve() error {
	for _, f := range l.files {
		for _, d := range f.defs {
			if err := l.resolveNode(d, d.node); err != nil {
				return err
			}
		}
	}
	return l.checkCycles()
}

func (l *loader) resolveNode(d *def, n *schemaNode) error {
	if n == nil {
		return nil
	}
	if n.kind == nodeRef {
		target, err := l.resolveRef(d, n.ref)
		if err != nil {
			return err
		}
		n.refTarget = target
		return nil
	}
	for _, p := range n.properties {
		if err := l.resolveNode(d, p.node); err != nil {
			return err
		}
	}
	return l.resolveNode(d, n.items)
}

// resolveRef resolves "#/$defs/X" (same file) and "other.json#/$defs/X"
// (relative to the referencing file's directory).
func (l *loader) resolveRef(from *def, ref string) (*def, error) {
	ctx := from.id()
	filePart, frag, found := strings.Cut(ref, "#")
	if !found || frag == "" {
		return nil, fmt.Errorf("%s: unsupported $ref %q (want \"#/$defs/Name\" or \"other.json#/$defs/Name\")", ctx, ref)
	}

	var targetPath string
	if filePart == "" {
		targetPath = from.file.path
	} else {
		targetPath = filepath.Clean(filepath.Join(filepath.Dir(from.file.path), filepath.FromSlash(filePart)))
	}

	parts := strings.Split(strings.TrimPrefix(frag, "/"), "/")
	if len(parts) != 2 || parts[1] == "" {
		return nil, fmt.Errorf("%s: unsupported $ref %q: only \"/$defs/<Name>\" fragments are supported", ctx, ref)
	}
	if parts[0] != "$defs" {
		return nil, fmt.Errorf("%s: unsupported $ref %q: only \"/$defs/<Name>\" fragments are supported", ctx, ref)
	}
	name := jsonPointerUnescape(parts[1])

	targetFile, ok := l.byPath[targetPath]
	if !ok {
		return nil, fmt.Errorf("%s: unresolved $ref %q: %s is not a schema file in %s", ctx, ref, filePart, l.displayDir)
	}
	target, ok := targetFile.index[name]
	if !ok {
		return nil, fmt.Errorf("%s: unresolved $ref %q: %s has no $defs entry %q", ctx, ref, filePart, name)
	}
	return target, nil
}

// jsonPointerUnescape decodes the ~1 / ~0 escapes of a JSON Pointer segment.
func jsonPointerUnescape(s string) string {
	if !strings.Contains(s, "~") {
		return s
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
}

// checkCycles rejects $ref cycles (self-references included) with the path that
// closes the loop.
func (l *loader) checkCycles() error {
	const (
		white = 0 // unvisited
		grey  = 1 // on the current DFS stack
		black = 2 // fully visited
	)
	state := make(map[*def]int, len(l.files))
	pos := make(map[*def]int, len(l.files))
	var stack []*def

	var visit func(d *def) error
	visit = func(d *def) error {
		switch state[d] {
		case grey:
			cycle := make([]string, 0, len(stack)-pos[d]+1)
			for _, s := range stack[pos[d]:] {
				cycle = append(cycle, s.id())
			}
			cycle = append(cycle, d.id())
			return fmt.Errorf("$ref cycle detected: %s", strings.Join(cycle, " -> "))
		case black:
			return nil
		}
		state[d] = grey
		pos[d] = len(stack)
		stack = append(stack, d)
		for _, target := range refTargets(d.node) {
			if err := visit(target); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		delete(pos, d)
		state[d] = black
		return nil
	}

	for _, f := range l.files {
		for _, d := range f.defs {
			if err := visit(d); err != nil {
				return err
			}
		}
	}
	return nil
}

// refTargets lists every definition reachable from n, in schema order.
func refTargets(n *schemaNode) []*def {
	if n == nil {
		return nil
	}
	if n.kind == nodeRef {
		return []*def{n.refTarget}
	}
	var out []*def
	for _, p := range n.properties {
		out = append(out, refTargets(p.node)...)
	}
	out = append(out, refTargets(n.items)...)
	return out
}
