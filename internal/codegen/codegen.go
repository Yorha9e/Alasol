package codegen

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Defaults matching the repository layout: schemas live in contracts/schema and
// the generated types land next to them in contracts.
const (
	DefaultSchemaDir = "contracts/schema"
	DefaultOutDir    = "contracts"
	DefaultPackage   = "contracts"
)

// Options configures one generation run.
type Options struct {
	// SchemaDir is the directory holding the *.json draft-07 schema files.
	SchemaDir string
	// OutDir is the directory the generated *.go files are written to.
	OutDir string
	// Package is the Go package name of the generated files.
	// Empty means DefaultPackage.
	Package string
	// Check turns the run into a drift detector: nothing is written and a
	// non-nil error is returned when any generated file is missing or stale.
	// This is what CI uses to enforce "re-generate → empty diff".
	Check bool
}

// Result reports one generated file.
type Result struct {
	// Schema is the display path of the source schema file.
	Schema string
	// Output is the path of the generated Go file (as joined from OutDir).
	Output string
	// Types is the number of $defs entries compiled into the file.
	Types int
	// Bytes is the size of the generated (gofmt-clean) source.
	Bytes int
	// Written reports whether the file was actually rewritten. A file whose
	// content already matches is left untouched, keeping its mtime stable.
	Written bool
}

// Run compiles every *.json schema in opts.SchemaDir and writes one Go file per
// schema into opts.OutDir.
//
// It returns the results in deterministic (file name) order, or an error that
// names the offending schema file and $defs entry. Nothing is written when the
// run fails, so a broken schema never leaves a half-generated tree behind.
func Run(opts Options) ([]Result, error) {
	if strings.TrimSpace(opts.SchemaDir) == "" {
		return nil, errors.New("codegen: SchemaDir must not be empty")
	}
	if strings.TrimSpace(opts.OutDir) == "" {
		return nil, errors.New("codegen: OutDir must not be empty")
	}
	pkg := opts.Package
	if pkg == "" {
		pkg = DefaultPackage
	}
	if !validGoIdent(pkg) || goKeywords[pkg] {
		return nil, fmt.Errorf("codegen: %q is not a valid Go package name", pkg)
	}

	schemas, err := loadSchemas(opts.SchemaDir)
	if err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	if !opts.Check {
		if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
			return nil, fmt.Errorf("codegen: create output dir %s: %w", displayDir(opts.OutDir), err)
		}
	}

	// Phase 1: compile everything in memory. A single broken schema aborts the
	// run before anything is written, so the output tree is never left
	// half-generated.
	type pendingWrite struct {
		res Result
		src []byte
	}
	var writes []pendingWrite
	owners := make(map[string]string, len(schemas.files))
	declared := make(map[string]string) // package-wide name → declaring schema
	for _, f := range schemas.files {
		name, err := goFileName(f.path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.rel, err)
		}
		if prev, dup := owners[name]; dup {
			return nil, fmt.Errorf("codegen: %s and %s both generate %s", prev, f.rel, name)
		}
		owners[name] = f.rel

		src, names, err := generateGoFile(f, pkg)
		if err != nil {
			return nil, fmt.Errorf("codegen: %w", err)
		}
		// All generated files share one package, so a name may only be declared
		// once across the whole run.
		for _, n := range names {
			if prev, dup := declared[n]; dup {
				return nil, fmt.Errorf("codegen: %s and %s both declare %s: generated files share package %s, so a $defs name must be unique across all schema files", prev, f.rel, n, pkg)
			}
			declared[n] = f.rel
		}
		writes = append(writes, pendingWrite{
			res: Result{
				Schema: f.rel,
				Output: filepath.ToSlash(filepath.Join(opts.OutDir, name)),
				Types:  len(f.defs),
				Bytes:  len(src),
			},
			src: src,
		})
	}

	// Phase 2: compare with what is on disk and write the differences.
	results := make([]Result, 0, len(writes))
	for _, w := range writes {
		existing, readErr := os.ReadFile(w.res.Output)
		switch {
		case readErr != nil && !errors.Is(readErr, fs.ErrNotExist):
			return nil, fmt.Errorf("codegen: read %s: %w", w.res.Output, readErr)
		case bytes.Equal(existing, w.src):
			// Already up to date: do not touch the file (stable mtime, and a
			// second run is a no-op).
		case opts.Check:
			if errors.Is(readErr, fs.ErrNotExist) {
				return nil, fmt.Errorf("codegen: %s is missing: run go run ./cmd/codegen", w.res.Output)
			}
			return nil, fmt.Errorf("codegen: %s is out of date: run go run ./cmd/codegen", w.res.Output)
		default:
			if err := os.WriteFile(w.res.Output, w.src, 0o644); err != nil {
				return nil, fmt.Errorf("codegen: write %s: %w", w.res.Output, err)
			}
			w.res.Written = true
		}
		results = append(results, w.res)
	}
	return results, nil
}

// goFileName maps a schema file name to its generated Go file name:
// tool-result.json → tool_result.go.
func goFileName(schemaPath string) (string, error) {
	base := filepath.Base(schemaPath)
	stem := strings.TrimSuffix(base, ".json")
	stem = strings.ReplaceAll(stem, "-", "_")
	if stem == "" {
		return "", fmt.Errorf("codegen: schema file name %q has no stem", base)
	}
	for i, r := range stem {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case i > 0 && r >= '0' && r <= '9':
		default:
			return "", fmt.Errorf("codegen: schema file name %q does not map to a valid Go file name", base)
		}
	}
	return stem + ".go", nil
}
