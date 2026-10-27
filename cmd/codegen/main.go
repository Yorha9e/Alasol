// Command codegen compiles the hand-written JSON Schema files in
// contracts/schema into the Go types in contracts.
//
// Usage:
//
//	go run ./cmd/codegen [-schema dir] [-out dir] [-package name] [-check]
//
// The generated files are checked in; CI re-runs the generator with -check so
// that "re-generate → empty diff" is enforced. Any schema error ($ref cycle,
// missing $defs, unsupported keyword, ...) is reported on stderr and exits
// non-zero without writing anything.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Yorha9e/alasol/internal/codegen"
)

func main() {
	fs := flag.NewFlagSet("codegen", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: go run ./cmd/codegen [flags]\n\n")
		fmt.Fprintf(fs.Output(), "Generates Go types from the JSON Schema contract files.\n\n")
		fs.PrintDefaults()
	}
	schemaDir := fs.String("schema", codegen.DefaultSchemaDir, "directory containing the *.json schema files")
	outDir := fs.String("out", codegen.DefaultOutDir, "directory the generated *.go files are written to")
	pkg := fs.String("package", codegen.DefaultPackage, "Go package name of the generated files")
	check := fs.Bool("check", false, "drift check only: write nothing, fail if any generated file is missing or stale")
	if err := fs.Parse(os.Args[1:]); err != nil {
		// flag.ExitOnError already reported the problem on stderr.
		os.Exit(2)
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "codegen: unexpected argument %q (flags only, e.g. -schema/-out)\n", fs.Arg(0))
		fs.Usage()
		os.Exit(2)
	}

	results, err := codegen.Run(codegen.Options{
		SchemaDir: *schemaDir,
		OutDir:    *outDir,
		Package:   *pkg,
		Check:     *check,
	})
	if err != nil {
		// Every error returned by codegen.Run already carries the "codegen:"
		// prefix, so it is printed as-is.
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	for _, r := range results {
		if r.Removed {
			fmt.Printf("%s: removed (no matching schema file)\n", r.Output)
			continue
		}
		status := "generated"
		if *check {
			status = "up to date"
		} else if !r.Written {
			status = "unchanged"
		}
		fmt.Printf("%s: %d types, %d bytes (%s)\n", r.Output, r.Types, r.Bytes, status)
	}
}
