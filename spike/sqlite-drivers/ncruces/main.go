// Command ncruces runs the SQLite conformance suite against
// github.com/ncruces/go-sqlite3 — the real SQLite source compiled to
// WebAssembly and translated to Go (wasm2go), with a pure-Go VFS.
//
// Because it *is* SQLite (not a transpilation), its semantics are the
// reference. The cost is throughput: everything runs through a WASM sandbox.
//
// Subcommands:
//
//	ncruces                 run the in-process conformance suite
//	ncruces hold FILE MS    open FILE and keep writing to it (other process)
//	ncruces probe FILE      open FILE and report what another process writes
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"example.com/harness/spike/sqlitecommon"

	_ "github.com/ncruces/go-sqlite3/driver"
)

func config() sqlitecommon.Config {
	dsn := fmt.Sprintf("file:{FILE}?_pragma=journal_mode(WAL)&_pragma=busy_timeout%%3d%d",
		sqlitecommon.BusyTimeoutMS)
	return sqlitecommon.Config{
		DriverName: "sqlite3",
		DSNTemplate: dsn,
		TxLock:     "immediate",
		Label:      "github.com/ncruces/go-sqlite3",
	}
}

func main() {
	cfg := config()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "hold":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "usage: ncruces hold FILE MS")
				os.Exit(2)
			}
			ms, _ := strconv.Atoi(os.Args[3])
			n, err := sqlitecommon.Hold(cfg, os.Args[2], time.Duration(ms)*time.Millisecond)
			fmt.Printf("HOLD wrote=%d err=%v\n", n, err)
			return
		case "probe":
			if len(os.Args) < 3 {
				fmt.Fprintln(os.Stderr, "usage: ncruces probe FILE")
				os.Exit(2)
			}
			res, err := sqlitecommon.Probe(cfg, os.Args[2])
			fmt.Printf("PROBE visible=%d write_err=%v rows_after=%d err=%v\n",
				res.Visible, res.WriteErr, res.RowsAfter, err)
			return
		}
	}

	rep, err := sqlitecommon.Run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
	sqlitecommon.Print(rep)
}
