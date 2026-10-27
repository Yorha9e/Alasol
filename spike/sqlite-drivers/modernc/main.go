// Command modernc runs the SQLite conformance suite against
// modernc.org/sqlite — a transpilation of the C SQLite source to pure Go
// (no cgo, so cross-compiling for Windows needs no C toolchain).
//
// Subcommands:
//
//	modernc                 run the in-process conformance suite
//	modernc hold FILE MS    open FILE and keep writing to it (other process)
//	modernc probe FILE      open FILE and report what another process writes
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"example.com/harness/spike/sqlitecommon"

	_ "modernc.org/sqlite"
)

func config() sqlitecommon.Config {
	// "_pragma" is applied by the driver to every connection it opens.
	// The "=" inside busy_timeout(...) is percent-encoded so the DSN stays
	// a valid URL query.
	dsn := fmt.Sprintf("file:{FILE}?_pragma=journal_mode(WAL)&_pragma=busy_timeout%%3d%d",
		sqlitecommon.BusyTimeoutMS)
	return sqlitecommon.Config{
		DriverName: "sqlite",
		DSNTemplate: dsn,
		TxLock:     "immediate",
		Label:      "modernc.org/sqlite",
	}
}

func main() {
	cfg := config()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "hold":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "usage: modernc hold FILE MS")
				os.Exit(2)
			}
			ms, _ := strconv.Atoi(os.Args[3])
			n, err := sqlitecommon.Hold(cfg, os.Args[2], time.Duration(ms)*time.Millisecond)
			fmt.Printf("HOLD wrote=%d err=%v\n", n, err)
			return
		case "probe":
			if len(os.Args) < 3 {
				fmt.Fprintln(os.Stderr, "usage: modernc probe FILE")
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
