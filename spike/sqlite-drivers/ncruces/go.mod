module example.com/harness/spike/sqlitedrivers/ncruces

go 1.26.0

require (
	example.com/harness/spike/sqlitecommon v0.0.0
	github.com/ncruces/go-sqlite3 v0.35.6
)

require (
	github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace example.com/harness/spike/sqlitecommon => ../common
