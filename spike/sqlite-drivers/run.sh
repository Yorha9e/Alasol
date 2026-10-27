#!/usr/bin/env bash
# Runs the SQLite driver conformance spike for every candidate driver:
#   1. in-process conformance suite
#   2. cross-process check (two processes on the same database file —
#      the real hub + CLI scenario)
#   3. pure-Go cross-compile check (windows/amd64, CGO_ENABLED=0)
#
#   ./run.sh              # run everything
#   ./run.sh modernc      # run one driver only
set -u

cd "$(dirname "$0")"

DRIVERS=("modernc" "ncruces")
if [ $# -gt 0 ]; then
  DRIVERS=("$@")
fi

export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export CGO_ENABLED="${CGO_ENABLED:-0}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "### Go: $(go version)"
echo "### GOPROXY: $GOPROXY"
echo

fail=0

for d in "${DRIVERS[@]}"; do
  echo "=============================================================="
  echo "### driver: $d"
  echo "=============================================================="
  (
    cd "$d" || exit 1
    go mod tidy >/dev/null 2>&1 || { echo "go mod tidy failed"; exit 1; }
    go vet ./... || { echo "go vet failed"; exit 1; }
    go build -o "$TMP/$d.exe" . || { echo "go build failed"; exit 1; }
    "$TMP/$d.exe"
  )
  rc=$?
  [ $rc -ne 0 ] && fail=1
  echo

  echo "--- cross-process check (two processes, same file) ---"
  db="$TMP/xproc-$d.db"
  "$TMP/$d.exe" hold "$db" 2500 > "$TMP/hold-$d.log" 2>&1 &
  holdpid=$!
  sleep 1.0
  "$TMP/$d.exe" probe "$db"
  wait $holdpid
  cat "$TMP/hold-$d.log"
  echo
done

echo "=============================================================="
echo "### pure-Go cross-compile check (windows/amd64, CGO_ENABLED=0)"
echo "=============================================================="
for d in "${DRIVERS[@]}"; do
  ( cd "$d" && GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o "$TMP/x-$d.exe" . ) \
    && echo "  [PASS] $d builds for windows/amd64 with CGO_ENABLED=0" \
    || { echo "  [FAIL] $d cross-compile failed"; fail=1; }
done

echo
if [ $fail -eq 0 ]; then
  echo "all drivers exercised"
else
  echo "one or more drivers failed to run (see above)"
fi
exit $fail
