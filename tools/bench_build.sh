#!/usr/bin/env bash
# What the Bazel migration buys, measured rather than asserted.
#
#   1. cold:   empty output base, empty remote cache
#   2. no-op:  nothing changed
#   3. fresh:  empty output base, warm remote cache, which is a new CI runner
#   4. edit:   one package changed; which tests re-run, which come from cache
set -euo pipefail
cd "$(dirname "$0")/.."
BAZEL=${BAZEL:-bazelisk}
CACHE_DIR=${CACHE_DIR:-/tmp/bazel-remote-data}
OUT=${OUT:-tools/build-bench.txt}
: > "$OUT"

cache_up() {
  docker rm -f bazel-remote >/dev/null 2>&1 || true
  rm -rf "$CACHE_DIR" && mkdir -p "$CACHE_DIR"
  docker run -d --name bazel-remote -p 127.0.0.1:9092:8080 -v "$CACHE_DIR":/data \
    buchgr/bazel-remote-cache --max_size=5 --dir=/data >/dev/null
  for _ in $(seq 1 30); do curl -fsS -o /dev/null http://127.0.0.1:9092/status && return; sleep 1; done
  echo "bazel-remote did not start" >&2; exit 1
}

run() { # label, then bazel args
  local label=$1; shift
  local t0 t1 procs
  t0=$(python3 -c 'import time; print(time.time())')
  $BAZEL "$@" > /tmp/bb.log 2>&1
  t1=$(python3 -c 'import time; print(time.time())')
  procs=$(grep -E "^INFO: [0-9]+ process(es)?:" /tmp/bb.log | tail -1 | sed 's/^INFO: //' || true)
  printf "%-8s %6.1fs  %s\n" "$label" "$(python3 -c "print($t1-$t0)")" "$procs" | tee -a "$OUT"
}

cache_up
$BAZEL clean --expunge >/dev/null 2>&1
run cold  test //... --config=remote
run noop  test //... --config=remote
$BAZEL clean --expunge >/dev/null 2>&1
run fresh test //... --config=remote

echo "--- tests affected by a change to lincheck ---" | tee -a "$OUT"
$BAZEL query 'kind(".*_test", rdeps(//..., //lincheck:lincheck))' 2>/dev/null | tee -a "$OUT"
# A comment-only edit: lincheck recompiles, but Go emits the same archive, so
# every test's inputs hash the same and nothing re-runs. That is early cutoff.
cp lincheck/lincheck.go /tmp/lincheck.go.orig
echo "// touched by tools/bench_build.sh" >> lincheck/lincheck.go
run comment test //... --config=remote
grep -E "PASSED|FAILED" /tmp/bb.log | sed 's/^/  /' | tee -a "$OUT"

# An unused function: the archive changes, but the linker drops dead code, so
# the test binary is identical and the test still does not re-run.
printf '\nfunc benchOnlyChange() int { return 1 }\n' >> lincheck/lincheck.go
run deadcode test //... --config=remote
grep -E "PASSED|FAILED" /tmp/bb.log | sed 's/^/  /' | tee -a "$OUT"

# A change that reaches the binary: a string the checker actually returns. Now
# lincheck's test must re-run, and the two tests that do not depend on lincheck
# must still come from cache. That is affected-target test selection.
cp /tmp/lincheck.go.orig lincheck/lincheck.go
sed -i.bak 's/no sequential order explains this history/no sequential order explains this history./' lincheck/lincheck.go && rm -f lincheck/lincheck.go.bak
run live test //... --config=remote
grep -E "PASSED|FAILED" /tmp/bb.log | sed 's/^/  /' | tee -a "$OUT"
cp /tmp/lincheck.go.orig lincheck/lincheck.go
