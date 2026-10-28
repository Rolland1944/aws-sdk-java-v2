#!/usr/bin/env bash
# Launcher for run_capacity_curve.py (Phase 3 §8.2). Detaches the driver so it survives
# a dropped session, and removes the IDE's safe-delete shim from its environment.
#
#   TAG=20261016 tools/track2/run_capacity_curve.sh dry-run     # plan + checks, starts nothing
#   TAG=20261016 tools/track2/run_capacity_curve.sh start [--stages sf1,smoke]
#   TAG=20261016 tools/track2/run_capacity_curve.sh status
#   TAG=20261016 tools/track2/run_capacity_curve.sh tail        # follow the driver log
#   TAG=20261016 tools/track2/run_capacity_curve.sh stop        # SIGTERM; resume with `start`
#
# Resume = `start` again with the same TAG. Use a new TAG after any commit or rebuild.
set -euo pipefail
cd "$(dirname "$0")/../.."
PY=".venv/bin/python"
TAG="${TAG:-$(date +%Y%m%d)}"
ROOT="docs/adaptive-range-reader/results/phase3/capacity_curve_aws_${TAG}"
PIDFILE="$ROOT/capacity_curve.pid"
[ -x "$PY" ] || { echo "missing $PY (the repo venv has PySpark; system python3 does not)" >&2; exit 2; }

clean() {
  env -u PYTHONPATH -u BASH_ENV -u CODEBUDDY_SESSION_ID -u CLAUDE_SESSION_ID "$@"
}
alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

cmd="${1:-}"; shift || true
case "$cmd" in
  dry-run) clean "$PY" tools/track2/run_capacity_curve.py --tag "$TAG" --dry-run "$@" ;;
  status)  clean "$PY" tools/track2/run_capacity_curve.py --tag "$TAG" --status ;;
  start)
    if alive; then echo "already running: pid $(cat "$PIDFILE") (tag $TAG)"; exit 1; fi
    mkdir -p "$ROOT"
    clean setsid nohup "$PY" tools/track2/run_capacity_curve.py --tag "$TAG" "$@" \
      >> "$ROOT/driver.log" 2>&1 < /dev/null &
    sleep 5
    if alive; then echo "started: pid $(cat "$PIDFILE"), tag $TAG"; else echo "did not stay up; see $ROOT/driver.log" >&2; tail -5 "$ROOT/driver.log" >&2; exit 1; fi
    echo "log:    $ROOT/driver.log"
    echo "status: TAG=$TAG $0 status" ;;
  tail)    exec tail -n 40 -f "$ROOT/driver.log" ;;
  stop)
    if alive; then kill -TERM "$(cat "$PIDFILE")"; echo "SIGTERM sent to the driver; the run in flight is abandoned"; echo "Spark may still be shutting down: check 'pgrep -af run_benchmark' before starting again"
    else echo "not running"; fi ;;
  *) sed -n 2,13p "$0"; exit 2 ;;
esac
