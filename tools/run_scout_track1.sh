#!/usr/bin/env bash
# Scout-round Track1 real-S3 pipeline (PROJECT3 measure-only, reduced matrix).
# Idempotent: skips stages whose stamp files already exist under $OUT_DIR.
#
# Usage:
#   nohup tools/run_scout_track1.sh > /mnt/nvme/logs/scout_track1.log 2>&1 &
#
# Stages: wait_provision -> compile -> E2 -> E3 -> E4 -> archive
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

DATE_TAG="${DATE_TAG:-$(date +%Y%m%d)}"
OUT_DIR="${OUT_DIR:-$REPO/docs/adaptive-range-reader/results/aws-s3_scout_${DATE_TAG}}"
LOG_DIR="${LOG_DIR:-/mnt/nvme/logs}"
STAMP_DIR="$OUT_DIR/stamps"
BENCH_DIR="$REPO/services-custom/s3-adaptive-range-reader/target/s3arr-benchmark"
TRACE="${TRACE:-$REPO/traces/mixed_holdout_s3.csv}"
MODULE="services-custom/s3-adaptive-range-reader"
BUCKET="${S3ARR_BUCKET:-home-haoyue}"
REGION="${S3ARR_REGION:-us-east-2}"
ENDPOINT="${S3ARR_ENDPOINT:-https://s3.${REGION}.amazonaws.com}"
STRIP_PREFIX="mixed_holdout"

MVN_FLAGS=(-pl "$MODULE"
  -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Dpmd.skip=true
  -DfailIfNoTests=false)

mkdir -p "$OUT_DIR" "$STAMP_DIR" "$LOG_DIR" "$BENCH_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

stamp() { touch "$STAMP_DIR/$1"; }
has_stamp() { [[ -f "$STAMP_DIR/$1" ]]; }

require_trace() {
  if [[ ! -f "$TRACE" ]]; then
    log "ERROR: missing $TRACE (provisioner writes it). Abort."
    exit 1
  fi
}

export_s3arr_env() {
  export S3ARR_BUCKET="$BUCKET"
  export S3ARR_REGION="$REGION"
  export S3ARR_ENDPOINT="$ENDPOINT"
  if [[ -z "${S3ARR_ACCESS_KEY:-}" ]]; then
    export S3ARR_ACCESS_KEY
    S3ARR_ACCESS_KEY="$(aws configure get aws_access_key_id)"
  fi
  if [[ -z "${S3ARR_SECRET_KEY:-}" ]]; then
    export S3ARR_SECRET_KEY
    S3ARR_SECRET_KEY="$(aws configure get aws_secret_access_key)"
  fi
  if [[ -z "${S3ARR_ACCESS_KEY:-}" || -z "${S3ARR_SECRET_KEY:-}" ]]; then
    log "ERROR: cannot resolve S3ARR_ACCESS_KEY / S3ARR_SECRET_KEY"
    exit 1
  fi
  log "S3ARR env ready (bucket=$BUCKET region=$REGION endpoint=$ENDPOINT)"
}

wait_provision() {
  if has_stamp wait_provision; then log "skip wait_provision"; return; fi
  log "=== wait for mixed_holdout provision ==="
  local deadline=$((SECONDS + 7200))
  while (( SECONDS < deadline )); do
    if ! pgrep -f 'provision_mixed_holdout.py' >/dev/null 2>&1; then
      if [[ -f /mnt/nvme/logs/mixed_holdout_manifest.json ]]; then
        log "provision finished (manifest present)"
        break
      fi
      # maybe finished before stamp; verify object count/size on S3
      local n total
      n="$(aws s3 ls "s3://${BUCKET}/mixed_holdout/" --region "$REGION" --recursive --summarize 2>/dev/null | awk '/Total Objects/{print $3}')"
      total="$(aws s3 ls "s3://${BUCKET}/mixed_holdout/" --region "$REGION" --recursive --summarize 2>/dev/null | awk '/Total Size/{print $3}')"
      log "provision process gone; s3 objects=${n:-?} bytes=${total:-?}"
      if [[ "${n:-0}" -ge 30 && "${total:-0}" -ge 15000000000 ]]; then
        break
      fi
      log "ERROR: provision seems incomplete"
      exit 1
    fi
    local total
    total="$(aws s3 ls "s3://${BUCKET}/mixed_holdout/" --region "$REGION" --recursive --summarize 2>/dev/null | awk '/Total Size/{print $3}')"
    log "still provisioning... uploaded_bytes=${total:-0}"
    sleep 60
  done
  if (( SECONDS >= deadline )); then
    log "ERROR: provision wait timed out"
    exit 1
  fi
  stamp wait_provision
}

stage_compile() {
  if has_stamp compile; then log "skip compile"; return; fi
  log "=== compile module ==="
  # build-tools needed by checkstyle plugin resolution even when skipped
  mvn -q -pl build-tools -DskipTests install
  mvn -q "${MVN_FLAGS[@]}" -DskipTests test-compile
  stamp compile
}

run_bench() {
  local label="$1"; shift
  log "RUN label=$label $*"
  # Move aside cumulative CSV so labels don't collide across stages
  if [[ -f "$BENCH_DIR/results-v2.csv" ]]; then
    mv -f "$BENCH_DIR/results-v2.csv" "$BENCH_DIR/results-v2.${label}.prev.csv" || true
  fi
  mvn -q "${MVN_FLAGS[@]}" \
    -Dtest=AdaptiveReaderSystemBenchmark test \
    -Ds3arr.backend=s3 \
    -Ds3arr.trace="$TRACE" \
    -Ds3arr.stripKeyPrefix="$STRIP_PREFIX" \
    -Ds3arr.label="$label" \
    -Ds3arr.warmup=0 -Ds3arr.iters=1 \
    -DargLine="-Xmx12g" \
    "$@"
  # Copy fresh rows into OUT_DIR
  if [[ -f "$BENCH_DIR/results-v2.csv" ]]; then
    cp -f "$BENCH_DIR/results-v2.csv" "$OUT_DIR/results-v2.${label}.csv"
  fi
  # copy newest report
  local report
  report="$(ls -t "$BENCH_DIR"/report-"${label}"-*.txt 2>/dev/null | head -1 || true)"
  if [[ -n "$report" ]]; then
    cp -f "$report" "$OUT_DIR/"
  fi
}

stage_e2() {
  if has_stamp e2; then log "skip e2"; return; fi
  log "=== E2 passthrough baseline x2 ==="
  require_trace
  run_bench "e2_passthrough_r1" -Ds3arr.selector=passthrough -Ds3arr.apps=1
  run_bench "e2_passthrough_r2" -Ds3arr.selector=passthrough -Ds3arr.apps=1
  stamp e2
}

# cost-oracle table at (256MiB, block=1, depth=0) from access_report/sdk_sweep_oracles.json
# From access_report/sdk_sweep_oracles.json key "256:1:0" / cost
COST_ORACLE_256="mixed_holdout/clickbench:template_locality,mixed_holdout/emb_real:s3a_prefetch,mixed_holdout/fmnist:s3a_prefetch,mixed_holdout/lastfm:s3a_prefetch,mixed_holdout/mm:s3a_prefetch,mixed_holdout/sift:s3a_prefetch,mixed_holdout/taxi:template_multimodal,mixed_holdout/tpch:s3a_prefetch"

stage_e3() {
  if has_stamp e3; then log "skip e3"; return; fi
  log "=== E3 Track1 four-column + oracle@256 + depth 0/1 ==="
  require_trace
  local b
  for b in 128 256 1024; do
    # default four-column comparison (no selector)
    run_bench "e3_four_b${b}_d0" \
      -Ds3arr.cacheBudgetMiB="$b" \
      -Ds3arr.prefetchBlockMiB=1 \
      -Ds3arr.prefetchDepth=0 \
      -Ds3arr.apps=1
  done
  # second repeat only at 256
  run_bench "e3_four_b256_d0_r2" \
    -Ds3arr.cacheBudgetMiB=256 \
    -Ds3arr.prefetchBlockMiB=1 \
    -Ds3arr.prefetchDepth=0 \
    -Ds3arr.apps=1

  # depth 1 at 256 with thinkMs for overlap window
  run_bench "e3_four_b256_d1" \
    -Ds3arr.cacheBudgetMiB=256 \
    -Ds3arr.prefetchBlockMiB=1 \
    -Ds3arr.prefetchDepth=1 \
    -Ds3arr.thinkMs=5 \
    -Ds3arr.apps=1

  # oracle_static: four forced policies @ 256
  local pol
  for pol in s3a_random template_locality s3a_prefetch template_multimodal; do
    run_bench "e3_force_${pol}_b256" \
      -Ds3arr.cacheBudgetMiB=256 \
      -Ds3arr.prefetchBlockMiB=1 \
      -Ds3arr.prefetchDepth=0 \
      -Ds3arr.forcePolicy="$pol" \
      -Ds3arr.apps=1
  done

  # oracle_perworkload (proxy cost table) @ 256 — scout only; formal run recomputes from wall-clock
  run_bench "e3_oracle_cost_b256" \
    -Ds3arr.cacheBudgetMiB=256 \
    -Ds3arr.prefetchBlockMiB=1 \
    -Ds3arr.prefetchDepth=0 \
    -Ds3arr.oracleMap="$COST_ORACLE_256" \
    -Ds3arr.apps=1

  stamp e3
}

stage_e4() {
  if has_stamp e4; then log "skip e4"; return; fi
  log "=== E4 g* block sweep (depth=0, force s3a_prefetch) ==="
  require_trace
  # 256KiB, 1MiB, 4MiB, 16MiB — g* from E1 ≈ 2.56 MiB
  local kib
  for kib in 256 1024 4096 16384; do
    run_bench "e4_block_${kib}KiB" \
      -Ds3arr.cacheBudgetMiB=256 \
      -Ds3arr.prefetchBlockKiB="$kib" \
      -Ds3arr.prefetchDepth=0 \
      -Ds3arr.forcePolicy=s3a_prefetch \
      -Ds3arr.apps=1
  done
  stamp e4
}

stage_archive() {
  if has_stamp archive; then log "skip archive"; return; fi
  log "=== archive meta ==="
  {
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "region=$REGION"
    echo "bucket=$BUCKET"
    echo "trace=$TRACE"
    echo "git=$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
    echo "java=$(java -version 2>&1 | head -1)"
    echo "e1_summary=$REPO/docs/adaptive-range-reader/results/aws-s3_scout_e1/e1_summary.json"
  } > "$OUT_DIR/env_meta.txt"
  # copy e1 if present
  if [[ -d "$REPO/docs/adaptive-range-reader/results/aws-s3_scout_e1" ]]; then
    mkdir -p "$OUT_DIR/e1"
    cp -f "$REPO/docs/adaptive-range-reader/results/aws-s3_scout_e1/"* "$OUT_DIR/e1/" 2>/dev/null || true
  fi
  # concatenate all results-v2 snippets
  {
    echo "timestamp,label,mode,scope,prefix,reads,logicalBytes,remoteGets,remoteBytes,readAmp,cacheHitRate,policySwitches,fallbackReads,demandClampReads,ioLatencySumNanos,nsPerOp,p50Ns,p95Ns,p99Ns,prefetchGets,prefetchBytes,prefetchUsefulBytes,prefetchWastedBytes,prefetchCancelledBytes"
    for f in "$OUT_DIR"/results-v2.*.csv; do
      [[ -f "$f" ]] || continue
      tail -n +2 "$f"
    done
  } > "$OUT_DIR/results-v2.all.csv"
  stamp archive
  log "ALL SCOUT STAGES DONE -> $OUT_DIR"
}

main() {
  log "scout start REPO=$REPO OUT_DIR=$OUT_DIR"
  export_s3arr_env
  wait_provision
  stage_compile
  stage_e2
  stage_e3
  stage_e4
  stage_archive
  log "done"
}

main "$@"
