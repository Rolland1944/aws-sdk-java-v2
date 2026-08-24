#!/usr/bin/env python3
"""One-pass column statistics over the canonical TPC-H source.

Virtual footer needs, for every (table, column): NDV, null count, and a 1000-bucket
approximate quantile CDF. DB2 reads these from the catalog; we have to scan the
canonical DuckDB dbgen source once and reuse the result for every candidate.

DuckDB, not Spark: the numbers are data properties, not engine properties, and
approx_quantile / approx_count_distinct are the matching primitives. SF100
lineitem is ~22 GiB compressed; expect tens of minutes.

Usage:
  python3 tools/track2/column_stats.py \
      --source /data/home/haoyueli/track2-data/tpch_sf100 \
      --out docs/adaptive-range-reader/results/track2/e5_whatif/column_stats.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timezone

import duckdb

TPCH_TABLES = ["customer", "lineitem", "nation", "orders",
               "part", "partsupp", "region", "supplier"]

N_BUCKETS = 1000
# DuckDB approx_quantile has no VARCHAR overload. String predicates use NDV.
SKIP_QUANTILE_TYPES = ("VARCHAR", "BLOB", "BOOLEAN")


def _jsonable(v):
    if v is None:
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except ImportError:
        pass
    if hasattr(v, "as_py"):
        return _jsonable(v.as_py())
    return v


def list_columns(con, path):
    parquet = parquet_scan(path)
    rel = con.sql(f"SELECT * FROM {parquet} LIMIT 0")
    return [(c, str(t)) for c, t in zip(rel.columns, rel.types)]


def parquet_scan(path):
    if os.path.isfile(path):
        return f"read_parquet('{path}')"
    return f"read_parquet('{path}/*.parquet')"


def _ident(name):
    return f'"{name}"'


def stats_for_table(con, table, path, only_columns=None, date_expr=None):
    """One scan per table: NDV + nulls + quantile CDF for every usable column."""
    cols = list_columns(con, path)
    if only_columns:
        want = set(only_columns)
        cols = [(n, t) for n, t in cols if n in want]
        missing = want - {n for n, _ in cols}
        if missing:
            raise SystemExit(f"{table}: columns not in schema: {sorted(missing)}")
    started = time.time()
    parquet = parquet_scan(path)
    probs = ", ".join(f"{i / N_BUCKETS:.6f}::FLOAT" for i in range(N_BUCKETS + 1))
    select_parts = ["count(*) AS n_rows"]
    aliases = []
    for name, dtype in cols:
        expr = (date_expr or {}).get(name) or _ident(name)
        out_dtype = "DATE" if name in (date_expr or {}) else dtype
        select_parts.append(f"approx_count_distinct({expr}) AS {name}__ndv")
        select_parts.append(
            f"count(*) FILTER (WHERE {expr} IS NULL) AS {name}__null")
        has_q = not any(t in out_dtype.upper() for t in SKIP_QUANTILE_TYPES)
        aliases.append((name, out_dtype, has_q))
        if has_q:
            select_parts.append(
                f"approx_quantile({expr}, [{probs}]) AS {name}__q")
    row = con.sql(f"SELECT {', '.join(select_parts)} FROM {parquet}").fetchone()
    n_rows = int(row[0])
    row = row[1:]
    out = {"n_rows": n_rows, "columns": {},
           "elapsed_s": round(time.time() - started, 2)}
    idx = 0
    for name, dtype, has_q in aliases:
        ndv = int(row[idx] or 0)
        n_null = int(row[idx + 1] or 0)
        idx += 2
        cdf = None
        if has_q:
            cdf = [_jsonable(x) for x in (row[idx] or [])]
            idx += 1
        out["columns"][name] = {
            "dtype": dtype,
            "ndv": ndv,
            "n_null": n_null,
            "null_frac": (n_null / n_rows) if n_rows else None,
            "quantile_buckets": N_BUCKETS if cdf is not None else 0,
            "cdf": cdf,
        }
        print(f"    {table}.{name:20s}  ndv={ndv:<12}", flush=True)
    print(f"    {table}  {out['elapsed_s']:.1f}s", flush=True)
    return out


def cdf_selectivity(cdf, lo=None, hi=None, inclusive_lo=True, inclusive_hi=False):
    """Fraction of the CDF in [lo, hi). cdf[i] is the value at rank i/N."""
    if not cdf:
        return 1.0
    n = len(cdf) - 1
    if n <= 0:
        return 1.0

    def rank_ge(value):
        # first i with cdf[i] >= value
        lo_i, hi_i = 0, n
        while lo_i < hi_i:
            mid = (lo_i + hi_i) // 2
            if cdf[mid] < value:
                lo_i = mid + 1
            else:
                hi_i = mid
        return lo_i

    def rank_gt(value):
        lo_i, hi_i = 0, n
        while lo_i < hi_i:
            mid = (lo_i + hi_i) // 2
            if cdf[mid] <= value:
                lo_i = mid + 1
            else:
                hi_i = mid
        return lo_i

    left = 0
    right = n
    if lo is not None:
        left = rank_ge(lo) if inclusive_lo else rank_gt(lo)
    if hi is not None:
        right = rank_ge(hi) if not inclusive_hi else rank_gt(hi)
    if right < left:
        return 0.0
    return max(0.0, min(1.0, (right - left) / n))


def shift_date_cdf(cdf, copies, days_per_copy):
    """Stitch equal-weight copies of a date CDF shifted by N days.

    ClickBench SF8 is 8 months of the same users; EventDate NDV and the
    quantile grid have to cover the shifted windows, not the source month.
    """
    from datetime import date, timedelta
    if not cdf or copies <= 1:
        return cdf
    expanded = []
    for i in range(copies):
        delta = timedelta(days=i * days_per_copy)
        for v in cdf:
            if v is None:
                continue
            d = v[:10] if isinstance(v, str) else str(v)
            expanded.append((date.fromisoformat(d) + delta).isoformat())
    expanded.sort()
    n = N_BUCKETS
    last = len(expanded) - 1
    return [expanded[int(round(i * last / n))] for i in range(n + 1)]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tables", nargs="*", default=TPCH_TABLES)
    ap.add_argument("--columns", nargs="*", default=None,
                    help="restrict the scan to these columns (all tables)")
    ap.add_argument("--eventdate-as-date", action="store_true",
                    help="treat EventDate as days since 1970-01-01 (ClickBench source)")
    ap.add_argument("--expand-copies", type=int, default=1)
    ap.add_argument("--expand-days", type=int, default=30)
    ap.add_argument("--expand-date-column", default="EventDate")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("PRAGMA threads=16")
    started = time.time()
    tables = {}
    date_expr = None
    if args.eventdate_as_date:
        date_expr = {
            "EventDate": "(DATE '1970-01-01' + CAST(\"EventDate\" AS INTEGER))",
        }
    for table in args.tables:
        path = os.path.join(args.source, table)
        if not os.path.exists(path):
            path = args.source  # single-file / single-table source
        print(f"# {table}", flush=True)
        rec = stats_for_table(con, table, path, args.columns, date_expr)
        try:
            _here = os.path.dirname(os.path.abspath(__file__))
            if _here not in sys.path:
                sys.path.insert(0, _here)
            from parse_footer import clustering_from_path
            cluster = clustering_from_path(path, args.columns)
            for name, cs in cluster.items():
                rec.setdefault("columns", {}).setdefault(name, {})
                rec["columns"][name]["rg_span"] = cs["rg_span"]
                rec["columns"][name]["n_rg_with_stats"] = cs["n_rg_with_stats"]
            if cluster:
                print(f"    {table} clustering  "
                      + ", ".join(f"{n}={cluster[n]['rg_span']:.3f}"
                                  for n in list(cluster)[:8]), flush=True)
        except Exception as exc:
            print(f"    {table} clustering skipped ({exc})", flush=True)
        if args.expand_copies > 1 and args.expand_date_column in rec["columns"]:
            col = rec["columns"][args.expand_date_column]
            if col.get("cdf"):
                col["cdf"] = shift_date_cdf(
                    col["cdf"], args.expand_copies, args.expand_days)
                col["ndv"] = int(col["ndv"] or 0) * args.expand_copies
                col["note"] = (
                    f"CDF/NDV expanded {args.expand_copies}× by "
                    f"{args.expand_days}d (ClickBench SF8)")
            rec["n_rows"] = int(rec["n_rows"]) * args.expand_copies
        tables[table] = rec

    result = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 2.2",
        "source": args.source,
        "n_buckets": N_BUCKETS,
        "elapsed_s": round(time.time() - started, 1),
        "tables": tables,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"\n# done  {result['elapsed_s']}s  {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
