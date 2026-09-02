#!/usr/bin/env python3
"""First layer, LLM variant: the control that has to beat the rules.

*** Do not run this before the deterministic generator has been shown to work. ***

The order is a claim about evidence, not a preference. If an LLM plan is
measured first and it helps, nothing has been learned: the gain could be
entirely reproducible by the rules in plan_deterministic.py, and the paper
would be arguing for a model that a hundred lines of seriation replaces. So the
necessary conditions, checked by `--check-preconditions` and printed by every
run, are:

  1. the deterministic plan has passed E-0 (wall clock down on ClickBench SF1);
  2. the deterministic plan shows stable gains on the E-B ablations;
  3. this generator beats it, on L1 or measured, by finding something the rules
     do not encode -- a column cluster, a codec pairing -- or by making a
     search tractable that the rules cannot finish on a very wide schema.

Failing (3), the honest result is that the LLM is unnecessary, and that is a
publishable finding rather than a failure.

The prompt gets exactly what the deterministic generator gets: the access
profile summary and the footer summary. No query text, no schema semantics
beyond column names, no measured timings. Anything more and the comparison is
not between two planners over the same evidence.

Output is validated, not trusted. A plan that fails schema or L0 checks is fed
back with the violations appended and retried up to --max-retries; the
transcript of every attempt is written next to the plan so that "the model
needed four tries to emit a legal permutation" is a reportable number rather
than a detail lost in a retry loop.

Usage:
  export TRACK2_LLM_API_KEY=...
  python3 tools/track2/plan_llm.py \
      --dataset-snapshot .../dataset_snapshot.json \
      --access-profile .../access_profile.json \
      --model gpt-4o --out plans/llm-001.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import layout_actions as la  # noqa: E402
import whatif  # noqa: E402

DEFAULT_MAX_RETRIES = 3
# Columns described in the prompt. A 105-column table fits; a 2000-column one
# would not, and truncating by weight is the only defensible cut.
DEFAULT_TOP_COLUMNS = 150
DEFAULT_TOP_PAIRS = 120


SYSTEM_PROMPT = """\
You are a Parquet layout planner. You receive evidence gathered from object
storage I/O traces and Parquet footers, and you emit a layout plan as JSON.

You do NOT receive query text, query plans or predicates, and you must not
assume any. Every decision must be justifiable from the evidence given.

Emit ONLY a JSON object, no prose and no code fences, of the form:

{"plan_id": "...", "actions": [{"canonical": "...", "value": ..., "table": "..."}],
 "rationale": ["one short sentence per action group"]}

The action vocabulary is closed. These are the only legal canonical names:

  write.parquet.column-order            value: full list of column names
  write.parquet.row-group-size-bytes    value: integer bytes
  write.target-file-size-bytes          value: integer bytes
  write.parquet.compression-codec       value: one of uncompressed|snappy|gzip|zstd|lz4
  write.parquet.page-size-bytes         value: integer bytes
  write.parquet.page-row-limit          value: positive integer
  write.parquet.compression-codec.column.<COLUMN>   value: codec
  write.parquet.encoding.column.<COLUMN>            value: encoding
  write.parquet.dict-encoding-enabled.column.<COLUMN>  value: true|false

Hard constraints, all checked; a violation is rejected and returned to you:

  * A column order must be a PERMUTATION of the schema. Every column appears
    exactly once. Dropping a column is not a layout action.
  * page-size-bytes <= row-group-size-bytes <= target-file-size-bytes.
  * row-group-size-bytes <= 134217728 (the reader times out above this).
  * An encoding must match the column's physical type:
      DELTA_BINARY_PACKED   INT32, INT64
      DELTA_BYTE_ARRAY      BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY
      DELTA_LENGTH_BYTE_ARRAY  BYTE_ARRAY
      BYTE_STREAM_SPLIT     FLOAT, DOUBLE
      PLAIN, RLE_DICTIONARY any type
  * Do not emit sort.columns or partition.spec. They are not layout actions.
  * Page index is always on and is not yours to set.

What the layout can buy, so you know what to optimise:

  * Columns read in the same access episode should be ADJACENT in the column
    order, because the reader merges byte ranges that are close together and
    pays one round trip instead of two. Columns never read should be at the
    end, so the hot prefix is one contiguous span.
  * Fewer, larger requests win when round-trip time dominates; smaller pages
    win when the reader is already sub-dividing column chunks.
"""


def evidence_packet(catalog, top_columns=DEFAULT_TOP_COLUMNS,
                    top_pairs=DEFAULT_TOP_PAIRS, probe=None):
    """Exactly the evidence the deterministic generator reads, serialised.

    Keeping the two inputs identical is what makes E-E a comparison of planners
    rather than a comparison of context windows.
    """
    tables = {}
    for table in (catalog.tables_observed() or [catalog.largest_table()]):
        geom = catalog.BASELINE_GEOMETRY.get(table) or {}
        weights = catalog.COLUMN_WEIGHT.get(table) or {}
        stats = ((catalog.COLUMN_STATS.get(table) or {}).get("columns") or {})
        schema = catalog.ALL_COLUMNS.get(table) or []
        hot = list(weights)[:top_columns]

        columns = []
        for name in schema:
            rec = weights.get(name) or {}
            stat = stats.get(name) or {}
            entry = {
                "name": name,
                "physical_type": stat.get("physical_type"),
                "read_in_pct_of_episodes": round((rec.get("episode_share") or 0) * 100, 1),
                "byte_share_pct": round((rec.get("byte_share") or 0) * 100, 2),
            }
            if stat.get("ndv") is not None:
                entry["ndv"] = stat["ndv"]
            if probe:
                pr = (((probe.get(table) or {}).get("columns") or {}).get(name) or {})
                if pr.get("best_codec"):
                    entry["best_codec_measured"] = pr["best_codec"]
                    entry["best_ratio_measured"] = pr.get("best_ratio")
            columns.append(entry)

        matrix = catalog.coaccess_matrix(table)
        seen, pairs = set(), []
        for (a, b), w in sorted(matrix.items(), key=lambda kv: -kv[1]):
            key = tuple(sorted((a, b)))
            if key in seen:
                continue
            seen.add(key)
            pairs.append({"columns": list(key), "episodes_together": w})
            if len(pairs) >= top_pairs:
                break

        tables[table] = {
            "current_geometry": {
                "files": geom.get("files"),
                "row_groups": geom.get("n_rg"),
                "row_group_bytes_mean": geom.get("rg_bytes"),
                "compressed_bytes": geom.get("compressed_bytes"),
            },
            "current_column_order": schema,
            "columns": columns,
            "top_coaccess_pairs": pairs,
            "n_columns_read": len(hot),
            "n_columns_never_read": len(schema) - len(weights),
        }

    return {
        "evidence_sources": ["object storage SDK byte ranges", "parquet footers"],
        "n_access_episodes": catalog.profile.n_episodes,
        "request_shape": catalog.REQUEST_SHAPE,
        "access_patterns": [
            {"columns": p["columns"], "episodes": p["n_episodes"],
             "row_groups_per_episode": p.get("rg_per_episode")}
            for p in catalog.PATTERNS[:40]
        ],
        "tables": tables,
    }


def call_llm(messages, model, api_key=None, base_url=None, temperature=0.2):
    """One chat completion. OpenAI-compatible; any provider exposing that API."""
    try:
        from openai import OpenAI
    except ModuleNotFoundError:
        raise SystemExit(
            "the openai package is not installed. `pip install openai`, or "
            "point --base-url at any OpenAI-compatible endpoint.")
    client = OpenAI(api_key=api_key or os.environ.get("TRACK2_LLM_API_KEY"),
                    base_url=base_url or os.environ.get("TRACK2_LLM_BASE_URL"))
    response = client.chat.completions.create(
        model=model, messages=messages, temperature=temperature,
        response_format={"type": "json_object"})
    return response.choices[0].message.content


def parse_plan(text):
    """Tolerate a fenced block; refuse anything else."""
    body = text.strip()
    if body.startswith("```"):
        body = body.split("```")[1]
        if body.startswith("json"):
            body = body[4:]
    return json.loads(body)


def generate(catalog, model, packet, max_retries=DEFAULT_MAX_RETRIES,
             api_key=None, base_url=None, temperature=0.2):
    """Ask, validate, feed violations back, repeat. Returns (plan, transcript)."""
    table = catalog.largest_table()
    schema = catalog.ALL_COLUMNS.get(table)
    ptypes = whatif.physical_types(table)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(packet, indent=1)},
    ]
    transcript = []

    for attempt in range(1, max_retries + 1):
        raw = call_llm(messages, model, api_key, base_url, temperature)
        record = {"attempt": attempt, "raw": raw}
        try:
            plan = parse_plan(raw)
        except (json.JSONDecodeError, IndexError) as exc:
            record["problems"] = [f"response is not JSON: {exc}"]
            transcript.append(record)
            messages += [
                {"role": "assistant", "content": raw},
                {"role": "user", "content": f"That was not valid JSON: {exc}. "
                                            f"Emit only the JSON object."},
            ]
            continue

        problems = la.validate_plan(plan, schema=schema, physical_types=ptypes,
                                    writer="pyarrow", table=table)
        record["problems"] = problems
        record["n_actions"] = len(plan.get("actions") or [])
        transcript.append(record)
        if not problems:
            return plan, transcript

        messages += [
            {"role": "assistant", "content": raw},
            {"role": "user", "content":
                "The plan was rejected. Fix every problem and re-emit the whole "
                "plan as JSON:\n" + "\n".join(f"- {p}" for p in problems)},
        ]

    return None, transcript


def preconditions(e0_report=None, ablation_report=None):
    """Whether the deterministic path has earned an LLM comparison yet."""
    checks = []

    passed_e0 = None
    if e0_report and os.path.exists(e0_report):
        with open(e0_report) as fh:
            passed_e0 = bool(json.load(fh).get("pass"))
    checks.append({
        "condition": "E-0 passed: deterministic plan reduced ClickBench SF1 wall clock",
        "satisfied": passed_e0,
        "evidence": e0_report or "not supplied",
    })

    ablation_ok = None
    if ablation_report and os.path.exists(ablation_report):
        with open(ablation_report) as fh:
            doc = json.load(fh)
        ranked = doc.get("ranked") or []
        ablation_ok = any((r.get("improve_frac") or 0) > 0 for r in ranked)
    checks.append({
        "condition": "E-B: deterministic plan shows stable gains under ablation",
        "satisfied": ablation_ok,
        "evidence": ablation_report or "not supplied",
    })

    checks.append({
        "condition": "E-E: the LLM plan must beat the deterministic plan; running "
                     "it earlier cannot establish that the model was needed",
        "satisfied": None,
        "evidence": "decided after this run, by comparing the two plans",
    })
    return checks


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--api-key", default=None, help="default $TRACK2_LLM_API_KEY")
    ap.add_argument("--base-url", default=None, help="default $TRACK2_LLM_BASE_URL")
    ap.add_argument("--temperature", type=float, default=0.2)
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    ap.add_argument("--compression-probe", default=None)
    ap.add_argument("--top-columns", type=int, default=DEFAULT_TOP_COLUMNS)
    ap.add_argument("--top-pairs", type=int, default=DEFAULT_TOP_PAIRS)
    ap.add_argument("--e0-report", default=None,
                    help="run_e0_smoke.py report, for the precondition check")
    ap.add_argument("--ablation-report", default=None,
                    help="whatif search report over the deterministic ablations")
    ap.add_argument("--check-preconditions", action="store_true",
                    help="report whether the LLM comparison is warranted, then exit")
    ap.add_argument("--dump-packet", default=None,
                    help="write the evidence packet and exit without calling any model")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    checks = preconditions(args.e0_report, args.ablation_report)
    print("# preconditions for running the LLM generator")
    for check in checks:
        mark = {True: "yes", False: "NO", None: "unknown"}[check["satisfied"]]
        print(f"  [{mark:>7}] {check['condition']}")
    if args.check_preconditions:
        return 0 if all(c["satisfied"] is not False for c in checks) else 1
    if any(c["satisfied"] is False for c in checks):
        print("\nA precondition is unsatisfied. Running anyway produces a number "
              "that cannot show the model was necessary.")

    whatif.bind_catalog(advisor_catalog.from_args(args))
    catalog = whatif.catalog
    probe = None
    if args.compression_probe and os.path.exists(args.compression_probe):
        with open(args.compression_probe) as fh:
            probe = json.load(fh).get("tables")

    packet = evidence_packet(catalog, args.top_columns, args.top_pairs, probe)
    if args.dump_packet:
        with open(args.dump_packet, "w") as fh:
            json.dump(packet, fh, indent=2)
        print(f"\nevidence packet -> {args.dump_packet} (no model called)")
        return 0

    plan, transcript = generate(catalog, args.model, packet, args.max_retries,
                                args.api_key, args.base_url, args.temperature)

    print(f"\n# LLM plan ({args.model}), {len(transcript)} attempt(s)")
    for record in transcript:
        problems = record.get("problems") or []
        status = "accepted" if not problems else f"{len(problems)} violation(s)"
        print(f"  attempt {record['attempt']}: {status}")
        for problem in problems[:3]:
            print(f"      - {problem}")

    if plan is None:
        print(f"\nno legal plan after {args.max_retries} attempts")
        if args.out:
            with open(args.out + ".transcript.json", "w") as fh:
                json.dump(transcript, fh, indent=2)
        return 1

    plan.setdefault("plan_id", f"llm-{args.model}")
    plan["generator"] = "llm"
    plan["generated_at"] = datetime.now(timezone.utc).isoformat()
    plan["model"] = args.model
    plan["evidence"] = {"source": ["sdk_io", "parquet_footer"],
                        "episodes": catalog.profile.n_episodes,
                        "packet_identical_to": "plan_deterministic"}
    plan["attempts"] = len(transcript)
    plan.setdefault("constraints", {
        "format": "parquet", "page_index": "required_on",
        "readable_by": ["parquet-mr", "pyarrow"]})
    plan["preconditions"] = checks

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(plan, fh, indent=2)
        with open(args.out + ".transcript.json", "w") as fh:
            json.dump(transcript, fh, indent=2)
        print(f"  wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
