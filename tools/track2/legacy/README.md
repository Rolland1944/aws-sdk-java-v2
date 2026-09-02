# Retired Track 2 v1 modules

These modules implemented the **Semantic** telemetry layer and the sort/partition
action space. Both were removed from the advisor in contract revision **r5**
(see [TRACK2_M0_CONTRACT.md](../../../docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md) §0.1
and [TRACK2_V2_PLAN.md](../../../docs/adaptive-range-reader/TRACK2_V2_PLAN.md)).

They are kept, not deleted, because the r1–r4 results were produced with them and
those results stand as historical comparisons. Nothing in the live pipeline imports
from this directory.

| Module | What it did | Why it was retired |
| --- | --- | --- |
| `collect_semantic.py` | Parsed Spark event logs into per-execution scan fragments (predicates, projections, time windows) | The advisor no longer reads query plans; `correlate.py` joins bytes to chunks geometrically and groups by access episode |
| `predicates_from_runtime.py` | Turned pushed-down filters into sort/partition key candidates | `sort.columns` and `partition.spec` left the action space |
| `workload_snapshot.py` | `QUERIES`: scans per query, plus measured per-query medians | Replaced by `access_profile.py`, whose unit is an access pattern (table + column set) rather than a query |
| `hand_catalog_tpch.py`, `hand_catalog_clickbench.py` | Hand-transcribed scan catalogues, kept for validating the event-log parser | Validate a parser that is no longer in the pipeline |
| `analyze_layout.py` | Enumerated the sort × file-size × partition candidate grid into `candidates_per_table.json` | Candidate generation moved to `plan_deterministic.py`, whose action space has no sort or partition axis to enumerate |

To reproduce a v1 result, check out a commit from before r5; these files alone are
not sufficient because `whatif.py` and `virtual_footer.py` no longer carry the
pruning model they fed.
