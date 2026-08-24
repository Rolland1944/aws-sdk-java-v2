#!/usr/bin/env python3
"""Retired hand-transcribed TPC-H scan catalogue. Evidence, not an input.

This module used to *be* the advisor's view of TPC-H: the scan catalogue, the
baseline geometry, the column order and shares, synthetic CDFs, and the file /
row-group / sort / partition grids. All of that is now measured --
`dataset_snapshot.py` reads the footers and the object listing,
`workload_snapshot.py` reads the event logs, `adaptive_physical_options.py`
derives the action space from the geometry, and `advisor_policy.py` holds the
thresholds. Nothing imports this module to make a decision.

What is left is the 22 queries as one person read them out of the frozen
DuckDB query text, kept for exactly one job: `workload_snapshot.py --compare
tpch` checks the runtime-derived catalogue against it. That cross-check is
what showed the transcription was both incomplete and wrong in ways nobody
could have noticed by rereading it --

  * missing scans. The plan opens partsupp, supplier, nation and region twice
    in Q2, lineitem three times in Q18, customer twice in Q22: correlated
    subqueries rescan, and the transcription counted each table once. L1 was
    under-pricing every one of those queries.
  * missing predicates. Q19 pushes p_brand, p_container, p_size and
    l_quantity down; the transcription has none of them.

It is frozen at that state on purpose. Do not add to it.
"""

from __future__ import annotations

# date literals as ISO strings so they compare against the CDF (also ISO).
D = lambda s: s


def P(column, op, value, value2=None):
    """Predicate. op: ge, gt, le, lt, eq, between, in, like, ne."""
    rec = {"column": column, "op": op, "value": value}
    if value2 is not None:
        rec["value2"] = value2
    return rec


def S(table, columns, predicates=None):
    return {"table": table, "columns": list(columns), "predicates": list(predicates or [])}


# Full projections (what Spark actually reads after pushdown). Conservative:
# include columns used in SELECT, WHERE, JOIN, GROUP BY on that scan.
QUERIES = {
    1: [S("lineitem",
          ["l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice",
           "l_discount", "l_tax", "l_shipdate"],
          [P("l_shipdate", "le", D("1998-09-02"))])],
    2: [
        S("part", ["p_partkey", "p_mfgr", "p_size", "p_type"],
          [P("p_size", "eq", 15), P("p_type", "like", "%BRASS")]),
        S("supplier", ["s_suppkey", "s_acctbal", "s_name", "s_address", "s_phone",
                       "s_comment", "s_nationkey"]),
        S("partsupp", ["ps_partkey", "ps_suppkey", "ps_supplycost"]),
        S("nation", ["n_nationkey", "n_name", "n_regionkey"]),
        S("region", ["r_regionkey", "r_name"], [P("r_name", "eq", "EUROPE")]),
    ],
    3: [
        S("customer", ["c_custkey", "c_mktsegment"],
          [P("c_mktsegment", "eq", "BUILDING")]),
        S("orders", ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
          [P("o_orderdate", "lt", D("1995-03-15"))]),
        S("lineitem", ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
          [P("l_shipdate", "gt", D("1995-03-15"))]),
    ],
    4: [
        S("orders", ["o_orderkey", "o_orderpriority", "o_orderdate"],
          [P("o_orderdate", "ge", D("1993-07-01")), P("o_orderdate", "lt", D("1993-10-01"))]),
        S("lineitem", ["l_orderkey", "l_commitdate", "l_receiptdate"]),
    ],
    5: [
        S("customer", ["c_custkey", "c_nationkey"]),
        S("orders", ["o_orderkey", "o_custkey", "o_orderdate"],
          [P("o_orderdate", "ge", D("1994-01-01")), P("o_orderdate", "lt", D("1995-01-01"))]),
        S("lineitem", ["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"]),
        S("supplier", ["s_suppkey", "s_nationkey"]),
        S("nation", ["n_nationkey", "n_name", "n_regionkey"]),
        S("region", ["r_regionkey", "r_name"], [P("r_name", "eq", "ASIA")]),
    ],
    6: [S("lineitem",
          ["l_extendedprice", "l_discount", "l_shipdate", "l_quantity"],
          [P("l_shipdate", "ge", D("1994-01-01")), P("l_shipdate", "lt", D("1995-01-01")),
           P("l_discount", "between", 0.05, 0.07), P("l_quantity", "lt", 24)])],
    7: [
        S("supplier", ["s_suppkey", "s_nationkey"]),
        S("lineitem", ["l_suppkey", "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
          [P("l_shipdate", "ge", D("1995-01-01")), P("l_shipdate", "le", D("1996-12-31"))]),
        S("orders", ["o_orderkey", "o_custkey"]),
        S("customer", ["c_custkey", "c_nationkey"]),
        S("nation", ["n_nationkey", "n_name"]),
    ],
    8: [
        S("part", ["p_partkey", "p_type"], [P("p_type", "eq", "ECONOMY ANODIZED STEEL")]),
        S("supplier", ["s_suppkey", "s_nationkey"]),
        S("lineitem", ["l_partkey", "l_suppkey", "l_orderkey", "l_extendedprice", "l_discount"]),
        S("orders", ["o_orderkey", "o_custkey", "o_orderdate"],
          [P("o_orderdate", "ge", D("1995-01-01")), P("o_orderdate", "le", D("1996-12-31"))]),
        S("customer", ["c_custkey", "c_nationkey"]),
        S("nation", ["n_nationkey", "n_name", "n_regionkey"]),
        S("region", ["r_regionkey", "r_name"], [P("r_name", "eq", "AMERICA")]),
    ],
    9: [
        S("part", ["p_partkey", "p_name"], [P("p_name", "like", "%green%")]),
        S("supplier", ["s_suppkey", "s_nationkey"]),
        S("lineitem", ["l_suppkey", "l_partkey", "l_orderkey", "l_extendedprice",
                       "l_discount", "l_quantity"]),
        S("partsupp", ["ps_suppkey", "ps_partkey", "ps_supplycost"]),
        S("orders", ["o_orderkey", "o_orderdate"]),
        S("nation", ["n_nationkey", "n_name"]),
    ],
    10: [
        S("customer", ["c_custkey", "c_name", "c_acctbal", "c_phone", "c_address",
                       "c_comment", "c_nationkey"]),
        S("orders", ["o_orderkey", "o_custkey", "o_orderdate"],
          [P("o_orderdate", "ge", D("1993-10-01")), P("o_orderdate", "lt", D("1994-01-01"))]),
        S("lineitem", ["l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"],
          [P("l_returnflag", "eq", "R")]),
        S("nation", ["n_nationkey", "n_name"]),
    ],
    11: [
        S("partsupp", ["ps_partkey", "ps_suppkey", "ps_supplycost", "ps_availqty"]),
        S("supplier", ["s_suppkey", "s_nationkey"]),
        S("nation", ["n_nationkey", "n_name"], [P("n_name", "eq", "GERMANY")]),
    ],
    12: [
        S("orders", ["o_orderkey", "o_orderpriority"]),
        S("lineitem", ["l_orderkey", "l_shipmode", "l_commitdate", "l_receiptdate", "l_shipdate"],
          [P("l_shipmode", "in", ["MAIL", "SHIP"]),
           P("l_receiptdate", "ge", D("1994-01-01")),
           P("l_receiptdate", "lt", D("1995-01-01"))]),
    ],
    13: [
        S("customer", ["c_custkey"]),
        S("orders", ["o_orderkey", "o_custkey", "o_comment"]),
    ],
    14: [
        S("lineitem", ["l_partkey", "l_extendedprice", "l_discount", "l_shipdate"],
          [P("l_shipdate", "ge", D("1995-09-01")), P("l_shipdate", "lt", D("1995-10-01"))]),
        S("part", ["p_partkey", "p_type"]),
    ],
    15: [
        S("lineitem", ["l_suppkey", "l_extendedprice", "l_discount", "l_shipdate"],
          [P("l_shipdate", "ge", D("1996-01-01")), P("l_shipdate", "lt", D("1996-04-01"))]),
        S("supplier", ["s_suppkey", "s_name", "s_address", "s_phone"]),
    ],
    16: [
        S("partsupp", ["ps_partkey", "ps_suppkey"]),
        S("part", ["p_partkey", "p_brand", "p_type", "p_size"],
          [P("p_brand", "ne", "Brand#45")]),
        S("supplier", ["s_suppkey", "s_comment"]),
    ],
    17: [
        S("lineitem", ["l_partkey", "l_extendedprice", "l_quantity"]),
        S("lineitem", ["l_partkey", "l_quantity"]),  # subquery avg
        S("part", ["p_partkey", "p_brand", "p_container"],
          [P("p_brand", "eq", "Brand#23"), P("p_container", "eq", "MED BOX")]),
    ],
    18: [
        S("lineitem", ["l_orderkey", "l_quantity"]),  # HAVING sum(qty)>300
        S("lineitem", ["l_orderkey", "l_quantity"]),
        S("customer", ["c_custkey", "c_name"]),
        S("orders", ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"]),
    ],
    19: [
        S("lineitem", ["l_partkey", "l_extendedprice", "l_discount", "l_quantity",
                       "l_shipmode", "l_shipinstruct"],
          [P("l_shipmode", "in", ["AIR", "AIR REG"]),
           P("l_shipinstruct", "eq", "DELIVER IN PERSON")]),
        S("part", ["p_partkey", "p_brand", "p_container", "p_size"]),
    ],
    20: [
        S("supplier", ["s_suppkey", "s_name", "s_address", "s_nationkey"]),
        S("nation", ["n_nationkey", "n_name"], [P("n_name", "eq", "CANADA")]),
        S("partsupp", ["ps_partkey", "ps_suppkey", "ps_availqty"]),
        S("part", ["p_partkey", "p_name"], [P("p_name", "like", "forest%")]),
        S("lineitem", ["l_partkey", "l_suppkey", "l_quantity", "l_shipdate"],
          [P("l_shipdate", "ge", D("1994-01-01")), P("l_shipdate", "lt", D("1995-01-01"))]),
    ],
    21: [
        S("supplier", ["s_suppkey", "s_name", "s_nationkey"]),
        S("lineitem", ["l_suppkey", "l_orderkey", "l_receiptdate", "l_commitdate"]),  # l1
        S("lineitem", ["l_orderkey", "l_suppkey"]),  # EXISTS l2
        S("lineitem", ["l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"]),  # NOT EXISTS l3
        S("orders", ["o_orderkey", "o_orderstatus"], [P("o_orderstatus", "eq", "F")]),
        S("nation", ["n_nationkey", "n_name"], [P("n_name", "eq", "SAUDI ARABIA")]),
    ],
    22: [
        S("customer", ["c_custkey", "c_phone", "c_acctbal"]),
        S("orders", ["o_custkey"]),
    ],
}
