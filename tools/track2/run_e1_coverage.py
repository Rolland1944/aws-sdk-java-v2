#!/usr/bin/env python3
"""M1 coverage gate: run one TPC-H query on S3 and measure GET attribution.

Contract (TRACK2_M0_CONTRACT.md 1.2 / TRACK2_PLAN.md step 2): >=95% of ranged-GET
bytes must map to query -> object -> row group/column chunk. Page-level coverage
is reported separately and is not claimed unless page indexes are present.

This is the real gate, not the synthetic correlate.py unit test. It:
  1. parses footers of the objects that will be scanned
  2. runs TPC-H Q6 (lineitem-only) through Spark/S3A with the interceptor
  3. collects the Spark event log (semantic layer)
  4. correlates the three layers and writes the coverage report

Q6 is the coverage vehicle because it is a single-table scan with pushed
predicates, so the query window is unambiguous and the GET mix is dominated by
column-chunk reads rather than joins. SF300 on S3 is used because that is the
dataset already uploaded; the mapping machinery does not depend on scale factor.

Usage:
  python3 tools/track2/run_e1_coverage.py
  python3 tools/track2/run_e1_coverage.py --parts 3   # first N lineitem parts
"""

import argparse
import configparser
import glob
import json
import os
import subprocess
import sys
import time

BUCKET = os.environ.get("S3ARR_BUCKET", "home-haoyue")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
PREFIX = os.environ.get("S3ARR_LINEITEM_PREFIX", "tpch300/lineitem")

TRACK2_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INTERCEPTOR_JAR = os.path.join(
    TRACK2_ROOT, "services-custom/s3-adaptive-range-reader/target",
    "aws-sdk-java-s3-adaptive-range-reader-2.25.70.jar")
S3A_JARS = os.path.expanduser("~/track2-env/s3a-jars")
BUNDLE_JAR = os.path.join(S3A_JARS, "bundle-2.29.52.jar")
HADOOP_AWS_JAR = os.path.join(S3A_JARS, "hadoop-aws-3.4.2.jar")
PY = os.path.join(TRACK2_ROOT, ".venv-track2/bin/python")
Q6_PATH = os.path.expanduser("~/track2-data/tpch_sf100/_tpch_queries.json")


def load_creds():
    path = os.path.expanduser("~/.aws/credentials")
    cp = configparser.ConfigParser()
    cp.read(path)
    section = "default" if "default" in cp else cp.sections()[0]
    return cp[section]["aws_access_key_id"], cp[section]["aws_secret_access_key"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parts", type=int, default=1,
                    help="how many lineitem parts to scan (default 1 = ~650MiB object)")
    ap.add_argument("--out-dir",
                    default=os.path.join(TRACK2_ROOT,
                                         "docs/adaptive-range-reader/results/track2/e1_coverage"))
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    ak, sk = load_creds()
    os.environ["AWS_ACCESS_KEY_ID"] = ak
    os.environ["AWS_SECRET_ACCESS_KEY"] = sk
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_REGION"] = REGION

    uris = [f"s3a://{BUCKET}/{PREFIX}/part-{i:04d}.parquet" for i in range(args.parts)]
    s3_uris = [u.replace("s3a://", "s3://") for u in uris]
    collector_dir = os.path.join(args.out_dir, "io")
    eventlog_dir = os.path.join(args.out_dir, "eventlogs")
    os.makedirs(collector_dir, exist_ok=True)
    os.makedirs(eventlog_dir, exist_ok=True)
    os.environ["TRACK2_COLLECTOR_DIR"] = collector_dir

    footer_out = os.path.join(args.out_dir, "footer.parquet")
    print("# E1 coverage: parse footers")
    for uri in s3_uris:
        print(f"  {uri}")
    rc = subprocess.call(
        [PY, os.path.join(TRACK2_ROOT, "tools/track2/parse_footer.py"),
         "--input", os.path.dirname(s3_uris[0]) if args.parts > 1 else s3_uris[0],
         "--limit", str(args.parts),
         "--format", "parquet", "--out", footer_out])
    if rc != 0:
        return rc

    with open(Q6_PATH) as fh:
        q6 = json.load(fh)["6"]

    cp = ":".join([BUNDLE_JAR, HADOOP_AWS_JAR, INTERCEPTOR_JAR])
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("track2-e1-coverage")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.extraClassPath", cp)
        .config("spark.executor.extraClassPath", cp)
        .config("spark.driver.extraJavaOptions", f"-Dtrack2.collector.dir={collector_dir}")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", eventlog_dir)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.audit.enabled", "true")
        .config("spark.hadoop.fs.s3a.audit.referrer.enabled", "true")
        .config("spark.hadoop.fs.s3a.audit.execution.interceptors",
                "software.amazon.awssdk.s3.adaptive.telemetry.Track2IoCollectorInterceptor")
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "true")
        .config("spark.hadoop.fs.s3a.input.fadvise", "random")
        .config("spark.sql.parquet.filterPushdown", "true")
        .getOrCreate()
    )
    print(f"# E1 coverage: run Q6 on {len(uris)} part(s)")
    t0 = time.time()
    df = spark.read.parquet(*uris)
    df.createOrReplaceTempView("lineitem")
    result = spark.sql(q6).collect()
    elapsed = time.time() - t0
    print(f"  q6 result   {result}")
    print(f"  elapsed     {elapsed:.1f}s")
    spark.stop()

    semantic_out = os.path.join(args.out_dir, "semantic.json")
    print("# E1 coverage: collect semantic")
    rc = subprocess.call(
        [PY, os.path.join(TRACK2_ROOT, "tools/track2/collect_semantic.py"),
         "--eventlog", eventlog_dir, "--out", semantic_out])
    if rc != 0:
        return rc

    ndjson = glob.glob(os.path.join(collector_dir, "track2-io-*.ndjson"))
    if not ndjson:
        print("FAIL: no interceptor NDJSON")
        return 1

    report_out = os.path.join(args.out_dir, "coverage.json")
    bundle_out = os.path.join(args.out_dir, "observation_bundle.parquet")
    print("# E1 coverage: correlate")
    return subprocess.call(
        [PY, os.path.join(TRACK2_ROOT, "tools/track2/correlate.py"),
         "--io", *ndjson,
         "--footer", footer_out,
         "--semantic", semantic_out,
         "--out", bundle_out,
         "--report", report_out])


if __name__ == "__main__":
    sys.exit(main())
