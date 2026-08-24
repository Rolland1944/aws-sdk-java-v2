#!/usr/bin/env python3
"""E0 real-S3 smoke: load Track2IoCollectorInterceptor through S3A and assert records.

Reads one Parquet object via Spark/S3A with the interceptor on
fs.s3a.audit.execution.interceptors. Passes if the collector wrote at least one
ranged GET with range_offset/range_length and an audit_op from the Referer
header (Hadoop logging auditor). Vectored-IO merging is reported, not gated:
DuckDB-written source files may not produce mergeable adjacent column chunks.

Credentials come from ~/.aws/credentials (never printed).
"""

import configparser
import glob
import json
import os
import sys
import tempfile


BUCKET = os.environ.get("S3ARR_BUCKET", "home-haoyue")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
KEY = os.environ.get("S3ARR_SMOKE_KEY", "tpch300/lineitem/part-0000.parquet")

TRACK2_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INTERCEPTOR_JAR = os.path.join(
    TRACK2_ROOT,
    "services-custom/s3-adaptive-range-reader/target",
    "aws-sdk-java-s3-adaptive-range-reader-2.25.70.jar",
)
S3A_JARS = os.path.expanduser("~/track2-env/s3a-jars")
BUNDLE_JAR = os.path.join(S3A_JARS, "bundle-2.29.52.jar")
HADOOP_AWS_JAR = os.path.join(S3A_JARS, "hadoop-aws-3.4.2.jar")


def load_creds():
    path = os.path.expanduser("~/.aws/credentials")
    if not os.path.exists(path):
        sys.exit("missing ~/.aws/credentials")
    cp = configparser.ConfigParser()
    cp.read(path)
    section = "default" if "default" in cp else cp.sections()[0]
    ak = cp[section].get("aws_access_key_id")
    sk = cp[section].get("aws_secret_access_key")
    if not ak or not sk:
        sys.exit("credentials file has no aws_access_key_id / aws_secret_access_key")
    return ak, sk


def main():
    for p in (INTERCEPTOR_JAR, BUNDLE_JAR, HADOOP_AWS_JAR):
        if not os.path.exists(p):
            sys.exit(f"missing jar: {p}")

    ak, sk = load_creds()
    os.environ["AWS_ACCESS_KEY_ID"] = ak
    os.environ["AWS_SECRET_ACCESS_KEY"] = sk
    os.environ["AWS_DEFAULT_REGION"] = REGION

    out_dir = os.environ.get("TRACK2_COLLECTOR_DIR") or tempfile.mkdtemp(prefix="track2-io-")
    os.makedirs(out_dir, exist_ok=True)
    os.environ["TRACK2_COLLECTOR_DIR"] = out_dir

    cp = ":".join([BUNDLE_JAR, HADOOP_AWS_JAR, INTERCEPTOR_JAR])
    uri = f"s3a://{BUCKET}/{KEY}"

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("track2-e0-s3a-smoke")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.extraClassPath", cp)
        .config("spark.executor.extraClassPath", cp)
        .config("spark.driver.extraJavaOptions", f"-Dtrack2.collector.dir={out_dir}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.audit.enabled", "true")
        .config("spark.hadoop.fs.s3a.audit.referrer.enabled", "true")
        .config(
            "spark.hadoop.fs.s3a.audit.execution.interceptors",
            "software.amazon.awssdk.s3.adaptive.telemetry.Track2IoCollectorInterceptor",
        )
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "true")
        .config("spark.hadoop.fs.s3a.input.fadvise", "random")
        .getOrCreate()
    )

    print(f"# E0 S3A interceptor smoke")
    print(f"  spark     {spark.version}")
    print(f"  hadoop    {spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")
    print(f"  uri       s3a://{BUCKET}/{KEY}")
    print(f"  collector {out_dir}")

    df = spark.read.parquet(uri)
    rows = df.select("l_orderkey", "l_shipdate").limit(5).collect()
    print(f"  rows      {len(rows)}")
    spark.stop()

    ndjson = glob.glob(os.path.join(out_dir, "track2-io-*.ndjson"))
    if not ndjson:
        print("FAIL: interceptor wrote no NDJSON (class not on S3A chain, or collector dir wrong)")
        return 1

    records = []
    for path in ndjson:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    ranged = [r for r in records if r.get("range_offset") is not None]
    with_audit = [r for r in records if r.get("audit_op")]
    gets = [r for r in records if r.get("method") == "GET"]
    print(f"  ndjson    {ndjson[0]}")
    print(f"  records   {len(records)}  GET={len(gets)}  ranged={len(ranged)}  with_audit_op={len(with_audit)}")
    if ranged:
        sample = ranged[0]
        print(f"  sample    method={sample.get('method')} status={sample.get('http_status')} "
              f"offset={sample.get('range_offset')} length={sample.get('range_length')} "
              f"audit_op={sample.get('audit_op')}")
    elif records:
        sample = records[0]
        print(f"  sample    method={sample.get('method')} status={sample.get('http_status')} "
              f"audit_op={sample.get('audit_op')}")

    # vectored IO: a GET whose range covers more than one typical column-chunk
    # is reported, not gated -- see TRACK2_M0_CONTRACT.md O-2
    long_gets = [r for r in ranged if (r.get("range_length") or 0) > 2 * 1024 * 1024]
    print(f"  long GETs (>2MiB, possible merge) {len(long_gets)}")

    ok = True
    if not ranged:
        print("FAIL: no ranged GET recorded")
        ok = False
    if not with_audit:
        print("FAIL: no audit_op (Referer header not visible to interceptor)")
        ok = False
    if ok:
        print("PASS: interceptor loaded, ranged GETs recorded, audit header visible")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
