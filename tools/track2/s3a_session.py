#!/usr/bin/env python3
"""Shared Spark + S3A session wiring for Track 2 (frozen reader config, contract 1.4)."""

import configparser
import os

BUCKET = os.environ.get("S3ARR_BUCKET", "home-haoyue")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")

TRACK2_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
INTERCEPTOR_JAR = os.path.join(
    TRACK2_ROOT, "services-custom/s3-adaptive-range-reader/target",
    "aws-sdk-java-s3-adaptive-range-reader-2.25.70.jar")
S3A_JARS = os.path.expanduser("~/track2-env/s3a-jars")
BUNDLE_JAR = os.path.join(S3A_JARS, "bundle-2.29.52.jar")
HADOOP_AWS_JAR = os.path.join(S3A_JARS, "hadoop-aws-3.4.2.jar")


def load_creds():
    path = os.path.expanduser("~/.aws/credentials")
    if not os.path.exists(path):
        raise FileNotFoundError("missing ~/.aws/credentials")
    cp = configparser.ConfigParser()
    cp.read(path)
    section = "default" if "default" in cp else cp.sections()[0]
    ak = cp[section].get("aws_access_key_id")
    sk = cp[section].get("aws_secret_access_key")
    if not ak or not sk:
        raise ValueError("credentials file missing key/secret")
    return ak, sk


def extra_classpath():
    return ":".join([BUNDLE_JAR, HADOOP_AWS_JAR, INTERCEPTOR_JAR])


def spark_scratch():
    """Shuffle/spill directory.

    This VM's /tmp lives on a ~100 GiB root disk. SF100 Q8/Q9 joins spill more
    than that, which aborted the first E2 attempt with ENOSPC. /data has ~450 GiB.
    Not a Reader freeze item — just where the JVM is allowed to write scratch.
    """
    path = os.environ.get("SPARK_LOCAL_DIRS", os.path.expanduser("~/track2-scratch"))
    os.makedirs(path, exist_ok=True)
    os.environ["SPARK_LOCAL_DIRS"] = path
    return path


def apply_frozen_reader(builder, ak, sk, collector_dir=None, eventlog_dir=None,
                        interceptor=True):
    """Apply TRACK2_M0_CONTRACT.md 1.4 frozen reader settings."""
    java_opts = []
    if collector_dir:
        os.makedirs(collector_dir, exist_ok=True)
        os.environ["TRACK2_COLLECTOR_DIR"] = collector_dir
        java_opts.append(f"-Dtrack2.collector.dir={collector_dir}")
    builder = (
        builder
        .config("spark.driver.extraClassPath", extra_classpath())
        .config("spark.executor.extraClassPath", extra_classpath())
        .config("spark.local.dir", spark_scratch())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.input.fadvise", "random")
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "true")
        # parquet-hadoop 1.16 hardcodes HADOOP_VECTORED_READ_TIMEOUT_SECONDS=300;
        # there is no Spark/Hadoop key to raise it. Layouts whose column chunks
        # hang past 300s are rejected in whatif.l0_check (MAX_READABLE_RG_BYTES).
        .config("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "131072")
        .config("spark.hadoop.fs.s3a.vectored.read.max.merged.size", "2097152")
        .config("spark.hadoop.fs.s3a.vectored.active.ranged.reads", "4")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.hadoop.parquet.filter.stats.enabled", "true")
        .config("spark.hadoop.parquet.filter.dictionary.enabled", "true")
        .config("spark.hadoop.parquet.filter.columnindex.enabled", "true")
        .config("spark.hadoop.parquet.filter.bloom.enabled", "true")
    )
    if interceptor:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.audit.enabled", "true")
            .config("spark.hadoop.fs.s3a.audit.referrer.enabled", "true")
            .config("spark.hadoop.fs.s3a.audit.execution.interceptors",
                    "software.amazon.awssdk.s3.adaptive.telemetry.Track2IoCollectorInterceptor")
        )
    if java_opts:
        builder = builder.config("spark.driver.extraJavaOptions", " ".join(java_opts))
    if eventlog_dir:
        os.makedirs(eventlog_dir, exist_ok=True)
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.compress", "true")
            .config("spark.eventLog.dir", eventlog_dir)
        )
    return builder


def export_aws_env(ak, sk):
    os.environ["AWS_ACCESS_KEY_ID"] = ak
    os.environ["AWS_SECRET_ACCESS_KEY"] = sk
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_REGION"] = REGION
