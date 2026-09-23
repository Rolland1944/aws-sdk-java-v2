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


TRACK1_S3_CLIENT_FACTORY = (
    "software.amazon.awssdk.s3.adaptive.s3a.Track1S3ClientFactory")


def apply_frozen_reader(builder, ak, sk, collector_dir=None, eventlog_dir=None,
                        interceptor=True, track1_s3a=False,
                        track1_d1=False, track1_d2=False, track1_d4=False,
                        track1_d2_wait_us=0, track1_d1_admit_bytes=256 * 1024,
                        track1_d1_cache_mib=256, track1_d1_block_bytes=None,
                        track1_d1_adaptive=False, track1_d1_hard_mib=None,
                        track1_d1_coverage=None, track1_d1_observe_gets=None,
                        track1_d1_min_mib=None, track1_d1_fixed_capacity=False,
                        track1_d1_fixed_admission=False, track1_d1_profile=False,
                        track1_d1_zero_copy=True, track1_d1_doorkeeper=True,
                        track1_d1_shared_backing=True):
    """Apply TRACK2_M0_CONTRACT.md 1.4 frozen reader settings.

    ``track1_s3a`` is off by default so existing Track 2 E-0 numbers stay on
    the native S3A client. When on, S3A constructs AWS clients through
    ``Track1S3ClientFactory``. D1/D2/D4 default off (P0 passthrough).

    ``track1_d1_admit_bytes`` and ``track1_d1_cache_mib`` have to move
    together: raising the admission cap alone lets large scans evict the small
    reads that supply most of the hits. ``track1_d1_block_bytes`` defaults to
    the reader's own 1 MiB so an admitted range is not split needlessly.
    """
    java_opts = []
    if collector_dir:
        os.makedirs(collector_dir, exist_ok=True)
        os.environ["TRACK2_COLLECTOR_DIR"] = collector_dir
        java_opts.append(f"-Dtrack2.collector.dir={collector_dir}")
    if track1_s3a and collector_dir:
        probe_dir = os.path.join(collector_dir, "track1-probe")
        os.makedirs(probe_dir, exist_ok=True)
        os.environ["TRACK1_S3A_PROBE_DIR"] = probe_dir
        java_opts.append(f"-Dtrack1.s3a.probe.dir={probe_dir}")
    if track1_s3a:
        java_opts.append(f"-Dtrack1.d1={str(bool(track1_d1)).lower()}")
        java_opts.append(f"-Dtrack1.d2={str(bool(track1_d2)).lower()}")
        java_opts.append(f"-Dtrack1.d4={str(bool(track1_d4)).lower()}")
        java_opts.append(f"-Dtrack1.d2.wait.us={int(track1_d2_wait_us)}")
        java_opts.append(f"-Dtrack1.d1.admit.bytes={int(track1_d1_admit_bytes)}")
        java_opts.append(f"-Dtrack1.d1.cache.mib={int(track1_d1_cache_mib)}")
        java_opts.append(
            f"-Dtrack1.d1.adaptive={str(bool(track1_d1_adaptive)).lower()}")
        java_opts.append(
            f"-Dtrack1.d1.fixed.capacity={str(bool(track1_d1_fixed_capacity)).lower()}")
        java_opts.append(
            f"-Dtrack1.d1.fixed.admission={str(bool(track1_d1_fixed_admission)).lower()}")
        java_opts.append(f"-Dtrack1.d1.profile={str(bool(track1_d1_profile)).lower()}")
        java_opts.append(f"-Dtrack1.d1.zero.copy={str(bool(track1_d1_zero_copy)).lower()}")
        java_opts.append(f"-Dtrack1.d1.doorkeeper={str(bool(track1_d1_doorkeeper)).lower()}")
        java_opts.append(f"-Dtrack1.d1.shared.backing={str(bool(track1_d1_shared_backing)).lower()}")
        if track1_d1_block_bytes:
            java_opts.append(
                f"-Dtrack1.d1.block.bytes={int(track1_d1_block_bytes)}")
        if track1_d1_hard_mib is not None:
            java_opts.append(f"-Dtrack1.d1.hard.mib={int(track1_d1_hard_mib)}")
        if track1_d1_coverage is not None:
            java_opts.append(f"-Dtrack1.d1.coverage={float(track1_d1_coverage)}")
        if track1_d1_observe_gets is not None:
            java_opts.append(
                f"-Dtrack1.d1.observe.gets={int(track1_d1_observe_gets)}")
        if track1_d1_min_mib is not None:
            java_opts.append(f"-Dtrack1.d1.min.mib={int(track1_d1_min_mib)}")
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
        # Pin the split size L1 uses for n_scan_units. Leaving this at Spark's
        # default of 128 MiB is fine, but recording it here makes the model
        # and the reader share one number.
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.hadoop.parquet.filter.stats.enabled", "true")
        .config("spark.hadoop.parquet.filter.dictionary.enabled", "true")
        .config("spark.hadoop.parquet.filter.columnindex.enabled", "true")
        .config("spark.hadoop.parquet.filter.bloom.enabled", "true")
    )
    if track1_s3a:
        builder = builder.config(
            "spark.hadoop.fs.s3a.s3.client.factory.impl",
            TRACK1_S3_CLIENT_FACTORY)
    if interceptor:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.audit.enabled", "true")
            .config("spark.hadoop.fs.s3a.audit.referrer.enabled", "true")
            .config("spark.hadoop.fs.s3a.audit.execution.interceptors",
                    "software.amazon.awssdk.s3.adaptive.telemetry.Track2IoCollectorInterceptor")
        )
    if java_opts:
        extra_java = " ".join(java_opts)
        builder = builder.config("spark.driver.extraJavaOptions", extra_java)
        if track1_s3a:
            # local[*] runs tasks in the driver JVM; a real cluster would
            # otherwise construct unwrapped clients on the executors.
            builder = builder.config("spark.executor.extraJavaOptions", extra_java)
    if eventlog_dir:
        os.makedirs(eventlog_dir, exist_ok=True)
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.compress", "true")
            .config("spark.eventLog.dir", eventlog_dir)
        )
    return builder


def frozen_reader_evidence():
    """The split / vectored knobs L1 must price against. Contract §1.4."""
    return {
        "split_size_bytes": 128 * 1024 * 1024,
        "min_seek_bytes": 131072,
        "max_merged_bytes": 2097152,
        "active_ranged_reads": 4,
        "source": "s3a_session.apply_frozen_reader (contract 1.4)",
    }


def export_aws_env(ak, sk):
    os.environ["AWS_ACCESS_KEY_ID"] = ak
    os.environ["AWS_SECRET_ACCESS_KEY"] = sk
    os.environ["AWS_DEFAULT_REGION"] = REGION
    os.environ["AWS_REGION"] = REGION
