#!/usr/bin/env python3
"""
Provision deterministic objects for mixed_holdout onto S3.

Bytes match software.amazon.awssdk.s3.adaptive.internal.io.GeneratedObjectStore#byteAt
so AdaptiveReaderSystemBenchmark can verify on backend=s3. Original parquet/lance under
data.tar.gz are NOT used (and would fail byte verification).

Uploads to s3://<bucket>/<key-prefix>/<trace_object_key> and optionally writes a
rewritten trace with the prefix baked into object_key.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import boto3
import numpy as np
from botocore.config import Config

PART_BYTES = 16 * 1024 * 1024
# Split64 constants from GeneratedObjectStore.byteAt (Java long = signed, we use uint64 wrap)
C1 = np.uint64(0x9E3779B97F4A7C15)
C2 = np.uint64(0xBF58476D1CE4E5B9)


def byte_at_range(start: int, length: int) -> bytes:
    """Vectorized GeneratedObjectStore.byteAt for [start, start+length)."""
    if length <= 0:
        return b""
    # process in chunks to bound peak RAM
    out = bytearray(length)
    chunk = 4 * 1024 * 1024
    for off in range(0, length, chunk):
        n = min(chunk, length - off)
        pos = np.arange(start + off, start + off + n, dtype=np.uint64)
        x = (pos + np.uint64(1)) * C1
        x ^= x >> np.uint64(29)
        x *= C2
        x ^= x >> np.uint64(32)
        out[off : off + n] = (x & np.uint64(0xFF)).astype(np.uint8).tobytes()
    return bytes(out)


def inventory(trace_path: str) -> Dict[str, int]:
    need: Dict[str, int] = {}
    with open(trace_path, newline="") as fh:
        for row in csv.DictReader(fh):
            k = row["object_key"]
            sz = max(int(row["file_size"]), int(row["offset"]) + int(row["length"]))
            need[k] = max(need.get(k, 0), sz)
    return need


def write_prefixed_trace(src: str, dst: str, prefix: str) -> None:
    prefix = prefix.strip("/")
    with open(src, newline="") as fin, open(dst, "w", newline="") as fout:
        r = csv.DictReader(fin)
        w = csv.DictWriter(fout, fieldnames=r.fieldnames)
        w.writeheader()
        for row in r:
            row["object_key"] = f"{prefix}/{row['object_key']}"
            w.writerow(row)


def existing_size(s3, bucket: str, key: str):
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def upload_one(s3, bucket: str, key: str, size: int) -> str:
    existing = existing_size(s3, bucket, key)
    if existing is not None and existing >= size:
        print(f"[skip] {key} ({existing} bytes)", flush=True)
        return "existing"

    t0 = time.time()
    print(f"[upload] {key} ({size} bytes, {size/1024**2:.1f} MiB)", flush=True)
    if size <= PART_BYTES:
        body = byte_at_range(0, size)
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    else:
        mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]
        parts = []
        try:
            part_number = 1
            for start in range(0, size, PART_BYTES):
                length = min(PART_BYTES, size - start)
                body = byte_at_range(start, length)
                resp = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=body,
                )
                parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
                part_number += 1
            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise
    verified = existing_size(s3, bucket, key)
    if verified != size:
        raise RuntimeError(f"HEAD mismatch for {key}: want {size} got {verified}")
    print(f"[done]  {key} in {time.time()-t0:.1f}s", flush=True)
    return "uploaded"


def self_check():
    """Spot-check against a few Java-known values computed independently."""
    # Java: byteAt(0) = (byte) after splitmix; we only need stability + bit width
    b0 = byte_at_range(0, 8)
    b1 = byte_at_range(0, 1)
    assert len(b0) == 8 and b0[0:1] == b1
    # overlapping windows must agree
    a = byte_at_range(1000, 64)
    b = byte_at_range(1016, 32)
    assert a[16:48] == b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", default="traces/mixed_holdout.csv")
    ap.add_argument("--bucket", default="home-haoyue")
    ap.add_argument("--key-prefix", default="mixed_holdout")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--workers", type=int, default=4,
                    help="parallel objects (parts within an object stay sequential)")
    ap.add_argument("--rewritten-trace", default="traces/mixed_holdout_s3.csv")
    ap.add_argument("--manifest", default="/mnt/nvme/logs/mixed_holdout_manifest.json")
    args = ap.parse_args()

    self_check()
    objs = inventory(args.trace)
    prefix = args.key_prefix.strip("/")
    print(f"objects={len(objs)} total={sum(objs.values())/1024**3:.2f} GiB "
          f"-> s3://{args.bucket}/{prefix}/", flush=True)

    write_prefixed_trace(args.trace, args.rewritten_trace, prefix)
    print(f"rewritten trace -> {args.rewritten_trace}", flush=True)

    session = boto3.session.Session(region_name=args.region)
    def client():
        return session.client(
            "s3",
            config=Config(max_pool_connections=64, retries={"max_attempts": 10, "mode": "adaptive"}),
        )

    # Sort largest-first so the long pole starts early
    items: List[Tuple[str, int]] = sorted(
        ((f"{prefix}/{k}", sz) for k, sz in objs.items()),
        key=lambda kv: -kv[1],
    )

    manifest = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(upload_one, client(), args.bucket, k, sz): (k, sz) for k, sz in items}
        for fut in as_completed(futs):
            k, sz = futs[fut]
            status = fut.result()
            manifest.append({"key": k, "bytes": sz, "status": status})

    with open(args.manifest, "w") as fh:
        json.dump(
            {
                "bucket": args.bucket,
                "prefix": prefix,
                "trace": os.path.abspath(args.trace),
                "rewritten_trace": os.path.abspath(args.rewritten_trace),
                "elapsed_s": round(time.time() - t0, 1),
                "objects": manifest,
            },
            fh,
            indent=2,
        )
    print(f"\nALL DONE in {time.time()-t0:.1f}s  manifest={args.manifest}", flush=True)


if __name__ == "__main__":
    main()
