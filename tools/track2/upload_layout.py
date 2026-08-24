#!/usr/bin/env python3
"""Upload a written layout directory to S3, preserving the <table>/<file> tree.

The benchmark reads layouts over s3a://, so a layout produced locally by
write_layout.py has to be copied up before E8. The AWS CLI is not installed on
this host; boto3 is, and it already has the credentials the SDK interceptor
uses.

Usage:
  python3 tools/track2/upload_layout.py \
      --source /data/home/haoyueli/track2-data/clickbench_sf1_eventdate \
      --dest s3://home-haoyue/track2/clickbench_sf1_eventdate
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time

import boto3
from boto3.s3.transfer import TransferConfig


def iter_files(root):
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            full = os.path.join(dirpath, name)
            yield full, os.path.relpath(full, root).replace(os.sep, "/")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True)
    ap.add_argument("--dest", required=True, help="s3://bucket/prefix")
    ap.add_argument("--concurrency", type=int, default=8)
    args = ap.parse_args()

    if not args.dest.startswith("s3://"):
        sys.exit("--dest must be s3://bucket/prefix")
    bucket, _, prefix = args.dest[len("s3://"):].partition("/")
    prefix = prefix.rstrip("/")

    entries = sorted(iter_files(args.source))
    total = sum(os.path.getsize(f) for f, _ in entries)
    print(f"# upload {len(entries)} files, {total / 2 ** 30:.2f} GiB")
    print(f"  {args.source} -> s3://{bucket}/{prefix}")

    client = boto3.client("s3")
    config = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                            multipart_chunksize=64 * 1024 * 1024,
                            max_concurrency=args.concurrency)
    done = [0]
    lock = threading.Lock()
    started = time.time()

    def progress(nbytes):
        with lock:
            done[0] += nbytes
            frac = done[0] / total if total else 1.0
            elapsed = time.time() - started
            rate = done[0] / elapsed / 2 ** 20 if elapsed else 0
            print(f"\r  {frac * 100:5.1f}%  {done[0] / 2 ** 30:6.2f} GiB  "
                  f"{rate:6.1f} MiB/s  {elapsed / 60:5.1f} min", end="", flush=True)

    for full, rel in entries:
        client.upload_file(full, bucket, f"{prefix}/{rel}",
                           Config=config, Callback=progress)
    elapsed = time.time() - started
    print(f"\n  done in {elapsed / 60:.1f} min "
          f"({total / elapsed / 2 ** 20:.1f} MiB/s average)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
