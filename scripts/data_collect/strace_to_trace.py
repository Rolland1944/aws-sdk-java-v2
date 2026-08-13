#!/usr/bin/env python3
"""
strace 日志 -> 访问 trace CSV（Lance 等原生 IO 引擎的采集口子）。

为什么用 strace：
  Lance 是 Rust 原生 IO，读 .lance 不走 pyarrow，所以 TPC-H 那套 LoggingFileSystem
  对它失效。strace 在 syscall 层拦截 pread64/preadv，能拿到任何本地读取器的精确
  (文件, offset, length)，且引擎无关（未来 ML/多模态本地负载同样适用）。

输入：用如下方式产生的 strace 日志（关键：-y 把 fd 标注成路径，-ttt 给时间戳）
  strace -f -e trace=pread64,preadv,write -y -ttt -s 200 -o run.strace <cmd>

query_id 标注：查询脚本在每条查询前往 marker 文件写一行 "QUERY <qid>"，
  strace 里表现为 write(fd<...marker>, "QUERY <qid>\n", n)；本脚本按时间顺序把
  marker 之后、下一个 marker 之前的所有 pread 归到该 qid。

输出：timestamp,object_key,offset,length,file_size,query_id（与 heatmap 对齐）
"""
from __future__ import annotations

import argparse
import os
import re

# 完整一行：  TID  TS  syscall(args) = ret
RE_COMPLETE = re.compile(r"^(\d+)\s+([\d.]+)\s+(\w+)\((.*)\)\s*=\s*(-?\d+)")
# 被打断：    TID  TS  pread64(fd<...>, "buf"... <unfinished ...>
RE_UNFIN = re.compile(r"^(\d+)\s+([\d.]+)\s+(.*?)<unfinished \.\.\.>\s*$")
# 续上：      TID  TS  <... pread64 resumed> ..., count, offset) = ret
RE_RESUME = re.compile(r"^(\d+)\s+([\d.]+)\s+<\.\.\. \w+ resumed>(.*)$")

RE_FD_PATH = re.compile(r"\d+<([^>]+)>")
# pread64/preadv 参数尾部固定是 ", <count>, <offset>"
RE_PREAD_TAIL = re.compile(r",\s*(\d+),\s*(\d+)\s*$")
RE_QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')


def parse(strace_log: str, root: str, marker_name: str):
    root = os.path.abspath(root)
    events = []          # (ts, kind, payload)
    pending = {}         # tid -> head（未完成的 syscall 前半段）

    def handle(ts, syscall, args, ret):
        if syscall in ("pread64", "preadv"):
            m = RE_FD_PATH.search(args)
            if not m:
                return
            path = m.group(1)
            ap = os.path.abspath(path)
            if not ap.startswith(root):
                return
            t = RE_PREAD_TAIL.search(args)
            if not t:
                return
            count, offset = int(t.group(1)), int(t.group(2))
            length = ret if ret and ret > 0 else count
            events.append((ts, "read", (ap, offset, length)))
        elif syscall == "write":
            m = RE_FD_PATH.search(args)
            if not m or marker_name not in m.group(1):
                return
            q = RE_QUOTED.search(args)
            if not q:
                return
            s = q.group(1)
            if s.startswith("QUERY "):
                # 清理 strace 转义残留（如字面 \n）与分隔符
                qid = s[6:].replace("\\n", "").replace("\\t", "").strip(" ;\t")
                events.append((ts, "marker", qid))

    with open(strace_log, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            mr = RE_RESUME.match(line)
            if mr:
                tid, ts, tail = mr.group(1), float(mr.group(2)), mr.group(3)
                head = pending.pop(tid, None)
                if head is None:
                    continue
                inner = head + tail  # 形如 pread64(...args...) = ret
                mc = re.match(r"^(\w+)\((.*)\)\s*=\s*(-?\d+)", inner)
                if mc:
                    handle(ts, mc.group(1), mc.group(2), int(mc.group(3)))
                continue
            mu = RE_UNFIN.match(line)
            if mu:
                pending[mu.group(1)] = mu.group(3)  # 存 head（含 syscall 名）
                continue
            mc = RE_COMPLETE.match(line)
            if mc:
                handle(float(mc.group(2)), mc.group(3), mc.group(4), int(mc.group(5)))

    return events


def to_csv(events, root, out):
    import pandas as pd

    root = os.path.abspath(root)
    events.sort(key=lambda e: e[0])
    rows = []
    cur_q = "?"
    t0 = None
    sizes: dict[str, int] = {}
    for ts, kind, payload in events:
        if kind == "marker":
            cur_q = payload
            continue
        path, offset, length = payload
        if t0 is None:
            t0 = ts
        if path not in sizes:
            sizes[path] = os.path.getsize(path) if os.path.exists(path) else 0
        rows.append((ts - t0, os.path.relpath(path, root), offset, length, sizes[path], cur_q))

    df = pd.DataFrame(rows, columns=[
        "timestamp", "object_key", "offset", "length", "file_size", "query_id"])
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    df.to_csv(out, index=False)
    return df


def main():
    ap = argparse.ArgumentParser(description="strace 日志 -> 访问 trace CSV")
    ap.add_argument("--strace-log", required=True)
    ap.add_argument("--root", required=True, help="数据集 uri，用于过滤 + 计算 object_key")
    ap.add_argument("--out", required=True)
    ap.add_argument("--marker-name", default="_marker", help="marker 文件名匹配片段")
    args = ap.parse_args()

    events = parse(args.strace_log, args.root, args.marker_name)
    df = to_csv(events, args.root, args.out)
    reads = (df.query_id != "?").sum() if len(df) else 0
    print(f"[parse] {len(df)} 次 read（含标注 {reads}）-> {os.path.abspath(args.out)}")
    if len(df):
        g = df.groupby(df.object_key.str.split("/").str[0]).agg(
            reads=("length", "size"), MB=("length", lambda s: s.sum() / 1e6)).round(2)
        print("[parse] 按顶层目录:")
        print(g.to_string())


if __name__ == "__main__":
    main()
