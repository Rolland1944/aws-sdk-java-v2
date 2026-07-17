#!/usr/bin/env python3
"""Trace-driven replay simulator for lakehouse range-read traces.

The simulator replays logical range-read traces against pluggable policies:

* existing implementation baselines (AWS Range GET, Hadoop S3A fadvise,
  Hadoop S3A prefetch), and
* rule templates derived from the project's workload characterization.

The goal is not byte-perfect Hadoop emulation. It is a transparent offline model
that preserves the important tradeoffs: GET count, remote bytes, cache hits,
prefetch usefulness, wasted bytes, and simulated logical latency.
"""
from __future__ import annotations

import argparse
import bisect
import heapq
import json
import os
import sys
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, replace
from typing import Iterable, Literal, Protocol

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "object_key", "offset", "length"}
OPTIONAL_COLUMNS = {"file_size", "query_id", "file_type"}
FetchKind = Literal["demand", "prefetch"]
WasteKind = Literal["sequential_skip", "unused_prefetch", "evicted_unused_block", "overfetch"]


def clamp_end(end: int, file_size: float | int | None) -> int:
    if file_size is None or pd.isna(file_size) or int(file_size) <= 0:
        return end
    return min(end, int(file_size))


def align_down(value: int, block_size: int) -> int:
    return (value // block_size) * block_size


def align_up(value: int, block_size: int) -> int:
    return ((value + block_size - 1) // block_size) * block_size


@dataclass(frozen=True)
class CostModel:
    """Simple remote-read latency model.

    Latency is modeled per remote GET as:
        request_rtt_ms + transferred_bytes / effective_bandwidth
    """

    request_rtt_ms: float = 50.0
    bandwidth_mib_s: float = 100.0

    def latency_ms(self, transferred_bytes: int) -> float:
        if transferred_bytes <= 0:
            return 0.0
        bytes_per_ms = self.bandwidth_mib_s * 1024 * 1024 / 1000.0
        if bytes_per_ms <= 0:
            raise ValueError("bandwidth_mib_s must be positive")
        return self.request_rtt_ms + transferred_bytes / bytes_per_ms

    def transfer_ms(self, transferred_bytes: int) -> float:
        if transferred_bytes <= 0:
            return 0.0
        bytes_per_ms = self.bandwidth_mib_s * 1024 * 1024 / 1000.0
        if bytes_per_ms <= 0:
            raise ValueError("bandwidth_mib_s must be positive")
        return transferred_bytes / bytes_per_ms


@dataclass(frozen=True)
class Request:
    timestamp: float
    object_key: str
    offset: int
    length: int
    file_size: float | int | None = None
    query_id: str = "?"
    file_type: str = "?"
    source_trace: str = "?"

    @property
    def end(self) -> int:
        return self.offset + self.length


@dataclass(frozen=True)
class Fetch:
    object_key: str
    start: int
    end: int
    kind: FetchKind = "demand"
    ready_at_ms: float | None = None
    useful_ranges: tuple[tuple[int, int], ...] = ()
    waste_kind: WasteKind | None = None
    counts_get: bool = True
    cacheable: bool = True

    @property
    def length(self) -> int:
        return max(0, self.end - self.start)


@dataclass
class IntervalCache:
    """Non-overlapping byte-interval cache for one object."""

    intervals: list[tuple[int, int]] = field(default_factory=list)

    def missing_ranges(self, start: int, end: int) -> list[tuple[int, int]]:
        """Return subranges in [start, end) not covered by cached intervals."""
        if start >= end:
            return []

        missing: list[tuple[int, int]] = []
        cursor = start
        idx = bisect.bisect_left(self.intervals, (start, -1))
        if idx > 0 and self.intervals[idx - 1][1] > start:
            idx -= 1

        while idx < len(self.intervals):
            cached_start, cached_end = self.intervals[idx]
            if cached_start >= end:
                break
            if cached_end <= cursor:
                idx += 1
                continue
            if cached_start > cursor:
                missing.append((cursor, min(cached_start, end)))
            cursor = max(cursor, cached_end)
            if cursor >= end:
                break
            idx += 1

        if cursor < end:
            missing.append((cursor, end))
        return missing

    def add(self, start: int, end: int) -> None:
        """Insert [start, end), merging overlapping or adjacent intervals."""
        if start >= end:
            return

        idx = bisect.bisect_left(self.intervals, (start, end))
        if idx > 0 and self.intervals[idx - 1][1] >= start:
            idx -= 1

        merged_start, merged_end = start, end
        remove_until = idx
        while remove_until < len(self.intervals):
            cached_start, cached_end = self.intervals[remove_until]
            if cached_start > merged_end:
                break
            merged_start = min(merged_start, cached_start)
            merged_end = max(merged_end, cached_end)
            remove_until += 1

        self.intervals[idx:remove_until] = [(merged_start, merged_end)]

    @property
    def cached_bytes(self) -> int:
        return sum(end - start for start, end in self.intervals)


@dataclass
class CacheBlock:
    start: int
    end: int
    source: FetchKind
    used_bytes: int = 0

    @property
    def length(self) -> int:
        return self.end - self.start


@dataclass
class BlockCache:
    """LRU block cache used by prefetch and template policies.

    A per-object index of block starts (sorted) keeps `find_covering` from
    scanning the whole cache on every read; on page-aligned traces the covering
    block is the one with the largest start <= query start, so lookups are
    effectively O(log n). LRU order is still tracked on `blocks`.
    """

    budget_bytes: int
    blocks: OrderedDict[tuple[str, int, int], CacheBlock] = field(default_factory=OrderedDict)
    _starts: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    _ends_by_start: dict[str, dict[int, list[int]]] = field(default_factory=lambda: defaultdict(dict))
    _bytes: int = 0

    def _index_add(self, key: str, start: int, end: int) -> None:
        ends = self._ends_by_start[key]
        bucket = ends.get(start)
        if bucket is None:
            ends[start] = [end]
            bisect.insort(self._starts[key], start)
        else:
            bisect.insort(bucket, end)

    def _index_remove(self, key: str, start: int, end: int) -> None:
        ends = self._ends_by_start.get(key)
        if ends is None:
            return
        bucket = ends.get(start)
        if not bucket:
            return
        pos = bisect.bisect_left(bucket, end)
        if pos < len(bucket) and bucket[pos] == end:
            bucket.pop(pos)
        if not bucket:
            del ends[start]
            starts = self._starts.get(key)
            if starts:
                idx = bisect.bisect_left(starts, start)
                if idx < len(starts) and starts[idx] == start:
                    starts.pop(idx)

    def get(self, key: str, start: int, end: int) -> CacheBlock | None:
        block_key = (key, start, end)
        block = self.blocks.get(block_key)
        if block is not None:
            self.blocks.move_to_end(block_key)
        return block

    def put(self, key: str, block: CacheBlock) -> list[CacheBlock]:
        block_key = (key, block.start, block.end)
        existing = self.blocks.pop(block_key, None)
        if existing is not None:
            block.used_bytes = max(block.used_bytes, existing.used_bytes)
            self._bytes -= existing.length
        else:
            self._index_add(key, block.start, block.end)
        self.blocks[block_key] = block
        self._bytes += block.length
        evicted: list[CacheBlock] = []
        while self._bytes > self.budget_bytes and self.blocks:
            (vkey, vstart, vend), victim = self.blocks.popitem(last=False)
            self._bytes -= victim.length
            self._index_remove(vkey, vstart, vend)
            evicted.append(victim)
        return evicted

    def find_covering(self, key: str, start: int, end: int) -> CacheBlock | None:
        starts = self._starts.get(key)
        if not starts:
            return None
        ends = self._ends_by_start[key]
        idx = bisect.bisect_right(starts, start)
        # scan candidate starts (<= query start) from largest downward; for each,
        # the widest end in its bucket is the best coverage candidate.
        for j in range(idx - 1, -1, -1):
            bstart = starts[j]
            bucket = ends[bstart]
            best_end = bucket[-1]
            if best_end < end:
                continue
            block = self.blocks.get((key, bstart, best_end))
            if block is not None:
                self.blocks.move_to_end((key, bstart, best_end))
                return block
        return None

    @property
    def cached_bytes(self) -> int:
        return self._bytes


@dataclass
class ObjectState:
    stream_start: int | None = None
    stream_pos: int | None = None
    stream_end: int | None = None
    normal_random: bool = False
    recent: deque[Request] = field(default_factory=lambda: deque(maxlen=8))
    seen_pages: dict[int, int] = field(default_factory=dict)


@dataclass
class ReplayState:
    cost: CostModel
    block_cache: BlockCache
    now_ms: float = 0.0
    objects: dict[str, ObjectState] = field(default_factory=lambda: defaultdict(ObjectState))
    ready_prefetch: dict[tuple[str, int, int], Fetch] = field(default_factory=dict)
    inflight_prefetch: list[tuple[float, int, Fetch]] = field(default_factory=list)
    next_fetch_id: int = 0
    history: deque[Request] = field(default_factory=lambda: deque(maxlen=64))

    def object_state(self, key: str) -> ObjectState:
        return self.objects[key]

    def note_request(self, req: Request) -> None:
        self.object_state(req.object_key).recent.append(req)
        self.history.append(req)

    def materialize_ready_prefetch(self) -> list[Fetch]:
        ready: list[Fetch] = []
        while self.inflight_prefetch and self.inflight_prefetch[0][0] <= self.now_ms:
            _, _, fetch = heapq.heappop(self.inflight_prefetch)
            ready.append(fetch)
        return ready

    def schedule_prefetch(self, fetch: Fetch) -> None:
        if fetch.length <= 0:
            return
        ready_at = fetch.ready_at_ms
        if ready_at is None:
            ready_at = self.now_ms + self.cost.latency_ms(fetch.length)
        fetch = replace(fetch, ready_at_ms=ready_at)
        heapq.heappush(self.inflight_prefetch, (ready_at, self.next_fetch_id, fetch))
        self.next_fetch_id += 1

    def find_inflight(self, key: str, start: int, end: int) -> Fetch | None:
        for _, _, fetch in self.inflight_prefetch:
            if fetch.object_key == key and fetch.start <= start and fetch.end >= end:
                return fetch
        return None

    def cached_bytes(self) -> int:
        return self.block_cache.cached_bytes


@dataclass
class MetricsBucket:
    logical_reads: int = 0
    logical_bytes: int = 0
    remote_gets: int = 0
    remote_bytes: int = 0
    demand_bytes: int = 0
    prefetch_bytes: int = 0
    prefetch_useful_bytes: int = 0
    prefetch_wasted_bytes: int = 0
    sequential_skipped_bytes: int = 0
    evicted_unused_bytes: int = 0
    overfetch_wasted_bytes: int = 0
    prefetch_hit_reads: int = 0
    prefetch_wait_reads: int = 0
    cache_hit_reads: int = 0
    cache_hit_bytes: int = 0
    simulated_latency_ms: list[float] = field(default_factory=list)

    def add_read(self, logical_bytes: int, hit_bytes: int, latency_ms: float) -> None:
        self.logical_reads += 1
        self.logical_bytes += logical_bytes
        self.cache_hit_bytes += hit_bytes
        if hit_bytes == logical_bytes:
            self.cache_hit_reads += 1
        self.simulated_latency_ms.append(latency_ms)

    def add_fetch(self, fetch: Fetch) -> None:
        if fetch.counts_get:
            self.remote_gets += 1
        self.remote_bytes += fetch.length
        if fetch.kind == "prefetch":
            self.prefetch_bytes += fetch.length
        else:
            self.demand_bytes += fetch.length
        if fetch.waste_kind == "sequential_skip":
            self.sequential_skipped_bytes += fetch.length
            self.prefetch_wasted_bytes += fetch.length
        elif fetch.waste_kind == "overfetch":
            self.overfetch_wasted_bytes += fetch.length
            self.prefetch_wasted_bytes += fetch.length

    def add_evicted(self, block: CacheBlock) -> None:
        unused = max(0, block.length - block.used_bytes)
        if unused and block.source == "prefetch":
            self.evicted_unused_bytes += unused
            self.prefetch_wasted_bytes += unused

    def merge(self, other: "MetricsBucket") -> None:
        self.logical_reads += other.logical_reads
        self.logical_bytes += other.logical_bytes
        self.remote_gets += other.remote_gets
        self.remote_bytes += other.remote_bytes
        self.demand_bytes += other.demand_bytes
        self.prefetch_bytes += other.prefetch_bytes
        self.prefetch_useful_bytes += other.prefetch_useful_bytes
        self.prefetch_wasted_bytes += other.prefetch_wasted_bytes
        self.sequential_skipped_bytes += other.sequential_skipped_bytes
        self.evicted_unused_bytes += other.evicted_unused_bytes
        self.overfetch_wasted_bytes += other.overfetch_wasted_bytes
        self.prefetch_hit_reads += other.prefetch_hit_reads
        self.prefetch_wait_reads += other.prefetch_wait_reads
        self.cache_hit_reads += other.cache_hit_reads
        self.cache_hit_bytes += other.cache_hit_bytes
        self.simulated_latency_ms.extend(other.simulated_latency_ms)

    def to_dict(self, memory_bytes: int) -> dict[str, float | int]:
        lat = np.asarray(self.simulated_latency_ms, dtype=float)
        logical = max(self.logical_bytes, 1)
        reads = max(self.logical_reads, 1)
        prefetch = max(self.prefetch_bytes, 1)
        return {
            "logical_reads": self.logical_reads,
            "logical_bytes": self.logical_bytes,
            "remote_gets": self.remote_gets,
            "remote_bytes": self.remote_bytes,
            "read_amplification": self.remote_bytes / logical,
            "demand_bytes": self.demand_bytes,
            "prefetch_bytes": self.prefetch_bytes,
            "prefetch_useful_bytes": self.prefetch_useful_bytes,
            "prefetch_consumption_rate": self.prefetch_useful_bytes / prefetch
            if self.prefetch_bytes
            else 0.0,
            "prefetch_wasted_bytes": self.prefetch_wasted_bytes,
            "sequential_skipped_bytes": self.sequential_skipped_bytes,
            "unused_prefetch_bytes": self.prefetch_wasted_bytes,
            "evicted_unused_bytes": self.evicted_unused_bytes,
            "overfetch_wasted_bytes": self.overfetch_wasted_bytes,
            "prefetch_hit_reads": self.prefetch_hit_reads,
            "prefetch_wait_reads": self.prefetch_wait_reads,
            "cache_hit_reads": self.cache_hit_reads,
            "cache_hit_read_rate": self.cache_hit_reads / reads,
            "cache_hit_bytes": self.cache_hit_bytes,
            "cache_hit_byte_rate": self.cache_hit_bytes / logical,
            "memory_bytes": memory_bytes,
            "latency_p50_ms": float(np.percentile(lat, 50)) if len(lat) else 0.0,
            "latency_p95_ms": float(np.percentile(lat, 95)) if len(lat) else 0.0,
            "latency_p99_ms": float(np.percentile(lat, 99)) if len(lat) else 0.0,
            "latency_sum_ms": float(lat.sum()) if len(lat) else 0.0,
        }


@dataclass
class ReplayMetrics:
    total: MetricsBucket = field(default_factory=MetricsBucket)
    by_query: dict[str, MetricsBucket] = field(default_factory=lambda: defaultdict(MetricsBucket))
    by_object: dict[str, MetricsBucket] = field(default_factory=lambda: defaultdict(MetricsBucket))

    def add_read(self, req: Request, logical_bytes: int, hit_bytes: int, latency_ms: float) -> None:
        self.total.add_read(logical_bytes, hit_bytes, latency_ms)
        self.by_query[req.query_id].add_read(logical_bytes, hit_bytes, latency_ms)
        self.by_object[req.object_key].add_read(logical_bytes, hit_bytes, latency_ms)

    def add_fetch(self, req: Request, fetch: Fetch) -> None:
        self.total.add_fetch(fetch)
        self.by_query[req.query_id].add_fetch(fetch)
        self.by_object[fetch.object_key].add_fetch(fetch)

    def add_evicted(self, req: Request, block: CacheBlock) -> None:
        self.total.add_evicted(block)
        self.by_query[req.query_id].add_evicted(block)
        self.by_object[req.object_key].add_evicted(block)

    def to_dict(self, memory_bytes: int) -> dict[str, object]:
        return {
            **self.total.to_dict(memory_bytes),
            "by_query_id": {
                key: bucket.to_dict(0) for key, bucket in sorted(self.by_query.items())
            },
            "by_object": {
                key: bucket.to_dict(0) for key, bucket in sorted(self.by_object.items())
            },
        }


class Policy(Protocol):
    name: str

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        ...


@dataclass
class AWSRangeGetPolicy:
    name: str = "aws_range_get"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        return [Fetch(req.object_key, req.offset, req.end, "demand", useful_ranges=((req.offset, req.end),), cacheable=False)]


@dataclass
class S3AFadviseRandomPolicy:
    readahead: int = 64 * 1024
    cache_budget_bytes: int = 64 * 1024 * 1024
    name: str = "s3a_random"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            return []
        end = clamp_end(req.offset + max(req.length, self.readahead), req.file_size)
        return [Fetch(req.object_key, req.offset, end, "demand", useful_ranges=((req.offset, req.end),))]


@dataclass
class S3AFadviseSequentialPolicy:
    stream_window: int = 128 * 1024 * 1024
    max_forward_skip: int = 8 * 1024 * 1024
    name: str = "s3a_seq"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        obj = state.object_state(req.object_key)
        if obj.stream_pos is not None and obj.stream_pos <= req.offset:
            gap = req.offset - obj.stream_pos
            if req.end <= (obj.stream_end or 0) and gap <= self.max_forward_skip:
                transfer_start = obj.stream_pos
                obj.stream_pos = req.end
                return [Fetch(
                    req.object_key,
                    transfer_start,
                    req.end,
                    "demand",
                    useful_ranges=((req.offset, req.end),),
                    counts_get=False,
                    cacheable=False,
                )]

        end = clamp_end(req.offset + max(req.length, self.stream_window), req.file_size)
        obj.stream_start = req.offset
        obj.stream_pos = req.end
        obj.stream_end = end
        return [Fetch(
            req.object_key,
            req.offset,
            req.end,
            "demand",
            useful_ranges=((req.offset, req.end),),
            cacheable=False,
        )]


@dataclass
class S3AFadviseNormalPolicy:
    sequential: S3AFadviseSequentialPolicy
    random: S3AFadviseRandomPolicy
    name: str = "s3a_normal"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        obj = state.object_state(req.object_key)
        if obj.recent and req.offset < obj.recent[-1].offset:
            obj.normal_random = True
        if obj.normal_random:
            return self.random.on_read(state, req)
        return self.sequential.on_read(state, req)


@dataclass
class S3APrefetchPolicy:
    block_size: int = 8 * 1024 * 1024
    prefetch_blocks: int = 8
    name: str = "s3a_prefetch"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        block_start = align_down(req.offset, self.block_size)
        block_end = clamp_end(block_start + self.block_size, req.file_size)
        key = (req.object_key, block_start, block_end)
        fetches: list[Fetch] = []

        block = state.block_cache.get(req.object_key, block_start, block_end)
        if block is None:
            ready = state.ready_prefetch.pop(key, None)
            if ready is not None:
                fetches.append(replace(ready, kind="prefetch", counts_get=False))
            elif (inflight := state.find_inflight(req.object_key, block_start, block_end)) is not None:
                fetches.append(replace(inflight, kind="prefetch", counts_get=False))
            else:
                fetches.append(Fetch(req.object_key, block_start, block_end, "demand", useful_ranges=((req.offset, req.end),)))

        for i in range(1, self.prefetch_blocks + 1):
            start = block_start + i * self.block_size
            end = clamp_end(start + self.block_size, req.file_size)
            if end <= start:
                continue
            pkey = (req.object_key, start, end)
            if (
                state.block_cache.get(req.object_key, start, end) is None
                and pkey not in state.ready_prefetch
                and not any(item[2].object_key == req.object_key and item[2].start == start and item[2].end == end for item in state.inflight_prefetch)
            ):
                fetches.append(Fetch(req.object_key, start, end, "prefetch"))
        return fetches


@dataclass
class TemplateSequentialPolicy:
    window: int = 2 * 1024 * 1024
    max_gap: int = 256 * 1024
    min_request: int = 64 * 1024
    name: str = "template_seq"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            return []
        obj = state.object_state(req.object_key)
        if obj.recent:
            prev = obj.recent[-1]
            gap = req.offset - prev.end
            sequential = 0 <= gap <= self.max_gap
        else:
            sequential = req.length >= self.min_request
        size = self.window if sequential or req.length >= self.min_request else req.length
        end = clamp_end(req.offset + max(req.length, size), req.file_size)
        return [Fetch(req.object_key, req.offset, end, "demand", useful_ranges=((req.offset, req.end),))]


@dataclass
class TemplateSmallRandomCoalescePolicy:
    page_size: int = 64 * 1024
    name: str = "template_small_random"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        start = align_down(req.offset, self.page_size)
        end = clamp_end(align_up(req.end, self.page_size), req.file_size)
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            return []
        return [Fetch(req.object_key, start, end, "demand", useful_ranges=((req.offset, req.end),))]


@dataclass
class TemplateLocalityCachePolicy:
    page_size: int = 256 * 1024
    small_readahead: int = 64 * 1024
    name: str = "template_locality"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            return []
        page = align_down(req.offset, self.page_size)
        obj = state.object_state(req.object_key)
        obj.seen_pages[page] = obj.seen_pages.get(page, 0) + 1
        if obj.seen_pages[page] >= 2:
            start = page
            end = clamp_end(page + self.page_size, req.file_size)
        else:
            start = req.offset
            end = clamp_end(req.offset + max(req.length, self.small_readahead), req.file_size)
        return [Fetch(req.object_key, start, end, "demand", useful_ranges=((req.offset, req.end),))]


@dataclass
class TemplateMultimodalTwoPhasePolicy:
    small_threshold: int = 16 * 1024
    small_page: int = 64 * 1024
    blob_prefetch: int = 0
    name: str = "template_multimodal"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            return []
        if req.length <= self.small_threshold:
            start = align_down(req.offset, self.small_page)
            end = clamp_end(align_up(req.end, self.small_page), req.file_size)
        else:
            start = req.offset
            end = req.end
        return [Fetch(req.object_key, start, end, "demand", useful_ranges=((req.offset, req.end),))]


@dataclass
class TemplateAutoPolicy:
    sequential: TemplateSequentialPolicy
    small_random: TemplateSmallRandomCoalescePolicy
    locality: TemplateLocalityCachePolicy
    multimodal: TemplateMultimodalTwoPhasePolicy
    name: str = "template_auto"

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        obj = state.object_state(req.object_key)
        recent = list(obj.recent)
        if req.query_id.startswith("retrieval") or req.length >= 128 * 1024:
            return self.multimodal.on_read(state, req)
        if len(recent) >= 2:
            forward = sum(1 for a, b in zip(recent, recent[1:]) if b.offset >= a.offset)
            if forward / max(len(recent) - 1, 1) >= 0.7 and req.length >= 32 * 1024:
                return self.sequential.on_read(state, req)
        page = align_down(req.offset, self.locality.page_size)
        if obj.seen_pages.get(page, 0) >= 1:
            return self.locality.on_read(state, req)
        if req.length <= 16 * 1024:
            return self.small_random.on_read(state, req)
        return self.sequential.on_read(state, req)


# ---------------------------------------------------------------------------
# Track 1: statistical IO-policy selector (offline-trained decision tree / SVM)
# ---------------------------------------------------------------------------
# The selector is a *meta-policy*: it does not implement any new IO behaviour.
# On every read it derives purely IO-stream features (no query_id / workload
# label -> no oracle leakage) over the recent request history, asks a trained
# model which of the existing sub-policies fits best, then delegates on_read to
# that sub-policy. Training targets come from the per-workload best strategy in
# PROJECT2.md 6.2. Features are computed identically here and in
# train_policy_selector.py so training and inference stay consistent.

SMALL_READ_BYTES = 16 * 1024
LARGE_READ_BYTES = 128 * 1024
SEQ_GAP_BYTES = 64 * 1024
FEATURE_PAGE_SIZE = 256 * 1024

FEATURE_NAMES: list[str] = [
    "log2_cur_size",
    "log2_med_size",
    "frac_small",
    "frac_large",
    "sequentiality",
    "forward_ratio",
    "log2_med_gap",
    "page_revisit_ratio",
    "distinct_obj_ratio",
    "cur_size_over_filesize",
]


def _valid_file_size(file_size: float | int | None) -> float | None:
    if file_size is None:
        return None
    try:
        value = float(file_size)
    except (TypeError, ValueError):
        return None
    if np.isnan(value) or value <= 0:
        return None
    return value


def extract_features(history: list[Request], req: Request) -> np.ndarray:
    """Purely IO-derived feature vector for one read.

    `history` is the recent request stream (all reads, hits included) observed
    *before* `req`. Matches ReplayState.history (bounded deque) at inference and
    an identical bounded window during offline training.
    """

    window = history + [req]
    n = len(window)
    sizes = [r.length for r in window]
    cur_size = max(1, req.length)
    med_size = float(np.median(sizes)) if sizes else float(cur_size)
    frac_small = sum(1 for s in sizes if s <= SMALL_READ_BYTES) / n
    frac_large = sum(1 for s in sizes if s >= LARGE_READ_BYTES) / n

    obj_window = [r for r in window if r.object_key == req.object_key]
    pairs = list(zip(obj_window, obj_window[1:]))
    if pairs:
        seq = sum(1 for a, b in pairs if 0 <= (b.offset - a.end) <= SEQ_GAP_BYTES) / len(pairs)
        fwd = sum(1 for a, b in pairs if b.offset >= a.offset) / len(pairs)
        gaps = [abs(b.offset - a.end) for a, b in pairs]
        med_gap = float(np.median(gaps)) if gaps else 0.0
    else:
        seq = 0.0
        fwd = 1.0
        med_gap = 0.0

    seen: set[tuple[str, int]] = set()
    revisits = 0
    for r in window:
        page = (r.object_key, r.offset // FEATURE_PAGE_SIZE)
        if page in seen:
            revisits += 1
        else:
            seen.add(page)
    page_revisit = revisits / n

    distinct_obj = len({r.object_key for r in window}) / n

    fsize = _valid_file_size(req.file_size)
    size_ratio = min(1.0, req.length / fsize) if fsize else 0.0

    return np.array(
        [
            np.log2(cur_size),
            np.log2(max(1.0, med_size)),
            frac_small,
            frac_large,
            seq,
            fwd,
            np.log2(1.0 + med_gap),
            page_revisit,
            distinct_obj,
            size_ratio,
        ],
        dtype=float,
    )


@dataclass
class StatisticalPolicySelector:
    """Meta-policy: trained model picks a sub-policy per read, with hysteresis."""

    model: object
    sub_policies: dict[str, Policy]
    hysteresis: int = 3
    name: str = "stat_selector"
    _current: str | None = field(default=None, init=False)
    _pending: str | None = field(default=None, init=False)
    _pending_n: int = field(default=0, init=False)
    _cache: dict[tuple[int, ...], str] = field(default_factory=dict, init=False)

    def _predict(self, feats: np.ndarray) -> str:
        # Quantize features so repeated access patterns reuse a cached prediction
        # (avoids a per-read sklearn call on 100k+ row traces).
        key = tuple(int(round(v * 100)) for v in feats)
        label = self._cache.get(key)
        if label is None:
            label = str(self.model.predict(feats.reshape(1, -1))[0])
            self._cache[key] = label
        return label

    def _apply_hysteresis(self, label: str) -> None:
        if self._current is None:
            self._current = label
            return
        if label == self._current:
            self._pending = None
            self._pending_n = 0
            return
        if label == self._pending:
            self._pending_n += 1
        else:
            self._pending = label
            self._pending_n = 1
        if self._pending_n >= self.hysteresis:
            self._current = label
            self._pending = None
            self._pending_n = 0

    def on_read(self, state: ReplayState, req: Request) -> list[Fetch]:
        feats = extract_features(list(state.history), req)
        label = self._predict(feats)
        if label not in self.sub_policies:
            label = self._current or next(iter(self.sub_policies))
        self._apply_hysteresis(label)
        return self.sub_policies[self._current].on_read(state, req)


def load_selector_bundle(path: str) -> dict:
    import joblib

    bundle = joblib.load(path)
    expected = list(bundle.get("feature_names", []))
    if expected and expected != FEATURE_NAMES:
        raise ValueError(
            "model feature schema mismatch:\n"
            f"  model:     {expected}\n"
            f"  simulator: {FEATURE_NAMES}"
        )
    return bundle


def load_trace(paths: Iterable[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    time_shift = 0.0

    for path in paths:
        df = pd.read_csv(path)
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"{path} missing required columns: {sorted(missing)}")

        df = df.copy()
        df["source_trace"] = path
        for col in OPTIONAL_COLUMNS - set(df.columns):
            if col == "query_id":
                df[col] = "?"
            else:
                df[col] = np.nan

        df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
        ts = pd.to_numeric(df["timestamp"], errors="raise")
        if len(ts):
            ts = ts - ts.min()
        df["timestamp"] = ts + time_shift
        df["offset"] = pd.to_numeric(df["offset"], errors="raise").astype(np.int64)
        df["length"] = pd.to_numeric(df["length"], errors="raise").astype(np.int64)
        df["object_key"] = df["object_key"].astype(str)
        df["query_id"] = df["query_id"].fillna("?").astype(str)

        if (df["offset"] < 0).any() or (df["length"] < 0).any():
            raise ValueError(f"{path} contains negative offset or length")

        frames.append(df)
        if len(df):
            time_shift = float(df["timestamp"].max()) + 1e-6

    if not frames:
        raise ValueError("no trace paths provided")
    return pd.concat(frames, ignore_index=True).sort_values("timestamp", kind="stable")


def overlap_bytes(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start))


def useful_bytes(fetch: Fetch) -> int:
    if not fetch.useful_ranges:
        return 0
    return sum(overlap_bytes(fetch.start, fetch.end, start, end) for start, end in fetch.useful_ranges)


def row_to_request(row) -> Request:
    return Request(
        timestamp=float(row.timestamp),
        object_key=str(row.object_key),
        offset=int(row.offset),
        length=int(row.length),
        file_size=getattr(row, "file_size", np.nan),
        query_id=str(getattr(row, "query_id", "?")),
        file_type=str(getattr(row, "file_type", "?")),
        source_trace=str(getattr(row, "source_trace", "?")),
    )


def cache_completed_prefetches(state: ReplayState, metrics: ReplayMetrics, req: Request) -> None:
    for fetch in state.materialize_ready_prefetch():
        evicted = state.block_cache.put(
            fetch.object_key,
            CacheBlock(fetch.start, fetch.end, source="prefetch"),
        )
        for block in evicted:
            metrics.add_evicted(req, block)


def account_overfetch(metrics: ReplayMetrics, req: Request, fetch: Fetch) -> None:
    if not fetch.useful_ranges:
        return
    useful = useful_bytes(fetch)
    wasted = max(0, fetch.length - useful)
    if wasted <= 0:
        return
    if fetch.waste_kind == "sequential_skip" or not fetch.counts_get:
        metrics.total.sequential_skipped_bytes += wasted
        metrics.by_query[req.query_id].sequential_skipped_bytes += wasted
        metrics.by_object[req.object_key].sequential_skipped_bytes += wasted
    else:
        metrics.total.overfetch_wasted_bytes += wasted
        metrics.by_query[req.query_id].overfetch_wasted_bytes += wasted
        metrics.by_object[req.object_key].overfetch_wasted_bytes += wasted
    metrics.total.prefetch_wasted_bytes += wasted
    metrics.by_query[req.query_id].prefetch_wasted_bytes += wasted
    metrics.by_object[req.object_key].prefetch_wasted_bytes += wasted


def execute_fetch(state: ReplayState, metrics: ReplayMetrics, req: Request, fetch: Fetch) -> float:
    if fetch.length <= 0:
        return 0.0

    if fetch.kind == "prefetch" and fetch.counts_get:
        metrics.add_fetch(req, fetch)
        state.schedule_prefetch(fetch)
        return 0.0

    if fetch.kind == "prefetch" and not fetch.counts_get:
        ready_at = fetch.ready_at_ms if fetch.ready_at_ms is not None else state.now_ms
        wait_ms = max(0.0, ready_at - state.now_ms)
        if wait_ms > 0:
            metrics.total.prefetch_wait_reads += 1
            metrics.by_query[req.query_id].prefetch_wait_reads += 1
            metrics.by_object[req.object_key].prefetch_wait_reads += 1
            state.now_ms = ready_at
        else:
            metrics.total.prefetch_hit_reads += 1
            metrics.by_query[req.query_id].prefetch_hit_reads += 1
            metrics.by_object[req.object_key].prefetch_hit_reads += 1
        if (fetch.object_key, fetch.start, fetch.end) in state.ready_prefetch:
            state.ready_prefetch.pop((fetch.object_key, fetch.start, fetch.end), None)
        evicted = state.block_cache.put(fetch.object_key, CacheBlock(fetch.start, fetch.end, "prefetch"))
        for block in evicted:
            metrics.add_evicted(req, block)
        return wait_ms

    latency = state.cost.latency_ms(fetch.length) if fetch.counts_get else state.cost.transfer_ms(fetch.length)
    metrics.add_fetch(req, fetch)
    account_overfetch(metrics, req, fetch)
    if fetch.cacheable:
        evicted = state.block_cache.put(fetch.object_key, CacheBlock(fetch.start, fetch.end, fetch.kind))
        for block in evicted:
            metrics.add_evicted(req, block)
    return latency


def mark_cache_use(block: CacheBlock, req: Request) -> int:
    used = overlap_bytes(block.start, block.end, req.offset, req.end)
    block.used_bytes = min(block.length, block.used_bytes + used)
    return used


def replay(
    df: pd.DataFrame,
    policy: Policy,
    cost: CostModel,
    cache_budget_bytes: int,
    records: list[dict] | None = None,
) -> tuple[ReplayMetrics, ReplayState]:
    metrics = ReplayMetrics()
    state = ReplayState(cost=cost, block_cache=BlockCache(cache_budget_bytes))

    for row in df.itertuples(index=False):
        req = row_to_request(row)
        state.now_ms = max(state.now_ms, req.timestamp * 1000.0)
        remote_before = metrics.total.remote_bytes
        cache_completed_prefetches(state, metrics, req)

        hit_bytes = 0
        latency_ms = 0.0
        block = state.block_cache.find_covering(req.object_key, req.offset, req.end)
        if block is not None:
            hit_bytes = mark_cache_use(block, req)
            if block.source == "prefetch":
                metrics.total.prefetch_useful_bytes += hit_bytes
                metrics.by_query[req.query_id].prefetch_useful_bytes += hit_bytes
                metrics.by_object[req.object_key].prefetch_useful_bytes += hit_bytes
        else:
            fetches = policy.on_read(state, req)
            for fetch in fetches:
                latency_ms += execute_fetch(state, metrics, req, fetch)
                if fetch.kind == "prefetch" and not fetch.counts_get:
                    hit_bytes = max(hit_bytes, req.length)
                    metrics.total.prefetch_useful_bytes += req.length
                    metrics.by_query[req.query_id].prefetch_useful_bytes += req.length
                    metrics.by_object[req.object_key].prefetch_useful_bytes += req.length

        metrics.add_read(req, req.length, hit_bytes, latency_ms)
        state.now_ms += latency_ms
        state.note_request(req)

        if records is not None:
            records.append(
                {
                    "source_trace": req.source_trace,
                    "query_id": req.query_id,
                    "logical_bytes": req.length,
                    "remote_bytes": metrics.total.remote_bytes - remote_before,
                    "latency_ms": latency_ms,
                    "hit": int(hit_bytes >= req.length),
                }
            )

    return metrics, state


def build_policy_dict(args: argparse.Namespace) -> dict[str, Policy]:
    readahead = args.readahead_kib * 1024
    block_size = args.block_size_mib * 1024 * 1024
    cache_budget = args.cache_budget_mib * 1024 * 1024
    seq = S3AFadviseSequentialPolicy(
        stream_window=args.stream_window_mib * 1024 * 1024,
        max_forward_skip=args.max_forward_skip_kib * 1024,
    )
    rnd = S3AFadviseRandomPolicy(readahead=readahead, cache_budget_bytes=cache_budget)

    policies: dict[str, Policy] = {
        "aws_range_get": AWSRangeGetPolicy(),
        "s3a_seq": seq,
        "s3a_random": rnd,
        "s3a_normal": S3AFadviseNormalPolicy(sequential=seq, random=rnd),
        "s3a_prefetch": S3APrefetchPolicy(block_size=block_size, prefetch_blocks=args.prefetch_blocks),
        "template_seq": TemplateSequentialPolicy(window=args.template_window_kib * 1024),
        "template_small_random": TemplateSmallRandomCoalescePolicy(page_size=args.template_small_page_kib * 1024),
        "template_locality": TemplateLocalityCachePolicy(page_size=args.template_locality_page_kib * 1024),
        "template_multimodal": TemplateMultimodalTwoPhasePolicy(),
        "template_auto": TemplateAutoPolicy(
            sequential=TemplateSequentialPolicy(window=args.template_window_kib * 1024),
            small_random=TemplateSmallRandomCoalescePolicy(page_size=args.template_small_page_kib * 1024),
            locality=TemplateLocalityCachePolicy(page_size=args.template_locality_page_kib * 1024),
            multimodal=TemplateMultimodalTwoPhasePolicy(),
        ),
    }

    model_path = getattr(args, "model", None)
    if model_path:
        bundle = load_selector_bundle(model_path)
        policies["stat_selector"] = StatisticalPolicySelector(
            model=bundle["model"],
            sub_policies={name: pol for name, pol in policies.items()},
            hysteresis=getattr(args, "selector_hysteresis", 3),
        )
    return policies


def build_policy(args: argparse.Namespace) -> Policy:
    policies = build_policy_dict(args)
    if args.policy not in policies:
        raise ValueError(
            f"policy '{args.policy}' unavailable "
            f"(did you pass --model for stat_selector?)"
        )
    return policies[args.policy]


def policy_names(include_selector: bool = False) -> list[str]:
    names = [
        "aws_range_get",
        "s3a_seq",
        "s3a_random",
        "s3a_normal",
        "s3a_prefetch",
        "template_seq",
        "template_small_random",
        "template_locality",
        "template_multimodal",
        "template_auto",
    ]
    if include_selector:
        names.append("stat_selector")
    return names


def format_bytes(value: int | float) -> str:
    value = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TiB"


def compact_group_summary(groups: dict[str, object], topn: int = 5) -> list[tuple[str, dict[str, object]]]:
    items = [(key, value) for key, value in groups.items() if isinstance(value, dict)]
    items.sort(key=lambda item: int(item[1].get("remote_bytes", 0)), reverse=True)
    return items[:topn]


def print_summary(summary: dict[str, object]) -> None:
    print("=== replay summary ===")
    print(f"policy               : {summary['policy']}")
    print(f"logical reads        : {summary['logical_reads']}")
    print(f"logical bytes        : {format_bytes(summary['logical_bytes'])}")
    print(f"remote GETs          : {summary['remote_gets']}")
    print(f"remote bytes         : {format_bytes(summary['remote_bytes'])}")
    print(f"read amplification   : {summary['read_amplification']:.3f}x")
    print(f"demand bytes         : {format_bytes(summary['demand_bytes'])}")
    print(f"prefetch bytes       : {format_bytes(summary['prefetch_bytes'])}")
    print(f"prefetch useful bytes: {format_bytes(summary['prefetch_useful_bytes'])}")
    print(f"prefetch wasted bytes: {format_bytes(summary['prefetch_wasted_bytes'])}")
    print(f"  sequential skipped : {format_bytes(summary['sequential_skipped_bytes'])}")
    print(f"  evicted unused     : {format_bytes(summary['evicted_unused_bytes'])}")
    print(f"  overfetch wasted   : {format_bytes(summary['overfetch_wasted_bytes'])}")
    print(f"prefetch hit/wait reads: {summary['prefetch_hit_reads']} / {summary['prefetch_wait_reads']}")
    print(f"cache hit read rate  : {summary['cache_hit_read_rate'] * 100:.2f}%")
    print(f"cache hit byte rate  : {summary['cache_hit_byte_rate'] * 100:.2f}%")
    print(f"memory bytes         : {format_bytes(summary['memory_bytes'])}")
    print(
        "logical latency p50/p95/p99: "
        f"{summary['latency_p50_ms']:.2f} / "
        f"{summary['latency_p95_ms']:.2f} / "
        f"{summary['latency_p99_ms']:.2f} ms"
    )
    if "by_query_id" in summary:
        print("\n=== top query_id by remote bytes ===")
        for key, value in compact_group_summary(summary["by_query_id"]):
            print(
                f"{key:20s} GETs={value['remote_gets']:<8} "
                f"remote={format_bytes(value['remote_bytes']):>12} "
                f"amp={value['read_amplification']:.2f}x"
            )
    if "by_object" in summary:
        print("\n=== top objects by remote bytes ===")
        for key, value in compact_group_summary(summary["by_object"]):
            label = key if len(key) <= 42 else "..." + key[-39:]
            print(
                f"{label:42s} GETs={value['remote_gets']:<8} "
                f"remote={format_bytes(value['remote_bytes']):>12} "
                f"amp={value['read_amplification']:.2f}x"
            )


def print_comparison(runs: dict[str, dict[str, object]]) -> None:
    print("=== policy comparison ===")
    print(f"{'policy':24s} {'GETs':>8s} {'remote':>12s} {'amp':>8s} {'hit%':>8s} {'p95_ms':>10s}")
    for name, summary in runs.items():
        print(
            f"{name:24s} "
            f"{summary['remote_gets']:8d} "
            f"{format_bytes(summary['remote_bytes']):>12s} "
            f"{summary['read_amplification']:8.2f} "
            f"{summary['cache_hit_read_rate'] * 100:8.2f} "
            f"{summary['latency_p95_ms']:10.2f}"
        )
def main() -> None:
    ap = argparse.ArgumentParser(description="Trace-driven range-read replay simulator")
    ap.add_argument("--trace", nargs="+", required=True, help="one or more trace CSV files")
    ap.add_argument(
        "--policy",
        default="aws_range_get",
        choices=[
            "all",
            "aws_range_get",
            "s3a_seq",
            "s3a_random",
            "s3a_normal",
            "s3a_prefetch",
            "template_seq",
            "template_small_random",
            "template_locality",
            "template_multimodal",
            "template_auto",
            "stat_selector",
        ],
    )
    ap.add_argument("--model", default=None, help="joblib bundle for stat_selector")
    ap.add_argument("--selector-hysteresis", type=int, default=3,
                    help="consecutive agreeing predictions before switching sub-policy")
    ap.add_argument("--request-rtt-ms", type=float, default=50.0)
    ap.add_argument("--bandwidth-mib-s", type=float, default=100.0)
    ap.add_argument("--readahead-kib", type=int, default=64)
    ap.add_argument("--block-size-mib", type=int, default=8)
    ap.add_argument("--prefetch-blocks", type=int, default=8)
    ap.add_argument("--cache-budget-mib", type=int, default=256)
    ap.add_argument("--stream-window-mib", type=int, default=128)
    ap.add_argument("--max-forward-skip-kib", type=int, default=8192)
    ap.add_argument("--template-window-kib", type=int, default=2048)
    ap.add_argument("--template-small-page-kib", type=int, default=64)
    ap.add_argument("--template-locality-page-kib", type=int, default=256)
    ap.add_argument("--out-json", default=None, help="optional path for summary JSON")
    args = ap.parse_args()

    try:
        trace = load_trace(args.trace)
        cost = CostModel(
            request_rtt_ms=args.request_rtt_ms,
            bandwidth_mib_s=args.bandwidth_mib_s,
        )
    except Exception as exc:
        sys.exit(f"error: {exc}")

    def run_one(policy_name: str) -> dict[str, object]:
        run_args = argparse.Namespace(**vars(args))
        run_args.policy = policy_name
        policy = build_policy(run_args)
        metrics, state = replay(
            trace,
            policy,
            cost,
            cache_budget_bytes=args.cache_budget_mib * 1024 * 1024,
        )
        summary = metrics.to_dict(memory_bytes=state.cached_bytes())
        summary["policy"] = policy_name
        summary["trace_files"] = args.trace
        summary["objects"] = int(trace["object_key"].nunique())
        summary["request_rtt_ms"] = args.request_rtt_ms
        summary["bandwidth_mib_s"] = args.bandwidth_mib_s
        summary["cache_budget_mib"] = args.cache_budget_mib
        return summary

    if args.policy == "all":
        runs = {name: run_one(name) for name in policy_names(include_selector=bool(args.model))}
        print_comparison(runs)
        output: dict[str, object] = {"runs": runs, "trace_files": args.trace}
    else:
        summary = run_one(args.policy)
        print_summary(summary)
        output = summary

    if args.out_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
        with open(args.out_json, "w") as f:
            json.dump(output, f, indent=2, sort_keys=True)
            f.write("\n")
        print(f"\nwrote {args.out_json}")


if __name__ == "__main__":
    main()
