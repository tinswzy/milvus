#!/usr/bin/env python3
# Licensed to the LF AI & Data foundation under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.
#
# issue #49435 reproduction driver.
#
# Scenario (mirrors the QA fouram case that reproduces the bug):
#   - collection: int64 pk + float_vector(dim=128), 2 shards, 16 partitions
#   - continuous upsert: nb=2000 per batch, sequential PKs cycling 0..NUM_ROWS
#     (re-upserting the SAME PKs -> heavy deletes + continuous sort-compaction)
#   - concurrent Strong-consistency count(*) queries
#
# Expected failure signature when the bug triggers:
#   MilvusException code=505 "channel tsafe stalled" / "Timestamp lag too large"
#
# The instrumented server branch (debug/issue-49435-delete-apply-probe) exposes
# per-step histograms:
#   issue49435_delete_apply_step_latency{step=total|parse|pin_ts|pin_pk|scan|
#                                             read_insert_ts|skiplist_insert}
#   issue49435_delete_apply_matched_rows
# plus one INFO log line per apply: grep "[DeleteApplyProbe]".
#
# Usage:
#   pip install pymilvus numpy
#   python3 repro_upsert_strong_query.py --uri http://127.0.0.1:19530
#
# Tunables (defaults mirror the QA case, rates calibrated from the actual
# QA reproduction measured in Prometheus on 2026-07-08: upsert 0.45 batch/s,
# strong count(*) 0.45 qps, first 505 ~1-2h after start, mem 6->8->12GB):
#   --rows 2000000 --nb 2000 --dim 128 --shards 2 --partitions 16
#   --upsert-workers 1 --upsert-interval 2.0
#   --query-workers 2  --query-interval 4.0
#   --duration 0 (0 = run forever)
# Server environment REQUIRED to reproduce: Linux standalone with cgroup
# limits cpus=8 memory=17g (resource starvation is part of the trigger),
# MinIO object storage, default StorageV2 + streaming(woodpecker).

import argparse
import random
import signal
import sys
import threading
import time
from collections import defaultdict

import numpy as np
from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
    MilvusException,
)

STOP = threading.Event()


class Stats:
    """Thread-safe latency/error accumulator, printed periodically."""

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.lat_ms = []
        self.errors = defaultdict(int)
        self.err_samples = []
        self.count = 0

    def ok(self, ms):
        with self.lock:
            self.count += 1
            self.lat_ms.append(ms)

    def fail(self, exc):
        key = getattr(exc, "code", None) or type(exc).__name__
        msg = str(exc)
        with self.lock:
            self.count += 1
            self.errors[key] += 1
            if len(self.err_samples) < 200:
                self.err_samples.append(
                    (time.strftime("%H:%M:%S"), msg[:300]))

    def snapshot_and_reset(self):
        with self.lock:
            lat, errs, n = self.lat_ms, dict(self.errors), self.count
            self.lat_ms, self.count = [], 0
            self.errors = defaultdict(int)
        line = f"[{self.name}] n={n}"
        if lat:
            a = np.array(lat)
            line += (f" p50={np.percentile(a, 50):.0f}ms"
                     f" p95={np.percentile(a, 95):.0f}ms"
                     f" p99={np.percentile(a, 99):.0f}ms"
                     f" max={a.max():.0f}ms")
        if errs:
            line += f" ERRORS={errs}"
        return line


def build_collection(client, args):
    if client.has_collection(args.collection):
        if args.recreate:
            client.drop_collection(args.collection)
        else:
            print(f"collection {args.collection} exists, reusing "
                  "(use --recreate to drop)")
            client.load_collection(args.collection)
            return
    fields = [
        FieldSchema("id", DataType.INT64, is_primary=True),
        FieldSchema("float_vector", DataType.FLOAT_VECTOR, dim=args.dim),
    ]
    schema = CollectionSchema(fields, description="issue49435 repro")
    client.create_collection(
        args.collection, schema=schema, num_shards=args.shards)
    for p in range(args.partitions):
        client.create_partition(args.collection, f"p{p}")
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="float_vector", index_type="AUTOINDEX",
        metric_type="L2")
    client.create_index(args.collection, index_params)
    client.load_collection(args.collection)
    print(f"created {args.collection}: shards={args.shards} "
          f"partitions={args.partitions} dim={args.dim}")


def pk_partition(args, pk):
    # stable pk -> partition mapping so re-upserts hit the same partition
    block = args.rows // args.partitions or 1
    return f"p{min(pk // block, args.partitions - 1)}"


def initial_fill(client, args, stats):
    """First full pass over 0..rows so every later pass is a real upsert."""
    print(f"initial fill: {args.rows} rows, nb={args.nb} ...")
    t0 = time.time()
    for start in range(0, args.rows, args.nb):
        if STOP.is_set():
            return
        end = min(start + args.nb, args.rows)
        rows = [{
            "id": pk,
            "float_vector": np.random.rand(args.dim).astype(
                np.float32).tolist(),
        } for pk in range(start, end)]
        client.upsert(args.collection, rows,
                      partition_name=pk_partition(args, start))
        if (start // args.nb) % 100 == 0:
            print(f"  fill {end}/{args.rows} "
                  f"({end / max(time.time() - t0, 1):.0f} rows/s)")
    print(f"initial fill done in {time.time() - t0:.0f}s")


def upsert_worker(client, args, stats, worker_id):
    """Endless sequential re-upsert of the same PK space (the churn)."""
    # Align the start cursor to the nb grid: pk_partition() maps a batch by
    # its START pk, so every re-upsert of a pk must reuse the same batch grid
    # as the initial fill — otherwise the delete of the old version would be
    # routed to a different partition and miss.
    cursor = ((args.rows // max(args.upsert_workers, 1)) * worker_id
              // args.nb) * args.nb
    while not STOP.is_set():
        start = cursor % args.rows
        end = min(start + args.nb, args.rows)
        rows = [{
            "id": pk,
            "float_vector": np.random.rand(args.dim).astype(
                np.float32).tolist(),
        } for pk in range(start, end)]
        t0 = time.time()
        try:
            client.upsert(args.collection, rows,
                          partition_name=pk_partition(args, start))
            stats.ok((time.time() - t0) * 1000)
        except MilvusException as e:
            stats.fail(e)
        except Exception as e:  # noqa: BLE001 - keep the loop alive
            stats.fail(e)
        cursor = 0 if end >= args.rows else end
        # pace to the QA-measured rate (~0.45 batch/s); 0 = as fast as possible
        pause = args.upsert_interval - (time.time() - t0)
        if pause > 0:
            STOP.wait(pause)


def delete_worker(client, args, stats, worker_id):
    """Optional accelerator: explicit delete storm.

    Pure deletes are much cheaper per row than upserts (no vector payload),
    so this fattens the delegator delete buffer several-fold -> each fresh
    compacted segment replays a much larger batch -> the tsafe-freezing
    apply crosses the 3s stall threshold sooner. Rows deleted here are
    re-inserted by the upsert sweep on its next pass.
    """
    # offset the cursor half a PK-space away from the upsert sweep so we
    # mostly delete rows that exist (not ones just rewritten)
    cursor = ((args.rows // 2 + (args.rows // 4) * worker_id)
              // args.nb) * args.nb
    while not STOP.is_set():
        start = cursor % args.rows
        end = min(start + args.nb, args.rows)
        t0 = time.time()
        try:
            client.delete(args.collection,
                          ids=list(range(start, end)),
                          partition_name=pk_partition(args, start))
            stats.ok((time.time() - t0) * 1000)
        except MilvusException as e:
            stats.fail(e)
        except Exception as e:  # noqa: BLE001
            stats.fail(e)
        cursor = 0 if end >= args.rows else end
        pause = args.delete_interval - (time.time() - t0)
        if pause > 0:
            STOP.wait(pause)


def query_worker(client, args, stats):
    """Strong-consistency count(*) in a tight loop (the victim)."""
    while not STOP.is_set():
        t0 = time.time()
        try:
            client.query(args.collection, filter="",
                         output_fields=["count(*)"],
                         consistency_level="Strong",
                         timeout=args.query_timeout)
            stats.ok((time.time() - t0) * 1000)
        except MilvusException as e:
            stats.fail(e)
        except Exception as e:  # noqa: BLE001
            stats.fail(e)
        # pace to the QA-measured rate (~0.45 qps total); 0 = tight loop
        pause = args.query_interval - (time.time() - t0)
        if pause > 0:
            STOP.wait(pause)


def main():
    ap = argparse.ArgumentParser(description="issue #49435 repro driver")
    ap.add_argument("--uri", default="http://127.0.0.1:19530")
    ap.add_argument("--token", default="")
    ap.add_argument("--collection", default="issue49435_repro")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--nb", type=int, default=2000)
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--shards", type=int, default=2)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--upsert-workers", type=int, default=1)
    ap.add_argument("--query-workers", type=int, default=2)
    ap.add_argument("--upsert-interval", type=float, default=2.0,
                    help="seconds per upsert batch per worker; QA-measured "
                         "steady state is ~0.45 batch/s total (2.2s). 0=max")
    ap.add_argument("--query-interval", type=float, default=4.0,
                    help="seconds per query per worker; with 2 workers this "
                         "matches the QA-measured ~0.45 qps. 0=max")
    ap.add_argument("--delete-workers", type=int, default=0,
                    help="optional accelerator: explicit delete-storm "
                         "workers (see delete_worker docstring)")
    ap.add_argument("--delete-interval", type=float, default=1.0)
    ap.add_argument("--query-timeout", type=float, default=60.0)
    ap.add_argument("--duration", type=int, default=0,
                    help="seconds to run after fill; 0 = forever")
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument("--skip-fill", action="store_true")
    args = ap.parse_args()

    random.seed(49435)
    np.random.seed(49435)
    signal.signal(signal.SIGINT, lambda *a: STOP.set())
    signal.signal(signal.SIGTERM, lambda *a: STOP.set())

    client = MilvusClient(uri=args.uri, token=args.token or None)
    build_collection(client, args)

    up_stats, q_stats = Stats("upsert"), Stats("strong-count(*)")
    del_stats = Stats("delete")
    if not args.skip_fill:
        initial_fill(client, args, up_stats)

    workers = []
    for i in range(args.upsert_workers):
        # one client per worker: pymilvus client is not thread-safe enough
        c = MilvusClient(uri=args.uri, token=args.token or None)
        workers.append(threading.Thread(
            target=upsert_worker, args=(c, args, up_stats, i), daemon=True))
    for i in range(args.delete_workers):
        c = MilvusClient(uri=args.uri, token=args.token or None)
        workers.append(threading.Thread(
            target=delete_worker, args=(c, args, del_stats, i), daemon=True))
    for _ in range(args.query_workers):
        c = MilvusClient(uri=args.uri, token=args.token or None)
        workers.append(threading.Thread(
            target=query_worker, args=(c, args, q_stats), daemon=True))
    for w in workers:
        w.start()
    print(f"running: upsert_workers={args.upsert_workers} "
          f"query_workers={args.query_workers} (Ctrl-C to stop)")

    deadline = time.time() + args.duration if args.duration else None
    total_errs = 0
    try:
        while not STOP.is_set():
            time.sleep(10)
            print(time.strftime("%H:%M:%S"), up_stats.snapshot_and_reset())
            if args.delete_workers:
                print(time.strftime("%H:%M:%S"),
                      del_stats.snapshot_and_reset())
            print(time.strftime("%H:%M:%S"), q_stats.snapshot_and_reset())
            with q_stats.lock:
                for ts, msg in q_stats.err_samples:
                    print(f"   [query-err @{ts}] {msg}")
                    total_errs += 1
                q_stats.err_samples.clear()
            if deadline and time.time() > deadline:
                STOP.set()
    finally:
        STOP.set()
        print(f"done. total sampled query errors: {total_errs}")
        sys.exit(0)


if __name__ == "__main__":
    main()
