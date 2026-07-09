# issue #49435 debug kit — tsafe stall / LoadDeletedRecord decomposition

Branch `debug/issue-49435-delete-apply-probe` instruments the delete-apply
path (`LoadDeletedRecord` → `LoadPush` → `search_batch_pks`) that holds the
delegator `deleteMut.RLock` during post-load delete replay and starves
`ProcessDelete`/`UpdateTSafe` (→ `code=505 channel tsafe stalled`).

## What the instrumentation adds

1. **Per-step Prometheus histograms** (C++ side, near-zero overhead,
   one observation set per `LoadDeletedRecord` call):

   | metric | meaning |
   |---|---|
   | `issue49435_delete_apply_step_latency{step="total"}` | whole CGO call (ms) |
   | `...{step="parse"}` | ParsePksFromIDs |
   | `...{step="pin_ts"}` | one-time TS column GetAllChunks |
   | `...{step="pin_pk"}` | one-time PK column GetAllChunks |
   | `...{step="scan"}` | binary-search loop (per-PK lower_bound + read_ts filter) |
   | `...{step="read_insert_ts"}` | per-matched-row insert_ts read (`get_insert_timestamp_func_` → per-row `ts_col->GetChunk`) |
   | `...{step="skiplist_insert"}` | per-matched-row folly ConcurrentSkipList insert + mask set |
   | `issue49435_delete_apply_matched_rows` | rows matched per call |

2. **One INFO log line per call** (low volume: ~1/compaction):
   `grep "\[DeleteApplyProbe\]"` → same numbers, for correlating with
   per-call log brackets in Loki.

## Grafana queries

Per-step average ms/call over time:

```promql
increase(issue49435_delete_apply_step_latency_sum{pod="$POD"}[2m])
/ clamp_min(increase(issue49435_delete_apply_step_latency_count{pod="$POD"}[2m]), 1)
```
(legend: `{{step}}` — one curve per step; the biggest curve is the answer.)

p99 per step:

```promql
histogram_quantile(0.99, sum by (le, step)
  (rate(issue49435_delete_apply_step_latency_bucket{pod="$POD"}[5m])))
```

Calls that individually exceeded 10s:

```promql
increase(issue49435_delete_apply_step_latency_count{step="total",pod="$POD"}[5m])
- increase(issue49435_delete_apply_step_latency_bucket{step="total",le="10000",pod="$POD"}[5m])
```

## Reproduction driver

`repro_upsert_strong_query.py` mirrors the QA fouram case
(sift-2M, 2 shards, 16 partitions, upsert nb=2000 cycling the same PKs,
concurrent Strong `count(*)`):

```bash
pip install pymilvus numpy
python3 repro_upsert_strong_query.py --uri http://<milvus>:19530
# after the initial 2M fill it re-upserts forever; failure signature:
#   MilvusException code=505 ... channel tsafe stalled
```

Suggested server sizing to reproduce: standalone, **8 CPU / 16-17 GB**
limits (the bug is resource-starvation-driven; oversized nodes reproduce
more slowly or not at all).

### Timing & acceleration

QA-faithful defaults (upsert 0.5 batch/s, query 0.5 qps) reproduce in
**~1.5-2h** from cold start (measured on the 07-08 QA run: first
LoadDeletedRecord>3s spike 1h45m after pod start, recurring hourly).

The gates are (a) treadmill cadence: one full PK sweep before segments
become mostly-dead (2M / 900 rows/s = 37 min), (b) buffered-delete volume
per fresh-segment replay, (c) starvation onset (CPU near the 8-core cap +
memory climbing past ~8 GB). A 505 only needs the apply stretched past the
**3s** tsafe stall timeout — not the full 26s.

Accelerated invocation targeting **first 505 in ~30-45 min**:

```bash
python3 repro_upsert_strong_query.py --uri http://<milvus>:19530 \
  --upsert-interval 0.3 \                   # sweep 2M in ~5 min (7x treadmill)
  --query-workers 4 --query-interval 0.5 \  # ~8 qps cold count(*) scans
  --delete-workers 1 --delete-interval 1.0  # fatten the delete buffer
```

Escalation order if still quiet after ~1h: `--upsert-interval 0` (WAL
backpressure becomes the natural limiter) → drop server CPU limit to 6 →
`--rows 1000000` (halves the sweep, keeps ~200K-row segments).

## Closed loop

1. Deploy this branch's image, run the driver → collect
   `issue49435_delete_apply_step_latency` during 505 bursts →
   the dominant `step` is the verified root cause inside the 20-30s call.
2. Apply the fix (delegator lock hoist + segcore per-row read elimination).
3. Re-run the same driver → 505 gone AND `step` latencies back to ms-level
   = fix verified with the same evidence chain.
