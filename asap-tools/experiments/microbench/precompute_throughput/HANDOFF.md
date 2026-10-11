# Precompute-engine throughput microbenchmark: how to run

Measures throughput (samples/s) vs engine CPU of the standalone
`precompute_engine` for several StreamingConfigs and input cardinalities.
No Prometheus involved: a dedicated load generator speaks remote-write
directly to the engine.

## Pieces

| File | Role |
|---|---|
| `asap-query-engine/src/bin/bench_ingest_sender.rs` | Load generator (separate process). Pre-encodes remote-write bodies on its own threads, 16 senders each owning a disjoint slice of series with one request in flight, optional rate limit. Prints one JSON line: acked samples/s, engine CPU cores, sender CPU cores, engine peak RSS. |
| `asap-query-engine/src/bin/precompute_engine.rs` | Engine under test. Benchmark relies on `--num-aggregates-to-retain` (added for this) and `--wall-clock-grace-period-ms`. |
| `configs/{kll,ddsketch,cms_heap,hydra_kll}.yaml` | The four pipelines (10s tumbling windows). |
| `sweep.py` | Runs one experiment: per point, starts a fresh engine + sender, appends a JSON line to `--out`, engine log to `results/logs/`. |
| `plot.py` | One figure per experiment. `main` is sized for one column of a two-column paper. |

Experiments in `sweep.py`:

- `main`: 4 pipelines (+ `raw`, see caveats) x {1K, 10K, 100K, 1M} series, 8 cores. Open-loop saturation run, then 10/25/50/75/90% of it. 120 points, ~1.5 h.
- `fanin`: KLL at 100K series, grouped by all labels / 2 labels / none. 3 points.
- `window`: KLL and DDSketch at 100K, tumbling 10s vs sliding 60s/1s. 4 points.
- `scaling`: KLL and HydraKLL at 1M, 1/2/4/8/14 cores (workers = cores). 10 points.

Each point = 15s warmup + 30s measurement + startup, ~50s.

## Where it ran

- Node: `milindsr@node5.scratch3.cloudmigration-PG0.clemson.cloudlab.us`
  (2x 14-core Xeon E5-2683 v3, 256 GB). Socket 0 = even CPUs, socket 1 = odd;
  hyperthread sibling of CPU n is n+28.
- Code there: `/scratch/sketch_db_for_prometheus/code` (plain rsync copy, not a
  git checkout; no `ASAPQuery/` subdir). Cargo is at `~/.cargo/bin/cargo` (not on
  the non-login PATH).
- Results there: `.../asap-tools/experiments/microbench/precompute_throughput/results/`.
  Copied locally to the same path in this repo.

## Steps

1. Sync only the benchmark files (the rest of the remote tree matched local;
   check with `rsync -anci` before overwriting anything else):

   ```bash
   H=milindsr@node5.scratch3.cloudmigration-PG0.clemson.cloudlab.us
   R=/scratch/sketch_db_for_prometheus/code
   rsync -avR asap-query-engine/Cargo.toml \
     asap-query-engine/src/bin/precompute_engine.rs \
     asap-query-engine/src/bin/bench_ingest_sender.rs \
     asap-tools/experiments/microbench/precompute_throughput/{sweep.py,plot.py,configs} \
     $H:$R/
   ```

2. Build on the node:

   ```bash
   ssh $H "cd $R/asap-query-engine && ~/.cargo/bin/cargo build --release --bin precompute_engine --bin bench_ingest_sender"
   ```

3. Run in tmux so it survives disconnects (engine on socket 0, one hyperthread
   per physical core; sender on socket 1):

   ```bash
   ssh $H "cd $R/asap-tools/experiments/microbench/precompute_throughput && mkdir -p results && \
     tmux new-session -d -s pcbench 'for e in fanin window scaling main; do \
       python3 -u sweep.py --bin-dir ../../../../target/release \
         --engine-cpus \$(seq -s, 0 2 26) --sender-cpus 1-55:2 \
         --experiment \$e --out results/\$e.jsonl > results/\$e.out 2>&1; \
       echo EXIT=\$? >> results/\$e.out; done; sleep 86400'"
   ```

   Run `fanin` alone first as a smoke test (~3 min): it fails loudly if a
   config is rejected. Progress: `tail results/<exp>.out`. `--out` appends, so
   move old `.jsonl` files aside before re-running an experiment.

4. Plot locally after copying results back:

   ```bash
   grep -v '"label": "raw"' results/main.jsonl > results/main_noraw.jsonl
   python3 plot.py --input results/main_noraw.jsonl --experiment main --out results/main.pdf
   python3 plot.py --input results/fanin.jsonl   --experiment fanin   --out results/fanin.png
   python3 plot.py --input results/window.jsonl  --experiment window  --out results/window.png
   python3 plot.py --input results/scaling.jsonl --experiment scaling --out results/scaling.png
   ```

## Settings that matter, and why (each fixed a real problem)

- **Engine pinned to N cores for N workers** (`--engine-cpus`, first N used).
  `--num-workers` alone doesn't bound CPU: decode/routing run on the async
  runtime across every allowed CPU (4 workers used ~16 cores unpinned).
- **`NUM_AGGREGATES_TO_RETAIN = 1`.** The store keeps retain x 4 windows per
  key (`per_key.rs` `max_epochs`). At 10, KLL at 100K series hit 34 GB; 1M
  series would exceed the node's RAM. The standalone binary otherwise retains
  without bound.
- **`--wall-clock-grace-period-ms 0`.** The engine's wall-clock fallback
  force-closes panes idle for window+grace of wall time. With per-series
  groups at low rates, one pass over 1M series can exceed that, silently
  dropping samples that the sender still counts as acked.
- **16 senders always, 2000 samples/request max.** The engine handles each
  request mostly serially, so in-flight requests must exceed engine cores. With
  1 sender at 1K series, measured throughput was 0.13 M/s instead of 1.40 M/s.
  More than 16 didn't help at 100K (16 vs 50 senders: same 0.73 M/s).
- **Sender lockstep (`MAX_PASS_SKEW = 2`).** Groups span senders and the engine
  keeps one watermark per group, so a sender running ahead would make others'
  samples late. Skew x step (2s) must stay below `--allowed-lateness-ms`
  (5000).
- **Event time advances 1s per pass over all series, independent of wall
  clock**, so window closes per sample are constant across send rates.

## Sanity checks to look at in results

- `sender_cpu_cores` should be well below the sender's 28 threads; if near it,
  the sender is the bottleneck.
- Rate-limited points should hit their target (`achieved_rate` ≈
  `target_rate`).
- `engine_peak_rss_mb` vs node RAM at 1M series (KLL peaked ~11.5 GB).

## Caveats / known issues

- **`raw` is not an overhead floor.** `--pass-raw-samples` stores every
  sample individually and is ~7x slower than KLL. It's excluded from the main
  plot; a `Sum` pipeline would be a real floor. Consider removing `raw` from
  `main_experiment` to save ~25 min.
- **Single run per point.** CPU at low load is noisy (curves zigzag at
  1K/10K). Median of 3 runs would fix it (~3x time).
- **Per-series sketches leave cores idle** (KLL/DDSketch saturate at ~5 of 8
  cores at 100K+ series; KLL stops scaling past 8 cores). Not profiled.
  Suspect: the store has one RwLock per aggregation id
  (`stores/simple_map_store/per_key.rs`), and per-series grouping emits one
  output per series per window. Confirm with a discard-output sink or an
  off-CPU profile.
- **HydraKLL at 1K series is slow** because saturating a tiny series set makes
  event time race, so expensive window emits dominate. Benchmark artifact.
- **HydraKLL scaling drops at 14 cores**: only 10 groups, so at most 10
  workers have work and hashing can stack groups on one worker.
- When killing runs over ssh, don't `pkill -f bench_ingest_sender` inside an
  ssh command line that contains that string: it kills the ssh session itself.

## Superseded results (kept for reference, don't plot)

`fanin_retain10.*` (retention 10, unpinned), `fanin_28threads.*` (unpinned),
`main_aborted.*` (before the 16-sender fix).
