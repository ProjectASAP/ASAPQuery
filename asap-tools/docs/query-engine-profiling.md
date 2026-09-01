# Query engine profiling

Set `profiling.query_engine: true` and
`use_container.query_engine: false` for a Rust query-engine profile.

The remote workflow builds and launches the separate
`target/release/query_engine_rust_fp` binary, records compact frame-pointer
call chains with `perf record`, and converts the recording to a symbolized
`perf.script` with `perf script --header` on the remote node. FP makes this
conversion fast enough to complete before the experiment output is rsynced.

After `experiment_run_e2e.py` rsyncs the experiment output back locally, run:

```bash
./asap-tools/experiments/postprocess_query_engine_profiles.sh \
  experiment_outputs/<experiment>/sketchdb/query_engine_profiles
```

This reuses the remote `.script` file when present and produces, for each
`perf_*.data` file:

- `.script`: symbolized Perf samples;
- `.folded`: collapsed stacks for Speedscope or FlameGraph;
- `.svg`: a browser-viewable flamegraph.

The script uses `/tmp/FlameGraph` by default. Pass a different FlameGraph
directory as its second argument when needed.
