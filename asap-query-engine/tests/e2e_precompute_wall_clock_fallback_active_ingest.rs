//! End-to-end test: JSON file ingestion of a bulk one-shot batch whose rows
//! all share one event-time (so the watermark never advances) must not lose
//! rows even when ingestion takes longer, in real wall-clock time, than
//! `window_size_ms + wall_clock_grace_period_ms`. The wall-clock fallback in
//! `flush_all` exists to force-close a window when event-time stagnates, but
//! it must only do so once a pane has gone genuinely idle — never while a
//! pane is still actively receiving samples, however long it's been open.
//! (A prior bug aged a pane by time-since-*first-touch* instead of
//! time-since-*last-touch*, so it force-closed the window mid-ingest and
//! every row arriving afterward hit the hardcoded-Drop late-data path — a
//! silent, uniform undercount.)
//!
//! This drives the real `JsonFileIngestSource → PrecomputeEngine → sink`
//! path, not `Worker` directly, to catch wiring gaps the `worker.rs` unit
//! test (`wall_clock_fallback_does_not_close_a_pane_still_receiving_samples`)
//! can't — e.g. `PrecomputeEngineConfig` not actually reaching the workers.
//! That unit test remains the precise, fully deterministic proof of the
//! fallback's behavior; this test is a wiring sanity check on top, and
//! relies on two things to be deterministic-by-construction rather than
//! best-effort:
//!
//! 1. `PrecomputeEngine::with_now_ms_fn` (a real, not `#[cfg(test)]`-gated,
//!    test-only hook) — used here with an *unscaled* real clock (no
//!    multiplier). An earlier version of this test scaled real elapsed time
//!    up 50x to reach a multi-second-equivalent deadline in tens of real
//!    milliseconds, but that amplifies ordinary scheduling jitter right
//!    along with intentional delay — a batch arriving a few tens of ms late
//!    (a common, harmless hiccup under load) reads as *seconds* of
//!    simulated time, enough to spuriously cross the deadline between two
//!    consecutive touches. Using the real clock as-is with seconds-scale
//!    delays (below) avoids that: jitter stays a small fraction of the
//!    margin instead of dominating it.
//! 2. `JsonFileIngestConfig::batch_delay_ms` — an ordinary throttling knob
//!    (not test-only; it just has no other caller yet) set here to a real,
//!    explicit delay after each batch send, so ingest is *guaranteed* (not
//!    merely likely, given incidental scheduling overhead) to span real
//!    wall-clock time. Combined with a short `flush_interval_ms`, the
//!    periodic flush timer is given many real chances to land mid-ingest,
//!    not just one narrow shot.
//!
//! Timing margins (deliberately generous, not tight):
//! - deadline (window_size_ms + wall_clock_grace_period_ms) = 3000ms.
//! - batch_delay_ms = 400ms nominal per-gap, ~7.5x under the deadline — even
//!   a few hundred ms of scheduling jitter on any single gap is absorbed.
//! - 10 batches x 400ms ≈ 4000ms total ingest span, comfortably past the
//!   3000ms deadline, so the old birth-time bug (were it still present)
//!   would have real room to force-close mid-stream and drop later batches.
//!
//! This trades test runtime (~4-5 real seconds) for robustness against jitter.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, WindowType};

use query_engine_rust::data_model::StreamingConfig;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::CapturingOutputSink;
use query_engine_rust::precompute_engine::{
    IngestSource, JsonFileIngestConfig, JsonFileIngestSource, PrecomputeEngine, TimestampUnit,
};
use query_engine_rust::precompute_operators::sum_accumulator::SumAccumulator;

fn netflow_agg_config(metric: &str, window_size_ms: u64) -> AggregationConfig {
    AggregationConfig::new(
        1,
        AggregationType::SingleSubpopulation,
        "Sum".to_string(),
        HashMap::new(),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_size_ms,
        0,
        WindowType::Tumbling,
        metric.to_string(),
        metric.to_string(),
        None,
        None,
        None,
        None,
    )
}

#[tokio::test]
async fn bulk_single_timestamp_ingest_is_not_dropped_while_still_active() {
    // --- Synthesize a NetFlow JSONL where every record shares one second,
    // spread across enough batches to give real ingest measurable span.
    let num_rows = 20;
    let rows: Vec<(&str, String, String, f64)> = (0..num_rows)
        .map(|i| {
            (
                "2024-06-01 12:00:00",
                format!("10.0.0.{}", i * 2 + 1),
                format!("10.0.0.{}", i * 2 + 2),
                100.0 + i as f64,
            )
        })
        .collect();
    let expected_total_bytes: f64 = rows.iter().map(|r| r.3).sum();

    let path = std::env::temp_dir().join(format!(
        "wall_clock_fallback_active_ingest_{}.jsonl",
        std::process::id()
    ));
    {
        let mut f = std::fs::File::create(&path).expect("create temp netflow file");
        for (ts, src, dst, bytes) in rows.iter() {
            writeln!(
                f,
                r#"{{"timestamp":"{ts}","src_ip":"{src}","dst_ip":"{dst}","bytes":{bytes}}}"#
            )
            .unwrap();
        }
    }

    // --- Wire up the real engine: 1s tumbling Sum over `bytes`.
    let metric = "netflow_bytes";
    let mut agg_map = HashMap::new();
    agg_map.insert(1u64, netflow_agg_config(metric, 1_000));
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let json_cfg = JsonFileIngestConfig {
        path: path.to_string_lossy().to_string(),
        metric_name: metric.to_string(),
        value_col: "bytes".to_string(),
        label_cols: vec![], // aggregate all flows in the second together
        timestamp_col: "timestamp".to_string(),
        timestamp_unit: TimestampUnit::Seconds,
        batch_size: 2, // 20 rows -> 10 batches, each a separate worker touch
        // Force real wall-clock separation between batches so ingest is
        // *guaranteed* (not merely likely) to span the flush timer below.
        // See the module doc comment for the margin reasoning.
        batch_delay_ms: 400,
    };

    let sink = Arc::new(CapturingOutputSink::new());
    let sources: Vec<Box<dyn IngestSource>> = vec![Box::new(JsonFileIngestSource::new(json_cfg))];

    let engine_config = PrecomputeEngineConfig {
        num_workers: 1,
        // Every flush_all() call unconditionally nudges the event-time
        // watermark forward by 1ms (the "+1ms boundary advance" that lets an
        // idle stream make progress), independent of the wall-clock
        // fallback this test targets. With multiple batches spaced across
        // several flush ticks, allowed_lateness_ms=0 would make that 1ms
        // drift alone mark the next same-timestamp batch "late" and drop it
        // via a completely different code path — not the bug under test.
        // A generous, production-realistic value (default is 5000ms) keeps
        // that unrelated mechanism from firing during this test's ~4s span.
        allowed_lateness_ms: 10_000,
        max_buffer_per_series: 10_000,
        // Real flush ticks every 50ms, many chances to land during the
        // ~4000ms (10 batches x 400ms) real span of ingest.
        flush_interval_ms: 50,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
        // window_size_ms (1000) + this (2000) = a 3000ms deadline.
        wall_clock_grace_period_ms: 2_000,
    };

    // Fake clock anchored to a real Instant, unscaled — see the module doc
    // comment for why this must NOT be amplified (jitter amplifies with it).
    let start = Instant::now();
    let engine = PrecomputeEngine::new(engine_config, streaming_config, sink.clone(), sources)
        .with_now_ms_fn(move || 1_000_000 + start.elapsed().as_millis() as i64);

    engine.run().await.expect("engine run failed");

    let _ = std::fs::remove_file(&path);

    // --- Verify: no row was lost to a premature force-close, whether the
    // window closed via a mid-ingest flush tick or the shutdown force-close.
    let captured = sink.drain();
    assert_eq!(
        captured.len(),
        1,
        "the single-second window must be emitted exactly once (got {} outputs)",
        captured.len()
    );

    let (output, acc) = &captured[0];
    assert_eq!(
        output.end_timestamp - output.start_timestamp,
        1_000,
        "expected a 1s window, got [{}, {})",
        output.start_timestamp,
        output.end_timestamp
    );

    let sum_acc = acc
        .as_any()
        .downcast_ref::<SumAccumulator>()
        .expect("NetFlow Sum aggregation should emit a SumAccumulator");
    assert!(
        (sum_acc.sum - expected_total_bytes).abs() < 1e-9,
        "window must hold the summed bytes of all 8 rows with none dropped \
         to a premature force-close: expected {}, got {}",
        expected_total_bytes,
        sum_acc.sum
    );
}
