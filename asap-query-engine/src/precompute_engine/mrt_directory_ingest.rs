use super::derived_value::DerivedValueConfig;
use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::computed_labels::ComputedLabelConfig;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use crate::precompute_engine::mrt_ingest::{
    build_row_expansion_config, ingest_one_mrt_file_blocking,
};
use crate::precompute_engine::row_expansion::RowExpansionConfig;
use crate::precompute_engine::stateful_transition::StatefulTransitionConfig;
use futures::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tracing::{info, warn};

/// Ingests every MRT file in a directory, then keeps polling for new ones
/// forever - the same mechanism whether the directory is a live collector
/// drop point (new 5-minute dumps keep appearing) or a folder of already-
/// downloaded historical files (the poll loop just finds nothing new after
/// the first pass and idles). Unifying these two cases is the point: a
/// user doing historical analysis gets the same precompute-amortized
/// querying a live deployment gets, just by pointing this at a folder
/// instead of waiting for a feed.
pub struct MrtDirectoryIngestConfig {
    pub dir_path: String,
    pub metric_name: String,
    /// RIS/RouteViews collector name (e.g. "rrc00") - see
    /// `MrtFileIngestConfig::collector` for why this isn't derived from the
    /// files themselves.
    pub collector: String,
    pub computed_label_cols: HashMap<String, ComputedLabelConfig>,
    pub stateful_transitions: Vec<StatefulTransitionConfig>,
    pub derived_value_cols: Vec<DerivedValueConfig>,
    pub batch_size: usize,
    /// How often to re-scan the directory for files not yet ingested.
    pub poll_interval_ms: u64,
}

pub struct MrtDirectoryIngestSource {
    config: MrtDirectoryIngestConfig,
}

impl MrtDirectoryIngestSource {
    pub fn new(config: MrtDirectoryIngestConfig) -> Self {
        Self { config }
    }
}

/// Lists `dir_path` for regular files not already in `seen`, sorted by
/// filename (RIS/RouteViews dump names are zero-padded date/time-stamped,
/// so lexicographic order is chronological order) and old enough to be
/// unlikely still mid-download. Hidden files (leading `.`) are skipped -
/// the usual convention for in-progress downloads and OS metadata files.
///
/// The "old enough" check (skip anything modified within the last
/// `poll_interval_ms`) is a deliberately simple safeguard against reading a
/// file a collector script is still writing to, not a real lock/rename
/// protocol - a source that needs stronger guarantees should have its
/// downloader write under a temp name and rename into place atomically,
/// which this check does not require but works fine alongside.
fn discover_new_files(
    dir_path: &str,
    seen: &HashSet<PathBuf>,
    poll_interval_ms: u64,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now();
    let min_age = std::time::Duration::from_millis(poll_interval_ms);

    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if !entry.file_type()?.is_file() {
            continue;
        }
        if seen.contains(&path) {
            continue;
        }
        let is_hidden = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'));
        if is_hidden {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        let age = now.duration_since(modified).unwrap_or_default();
        if age < min_age {
            // Too recent - possibly still being written. Leave it for the
            // next poll.
            continue;
        }

        candidates.push(path);
    }

    candidates.sort();
    Ok(candidates)
}

#[async_trait::async_trait]
impl IngestSource for MrtDirectoryIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config;

        if !std::path::Path::new(&config.dir_path).is_dir() {
            return Err(std::io::Error::other(format!(
                "MRT directory ingest path '{}' is not a directory",
                config.dir_path
            ))
            .into());
        }

        let row_expansion_config = build_row_expansion_config(
            &config.metric_name,
            &config.computed_label_cols,
            &config.derived_value_cols,
        )?;

        info!(
            "MRT directory ingest watching {} (poll every {}ms)",
            config.dir_path, config.poll_interval_ms
        );

        let mut seen: HashSet<PathBuf> = HashSet::new();
        let poll_interval = std::time::Duration::from_millis(config.poll_interval_ms);

        // Runs forever, like HttpIngestSource - a directory watch has no
        // natural end. A purely historical folder just settles into
        // finding nothing new every poll, which is a cheap no-op; a live
        // collector drop point keeps being picked up for as long as the
        // process runs. Window materialization for a folder that never
        // grows again relies on the engine's own wall_clock_grace_period_ms
        // fallback (see PrecomputeSettings), not on this source ever
        // deciding "done" and shutting down - it doesn't have a way to know
        // that.
        loop {
            let new_files =
                discover_new_files(&config.dir_path, &seen, config.poll_interval_ms)?;

            if !new_files.is_empty() {
                info!(
                    "MRT directory ingest found {} new file(s) in {}",
                    new_files.len(),
                    config.dir_path
                );
            }

            for path in new_files {
                let path_str = path.to_string_lossy().to_string();
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(8);

                let collector = config.collector.clone();
                let row_expansion_config_clone = row_expansion_config.clone();
                let batch_size = config.batch_size;
                let path_for_blocking = path_str.clone();
                let stateful_transitions = config.stateful_transitions.clone();
                let reader_handle = tokio::task::spawn_blocking(move || {
                    ingest_one_mrt_file_blocking(
                        &path_for_blocking,
                        &collector,
                        &row_expansion_config_clone,
                        &stateful_transitions,
                        batch_size,
                        &tx,
                    )
                });

                let mut file_samples: u64 = 0;
                while let Some(batch) = rx.recv().await {
                    file_samples += batch.len() as u64;
                    route_decoded_samples(&ctx, batch, Instant::now()).await?;
                }

                match reader_handle.await {
                    Ok(Ok((elems, errors))) => {
                        info!(
                            "MRT directory ingest: {} - {} elements, {} malformed records skipped, {} samples routed",
                            path_str, elems, errors, file_samples
                        );
                    }
                    Ok(Err(e)) => {
                        // A whole file failing to open/parse (corrupt
                        // download, truncated transfer) shouldn't take down
                        // an indefinitely-running directory watch - log and
                        // move on to the next file, same log-and-skip
                        // philosophy as per-record errors within a file.
                        warn!("Skipping unreadable MRT file {}: {}", path_str, e);
                    }
                    Err(e) => {
                        warn!("MRT parse task for {} panicked: {}", path_str, e);
                    }
                }

                seen.insert(path);
            }

            // Materialize whatever just landed so it's queryable promptly,
            // rather than waiting solely on the wall-clock grace period.
            ctx.router.broadcast_flush().await?;

            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Ingests every MRT file already present in a directory exactly once, then
/// flushes and shuts the router down - unlike `MrtDirectoryIngestSource`,
/// which polls forever for new arrivals. For a folder of already-downloaded
/// historical files (e.g. a full month pulled from a RIS/RouteViews
/// archive), "watch forever" has no natural end and the caller has no signal
/// to know ingest is done; this variant gives batch callers (offline
/// backfills, evaluation harnesses) a real completion signal to wait on,
/// the same way `MrtFileIngestSource` does for a single file.
///
/// Files are ingested with bounded concurrency (`concurrency` files'
/// `spawn_blocking` tasks in flight at once) rather than one at a time,
/// since a month of 5-minute dumps is thousands of small files and this
/// machine's core count is the actual constraint, not I/O.
///
/// When `pace_interval_ms` is set, this instead ingests files strictly
/// sequentially, sleeping between them so that files arrive at a controlled
/// rate rather than as fast as the machine can process them - a faithful
/// (optionally accelerated) replay of RIPE RIS/RouteViews' real publishing
/// cadence (one 5-minute MRT dump per collector, every 5 minutes) instead of
/// a burst-load benchmark. `concurrency` is ignored in this mode.
pub struct MrtBatchDirectoryIngestConfig {
    pub dir_path: String,
    pub metric_name: String,
    pub collector: String,
    pub computed_label_cols: HashMap<String, ComputedLabelConfig>,
    pub stateful_transitions: Vec<StatefulTransitionConfig>,
    pub derived_value_cols: Vec<DerivedValueConfig>,
    pub batch_size: usize,
    /// Max number of files being parsed concurrently. Ignored when
    /// `pace_interval_ms` is set.
    pub concurrency: usize,
    /// If set, ingest files one at a time, sleeping this many milliseconds
    /// between the end of one file's ingest and the start of the next -
    /// simulating a real (optionally accelerated) collector arrival rate
    /// instead of ingesting the whole backlog at ceiling throughput.
    pub pace_interval_ms: Option<u64>,
    /// If set, only ingest the first `max_files` files (in the same sorted
    /// order `discover_new_files` returns), then flush/shutdown normally as
    /// if that were the whole directory - a deliberate, graceful early stop
    /// rather than ingesting everything. Exists for benchmark runs that need
    /// to finish and flush well inside an external time budget rather than
    /// race an unpredictable external kill partway through an unbounded run.
    pub max_files: Option<usize>,
}

pub struct MrtBatchDirectoryIngestSource {
    config: MrtBatchDirectoryIngestConfig,
}

impl MrtBatchDirectoryIngestSource {
    pub fn new(config: MrtBatchDirectoryIngestConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl IngestSource for MrtBatchDirectoryIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config;

        if !std::path::Path::new(&config.dir_path).is_dir() {
            return Err(std::io::Error::other(format!(
                "MRT batch directory ingest path '{}' is not a directory",
                config.dir_path
            ))
            .into());
        }

        let row_expansion_config = Arc::new(build_row_expansion_config(
            &config.metric_name,
            &config.computed_label_cols,
            &config.derived_value_cols,
        )?);

        let mut files = discover_new_files(&config.dir_path, &HashSet::new(), 0)?;
        if let Some(max_files) = config.max_files {
            files.truncate(max_files);
            info!(
                "MRT batch directory ingest: max_files={} - stopping after this many, not the full directory",
                max_files
            );
        }

        if let Some(pace_ms) = config.pace_interval_ms {
            return Self::run_paced(&ctx, &config, files, row_expansion_config, pace_ms).await;
        }

        info!(
            "MRT batch directory ingest: {} file(s) in {} (concurrency {})",
            files.len(),
            config.dir_path,
            config.concurrency
        );

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(64);
        let collector = config.collector.clone();
        let stateful_transitions = Arc::new(config.stateful_transitions.clone());
        let batch_size = config.batch_size;
        let concurrency = config.concurrency.max(1);
        let files_total = files.len();
        let files_done = Arc::new(AtomicUsize::new(0));
        let progress_start = Instant::now();

        let producer_handle = tokio::spawn(async move {
            stream::iter(files.into_iter())
                .map(|path| {
                    let tx = tx.clone();
                    let row_expansion_config = row_expansion_config.clone();
                    let collector = collector.clone();
                    let stateful_transitions = stateful_transitions.clone();
                    let files_done = files_done.clone();
                    async move {
                        let path_str = path.to_string_lossy().to_string();
                        let path_for_blocking = path_str.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            ingest_one_mrt_file_blocking(
                                &path_for_blocking,
                                &collector,
                                &row_expansion_config,
                                &stateful_transitions,
                                batch_size,
                                &tx,
                            )
                        })
                        .await;

                        let done = files_done.fetch_add(1, Ordering::Relaxed) + 1;
                        // Every 250 files (and always the last one) - frequent
                        // enough to estimate an ETA on an hours-long month-scale
                        // ingest, rare enough not to add its own overhead.
                        if done % 250 == 0 || done == files_total {
                            let elapsed = progress_start.elapsed().as_secs_f64();
                            let rate = done as f64 / elapsed.max(0.001);
                            let remaining = (files_total - done) as f64 / rate.max(0.001);
                            info!(
                                "MRT batch directory ingest progress: {}/{} files ({:.1}/s, ~{:.0}s remaining)",
                                done, files_total, rate, remaining
                            );
                        }

                        (path_str, result)
                    }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await
        });

        // route_decoded_samples does real per-sample CPU work (label
        // parsing, spatial-filter evaluation against every candidate
        // aggregation) - a single sequential consumer here was the actual
        // ingest throughput ceiling regardless of file-decode parallelism
        // above: `concurrency` files decode concurrently via spawn_blocking,
        // but every decoded batch, from all of them, used to funnel through
        // one task processing one batch at a time, so wall-clock throughput
        // never used more than one core no matter how many were free.
        // Spreading the same channel across `concurrency` consumer tasks
        // (behind a shared mutex - recv() itself is cheap, all the real
        // work happens after it returns) parallelizes the actual bottleneck.
        let total_samples = Arc::new(AtomicU64::new(0));
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let mut consumer_handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let rx = rx.clone();
            let ctx = ctx.clone();
            let total_samples = total_samples.clone();
            consumer_handles.push(tokio::spawn(async move {
                loop {
                    let batch = { rx.lock().await.recv().await };
                    let Some(batch) = batch else { break };
                    total_samples.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    route_decoded_samples(&ctx, batch, Instant::now()).await?;
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }));
        }

        let outcomes = producer_handle.await?;
        for handle in consumer_handles {
            handle.await??;
        }
        let total_samples = total_samples.load(Ordering::Relaxed);
        let mut total_elems: u64 = 0;
        let mut total_errors: u64 = 0;
        let mut files_failed: usize = 0;
        let files_total = outcomes.len();
        for (path_str, result) in outcomes {
            match result {
                Ok(Ok((elems, errors))) => {
                    total_elems += elems;
                    total_errors += errors;
                }
                Ok(Err(e)) => {
                    files_failed += 1;
                    warn!("Skipping unreadable MRT file {}: {}", path_str, e);
                }
                Err(e) => {
                    files_failed += 1;
                    warn!("MRT parse task for {} panicked: {}", path_str, e);
                }
            }
        }

        info!(
            "MRT batch directory ingest complete: {}/{} files ok, {} elements, {} malformed records skipped, {} samples routed",
            files_total - files_failed, files_total, total_elems, total_errors, total_samples
        );

        ctx.router.broadcast_flush().await?;
        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

impl MrtBatchDirectoryIngestSource {
    /// Ingests `files` strictly one at a time, sleeping `pace_interval_ms`
    /// between them, instead of the bounded-concurrency ceiling-throughput
    /// path above. Flushes after every file so newly-landed data is
    /// promptly queryable by a concurrently-running dashboard, matching
    /// `MrtDirectoryIngestSource`'s per-poll flush behavior - the whole
    /// point of pacing is to look like a live feed, not a fast batch job.
    async fn run_paced(
        ctx: &IngestContext,
        config: &MrtBatchDirectoryIngestConfig,
        files: Vec<PathBuf>,
        row_expansion_config: Arc<RowExpansionConfig>,
        pace_interval_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let files_total = files.len();
        let pace = std::time::Duration::from_millis(pace_interval_ms);
        info!(
            "MRT paced directory ingest: {} file(s) in {} (pace {}ms/file)",
            files_total, config.dir_path, pace_interval_ms
        );

        let mut total_samples: u64 = 0;
        let mut total_elems: u64 = 0;
        let mut total_errors: u64 = 0;
        let mut files_failed: usize = 0;

        for (idx, path) in files.into_iter().enumerate() {
            let path_str = path.to_string_lossy().to_string();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(64);
            let collector = config.collector.clone();
            let row_expansion_config_clone = row_expansion_config.clone();
            let batch_size = config.batch_size;
            let path_for_blocking = path_str.clone();
            let file_start = Instant::now();

            let stateful_transitions = config.stateful_transitions.clone();
            let reader_handle = tokio::task::spawn_blocking(move || {
                ingest_one_mrt_file_blocking(
                    &path_for_blocking,
                    &collector,
                    &row_expansion_config_clone,
                    &stateful_transitions,
                    batch_size,
                    &tx,
                )
            });

            let mut file_samples: u64 = 0;
            while let Some(batch) = rx.recv().await {
                file_samples += batch.len() as u64;
                total_samples += batch.len() as u64;
                route_decoded_samples(ctx, batch, Instant::now()).await?;
            }
            let _ = file_samples;

            match reader_handle.await {
                Ok(Ok((elems, errors))) => {
                    total_elems += elems;
                    total_errors += errors;
                }
                Ok(Err(e)) => {
                    files_failed += 1;
                    warn!("Skipping unreadable MRT file {}: {}", path_str, e);
                }
                Err(e) => {
                    files_failed += 1;
                    warn!("MRT parse task for {} panicked: {}", path_str, e);
                }
            }

            ctx.router.broadcast_flush().await?;

            let done = idx + 1;
            let file_elapsed = file_start.elapsed();
            if done % 250 == 0 || done == files_total {
                let remaining_files = files_total - done;
                let remaining_secs = remaining_files as f64 * (pace_interval_ms as f64 / 1000.0);
                info!(
                    "MRT paced directory ingest progress: {}/{} files (~{:.0}s of paced time remaining), {} samples routed so far",
                    done, files_total, remaining_secs, total_samples
                );
            }

            if file_elapsed >= pace {
                warn!(
                    "MRT paced directory ingest fell behind pace on {}: took {:?}, pace interval is {:?} - reduce the acceleration factor or increase pace_interval_ms",
                    path_str, file_elapsed, pace
                );
            } else {
                tokio::time::sleep(pace - file_elapsed).await;
            }
        }

        info!(
            "MRT paced directory ingest complete: {}/{} files ok, {} elements, {} malformed records skipped, {} samples routed",
            files_total - files_failed, files_total, total_elems, total_errors, total_samples
        );

        ctx.router.broadcast_flush().await?;
        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_only_new_stable_visible_files_sorted_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let old_enough = std::time::Duration::from_millis(0);

        std::fs::write(dir.path().join("updates.20240103.0005.bz2"), b"b").unwrap();
        std::fs::write(dir.path().join("updates.20240103.0000.bz2"), b"a").unwrap();
        std::fs::write(dir.path().join(".DS_Store"), b"hidden").unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();

        let files = discover_new_files(dir.path().to_str().unwrap(), &HashSet::new(), 0).unwrap();
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert_eq!(
            names,
            vec!["updates.20240103.0000.bz2", "updates.20240103.0005.bz2"]
        );
        let _ = old_enough;
    }

    #[test]
    fn already_seen_files_are_not_rediscovered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("updates.20240103.0000.bz2");
        std::fs::write(&path, b"a").unwrap();

        let mut seen = HashSet::new();
        seen.insert(path.clone());

        let files = discover_new_files(dir.path().to_str().unwrap(), &seen, 0).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn very_recently_modified_files_are_deferred() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("updates.20240103.0000.bz2"), b"a").unwrap();

        // A large min-age means the just-written file looks "too fresh".
        let files = discover_new_files(dir.path().to_str().unwrap(), &HashSet::new(), 60_000).unwrap();
        assert!(files.is_empty());
    }
}

