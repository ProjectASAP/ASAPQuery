// Standard library
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

// External crates
use clap::Parser;
use serde::Deserialize;

// Internal imports from QueryEngineRust
use promql_utilities::query_logics::enums::{AggregationType, QueryPatternType, Statistic};
use query_engine_rust::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use query_engine_rust::precompute_operators::*;

/// CLI Arguments
#[derive(Parser, Debug)]
#[command(name = "test_offline_precomputes")]
#[command(about = "Test offline precomputes for SimpleEngine functionality")]
struct Args {
    /// Path to the dumped precomputes file (.msgpack)
    #[arg(short, long)]
    input_file: PathBuf,

    /// Test mode: "merge", "query", or "both"
    #[arg(short, long, default_value = "both")]
    mode: String,

    /// Query pattern type for testing: "temporal", "spatial", or "temporal_spatial"
    #[arg(short, long, default_value = "temporal")]
    pattern_type: String,

    /// Aggregation type for merging (e.g., "Sum", "DatasketchesKLL", "DeltaSetAggregator")
    #[arg(short, long, default_value = "Sum")]
    aggregation_type: String,

    /// Statistic to query: "sum", "count", "avg", "min", "max", "quantile", etc.
    #[arg(short, long, default_value = "sum")]
    statistic: String,

    /// Optional quantile parameter (for quantile queries)
    #[arg(long)]
    quantile: Option<String>,

    /// Maximum number of precomputes to load (for testing)
    #[arg(long)]
    max_records: Option<usize>,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Window size for sliding window merges (number of precomputes per window)
    #[arg(long)]
    window_size: Option<usize>,

    /// Number of sliding window iterations (default: 1 if window_size set)
    #[arg(long)]
    iterations: Option<usize>,

    /// Step size for sliding (defaults to window_size for tumbling windows)
    #[arg(long)]
    slide_step: Option<usize>,

    /// Keep only last merged result for query testing (default: true)
    #[arg(long, default_value = "true")]
    keep_last_only: bool,
}

/// Represents a single loaded precompute dump from the file
struct LoadedPrecompute {
    metadata: PrecomputedOutput,
    accumulator: Box<dyn AggregateCore>,
}

/// Deserializable version matching the dump format
/// This must match the PrecomputeDump struct in src/utils/precompute_dumper.rs
#[derive(Deserialize, Debug)]
struct PrecomputeDumpRaw {
    #[allow(dead_code)]
    timestamp: u64,
    metadata: PrecomputedOutput,
    accumulator_type: String,
    accumulator_data_bytes: Vec<u8>,
}

/// Type alias for merged precomputes result
type MergedPrecomputes = HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>;

/// Statistics for analysis
#[derive(Debug, Default)]
struct LoadStatistics {
    total_records: usize,
    records_by_type: HashMap<String, usize>,
    records_by_aggregation_id: HashMap<u64, usize>,
    time_range: (u64, u64), // (min_start, max_end)
}

/// Window configuration for sliding window merges
#[derive(Debug, Clone)]
struct WindowConfig {
    window_size: usize,
    iterations: usize,
    slide_step: usize,
    keep_last_only: bool,
}

impl WindowConfig {
    fn from_args(args: &Args) -> Option<Self> {
        args.window_size.map(|window_size| Self {
            window_size,
            iterations: args.iterations.unwrap_or(1),
            slide_step: args.slide_step.unwrap_or(window_size),
            keep_last_only: args.keep_last_only,
        })
    }

    /// Calculate window boundaries for given total precomputes
    fn calculate_windows(&self, total: usize) -> Vec<(usize, usize)> {
        let mut windows = Vec::new();
        for i in 0..self.iterations {
            let start = i * self.slide_step;
            if start >= total {
                break;
            }
            let end = std::cmp::min(start + self.window_size, total);
            if start < end {
                windows.push((start, end));
            }
        }
        windows
    }
}

/// Statistics for a single window merge operation
#[derive(Debug, Clone)]
struct WindowStats {
    precompute_count: usize,
    merge_time: std::time::Duration,
}

/// Statistics for windowed merge operations
#[derive(Debug, Default)]
struct WindowMergeStatistics {
    window_stats: HashMap<Option<KeyByLabelValues>, Vec<WindowStats>>,
    total_windows: usize,
    total_merges: usize,
    total_merge_time: std::time::Duration,
}

impl WindowMergeStatistics {
    fn new() -> Self {
        Self::default()
    }

    fn add_window_stat(&mut self, key: Option<KeyByLabelValues>, stat: WindowStats) {
        self.total_windows += 1;
        self.total_merges += stat.precompute_count;
        self.total_merge_time += stat.merge_time;

        self.window_stats.entry(key).or_default().push(stat);
    }
}

/// Validate window-related CLI arguments
fn validate_window_args(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ws) = args.window_size {
        if ws == 0 {
            return Err("window_size must be greater than 0".into());
        }
        if let Some(step) = args.slide_step {
            if step == 0 {
                return Err("slide_step must be greater than 0".into());
            }
        }
    } else {
        // Ensure window-related args not used without window_size
        if args.iterations.is_some() {
            return Err("iterations requires window_size".into());
        }
        if args.slide_step.is_some() {
            return Err("slide_step requires window_size".into());
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse CLI arguments
    let args = Args::parse();

    // 2. Validate window arguments
    validate_window_args(&args)?;

    // 3. Initialize logging
    init_logging(args.verbose);

    // 4. Parse window configuration
    let window_config = WindowConfig::from_args(&args);

    // 5. Load precomputes from file
    println!("Loading precomputes from: {:?}", args.input_file);
    let (precomputes, stats) = load_precomputes_from_file(&args.input_file, args.max_records)?;

    // 6. Display load statistics
    print_load_statistics(&stats);

    // 7. Group precomputes by key for testing
    let grouped_precomputes = group_precomputes_by_key(precomputes);

    // 8. Run tests based on mode
    match args.mode.as_str() {
        "merge" => {
            println!("\n=== TESTING MERGE FUNCTIONALITY ===\n");
            test_merge_functionality(
                &grouped_precomputes,
                parse_pattern_type(&args.pattern_type),
                &args.aggregation_type,
                window_config,
            )?;
        }
        "query" => {
            if window_config.is_some() {
                println!("Warning: window parameters ignored in 'query' mode");
            }
            println!("\n=== TESTING QUERY FUNCTIONALITY ===\n");
            test_query_functionality(
                &grouped_precomputes,
                parse_statistic(&args.statistic)?,
                build_query_kwargs(&args),
            )?;
        }
        "both" => {
            println!("\n=== TESTING MERGE FUNCTIONALITY ===\n");
            let merged = test_merge_functionality(
                &grouped_precomputes,
                parse_pattern_type(&args.pattern_type),
                &args.aggregation_type,
                window_config,
            )?;

            println!("\n=== TESTING QUERY FUNCTIONALITY ===\n");
            test_query_on_merged(
                &merged,
                parse_statistic(&args.statistic)?,
                build_query_kwargs(&args),
            )?;
        }
        _ => {
            return Err(format!("Invalid mode: {}", args.mode).into());
        }
    }

    println!("\n=== TESTING COMPLETE ===");
    Ok(())
}

/// Load precomputes from a MessagePack dump file
///
/// File format (from precompute_dumper.rs):
/// - 4 bytes: length prefix (u32, little-endian)
/// - N bytes: MessagePack-serialized PrecomputeDumpRaw
/// - Repeat...
fn load_precomputes_from_file(
    file_path: &PathBuf,
    max_records: Option<usize>,
) -> Result<(Vec<LoadedPrecompute>, LoadStatistics), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut precomputes = Vec::new();
    let mut stats = LoadStatistics::default();

    let mut count = 0;
    loop {
        // Check if we've reached max_records
        if let Some(max) = max_records {
            if count >= max {
                println!("Reached max_records limit: {}", max);
                break;
            }
        }

        // Read length prefix (4 bytes, little-endian)
        let mut length_bytes = [0u8; 4];
        match reader.read_exact(&mut length_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // End of file reached
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let length = u32::from_le_bytes(length_bytes) as usize;

        // Read the serialized data
        let mut data_bytes = vec![0u8; length];
        reader.read_exact(&mut data_bytes)?;

        // Deserialize from MessagePack
        let dump: PrecomputeDumpRaw = rmp_serde::from_slice(&data_bytes)
            .map_err(|e| format!("Failed to deserialize record {}: {}", count, e))?;

        // Deserialize accumulator from bytes
        let accumulator =
            deserialize_accumulator(&dump.accumulator_type, &dump.accumulator_data_bytes)?;

        // Update statistics
        update_statistics(&mut stats, &dump);

        // Create loaded precompute
        precomputes.push(LoadedPrecompute {
            metadata: dump.metadata,
            accumulator,
        });

        count += 1;
        if count % 10000 == 0 {
            println!("Loaded {} precomputes...", count);
        }
    }

    stats.total_records = count;
    println!("Total precomputes loaded: {}", count);

    Ok((precomputes, stats))
}

/// Deserialize accumulator from bytes based on type
/// Only supports accumulators with deserialize_from_bytes_arroyo method
fn deserialize_accumulator(
    accumulator_type: &str,
    bytes: &[u8],
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>> {
    match accumulator_type {
        "SumAccumulator" => Ok(Box::new(
            sum_accumulator::SumAccumulator::deserialize_from_bytes_arroyo(bytes)?,
        )),
        "MultipleIncreaseAccumulator" => Ok(Box::new(
            multiple_increase_accumulator::MultipleIncreaseAccumulator::deserialize_from_bytes_arroyo(bytes)?,
        )),
        "CountMinSketchAccumulator" => Ok(Box::new(
            count_min_sketch_accumulator::CountMinSketchAccumulator::deserialize_from_bytes_arroyo(bytes)?,
        )),
        "CountMinSketchWithHeapAccumulator" => Ok(Box::new(
            count_min_sketch_with_heap_accumulator::CountMinSketchWithHeapAccumulator::deserialize_from_bytes_arroyo(
                bytes,
            )?,
        )),
        "DatasketchesKLLAccumulator" => Ok(Box::new(
            datasketches_kll_accumulator::DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(bytes)?,
        )),
        "DeltaSetAggregatorAccumulator" => Ok(Box::new(
            delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator::deserialize_from_bytes_arroyo(
                bytes,
            )?,
        )),
        "SetAggregatorAccumulator" => Ok(Box::new(
            set_aggregator_accumulator::SetAggregatorAccumulator::deserialize_from_bytes_arroyo(bytes)?,
        )),
        _ => Err(format!("Unsupported accumulator type: {} (only Arroyo-based accumulators supported)", accumulator_type).into()),
    }
}

/// Group precomputes by their key to prepare for merging
///
/// This simulates how simple_engine.rs groups precomputes from the store
/// into HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>
///
/// Returns: HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>
fn group_precomputes_by_key(
    precomputes: Vec<LoadedPrecompute>,
) -> HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>> {
    let mut grouped: HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>> =
        HashMap::new();

    for precompute in precomputes {
        grouped
            .entry(precompute.metadata.key.clone())
            .or_default()
            .push(precompute.accumulator);
    }

    println!(
        "Grouped {} precomputes into {} unique keys",
        grouped.values().map(|v| v.len()).sum::<usize>(),
        grouped.len()
    );

    grouped
}

/// Test merge_precomputed_outputs functionality with optional windowing
/// This replicates the logic from simple_engine.rs:1334-1409
///
/// Reference: SimpleEngine::merge_precomputed_outputs
fn test_merge_functionality(
    grouped_precomputes: &HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
    query_pattern_type: QueryPatternType,
    aggregation_type: &str,
    window_config: Option<WindowConfig>,
) -> Result<MergedPrecomputes, Box<dyn std::error::Error>> {
    println!("Testing merge with pattern type: {:?}", query_pattern_type);
    println!("Aggregation type: {}", aggregation_type);

    if let Some(ref config) = window_config {
        println!("\n=== WINDOWED MERGE CONFIGURATION ===");
        println!("Window size: {}", config.window_size);
        println!("Iterations: {}", config.iterations);
        println!("Slide step: {}", config.slide_step);

        test_merge_with_windows(
            grouped_precomputes,
            query_pattern_type,
            aggregation_type,
            config,
        )
    } else {
        println!("Mode: Standard (merge all)");
        test_merge_all(grouped_precomputes, query_pattern_type, aggregation_type)
    }
}

/// Merge all precomputes for each key (standard mode)
fn test_merge_all(
    grouped_precomputes: &HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
    query_pattern_type: QueryPatternType,
    aggregation_type: &str,
) -> Result<MergedPrecomputes, Box<dyn std::error::Error>> {
    let mut merged_results = HashMap::new();
    let mut merge_times = Vec::new();

    for (key, precomputes) in grouped_precomputes.iter() {
        if precomputes.is_empty() {
            continue;
        }

        let start = std::time::Instant::now();

        let merged = if should_merge(query_pattern_type, aggregation_type) {
            merge_accumulators(precomputes)?
        } else {
            println!("  No merge needed, taking single precompute");
            assert_eq!(
                precomputes.len(),
                1,
                "Expected exactly 1 precompute for spatial query without DeltaSetAggregator"
            );
            precomputes[0].clone()
        };

        let elapsed = start.elapsed();
        merge_times.push(elapsed);

        println!(
            "  Merge completed in {:.2}ms",
            elapsed.as_secs_f64() * 1000.0
        );
        println!("  Result type: {}", merged.get_accumulator_type());

        merged_results.insert(key.clone(), merged);
    }

    // Print statistics
    if !merge_times.is_empty() {
        let total: std::time::Duration = merge_times.iter().sum();
        let avg = total / merge_times.len() as u32;
        println!("\n=== Merge Statistics ===");
        println!("Total merges: {}", merge_times.len());
        println!("Total time: {:.2}ms", total.as_secs_f64() * 1000.0);
        println!("Average time: {:.2}ms", avg.as_secs_f64() * 1000.0);
    }

    Ok(merged_results)
}

/// Merge precomputes using sliding windows
fn test_merge_with_windows(
    grouped_precomputes: &HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
    query_pattern_type: QueryPatternType,
    aggregation_type: &str,
    config: &WindowConfig,
) -> Result<MergedPrecomputes, Box<dyn std::error::Error>> {
    let mut final_results = HashMap::new();
    let mut window_stats = WindowMergeStatistics::new();

    println!(
        "\nProcessing {} keys with sliding window merge",
        grouped_precomputes.len()
    );

    for (key, precomputes) in grouped_precomputes.iter() {
        if precomputes.is_empty() {
            continue;
        }

        //println!("\nKey: {:?}", key);
        //println!("Total precomputes: {}", precomputes.len());

        // Calculate window boundaries
        let windows = config.calculate_windows(precomputes.len());

        if windows.is_empty() {
            println!("  Warning: No valid windows for this key");
            continue;
        }

        //println!("  Windows to process: {}", windows.len());

        // Process each window
        let mut window_results = Vec::new();

        for (window_idx, (start_idx, end_idx)) in windows.iter().enumerate() {
            //println!("  Window {}/{}: merging precomputes [{}..{}] ({} items)",
            //         window_idx + 1, windows.len(), start_idx, end_idx, end_idx - start_idx);

            let window_start = std::time::Instant::now();

            // Extract window slice
            let window_slice = &precomputes[*start_idx..*end_idx];

            // Perform merge if needed
            let merged = if should_merge(query_pattern_type, aggregation_type) {
                merge_accumulators(window_slice)?
            } else {
                // For spatial queries without DeltaSetAggregator
                if window_slice.len() != 1 {
                    println!("    Warning: Expected 1 precompute for spatial query, got {}. Taking first.", window_slice.len());
                }
                window_slice[0].clone()
            };

            let window_elapsed = window_start.elapsed();

            // Record statistics
            let stat = WindowStats {
                precompute_count: end_idx - start_idx,
                merge_time: window_elapsed,
            };

            window_stats.add_window_stat(key.clone(), stat);

            //println!("    Window merge time: {:.2}ms", window_elapsed.as_secs_f64() * 1000.0);
            //println!("    Result type: {}", merged.get_accumulator_type());

            // Store result
            if config.keep_last_only {
                // Only keep the last window's result
                if window_idx == windows.len() - 1 {
                    window_results.push(merged);
                }
            } else {
                // Keep all window results
                window_results.push(merged);
            }
        }

        // Store final result for this key
        if !window_results.is_empty() {
            // For query testing, we use the last result
            let final_result = window_results.into_iter().last().unwrap();
            final_results.insert(key.clone(), final_result);
        }
    }

    // Print comprehensive statistics
    print_window_merge_statistics(&window_stats);

    Ok(final_results)
}

/// Print detailed statistics for windowed merge operations
fn print_window_merge_statistics(stats: &WindowMergeStatistics) {
    println!("\n=== WINDOWED MERGE STATISTICS ===");
    println!("Total windows processed: {}", stats.total_windows);
    println!("Total precomputes merged: {}", stats.total_merges);
    println!(
        "Total merge time: {:.2}ms",
        stats.total_merge_time.as_secs_f64() * 1000.0
    );

    if stats.total_windows > 0 {
        let avg_window_time = stats.total_merge_time / stats.total_windows as u32;
        println!(
            "Average time per window: {:.2}ms",
            avg_window_time.as_secs_f64() * 1000.0
        );
    }

    // Per-key breakdown
    println!("\n=== PER-KEY STATISTICS ===");
    for key_stats in stats.window_stats.values() {
        //println!("\nKey: {:?}", key);
        //println!("  Windows: {}", key_stats.len());

        let total_precomputes: usize = key_stats.iter().map(|s| s.precompute_count).sum();
        let total_time: std::time::Duration = key_stats.iter().map(|s| s.merge_time).sum();

        println!("  Total precomputes: {}", total_precomputes);
        println!("  Total time: {:.2}ms", total_time.as_secs_f64() * 1000.0);

        if !key_stats.is_empty() {
            let avg_time = total_time / key_stats.len() as u32;
            println!(
                "  Average time per window: {:.2}ms",
                avg_time.as_secs_f64() * 1000.0
            );
        }
    }
}

/// Determine if merging should happen based on pattern type and aggregation type
/// Mirrors logic from simple_engine.rs:1360-1395
fn should_merge(pattern_type: QueryPatternType, aggregation_type: &str) -> bool {
    match pattern_type {
        QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => true,
        QueryPatternType::OnlySpatial => aggregation_type == "DeltaSetAggregator",
    }
}

/// Merge multiple accumulators
/// This replicates simple_engine.rs:1413-1441
///
/// Reference: SimpleEngine::merge_accumulators
fn merge_accumulators(
    accumulators: &[Box<dyn AggregateCore>],
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>> {
    if accumulators.is_empty() {
        return Err("No accumulators to merge".into());
    }

    if accumulators.len() == 1 {
        return Ok(accumulators[0].clone());
    }

    let mut result = accumulators[0].clone();

    for (i, accumulator) in accumulators[1..].iter().enumerate() {
        //println!("    Merging accumulator {} of {}", i + 2, accumulators.len());
        match result.merge_with(accumulator.as_ref()) {
            Ok(merged) => {
                result = merged;
            }
            Err(e) => {
                eprintln!("    Warning: Failed to merge accumulator {}: {}", i + 2, e);
                // Continue with current result
            }
        }
    }

    Ok(result)
}

/// Test query_precompute_for_statistic functionality on merged results
fn test_query_on_merged(
    merged_precomputes: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
    statistic: Statistic,
    query_kwargs: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing query with statistic: {:?}", statistic);
    println!("Query kwargs: {:?}", query_kwargs);

    let mut query_results = Vec::new();

    for (idx, (key, precompute)) in merged_precomputes.iter().enumerate() {
        println!(
            "\n--- Querying key {} of {} ---",
            idx + 1,
            merged_precomputes.len()
        );
        println!("Key: {:?}", key);
        println!("Accumulator type: {}", precompute.get_accumulator_type());

        let start = std::time::Instant::now();

        let result =
            query_precompute_for_statistic(precompute.as_ref(), &statistic, key, &query_kwargs)?;

        let elapsed = start.elapsed();

        println!("  Query result: {}", result);
        println!("  Query time: {:.2}μs", elapsed.as_micros());

        query_results.push((key.clone(), result));
    }

    // Print summary
    println!("\n=== Query Results Summary ===");
    println!("Total results: {}", query_results.len());
    for (key, value) in &query_results {
        println!("  {:?} => {}", key, value);
    }

    Ok(())
}

/// Also test querying functionality on ungrouped precomputes
fn test_query_functionality(
    grouped_precomputes: &HashMap<Option<KeyByLabelValues>, Vec<Box<dyn AggregateCore>>>,
    statistic: Statistic,
    query_kwargs: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing query on individual (unmerged) precomputes");
    println!("Statistic: {:?}", statistic);

    for (key, precomputes) in grouped_precomputes {
        println!(
            "\n--- Key: {:?} ({} precomputes) ---",
            key,
            precomputes.len()
        );

        for (i, precompute) in precomputes.iter().enumerate() {
            println!(
                "  Precompute {}: type = {}",
                i,
                precompute.get_accumulator_type()
            );

            let result = query_precompute_for_statistic(
                precompute.as_ref(),
                &statistic,
                key,
                &query_kwargs,
            )?;

            println!("    Result: {}", result);
        }
    }

    Ok(())
}

/// Query a precompute for a specific statistic
/// Only supports Arroyo-based accumulators
fn query_precompute_for_statistic(
    precompute: &dyn AggregateCore,
    statistic: &Statistic,
    key: &Option<KeyByLabelValues>,
    query_kwargs: &HashMap<String, String>,
) -> Result<f64, Box<dyn std::error::Error>> {
    match precompute.get_accumulator_type() {
        AggregationType::Sum => {
            let acc = precompute
                .as_any()
                .downcast_ref::<sum_accumulator::SumAccumulator>()
                .ok_or("Failed to downcast to SumAccumulator")?;
            use query_engine_rust::data_model::SingleSubpopulationAggregate;
            acc.query(*statistic, None)
                .map_err(|e| format!("{}", e).into())
        }
        AggregationType::MultipleIncrease => {
            let acc = precompute
                .as_any()
                .downcast_ref::<multiple_increase_accumulator::MultipleIncreaseAccumulator>()
                .ok_or("Failed to downcast to MultipleIncreaseAccumulator")?;
            let key_val = key
                .as_ref()
                .ok_or("Key required for MultipleIncreaseAccumulator")?;
            use query_engine_rust::data_model::MultipleSubpopulationAggregate;
            acc.query(*statistic, key_val, Some(query_kwargs))
                .map_err(|e| format!("{}", e).into())
        }
        AggregationType::CountMinSketch => {
            let acc = precompute
                .as_any()
                .downcast_ref::<count_min_sketch_accumulator::CountMinSketchAccumulator>()
                .ok_or("Failed to downcast to CountMinSketchAccumulator")?;
            let key_val = key
                .as_ref()
                .ok_or("Key required for CountMinSketchAccumulator")?;
            use query_engine_rust::data_model::MultipleSubpopulationAggregate;
            acc.query(*statistic, key_val, Some(query_kwargs))
                .map_err(|e| format!("{}", e).into())
        }
        AggregationType::CountMinSketchWithHeap => {
            let acc = precompute
                .as_any()
                .downcast_ref::<count_min_sketch_with_heap_accumulator::CountMinSketchWithHeapAccumulator>()
                .ok_or("Failed to downcast to CountMinSketchWithHeapAccumulator")?;
            let key_val = key
                .as_ref()
                .ok_or("Key required for CountMinSketchWithHeapAccumulator")?;
            use query_engine_rust::data_model::MultipleSubpopulationAggregate;
            acc.query(*statistic, key_val, Some(query_kwargs))
                .map_err(|e| format!("{}", e).into())
        }
        AggregationType::DatasketchesKLL => {
            let acc = precompute
                .as_any()
                .downcast_ref::<datasketches_kll_accumulator::DatasketchesKLLAccumulator>()
                .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;
            use query_engine_rust::data_model::SingleSubpopulationAggregate;
            acc.query(*statistic, Some(query_kwargs))
                .map_err(|e| format!("{}", e).into())
        }
        AggregationType::DeltaSetAggregator => {
            let acc = precompute
                .as_any()
                .downcast_ref::<delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator>()
                .ok_or("Failed to downcast to DeltaSetAggregatorAccumulator")?;
            if let Some(key_val) = key {
                use query_engine_rust::data_model::MultipleSubpopulationAggregate;
                acc.query(*statistic, key_val, Some(query_kwargs))
                    .map_err(|e| format!("{}", e).into())
            } else {
                Ok((acc.added.union(&acc.removed).count()) as f64)
            }
        }
        AggregationType::SetAggregator => {
            let acc = precompute
                .as_any()
                .downcast_ref::<set_aggregator_accumulator::SetAggregatorAccumulator>()
                .ok_or("Failed to downcast to SetAggregatorAccumulator")?;
            if let Some(key_val) = key {
                use query_engine_rust::data_model::MultipleSubpopulationAggregate;
                acc.query(*statistic, key_val, Some(query_kwargs))
                    .map_err(|e| format!("{}", e).into())
            } else {
                Ok(acc.added.len() as f64)
            }
        }
        other => Err(format!("Unsupported accumulator type: {other:?}").into()),
    }
}

/// Initialize logging based on verbosity
fn init_logging(verbose: bool) {
    use tracing_subscriber;

    let level = if verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt().with_max_level(level).init();
}

/// Update statistics during loading
fn update_statistics(stats: &mut LoadStatistics, dump: &PrecomputeDumpRaw) {
    // Count by type
    *stats
        .records_by_type
        .entry(dump.accumulator_type.clone())
        .or_insert(0) += 1;

    // Count by aggregation_id
    *stats
        .records_by_aggregation_id
        .entry(dump.metadata.aggregation_id)
        .or_insert(0) += 1;

    // Track time range
    if stats.time_range.0 == 0 || dump.metadata.start_timestamp < stats.time_range.0 {
        stats.time_range.0 = dump.metadata.start_timestamp;
    }
    if dump.metadata.end_timestamp > stats.time_range.1 {
        stats.time_range.1 = dump.metadata.end_timestamp;
    }
}

/// Print load statistics
fn print_load_statistics(stats: &LoadStatistics) {
    println!("\n=== Load Statistics ===");
    println!("Total records: {}", stats.total_records);

    println!("\nRecords by accumulator type:");
    for (acc_type, count) in &stats.records_by_type {
        println!("  {}: {}", acc_type, count);
    }

    println!("\nRecords by aggregation ID:");
    for (agg_id, count) in &stats.records_by_aggregation_id {
        println!("  Aggregation {}: {}", agg_id, count);
    }

    println!("\nTime range:");
    println!("  Start: {}", stats.time_range.0);
    println!("  End: {}", stats.time_range.1);
    println!("  Duration: {} ms", stats.time_range.1 - stats.time_range.0);
}

/// Parse pattern type string to enum
fn parse_pattern_type(s: &str) -> QueryPatternType {
    match s.to_lowercase().as_str() {
        "temporal" | "only_temporal" => QueryPatternType::OnlyTemporal,
        "spatial" | "only_spatial" => QueryPatternType::OnlySpatial,
        "temporal_spatial" | "one_temporal_one_spatial" => QueryPatternType::OneTemporalOneSpatial,
        _ => {
            eprintln!("Unknown pattern type '{}', defaulting to OnlyTemporal", s);
            QueryPatternType::OnlyTemporal
        }
    }
}

/// Parse statistic string to enum
fn parse_statistic(s: &str) -> Result<Statistic, Box<dyn std::error::Error>> {
    s.parse::<Statistic>()
        .map_err(|_| format!("Invalid statistic: {}", s).into())
}

/// Build query kwargs from CLI args
fn build_query_kwargs(args: &Args) -> HashMap<String, String> {
    let mut kwargs = HashMap::new();

    if let Some(ref quantile) = args.quantile {
        kwargs.insert("quantile".to_string(), quantile.clone());
    }

    kwargs
}
