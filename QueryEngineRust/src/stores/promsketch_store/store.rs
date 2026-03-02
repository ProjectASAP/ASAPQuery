// use std::sync::atomic::{AtomicU64, Ordering};
// use std::sync::RwLock;

// use dashmap::DashMap;

// use super::config::PromSketchConfig;
// use super::metrics;
// use super::query;
// use super::series::PromSketchMemSeries;
// use super::types::{promsketch_func_map, PromSketchType};

// /// Concurrent store for live per-time-series sketch instances.
// ///
// /// This is NOT a `Store` trait implementation — it stores live sketch instances
// /// for direct insert/query, not precomputed aggregation buckets.
// pub struct PromSketchStore {
//     /// Concurrent outer map keyed by label string, per-series RwLock.
//     series: DashMap<String, RwLock<PromSketchMemSeries>>,
//     /// Total number of distinct series.
//     num_series: AtomicU64,
//     /// Sketch configuration shared across all series.
//     config: PromSketchConfig,
// }

// impl PromSketchStore {
//     /// Create a new store with the given configuration.
//     pub fn new(config: PromSketchConfig) -> Self {
//         Self {
//             series: DashMap::new(),
//             num_series: AtomicU64::new(0),
//             config,
//         }
//     }

//     /// Create a new store with default configuration.
//     pub fn with_default_config() -> Self {
//         Self::new(PromSketchConfig::default())
//     }

//     /// Return the number of distinct series tracked.
//     pub fn num_series(&self) -> u64 {
//         self.num_series.load(Ordering::Relaxed)
//     }

//     /// Get or create a series entry for the given labels string.
//     /// Returns true if a new series was created.
//     pub fn get_or_create(&self, labels: &str) -> bool {
//         use dashmap::mapref::entry::Entry;
//         match self.series.entry(labels.to_string()) {
//             Entry::Occupied(_) => false,
//             Entry::Vacant(vacant) => {
//                 vacant.insert(RwLock::new(PromSketchMemSeries::new(labels.to_string())));
//                 self.num_series.fetch_add(1, Ordering::Relaxed);
//                 metrics::SERIES_TOTAL.inc();
//                 true
//             }
//         }
//     }

//     /// Initialize all 3 sketch types (EHUniv, EHKLL, USampling) for a series.
//     ///
//     /// Idempotent — calls `ensure_initialized` which is a no-op if already initialized.
//     /// Intended for use by the raw Kafka consumer on first data arrival for a new series.
//     pub fn ensure_all_sketches(
//         &self,
//         labels: &str,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         self.get_or_create(labels);

//         let entry = self
//             .series
//             .get(labels)
//             .ok_or("series disappeared unexpectedly")?;
//         let mut series = entry.write().map_err(|e| format!("lock poisoned: {}", e))?;

//         series
//             .sketch_instances
//             .ensure_initialized(PromSketchType::EHUniv, &self.config);
//         series
//             .sketch_instances
//             .ensure_initialized(PromSketchType::EHKLL, &self.config);
//         series
//             .sketch_instances
//             .ensure_initialized(PromSketchType::USampling, &self.config);

//         Ok(())
//     }

//     /// Initialize sketch instances for a given function on a series.
//     /// Creates the series if it doesn't exist, then lazily initializes
//     /// whichever sketch types the function requires.
//     pub fn new_sketch_cache_instance(
//         &self,
//         labels: &str,
//         func_name: &str,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         let stypes = promsketch_func_map(func_name)
//             .ok_or_else(|| format!("unsupported function: {}", func_name))?;

//         self.get_or_create(labels);

//         let entry = self
//             .series
//             .get(labels)
//             .ok_or("series disappeared unexpectedly")?;
//         let mut series = entry.write().map_err(|e| format!("lock poisoned: {}", e))?;

//         for &stype in stypes {
//             series
//                 .sketch_instances
//                 .ensure_initialized(stype, &self.config);
//         }

//         Ok(())
//     }

//     /// Initialize sketch instances with a custom config (for overriding time windows, etc.).
//     pub fn new_sketch_cache_instance_with_config(
//         &self,
//         labels: &str,
//         func_name: &str,
//         config: &PromSketchConfig,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         let stypes = promsketch_func_map(func_name)
//             .ok_or_else(|| format!("unsupported function: {}", func_name))?;

//         self.get_or_create(labels);

//         let entry = self
//             .series
//             .get(labels)
//             .ok_or("series disappeared unexpectedly")?;
//         let mut series = entry.write().map_err(|e| format!("lock poisoned: {}", e))?;

//         for &stype in stypes {
//             series.sketch_instances.ensure_initialized(stype, config);
//         }

//         Ok(())
//     }

//     /// Insert a data point into all active sketches for the given series.
//     /// No-op if the series or its sketches haven't been initialized.
//     pub fn sketch_insert(
//         &self,
//         labels: &str,
//         time: u64,
//         value: f64,
//     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         let entry = match self.series.get(labels) {
//             Some(e) => e,
//             None => return Ok(()), // No series yet — silent no-op like Go version
//         };

//         let mut series = entry.write().map_err(|e| format!("lock poisoned: {}", e))?;
//         series.insert(time, value);
//         Ok(())
//     }

//     /// Check whether the sketches for a given function cover the requested time range.
//     pub fn lookup(&self, labels: &str, func_name: &str, mint: u64, maxt: u64) -> bool {
//         let stypes = match promsketch_func_map(func_name) {
//             Some(s) => s,
//             None => return false,
//         };

//         let entry = match self.series.get(labels) {
//             Some(e) => e,
//             None => return false,
//         };

//         let series = match entry.read() {
//             Ok(s) => s,
//             Err(_) => return false,
//         };

//         for &stype in stypes {
//             if !series.sketch_instances.cover(stype, mint, maxt) {
//                 return false;
//             }
//         }

//         true
//     }

//     /// Evaluate a PromQL function on the sketches for a given series and time range.
//     pub fn eval(
//         &self,
//         func_name: &str,
//         labels: &str,
//         args: f64,
//         mint: u64,
//         maxt: u64,
//     ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
//         let entry = self
//             .series
//             .get(labels)
//             .ok_or_else(|| format!("series not found: {}", labels))?;

//         let series = entry.read().map_err(|e| format!("lock poisoned: {}", e))?;

//         query::eval_function(func_name, &series, args, mint, maxt)
//     }

//     /// Return the labels of all series whose labels string starts with the given metric name.
//     /// If `metric_or_labels` contains `{`, matches exactly; otherwise matches series
//     /// whose labels string starts with `metric_or_labels`.
//     pub fn matching_series_labels(&self, metric_or_labels: &str) -> Vec<String> {
//         let mut matched = Vec::new();
//         for entry in self.series.iter() {
//             let key = entry.key();
//             if key == metric_or_labels
//                 || key.starts_with(&format!("{}{}", metric_or_labels, "{"))
//                 || key.starts_with(&format!("{},", metric_or_labels))
//             {
//                 matched.push(key.clone());
//             }
//         }
//         // If nothing matched by prefix, try exact match (the metric_or_labels IS the full key)
//         if matched.is_empty() && self.series.contains_key(metric_or_labels) {
//             matched.push(metric_or_labels.to_string());
//         }
//         matched
//     }

//     /// Evaluate a function across all series whose labels match the given metric/labels key,
//     /// returning results keyed by full labels string.
//     pub fn eval_matching(
//         &self,
//         func_name: &str,
//         metric_or_labels: &str,
//         args: f64,
//         mint: u64,
//         maxt: u64,
//     ) -> Result<Vec<(String, f64)>, Box<dyn std::error::Error + Send + Sync>> {
//         let matched_labels = self.matching_series_labels(metric_or_labels);
//         let mut results = Vec::new();
//         for labels in matched_labels {
//             match self.eval(func_name, &labels, args, mint, maxt) {
//                 Ok(value) => results.push((labels, value)),
//                 Err(e) => {
//                     tracing::debug!("Skipping series {}: {}", labels, e);
//                 }
//             }
//         }
//         Ok(results)
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_get_or_create() {
//         let store = PromSketchStore::with_default_config();
//         assert_eq!(store.num_series(), 0);

//         let created = store.get_or_create("metric{host=\"a\"}");
//         assert!(created);
//         assert_eq!(store.num_series(), 1);

//         // Second call should not create
//         let created = store.get_or_create("metric{host=\"a\"}");
//         assert!(!created);
//         assert_eq!(store.num_series(), 1);

//         // Different labels should create
//         let created = store.get_or_create("metric{host=\"b\"}");
//         assert!(created);
//         assert_eq!(store.num_series(), 2);
//     }

//     #[test]
//     fn test_new_sketch_cache_instance() {
//         let store = PromSketchStore::with_default_config();
//         let result = store.new_sketch_cache_instance("m1", "quantile_over_time");
//         assert!(result.is_ok());
//         assert_eq!(store.num_series(), 1);

//         // Verify EHKLL was initialized
//         let entry = store.series.get("m1").unwrap();
//         let series = entry.read().unwrap();
//         assert!(series.sketch_instances.eh_kll.is_some());
//         assert!(series.sketch_instances.eh_univ.is_none());
//         assert!(series.sketch_instances.eh_sampling.is_none());
//     }

//     #[test]
//     fn test_insert_lookup_eval_roundtrip() {
//         let store = PromSketchStore::with_default_config();

//         // Initialize KLL sketches for quantile queries
//         store
//             .new_sketch_cache_instance("ts1", "quantile_over_time")
//             .unwrap();

//         // Insert data points
//         for i in 1..=100u64 {
//             store.sketch_insert("ts1", i, i as f64).unwrap();
//         }

//         // Lookup should succeed
//         assert!(store.lookup("ts1", "quantile_over_time", 1, 100));

//         // Eval median
//         let result = store.eval("quantile_over_time", "ts1", 0.5, 1, 100);
//         assert!(result.is_ok());
//         let val = result.unwrap();
//         assert!(val > 30.0 && val < 70.0, "median was {}", val);
//     }

//     #[test]
//     fn test_insert_noop_for_unknown_series() {
//         let store = PromSketchStore::with_default_config();
//         // Insert into non-existent series should be a no-op
//         let result = store.sketch_insert("nonexistent", 1, 1.0);
//         assert!(result.is_ok());
//     }

//     #[test]
//     fn test_lookup_returns_false_for_missing() {
//         let store = PromSketchStore::with_default_config();
//         assert!(!store.lookup("missing", "quantile_over_time", 1, 100));
//     }

//     #[test]
//     fn test_multiple_sketch_types_on_same_series() {
//         let store = PromSketchStore::with_default_config();

//         store
//             .new_sketch_cache_instance("ts1", "quantile_over_time")
//             .unwrap();
//         store
//             .new_sketch_cache_instance("ts1", "entropy_over_time")
//             .unwrap();
//         store
//             .new_sketch_cache_instance("ts1", "avg_over_time")
//             .unwrap();

//         let entry = store.series.get("ts1").unwrap();
//         let series = entry.read().unwrap();
//         assert!(series.sketch_instances.eh_kll.is_some());
//         assert!(series.sketch_instances.eh_univ.is_some());
//         assert!(series.sketch_instances.eh_sampling.is_some());
//     }

//     #[test]
//     fn test_concurrent_insert_and_query() {
//         use std::sync::Arc;
//         use std::thread;

//         let store = Arc::new(PromSketchStore::with_default_config());

//         // Initialize
//         store
//             .new_sketch_cache_instance("ts1", "quantile_over_time")
//             .unwrap();

//         let n_threads = 4;
//         let n_inserts = 100;

//         // Spawn writer threads
//         let mut handles = Vec::new();
//         for t in 0..n_threads {
//             let store = Arc::clone(&store);
//             handles.push(thread::spawn(move || {
//                 for i in 0..n_inserts {
//                     let time = (t * n_inserts + i + 1) as u64;
//                     store.sketch_insert("ts1", time, time as f64).unwrap();
//                 }
//             }));
//         }

//         // Spawn a reader thread
//         let store_read = Arc::clone(&store);
//         handles.push(thread::spawn(move || {
//             for _ in 0..50 {
//                 let _ = store_read.lookup("ts1", "quantile_over_time", 1, 400);
//             }
//         }));

//         for h in handles {
//             h.join().unwrap();
//         }

//         // After all inserts, eval should work
//         let result = store.eval(
//             "quantile_over_time",
//             "ts1",
//             0.5,
//             1,
//             (n_threads * n_inserts) as u64,
//         );
//         assert!(result.is_ok());
//     }

//     #[test]
//     fn test_ensure_all_sketches() {
//         let store = PromSketchStore::with_default_config();
//         store.ensure_all_sketches("ts_all").unwrap();

//         assert_eq!(store.num_series(), 1);
//         let entry = store.series.get("ts_all").unwrap();
//         let series = entry.read().unwrap();
//         assert!(series.sketch_instances.eh_univ.is_some());
//         assert!(series.sketch_instances.eh_kll.is_some());
//         assert!(series.sketch_instances.eh_sampling.is_some());
//     }

//     #[test]
//     fn test_ensure_all_sketches_idempotent() {
//         let store = PromSketchStore::with_default_config();
//         store.ensure_all_sketches("ts_idem").unwrap();
//         store.ensure_all_sketches("ts_idem").unwrap(); // second call should be no-op

//         assert_eq!(store.num_series(), 1);
//         let entry = store.series.get("ts_idem").unwrap();
//         let series = entry.read().unwrap();
//         assert!(series.sketch_instances.eh_univ.is_some());
//         assert!(series.sketch_instances.eh_kll.is_some());
//         assert!(series.sketch_instances.eh_sampling.is_some());
//     }

//     #[test]
//     fn test_auto_init_insert_query_roundtrip() {
//         let store = PromSketchStore::with_default_config();

//         // Auto-init all sketches (as the raw consumer would do)
//         store.ensure_all_sketches("ts_rt").unwrap();

//         // Insert data points
//         for i in 1..=100u64 {
//             store.sketch_insert("ts_rt", i, i as f64).unwrap();
//         }

//         // Verify quantile (EHKLL)
//         assert!(store.lookup("ts_rt", "quantile_over_time", 1, 100));
//         let val = store
//             .eval("quantile_over_time", "ts_rt", 0.5, 1, 100)
//             .unwrap();
//         assert!(val > 30.0 && val < 70.0, "median was {}", val);

//         // Verify avg (USampling)
//         assert!(store.lookup("ts_rt", "avg_over_time", 1, 100));
//         let avg = store.eval("avg_over_time", "ts_rt", 0.0, 1, 100).unwrap();
//         assert!(avg > 30.0 && avg < 70.0, "avg was {}", avg);

//         // Verify entropy (EHUniv)
//         assert!(store.lookup("ts_rt", "entropy_over_time", 1, 100));
//         let entropy = store
//             .eval("entropy_over_time", "ts_rt", 0.0, 1, 100)
//             .unwrap();
//         assert!(entropy >= 0.0, "entropy was {}", entropy);
//     }
// }
