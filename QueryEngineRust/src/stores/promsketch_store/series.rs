// use sketchlib_rust::{
//     EHSketchList, EHUnivOptimized, ExponentialHistogram, SketchInput, UniformSampling, KLL,
// };

// use super::config::PromSketchConfig;
// use super::PromSketchType;

// /// Per-series sketch instances. Each field wraps a different EHSketchList type
// /// inside an ExponentialHistogram for time-windowed merging.
// pub struct PromSketchInstances {
//     /// Optimized hybrid EH for UnivMon — entropy, cardinality, L1, L2, distinct.
//     pub eh_univ: Option<EHUnivOptimized>,
//     /// EH wrapping KLL — for quantile, min, max.
//     pub eh_kll: Option<ExponentialHistogram>,
//     /// EH wrapping UniformSampling — for avg, count, sum, stddev, stdvar.
//     pub eh_sampling: Option<ExponentialHistogram>,
// }

// impl Default for PromSketchInstances {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl PromSketchInstances {
//     pub fn new() -> Self {
//         Self {
//             eh_univ: None,
//             eh_kll: None,
//             eh_sampling: None,
//         }
//     }

//     /// Lazily initialize the sketch for the given type if not already present.
//     pub fn ensure_initialized(&mut self, stype: PromSketchType, config: &PromSketchConfig) {
//         match stype {
//             PromSketchType::EHUniv => {
//                 if self.eh_univ.is_none() {
//                     self.eh_univ = Some(EHUnivOptimized::with_defaults(
//                         config.eh_univ.k,
//                         config.eh_univ.time_window,
//                     ));
//                 }
//             }
//             PromSketchType::EHKLL => {
//                 if self.eh_kll.is_none() {
//                     let chapter = EHSketchList::KLL(KLL::init_kll(config.eh_kll.kll_k));
//                     self.eh_kll = Some(ExponentialHistogram::new(
//                         config.eh_kll.k,
//                         config.eh_kll.time_window,
//                         chapter,
//                     ));
//                 }
//             }
//             PromSketchType::USampling => {
//                 if self.eh_sampling.is_none() {
//                     let chapter =
//                         EHSketchList::UNIFORM(UniformSampling::new(config.sampling.sample_rate));
//                     self.eh_sampling = Some(ExponentialHistogram::new(
//                         config.eh_kll.k,
//                         config.sampling.time_window,
//                         chapter,
//                     ));
//                 }
//             }
//         }
//     }

//     /// Insert a data point into all active sketches.
//     pub fn insert(&mut self, time: u64, value: f64) {
//         let input = SketchInput::F64(value);

//         if let Some(ref mut eh) = self.eh_univ {
//             // EHUnivOptimized::update(time, key, frequency_count)
//             eh.update(time, &input, 1);
//         }
//         if let Some(ref mut eh) = self.eh_kll {
//             eh.update(time, &input);
//         }
//         if let Some(ref mut eh) = self.eh_sampling {
//             eh.update(time, &input);
//         }
//     }

//     /// Check whether the sketch for the given type covers the time range.
//     pub fn cover(&self, stype: PromSketchType, mint: u64, maxt: u64) -> bool {
//         match stype {
//             PromSketchType::EHUniv => self.eh_univ.as_ref().is_some_and(|eh| eh.cover(mint, maxt)),
//             PromSketchType::EHKLL => self.eh_kll.as_ref().is_some_and(|eh| eh.cover(mint, maxt)),
//             PromSketchType::USampling => self
//                 .eh_sampling
//                 .as_ref()
//                 .is_some_and(|eh| eh.cover(mint, maxt)),
//         }
//     }
// }

// /// A single time series with its label string and associated sketch instances.
// pub struct PromSketchMemSeries {
//     pub labels: String,
//     pub sketch_instances: PromSketchInstances,
//     /// Earliest timestamp seen for this series (-1 means uninitialized).
//     pub oldest_timestamp: i64,
// }

// impl PromSketchMemSeries {
//     pub fn new(labels: String) -> Self {
//         Self {
//             labels,
//             sketch_instances: PromSketchInstances::new(),
//             oldest_timestamp: -1,
//         }
//     }

//     /// Insert a data point, updating oldest_timestamp tracking.
//     pub fn insert(&mut self, time: u64, value: f64) {
//         if self.oldest_timestamp == -1 {
//             self.oldest_timestamp = time as i64;
//         }
//         self.sketch_instances.insert(time, value);
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_ensure_initialized_creates_correct_types() {
//         let config = PromSketchConfig::default();
//         let mut instances = PromSketchInstances::new();

//         assert!(instances.eh_univ.is_none());
//         assert!(instances.eh_kll.is_none());
//         assert!(instances.eh_sampling.is_none());

//         instances.ensure_initialized(PromSketchType::EHUniv, &config);
//         assert!(instances.eh_univ.is_some());
//         assert!(instances.eh_kll.is_none());

//         instances.ensure_initialized(PromSketchType::EHKLL, &config);
//         assert!(instances.eh_kll.is_some());
//         assert!(instances.eh_sampling.is_none());

//         instances.ensure_initialized(PromSketchType::USampling, &config);
//         assert!(instances.eh_sampling.is_some());
//     }

//     #[test]
//     fn test_ensure_initialized_idempotent() {
//         let config = PromSketchConfig::default();
//         let mut instances = PromSketchInstances::new();

//         instances.ensure_initialized(PromSketchType::EHUniv, &config);
//         let ptr1 = instances.eh_univ.as_ref().unwrap() as *const EHUnivOptimized;

//         // Calling again should not replace the instance.
//         instances.ensure_initialized(PromSketchType::EHUniv, &config);
//         let ptr2 = instances.eh_univ.as_ref().unwrap() as *const EHUnivOptimized;
//         assert_eq!(ptr1, ptr2);
//     }

//     #[test]
//     fn test_mem_series_insert_updates_oldest() {
//         let mut series = PromSketchMemSeries::new("test_metric".to_string());
//         assert_eq!(series.oldest_timestamp, -1);

//         let config = PromSketchConfig::default();
//         series
//             .sketch_instances
//             .ensure_initialized(PromSketchType::EHKLL, &config);

//         series.insert(100, 1.0);
//         assert_eq!(series.oldest_timestamp, 100);

//         series.insert(50, 2.0);
//         // oldest_timestamp should not change once set
//         assert_eq!(series.oldest_timestamp, 100);
//     }
// }
