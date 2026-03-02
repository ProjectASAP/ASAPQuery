use clap::{Parser, ValueEnum};
use hyper::{
    Request, Response, body::Incoming, header::CONTENT_TYPE, server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use prometheus::{
    Encoder, TextEncoder,
    core::{Collector, Desc},
    proto::MetricFamily,
};
use rand::{SeedableRng, rngs::SmallRng};
use rand_distr::{Distribution, Normal, Uniform, Zipf};
use std::{f64::consts::PI, net::Ipv4Addr, net::SocketAddr, sync::Mutex, time::{SystemTime, UNIX_EPOCH}};
use tokio::net::TcpListener;

/// Dataset/pattern types for metric generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Dataset {
    // === Random distribution types ===
    /// Uniform random distribution
    Uniform,
    /// Normal (Gaussian) random distribution
    Normal,
    /// Zipf power-law distribution
    Zipf,
    /// Cycles through Zipf -> Uniform -> Normal
    Dynamic,

    // === Deterministic pattern types (time-based) ===
    /// Constant value (varies per series)
    Constant,
    /// Linearly increasing over time
    LinearUp,
    /// Linearly decreasing over time
    LinearDown,
    /// Sinusoidal wave
    Sine,
    /// Sinusoidal wave with gaussian noise
    SineNoise,
    /// Step function (discrete levels)
    Step,
    /// Baseline with random spikes
    Spiky,
    /// Exponential growth
    ExpUp,
}

impl Dataset {
    /// Returns the pattern label value (for --add-pattern-label flag)
    fn as_label(&self) -> &'static str {
        match self {
            Dataset::Uniform => "uniform",
            Dataset::Normal => "normal",
            Dataset::Zipf => "zipf",
            Dataset::Dynamic => "dynamic",
            Dataset::Constant => "constant",
            Dataset::LinearUp => "linear_up",
            Dataset::LinearDown => "linear_down",
            Dataset::Sine => "sine",
            Dataset::SineNoise => "sine_noise",
            Dataset::Step => "step",
            Dataset::Spiky => "spiky",
            Dataset::ExpUp => "exp_up",
        }
    }
}

type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;

// === Dynamic distribution constants ===
const CONST_1M: u64 = 1_000_000;
const CONST_2M: u64 = 2_000_000;
const CONST_3M: u64 = 3_000_000;

const RNG_SEED: u64 = 0; // seed for rng used by all distributions

const ZIPF_ALPHA: f64 = 1.01; // zipf parameter

// === Pattern timing constants ===
const SINE_PERIOD_SECS: f64 = 120.0;           // 2 minute cycle
const STEP_PERIOD_SECS: f64 = 30.0;            // Step changes every 30s
const LINEAR_WRAP_PERIOD_SECS: f64 = 300.0;    // Linear resets every 5min

// === Pattern variation constants (per-series diversity) ===
const SINE_PHASE_VARIATION: f64 = 0.1;         // Phase offset per series
const SINE_AMPLITUDE_VARIATION: f64 = 0.2;     // Amplitude varies ±20%
const LINEAR_SLOPE_VARIATION: f64 = 0.1;       // Slope varies ±10%
const CONSTANT_NUM_LEVELS: usize = 10;         // 10 distinct constant values

// === Noise/spike constants ===
const NOISE_STDDEV_FRACTION: f64 = 0.1;        // Noise is 10% of signal
const SPIKE_PROBABILITY: f64 = 0.05;           // 5% chance per scrape
const SPIKE_MAGNITUDE: f64 = 5.0;              // Spike is 5x baseline

// === Step function constants ===
const STEP_NUM_LEVELS: usize = 4;              // 4 discrete levels

// === Exponential constants ===
const EXP_GROWTH_RATE: f64 = 0.01;             // Growth rate per second
const EXP_WRAP_PERIOD_SECS: f64 = 300.0;       // Exponential resets every 5min

// Normal distribution mean
fn get_mean(valuescale: f64) -> f64 {
    valuescale / 2.0
}
// Normal distribution standard deviation
fn get_sigma(valuescale: f64) -> f64 {
    valuescale / 8.0
}

// Converts string to vector of usize
fn get_num_vals_per_label(num_values_per_label_str: String, num_labels: usize) -> Vec<usize> {
    let parse = num_values_per_label_str
        .split(',')
        .map(str::trim) // drop any surrounding whitespace
        .filter(|s| !s.is_empty()) // skip empty segments, if any
        .map(str::parse::<usize>) // parse each into usize
        .collect();
    let num_values_per_label: Vec<usize> = match parse {
        Ok(list) => list,
        Err(error) => panic!("Couldn't parse num_values_per_label: {error:?}"),
    };

    let rv: Vec<usize>;

    if num_values_per_label.len() == 1 {
        rv = vec![num_values_per_label[0]; num_labels];
    } else {
        if num_values_per_label.len() != num_labels {
            panic!(
                "Number of num_values_per_label must be equal to num_labels (got {} vs {})",
                num_values_per_label.len(),
                num_labels
            );
        }
        rv = num_values_per_label;
    }

    return rv;
}

fn compute_labels(num_labels: usize, num_values_per_label: Vec<usize>, label_value_prefixes: &Option<Vec<String>>) -> Vec<Vec<String>> {
    // 1. Build values_per_label
    let mut values_per_label = Vec::with_capacity(num_labels);
    for label_idx in 0..num_labels {
        let count = num_values_per_label[label_idx];
        let mut bucket = Vec::with_capacity(count);
        for value_idx in 0..count {
            let value = match label_value_prefixes {
                Some(prefixes) if label_idx < prefixes.len() => {
                    format!("{}_{}", prefixes[label_idx], value_idx)
                }
                _ => format!("value_{}_value_{}", label_idx, value_idx),
            };
            bucket.push(value);
        }
        values_per_label.push(bucket);
    }

    // 2. Compute expected total combinations
    let expected: usize = num_values_per_label.iter().product();

    // 3. Cartesian product helper
    fn cartesian_product(pools: &[Vec<String>]) -> Vec<Vec<String>> {
        let mut result: Vec<Vec<String>> = vec![Vec::new()];
        for pool in pools {
            let mut next = Vec::new();
            for prefix in &result {
                for item in pool {
                    let mut new_prefix = prefix.clone();
                    new_prefix.push(item.clone());
                    next.push(new_prefix);
                }
            }
            result = next;
        }
        result
    }

    // 5. Generate combinations
    let combos = cartesian_product(&values_per_label);
    assert!(
        combos.len() == expected,
        "got {} combinations but expected {}",
        combos.len(),
        expected
    );

    combos
}

struct FakeCollector {
    valuescale: f64,                            // Max magnitude of value generation
    dataset: Dataset,                           // Dataset/pattern type
    label_value_combinations: Vec<Vec<String>>, // list of label sets for all metrics
    metric_type: String,                        // gauge or counter
    metric_name: String,                        // custom metric name
    label_names: Vec<String>,                   // custom label names
    add_pattern_label: bool,                    // whether to add a 'pattern' label to metrics
    rng: Mutex<SmallRng>,                       // seeded rng
    zipf_dist: Option<Zipf<f64>>,
    normal_dist: Option<Normal<f64>>,
    uniform_dist: Option<Uniform<f64>>,
    counter_state: Mutex<f64>,                  // tracking counter value
    total_samples: Mutex<u64>,                  // for dynamic distribution only
}

impl FakeCollector {
    fn new(
        valuescale: f64,
        dataset: Dataset,
        num_labels: usize,
        num_values_per_label: String,
        metric_type: String,
        metric_name: Option<String>,
        label_names: Option<String>,
        label_value_prefixes: Option<String>,
        add_pattern_label: bool,
    ) -> Self {
        let num_values_per_label = get_num_vals_per_label(num_values_per_label, num_labels);
        let prefixes: Option<Vec<String>> = label_value_prefixes
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect());
        let label_value_combinations = compute_labels(num_labels, num_values_per_label, &prefixes);

        // Determine metric name
        let metric_name = match metric_name {
            Some(name) => name,
            None => if metric_type == "counter" { "fake_metric_total".to_string() } else { "fake_metric".to_string() },
        };

        // Determine label names
        let label_names: Vec<String> = match label_names {
            Some(names) => {
                let parsed: Vec<String> = names.split(',').map(|s| s.trim().to_string()).collect();
                if parsed.len() != num_labels {
                    panic!(
                        "Number of label names ({}) must match num_labels ({})",
                        parsed.len(), num_labels
                    );
                }
                parsed
            }
            None => (0..num_labels).map(|i| format!("label_{}", i)).collect(),
        };
        let mut zipf_dist: Option<Zipf<f64>> = None;
        let mut normal_dist: Option<Normal<f64>> = None;
        let mut uniform_dist: Option<Uniform<f64>> = None;

        // Instantiate required distributions based on dataset type
        match dataset {
            Dataset::Zipf => {
                zipf_dist = Some(
                    Zipf::new(valuescale, ZIPF_ALPHA).expect("Failed to create Zipf distribution"),
                );
            }
            Dataset::Normal => {
                let mean = get_mean(valuescale);
                let sigma = get_sigma(valuescale);
                normal_dist =
                    Some(Normal::new(mean, sigma).expect("Failed to create Normal distribution"));
            }
            Dataset::Dynamic => {
                let mean = get_mean(valuescale);
                let sigma = get_sigma(valuescale);
                normal_dist =
                    Some(Normal::new(mean, sigma).expect("Failed to create Normal distribution"));
                zipf_dist = Some(
                    Zipf::new(valuescale, ZIPF_ALPHA).expect("Failed to create Zipf distribution"),
                );
                uniform_dist = Some(
                    Uniform::new_inclusive(0.0, valuescale)
                        .expect("Failed to create Uniform distribution"),
                );
            }
            Dataset::Uniform => {
                uniform_dist = Some(
                    Uniform::new_inclusive(0.0, valuescale)
                        .expect("Failed to create Uniform distribution"),
                );
            }
            Dataset::SineNoise => {
                // Needs normal distribution for noise
                let noise_stddev = valuescale * NOISE_STDDEV_FRACTION;
                normal_dist =
                    Some(Normal::new(0.0, noise_stddev).expect("Failed to create Normal distribution"));
            }
            Dataset::Spiky => {
                // Needs uniform for probability check, normal for spike magnitude variation
                uniform_dist = Some(
                    Uniform::new_inclusive(0.0, 1.0).expect("Failed to create Uniform distribution"),
                );
            }
            // Other patterns don't need distributions
            Dataset::Constant
            | Dataset::LinearUp
            | Dataset::LinearDown
            | Dataset::Sine
            | Dataset::Step
            | Dataset::ExpUp => {}
        }

        Self {
            valuescale,
            dataset,
            label_value_combinations,
            metric_type,
            metric_name,
            label_names,
            add_pattern_label,
            rng: Mutex::new(SmallRng::seed_from_u64(RNG_SEED)),
            zipf_dist,
            normal_dist,
            uniform_dist,
            counter_state: Mutex::new(0.0),
            total_samples: Mutex::new(0),
        }
    }

    /// Get current timestamp in seconds since epoch
    fn get_time_secs() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs_f64()
    }

    /// Generate a sample value for the given series
    /// For random distributions, series_id is ignored
    /// For patterns, series_id is used to create per-series variation
    fn get_sample(&self, series_id: usize) -> f64 {
        match self.dataset {
            // === Random distribution types ===
            Dataset::Zipf => {
                self.zipf_dist
                    .as_ref()
                    .expect("Zipf distribution not initialized")
                    .sample(&mut self.rng.lock().unwrap())
            }
            Dataset::Normal => {
                self.normal_dist
                    .as_ref()
                    .expect("Normal distribution not initialized")
                    .sample(&mut self.rng.lock().unwrap())
            }
            Dataset::Uniform => {
                self.uniform_dist
                    .as_ref()
                    .expect("Uniform distribution not initialized")
                    .sample(&mut self.rng.lock().unwrap())
            }
            Dataset::Dynamic => {
                let mut samples_mutex = self.total_samples.lock().unwrap();
                let rv = if *samples_mutex < CONST_1M {
                    self.zipf_dist
                        .as_ref()
                        .expect("Zipf distribution not initialized")
                        .sample(&mut self.rng.lock().unwrap())
                } else if *samples_mutex < CONST_2M {
                    self.uniform_dist
                        .as_ref()
                        .expect("Uniform distribution not initialized")
                        .sample(&mut self.rng.lock().unwrap())
                } else {
                    self.normal_dist
                        .as_ref()
                        .expect("Normal distribution not initialized")
                        .sample(&mut self.rng.lock().unwrap())
                };
                *samples_mutex = (*samples_mutex + 1) % CONST_3M;
                rv
            }

            // === Deterministic pattern types ===
            Dataset::Constant => {
                // Each series gets a different constant value based on its ID
                let level = series_id % CONSTANT_NUM_LEVELS;
                let base = self.valuescale / CONSTANT_NUM_LEVELS as f64;
                base * (level as f64 + 0.5) // center of each level
            }
            Dataset::LinearUp => {
                let now = Self::get_time_secs();
                // Slope varies per series
                let slope_multiplier = 1.0 + (series_id % 10) as f64 * LINEAR_SLOPE_VARIATION;
                let slope = (self.valuescale / LINEAR_WRAP_PERIOD_SECS) * slope_multiplier;
                (now * slope) % self.valuescale
            }
            Dataset::LinearDown => {
                let now = Self::get_time_secs();
                let slope_multiplier = 1.0 + (series_id % 10) as f64 * LINEAR_SLOPE_VARIATION;
                let slope = (self.valuescale / LINEAR_WRAP_PERIOD_SECS) * slope_multiplier;
                self.valuescale - ((now * slope) % self.valuescale)
            }
            Dataset::Sine => {
                let now = Self::get_time_secs();
                // Phase offset varies per series
                let phase = (series_id % 100) as f64 * SINE_PHASE_VARIATION;
                // Amplitude varies per series (±SINE_AMPLITUDE_VARIATION)
                let amplitude_multiplier = 1.0 + ((series_id % 5) as f64 - 2.0) * SINE_AMPLITUDE_VARIATION;
                let amplitude = (self.valuescale / 2.0) * amplitude_multiplier;
                let offset = self.valuescale / 2.0; // center the wave

                let angle = (2.0 * PI * now / SINE_PERIOD_SECS) + phase;
                offset + amplitude * angle.sin()
            }
            Dataset::SineNoise => {
                let now = Self::get_time_secs();
                let phase = (series_id % 100) as f64 * SINE_PHASE_VARIATION;
                let amplitude_multiplier = 1.0 + ((series_id % 5) as f64 - 2.0) * SINE_AMPLITUDE_VARIATION;
                let amplitude = (self.valuescale / 2.0) * amplitude_multiplier;
                let offset = self.valuescale / 2.0;

                let angle = (2.0 * PI * now / SINE_PERIOD_SECS) + phase;
                let base_value = offset + amplitude * angle.sin();

                // Add gaussian noise
                let noise = self.normal_dist
                    .as_ref()
                    .expect("Normal distribution not initialized for noise")
                    .sample(&mut self.rng.lock().unwrap());

                (base_value + noise).max(0.0) // clamp to non-negative
            }
            Dataset::Step => {
                let now = Self::get_time_secs();
                // Different series have different phase offsets for step timing
                let phase_offset = (series_id % STEP_NUM_LEVELS) as f64 * (STEP_PERIOD_SECS / STEP_NUM_LEVELS as f64);
                let adjusted_time = now + phase_offset;

                // Determine which step level we're at
                let step_index = ((adjusted_time / STEP_PERIOD_SECS) as usize) % STEP_NUM_LEVELS;
                let level_height = self.valuescale / STEP_NUM_LEVELS as f64;

                level_height * (step_index as f64 + 0.5)
            }
            Dataset::Spiky => {
                // Baseline value varies per series
                let baseline = self.valuescale * 0.2 * (1.0 + (series_id % 5) as f64 * 0.1);

                // Check if we should spike
                let should_spike = self.uniform_dist
                    .as_ref()
                    .expect("Uniform distribution not initialized for spike check")
                    .sample(&mut self.rng.lock().unwrap()) < SPIKE_PROBABILITY;

                if should_spike {
                    baseline * SPIKE_MAGNITUDE
                } else {
                    baseline
                }
            }
            Dataset::ExpUp => {
                let now = Self::get_time_secs();
                // Wrap time to avoid overflow
                let wrapped_time = now % EXP_WRAP_PERIOD_SECS;
                // Growth rate varies slightly per series
                let rate = EXP_GROWTH_RATE * (1.0 + (series_id % 5) as f64 * 0.1);

                // Exponential growth, scaled to valuescale
                let raw = (rate * wrapped_time).exp();
                // Normalize to valuescale range
                let max_value = (rate * EXP_WRAP_PERIOD_SECS).exp();
                (raw / max_value) * self.valuescale
            }
        }
    }

    // Generates a new random value based on the dataset, updates the counter,
    // and returns the current counter value
    // Note: Counter support for patterns is not fully implemented (uses series_id=0)
    fn get_next_counter_val(&self, series_id: usize) -> f64 {
        let random_val: f64 = self.get_sample(series_id);
        let mut counter_mutex = self.counter_state.lock().unwrap();
        // Update counter with val
        *counter_mutex += random_val;
        *counter_mutex
    }

    // Gets a metric family containing a counter family with all label_value combos
    // Note: Pattern support for counters is limited - use gauge metric type for patterns
    fn get_counter_family(&self) -> MetricFamily {
        let mut counter_family = MetricFamily::default();
        counter_family.set_name(self.metric_name.clone());
        counter_family.set_help(format!(
            "Generating fake time series data with {:?} dataset",
            self.dataset
        ));
        counter_family.set_field_type(prometheus::proto::MetricType::COUNTER);

        for (series_id, label_value_combination) in self.label_value_combinations.iter().enumerate() {
            let mut metric = prometheus::proto::Metric::default();
            let mut counter = prometheus::proto::Counter::default();
            let mut labels = Vec::new();

            // Add the pattern label if enabled
            if self.add_pattern_label {
                let mut pattern_label = prometheus::proto::LabelPair::default();
                pattern_label.set_name("pattern".to_string());
                pattern_label.set_value(self.dataset.as_label().to_string());
                labels.push(pattern_label);
            }

            for i in 0..label_value_combination.len() {
                let mut label_and_value = prometheus::proto::LabelPair::default();
                let label_val: &String = &label_value_combination[i];
                label_and_value.set_name(self.label_names[i].clone());
                label_and_value.set_value(label_val.to_string());
                labels.push(label_and_value);
            }

            metric.set_label(labels.into());
            counter.set_value(self.get_next_counter_val(series_id));
            metric.set_counter(counter);
            counter_family.mut_metric().push(metric);
        }
        counter_family
    }

    // Gets a metric family containing a gauge family with all label_value combos
    fn get_gauge_family(&self) -> MetricFamily {
        let mut gauge_family = MetricFamily::default();
        gauge_family.set_name(self.metric_name.clone());
        gauge_family.set_help(format!(
            "Generating fake time series data with {:?} dataset",
            self.dataset
        ));
        gauge_family.set_field_type(prometheus::proto::MetricType::GAUGE);

        for (series_id, label_value_combination) in self.label_value_combinations.iter().enumerate() {
            let mut metric = prometheus::proto::Metric::default();
            let mut gauge = prometheus::proto::Gauge::default();
            let mut labels = Vec::new();

            // Add the pattern label if enabled
            if self.add_pattern_label {
                let mut pattern_label = prometheus::proto::LabelPair::default();
                pattern_label.set_name("pattern".to_string());
                pattern_label.set_value(self.dataset.as_label().to_string());
                labels.push(pattern_label);
            }

            // Add the regular labels
            for i in 0..label_value_combination.len() {
                let mut label_and_value = prometheus::proto::LabelPair::default();
                let label_val: &String = &label_value_combination[i];
                label_and_value.set_name(self.label_names[i].clone());
                label_and_value.set_value(label_val.to_string());
                labels.push(label_and_value);
            }

            metric.set_label(labels.into());
            gauge.set_value(self.get_sample(series_id));
            metric.set_gauge(gauge);
            gauge_family.mut_metric().push(metric);
        }
        gauge_family
    }
}

// Interface used by prometheus
impl Collector for FakeCollector {
    fn desc(&self) -> Vec<&Desc> {
        // Return empty vec initially
        Vec::new()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut metric_families = Vec::new();

        if self.metric_type == "counter" {
            let counter_family = self.get_counter_family();
            metric_families.push(counter_family);
        } else if self.metric_type == "gauge" {
            let gauge_family = self.get_gauge_family();
            metric_families.push(gauge_family);
        } else {
            panic!("Metric type must be one of either 'counter' or 'gauge'")
        }

        metric_families
    }
}

async fn serve_req(_req: Request<Incoming>) -> Result<Response<String>, BoxedErr> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather(); // Calls collect() method
    let body = encoder.encode_to_string(&metric_families)?;
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(body)?;

    Ok(response)
}

#[derive(Parser)]
#[command(name = "fake_exporter")]
#[command(about = "A Prometheus fake exporter for generating test metrics")]
struct Args {
    #[arg(long, help = "Port to serve metrics on")]
    port: u16,

    #[arg(long, help = "Maximum scale for generated values")]
    valuescale: i32,

    #[arg(long, value_enum, help = "Dataset/pattern type for value generation")]
    dataset: Dataset,

    #[arg(long, help = "Number of labels per metric")]
    num_labels: usize,

    #[arg(long, help = "Comma-separated list of number of values per label")]
    num_values_per_label: String,

    #[arg(long, help = "Metric type (gauge or counter)")]
    metric_type: String,

    #[arg(long, help = "Custom metric name (default: fake_metric for gauge, fake_metric_total for counter)")]
    metric_name: Option<String>,

    #[arg(long, help = "Comma-separated custom label names (must match num-labels count)")]
    label_names: Option<String>,

    #[arg(long, help = "Comma-separated prefixes for label values (e.g. 'region,svc,inst' produces region_0, svc_0, inst_0)")]
    label_value_prefixes: Option<String>,

    #[arg(long, default_value = "false", help = "Add 'pattern' label to metrics with dataset name")]
    add_pattern_label: bool,
}

#[tokio::main]
async fn main() -> Result<(), BoxedErr> {
    let args = Args::parse();

    let fake_collector = Box::new(FakeCollector::new(
        args.valuescale as f64,
        args.dataset,
        args.num_labels,
        args.num_values_per_label,
        args.metric_type,
        args.metric_name,
        args.label_names,
        args.label_value_prefixes,
        args.add_pattern_label,
    ));

    // Register collector and start serving
    let _ = prometheus::register(fake_collector);
    let ip = Ipv4Addr::UNSPECIFIED;
    let addr: SocketAddr = (ip, args.port).into();
    println!("Fake exporter started on port {}", args.port);
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let service = service_fn(serve_req);
        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            eprintln!("server error: {:?}", err);
        };
    }
}
