use std::{
    net::SocketAddr,
    net::Ipv4Addr,
    env,
    sync::Mutex,
};
use hyper::{
    Request,
    Response,
    body::Incoming,
    header::CONTENT_TYPE,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use prometheus::{
    Encoder,
    TextEncoder,
    core::{Desc, Collector},
    proto::MetricFamily,
};
use tokio::net::TcpListener;
use rand_distr::{
    Zipf,
    Normal,
    Uniform,
    Distribution
};
use rand::{
    SeedableRng,
    rngs::SmallRng,
};

type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;

const CONST_1M: u64 = 1_000_000;
const CONST_2M: u64 = 2_000_000;
const CONST_3M: u64 = 3_000_000;

const RNG_SEED: u64 = 0; // seed for rng used by all distributions

const ZIPF_ALPHA: f64 = 1.01; // zipf parameter

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
                    .map(str::trim)                   // drop any surrounding whitespace
                    .filter(|s| !s.is_empty())       // skip empty segments, if any
                    .map(str::parse::<usize>)        // parse each into usize
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

fn compute_labels(
        num_labels: usize,
        num_values_per_label: Vec<usize>,
    ) -> Vec<Vec<String>> {

    // 1. Build values_per_label
    let mut values_per_label = Vec::with_capacity(num_labels);
    for label_idx in 0..num_labels {
        let count = num_values_per_label[label_idx];
        let mut bucket = Vec::with_capacity(count);
        for value_idx in 0..count {
            bucket.push(format!("value_{}_value_{}", label_idx, value_idx));
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
    valuescale: f64, // Max magnitude of random value generation
    dataset: String, // name of dataset (zipf, uniform, normal, dynamic)
    label_value_combinations: Vec<Vec<String>>, // list of label sets for all metrics
    metric_type: String, // gauge or counter
    rng: Mutex<SmallRng>, // seeded rng
    zipf_dist: Option<Zipf<f64>>,
    normal_dist: Option<Normal<f64>>,
    uniform_dist: Option<Uniform<f64>>,
    counter_state: Mutex<f64>, // tracking counter value
    total_samples: Mutex<u64>, // for dynamic distribution only
}

impl FakeCollector {
    fn new(
        valuescale: f64,
        dataset: String,
        num_labels: usize,
        num_values_per_label: String,
        metric_type: String
    ) -> Self {

        let num_values_per_label = get_num_vals_per_label(
            num_values_per_label,
            num_labels
        );
        let label_value_combinations = compute_labels(num_labels, num_values_per_label);
        let mut zipf_dist: Option<Zipf<f64>> = None;
        let mut normal_dist: Option<Normal<f64>> = None;
        let mut uniform_dist: Option<Uniform<f64>> = None;

        // Instantiate required distributions
        if dataset == "zipf" {
            zipf_dist = Some(
                Zipf::new(valuescale, ZIPF_ALPHA)
                .expect("Failed to create Zipf distribution")
            );
        } else if dataset == "normal" {
            let mean: f64 = get_mean(valuescale);
            let sigma: f64 = get_sigma(valuescale); // 99.997% of values will be within 4 std deviations
            normal_dist = Some(
                Normal::new(mean, sigma)
                .expect("Failed to create Normal distribution")
            );
        } else if dataset == "dynamic" {
            let mean: f64 = get_mean(valuescale);
            let sigma: f64 = get_sigma(valuescale); // 99.997% of values will be within 4 std deviations
            normal_dist = Some(
                Normal::new(mean, sigma)
                .expect("Failed to create Normal distribution")
            );
            zipf_dist = Some(
                Zipf::new(valuescale, ZIPF_ALPHA)
                .expect("Failed to create Zipf distribution")
            );
            uniform_dist = Some(
                Uniform::new_inclusive(0.0, valuescale)
                .expect("Failed to create Uniform distribution")
            )
        } else { // uniform distribution
            uniform_dist = Some(
                Uniform::new_inclusive(0.0, valuescale)
                .expect("Failed to create Uniform distribution")
            )
        }

        Self {
            valuescale,
            dataset,
            label_value_combinations,
            metric_type,
            rng: Mutex::new(SmallRng::seed_from_u64(RNG_SEED)),
            zipf_dist,
            normal_dist,
            uniform_dist,
            counter_state: Mutex::new(0.0),
            total_samples: Mutex::new(0),
        }

    }

    fn get_sample(&self) -> f64 {
        let rv: f64;
        let mut samples_mutex = self.total_samples.lock().unwrap(); // lock samples cnt

        if self.dataset == "zipf" {
            rv = if let Some(zipf_dist) = &self.zipf_dist {
                zipf_dist.sample(&mut self.rng.lock().unwrap())
            } else {
                panic!("Zipf distribution not initialized");
            };
        } else if self.dataset == "normal" {
            rv = if let Some(normal_dist) = &self.normal_dist {
                normal_dist.sample(&mut self.rng.lock().unwrap())
            } else {
                panic!("Normal distribution not initialized");
            };
        } else if self.dataset == "uniform" {
            rv = if let Some(uniform_dist) = &self.uniform_dist {
                uniform_dist.sample(&mut self.rng.lock().unwrap())
            } else {
                panic!("Uniform distribution not initialized")
            }
        } else { // Dynamic
            if *samples_mutex < CONST_1M {
                rv = if let Some(zipf_dist) = &self.zipf_dist {
                    zipf_dist.sample(&mut self.rng.lock().unwrap())
                } else {
                    panic!("Zipf distribution not initialized");
                };
            } else if *samples_mutex < CONST_2M {
                rv = if let Some(uniform_dist) = &self.uniform_dist {
                    uniform_dist.sample(&mut self.rng.lock().unwrap())
                } else {
                    panic!("Uniform distribution not initialized")
                }
            } else {
                rv = if let Some(normal_dist) = &self.normal_dist {
                    normal_dist.sample(&mut self.rng.lock().unwrap())
                } else {
                    panic!("Normal distribution not initialized");
                };
            }
        }

        // update total samples
        *samples_mutex = (*samples_mutex + 1) % CONST_3M;
        rv
    }

    // Generates a new random value based on the dataset, updates the counter,
    // and returns the current counter value
    fn get_next_counter_val(&self) -> f64 {
        let random_val: f64 = self.get_sample();
        let mut counter_mutex = self.counter_state.lock().unwrap();
        // Update counter with val
        *counter_mutex += random_val;
        *counter_mutex
    }

    // Gets a metric family containing a counter family with all label_value combos
    fn get_counter_family(&self) -> MetricFamily {
        let mut counter_family = MetricFamily::default();
        counter_family.set_name("fake_metric_total".to_string());
        counter_family.set_help(format!("Generating fake time series data with {} dataset", self.dataset));
        counter_family.set_field_type(prometheus::proto::MetricType::COUNTER);

        for label_value_combination in &self.label_value_combinations {
            let mut metric = prometheus::proto::Metric::default();
            let mut counter = prometheus::proto::Counter::default();
            let mut labels = Vec::new();
            for i in 0..label_value_combination.len() {
                let mut label_and_value = prometheus::proto::LabelPair::default();
                let label_val: &String = &label_value_combination[i];
                label_and_value.set_name(format!("label_{}", i));
                label_and_value.set_value(label_val.to_string());
                labels.push(label_and_value);
            }

            metric.set_label(labels.into());
            counter.set_value(self.get_next_counter_val());
            metric.set_counter(counter);
            counter_family.mut_metric().push(metric);
        }
        counter_family
    }

    // Gets a metric family containing a gauge family with all label_value combos
    fn get_gauge_family(&self) -> MetricFamily {
        let mut gauge_family = MetricFamily::default();
        gauge_family.set_name("fake_metric".to_string());
        gauge_family.set_help(format!("Generating fake time series data with {} dataset", self.dataset));
        gauge_family.set_field_type(prometheus::proto::MetricType::GAUGE);

        for label_value_combination in &self.label_value_combinations {
            let mut metric = prometheus::proto::Metric::default();
            let mut gauge = prometheus::proto::Gauge::default();
            let mut labels = Vec::new();
            for i in 0..label_value_combination.len() {
                let mut label_and_value = prometheus::proto::LabelPair::default();
                let label_val: &String = &label_value_combination[i];
                label_and_value.set_name(format!("label_{}", i));
                label_and_value.set_value(label_val.to_string());
                labels.push(label_and_value);
            }

            metric.set_label(labels.into());
            gauge.set_value(self.get_sample());
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

#[tokio::main]
async fn main() -> Result<(), BoxedErr> {
    // Parse args
    let args: Vec<String> = env::args().collect();
    if args.len() != 8 {
        panic!(
            "HELP: ./fake_exporter <output_dir> <port> <value_scale> <dataset> <num_labels> <num_values_per_label> <metric_type>"
        )
    }
    let _output_dir: String = args[1].clone(); // no output at the moment
    let port: u16 = args[2].parse::<u16>().expect(
        "Args[2] must be valid port"
    );
    let valuescale: f64 = args[3].parse::<f64>().expect(
        "Args[3] must be value scale as <f64>"
    );
    let dataset: String = args[4].clone();
    let num_labels: usize = args[5].parse::<usize>().expect(
        "Args[5] must be num_labels as an int"
    );
    let num_values_per_label: String = args[6].clone();
    let metric_type: String = args[7].clone();

    let fake_collector = Box::new(
        FakeCollector::new(
            valuescale, dataset, num_labels,
            num_values_per_label, metric_type
        )
    );

    // Register collector and start serving
    let _ = prometheus::register(fake_collector);
    let ip = Ipv4Addr::UNSPECIFIED;
    let addr: SocketAddr = (ip, port).into();
    println!("Listening on http://{}", addr);
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
