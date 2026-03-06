use clap::{Parser, ValueEnum};
use elasticsearch::{http::transport::Transport, BulkParts, Elasticsearch};
use flate2::read::GzDecoder;
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Uniform};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

const RNG_SEED: u64 = 42;

// Sample data for realistic values
const SEARCH_PHRASES: &[&str] = &[
    "", "", "", "", "", // Most are empty
    "clickhouse benchmark",
    "analytics database",
    "olap performance",
    "data warehouse",
    "columnar storage",
    "real-time analytics",
    "big data processing",
];

const URLS: &[&str] = &[
    "https://example.com/",
    "https://example.com/products",
    "https://example.com/about",
    "https://google.com/search?q=test",
    "https://example.com/blog/analytics",
    "https://news.example.com/article/123",
];

const REFERERS: &[&str] = &[
    "",
    "https://google.com/",
    "https://bing.com/",
    "https://duckduckgo.com/",
    "https://example.com/",
];

const MOBILE_PHONE_MODELS: &[&str] = &[
    "", "", "", // Most are empty (desktop)
    "iPhone",
    "Samsung Galaxy",
    "Pixel",
    "OnePlus",
];

const TITLES: &[&str] = &[
    "Home Page",
    "Product Catalog",
    "About Us",
    "Contact",
    "Blog - Analytics Tips",
    "Google Search Results",
    "Dashboard",
];

const BROWSER_LANGUAGES: &[&str] = &["en", "de", "fr", "es", "ru", "zh", "jp"];
const BROWSER_COUNTRIES: &[&str] = &["US", "DE", "FR", "GB", "RU", "CN", "JP"];

/// ClickBench-compatible hits record
/// Schema from: https://github.com/ClickHouse/ClickBench
#[derive(Serialize)]
struct HitsRecord {
    #[serde(rename = "WatchID")]
    watch_id: u64,
    #[serde(rename = "JavaEnable")]
    java_enable: u8,
    #[serde(rename = "Title")]
    title: String,
    #[serde(rename = "GoodEvent")]
    good_event: i16,
    #[serde(rename = "EventTime")]
    event_time: String,
    #[serde(rename = "EventDate")]
    event_date: String,
    #[serde(rename = "CounterID")]
    counter_id: u32,
    #[serde(rename = "ClientIP")]
    client_ip: u32,
    #[serde(rename = "RegionID")]
    region_id: u32,
    #[serde(rename = "UserID")]
    user_id: u64,
    #[serde(rename = "CounterClass")]
    counter_class: i8,
    #[serde(rename = "OS")]
    os: u8,
    #[serde(rename = "UserAgent")]
    user_agent: u8,
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "Referer")]
    referer: String,
    #[serde(rename = "IsRefresh")]
    is_refresh: u8,
    #[serde(rename = "RefererCategoryID")]
    referer_category_id: u16,
    #[serde(rename = "RefererRegionID")]
    referer_region_id: u32,
    #[serde(rename = "URLCategoryID")]
    url_category_id: u16,
    #[serde(rename = "URLRegionID")]
    url_region_id: u32,
    #[serde(rename = "ResolutionWidth")]
    resolution_width: u16,
    #[serde(rename = "ResolutionHeight")]
    resolution_height: u16,
    #[serde(rename = "ResolutionDepth")]
    resolution_depth: u8,
    #[serde(rename = "FlashMajor")]
    flash_major: u8,
    #[serde(rename = "FlashMinor")]
    flash_minor: u8,
    #[serde(rename = "FlashMinor2")]
    flash_minor2: String,
    #[serde(rename = "NetMajor")]
    net_major: u8,
    #[serde(rename = "NetMinor")]
    net_minor: u8,
    #[serde(rename = "UserAgentMajor")]
    user_agent_major: u16,
    #[serde(rename = "UserAgentMinor")]
    user_agent_minor: String,
    #[serde(rename = "CookieEnable")]
    cookie_enable: u8,
    #[serde(rename = "JavascriptEnable")]
    javascript_enable: u8,
    #[serde(rename = "IsMobile")]
    is_mobile: u8,
    #[serde(rename = "MobilePhone")]
    mobile_phone: u8,
    #[serde(rename = "MobilePhoneModel")]
    mobile_phone_model: String,
    #[serde(rename = "Params")]
    params: String,
    #[serde(rename = "IPNetworkID")]
    ip_network_id: u32,
    #[serde(rename = "TraficSourceID")]
    trafic_source_id: i8,
    #[serde(rename = "SearchEngineID")]
    search_engine_id: u16,
    #[serde(rename = "SearchPhrase")]
    search_phrase: String,
    #[serde(rename = "AdvEngineID")]
    adv_engine_id: u8,
    #[serde(rename = "IsArtifical")]
    is_artifical: u8,
    #[serde(rename = "WindowClientWidth")]
    window_client_width: u16,
    #[serde(rename = "WindowClientHeight")]
    window_client_height: u16,
    #[serde(rename = "ClientTimeZone")]
    client_time_zone: i16,
    #[serde(rename = "ClientEventTime")]
    client_event_time: String,
    #[serde(rename = "SilverlightVersion1")]
    silverlight_version1: u8,
    #[serde(rename = "SilverlightVersion2")]
    silverlight_version2: u8,
    #[serde(rename = "SilverlightVersion3")]
    silverlight_version3: u32,
    #[serde(rename = "SilverlightVersion4")]
    silverlight_version4: u16,
    #[serde(rename = "PageCharset")]
    page_charset: String,
    #[serde(rename = "CodeVersion")]
    code_version: u32,
    #[serde(rename = "IsLink")]
    is_link: u8,
    #[serde(rename = "IsDownload")]
    is_download: u8,
    #[serde(rename = "IsNotBounce")]
    is_not_bounce: u8,
    #[serde(rename = "FUniqID")]
    f_uniq_id: u64,
    #[serde(rename = "OriginalURL")]
    original_url: String,
    #[serde(rename = "HID")]
    hid: u32,
    #[serde(rename = "IsOldCounter")]
    is_old_counter: u8,
    #[serde(rename = "IsEvent")]
    is_event: u8,
    #[serde(rename = "IsParameter")]
    is_parameter: u8,
    #[serde(rename = "DontCountHits")]
    dont_count_hits: u8,
    #[serde(rename = "WithHash")]
    with_hash: u8,
    #[serde(rename = "HitColor")]
    hit_color: String,
    #[serde(rename = "LocalEventTime")]
    local_event_time: String,
    #[serde(rename = "Age")]
    age: u8,
    #[serde(rename = "Sex")]
    sex: u8,
    #[serde(rename = "Income")]
    income: u8,
    #[serde(rename = "Interests")]
    interests: u16,
    #[serde(rename = "Robotness")]
    robotness: u8,
    #[serde(rename = "RemoteIP")]
    remote_ip: u32,
    #[serde(rename = "WindowName")]
    window_name: i32,
    #[serde(rename = "OpenerName")]
    opener_name: i32,
    #[serde(rename = "HistoryLength")]
    history_length: i16,
    #[serde(rename = "BrowserLanguage")]
    browser_language: String,
    #[serde(rename = "BrowserCountry")]
    browser_country: String,
    #[serde(rename = "SocialNetwork")]
    social_network: String,
    #[serde(rename = "SocialAction")]
    social_action: String,
    #[serde(rename = "HTTPError")]
    http_error: u16,
    #[serde(rename = "SendTiming")]
    send_timing: u32,
    #[serde(rename = "DNSTiming")]
    dns_timing: u32,
    #[serde(rename = "ConnectTiming")]
    connect_timing: u32,
    #[serde(rename = "ResponseStartTiming")]
    response_start_timing: u32,
    #[serde(rename = "ResponseEndTiming")]
    response_end_timing: u32,
    #[serde(rename = "FetchTiming")]
    fetch_timing: u32,
    #[serde(rename = "SocialSourceNetworkID")]
    social_source_network_id: u8,
    #[serde(rename = "SocialSourcePage")]
    social_source_page: String,
    #[serde(rename = "ParamPrice")]
    param_price: i64,
    #[serde(rename = "ParamOrderID")]
    param_order_id: String,
    #[serde(rename = "ParamCurrency")]
    param_currency: String,
    #[serde(rename = "ParamCurrencyID")]
    param_currency_id: u16,
    #[serde(rename = "OpenstatServiceName")]
    openstat_service_name: String,
    #[serde(rename = "OpenstatCampaignID")]
    openstat_campaign_id: String,
    #[serde(rename = "OpenstatAdID")]
    openstat_ad_id: String,
    #[serde(rename = "OpenstatSourceID")]
    openstat_source_id: String,
    #[serde(rename = "UTMSource")]
    utm_source: String,
    #[serde(rename = "UTMMedium")]
    utm_medium: String,
    #[serde(rename = "UTMCampaign")]
    utm_campaign: String,
    #[serde(rename = "UTMContent")]
    utm_content: String,
    #[serde(rename = "UTMTerm")]
    utm_term: String,
    #[serde(rename = "FromTag")]
    from_tag: String,
    #[serde(rename = "HasGCLID")]
    has_gclid: u8,
    #[serde(rename = "RefererHash")]
    referer_hash: u64,
    #[serde(rename = "URLHash")]
    url_hash: u64,
    #[serde(rename = "CLID")]
    clid: u32,
}

// H2O Row Structure
#[derive(Debug, Deserialize, Serialize)]
struct H2oRow {
    id1: String,
    id2: String,
    id3: String,
    id4: i32,
    id5: i32,
    id6: i32,
    v1: i32,
    v2: i32,
    v3: f64,
}

// H2O Elasticsearch Document (with timestamp)
#[derive(Debug, Serialize)]
struct H2oEsDoc {
    timestamp: i64,
    id1: String,
    id2: String,
    id3: String,
    id4: i32,
    id5: i32,
    id6: i32,
    v1: i32,
    v2: i32,
    v3: f64,
}

fn random_choice<'a, T>(items: &'a [T], rng: &mut SmallRng) -> &'a T {
    let dist = Uniform::new(0, items.len());
    &items[dist.sample(rng)]
}

fn generate_hits_record(
    rng: &mut SmallRng,
    watch_id_counter: &mut u64,
    user_ids: &[u64],
    counter_ids: &[u32],
) -> HitsRecord {
    let now = chrono::Utc::now();
    let event_time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    let event_date = now.format("%Y-%m-%d").to_string();

    let dist_bool = Uniform::new(0u8, 2);
    let dist_u8 = Uniform::new(0u8, 100);
    let dist_u16 = Uniform::new(0u16, 10000);
    let dist_u32 = Uniform::new(0u32, 100000);
    let dist_u64 = Uniform::new(0u64, 1000000);
    let dist_region = Uniform::new(1u32, 250);
    let dist_resolution_w = Uniform::new(320u16, 2560);
    let dist_resolution_h = Uniform::new(240u16, 1440);
    let dist_age = Uniform::new(0u8, 100);
    let dist_timing = Uniform::new(0u32, 5000);

    *watch_id_counter += 1;
    let watch_id = *watch_id_counter;

    let user_id = *random_choice(user_ids, rng);
    let counter_id = *random_choice(counter_ids, rng);
    let url = random_choice(URLS, rng).to_string();
    let referer = random_choice(REFERERS, rng).to_string();
    let search_phrase = random_choice(SEARCH_PHRASES, rng).to_string();
    let mobile_phone_model = random_choice(MOBILE_PHONE_MODELS, rng).to_string();
    let is_mobile = if mobile_phone_model.is_empty() { 0 } else { 1 };
    let title = random_choice(TITLES, rng).to_string();

    let url_hash = url.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
    let referer_hash = referer.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

    HitsRecord {
        watch_id,
        java_enable: dist_bool.sample(rng),
        title,
        good_event: 1,
        event_time: event_time.clone(),
        event_date,
        counter_id,
        client_ip: dist_u32.sample(rng),
        region_id: dist_region.sample(rng),
        user_id,
        counter_class: (dist_bool.sample(rng) as i8),
        os: dist_u8.sample(rng) % 20,
        user_agent: dist_u8.sample(rng) % 50,
        url,
        referer,
        is_refresh: dist_bool.sample(rng),
        referer_category_id: dist_u16.sample(rng) % 20,
        referer_region_id: dist_region.sample(rng),
        url_category_id: dist_u16.sample(rng) % 20,
        url_region_id: dist_region.sample(rng),
        resolution_width: dist_resolution_w.sample(rng),
        resolution_height: dist_resolution_h.sample(rng),
        resolution_depth: 24,
        flash_major: dist_u8.sample(rng) % 20,
        flash_minor: dist_u8.sample(rng) % 10,
        flash_minor2: String::new(),
        net_major: dist_u8.sample(rng) % 5,
        net_minor: dist_u8.sample(rng) % 10,
        user_agent_major: dist_u16.sample(rng) % 100,
        user_agent_minor: String::new(),
        cookie_enable: 1,
        javascript_enable: 1,
        is_mobile,
        mobile_phone: if is_mobile == 1 { dist_u8.sample(rng) % 10 } else { 0 },
        mobile_phone_model,
        params: String::new(),
        ip_network_id: dist_u32.sample(rng),
        trafic_source_id: ((dist_u8.sample(rng) % 10) as i8) - 1,
        search_engine_id: dist_u16.sample(rng) % 30,
        search_phrase,
        adv_engine_id: dist_u8.sample(rng) % 30,
        is_artifical: 0,
        window_client_width: dist_resolution_w.sample(rng),
        window_client_height: dist_resolution_h.sample(rng),
        client_time_zone: ((dist_u8.sample(rng) % 24) as i16) - 12,
        client_event_time: event_time.clone(),
        silverlight_version1: 0,
        silverlight_version2: 0,
        silverlight_version3: 0,
        silverlight_version4: 0,
        page_charset: "UTF-8".to_string(),
        code_version: dist_u32.sample(rng) % 1000,
        is_link: dist_bool.sample(rng),
        is_download: dist_bool.sample(rng),
        is_not_bounce: dist_bool.sample(rng),
        f_uniq_id: dist_u64.sample(rng),
        original_url: String::new(),
        hid: dist_u32.sample(rng),
        is_old_counter: 0,
        is_event: dist_bool.sample(rng),
        is_parameter: 0,
        dont_count_hits: dist_bool.sample(rng),
        with_hash: 0,
        hit_color: "E".to_string(),
        local_event_time: event_time,
        age: dist_age.sample(rng),
        sex: dist_bool.sample(rng),
        income: dist_u8.sample(rng) % 5,
        interests: dist_u16.sample(rng),
        robotness: dist_bool.sample(rng),
        remote_ip: dist_u32.sample(rng),
        window_name: 0,
        opener_name: 0,
        history_length: (dist_u8.sample(rng) % 20) as i16,
        browser_language: random_choice(BROWSER_LANGUAGES, rng).to_string(),
        browser_country: random_choice(BROWSER_COUNTRIES, rng).to_string(),
        social_network: String::new(),
        social_action: String::new(),
        http_error: 0,
        send_timing: dist_timing.sample(rng),
        dns_timing: dist_timing.sample(rng) % 100,
        connect_timing: dist_timing.sample(rng) % 200,
        response_start_timing: dist_timing.sample(rng),
        response_end_timing: dist_timing.sample(rng),
        fetch_timing: dist_timing.sample(rng),
        social_source_network_id: 0,
        social_source_page: String::new(),
        param_price: 0,
        param_order_id: String::new(),
        param_currency: String::new(),
        param_currency_id: 0,
        openstat_service_name: String::new(),
        openstat_campaign_id: String::new(),
        openstat_ad_id: String::new(),
        openstat_source_id: String::new(),
        utm_source: String::new(),
        utm_medium: String::new(),
        utm_campaign: String::new(),
        utm_content: String::new(),
        utm_term: String::new(),
        from_tag: String::new(),
        has_gclid: 0,
        referer_hash,
        url_hash,
        clid: 0,
    }
}

#[derive(Clone, ValueEnum, Debug)]
enum Mode {
    /// Generate synthetic fake data and send to Kafka
    Fake,
    /// Read ClickBench JSON data from file and send to Kafka
    Clickbench,
    /// Read H2O CSV data from file and send to Kafka
    H2o,
    /// Read H2O CSV data from file and send to Elasticsearch
    H2oElasticsearch,
}

#[derive(Parser)]
#[command(name = "data_exporter")]
#[command(about = "ClickBench-compatible data exporter to Kafka (fake, clickbench, or h2o data)")]
struct Args {
    #[arg(long, value_enum, env = "DATA_MODE", help = "Data source mode (fake, clickbench, or h2o)")]
    mode: Mode,

    #[arg(long, env = "KAFKA_BROKER", help = "Kafka broker address")]
    kafka_broker: Option<String>,

    #[arg(long, env = "KAFKA_TOPIC", help = "Kafka topic name")]
    kafka_topic: Option<String>,

    #[arg(long, env = "DATA_BATCH_SIZE", help = "Number of records per batch")]
    batch_size: usize,

    #[arg(long, env = "DATA_FREQUENCY_SECONDS", help = "Seconds between batches (fake mode only)")]
    frequency: u64,

    #[arg(long, env = "FAKE_NUM_USERS", help = "Number of unique users (fake mode only)")]
    num_users: usize,

    #[arg(long, env = "FAKE_NUM_COUNTERS", help = "Number of unique counters (fake mode only)")]
    num_counters: usize,

    #[arg(long, env = "DEBUG_PRINT", default_value = "false", help = "Print records to console")]
    debug_print: bool,

    #[arg(long, env = "TOTAL_RECORDS", help = "Total records to generate/send (0 = infinite/all)")]
    total_records: Option<u64>,

    #[arg(long, env = "CLICKBENCH_FILE", help = "Path to hits.json or hits.json.gz (clickbench mode)")]
    input_file: Option<String>,

    #[arg(long, env = "INPUT_FILE", help = "Path to input file (h2o/general usage)")]
    general_input_file: Option<PathBuf>,

    #[arg(
        long,
        env = "ELASTIC_HOST",
        default_value = "localhost",
        help = "Elasticsearch host"
    )]
    elastic_host: String,

    #[arg(
        long,
        env = "ELASTIC_PORT",
        default_value = "9200",
        help = "Elasticsearch port"
    )]
    elastic_port: u16,

    #[arg(
        long,
        env = "ELASTIC_INDEX_NAME",
        default_value = "h2o_benchmark",
        help = "Elasticsearch index name"
    )]
    elastic_index: String,

    #[arg(
        long,
        env = "ELASTIC_API_KEY",
        help = "Elasticsearch API key (optional)"
    )]
    elastic_api_key: Option<String>,
}

async fn run_fake_mode(args: &Args, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let user_dist = Uniform::new(1u64, u64::MAX / 2);
    let counter_dist = Uniform::new(1u32, 1000);

    let user_ids: Vec<u64> = (0..args.num_users)
        .map(|_| user_dist.sample(&mut rng))
        .collect();

    let mut counter_ids: Vec<u32> = (0..args.num_counters)
        .map(|_| counter_dist.sample(&mut rng))
        .collect();
    counter_ids.push(62); // Required for ClickBench queries 37-43

    println!(
        "Generated {} unique users and {} unique counters",
        user_ids.len(),
        counter_ids.len()
    );
    println!(
        "Generating {} records per batch every {} second(s)",
        args.batch_size, args.frequency
    );

    let mut watch_id_counter: u64 = 0;
    let mut total_sent: u64 = 0;

    loop {
        for _ in 0..args.batch_size {
            let record = generate_hits_record(&mut rng, &mut watch_id_counter, &user_ids, &counter_ids);
            let record_str = serde_json::to_string(&record)?;

            if args.debug_print {
                println!("{}", record_str);
            }

            let delivery_status = producer
                .send(
                    FutureRecord::to(&args.kafka_topic.as_ref().unwrap())
                        .payload(&record_str)
                        .key(&watch_id_counter.to_string()),
                    Duration::from_secs(0),
                )
                .await;

            if let Err((err, _)) = delivery_status {
                eprintln!("Failed to send message to Kafka: {}", err);
            }

            total_sent += 1;

            if let Some(limit) = args.total_records {
                if limit > 0 && total_sent >= limit {
                    println!("Reached target of {} records. Exiting.", limit);
                    return Ok(());
                }
            }
        }

        println!("Sent batch. Total records: {}", total_sent);

        if args.total_records.map_or(true, |l| l == 0 || total_sent < l) {
            sleep(Duration::from_secs(args.frequency)).await;
        }
    }
}

async fn run_clickbench_mode(args: &Args, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    let input_file = args.input_file.as_deref()
        .ok_or("--input-file is required for clickbench mode")?;

    println!("Reading ClickBench data from: {}", input_file);

    let file = File::open(input_file)?;
    let reader: Box<dyn BufRead> = if input_file.ends_with(".gz") {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut total_sent: u64 = 0;
    let total_limit = args.total_records.unwrap_or(0);
    let mut batch: Vec<(String, String)> = Vec::with_capacity(args.batch_size); // (key, payload)

    // ClickBench dataset has ~100M rows
    let total_rows = if total_limit > 0 { total_limit } else { 99_997_497 };
    let pb = ProgressBar::new(total_rows);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {per_sec} ETA: {eta}")?
        .progress_chars("#>-"));

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Warning: Error reading line: {}", e);
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let key = (total_sent + batch.len() as u64).to_string();
        batch.push((key, line.to_string()));

        if batch.len() >= args.batch_size {
            let futures: Vec<_> = batch
                .iter()
                .map(|(key, payload)| {
                    producer.send(
                        FutureRecord::to(&args.kafka_topic.as_ref().unwrap())
                            .payload(payload)
                            .key(key),
                        Duration::from_secs(5),
                    )
                })
                .collect();

            let results = join_all(futures).await;
            for result in results {
                if let Err((err, _)) = result {
                    eprintln!("Failed to send message to Kafka: {}", err);
                }
            }

            total_sent += batch.len() as u64;
            pb.set_position(total_sent);
            batch.clear();

            if total_limit > 0 && total_sent >= total_limit {
                break;
            }

            if args.frequency > 0 {
                sleep(Duration::from_secs(args.frequency)).await;
            }
        }
    }

    // Send remaining records
    if !batch.is_empty() && (total_limit == 0 || total_sent < total_limit) {
        let futures: Vec<_> = batch
            .iter()
            .map(|(key, payload)| {
                producer.send(
                    FutureRecord::to(&args.kafka_topic.as_ref().unwrap())
                        .payload(payload)
                        .key(key),
                    Duration::from_secs(5),
                )
            })
            .collect();

        let results = join_all(futures).await;
        for result in results {
            if let Err((err, _)) = result {
                eprintln!("Failed to send message to Kafka: {}", err);
            }
        }
        total_sent += batch.len() as u64;
        pb.set_position(total_sent);
    }

    pb.finish_with_message(format!("Done! Sent {} records", total_sent));
    Ok(())
}

async fn run_h2o_mode(args: &Args, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Handle Input File Selection safely
    let file_path = if let Some(path) = &args.general_input_file {
        path.clone()
    } else if let Some(path_str) = &args.input_file {
        PathBuf::from(path_str)
    } else {
        panic!("Input file required for H2O mode (use --input-file or --clickbench-file)");
    };

    println!("Reading H2O data from: {:?}", file_path);
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut batch = Vec::with_capacity(args.batch_size);
    let mut total_sent = 0;

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] {msg}")?);

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() || line.starts_with("id1") { continue; } // Skip header

        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 9 { continue; }

        let row = H2oRow {
            id1: cols[0].to_string(),
            id2: cols[1].to_string(),
            id3: cols[2].to_string(),
            id4: cols[3].parse().unwrap_or(0),
            id5: cols[4].parse().unwrap_or(0),
            id6: cols[5].parse().unwrap_or(0),
            v1: cols[6].parse().unwrap_or(0),
            v2: cols[7].parse().unwrap_or(0),
            v3: cols[8].parse().unwrap_or(0.0),
        };

        let payload = serde_json::to_string(&row)?;
        batch.push(payload);

        // SEND BATCH
        if batch.len() >= args.batch_size {
            let mut futures = Vec::with_capacity(batch.len());

            for payload in batch.iter() {
                let future = producer.send(
                    FutureRecord::to(&args.kafka_topic.as_ref().unwrap()).payload(payload).key(""),
                    Duration::from_secs(5),
                );
                futures.push(future);
            }

            // Wait for all sends to complete
            join_all(futures).await;

            // NOW it is safe to clear the batch, as futures are done
            batch.clear();

            total_sent += args.batch_size;
            pb.set_message(format!("Sent {} records", total_sent));

            if args.frequency > 0 {
                sleep(Duration::from_secs(args.frequency)).await;
            }
        }
    }

    // SEND REMAINING
    if !batch.is_empty() {
        let mut futures = Vec::with_capacity(batch.len());
        let count = batch.len();

        for payload in batch.iter() {
            let future = producer.send(
                FutureRecord::to(&args.kafka_topic.as_ref().unwrap()).payload(payload).key(""),
                Duration::from_secs(5),
            );
            futures.push(future);
        }

        join_all(futures).await;
        batch.clear();

        total_sent += count;
    }

    pb.finish_with_message(format!("Done! Sent {} H2O records", total_sent));
    Ok(())
}

async fn run_h2o_elasticsearch_mode(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let file_path = if let Some(path) = &args.general_input_file {
        path.clone()
    } else if let Some(path_str) = &args.input_file {
        PathBuf::from(path_str)
    } else {
        return Err("Input file required for H2O Elasticsearch mode".into());
    };

    println!("Reading H2O data from: {:?}", file_path);

    // Connect to Elasticsearch with optional API key
    let elastic_url = format!("http://{}:{}", args.elastic_host, args.elastic_port);

    let transport = if let Some(api_key) = &args.elastic_api_key {
        println!("Using API key authentication");
        use elasticsearch::http::headers::HeaderMap;
        use elasticsearch::http::headers::HeaderValue;
        use elasticsearch::http::transport::TransportBuilder;

        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("ApiKey {}", api_key))?,
        );

        TransportBuilder::new(
            elasticsearch::http::transport::SingleNodeConnectionPool::new(elastic_url.parse()?),
        )
        .headers(headers)
        .build()?
    } else {
        println!("No API key provided, connecting without authentication");
        Transport::single_node(&elastic_url)?
    };

    let client = Elasticsearch::new(transport);

    println!("Connected to Elasticsearch at {}", elastic_url);

    // Check if index exists, create if not
    let index_exists = client
        .indices()
        .exists(elasticsearch::indices::IndicesExistsParts::Index(&[
            &args.elastic_index
        ]))
        .send()
        .await?
        .status_code()
        .is_success();

    if !index_exists {
        println!("Creating index: {}", args.elastic_index);
        let create_response = client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                &args.elastic_index,
            ))
            .body(json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "refresh_interval": "30s"
                },
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date", "format": "epoch_millis"},
                        "id1": {"type": "keyword"},
                        "id2": {"type": "keyword"},
                        "id3": {"type": "keyword"},
                        "id4": {"type": "long"},
                        "id5": {"type": "long"},
                        "id6": {"type": "long"},
                        "v1": {"type": "long"},
                        "v2": {"type": "long"},
                        "v3": {"type": "double"}
                    }
                }
            }))
            .send()
            .await?;

        if !create_response.status_code().is_success() {
            let error_text = create_response.text().await?;
            eprintln!("Failed to create index. Error response: {}", error_text);
            return Err("Failed to create index".into());
        }
        println!("Index created successfully");
    } else {
        println!(
            "Index {} already exists, skipping creation",
            args.elastic_index
        );
    }

    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut batch = Vec::with_capacity(args.batch_size);
    let mut total_sent = 0u64;
    let mut row_num = 0i64;

    let base_timestamp =
        chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")?.timestamp_millis();

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] {msg}")?,
    );

    for line in reader.lines() {
        let line = line?;
        if line.is_empty() || line.starts_with("id1") {
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 9 {
            continue;
        }

        // Create document with timestamp
        let doc = H2oEsDoc {
            timestamp: base_timestamp + (row_num * 1000), // Increment by 1 second per row
            id1: cols[0].to_string(),
            id2: cols[1].to_string(),
            id3: cols[2].to_string(),
            id4: cols[3].parse().unwrap_or(0),
            id5: cols[4].parse().unwrap_or(0),
            id6: cols[5].parse().unwrap_or(0),
            v1: cols[6].parse().unwrap_or(0),
            v2: cols[7].parse().unwrap_or(0),
            v3: cols[8].parse().unwrap_or(0.0),
        };

        batch.push(serde_json::to_value(&doc)?);
        row_num += 1;

        // Send batch when full
        if batch.len() >= args.batch_size {
            let mut body: Vec<String> = Vec::with_capacity(batch.len() * 2);

            for doc in &batch {
                body.push(serde_json::to_string(&json!({"index": {}}))?);
                body.push(serde_json::to_string(&doc)?);
            }

            let response = client
                .bulk(BulkParts::Index(&args.elastic_index))
                .body(body)
                .send()
                .await?;

            if !response.status_code().is_success() {
                eprintln!("Bulk indexing error: {:?}", response.text().await?);
            }

            total_sent += batch.len() as u64;
            batch.clear();

            pb.set_message(format!("Indexed {} documents", total_sent));

            if args.frequency > 0 {
                sleep(Duration::from_secs(args.frequency)).await;
            }

            // Check limit
            if let Some(limit) = args.total_records {
                if limit > 0 && total_sent >= limit {
                    break;
                }
            }
        }
    }

    // Send remaining documents
    if !batch.is_empty() {
        let mut body: Vec<String> = Vec::with_capacity(batch.len() * 2);

        for doc in &batch {
            body.push(serde_json::to_string(&json!({"index": {}}))?);
            body.push(serde_json::to_string(&doc)?);
        }

        let response = client
            .bulk(BulkParts::Index(&args.elastic_index))
            .body(body)
            .send()
            .await?;

        if !response.status_code().is_success() {
            eprintln!("Bulk indexing error: {:?}", response.text().await?);
        }

        total_sent += batch.len() as u64;
    }

    // Refresh index
    println!("Refreshing index...");
    client
        .indices()
        .refresh(elasticsearch::indices::IndicesRefreshParts::Index(&[
            &args.elastic_index
        ]))
        .send()
        .await?;

    pb.finish_with_message(format!(
        "Done! Indexed {} H2O documents to Elasticsearch",
        total_sent
    ));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match args.mode {
        Mode::Fake | Mode::Clickbench | Mode::H2o => {
            // Kafka modes - require broker and topic
            let kafka_broker = args
                .kafka_broker
                .as_ref()
                .ok_or("--kafka-broker required for Kafka modes")?;
            let kafka_topic = args
                .kafka_topic
                .as_ref()
                .ok_or("--kafka-topic required for Kafka modes")?;

            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", kafka_broker)
                .set("message.timeout.ms", "30000")
                .set("queue.buffering.max.messages", "100000")
                .set("batch.num.messages", "1000")
                .create()
                .expect("Failed to create Kafka producer");

            println!(
                "Connected to Kafka broker: {}, topic: {}",
                kafka_broker, kafka_topic
            );

            match args.mode {
                Mode::Fake => {
                    println!("Mode: fake (generating synthetic data)");
                    run_fake_mode(&args, &producer).await
                }
                Mode::Clickbench => {
                    println!("Mode: clickbench (reading from file)");
                    run_clickbench_mode(&args, &producer).await
                }
                Mode::H2o => {
                    println!("Mode: h2o (reading from file)");
                    run_h2o_mode(&args, &producer).await
                }
                Mode::H2oElasticsearch => {
                    panic!("Invalid mode after setting up Kafka broker and topic");
                }
            }
        }
        Mode::H2oElasticsearch => {
            println!("Mode: h2o-elasticsearch (direct to Elasticsearch)");
            run_h2o_elasticsearch_mode(&args).await
        }
    }
}
