use crate::util::HydraConfig;
use tokio::process::Command;

/// Parse scrape interval string (e.g., "5s", "10m") to seconds as integer
fn parse_scrape_interval_to_seconds(interval: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let interval = interval.trim();
    if interval.ends_with('s') {
        Ok(interval.trim_end_matches('s').parse()?)
    } else if interval.ends_with('m') {
        let minutes: u32 = interval.trim_end_matches('m').parse()?;
        Ok(minutes * 60)
    } else {
        // If no suffix, assume it's already in seconds
        let value: u32 = interval.parse()?;
        Ok(value)
    }
}

/** CONSTANTS - matching the Python infrastructure */
// These match constants.py values
// Note: ASAP_DOCKER_COMPOSE_PATH is constructed dynamically in asap_up/asap_down functions
const _CLOUDLAB_HOME_DIR: &str = "/scratch/sketch_db_for_prometheus";
const FLINK_OUTPUT_TOPIC: &str = "flink_output";
const QUERY_ENGINE_RS_CONTAINER_NAME: &str = "sketchdb-queryengine-rust";
const CONTROLLER_CONTAINER_NAME: &str = "sketchdb-controller";
const ARROYOSKETCH_CONTAINER_NAME: &str = "asap-arroyosketch";

/// Get the ProjectASAP root directory based on the binary's location
/// Binary is at: ProjectASAP/Utilities/asap-cli/target/debug/asap-cli
/// So we go up 4 levels: debug -> target -> asap-cli -> Utilities -> ProjectASAP
fn get_project_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let binary_path = std::env::current_exe()?;

    let project_root = binary_path
        .parent() // Remove 'asap-cli' binary
        .and_then(|p| p.parent()) // Remove 'debug'
        .and_then(|p| p.parent()) // Remove 'target'
        .and_then(|p| p.parent()) // Remove 'asap-cli'
        .and_then(|p| p.parent()) // Remove 'Utilities'
        .ok_or("Failed to determine project root from binary location")?;

    Ok(project_root.to_path_buf())
}

pub struct ControllerComposeArgs {
    template_path: String,
    compose_output_path: String,
    controller_dir: String,
    container_name: String,
    controller_config_path: String,
    controller_output_dir: String,
    prometheus_scrape_interval: String,
    streaming_engine: String,
}

pub struct QueryEngineComposeArgs {
    template_path: String,
    compose_output_path: String,
    query_engine_dir: String,
    container_name: String,
    experiment_output_dir: String,
    controller_remote_output_dir: String,
    kafka_topic: String,
    input_format: String,
    prometheus_scrape_interval: String,
    log_level: String,
    streaming_engine: String,
    query_language: String,
    kafka_host: String,
    prometheus_host: String,
    prometheus_port: String,
    compress_json: bool,
    profile_query_engine: bool,
    forward_unsupported_queries: bool,
    manual: bool,
    kafka_proxy_container_name: String,
    http_port: String,
    lock_strategy: String,
    dump_precomputes: bool,
}

pub struct FakeExporterComposeArgs {
    template_path: String,
    compose_output_path: String,
    fake_exporter_dir: String,
    container_name: String,
    port: u16,
    valuescale: u32,
    dataset: String,
    num_labels: u8,
    num_values_per_label: u16,
    metric_type: String,
    experiment_output_dir: String,
    exporter_output_dir: String,
}

pub struct ArroyoSketchComposeArgs {
    template_path: String,
    compose_output_path: String,
    arroyosketch_dir: String,
    container_name: String,
    controller_output_dir: String,
    arroyosketch_output_dir: String,
    prometheus_base_port: u16,
    prometheus_path: String,
    prometheus_bind_ip: String,
    parallelism: u32,
    output_kafka_topic: String,
    output_format: String,
    pipeline_name: String,
    arroyo_url: String,
    bootstrap_servers: String,
}

/// Generate query engine docker-compose file using the Python script
/// This matches the Python infrastructure's QueryEngineRustService._start_containerized method
pub async fn generate_query_engine_compose(
    hydra_config: &HydraConfig,
    experiment_outputs_abs_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get project root based on binary location
    let project_root = get_project_root()?;
    let code_dir = project_root.to_string_lossy();

    // Paths for template and script
    let queryengine_dir = format!("{}/QueryEngineRust", code_dir);
    let template_path = format!("{}/query-engine-rust-cli-compose.yml.j2", queryengine_dir);
    let helper_script = format!("{}/Utilities/experiments/generate_queryengine_compose.py", code_dir);

    // Compose file output path (inside ProjectASAP)
    let compose_output_path = format!("{}/Utilities/docker/generated_compose_files/query-engine-compose.yml", code_dir);

    // Ensure compose output directory exists
    tokio::fs::create_dir_all(format!("{}/Utilities/docker/generated_compose_files", code_dir)).await?;

    // Experiment output paths (outside ProjectASAP, absolute paths for Docker volume mounts)
    let controller_remote_output_dir = format!("{}/controller_output", experiment_outputs_abs_path);
    let experiment_output_dir = format!("{}/sketchdb", experiment_outputs_abs_path);
    let output_dir = format!("{}/query_engine_output", experiment_output_dir);

    // Extract values from HydraConfig with defaults matching Python code
    // let streaming_engine = hydra_config.streaming.engine
    //     .as_ref()
    //     .unwrap_or(&"flink".to_string())
    //     .clone();
    // CLI only uses arroyo for now
    println!("Overriding streaming engine with 'arroyo' (asap-cli only supports arroyo for now)");
    let streaming_engine = String::from("arroyo");

    // Use flink_output_format for input_format (this is what Python does)
    let flink_output_format = hydra_config.streaming.flink_output_format
        .as_ref()
        .unwrap_or(&"json".to_string())
        .clone();

    let log_level = hydra_config.logging.level
        .as_ref()
        .unwrap_or(&"INFO".to_string())
        .clone();

    let prometheus_scrape_interval_str = hydra_config.prometheus.scrape_interval
        .as_ref()
        .unwrap_or(&"5s".to_string())
        .clone();

    // Parse scrape interval to seconds for the Python script
    let prometheus_scrape_interval = parse_scrape_interval_to_seconds(&prometheus_scrape_interval_str)?;

    // Based on Python's COMPRESS_JSON = True constant
    let compress_json = true;
    let profile_query_engine = hydra_config.profiling.query_engine.unwrap_or(false);
    // let forward_unsupported_queries = hydra_config.streaming.forward_unsupported_queries.unwrap_or(true);
    let forward_unsupported_queries = true; // always true for now
    let manual = hydra_config.manual.query_engine.unwrap_or(false);

    // Read query engine config with defaults matching config.yaml
    let lock_strategy = hydra_config.query_engine.lock_strategy
        .clone()
        .unwrap_or_else(|| "per-key".to_string());
    let dump_precomputes = hydra_config.query_engine.dump_precomputes.unwrap_or(false);

    // Should be 10.10.1.1 for cloudlab, but probably should not be localhost ever
    let kafka_host = "10.10.1.1".to_string();
    let prometheus_host = "10.10.1.1".to_string();
    let prometheus_port = "9090".to_string();  // Hardcoded for CloudLab, will be configurable via CLI later

    let args = QueryEngineComposeArgs {
        template_path,
        compose_output_path: compose_output_path.to_string(),
        query_engine_dir: queryengine_dir,
        container_name: QUERY_ENGINE_RS_CONTAINER_NAME.to_string(),
        experiment_output_dir: output_dir.clone(),
        controller_remote_output_dir,
        kafka_topic: FLINK_OUTPUT_TOPIC.to_string(),
        input_format: flink_output_format,
        prometheus_scrape_interval: prometheus_scrape_interval.to_string(),
        log_level,
        streaming_engine,
        query_language: "PROMQL".to_string(),  // Hardcoded to PROMQL for asap-cli
        kafka_host,
        prometheus_host,
        prometheus_port,
        compress_json,
        profile_query_engine,
        forward_unsupported_queries,
        manual,
        kafka_proxy_container_name: "sketchdb-kafka-proxy".to_string(),
        http_port: "8088".to_string(),
        lock_strategy,
        dump_precomputes,
    };

    // Call the Python script to generate the compose file
    call_generate_queryengine_compose_script(args, helper_script).await
}

/// Generate controller docker-compose file using the Python script
/// This matches the Python infrastructure's ControllerService._start_containerized method
pub async fn generate_controller_compose(
    hydra_config: &HydraConfig,
    _experiment_name: &str,
    controller_client_config: &str,
    experiment_outputs_abs_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get project root based on binary location
    let project_root = get_project_root()?;
    let code_dir = project_root.to_string_lossy();

    // Paths for template and script
    let controller_dir = format!("{}/Controller", code_dir);
    let template_path = format!("{}/controller-cli-compose.yml.j2", controller_dir);
    let helper_script = format!("{}/Utilities/experiments/generate_controller_compose.py", code_dir);

    // Compose file output path (inside ProjectASAP)
    let compose_output_path = format!("{}/Utilities/docker/generated_compose_files/controller-compose.yml", code_dir);

    // Ensure compose output directory exists
    tokio::fs::create_dir_all(format!("{}/Utilities/docker/generated_compose_files", code_dir)).await?;

    // Controller output directory (outside ProjectASAP, absolute path for Docker volume mounts)
    let controller_remote_output_dir = format!("{}/controller_output", experiment_outputs_abs_path);

    // Extract values from HydraConfig with defaults
    // CLI only uses arroyo for now
    println!("Overriding streaming engine with 'arroyo' (asap-cli only supports arroyo for now)");
    let streaming_engine = String::from("arroyo");

    let prometheus_scrape_interval_str = hydra_config.prometheus.scrape_interval
        .as_ref()
        .unwrap_or(&"5s".to_string())
        .clone();

    // Parse scrape interval to seconds for the Python script
    let prometheus_scrape_interval = parse_scrape_interval_to_seconds(&prometheus_scrape_interval_str)?;

    let args = ControllerComposeArgs {
        template_path,
        compose_output_path: compose_output_path.to_string(),
        controller_dir,
        container_name: CONTROLLER_CONTAINER_NAME.to_string(),
        controller_config_path: controller_client_config.to_string(),
        controller_output_dir: controller_remote_output_dir,
        prometheus_scrape_interval: prometheus_scrape_interval.to_string(),
        streaming_engine,
    };

    // Call the Python script to generate the compose file
    call_generate_controller_compose_script(args, helper_script).await
}

/// Call generate_queryengine_compose.py using uv run
/// This matches Python's implementation but uses uv for environment management
async fn call_generate_queryengine_compose_script(
    args: QueryEngineComposeArgs,
    helper_script: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use uv run with the generate_compose project directory (absolute path)
    let project_root = get_project_root()?;
    let uv_project = format!("{}/Utilities/asap-cli/uv_configs/generate_compose", project_root.to_string_lossy());

    let mut cmd = Command::new("uv");
    cmd.arg("run")
        .arg("--project")
        .arg(uv_project)
        .arg(&helper_script)
        .arg("--template-path").arg(&args.template_path)
        .arg("--output-path").arg(&args.compose_output_path)
        .arg("--queryengine-dir").arg(&args.query_engine_dir)
        .arg("--container-name").arg(&args.container_name)
        .arg("--experiment-output-dir").arg(&args.experiment_output_dir)
        .arg("--controller-remote-output-dir").arg(&args.controller_remote_output_dir)
        .arg("--kafka-topic").arg(&args.kafka_topic)
        .arg("--input-format").arg(&args.input_format)
        .arg("--prometheus-scrape-interval").arg(&args.prometheus_scrape_interval)
        .arg("--log-level").arg(&args.log_level)
        .arg("--streaming-engine").arg(&args.streaming_engine)
        .arg("--query-language").arg(&args.query_language)
        .arg("--lock-strategy").arg(&args.lock_strategy)
        .arg("--kafka-host").arg(&args.kafka_host)
        .arg("--prometheus-host").arg(&args.prometheus_host)
        .arg("--prometheus-port").arg(&args.prometheus_port)
        .arg("--kafka-proxy-container-name").arg(&args.kafka_proxy_container_name)
        .arg("--http-port").arg(&args.http_port);

    if args.compress_json {
        cmd.arg("--compress-json");
    }
    if args.profile_query_engine {
        cmd.arg("--profile-query-engine");
    }
    if args.forward_unsupported_queries {
        cmd.arg("--forward-unsupported-queries");
    }
    if args.manual {
        cmd.arg("--manual");
    }
    if args.dump_precomputes {
        cmd.arg("--dump-precomputes");
    }

    // Python also includes --kafka-proxy-container-name and --http-port
    cmd.arg("--kafka-proxy-container-name").arg(&args.kafka_proxy_container_name);
    cmd.arg("--http-port").arg(&args.http_port);

    println!("Calling generate_queryengine_compose.py with uv...");
    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        return Err("Failed to generate query engine compose file".into());
    }

    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

/// Call generate_controller_compose.py using uv run
/// This matches Python's ControllerService._start_containerized implementation
async fn call_generate_controller_compose_script(
    args: ControllerComposeArgs,
    helper_script: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use uv run with the generate_compose project directory (absolute path)
    let project_root = get_project_root()?;
    let uv_project = format!("{}/Utilities/asap-cli/uv_configs/generate_compose", project_root.to_string_lossy());

    let mut cmd = Command::new("uv");
    cmd.arg("run")
        .arg("--project")
        .arg(uv_project)
        .arg(&helper_script)
        .arg("--template-path").arg(&args.template_path)
        .arg("--compose-output-path").arg(&args.compose_output_path)
        .arg("--controller-dir").arg(&args.controller_dir)
        .arg("--container-name").arg(&args.container_name)
        .arg("--input-config-path").arg(&args.controller_config_path)
        .arg("--controller-output-dir").arg(&args.controller_output_dir)
        .arg("--prometheus-scrape-interval").arg(&args.prometheus_scrape_interval)
        .arg("--streaming-engine").arg(&args.streaming_engine);

    println!("Calling generate_controller_compose.py with uv...");
    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        return Err("Failed to generate controller compose file".into());
    }

    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

/// Call generate_fake_exporter_compose.py using uv run
/// This generates docker-compose files for fake exporters
async fn call_generate_fake_exporter_compose_script(
    args: FakeExporterComposeArgs,
    helper_script: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use uv run with the generate_compose project directory (absolute path)
    let project_root = get_project_root()?;
    let uv_project = format!("{}/Utilities/asap-cli/uv_configs/generate_compose", project_root.to_string_lossy());

    let mut cmd = Command::new("uv");
    cmd.arg("run")
        .arg("--project")
        .arg(uv_project)
        .arg(helper_script)
        .arg("--fake-exporter-dir").arg(args.fake_exporter_dir)
        .arg("--port").arg(args.port.to_string())
        .arg("--valuescale").arg(args.valuescale.to_string())
        .arg("--dataset").arg(args.dataset)
        .arg("--num-labels").arg(args.num_labels.to_string())
        .arg("--num-values-per-label").arg(args.num_values_per_label.to_string())
        .arg("--metric-type").arg(args.metric_type)
        .arg("--template-path").arg(args.template_path)
        .arg("--container-name").arg(args.container_name)
        .arg("--exporter-output-dir").arg(args.exporter_output_dir)
        .arg("--experiment-output-dir").arg(args.experiment_output_dir)
        .arg("--compose-output-path").arg(args.compose_output_path);

    println!("Calling generate_fake_exporter_compose.py with uv...");
    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        return Err("Failed to generate fake exporter compose file".into());
    }

    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

/// Generate fake exporter docker-compose files using the Python script
/// This generates individual compose files for each fake exporter and a master compose file
pub async fn generate_fake_exporters_compose(
    hydra_config: &HydraConfig,
    experiment_outputs_abs_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if experiment params exist and have fake exporter config
    let experiment_params = match &hydra_config.experiment_params {
        Some(params) => params,
        None => {
            println!("No experiment params found, skipping fake exporter generation");
            return Ok(());
        }
    };

    // Check if fake exporter is configured
    let fake_exporter_config = match experiment_params.exporters.exporter_list.get("fake_exporter") {
        Some(crate::util::ExporterConfig::FakeExporter {
            num_ports_per_server,
            start_port,
            dataset,
            synthetic_data_value_scale,
            num_labels,
            num_values_per_label,
            metric_type,
        }) => {
            (num_ports_per_server, start_port, dataset, synthetic_data_value_scale,
             num_labels, num_values_per_label, metric_type)
        },
        _ => {
            println!("No fake exporter config found, skipping fake exporter generation");
            return Ok(());
        }
    };

    let (num_ports_per_server, start_port, dataset, valuescale, num_labels, num_values_per_label, metric_type) = fake_exporter_config;

    // Get fake exporter language (default to python)
    // let language = hydra_config.fake_exporter_language
    //     .as_ref()
    //     .map(|s| s.as_str())
    //     .unwrap_or("python");
    let language = hydra_config.fake_exporter_language
            .as_deref()
            .unwrap_or("python");

    // Get project root based on binary location
    let project_root = get_project_root()?;
    let code_dir = project_root.to_string_lossy();

    // Paths for template and script
    let fake_exporter_dir = match language {
        "python" => format!("{}/PrometheusExporters/fake_exporter/fake_exporter_python", code_dir),
        "rust" => format!("{}/PrometheusExporters/fake_exporter/fake_exporter_rust/fake_exporter", code_dir),
        _ => return Err(format!("Unsupported fake exporter language: {}", language).into()),
    };

    // Use CLI-specific template
    let template_filename = match language {
        "python" => "fake-exporter-python-cli-compose.yml.j2",
        "rust" => "fake-exporter-rust-cli-compose.yml.j2",
        _ => return Err(format!("Unsupported fake exporter language for CLI template: {}", language).into()),
    };
    let template_path = format!("{}/{}", fake_exporter_dir, template_filename);
    let helper_script = format!("{}/Utilities/experiments/generate_fake_exporter_compose.py", code_dir);

    // Ensure compose output directories exist
    tokio::fs::create_dir_all(format!("{}/Utilities/docker/generated_compose_files", code_dir)).await?;
    tokio::fs::create_dir_all(format!("{}/Utilities/docker/generated_compose_files/fake_exporter_composes", code_dir)).await?;

    // Experiment output paths
    let experiment_output_dir = format!("{}/fake_exporter_output", experiment_outputs_abs_path);
    tokio::fs::create_dir_all(&experiment_output_dir).await?;

    // Generate compose file for each fake exporter port
    let mut compose_files = Vec::new();
    for i in 0..*num_ports_per_server {
        let port = start_port + i;
        let container_name = format!("sketchdb-fake-exporter-{}-{}", port, language);
        let compose_name = format!("fake-exporter-compose-{}-{}.yml", port, language);
        let compose_output_path = format!("{}/Utilities/docker/generated_compose_files/fake_exporter_composes/{}", code_dir, compose_name);

        let args = FakeExporterComposeArgs {
            template_path: template_path.clone(),
            compose_output_path: compose_output_path.clone(),
            fake_exporter_dir: fake_exporter_dir.clone(),
            container_name,
            port,
            valuescale: *valuescale,
            dataset: dataset.clone(),
            num_labels: *num_labels,
            num_values_per_label: *num_values_per_label,
            metric_type: metric_type.clone(),
            experiment_output_dir: experiment_outputs_abs_path.to_string(),
            exporter_output_dir: experiment_output_dir.clone(),
        };

        call_generate_fake_exporter_compose_script(args, helper_script.clone()).await?;
        compose_files.push(compose_name);
    }

    // Generate master compose file that includes all individual compose files
    generate_master_fake_exporters_compose(&compose_files).await?;

    Ok(())
}

/// Generate the master fake-exporters-compose.yml by reading and merging all individual compose files
async fn generate_master_fake_exporters_compose(
    compose_files: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let project_root = get_project_root()?;
    let master_compose_path = format!("{}/Utilities/docker/generated_compose_files/fake-exporter-compose.yml", project_root.to_string_lossy());

    // Create YAML content with merged services
    let mut content = String::from("# Master compose file for all fake exporters\n");
    content.push_str("# This file is auto-generated by asap-cli\n\n");
    content.push_str("services:\n");

    // Read each individual compose file and extract the service definition
    for (idx, compose_file) in compose_files.iter().enumerate() {
        let compose_path = format!("{}/Utilities/docker/generated_compose_files/fake_exporter_composes/{}", project_root.to_string_lossy(), compose_file);
        let compose_content = tokio::fs::read_to_string(&compose_path).await?;

        // Parse the YAML to extract the fake-exporter service
        // For now, use simple string manipulation to extract the service and rename it
        if let Some(services_start) = compose_content.find("services:") {
            if let Some(fake_exporter_start) = compose_content[services_start..].find("fake-exporter:") {
                let service_content_start = services_start + fake_exporter_start + "fake-exporter:".len();

                // Find the next top-level key (or end of file) to know where service definition ends
                let remaining = &compose_content[service_content_start..];
                let service_lines: Vec<&str> = remaining.lines()
                    .take_while(|line| line.is_empty() || line.starts_with(' ') || line.starts_with('\t'))
                    .collect();

                // Generate unique service name
                let service_name = format!("fake-exporter-{}", idx);
                content.push_str(&format!("  {}:\n", service_name));

                // Add the service content (already indented from the original file)
                for line in service_lines {
                    if !line.trim().is_empty() {
                        content.push_str(&format!("  {}\n", line));
                    }
                }
            }
        }
    }

    //// Add networks section so fake exporters can join asap-network
    //content.push_str("\nnetworks:\n");
    //content.push_str("  asap-network:\n");
    //content.push_str("    external: true\n");

    // Write the master compose file
    tokio::fs::write(&master_compose_path, content).await?;

    println!("Generated master fake exporters compose file: {}", master_compose_path);
    Ok(())
}

/// Generate ArroyoSketch docker-compose file using the Python script
pub async fn generate_arroyosketch_compose(
    hydra_config: &HydraConfig,
    experiment_name: &str,
    experiment_outputs_abs_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get project root
    let project_root = get_project_root()?;
    let code_dir = project_root.to_string_lossy();

    // Paths
    let arroyosketch_dir = format!("{}/ArroyoSketch", code_dir);
    let template_path = format!("{}/arroyosketch-cli-compose.yml.j2", arroyosketch_dir);
    let helper_script = format!("{}/Utilities/experiments/generate_arroyosketch_compose.py", code_dir);
    let compose_output_path = format!("{}/Utilities/docker/generated_compose_files/arroyosketch-compose.yml", code_dir);

    // Ensure compose output directory exists
    tokio::fs::create_dir_all(format!("{}/Utilities/docker/generated_compose_files", code_dir)).await?;

    // Experiment output paths (absolute)
    let controller_output_dir = format!("{}/controller_output", experiment_outputs_abs_path);
    let arroyosketch_output_dir = format!("{}/arroyosketch_output", experiment_outputs_abs_path);

    // Extract config values with defaults
    let parallelism = hydra_config.streaming.parallelism.unwrap_or(1);
    let output_format = hydra_config.streaming.flink_output_format
        .as_ref()
        .unwrap_or(&"json".to_string())
        .clone();

    let args = ArroyoSketchComposeArgs {
        template_path,
        compose_output_path: compose_output_path.to_string(),
        arroyosketch_dir,
        container_name: ARROYOSKETCH_CONTAINER_NAME.to_string(),
        controller_output_dir,
        arroyosketch_output_dir,
        prometheus_base_port: 9091,
        prometheus_path: "/receive".to_string(),
        prometheus_bind_ip: "0.0.0.0".to_string(),
        parallelism,
        output_kafka_topic: FLINK_OUTPUT_TOPIC.to_string(),
        output_format,
        pipeline_name: experiment_name.to_string(),
        arroyo_url: "http://arroyo:5115/api/v1".to_string(),
        bootstrap_servers: "kafka:9092".to_string(),
    };

    call_generate_arroyosketch_compose_script(args, helper_script).await
}

/// Call generate_arroyosketch_compose.py using uv run
async fn call_generate_arroyosketch_compose_script(
    args: ArroyoSketchComposeArgs,
    helper_script: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let project_root = get_project_root()?;
    let uv_project = format!("{}/Utilities/asap-cli/uv_configs/generate_compose", project_root.to_string_lossy());

    let mut cmd = Command::new("uv");
    cmd.arg("run")
        .arg("--project")
        .arg(uv_project)
        .arg(&helper_script)
        .arg("--template-path").arg(&args.template_path)
        .arg("--compose-output-path").arg(&args.compose_output_path)
        .arg("--arroyosketch-dir").arg(&args.arroyosketch_dir)
        .arg("--container-name").arg(&args.container_name)
        .arg("--controller-output-dir").arg(&args.controller_output_dir)
        .arg("--arroyosketch-output-dir").arg(&args.arroyosketch_output_dir)
        .arg("--prometheus-base-port").arg(args.prometheus_base_port.to_string())
        .arg("--prometheus-path").arg(&args.prometheus_path)
        .arg("--prometheus-bind-ip").arg(&args.prometheus_bind_ip)
        .arg("--parallelism").arg(args.parallelism.to_string())
        .arg("--output-kafka-topic").arg(&args.output_kafka_topic)
        .arg("--output-format").arg(&args.output_format)
        .arg("--pipeline-name").arg(&args.pipeline_name)
        .arg("--arroyo-url").arg(&args.arroyo_url)
        .arg("--bootstrap-servers").arg(&args.bootstrap_servers);

    println!("Calling generate_arroyosketch_compose.py with uv...");
    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        return Err("Failed to generate ArroyoSketch compose file".into());
    }

    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

/// Starts ProjectASAP by bringing up containers using asap-docker-compose.yml
pub async fn asap_up() -> Result<(), Box<dyn std::error::Error>> {
    let project_root = get_project_root()?;
    let compose_path = format!("{}/Utilities/docker/asap-docker-compose.yml", project_root.to_string_lossy());

    let mut cmd = Command::new("docker");
    println!("Starting ProjectASAP...");
    cmd.arg("compose")
       .arg("-f").arg(&compose_path)
       .arg("up")
       .arg("-d");

    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        Err("Failed to start ProjectASAP".into())
    } else {
        println!("ProjectASAP started successfully");
        Ok(())
    }
}

pub async fn asap_down() -> Result<(), Box<dyn std::error::Error>> {
    let project_root = get_project_root()?;
    let compose_path = format!("{}/Utilities/docker/asap-docker-compose.yml", project_root.to_string_lossy());

    let mut cmd = Command::new("docker");
    cmd.arg("compose")
       .arg("-f").arg(&compose_path)
       .arg("down")
       .arg("-v");  // Remove named volumes declared in the compose file

    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        Err("Failed to stop ProjectASAP".into())
    } else {
        println!("ProjectASAP stopped successfully (containers and volumes removed)");
        Ok(())
    }
}

pub async fn docker_ps() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new("docker");
    cmd.arg("ps");
    println!("Running 'docker ps'...");
    let output = cmd.output().await?;
    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        Err("Failed to run 'docker ps'".into())
    } else {
        println!("{}", String::from_utf8_lossy(&output.stdout));
        Ok(())
    }
}
