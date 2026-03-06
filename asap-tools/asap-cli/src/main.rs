use clap::{Args, Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::{thread, time::Duration};
use tokio::process::Command;
use lazy_static::lazy_static;
mod util;
mod docker_util;

const DEFAULT_RUN_NAME: &str = "demo";
const EXPERIMENT_CONFIG_SUFFIX: &str = ".yaml";

lazy_static! {
    pub static ref PROJECT_ROOT_DIR: PathBuf = get_project_root().unwrap();
    pub static ref HYDRA_CONFIGS_RELATIVE_PATH: PathBuf = PathBuf::from("asap-tools/experiments/config");
}

#[derive(Parser)]
#[command(name = "asap")]
#[command(about = "ASAP CLI - A command line interface for deploying ProjectASAP")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start a demo deployment")]
    Start(StartArgs),
    #[command(about = "Stop a demo deployment")]
    Stop,
}

#[derive(Args)]
struct StartArgs {
    #[arg(
        long = "experiment_config",
        required = false,
        value_name = "PATH",
        help = "Path to YAML experiment configuration file"
    )]
    experiment_config: Option<PathBuf>,

    #[arg(
        long = "experiment_type",
        required = false,
        value_name = "TYPE",
        help = "The name of the experiment type, e.g. 'cloud_demo'"
    )]
    experiment_type: Option<String>,

    #[arg(
        long = "experiment_name",
        short = 'n',
        required = false,
        value_name = "NAME",
        help = "Name for the experiment output directory (overrides config file). Defaults to 'demo' if not specified"
    )]
    experiment_name: Option<String>,
}

/// Get the ProjectASAP root directory based on the binary's location
/// Binary is at: ProjectASAP/asap-tools/asap-cli/target/debug/asap-cli
/// So we go up 4 levels: debug -> target -> asap-cli -> asap-tools -> ProjectASAP
fn get_project_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let binary_path = std::env::current_exe()?;

    // Go up 4 levels: asap-cli -> debug -> target -> asap-cli -> asap-tools -> ProjectASAP
    let project_root = binary_path
        .parent() // Remove 'asap-cli' binary
        .and_then(|p| p.parent()) // Remove 'debug'
        .and_then(|p| p.parent()) // Remove 'target'
        .and_then(|p| p.parent()) // Remove 'asap-cli'
        .and_then(|p| p.parent()) // Remove 'asap-tools'
        .ok_or("Failed to determine project root from binary location")?;

    Ok(project_root.to_path_buf())
}

/// Gets the path to the experiment config yaml file using the given experiment
/// type and the expected location in asap-tools/experiments/config/experiment_type/
pub fn get_experiment_config_path(experiment_type: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {

    let experiment_types_directory = PathBuf::from("experiment_type");
    let experiment_config_filename = PathBuf::from(
        format!("{}{}", experiment_type, EXPERIMENT_CONFIG_SUFFIX)
    );

    // ROOT/ + /asap-tools/experiments/config/ + /experiment_type/ + /<exp_type>.yaml
    let experiment_config_path = PROJECT_ROOT_DIR.join(HYDRA_CONFIGS_RELATIVE_PATH.as_path())
                                                 .join(experiment_types_directory.as_path())
                                                 .join(experiment_config_filename.as_path());

    Ok(experiment_config_path)
}

/// Generate all configuration and compose files needed for the experiment
async fn generate_configs_and_compose_files(
    config: &util::HydraConfig,
    experiment_config_path: &Path,
    experiment_name_override: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get project root based on binary location
    let project_root = get_project_root()?;

    let experiments_dir = project_root.join("asap-tools/experiments");

    println!("Generating configuration and compose files...");

    // Get experiment name from CLI flag, config file, or default to "demo"
    // Priority: CLI flag > config file > "demo"
    let experiment_name = experiment_name_override
        .or_else(
            || config.experiment.as_ref()
                                .and_then(|e| e.name.as_ref())
                                .map(|s| s.as_str())
        ).unwrap_or(DEFAULT_RUN_NAME);

    // Setup experiment output directories (relative to project root)
    let experiment_outputs_base = project_root.join("experiment_outputs");
    let experiment_output_dir = experiment_outputs_base.join(experiment_name);

    // If experiment name is "demo" and directory exists, delete it for fresh start
    // (demo/ is only used when no explicit experiment name is provided)
    if experiment_name == "demo" && experiment_output_dir.exists() {
        println!("  - No experiment name given, removing existing 'demo' experiment directory...");
        if let Err(e) = tokio::fs::remove_dir_all(&experiment_output_dir).await {
            println!("  - Warning: Could not delete existing demo/ directory: {}. Continuing anyway...", e);
            println!("  - Note: You may want to manually delete experiment_outputs/demo/ or provide an explicit experiment name.");
        }
    }

    // Create experiment output directory structure
    println!("  - Creating experiment output directories...");
    tokio::fs::create_dir_all(&experiment_outputs_base).await?;
    tokio::fs::create_dir_all(&experiment_output_dir).await?;
    tokio::fs::create_dir_all(experiment_output_dir.join("controller_output")).await?;
    tokio::fs::create_dir_all(experiment_output_dir.join("arroyosketch_output")).await?;
    tokio::fs::create_dir_all(experiment_output_dir.join("sketchdb")).await?;
    tokio::fs::create_dir_all(experiment_output_dir.join("sketchdb/query_engine_output")).await?;

    // Generate controller client configs
    if config.experiment_params.is_none() {
        return Err(
            "No experiment parameters found in experiment config: Unable to generate controller client config".into()
        );
    }

    let experiment_parameters = config.experiment_params.clone().unwrap();
    println!("  - Generating controller client configs...");
    let controller_client_config = util::generate_controller_client_config(
        experiment_parameters,
        &experiment_output_dir
    ).await?;

    let controller_client_config = controller_client_config.to_str()
                                                           .unwrap();

    // Get absolute path for Docker volume mounts
    let experiment_outputs_abs = experiment_output_dir
        .canonicalize()?
        .to_string_lossy()
        .to_string();

    // 1. Generate query engine compose file
    println!("  - Generating query engine compose...");
    docker_util::generate_query_engine_compose(
        config,
        &experiment_outputs_abs,
    ).await?;

    // 2. Generate controller compose file
    println!("  - Generating controller compose...");
    println!(" controller_client_config: {}", controller_client_config);
    docker_util::generate_controller_compose(
        config,
        experiment_name,
        controller_client_config,
        &experiment_outputs_abs,
    ).await?;

    // 2.5. Generate ArroyoSketch compose file
    println!("  - Generating ArroyoSketch compose...");
    docker_util::generate_arroyosketch_compose(
        config,
        experiment_name,
        &experiment_outputs_abs,
    ).await?;

    // 2.6. Generate fake exporter compose files
    println!("  - Generating fake exporter compose files...");
    docker_util::generate_fake_exporters_compose(
        config,
        &experiment_outputs_abs,
    ).await?;

    // 3. Generate prometheus config
    println!("  - Generating prometheus config...");
    let prometheus_output_dir = project_root.join("asap-tools/docker/prometheus");
    tokio::fs::create_dir_all(&prometheus_output_dir).await?;
    let num_nodes = config.cloudlab.num_nodes.unwrap_or(1);

    // For local deployment, use localhost for all IPs
    let node_ip_prefix = "localhost";
    let prometheus_client_ip = "localhost";

    // Get scrape and evaluation intervals from config
    let scrape_interval = config.prometheus.scrape_interval
        .as_ref()
        .unwrap_or(&"5s".to_string())
        .clone();

    let evaluation_interval = config.prometheus.evaluation_interval
        .as_ref()
        .unwrap_or(&"1s".to_string())
        .clone();

    // TODO: TEMPORARY SOLUTION - Currently using "localhost" for node_ip_prefix
    // which will generate prometheus targets like "localhost.1:50000", "localhost.2:50001"
    // This doesn't work for Docker networking without host mode.
    //
    // Future improvements:
    // 1. Use host machine IP address (e.g., host.docker.internal or actual IP)
    // 2. Use a pre-defined Docker network with service discovery
    // 3. Post-process the prometheus config to replace IPs with Docker service names
    //
    // For now, the Python script generates the config, and Docker service names
    // are already being used in the fake-exporters-compose.yml file.
    // The prometheus config will need manual adjustment or post-processing.

    let uv_project = project_root.join("asap-tools/asap-cli/uv_configs/generate_prometheus_config");

    // For local deployment, node_offset is 0
    // For CloudLab, this would be read from config
    let node_offset = 0;

    // Set up remote write configuration for SketchDB
    // For Docker deployment, use arroyo hostname and port 9091
    let remote_write_ip = config.streaming.remote_write
        .as_ref()
        .and_then(|rw| rw.ip.as_deref())
        .unwrap_or("arroyo");

    let remote_write_base_port = config.streaming.remote_write
        .as_ref()
        .and_then(|rw| rw.base_port)
        .unwrap_or(9091);

    let remote_write_path = config.streaming.remote_write
        .as_ref()
        .and_then(|rw| rw.path.as_deref())
        .unwrap_or("/receive");

    let parallelism = config.streaming.parallelism.unwrap_or(1);

    // Construct remote_write_url
    let remote_write_url = format!("http://{}:{}{}", remote_write_ip, remote_write_base_port, remote_write_path);

    // Get metrics to remote write from experiment params
    let metrics_to_remote_write = if let Some(ref experiment_params) = config.experiment_params {
        util::get_metrics_to_remote_write(experiment_params)
    } else {
        Vec::new()
    };
    let metrics_to_remote_write_str = metrics_to_remote_write.join(",");

    let mut cmd = Command::new("uv");
    cmd.args([
        "run",
        "--project",
        uv_project.to_str().unwrap(),
        experiments_dir
            .join("generate_prometheus_config.py")
            .to_str()
            .unwrap(),
        "--num_nodes",
        &num_nodes.to_string(),
        "--node-offset",
        &node_offset.to_string(),
        "--output_dir",
        prometheus_output_dir.to_str().unwrap(),
        "--experiment_config_file",
        experiment_config_path.to_str().unwrap(),
        "--node-ip-prefix",
        node_ip_prefix,
        "--prometheus-client-ip",
        prometheus_client_ip,
        "--scrape_interval",
        &scrape_interval,
        "--evaluation_interval",
        &evaluation_interval,
        "--remote_write_url",
        &remote_write_url,
        "--remote_write_base_port",
        &remote_write_base_port.to_string(),
        "--parallelism",
        &parallelism.to_string(),
    ]);

    // Add remote_write_metric_names if not empty
    if !metrics_to_remote_write_str.is_empty() {
        cmd.arg("--remote_write_metric_names");
        cmd.arg(&metrics_to_remote_write_str);
    }

    let status = cmd.status().await?;

    if !status.success() {
        return Err("Failed to generate prometheus config".into());
    }

    // Post-process Prometheus config to fix targets for Docker networking
    // The Python script generates targets like "localhost.2:50000" but for Docker
    // we need service names like "sketchdb-fake-exporter-50000-python:50000"
    // The Python script generates prometheus.yml in the output_dir
    let generated_config = prometheus_output_dir.join("prometheus.yml");
    fix_prometheus_targets_for_docker(config, &generated_config, &project_root).await?;

    println!("All configuration and compose files generated successfully!");
    Ok(())
}

/// Fix Prometheus targets to use Docker service names instead of localhost.X
async fn fix_prometheus_targets_for_docker(
    config: &util::HydraConfig,
    prometheus_config_file: &std::path::Path,
    _project_root: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read the prometheus config
    let content = tokio::fs::read_to_string(prometheus_config_file).await?;

    // Get fake exporter info from config
    let experiment_params = match &config.experiment_params {
        Some(params) => params,
        None => return Ok(()), // No experiment params, nothing to fix
    };

    let (num_ports, start_port, language) = match experiment_params.exporters.exporter_list.get("fake_exporter") {
        Some(util::ExporterConfig::FakeExporter {
            num_ports_per_server,
            start_port,
            ..
        }) => {
            let lang = config.fake_exporter_language.as_deref().unwrap_or("python");
            (*num_ports_per_server, *start_port, lang)
        },
        _ => return Ok(()), // No fake exporter, nothing to fix
    };

    // Build replacement map: localhost.X:port -> service-name:port
    // Note: Python generates all targets on same node (localhost.2:PORT for num_nodes=1)
    // not incrementing node number per port
    let mut replacements = Vec::new();
    for i in 0..num_ports {
        let port = start_port + i;
        // For num_nodes=1, Python generates localhost.2:port for all ports
        let old_target = format!("localhost.2:{}", port);
        let new_target = format!("sketchdb-fake-exporter-{}-{}:{}", port, language, port);
        replacements.push((old_target, new_target));
    }

    // Replace all occurrences
    let mut new_content = content;
    for (old, new) in replacements {
        new_content = new_content.replace(&old, &new);
    }

    // Write back the fixed config
    tokio::fs::write(prometheus_config_file, new_content).await?;

    println!("  - Fixed Prometheus targets to use Docker service names");
    Ok(())
}

/// Configure Grafana after containers are running
async fn configure_grafana(experiment_type: &str, experiment_name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let project_root = get_project_root()?;
    let experiments_dir = project_root.join("asap-tools/experiments");
    let uv_project = project_root.join("asap-tools/asap-cli/uv_configs/grafana_config");

    let experiment_type_arg: String = format!("experiment_type={}", experiment_type);
    let experiment_name_arg: String;

    if let Some(name) = experiment_name {
        experiment_name_arg = format!("experiment.name={}", name);
    } else {
        experiment_name_arg = format!("experiment.name={}", DEFAULT_RUN_NAME);
    }

    // Wait for Grafana health endpoint to be ready
    println!("Waiting for Grafana to be ready...");
    let max_retries = 30;
    let mut ready = false;

    for i in 1..=max_retries {
        let result = Command::new("curl")
            .args(["-s", "http://localhost:3000/api/health"])
            .output()
            .await;

        if let Ok(output) = result {
            if output.status.success() {
                println!("Grafana ready after {} attempts", i);
                ready = true;
                break;
            }
        }

        if i < max_retries {
            println!("Waiting for Grafana to be ready... ({}/{})", i, max_retries);
            thread::sleep(Duration::from_secs(5));
        }
    }

    if !ready {
        return Err("Grafana failed to become ready after 30 attempts".into());
    }

    println!("Configuring Grafana...");
    let status = Command::new("uv")
        .args([
            "run",
            "--project",
            uv_project.to_str().unwrap(),
            experiments_dir.join("grafana_config.py").to_str().unwrap(),
            &experiment_type_arg,
            &experiment_name_arg,
            // Override server URLs to use Docker service discovery
            "experiment_params.servers.0.url=http://prometheus:9090",
            "experiment_params.servers.1.url=http://queryengine-rust:8088",
            "--configure",
        ])
        .status()
        .await?;

    if !status.success() {
        return Err("Failed to configure Grafana".into());
    }

    println!("Grafana configured successfully!");
    Ok(())
}

/// Checks if a container is in the 'running' state
async fn check_container(container_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let mut cmd = Command::new("docker");
    cmd.arg("container")
       .arg("inspect")
       .arg("-f").arg("'{{.State.Status}}'")
       .arg(container_name);

    let output = cmd.output().await?;

    if !output.status.success() {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        Err("Failed to inspect docker container".into())
    } else {
        let is_running = String::from_utf8_lossy(&output.stdout).trim() == "'running'";
        Ok(is_running)
    }

}

/// Start routine
async fn start(args: StartArgs) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize project root
    let _project_root = &PROJECT_ROOT_DIR;
    println!("Starting ProjectASAP...");

    // Get Path to experiment configuration yaml file
    let experiment_config_path: PathBuf;
    let experiment_type: String;

    if let Some(exp_type) = args.experiment_type {
        experiment_config_path = get_experiment_config_path(&exp_type)?;
        experiment_type = exp_type.clone();
    } else {
        experiment_config_path = args.experiment_config.expect(
            "Error: Require one of either experiment type or experiment config path"
        );
        experiment_type = experiment_config_path.file_name()
                                                .expect("Error getting experiment type")
                                                .to_str()
                                                .expect("Error parsing experiment type to string")
                                                .strip_suffix(EXPERIMENT_CONFIG_SUFFIX)
                                                .expect("Given path to experiment config does not end in '.yaml'")
                                                .to_owned();
    }

    // Parse the experiment config
    println!("Experiment type: {}", experiment_type);
    println!("Experiment config path: {}", experiment_config_path.to_str().unwrap());
    println!("Parsing experiment config...");
    let config = util::parse_config_auto(&experiment_config_path).await?;
    println!("Successfully parsed experiment config:");

    println!("- Experiment Type: '{}'", experiment_type);
    // Display experiment params if available
    if let Some(exp_params) = &config.experiment_params {
        println!("- Experiment modes: {:?}", exp_params.experiment);
        println!("- Number of servers: {}", exp_params.servers.len());
        println!("- Number of query groups: {}", exp_params.query_groups.len());
        println!("- Number of metrics: {}", exp_params.metrics.len());
    }

    // Display streaming config if available
    if let Some(engine) = &config.streaming.engine {
        println!("- Streaming engine: {}", engine);
    }
    if let Some(format) = &config.streaming.flink_output_format {
        println!("- Flink output format: {}", format);
    }

    // Display other configs
    if let Some(lang) = &config.fake_exporter_language {
        println!("- Fake exporter language: {}", lang);
    }
    if let Some(num_nodes) = config.cloudlab.num_nodes {
        println!("- CloudLab nodes: {}", num_nodes);
    }

    // Convert experiment_config path to absolute path
    let experiment_config_abs = if experiment_config_path.is_absolute() {
        experiment_config_path.clone()
    } else {
        std::env::current_dir()?.join(&experiment_config_path)
    };

    // Generate all configuration and compose files
    generate_configs_and_compose_files(&config, &experiment_config_abs, args.experiment_name.as_deref()).await?;

    // Start the containers
    docker_util::asap_up().await?;

    println!("Waiting for containers to start...");
    while !check_container("asap-grafana").await? {
        thread::sleep(Duration::from_secs(5));
    }
    // Configure Grafana after containers are running
    configure_grafana(&experiment_type, args.experiment_name.as_deref()).await?;

    // Check container status
    docker_util::docker_ps().await?;
    Ok(())
}

async fn stop() -> Result<(), Box<dyn std::error::Error>> {
    println!("Stopping deployment...");
    docker_util::asap_down().await?;
    docker_util::docker_ps().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => {
            if let Err(e) = start(args).await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Stop => {
            if let Err(e) = stop().await {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
