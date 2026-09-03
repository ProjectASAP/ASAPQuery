import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SSH_OPTIONS = "-o StrictHostKeyChecking=no"

# CLOUDLAB_USERNAME = "milindsr"
CLOUDLAB_HOME_DIR = "/scratch/sketch_db_for_prometheus"
CLOUDLAB_QUERY_LOG_FILE = "/scratch/sketch_db_for_prometheus/prometheus/queries.log"

LOCAL_EXPERIMENT_DIR = os.path.join(os.path.dirname(ROOT_DIR), "experiment_outputs")

FLINK_INPUT_TOPIC = "flink_input"
FLINK_OUTPUT_TOPIC = "flink_output"
KAFKA_BROKER = "localhost:9092"

QUERY_ENGINE_RS_PROCESS_KEYWORD = "query_engine_rust"
QUERY_ENGINE_RS_BINARY_NAME = "query_engine_rust"
QUERY_ENGINE_RS_FP_BINARY_NAME = "query_engine_rust_fp"
QUERY_ENGINE_RS_CONTAINER_NAME = "sketchdb-queryengine-rust"

ARROYO_IMAGE = "ghcr.io/projectasap/asap-arroyo:v0.1.0"

ARROYO_THROUGHPUT_POLLING_INTERVAL = 1
PROMETHEUS_THROUGHPUT_POLLING_INTERVAL = 5
PROMETHEUS_HEALTH_POLLING_INTERVAL = 5

SKETCHDB_EXPERIMENT_NAME = "sketchdb"
BASELINE_EXPERIMENT_NAME = "baseline"
INGEST_MONITOR_STOP_FILE = ".ingest_monitor_stop"
# ClickHouse precompute-strategy arms (issue #491): same raw table/queries as
# baseline, but querying via ClickHouse's own precompute mechanisms.
BASELINE_SKETCH_EXPERIMENT_NAME = "baseline_sketch"
BASELINE_MV_EXPERIMENT_NAME = "baseline_mv"
BASELINE_MV_SKETCH_EXPERIMENT_NAME = "baseline_mv_sketch"
AVOID_REMOTE_MONITOR_LONG_SSH = True
AVOID_RUN_ARROYOSKETCH_LONG_SSH = True

# remote_monitor.py process lifecycle timing (start/exit polling)
REMOTE_MONITOR_START_TIMEOUT_SECONDS = 30
REMOTE_MONITOR_START_POLL_INTERVAL_SECONDS = 1
REMOTE_MONITOR_STARTUP_SETTLE_SECONDS = 1
REMOTE_MONITOR_EXIT_POLL_INTERVAL_SECONDS = 2
INGEST_MONITOR_SHUTDOWN_POLL_INTERVAL_SECONDS = 0.5
PROCESS_MONITOR_STOP_TIMEOUT_SECONDS = 30
PROCESS_MONITOR_JOIN_TIMEOUT_SECONDS = 10

PROMETHEUS_CONFIG_DIR = "prometheus_config"
PROMETHEUS_CONFIG_FILE = "prometheus.yml"

# VictoriaMetrics configuration files
VMAGENT_SCRAPE_CONFIG_FILE = "vmagent_scrape.yml"
VMAGENT_REMOTE_WRITE_CONFIG_FILE = "vmagent_remote_write.yml"
