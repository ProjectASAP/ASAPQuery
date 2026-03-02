# Architecture Documentation

Detailed architecture documentation for developers who want to understand or extend the experiment framework.

## Table of Contents

- [System Overview](#system-overview)
- [Experiment Lifecycle](#experiment-lifecycle)
- [Data Flow](#data-flow)
- [Services in the Experiment Framework](#services-in-the-experiment-framework)
- [Service Architecture](#service-architecture)
- [Infrastructure Provider Abstraction](#infrastructure-provider-abstraction)
- [Configuration System](#configuration-system)
- [Component Architecture](#component-architecture)
- [Extension Points](#extension-points)
- [Design Decisions](#design-decisions)

## System Overview

The experiment framework is a **service-oriented, provider-abstracted architecture** designed for:
1. Deploying distributed systems experiments to cloud infrastructure (currently CloudLab)
2. Running performance benchmarks comparing SketchDB vs Prometheus
3. Collecting and analyzing experimental results

### Key Architectural Principles

1. **Service Abstraction**: Uniform interface for all components (Kafka, Flink, Prometheus, etc.)
2. **Provider Abstraction**: Infrastructure-independent (currently CloudLab, extensible to AWS/K8s)
3. **Declarative Configuration**: Hydra-based hierarchical configuration composition
4. **Lifecycle Management**: Automated setup → run → teardown → data collection

### High-Level Architecture

```
experiment_run_e2e.py (Main Orchestrator)
├── InfrastructureProvider (CloudLabProvider)
│   └── SSH-based command execution
├── Service Layer
│   ├── Infrastructure Services (Kafka, Prometheus)
│   ├── Streaming Services (Flink, Arroyo)
│   ├── Query Services (QueryEngine)
│   ├── Data Generation (Exporters, DeathStar)
│   ├── Monitoring Services (Throughput, Health)
│   └── Control Services (Controller, RemoteMonitor)
├── Configuration System (Hydra)
│   ├── Base config (config.yaml)
│   ├── Experiment types (experiment_type/*.yaml)
│   └── Command-line overrides
└── Data Collection (rsync)
    └── Experiment outputs → local machine
```

## Experiment Lifecycle

### Phase 1: Initialization

**Lines 53-194 in experiment_run_e2e.py:**

```
Load Hydra configuration
  ↓
Validate required parameters
  ↓
Convert to Args object (backward compatibility)
  ↓
Create infrastructure provider
  ↓
Create output directories
  ↓
Initialize all services
  ↓
Generate controller/client configs
  ↓
Rsync configs to remote nodes
```

### Phase 2: Per-Mode Loop

**Lines 195-569**: For each experiment mode (e.g., "sketchdb", "prometheus"):

```
Stop all services (clean slate)
  ↓
Generate Prometheus config for this mode
  ↓
Rsync configs to remote nodes
  ↓
───────────────────────────────────────
IF mode == "sketchdb":
  ├─ Start controller
  ├─ Create Kafka topics
  ├─ Start exporters (fake/avalanche)
  ├─ Start DeathStar workload (if configured)
  ├─ Start Kafka adapter (if use_kafka_ingest)
  ├─ Start streaming engine (Flink or Arroyo)
  ├─ Start query engine
  └─ Start Prometheus
ELSE IF mode == "prometheus":
  ├─ Start exporters
  └─ Start Prometheus
───────────────────────────────────────
  ↓
Start system exporters (node_exporter, etc.)
  ↓
Start throughput/health monitors (if enabled)
  ↓
Wait for steady state (default 60s)
  ↓
Run remote monitor (execute queries)
  ├─ Execute PromQL queries via PrometheusClient
  ├─ Monitor process health
  ├─ Profile components (if enabled)
  └─ Record results
  ↓
Collect data (rsync Prometheus data)
  ↓
Teardown (stop services if not no_teardown)
  ↓
Rsync all experiment data to local machine
```

### Phase 3: Data Collection

```
Copy Prometheus data directory
  ↓
Rsync all experiment outputs
  ├─ Query results
  ├─ Logs
  ├─ Profiling data
  └─ Monitoring data
  ↓
Local analysis (separate scripts)
```

## Data Flow

<!-- ### SketchDB Mode with Kafka Ingest

```
Fake Exporters
  ↓ (expose metrics)
Prometheus
  ↓ (scrape metrics)
Prometheus Remote Write API
  ↓
PrometheusKafkaAdapter
  ↓ (convert to Kafka messages)
Kafka INPUT Topic
  ↓ (consume)
Flink/Arroyo (SketchJob)
  ├─ Parse metrics
  ├─ Build sketches
  └─ Serialize sketches
  ↓ (produce)
Kafka OUTPUT Topic
  ↓ (consume)
QueryEngine
  ├─ Deserialize sketches
  ├─ Parse PromQL queries
  └─ Execute queries over sketches
  ↓
PromQL Query Results
  ↓
PrometheusClient (logs results)
``` -->

### SketchDB Mode with Ingest from Prometheus Remote Write

```
Fake Exporters
  ↓ (expose metrics)
Prometheus
  ↓ (scrape metrics)
Prometheus Remote Write API
  ↓ (HTTP POST)
Arroyo RemoteWrite Endpoint
  ├─ Parse Prometheus remote write format
  ├─ Build sketches in real-time
  └─ Serialize sketches
  ↓ (produce)
Kafka OUTPUT Topic
  ↓ (consume)
QueryEngine
  └─ (same as above)
```

### Prometheus Baseline Mode

```
Fake Exporters
  ↓ (expose metrics)
Prometheus
  ↓ (scrape & store metrics)
Prometheus TSDB
  ↓ (query)
PromQL Query API
  ↓
PrometheusClient (logs results)
```

## Services in the Experiment Framework

When you run an experiment, you're orchestrating multiple distributed services that work together. Let's see what these services do in practice before diving into how they're implemented.

### Services in Action

In **experiment_run_e2e.py** (Lines 124-169), you'll see services being initialized:

```python
# Initialize all services
kafka_service = KafkaService(provider, args.node_offset, num_tries=5)
flink_service = FlinkService(provider, args.node_offset)
query_engine_service = QueryEngineServiceFactory.create_query_engine_service(
    args.query_engine_language,
    provider,
    use_container=args.use_container_query_engine,
    node_offset=args.node_offset,
)
prometheus_service = PrometheusService(provider, args.node_offset)
# ... more services
```

These services are then started/stopped throughout the experiment lifecycle. Let's understand what each category does:

### 1. Infrastructure Services

These provide the foundational messaging and monitoring infrastructure:

- **`KafkaService`** - Manages Kafka broker for streaming data between components
  - Creates topics for sketch data
  - Handles broker lifecycle
  - Used in: SketchDB mode for streaming sketches from Arroyo to QueryEngine

- **`PrometheusService` / `DockerPrometheusService`** - Runs Prometheus server
  - Scrapes metrics from exporters
  - Stores time-series data (baseline mode)
  - Sends data via remote write (SketchDB mode)
  - Used in: Both modes

- **`SystemExportersService`** - Deploys monitoring exporters
  - node_exporter: System metrics (CPU, memory, disk)
  - blackbox_exporter: Network probing
  - cadvisor: Container metrics
  - Used in: Monitoring experiment infrastructure itself

### 2. Streaming Engine Services

These process metrics streams and build sketches in real-time:

- **`FlinkService`** - Apache Flink cluster management
  - Starts JobManager and TaskManagers
  - Submits sketch-building jobs
  - Monitors job status
  - Used in: SketchDB mode with Flink

- **`ArroyoService`** - Arroyo streaming engine (containerized or bare-metal)
  - Receives Prometheus remote write directly
  - Builds sketches in real-time
  - Produces to Kafka output topic
  - Used in: SketchDB mode with Arroyo (current default)

### 3. Query Processing Services

These answer PromQL queries over sketches:

- **`QueryEngineService` (Python)** - Legacy Python implementation
- **`QueryEngineRustService` (Rust)** - Production Rust implementation
  - Consumes sketches from Kafka
  - Maintains sketch state
  - Executes PromQL queries
  - Returns approximate results
- **`QueryEngineServiceFactory`** - Creates appropriate engine based on language choice
  - Used in: SketchDB mode

### 4. Data Generation Services

These generate synthetic metric workloads for benchmarking:

- **`PythonExporterService`** - Python-based fake metric exporters
- **`RustExporterService`** - Rust fake exporters (much faster)
  - Expose metrics on multiple ports
  - Configurable cardinality, distributions
  - Used in: All experiments for controlled workloads

- **`AvalancheExporterService`** - High-cardinality load generator
  - Stress testing with extreme cardinality
  - Used in: Cardinality stress tests

- **`DeathstarService`** - DeathStar microservices benchmark
  - Real-world microservices architecture
  - Used in: Realistic workload experiments

### 5. Adapter Services

These bridge between different components:

- **`PrometheusKafkaAdapterService`** - Converts Prometheus remote write → Kafka
  - Receives HTTP remote write requests
  - Publishes to Kafka input topic
  - Used in: Legacy Kafka ingestion mode (deprecated)

### 6. Monitoring Services

These monitor the experiment itself:

- **`RemoteMonitorService`** - Orchestrates query execution
  - Executes PromQL queries via PrometheusClient
  - Monitors process health
  - Profiles components (CPU, memory)
  - Records timing and results

- **`ArroyoThroughputMonitor`** - Tracks Arroyo pipeline throughput
  - Monitors metrics/second processed
  - Used in: Performance analysis

- **`PrometheusThroughputMonitor`** - Tracks Prometheus ingestion rate
  - Monitors samples/second ingested
  - Used in: Performance comparison

- **`PrometheusHealthMonitor`** - Monitors scrape health
  - Tracks target health status
  - Measures scrape duration
  - Used in: Detecting performance degradation

### 7. Control Services

These provide control plane functionality:

- **`ControllerService`** - Manages sketch configurations
  - Updates accuracy/latency SLAs
  - Controls sketch parameters
  - Used in: Adaptive sketch sizing

- **`PrometheusClientService`** - Executes PromQL queries
  - Sends queries to Prometheus or QueryEngine
  - Logs results and timing
  - Used in: All query execution

- **`DumbKafkaConsumerService`** - Simple Kafka consumer for debugging
  - Consumes and prints Kafka messages
  - Used in: Debugging data flow

### How Services Work Together

In a typical SketchDB experiment:

1. **Setup Phase**:
   - `KafkaService` creates topics
   - `ArroyoService` starts and connects to Kafka
   - `QueryEngineService` starts consuming from Kafka output topic
   - `PrometheusService` configures remote write to Arroyo

2. **Workload Phase**:
   - `RustExporterService` exposes metrics
   - Prometheus scrapes and sends to Arroyo via remote write
   - Arroyo builds sketches and publishes to Kafka
   - QueryEngine maintains sketch state

3. **Query Phase**:
   - `RemoteMonitorService` coordinates query execution
   - `PrometheusClientService` sends PromQL queries to QueryEngine
   - Results are logged and compared

4. **Monitoring Phase**:
   - `ArroyoThroughputMonitor` tracks throughput
   - `PrometheusHealthMonitor` checks scrape health
   - `SystemExportersService` provides infrastructure metrics

Notice how all these services start, stop, and interact uniformly? That's because they're built on a common abstraction...

## Service Architecture

Now that you've seen what services do in practice, let's understand how they're implemented.

### Design Philosophy

All services follow a **uniform interface** pattern. Whether you're starting Kafka, Flink, or Prometheus, the code looks the same:

```python
service.start(**kwargs)  # Start the service
service.stop(**kwargs)   # Stop the service
service.is_healthy()     # Check health
service.restart(**kwargs) # Restart
```

This uniformity makes the orchestration code in `experiment_run_e2e.py` clean and predictable.

### Base Service Interface

Located in `experiment_utils/services/base.py`:

```python
from abc import ABC, abstractmethod

class BaseService(ABC):
    """Base class for all services"""

    def __init__(self, provider: InfrastructureProvider):
        self.provider = provider

    @abstractmethod
    def start(self, **kwargs):
        """Start the service"""
        pass

    @abstractmethod
    def stop(self, **kwargs):
        """Stop the service"""
        pass

    def is_healthy(self) -> bool:
        """Check if service is healthy (overridable)"""
        return True

    def restart(self, **kwargs):
        """Restart the service"""
        self.stop(**kwargs)
        self.start(**kwargs)
```

### Docker Service Base

For containerized services:

```python
class DockerServiceBase(BaseService):
    """Base class for Docker-based services"""

    def build_image(self, image_name: str, build_dir: str):
        """Build Docker image"""
        cmd = f"docker build -t {image_name} {build_dir}"
        self.provider.execute_command(...)

    def start_container(self, container_name: str, image: str, **docker_args):
        """Start Docker container"""
        cmd = f"docker run --name {container_name} {image}"
        self.provider.execute_command(...)

    def stop_container(self, container_name: str):
        """Stop Docker container"""
        cmd = f"docker stop {container_name} && docker rm {container_name}"
        self.provider.execute_command(...)
```

## Infrastructure Provider Abstraction

### Provider Interface

Located in `experiment_utils/providers/base.py`:

```python
from abc import ABC, abstractmethod

class InfrastructureProvider(ABC):
    """Abstract base class for infrastructure providers"""

    @abstractmethod
    def execute_command(self, node_idx: int, cmd: str, cmd_dir: str,
                        nohup: bool, popen: bool) -> str:
        """Execute command on a single node"""
        pass

    @abstractmethod
    def execute_command_parallel(self, node_idxs: List[int], cmd: str,
                                  cmd_dir: str, nohup: bool, popen: bool,
                                  wait: bool) -> Dict[int, str]:
        """Execute command on multiple nodes in parallel"""
        pass

    @abstractmethod
    def get_node_address(self, node_idx: int) -> str:
        """Get hostname/address for node"""
        pass

    @abstractmethod
    def get_node_ip(self, node_idx: int) -> str:
        """Get internal IP address for node"""
        pass

    @abstractmethod
    def get_home_dir(self) -> str:
        """Get home directory on remote nodes"""
        pass

    @abstractmethod
    def get_query_log_file(self) -> str:
        """Get query log file path"""
        pass
```

### CloudLab Provider Implementation

Located in `experiment_utils/providers/cloudlab.py`:

### Provider Factory

Located in `experiment_utils/providers/factory.py`:

```python
def create_provider(cfg: DictConfig) -> InfrastructureProvider:
    """Factory function to create provider based on config"""
    # Currently only supports CloudLab
    return CloudLabProvider(
        username=cfg.cloudlab.username,
        hostname_suffix=cfg.cloudlab.hostname_suffix,
        node_offset=cfg.cloudlab.node_offset
    )

    # Future: Detect provider type from config
    # if cfg.provider.type == "aws":
    #     return AWSProvider(...)
    # elif cfg.provider.type == "kubernetes":
    #     return KubernetesProvider(...)
```

## Configuration System

### Hydra Composition

**Base config.yaml:**
```yaml
defaults:
  - _self_
  - experiment_type: ???  # Required: must specify experiment type

experiment:
  name: ???  # Required

cloudlab:
  num_nodes: ???
  username: ???
  hostname_suffix: ???
  node_offset: 0

# ... rest of config
```

**Experiment type config** (e.g., `simple_config_fake_ports_2_card_20.yaml`):
```yaml
# @package experiment_params

# This content gets merged into experiment_params section
experiment:
  - mode: sketchdb
    query_prometheus_too: false

metrics:
  - metric: fake_metric_total
    labels: [label_0, label_1, label_2, instance, job]
    exporter: fake_exporter

# ... more config
```

**Composition result:**
```yaml
# Final composed config
experiment:
  name: my_test

cloudlab:
  num_nodes: 9
  username: myuser
  # ...

experiment_params:  # Merged from experiment_type config
  experiment:
    - mode: sketchdb
      query_prometheus_too: false
  metrics:
    - metric: fake_metric_total
      # ...
```

### Custom Resolvers

Hydra/OmegaConf custom resolvers allow dynamic computation of configuration values at runtime. We use them to solve two key problems:

#### Problem 1: Environment-Dependent Paths

**Why needed:** The experiment output directory path is different on each developer's machine and isn't known at config-writing time. Hardcoding paths in YAML would break portability.

**Solution:** `local_experiment_dir` resolver dynamically inserts the correct local path:

```python
# Registered in experiment_run_e2e.py Lines 43-47
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)
```

**Usage in config:**
```yaml
# Instead of hardcoding: /home/alice/experiments/my_subdir
# We use:
some_path: ${local_experiment_dir:}/my_subdir

# At runtime, resolves to the correct path for your environment
```

#### Problem 2: Network Configuration Dependent on Node Offset

**Why needed:** The remote write endpoint IP address depends on `node_offset`, which can vary per deployment. We need to compute the IP dynamically based on the CloudLab network topology (10.10.1.X).

**Solution:** `remote_write_ip` resolver computes the IP based on node_offset:

```python
# Registered in experiment_run_e2e.py Lines 48-50
OmegaConf.register_new_resolver(
    "remote_write_ip", lambda node_offset: f"10.10.1.{node_offset + 1}"
)
```

**Usage in config:**
```yaml
# Dynamically computes IP based on node_offset
prometheus:
  remote_write:
    url: http://${remote_write_ip:${cloudlab.node_offset}}:8491/write

# If node_offset=0, resolves to: http://10.10.1.1:8491/write
# If node_offset=5, resolves to: http://10.10.1.6:8491/write
```

**Benefits:**
- Single config file works across all environments
- No manual IP calculation or path updates
- Type-safe validation of the final resolved values

### Configuration Validation

**experiment_utils/config.py**: `validate_experiment_config()`

Validates required sections:
- `query_groups` with queries and client options
- `exporters` with exporter_list
- `metrics` with metric definitions
- Cross-validation of metric labels vs exporter num_labels

## Component Architecture

### File Organization

```
experiments/
├── experiment_run_e2e.py              # Main orchestrator script
├── config/                            # Hydra configuration files
│   ├── config.yaml                    # Base configuration
│   └── experiment_type/               # Experiment-specific configs
├── experiment_utils/                  # Core utilities
│   ├── __init__.py
│   ├── config.py                      # Configuration validation/generation
│   ├── sync.py                        # Rsync utilities
│   ├── constants.py                   # System constants
│   ├── providers/                     # Infrastructure providers
│   │   ├── base.py                    # Provider interface
│   │   ├── cloudlab.py                # CloudLab implementation
│   │   └── factory.py                 # Provider factory
│   └── services/                      # Service implementations
│       ├── base.py                    # Base service classes
│       ├── kafka.py                   # KafkaService
│       ├── flink.py                   # FlinkService
│       ├── arroyo.py                  # ArroyoService
│       ├── query_engine.py            # QueryEngineService
│       ├── prometheus.py              # PrometheusService
│       ├── exporters.py               # Exporter services
│       ├── controller.py              # ControllerService
│       ├── remote_monitor.py          # RemoteMonitorService
│       ├── monitoring.py              # Throughput/health monitors
│       └── __init__.py                # Service exports
└── post_experiment/                   # Analysis scripts
```

### Core Modules

**experiment_run_e2e.py** (Lines 1-573)
- Main orchestration script
- Initializes providers and services
- Manages experiment lifecycle
- Coordinates data collection

**experiment_utils/config.py**
- Configuration validation
- Prometheus config generation
- Controller client config generation
- Experiment parameter processing

**experiment_utils/sync.py**
- Rsync operations for code and configs
- Data collection from remote nodes
- Bidirectional sync utilities

**experiment_utils/providers/**
- Infrastructure abstraction
- Currently supports CloudLab via SSH
- Extensible to other cloud providers

**experiment_utils/services/**
- Service implementations
- Each service encapsulates a system component
- Uniform start/stop interface

## Extension Points

### 1. Adding a New Service

**File:** `experiment_utils/services/my_new_service.py`

```python
from .base import BaseService

class MyNewService(BaseService):
    def __init__(self, provider, node_offset):
        super().__init__(provider)
        self.node_offset = node_offset

    def start(self, **kwargs):
        # Implementation
        cmd = "start_my_service.sh"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir="/path/to/service",
            nohup=True,
            popen=False
        )

    def stop(self, **kwargs):
        # Implementation
        cmd = "pkill -f my_service"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir="",
            nohup=False,
            popen=False
        )
```

**Update:** `experiment_utils/services/__init__.py`
```python
from .my_new_service import MyNewService
```

**Use in experiment_run_e2e.py:**
```python
# Initialize (Lines 124-169)
my_service = MyNewService(provider, args.node_offset)

# Start/stop in appropriate lifecycle phase
my_service.start()
# ...
my_service.stop()
```

### 2. Adding a New Infrastructure Provider

**File:** `experiment_utils/providers/aws_provider.py`

```python
from .base import InfrastructureProvider
import boto3

class AWSProvider(InfrastructureProvider):
    def __init__(self, region, instance_ids):
        self.region = region
        self.instance_ids = instance_ids
        self.ec2 = boto3.client('ec2', region_name=region)
        self.ssm = boto3.client('ssm', region_name=region)

    def execute_command(self, node_idx, cmd, cmd_dir, nohup, popen):
        instance_id = self.instance_ids[node_idx]
        if cmd_dir:
            cmd = f"cd {cmd_dir} && {cmd}"

        response = self.ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [cmd]}
        )
        # Handle response...

    def get_node_address(self, node_idx):
        instance_id = self.instance_ids[node_idx]
        response = self.ec2.describe_instances(InstanceIds=[instance_id])
        return response['Reservations'][0]['Instances'][0]['PublicDnsName']

    # Implement other methods...
```

**Update:** `experiment_utils/providers/factory.py`
```python
def create_provider(cfg):
    if cfg.provider.type == "cloudlab":
        return CloudLabProvider(...)
    elif cfg.provider.type == "aws":
        return AWSProvider(
            region=cfg.aws.region,
            instance_ids=cfg.aws.instance_ids
        )
    else:
        raise ValueError(f"Unknown provider: {cfg.provider.type}")
```

### 3. Adding a New Experiment Type

**File:** `experiments/config/experiment_type/my_experiment.yaml`

```yaml
# @package experiment_params

experiment:
  - mode: sketchdb
    query_prometheus_too: false

metrics:
  - metric: my_metric_total
    labels: [label_0, instance, job]
    exporter: fake_exporter

exporters:
  exporter_list:
    fake_exporter:
      num_ports_per_node: 1
      num_labels: 1
      label_values_card: 10
      value_scale: 1000
      distribution: zipf
      sleep_time_between_ts: 5
      metric_type: counter
      metrics:
        - metric: my_metric_total
  only_start_if_queries_exist: true

query_groups:
  - queries:
      - sum_over_time(my_metric_total[1m])
    client_options:
      repetitions: 10
      starting_delay: 60
      repetition_delay: 10
      query_time_offset: 0
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0

servers:
  - name: prometheus
    url: http://localhost:9090
  - name: sketchdb
    url: http://localhost:8088
```

**Use:**
```bash
python experiment_run_e2e.py experiment_type=my_experiment ...
```

### 4. Adding a New Streaming Engine

**File:** `experiment_utils/services/my_streaming_engine.py`

```python
class MyStreamingEngineService(BaseService):
    def start(self, **kwargs):
        # Start cluster
        pass

    def run_myengine_sketch(self, experiment_output_dir, ...):
        # Submit sketch job
        # Return job_id
        pass

    def stop_myengine_sketch(self, job_id):
        # Stop specific job
        pass

    def stop(self, **kwargs):
        # Stop cluster
        pass
```

**Update experiment_run_e2e.py** (around Lines 334-391):

```python
if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
    if args.streaming_engine == "flink":
        # Existing Flink code
    elif args.streaming_engine == "arroyo":
        # Existing Arroyo code
    elif args.streaming_engine == "myengine":
        my_engine_service.start()
        job_id = my_engine_service.run_myengine_sketch(...)
```

## Design Decisions

### 1. Service-Oriented Architecture

**Rationale:** Each component (Kafka, Flink, Prometheus) is an independent service with uniform interface. This enables:
- Easy addition of new components
- Clear separation of concerns
- Independent testing of services
- Flexible deployment (Docker or bare-metal)

### 2. Provider Abstraction

**Rationale:** Infrastructure operations go through provider interface. This enables:
- Portability to different cloud providers
- Testing with local Docker Compose
- Easier mocking for unit tests

### 3. Hydra Configuration System

**Rationale:** Hierarchical composition allows:
- Reusable configuration fragments
- Experiment types extend base config without duplication
- Command-line overrides for quick iteration
- Type-safe configuration with validation

### 4. Coordinator-Worker Pattern

**Rationale:** Node 0 is always coordinator. This simplifies:
- Orchestration (single point of control)
- Data collection (rsync from coordinator)
- Service management (singletons on coordinator)

### 5. Parallel Execution

**Rationale:** All node operations use parallel SSH. This:
- Reduces deployment time
- Reduces experiment startup time
- Improves scalability to many nodes

### 6. Per-Service Containerization Flags

**Rationale:** Individual container flags allow:
- Hybrid deployments (some containerized, some bare-metal)
- Debugging specific components without Docker overhead
- Gradual migration to containerized deployment

### 7. No-Teardown Mode

**Rationale:** Keeps services running for post-experiment inspection. This is:
- Critical for debugging service issues
- Allows manual testing of queries
- Enables inspection of system state

**Limitation:** Only works with single experiment mode to avoid conflicts.

### 8. Experiment Modes

**Rationale:** Run multiple modes (sketchdb, prometheus) sequentially. This:
- Allows direct comparison (same data sources)
- Avoids resource contention
- Simplifies orchestration logic
