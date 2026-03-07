# CLUSTER DATA EXPORTER

A Prometheus exporter that exposes cluster resource usage metrics from Google and Alibaba cluster trace datasets.

## DESCRIPTION

This exporter reads CSV data from certain datasets provided by Google or Alibaba and exposes them as Prometheus metrics. The exporter supports both Google task resource usage data from 2011 and Alibaba node and microservice resource data from 2021 and 2022. Instructions for downloading this data are linked in this document.

## INSTALLATION

### Prerequisites

- Rust 1.70+ (edition 2021)
- Access to Google or Alibaba cluster datasets

### Building

```bash
cargo build --release
```

## USAGE

```bash
cluster_data_exporter -i <input_directory> -p <port> <provider> [OPTIONS]
```

### Google Provider

```bash
cluster_data_exporter -i ./google/clusterdata-2011/ -p 8080 google [OPTIONS]
```

### Alibaba Provider

```bash
cluster_data_exporter -i ./alibaba/2021/ -p 8080 alibaba [OPTIONS]
```

## DATA SOURCES

### Google Cluster Data

Instructions on how to download the Google Cluster 2011 task usage data:
https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md

The only part of the dataset used by the exporter is the task_usage section, so there's no need to install the whole dataset

Expected directory structure:
```
path/to/task/resource/usage/dir/
├── part-00000-of-00500.csv.gz
├── part-00001-of-00500.csv.gz
└── ...
```

### Alibaba Cluster Data

Instructions on downloading the Alibaba microservice trace datasets:
- 2021: https://github.com/alibaba/clusterdata/blob/master/cluster-trace-microservices-v2021/README.md#introduction-of-trace-data
- 2022: https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022#trace-data-download

The only parts of the datasets used by the exporter are the Node and MSResource sections, the rest can be discarded.

Expected directory structure (after preprocessing):

2021 Data:
```
path/to/Node/
├── Node_0.csv.gz
├── Node_1.csv.gz
└── ...

path/to/MSResource/
├── MSResource_0.csv.gz
├── MSResource_1.csv.gz
└── ...
```

2022 Data:
```
path/to/NodeMetrics/
├── NodeMetrics_0.csv.gz
├── NodeMetrics_1.csv.gz
└── ...

path/to/MSMetrics/
├── MSMetrics_0.csv.gz
├── MSMetrics_1.csv.gz
└── ...
```

## DATA PREPROCESSING FOR ALIBABA

IMPORTANT: Before running the exporter on Alibaba data, you must run the preprocessing script to sort the data by timestamp and recompress it as a .csv.gz:

```bash
./bin/alibaba/sort_and_format.sh <alibaba_data_directory> --year <2021|2022> [-n] [-m]
```

This script extracts, sorts by timestamp, and recompresses the Alibaba CSV files in a format the exporter can read (.csv.gz). The sorting is necessary because some datasets (mainly 2022 data) are not sorted by timestamp, which is required for proper metric export timing.

### Input Directory Structure

The input directory should contain one or both of the subdirectories with unprocessed files, i.e. the untouched /data/ directory created from running the fetchData.sh scripts from the Alibaba github repos. For example:

```
alibaba/2021/data/
├── Node/
│   ├── Node_0.tar.gz
│   ├── Node_1.tar.gz
│   └── ...
└── MSResource/
    ├── MSResource_0.tar.gz
    ├── MSResource_1.tar.gz
    └── ...

alibaba/2022/data/
├── NodeMetrics/
│   ├── NodeMetrics_0.tar.gz
│   ├── NodeMetrics_1.tar.gz
│   └── ...
└── MSMetrics/
    ├── MSMetrics_0.tar.gz
    ├── MSMetrics_1.tar.gz
    └── ...
```

Examples:

```bash
# Process 2021 Node data
./bin/alibaba/sort_and_format.sh alibaba/2021/data --year 2021 -n

# Process 2021 MSResource data
./bin/alibaba/sort_and_format.sh alibaba/2021/data --year 2021 -m

# Process both Node and MSResource data for 2021
./bin/alibaba/sort_and_format.sh alibaba/2021/data --year 2021 -n -m
```

## COMMAND LINE ARGUMENTS

- -i, --input-directory: Path to the directory containing CSV data files
- -p, --port: Port number for the HTTP server

### Provider-specific Options

#### Google
- --metrics: Specific metrics to export from task resource usage data
- --all-parts: Process all CSV parts (default behavior)
- --part-index: Process only a specific part index (0-499)

#### Alibaba
- --data-type: Type of data to export (node or msresource)
- --data-year: Year of the dataset (2021 or 2022)
- --all-parts: Process all CSV parts (default behavior)
- --part-index: Process only a specific part index

## DOCKER USAGE

### Prerequisites for Docker

1. Download and preprocess your CSV data as described in the DATA SOURCES section above
2. Place the preprocessed data in a local directory (e.g., `./data/`)

### Building and Running with Docker

Build the Docker image:
```bash
docker build -t cluster-data-exporter .
```

Run with Docker (example for Google data):
```bash
docker run -v ./data:/data:ro -p 40000:40000 cluster-data-exporter \
  --input-directory /data \
  --port 40000 \
  google \
  --metrics mean_cpu_usage_rate,canonical_memory_usage \
  --all-parts
```

Run with Docker (example for Alibaba data):
```bash
docker run -v ./data:/data:ro -p 40000:40000 cluster-data-exporter \
  --input-directory /data \
  --port 40000 \
  alibaba \
  --data-type node \
  --data-year 2021 \
  --all-parts
```

### Using Docker Compose

#### Automated Generation with Python Script

The `scripts/generate_docker_compose.py` script automatically generates docker-compose.yml files from the frame templates and fill in certain fields.

**Google Provider Example:**
```bash
python scripts/generate_docker_compose.py google --metrics mean_cpu_usage_rate,max_cpu_usage --port 8080 --input-dir ./data
```

**Alibaba Provider Example:**
```bash
python scripts/generate_docker_compose.py alibaba --data-type node --data-year 2021 --port 8080 --input-dir ./data
```

The script will:
- Validate your configuration options
- Generate a docker-compose.yml file with correct settings
- Update port mappings and volume mounts automatically

#### Manual Setup with Frame Files

Alternatively, the `docker_compose_frames/` directory contains pre-configured docker-compose files for different providers and configurations. These frame files will still require small edits before running docker-compose, see each frame file for more information.

- **Google Provider**: `google-docker-compose.yml` - Edit list of metrics to export
- **Alibaba Provider**: Provider-specific frames for each data type and year combination:
  - `alibaba-node-2021-docker-compose.yml`
  - `alibaba-node-2022-docker-compose.yml`
  - `alibaba-msresource-2021-docker-compose.yml`
  - `alibaba-msresource-2022-docker-compose.yml`

To use a frame file:
1. Copy the appropriate frame file from `docker_compose_frames/` to your working directory as `docker-compose.yml`
2. Edit the file with any options that still need to be filled in (marked with "CHANGE THIS" comments)
3. Run: `docker-compose up -d`

### Data Volume Requirements

- The container expects data to be mounted at `/data`
- Data must be preprocessed according to the instructions in the DATA SOURCES section
- For Alibaba data, ensure you've run the sorting and compression scripts before mounting
- Mount the volume as read-only (`:ro`)

## METRICS ENDPOINT

Once running, metrics are available at:
```
http://localhost:<port>/metrics
```
