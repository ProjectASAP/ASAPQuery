# ASAP Developer Documentation

## 01. Getting Started

Quick onboarding for new developers:
- [Overview & Key Concepts](01-getting-started/overview.md) - What is ASAP?
- [Architecture](01-getting-started/architecture.md) - System design & data flows
- [Local Setup](01-getting-started/local-setup.md) - Set up dev environment

## 02. Components

Deep dives into each component:
- [Component Index](02-components/README.md) - Component overview & 1-liners
- [Query Engine](02-components/query-engine.md) - Rust query processor
- [Arroyo](02-components/arroyo.md) - Streaming engine (fork + customizations)
- [asap-summary-ingest](02-components/arroyosketch.md) - Pipeline configurator
- [asap-planner-rs](02-components/controller.md) - Auto-configuration service
- [Exporters](02-components/exporters.md) - Metric generators
- [asap-tools](02-components/utilities.md) - Experiment framework

## 03. How-To Guides

Task-oriented guides for common operations:

### Development Tasks
- [Add Protocol Adapter](03-how-to-guides/development/add-protocol-adapter.md) - Support new query protocol
- [Add Fallback Backend](03-how-to-guides/development/add-fallback-backend.md) - Support new database
- [Add Query Language](03-how-to-guides/development/add-query-language.md) - Support new language (cross-component)
- [Add New Sketch](03-how-to-guides/development/add-new-sketch.md) - Implement new sketch algorithm
- [Modify Sketch Logic](03-how-to-guides/development/modify-sketch-logic.md) - Change existing sketch

### Experiment Tasks
- [Run an Experiment](03-how-to-guides/experiments/run-experiment.md) - End-to-end experiment workflow
- [Add Workload](03-how-to-guides/experiments/add-workload.md) - Create new query workload
- [Add Data Generator](03-how-to-guides/experiments/add-data-generator.md) - New exporters
- [Analyze Results](03-how-to-guides/experiments/analyze-results.md) - Post-experiment analysis

### Operations Tasks
- [Manual Stack Run for Prometheus](03-how-to-guides/operations/manual-stack-run-prometheus.md) - Run ASAP components manually to accelerate Prometheus
- [Bootstrap Config from Query Log](03-how-to-guides/operations/bootstrap-config-from-query-log.md) - Auto-generate sketch configs from Prometheus query traffic
- [Manual Stack Run for Clickhouse](03-how-to-guides/operations/manual-stack-run-clickhouse.md) - Run ASAP components manually to accelerate Clickhouse
- [Deploy to CloudLab](03-how-to-guides/operations/deploy-cloudlab.md) - Deployment guide
- [Troubleshooting](03-how-to-guides/operations/troubleshooting.md) - Common issues & solutions

## 04. Development

Developer practices and infrastructure:
- [Testing Guide](04-development/testing-guide.md) - Unit, integration, E2E tests
- [Code Style](04-development/code-style.md) - Conventions & pre-commit hooks
- [CI/CD](04-development/ci-cd.md) - GitHub Actions & Docker builds
- [Contributing](04-development/contributing.md) - PR process & guidelines

## Component-Specific Documentation

Technical details co-located with code:
- [asap-query-engine](../asap-query-engine/docs/README.md) - Extensibility guides
- [asap-tools/Experiments](../asap-tools/docs/architecture.md) - Experiment framework architecture
- [asap-planner-rs](../asap-planner-rs/README.md) - Controller internals
- [asap-summary-ingest](../asap-summary-ingest/README.md) - Pipeline configuration
- [asap-tools/data-sources/prometheus-exporters](../asap-tools/data-sources/prometheus-exporters/README.md) - Exporter implementations
