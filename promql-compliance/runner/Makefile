.PHONY: test run run-all run-for-ci run-cases

DATASET ?= ../datasets/single-rate.yaml
SUITE ?= ../suites/temporal.yaml
REPORT_DIR ?= /tmp/asapquery-differential-reports
RUN_ID ?= $(shell date +%s%N)
COMPOSE_PROJECT ?= asapquery-differential-$(RUN_ID)
BASE_TIME_MS ?= $(shell now_ms=$$(date +%s%3N); echo $$((now_ms / 300000 * 300000 - 1800000)))

CI_CASES := \
	single-rate-temporal:../datasets/single-rate.yaml:../suites/temporal.yaml \
	single-rate-off-grid-rate:../datasets/single-rate.yaml:../suites/off-grid-rate.yaml \
	aggregations-native-dag:../datasets/aggregations.yaml:../suites/native-dag-aggregations.yaml \
	aggregations:../datasets/aggregations.yaml:../suites/aggregations.yaml \
	quantiles:../datasets/quantiles.yaml:../suites/quantiles.yaml

# Tracks planner coverage (#738); most queries are expected to fail.
NON_CI_CASES := \
	olly-bench:../datasets/olly-bench.yaml:../suites/olly-bench.yaml

test:
	go test ./...

run:
	go run ./cmd/differential-runner \
		--dataset $(DATASET) \
		--suite $(SUITE) \
		--compose-file ../docker-compose.yml \
		--compose-project $(COMPOSE_PROJECT) \
		--base-time-ms $(BASE_TIME_MS)

run-all:
	@$(MAKE) --no-print-directory run-cases CASES="$(CI_CASES) $(NON_CI_CASES)"

run-for-ci:
	@$(MAKE) --no-print-directory run-cases CASES="$(CI_CASES)"

run-cases:
	@set -eu; \
	mkdir -p "$(REPORT_DIR)"; \
	result=0; \
	run_id="$$(date +%s%N)"; \
	now_ms="$$(date +%s%3N)"; \
	base_time_ms="$$((now_ms / 300000 * 300000 - 1800000))"; \
	port_seed="$$((run_id % 10000))"; \
	case_index=0; \
	for test_case in $(CASES); do \
		name="$${test_case%%:*}"; \
		rest="$${test_case#*:}"; \
		dataset="$${rest%%:*}"; \
		suite="$${rest#*:}"; \
		project="asapquery-differential-$$name-$$run_id"; \
		case_base_time_ms="$$((base_time_ms + case_index * 60000))"; \
		port_offset="$$((port_seed + case_index))"; \
		prometheus_port="$$((20000 + port_offset))"; \
		query_port="$$((30000 + port_offset))"; \
		ingest_port="$$((40000 + port_offset))"; \
		case_index="$$((case_index + 1))"; \
		echo "Running $$name ($$dataset, $$suite)"; \
		if ! PROMETHEUS_PORT="$$prometheus_port" ASAP_QUERY_PORT="$$query_port" ASAP_INGEST_PORT="$$ingest_port" go run ./cmd/differential-runner \
			--dataset "$$dataset" \
			--suite "$$suite" \
			--compose-file ../docker-compose.yml \
			--compose-project "$$project" \
			--reference-url "http://localhost:$$prometheus_port" \
			--reference-write-url "http://localhost:$$prometheus_port" \
			--test-url "http://localhost:$$query_port" \
			--test-write-url "http://localhost:$$ingest_port" \
			--base-time-ms "$$case_base_time_ms" \
			--output "$(REPORT_DIR)/$$name.json"; then \
			echo "FAILED: $$name" >&2; \
			result=1; \
		fi; \
	done; \
	exit $$result
