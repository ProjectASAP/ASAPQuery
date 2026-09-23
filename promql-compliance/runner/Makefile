.PHONY: test run run-all run-for-ci run-cases

DATASET ?= ../datasets/single-rate.yaml
SUITE ?= ../suites/temporal.yaml
REPORT_DIR ?= /tmp/asapquery-differential-reports

CI_CASES := \
	single-rate-temporal:../datasets/single-rate.yaml:../suites/temporal.yaml \
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
		--compose-file ../docker-compose.yml

run-all:
	@$(MAKE) --no-print-directory run-cases CASES="$(CI_CASES) $(NON_CI_CASES)"

run-for-ci:
	@$(MAKE) --no-print-directory run-cases CASES="$(CI_CASES)"

run-cases:
	@set -eu; \
	mkdir -p "$(REPORT_DIR)"; \
	result=0; \
	for test_case in $(CASES); do \
		name="$${test_case%%:*}"; \
		rest="$${test_case#*:}"; \
		dataset="$${rest%%:*}"; \
		suite="$${rest#*:}"; \
		echo "Running $$name ($$dataset, $$suite)"; \
		if ! go run ./cmd/differential-runner \
			--dataset "$$dataset" \
			--suite "$$suite" \
			--compose-file ../docker-compose.yml \
			--output "$(REPORT_DIR)/$$name.json"; then \
			echo "FAILED: $$name" >&2; \
			result=1; \
		fi; \
	done; \
	exit $$result
