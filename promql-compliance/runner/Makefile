.PHONY: test run run-all

DATASET ?= ../datasets/single-rate.yaml
SUITE ?= ../suites/temporal.yaml
REPORT_DIR ?= /tmp/asapquery-differential-reports

CASES := \
	single-rate-temporal:../datasets/single-rate.yaml:../suites/temporal.yaml \
	sparse-checkout-temporal:../datasets/sparse-checkout.yaml:../suites/temporal.yaml \
	aggregations:../datasets/aggregations.yaml:../suites/aggregations.yaml \
	aggregations-dense-cadence:../datasets/aggregations-dense-cadence.yaml:../suites/aggregations.yaml

test:
	go test ./...

run:
	go run ./cmd/differential-runner \
		--dataset $(DATASET) \
		--suite $(SUITE) \
		--compose-file ../docker-compose.yml

run-all:
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
