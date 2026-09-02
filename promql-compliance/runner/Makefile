.PHONY: test run

test:
	go test ./...

run:
	go run ./cmd/differential-runner \
		--dataset ../datasets/sparse-checkout.yaml \
		--suite ../suites/temporal.yaml \
		--compose-file ../docker-compose.yml
