.PHONY: test run

test:
	go test ./...

run:
	go run ./cmd/differential-runner \
		--dataset ../datasets/single-rate.yaml \
		--suite ../suites/temporal.yaml \
		--compose-file ../docker-compose.yml
