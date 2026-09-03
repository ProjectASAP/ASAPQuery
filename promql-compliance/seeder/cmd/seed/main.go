// Command seed pushes a dataset defined in package seeder or a fixture to two
// Prometheus-remote-write-compatible endpoints: a real Prometheus (started
// with --web.enable-remote-write-receiver) and ASAPQuery's own remote-write
// ingest endpoint. Using the same WriteRequest bytes against both means
// there's no risk of the two ingestion mechanisms disagreeing and producing
// false diffs in the differential PromQL runner (see issue
// #594).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
)

func main() {
	referenceURL := flag.String("reference-url", "", "base URL of the reference target (real Prometheus), e.g. http://localhost:9090")
	testURL := flag.String("test-url", "", "base URL of the test target (ASAPQuery), e.g. http://localhost:9091")
	datasetPath := flag.String("dataset", "", "optional YAML dataset fixture; defaults to the built-in dataset")
	baseTimeFlag := flag.Int64("base-time-ms", 0, "base Unix time in ms to anchor the dataset's offsets to (default: now, floored to the minute, minus 30 minutes so the whole 20-minute window is safely in the past)")
	flag.Parse()

	if *referenceURL == "" || *testURL == "" {
		fmt.Fprintln(os.Stderr, "usage: seed --reference-url=<url> --test-url=<url> [--base-time-ms=<ms>]")
		os.Exit(2)
	}

	baseTimeMs := *baseTimeFlag
	if baseTimeMs == 0 {
		now := time.Now().UTC().Truncate(time.Minute)
		baseTimeMs = now.Add(-30 * time.Minute).UnixMilli()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	if *datasetPath == "" {
		err = seeder.PushDataset(ctx, baseTimeMs, *referenceURL, *testURL)
	} else {
		fixture, loadErr := seeder.LoadFixture(*datasetPath)
		if loadErr != nil {
			log.Fatalf("loading dataset failed: %v", loadErr)
		}
		err = seeder.PushFixture(ctx, baseTimeMs, fixture, *referenceURL, *testURL)
	}
	if err != nil {
		log.Fatalf("seeding failed: %v", err)
	}

	fmt.Printf("seeded dataset to %s and %s\n", *referenceURL, *testURL)
	fmt.Printf("base-time-ms=%d\n", baseTimeMs)
}
