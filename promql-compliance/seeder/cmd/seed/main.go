// Command seed pushes the fixed dataset defined in package seeder to two
// Prometheus-remote-write-compatible endpoints: a real Prometheus (started
// with --web.enable-remote-write-receiver) and ASAPQuery's own remote-write
// ingest endpoint. Using the same WriteRequest bytes against both means
// there's no risk of the two ingestion mechanisms disagreeing and producing
// false diffs in the differential PromQL compliance harness (see issue
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

	if err := seeder.PushDataset(ctx, baseTimeMs, *referenceURL, *testURL); err != nil {
		log.Fatalf("seeding failed: %v", err)
	}

	fmt.Printf("seeded dataset to %s and %s\n", *referenceURL, *testURL)
	fmt.Printf("base-time-ms=%d (dataset offsets 0..1200s map to this base)\n", baseTimeMs)
	fmt.Printf("dataset window: [%d, %d] ms unix\n", baseTimeMs, baseTimeMs+1200*1000)
}
