package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/runner"
)

type stringList []string

func (values *stringList) String() string { return fmt.Sprint([]string(*values)) }

func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	var composeFiles stringList
	datasetPath := flag.String("dataset", "", "dataset fixture YAML path")
	suitePath := flag.String("suite", "", "query suite YAML path")
	referenceURL := flag.String("reference-url", "http://localhost:19090", "Prometheus query URL")
	testURL := flag.String("test-url", "http://localhost:18088", "ASAPQuery query URL")
	referenceWriteURL := flag.String("reference-write-url", "http://localhost:19090", "Prometheus remote-write base URL")
	testWriteURL := flag.String("test-write-url", "http://localhost:19091", "ASAPQuery remote-write base URL")
	baseTimeMS := flag.Int64("base-time-ms", 0, "dataset base Unix time in milliseconds; default is now minus 30 minutes")
	composeProject := flag.String("compose-project", "asapquery-differential", "Docker Compose project name")
	outputPath := flag.String("output", "differential-report.json", "JSON report path")
	keepServices := flag.Bool("keep-services", false, "leave Compose services running after the run")
	flag.Var(&composeFiles, "compose-file", "Compose file to start; may be repeated")
	flag.Parse()

	if *datasetPath == "" || *suitePath == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	report, err := runner.Run(ctx, runner.RunOptions{
		DatasetPath:       *datasetPath,
		SuitePath:         *suitePath,
		ReferenceURL:      *referenceURL,
		TestURL:           *testURL,
		ReferenceWriteURL: *referenceWriteURL,
		TestWriteURL:      *testWriteURL,
		ComposeFiles:      composeFiles,
		ComposeProject:    *composeProject,
		BaseTimeMS:        *baseTimeMS,
		KeepServices:      *keepServices,
	})
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Create(*outputPath)
	if err != nil {
		log.Fatalf("create report %q: %v", *outputPath, err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		_ = file.Close()
		log.Fatalf("write report %q: %v", *outputPath, err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("close report %q: %v", *outputPath, err)
	}

	fmt.Printf("dataset=%s suite=%s base-time=%s passed=%t report=%s\n", report.Dataset, report.Suite, report.BaseTime.Format(time.RFC3339), report.Passed, *outputPath)
	if !report.Passed {
		os.Exit(1)
	}
}
