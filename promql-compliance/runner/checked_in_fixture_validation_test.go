package runner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

func TestCheckedInFixturesAndSuites(t *testing.T) {
	pairs := []struct{ dataset, suite string }{
		{"../datasets/aggregations.yaml", "../suites/aggregations.yaml"},
		{"../datasets/topk-ordering.yaml", "../suites/topk-ordering.yaml"},
		{"../datasets/quantiles.yaml", "../suites/quantiles.yaml"},
		{"../datasets/olly-bench.yaml", "../suites/olly-bench.yaml"},
	}
	parser := promqlparser.NewParser(promqlparser.Options{})
	for _, pair := range pairs {
		if _, err := seeder.LoadFixture(pair.dataset); err != nil {
			t.Fatalf("LoadFixture: %v", err)
		}
		suite, err := LoadSuiteFile(pair.suite)
		if err != nil {
			t.Fatalf("LoadSuiteFile: %v", err)
		}
		for _, query := range suite.Queries {
			if _, err := parser.ParseExpr(query.Expr); err != nil {
				t.Fatalf("parse %q: %v", query.Expr, err)
			}
		}
	}
}

func TestCheckedInTemporalSuite(t *testing.T) {
	if _, err := seeder.LoadFixture("../datasets/single-rate.yaml"); err != nil {
		t.Fatalf("LoadFixture: %v", err)
	}
	suite, err := LoadSuiteFile("../suites/temporal.yaml")
	if err != nil {
		t.Fatalf("LoadSuiteFile: %v", err)
	}
	parser := promqlparser.NewParser(promqlparser.Options{})
	for _, query := range suite.Queries {
		if _, err := parser.ParseExpr(query.Expr); err != nil {
			t.Fatalf("parse %q: %v", query.Expr, err)
		}
	}
}

func TestCheckedInOffGridRateSuite(t *testing.T) {
	if _, err := seeder.LoadFixture("../datasets/single-rate.yaml"); err != nil {
		t.Fatalf("LoadFixture: %v", err)
	}
	suite, err := LoadSuiteFile("../suites/off-grid-rate.yaml")
	if err != nil {
		t.Fatalf("LoadSuiteFile: %v", err)
	}
	parser := promqlparser.NewParser(promqlparser.Options{})
	for _, query := range suite.Queries {
		if _, err := parser.ParseExpr(query.Expr); err != nil {
			t.Fatalf("parse %q: %v", query.Expr, err)
		}
	}
}

func TestCheckedInNativeDagSuites(t *testing.T) {
	parser := promqlparser.NewParser(promqlparser.Options{})
	for _, path := range []string{
		"../suites/native-dag-aggregations.yaml",
		"../suites/nested-aggregations.yaml",
	} {
		suite, err := LoadSuiteFile(path)
		if err != nil {
			t.Fatalf("LoadSuiteFile(%q): %v", path, err)
		}
		for _, query := range suite.Queries {
			if _, err := parser.ParseExpr(query.Expr); err != nil {
				t.Fatalf("parse %q: %v", query.Expr, err)
			}
		}
	}
}

// Every rollup suite query must have a query config in the fixed planner
// output, naming aggregations that exist, on a metric and labels the dataset
// has; a mismatch would otherwise surface only as a slow Docker-suite failure.
func TestCheckedInRollupSuiteMatchesFixedPlannerOutput(t *testing.T) {
	fixture, err := seeder.LoadFixture("../datasets/rollup.yaml")
	if err != nil {
		t.Fatalf("LoadFixture: %v", err)
	}
	suite, err := LoadSuiteFile("../suites/rollup.yaml")
	if err != nil {
		t.Fatalf("LoadSuiteFile: %v", err)
	}
	if suite.PlannerOutputDir == "" {
		t.Fatal("rollup suite has no planner_output_dir")
	}
	contents, err := os.ReadFile(filepath.Join(suite.PlannerOutputDir, plannerInferenceConfigFile))
	if err != nil {
		t.Fatalf("read inference config: %v", err)
	}
	var inference struct {
		Queries []struct {
			Query        string `yaml:"query"`
			Aggregations []struct {
				AggregationID int `yaml:"aggregation_id"`
			} `yaml:"aggregations"`
		} `yaml:"queries"`
	}
	if err := yaml.Unmarshal(contents, &inference); err != nil {
		t.Fatalf("parse inference config: %v", err)
	}
	streamingContents, err := os.ReadFile(filepath.Join(suite.PlannerOutputDir, plannerStreamingConfigFile))
	if err != nil {
		t.Fatalf("read streaming config: %v", err)
	}
	var streaming struct {
		Aggregations []struct {
			AggregationID int    `yaml:"aggregationId"`
			Metric        string `yaml:"metric"`
			Labels        struct {
				Grouping []string `yaml:"grouping"`
			} `yaml:"labels"`
		} `yaml:"aggregations"`
	}
	if err := yaml.Unmarshal(streamingContents, &streaming); err != nil {
		t.Fatalf("parse streaming config: %v", err)
	}
	datasetLabels := make(map[string]map[string]struct{})
	for _, series := range fixture.Series {
		if datasetLabels[series.Metric] == nil {
			datasetLabels[series.Metric] = make(map[string]struct{})
		}
		for label := range series.Labels {
			datasetLabels[series.Metric][label] = struct{}{}
		}
	}
	aggregationIDs := make(map[int]struct{}, len(streaming.Aggregations))
	for _, aggregation := range streaming.Aggregations {
		aggregationIDs[aggregation.AggregationID] = struct{}{}
		labels, ok := datasetLabels[aggregation.Metric]
		if !ok {
			t.Errorf("aggregation %d is on metric %q, which the dataset lacks", aggregation.AggregationID, aggregation.Metric)
			continue
		}
		for _, label := range aggregation.Labels.Grouping {
			if _, ok := labels[label]; !ok {
				t.Errorf("aggregation %d groups by %q, which %q series lack", aggregation.AggregationID, label, aggregation.Metric)
			}
		}
	}
	for _, query := range inference.Queries {
		for _, reference := range query.Aggregations {
			if _, ok := aggregationIDs[reference.AggregationID]; !ok {
				t.Errorf("query %q names aggregation %d, which the streaming config lacks", query.Query, reference.AggregationID)
			}
		}
	}
	configured := make(map[string]struct{}, len(inference.Queries))
	for _, query := range inference.Queries {
		configured[query.Query] = struct{}{}
	}
	parser := promqlparser.NewParser(promqlparser.Options{})
	for _, query := range suite.Queries {
		if _, err := parser.ParseExpr(query.Expr); err != nil {
			t.Fatalf("parse %q: %v", query.Expr, err)
		}
		if _, ok := configured[query.Expr]; !ok {
			t.Errorf("query %q has no query config in the fixed planner output", query.Expr)
		}
	}
	if got, want := len(inference.Queries), len(suite.Queries); got != want {
		t.Errorf("fixed planner output has %d query configs, suite has %d queries", got, want)
	}
}
