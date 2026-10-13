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
// output; a mismatched string would make the engine reject it rather than
// exercise a roll-up.
func TestCheckedInRollupSuiteMatchesFixedPlannerOutput(t *testing.T) {
	if _, err := seeder.LoadFixture("../datasets/rollup.yaml"); err != nil {
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
			Query string `yaml:"query"`
		} `yaml:"queries"`
	}
	if err := yaml.Unmarshal(contents, &inference); err != nil {
		t.Fatalf("parse inference config: %v", err)
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
