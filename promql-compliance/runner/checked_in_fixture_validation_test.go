package runner

import (
	"testing"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
)

func TestCheckedInFixturesAndSuites(t *testing.T) {
	pairs := []struct{ dataset, suite string }{
		{"../datasets/aggregations.yaml", "../suites/aggregations.yaml"},
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
