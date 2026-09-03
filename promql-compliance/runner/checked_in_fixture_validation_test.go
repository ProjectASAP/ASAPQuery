package runner

import (
	"testing"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
)

func TestCheckedInAggregationFixtureAndSuite(t *testing.T) {
	if _, err := seeder.LoadFixture("../datasets/aggregations.yaml"); err != nil {
		t.Fatalf("LoadFixture: %v", err)
	}
	suite, err := LoadSuiteFile("../suites/aggregations.yaml")
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
