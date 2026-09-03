package runner

import (
	"context"
	"time"
)

type Report struct {
	Suite    string        `json:"suite"`
	Dataset  string        `json:"dataset"`
	BaseTime time.Time     `json:"baseTime"`
	Queries  []QueryReport `json:"queries"`
	Passed   bool          `json:"passed"`
}

// CompareSuite evaluates a suite against both targets. It returns a report
// even when individual queries differ; infrastructure failures are returned as
// errors so callers can distinguish a failed test from a broken environment.
func CompareSuite(ctx context.Context, reference, test QueryAPI, suite Suite, datasetName string, base time.Time) (Report, error) {
	report := Report{
		Suite:    suite.Name,
		Dataset:  datasetName,
		BaseTime: base,
		Passed:   true,
	}
	for _, query := range suite.Queries {
		result, err := CompareQuery(ctx, reference, test, query, base, suite.ComparisonDefaults)
		if err != nil {
			return report, err
		}
		report.Queries = append(report.Queries, result)
		report.Passed = report.Passed && result.Passed
	}
	return report, nil
}
