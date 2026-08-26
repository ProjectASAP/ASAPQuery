package output

import (
	"fmt"
	"strings"

	"github.com/prometheus/compliance/promql/comparer"
	"github.com/prometheus/compliance/promql/config"
)

// Text produces text-based output for a number of query results.
func Text(results []*comparer.Result, includePassing bool, tweaks []*config.QueryTweak) {
	successes := 0
	unsupported := 0
	for _, res := range results {
		if res.Success() {
			successes++
			if !includePassing {
				continue
			}
		}
		if res.Unsupported || res.InstantUnsupported {
			unsupported++
		}

		fmt.Println(strings.Repeat("-", 80))
		fmt.Printf("QUERY: %v\n", res.TestCase.Query)
		fmt.Printf("START: %v, STOP: %v, STEP: %v\n", res.TestCase.Start, res.TestCase.End, res.TestCase.Resolution)

		// Range and instant results are reported separately, since a query can
		// pass one and fail the other (e.g. "PASS: instant, FAIL: range").
		fmt.Printf("RESULT (range): %v\n", rangeResultLabel(res))
		if !res.RangeSuccess() {
			if res.UnexpectedFailure != "" {
				fmt.Printf("Query failed unexpectedly: %v\n", res.UnexpectedFailure)
			}
			if res.UnexpectedSuccess {
				fmt.Println("Query succeeded, but should have failed.")
			}
			if res.Diff != "" {
				fmt.Println("Query returned different results:")
				fmt.Println(res.Diff)
			}
		}

		fmt.Printf("RESULT (instant): %v\n", instantResultLabel(res))
		if !res.InstantSuccess() {
			if res.InstantUnexpectedFailure != "" {
				fmt.Printf("Instant query failed unexpectedly: %v\n", res.InstantUnexpectedFailure)
			}
			if res.InstantUnexpectedSuccess {
				fmt.Println("Instant query succeeded, but should have failed.")
			}
			if res.InstantDiff != "" {
				fmt.Println("Instant query returned different results:")
				fmt.Println(res.InstantDiff)
			}
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("General query tweaks:")
	if len(tweaks) == 0 {
		fmt.Println("None.")
	}
	for _, t := range tweaks {
		fmt.Println("* ", t.Note)
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Total: %d / %d (%.2f%%) passed, %d unsupported\n", successes, len(results), 100*float64(successes)/float64(len(results)), unsupported)
}

func rangeResultLabel(res *comparer.Result) string {
	switch {
	case res.RangeSuccess():
		return "PASSED"
	case res.Unsupported:
		return "UNSUPPORTED"
	default:
		return "FAILED"
	}
}

func instantResultLabel(res *comparer.Result) string {
	switch {
	case res.InstantSuccess():
		return "PASSED"
	case res.InstantUnsupported:
		return "UNSUPPORTED"
	default:
		return "FAILED"
	}
}
