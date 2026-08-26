package comparer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/compliance/promql/config"
)

const (
	defaultFraction = 0.00001
	defaultMargin   = 0.0
)

// PromAPI allows running instant and range queries against a Prometheus-compatible API.
type PromAPI interface {
	// Query performs a query for the given time.
	Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
	// QueryRange performs a query for the given range.
	QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error)
}

// TestCase represents a fully expanded query to be tested.
type TestCase struct {
	Query          string        `json:"query"`
	SkipComparison bool          `json:"skipComparison"`
	ShouldFail     bool          `json:"shouldFail"`
	Start          time.Time     `json:"start"`
	End            time.Time     `json:"end"`
	Resolution     time.Duration `json:"resolution"`
}

// A Comparer allows comparing query results for test cases between a reference API and a test API.
type Comparer struct {
	refAPI         PromAPI
	testAPI        PromAPI
	queryTweaks    []*config.QueryTweak
	compareOptions cmp.Options
}

// New returns a new Comparer.
func New(refAPI, testAPI PromAPI, queryTweaks []*config.QueryTweak) *Comparer {
	var options cmp.Options
	addFloatCompareOptions(queryTweaks, &options)
	addDropResultLabelsOptions(queryTweaks, &options)
	addCaseInsensitiveCompareOptions(queryTweaks, &options)
	return &Comparer{
		refAPI:         refAPI,
		testAPI:        testAPI,
		queryTweaks:    queryTweaks,
		compareOptions: options,
	}
}

// Result tracks a single test case's query comparison result.
//
// The range-query outcome (Diff/UnexpectedFailure/UnexpectedSuccess/Unsupported)
// and the instant-query outcome (the Instant-prefixed fields) are tracked and
// reported independently, rather than folded into one aggregate pass/fail: a
// query can pass as a range query but fail as an instant query (or vice versa)
// -- see RangeSuccess/InstantSuccess.
type Result struct {
	TestCase          *TestCase `json:"testCase"`
	Diff              string    `json:"diff"`
	UnexpectedFailure string    `json:"unexpectedFailure"`
	UnexpectedSuccess bool      `json:"unexpectedSuccess"`
	Unsupported       bool      `json:"unsupported"`

	InstantDiff              string `json:"instantDiff"`
	InstantUnexpectedFailure string `json:"instantUnexpectedFailure"`
	InstantUnexpectedSuccess bool   `json:"instantUnexpectedSuccess"`
	InstantUnsupported       bool   `json:"instantUnsupported"`
}

// RangeSuccess returns true if the range-query comparison was successful.
func (r *Result) RangeSuccess() bool {
	return r.Diff == "" && !r.UnexpectedSuccess && r.UnexpectedFailure == ""
}

// InstantSuccess returns true if the instant-query comparison was successful.
func (r *Result) InstantSuccess() bool {
	return r.InstantDiff == "" && !r.InstantUnexpectedSuccess && r.InstantUnexpectedFailure == ""
}

// Success returns true if both the range-query and instant-query comparison results were successful.
func (r *Result) Success() bool {
	return r.RangeSuccess() && r.InstantSuccess()
}

// sortInstantValue sorts vector and matrix instant-query results into a
// deterministic order so that cmp.Diff doesn't report spurious differences
// due to ordering alone. Mirrors the sort.Sort(testResult.(model.Matrix)) call
// done for range-query results below. Scalars and strings need no sorting.
func sortInstantValue(v model.Value) {
	switch val := v.(type) {
	case model.Vector:
		sort.Sort(val)
	case model.Matrix:
		sort.Sort(val)
	}
}

// Compare runs a test case query against the reference API and the test API and compares the results.
//
// It runs both a range query (over [tc.Start, tc.End] at tc.Resolution) and an
// instant query (evaluated at tc.End, the same timestamp the range query ends
// on) against both APIs, and diffs each independently using the same
// tolerance/label-drop config. Prometheus's instant-query and range-query
// evaluation paths are not just two views of the same code -- ASAPQuery in
// particular has had multiple bugs where the two diverge -- so exercising only
// one (as upstream does) misses an entire class of bugs.
func (c *Comparer) Compare(tc *TestCase) (*Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r := v1.Range{
		Start: tc.Start,
		End:   tc.End,
		Step:  tc.Resolution,
	}

	// TODO: Handle warnings (second, ignored return value).
	refResult, _, refErr := c.refAPI.QueryRange(ctx, tc.Query, r)
	testResult, _, testErr := c.testAPI.QueryRange(ctx, tc.Query, r)
	refInstantResult, _, refInstantErr := c.refAPI.Query(ctx, tc.Query, tc.End)
	testInstantResult, _, testInstantErr := c.testAPI.Query(ctx, tc.Query, tc.End)

	if (refErr != nil) != tc.ShouldFail {
		if refErr != nil {
			return nil, fmt.Errorf("error querying reference API for %q: %w", tc.Query, refErr)
		}
		return nil, fmt.Errorf("expected reference API query %q to fail, but succeeded", tc.Query)
	}
	if (refInstantErr != nil) != tc.ShouldFail {
		if refInstantErr != nil {
			return nil, fmt.Errorf("error querying reference API (instant) for %q: %w", tc.Query, refInstantErr)
		}
		return nil, fmt.Errorf("expected reference API instant query %q to fail, but succeeded", tc.Query)
	}

	res := &Result{TestCase: tc}
	rangeErrMismatch := (testErr != nil) != tc.ShouldFail
	instantErrMismatch := (testInstantErr != nil) != tc.ShouldFail
	if rangeErrMismatch {
		if testErr != nil {
			res.UnexpectedFailure = testErr.Error()
			res.Unsupported = strings.Contains(testErr.Error(), "501")
		} else {
			res.UnexpectedSuccess = true
		}
	}
	if instantErrMismatch {
		if testInstantErr != nil {
			res.InstantUnexpectedFailure = testInstantErr.Error()
			res.InstantUnsupported = strings.Contains(testInstantErr.Error(), "501")
		} else {
			res.InstantUnexpectedSuccess = true
		}
	}
	if rangeErrMismatch || instantErrMismatch {
		return res, nil
	}

	if tc.SkipComparison || tc.ShouldFail {
		return res, nil
	}

	sort.Sort(testResult.(model.Matrix))
	sortInstantValue(refInstantResult)
	sortInstantValue(testInstantResult)

	for _, qt := range c.queryTweaks {
		if qt.IgnoreFirstStep {
			for _, r := range refResult.(model.Matrix) {
				if len(r.Values) > 0 && r.Values[0].Timestamp.Time().Sub(tc.Start) <= 2*time.Millisecond {
					r.Values = r.Values[1:]
				}
			}
		}
	}

	res.Diff = cmp.Diff(refResult, testResult, c.compareOptions)
	res.InstantDiff = cmp.Diff(refInstantResult, testInstantResult, c.compareOptions)

	return res, nil
}

func addFloatCompareOptions(queryTweaks []*config.QueryTweak, options *cmp.Options) {
	fraction := defaultFraction
	margin := defaultMargin
	for _, rt := range queryTweaks {
		if rt.AdjustValueTolerance != nil {
			if rt.AdjustValueTolerance.Fraction != nil {
				fraction = *rt.AdjustValueTolerance.Fraction
			}
			if rt.AdjustValueTolerance.Margin != nil {
				margin = *rt.AdjustValueTolerance.Margin
			}
		}
	}
	*options = append(
		*options,
		// Translate sample values into float64 so that cmpopts.EquateApprox() works.
		cmp.Transformer("TranslateFloat64", func(in model.SampleValue) float64 {
			return float64(in)
		}),
		cmpopts.EquateApprox(fraction, margin),
		// A NaN is usually not treated as equal to another NaN, but we want to treat it as such here.
		cmpopts.EquateNaNs(),
	)
}

func addDropResultLabelsOptions(queryTweaks []*config.QueryTweak, options *cmp.Options) {
	for _, rt := range queryTweaks {
		if len(rt.DropResultLabels) != 0 {
			localRt := rt
			*options = append(
				*options,
				cmp.Transformer(
					"DropResultLabels",
					func(in model.Metric) model.Metric {
						m := in.Clone()
						for _, ln := range localRt.DropResultLabels {
							delete(m, ln)
						}
						return m
					},
				),
			)
		}
	}
}

func addCaseInsensitiveCompareOptions(queryTweaks []*config.QueryTweak, options *cmp.Options) {
	for _, rt := range queryTweaks {
		if rt.IgnoreCase {
			*options = append(
				*options,
				// Translate metric names and labels into lowercase.
				cmp.Transformer("TranslateToLowerCase",
					func(in model.Metric) model.Metric {
						m := map[model.LabelName]model.LabelValue{}
						for key, val := range in {
							m[model.LabelName(strings.ToLower(string(key)))] = model.LabelValue(strings.ToLower(string(val)))
						}
						return m
					},
				),
			)
		}
	}
}
