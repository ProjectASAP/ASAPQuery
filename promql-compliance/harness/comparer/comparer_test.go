package comparer

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// fakeAPI is an in-process PromAPI double that lets tests script distinct
// instant (Query) and range (QueryRange) responses, without needing a real
// HTTP server. It exists specifically to exercise the instant-query diffing
// path added to Comparer.Compare, which upstream promql-compliance-tester
// never invoked (it only ever called QueryRange).
type fakeAPI struct {
	queryValue model.Value
	queryErr   error

	rangeValue model.Value
	rangeErr   error
}

func (f *fakeAPI) Query(_ context.Context, _ string, _ time.Time, _ ...v1.Option) (model.Value, v1.Warnings, error) {
	return f.queryValue, nil, f.queryErr
}

func (f *fakeAPI) QueryRange(_ context.Context, _ string, _ v1.Range, _ ...v1.Option) (model.Value, v1.Warnings, error) {
	return f.rangeValue, nil, f.rangeErr
}

func vectorOf(value float64, labels model.Metric) model.Vector {
	return model.Vector{
		&model.Sample{
			Metric:    labels,
			Value:     model.SampleValue(value),
			Timestamp: model.Time(0),
		},
	}
}

func matrixOf(value float64, labels model.Metric) model.Matrix {
	return model.Matrix{
		&model.SampleStream{
			Metric: labels,
			Values: []model.SamplePair{{Timestamp: model.Time(0), Value: model.SampleValue(value)}},
		},
	}
}

func newTestCase(query string) *TestCase {
	end := time.Unix(1000, 0).UTC()
	return &TestCase{
		Query:      query,
		Start:      end.Add(-time.Minute),
		End:        end,
		Resolution: 15 * time.Second,
	}
}

func TestCompare_BothMatch_Succeeds(t *testing.T) {
	labels := model.Metric{"__name__": "up"}
	ref := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(1, labels)}
	test := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(1, labels)}

	c := New(ref, test, nil)
	res, err := c.Compare(newTestCase("up"))
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}
	if !res.RangeSuccess() {
		t.Errorf("expected RangeSuccess() to be true, diff: %q", res.Diff)
	}
	if !res.InstantSuccess() {
		t.Errorf("expected InstantSuccess() to be true, diff: %q", res.InstantDiff)
	}
	if !res.Success() {
		t.Errorf("expected Success() to be true")
	}
}

// TestCompare_InstantDivergesRangeMatches reproduces the exact shape of
// ASAPQuery's known bug class (see issue #589): a query whose range-query
// evaluation matches the reference target but whose instant-query evaluation
// does not. Comparer.Compare must surface this as "PASS: range, FAIL:
// instant" rather than as one aggregate pass, which is only possible because
// range and instant are diffed independently.
func TestCompare_InstantDivergesRangeMatches(t *testing.T) {
	labels := model.Metric{"__name__": "up"}
	ref := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(1, labels)}
	test := &fakeAPI{queryValue: vectorOf(2, labels), rangeValue: matrixOf(1, labels)}

	c := New(ref, test, nil)
	res, err := c.Compare(newTestCase("up"))
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}
	if !res.RangeSuccess() {
		t.Errorf("expected RangeSuccess() to be true, diff: %q", res.Diff)
	}
	if res.InstantSuccess() {
		t.Errorf("expected InstantSuccess() to be false")
	}
	if res.InstantDiff == "" {
		t.Errorf("expected a non-empty InstantDiff")
	}
	if res.Success() {
		t.Errorf("expected Success() to be false when only the instant result diverges")
	}
}

// TestCompare_RangeDivergesInstantMatches is the mirror image of the above:
// range diverges but instant matches.
func TestCompare_RangeDivergesInstantMatches(t *testing.T) {
	labels := model.Metric{"__name__": "up"}
	ref := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(1, labels)}
	test := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(2, labels)}

	c := New(ref, test, nil)
	res, err := c.Compare(newTestCase("up"))
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}
	if res.RangeSuccess() {
		t.Errorf("expected RangeSuccess() to be false")
	}
	if !res.InstantSuccess() {
		t.Errorf("expected InstantSuccess() to be true, diff: %q", res.InstantDiff)
	}
	if res.Success() {
		t.Errorf("expected Success() to be false when only the range result diverges")
	}
}

func TestCompare_InstantUnexpectedFailure(t *testing.T) {
	labels := model.Metric{"__name__": "up"}
	ref := &fakeAPI{queryValue: vectorOf(1, labels), rangeValue: matrixOf(1, labels)}
	test := &fakeAPI{queryErr: errors.New("501 Not Implemented"), rangeValue: matrixOf(1, labels)}

	c := New(ref, test, nil)
	res, err := c.Compare(newTestCase("up"))
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}
	if res.InstantUnexpectedFailure == "" {
		t.Errorf("expected InstantUnexpectedFailure to be set")
	}
	if !res.InstantUnsupported {
		t.Errorf("expected InstantUnsupported to be true for a 501 error")
	}
	if res.Success() {
		t.Errorf("expected Success() to be false")
	}
}

func TestCompare_ShouldFail_BothAPIsFail_Skips(t *testing.T) {
	ref := &fakeAPI{queryErr: errors.New("boom"), rangeErr: errors.New("boom")}
	test := &fakeAPI{queryErr: errors.New("boom"), rangeErr: errors.New("boom")}

	c := New(ref, test, nil)
	tc := newTestCase("invalid_query(")
	tc.ShouldFail = true
	res, err := c.Compare(tc)
	if err != nil {
		t.Fatalf("Compare returned error: %v", err)
	}
	if !res.Success() {
		t.Errorf("expected Success() to be true when both APIs failed as expected")
	}
}
