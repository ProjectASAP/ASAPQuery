package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func TestCompareQueryRejectsUnexpectedSharedErrors(t *testing.T) {
	err := errors.New("query failed")
	outcome := responseComparison(nil, nil, err, err, ComparisonPolicy{})

	if outcome.Passed {
		t.Fatal("comparison passed even though both targets failed unexpectedly")
	}
	if outcome.ReferenceError != err.Error() || outcome.TestError != err.Error() {
		t.Fatalf("errors = %#v, want both target errors recorded", outcome)
	}
}

type fakeTarget struct {
	rangeValue  model.Value
	instantByMS map[int64]model.Value
}

type errorTarget struct{ err error }

func (target errorTarget) Query(context.Context, string, time.Time, ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return nil, nil, target.err
}

func (target errorTarget) QueryRange(context.Context, string, clientv1.Range, ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return nil, nil, target.err
}

func (f fakeTarget) Query(_ context.Context, _ string, ts time.Time, _ ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return f.instantByMS[ts.UnixMilli()], nil, nil
}

func (f fakeTarget) QueryRange(_ context.Context, _ string, _ clientv1.Range, _ ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return f.rangeValue, nil, nil
}

func TestCompareQueryDoesNotPassWhenBothTargetsFailUnexpectedly(t *testing.T) {
	err := errors.New("query failed")
	query := QueryCase{
		Name:                  "failing-query",
		Expr:                  "rate(up[5m])",
		InstantOffsetsSeconds: []float64{0},
	}

	report, compareErr := CompareQuery(
		context.Background(), errorTarget{err: err}, errorTarget{err: err}, query,
		time.Unix(1_700_000_000, 0).UTC(), ComparisonPolicy{},
	)
	if compareErr != nil {
		t.Fatalf("CompareQuery: %v", compareErr)
	}
	if report.Passed {
		t.Fatalf("query passed despite both targets failing: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryChecksEveryInstantTimeAndTargetParity(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	first := model.Time(base.UnixMilli())
	second := model.Time(base.Add(time.Minute).UnixMilli())

	rangeValue := model.Matrix{&model.SampleStream{
		Metric: model.Metric{"__name__": "up"},
		Values: []model.SamplePair{
			{Timestamp: first, Value: 1},
			{Timestamp: second, Value: 1},
		},
	}}
	ref := fakeTarget{
		rangeValue: rangeValue,
		instantByMS: map[int64]model.Value{
			base.UnixMilli():                  model.Vector{&model.Sample{Metric: model.Metric{"__name__": "up"}, Value: 1, Timestamp: first}},
			base.Add(time.Minute).UnixMilli(): model.Vector{&model.Sample{Metric: model.Metric{"__name__": "up"}, Value: 1, Timestamp: second}},
		},
	}
	testTarget := fakeTarget{
		rangeValue: rangeValue,
		instantByMS: map[int64]model.Value{
			base.UnixMilli():                  model.Vector{&model.Sample{Metric: model.Metric{"__name__": "up"}, Value: 1, Timestamp: first}},
			base.Add(time.Minute).UnixMilli(): model.Vector{&model.Sample{Metric: model.Metric{"__name__": "up"}, Value: 2, Timestamp: second}},
		},
	}
	query := QueryCase{
		Name:                  "up-at-both-steps",
		Expr:                  "up",
		InstantOffsetsSeconds: []float64{0, 60},
		Range:                 &RangeSpec{StartOffsetSeconds: 0, EndOffsetSeconds: 60, StepSeconds: 60},
	}

	report, err := CompareQuery(context.Background(), ref, testTarget, query, base, ComparisonPolicy{})
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if len(report.Instant) != 2 {
		t.Fatalf("instant comparisons = %d, want 2", len(report.Instant))
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("first instant comparison failed: %#v", report.Instant[0])
	}
	if report.Instant[1].Comparison.Passed {
		t.Fatal("second instant comparison passed despite target mismatch")
	}
	if report.TestParity[1].Comparison.Passed {
		t.Fatal("ASAPQuery range/instant parity passed despite second-step mismatch")
	}
	if !report.ReferenceParity[0].Comparison.Passed {
		t.Fatalf("Prometheus parity failed unexpectedly: %#v", report.ReferenceParity[0])
	}
}

func TestCompareValuesHonorsExplicitToleranceOnlyForValues(t *testing.T) {
	left := model.Vector{&model.Sample{
		Metric:    model.Metric{"__name__": "up"},
		Timestamp: model.Time(1000),
		Value:     100,
	}}
	right := model.Vector{&model.Sample{
		Metric:    model.Metric{"__name__": "up"},
		Timestamp: model.Time(1000),
		Value:     101,
	}}
	tolerance := ComparisonPolicy{ValueTolerance: &Tolerance{Relative: floatPtr(0.02)}}
	if diff := compareValues(left, right, tolerance); diff != "" {
		t.Fatalf("comparison rejected explicit tolerance: %s", diff)
	}

	differentLabels := model.Vector{&model.Sample{
		Metric:    model.Metric{"__name__": "other"},
		Timestamp: model.Time(1000),
		Value:     101,
	}}
	if diff := compareValues(left, differentLabels, tolerance); diff == "" {
		t.Fatal("comparison accepted different labels because values were within tolerance")
	}
}

func TestCompareQueryRejectsReorderedInstantVectorWhenOrderIsRequired(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	reference := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 2, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
	}
	test := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 2, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "ordered-topk",
		Expr:                  "topk(2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "descending",
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): reference}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): test}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if report.Instant[0].Comparison.Passed {
		t.Fatal("ordered instant comparison accepted reordered samples")
	}
}

func TestCompareQueryKeepsDefaultInstantVectorComparisonOrderInsensitive(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	reference := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 2, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
	}
	test := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 2, Timestamp: timestamp},
	}
	query := QueryCase{Name: "unordered-vector", Expr: "up", InstantOffsetsSeconds: []float64{0}}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): reference}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): test}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("default instant comparison rejected reordered samples: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryRequiresFullLabelOrderForEqualInstantValues(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	reference := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
	}
	test := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 1, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "tied-topk",
		Expr:                  "topk(2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "descending",
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): reference}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): test}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if report.Instant[0].Comparison.Passed {
		t.Fatal("ordered instant comparison accepted reversed equal-value labels")
	}
}

func TestCompareQueryAcceptsPrometheusTieOrderAsTheReference(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	prometheusOrder := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 1, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "tied-topk",
		Expr:                  "topk(2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: instantOrderDescending,
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): prometheusOrder}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): prometheusOrder}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("ordered instant comparison rejected Prometheus's tied sequence: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryAllowsGroupedInstantBucketsInAnyOrder(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	frontend := []*model.Sample{
		{Metric: model.Metric{"job": "frontend", "instance": "a"}, Value: 3, Timestamp: timestamp},
		{Metric: model.Metric{"job": "frontend", "instance": "b"}, Value: 2, Timestamp: timestamp},
	}
	backend := []*model.Sample{
		{Metric: model.Metric{"job": "backend", "instance": "a"}, Value: 4, Timestamp: timestamp},
		{Metric: model.Metric{"job": "backend", "instance": "b"}, Value: 1, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "grouped-topk",
		Expr:                  "topk by (job) (2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "descending",
			Grouping:  &OrderGrouping{Mode: "by", Labels: []string{"job"}},
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): model.Vector(append(frontend, backend...))}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): model.Vector(append(backend, frontend...))}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("grouped instant comparison rejected reordered buckets: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryAllowsWithoutGroupedInstantBucketsInAnyOrder(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	frontend := []*model.Sample{
		{Metric: model.Metric{"job": "frontend", "instance": "a"}, Value: 3, Timestamp: timestamp},
		{Metric: model.Metric{"job": "frontend", "instance": "b"}, Value: 2, Timestamp: timestamp},
	}
	backend := []*model.Sample{
		{Metric: model.Metric{"job": "backend", "instance": "a"}, Value: 4, Timestamp: timestamp},
		{Metric: model.Metric{"job": "backend", "instance": "b"}, Value: 1, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "without-grouped-topk",
		Expr:                  "topk without (instance) (2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "descending",
			Grouping:  &OrderGrouping{Mode: "without", Labels: []string{"instance"}},
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): model.Vector(append(frontend, backend...))}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): model.Vector(append(backend, frontend...))}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("without-grouped instant comparison rejected reordered buckets: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryAcceptsAscendingInstantVectorOrder(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	ordered := model.Vector{
		&model.Sample{Metric: model.Metric{"instance": "a"}, Value: 1, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"instance": "b"}, Value: 2, Timestamp: timestamp},
	}
	query := QueryCase{
		Name:                  "ordered-bottomk",
		Expr:                  "bottomk(2, up)",
		InstantOffsetsSeconds: []float64{0},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "ascending",
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): ordered}},
		fakeTarget{instantByMS: map[int64]model.Value{base.UnixMilli(): ordered}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if !report.Instant[0].Comparison.Passed {
		t.Fatalf("ascending instant comparison failed: %#v", report.Instant[0].Comparison)
	}
}

func TestCompareQueryKeepsRangeComparisonsOrderInsensitive(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	timestamp := model.Time(base.UnixMilli())
	first := &model.SampleStream{Metric: model.Metric{"instance": "a"}, Values: []model.SamplePair{{Timestamp: timestamp, Value: 2}}}
	second := &model.SampleStream{Metric: model.Metric{"instance": "b"}, Values: []model.SamplePair{{Timestamp: timestamp, Value: 1}}}
	query := QueryCase{
		Name:  "range-topk",
		Expr:  "topk(2, up)",
		Range: &RangeSpec{StartOffsetSeconds: 0, EndOffsetSeconds: 60, StepSeconds: 60},
		Comparison: &ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
			Direction: "descending",
		}},
	}

	report, err := CompareQuery(
		context.Background(),
		fakeTarget{rangeValue: model.Matrix{first, second}},
		fakeTarget{rangeValue: model.Matrix{second, first}},
		query, base, ComparisonPolicy{},
	)
	if err != nil {
		t.Fatalf("CompareQuery: %v", err)
	}
	if report.Range == nil || !report.Range.Passed {
		t.Fatalf("range comparison rejected reordered streams: %#v", report.Range)
	}
}

func TestInstantVectorOrderRejectsInvalidResponseShapesAndSequences(t *testing.T) {
	order := ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{Direction: instantOrderDescending}}
	if diff := compareInstantValues(&model.Scalar{Value: 1}, model.Vector{}, order); diff == "" {
		t.Fatal("ordered comparison accepted a non-vector response")
	}

	timestamp := model.Time(1)
	grouped := ComparisonPolicy{InstantVectorOrder: &InstantVectorOrder{
		Direction: instantOrderDescending,
		Grouping:  &OrderGrouping{Mode: orderGroupingBy, Labels: []string{"job"}},
	}}
	nonContiguous := model.Vector{
		&model.Sample{Metric: model.Metric{"job": "a"}, Value: 3, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"job": "b"}, Value: 2, Timestamp: timestamp},
		&model.Sample{Metric: model.Metric{"job": "a"}, Value: 1, Timestamp: timestamp},
	}
	if diff := compareInstantValues(nonContiguous, nonContiguous, grouped); diff == "" {
		t.Fatal("ordered comparison accepted non-contiguous grouping buckets")
	}
}
