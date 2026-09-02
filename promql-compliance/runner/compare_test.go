package runner

import (
	"context"
	"testing"
	"time"

	clientv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type fakeTarget struct {
	rangeValue  model.Value
	instantByMS map[int64]model.Value
}

func (f fakeTarget) Query(_ context.Context, _ string, ts time.Time, _ ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return f.instantByMS[ts.UnixMilli()], nil, nil
}

func (f fakeTarget) QueryRange(_ context.Context, _ string, _ clientv1.Range, _ ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return f.rangeValue, nil, nil
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
