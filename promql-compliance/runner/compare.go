package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	clientv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// QueryAPI is the small seam implemented by both Prometheus and ASAPQuery.
// The comparison engine does not know whether a target is local, remote, or
// running in Docker.
type QueryAPI interface {
	Query(context.Context, string, time.Time, ...clientv1.Option) (model.Value, clientv1.Warnings, error)
	QueryRange(context.Context, string, clientv1.Range, ...clientv1.Option) (model.Value, clientv1.Warnings, error)
}

type QueryReport struct {
	Name            string              `json:"name"`
	Expr            string              `json:"expr"`
	Comparison      ComparisonPolicy    `json:"comparison"`
	Range           *ComparisonOutcome  `json:"range,omitempty"`
	Instant         []InstantComparison `json:"instant,omitempty"`
	ReferenceParity []ParityComparison  `json:"referenceParity,omitempty"`
	TestParity      []ParityComparison  `json:"testParity,omitempty"`
	Passed          bool                `json:"passed"`
}

type ComparisonOutcome struct {
	Passed         bool   `json:"passed"`
	Diff           string `json:"diff,omitempty"`
	ReferenceError string `json:"referenceError,omitempty"`
	TestError      string `json:"testError,omitempty"`
}

type InstantComparison struct {
	OffsetSeconds float64           `json:"offsetSeconds"`
	Time          time.Time         `json:"time"`
	Comparison    ComparisonOutcome `json:"comparison"`
}

type ParityComparison struct {
	OffsetSeconds float64           `json:"offsetSeconds"`
	Time          time.Time         `json:"time"`
	Comparison    ComparisonOutcome `json:"comparison"`
}

// CompareQuery runs every configured query shape at every configured time.
// It compares the two targets and also checks range-at-t against
// instant-at-t within each target.
func CompareQuery(ctx context.Context, reference, test QueryAPI, query QueryCase, base time.Time, defaults ComparisonPolicy) (QueryReport, error) {
	effective := query.EffectiveComparison(defaults)
	report := QueryReport{
		Name:       query.Name,
		Expr:       query.Expr,
		Comparison: effective,
		Passed:     true,
	}

	var referenceRange, testRange model.Value
	var referenceRangeErr, testRangeErr error
	if query.Range != nil {
		rng, err := query.RangeAt(base)
		if err != nil {
			return QueryReport{}, err
		}
		referenceRange, _, referenceRangeErr = reference.QueryRange(ctx, query.Expr, rng)
		testRange, _, testRangeErr = test.QueryRange(ctx, query.Expr, rng)
		rangeOutcome := responseComparison(referenceRange, testRange, referenceRangeErr, testRangeErr, effective)
		report.Range = &rangeOutcome
		report.Passed = report.Passed && rangeOutcome.Passed
	}

	instantTimes := query.InstantTimes(base)
	for index, instantTime := range instantTimes {
		referenceInstant, _, referenceErr := reference.Query(ctx, query.Expr, instantTime)
		testInstant, _, testErr := test.Query(ctx, query.Expr, instantTime)
		outcome := instantResponseComparison(referenceInstant, testInstant, referenceErr, testErr, effective)
		report.Instant = append(report.Instant, InstantComparison{
			OffsetSeconds: query.InstantOffsetsSeconds[index],
			Time:          instantTime,
			Comparison:    outcome,
		})
		report.Passed = report.Passed && outcome.Passed

		if query.Range == nil || referenceRangeErr != nil || testRangeErr != nil || referenceErr != nil || testErr != nil {
			continue
		}
		referenceParity := parityComparison(referenceRange, referenceInstant, instantTime, effective)
		testParity := parityComparison(testRange, testInstant, instantTime, effective)
		report.ReferenceParity = append(report.ReferenceParity, ParityComparison{
			OffsetSeconds: query.InstantOffsetsSeconds[index],
			Time:          instantTime,
			Comparison:    referenceParity,
		})
		report.TestParity = append(report.TestParity, ParityComparison{
			OffsetSeconds: query.InstantOffsetsSeconds[index],
			Time:          instantTime,
			Comparison:    testParity,
		})
		report.Passed = report.Passed && referenceParity.Passed && testParity.Passed
	}

	return report, nil
}

func (q QueryCase) RangeAt(base time.Time) (clientv1.Range, error) {
	if q.Range == nil {
		return clientv1.Range{}, fmt.Errorf("query %q has no range", q.Name)
	}
	if err := q.Range.validate(); err != nil {
		return clientv1.Range{}, err
	}
	return clientv1.Range{
		Start: addSeconds(base, q.Range.StartOffsetSeconds),
		End:   addSeconds(base, q.Range.EndOffsetSeconds),
		Step:  time.Duration(q.Range.StepSeconds * float64(time.Second)),
	}, nil
}

func responseComparison(reference, test model.Value, referenceErr, testErr error, tolerance ComparisonPolicy) ComparisonOutcome {
	return responseComparisonWith(reference, test, referenceErr, testErr, func(reference, test model.Value) string {
		return compareValues(reference, test, tolerance)
	})
}

func instantResponseComparison(reference, test model.Value, referenceErr, testErr error, policy ComparisonPolicy) ComparisonOutcome {
	return responseComparisonWith(reference, test, referenceErr, testErr, func(reference, test model.Value) string {
		return compareInstantValues(reference, test, policy)
	})
}

func responseComparisonWith(reference, test model.Value, referenceErr, testErr error, compare func(model.Value, model.Value) string) ComparisonOutcome {
	outcome := ComparisonOutcome{}
	if referenceErr != nil {
		outcome.ReferenceError = referenceErr.Error()
	}
	if testErr != nil {
		outcome.TestError = testErr.Error()
	}
	if referenceErr != nil || testErr != nil {
		outcome.Passed = false
		return outcome
	}
	outcome.Diff = compare(reference, test)
	outcome.Passed = outcome.Diff == ""
	return outcome
}

func parityComparison(rangeValue, instantValue model.Value, timestamp time.Time, tolerance ComparisonPolicy) ComparisonOutcome {
	rangeSnapshot, err := normalizeAt(rangeValue, timestamp)
	if err != nil {
		return ComparisonOutcome{Diff: err.Error()}
	}
	instantSnapshot, err := normalizeAt(instantValue, timestamp)
	if err != nil {
		return ComparisonOutcome{Diff: err.Error()}
	}
	diff := compareNormalized(rangeSnapshot, instantSnapshot, tolerance)
	return ComparisonOutcome{Passed: diff == "", Diff: diff}
}

func compareValues(reference, test model.Value, tolerance ComparisonPolicy) string {
	referenceNormalized, err := normalize(reference)
	if err != nil {
		return fmt.Sprintf("reference result cannot be compared: %v", err)
	}
	testNormalized, err := normalize(test)
	if err != nil {
		return fmt.Sprintf("test result cannot be compared: %v", err)
	}
	return compareNormalized(referenceNormalized, testNormalized, tolerance)
}

func compareInstantValues(reference, test model.Value, policy ComparisonPolicy) string {
	if policy.InstantVectorOrder == nil {
		return compareValues(reference, test, policy)
	}
	referenceVector, referenceIsVector := reference.(model.Vector)
	testVector, testIsVector := test.(model.Vector)
	if !referenceIsVector || !testIsVector {
		return fmt.Sprintf(
			"instant vector order policy requires vector results, got reference %T and test %T",
			reference,
			test,
		)
	}

	referenceGroups, err := orderedVectorGroups(referenceVector, policy.InstantVectorOrder.Grouping)
	if err != nil {
		return orderedVectorDiff("reference instant vector grouping is invalid: "+err.Error(), referenceVector, testVector)
	}
	testGroups, err := orderedVectorGroups(testVector, policy.InstantVectorOrder.Grouping)
	if err != nil {
		return orderedVectorDiff("test instant vector grouping is invalid: "+err.Error(), referenceVector, testVector)
	}
	if len(referenceGroups) != len(testGroups) {
		return orderedVectorDiff(fmt.Sprintf("instant vector ordered group count differs: reference %d, test %d", len(referenceGroups), len(testGroups)), referenceVector, testVector)
	}
	for key, referenceGroup := range referenceGroups {
		testGroup, found := testGroups[key]
		if !found {
			return orderedVectorDiff(fmt.Sprintf("instant vector ordered group %q is absent from test result", key), referenceVector, testVector)
		}
		if err := validateOrderedValues(testGroup, policy.InstantVectorOrder.Direction); err != nil {
			return orderedVectorDiff(fmt.Sprintf("test instant vector ordered group %q: %v", key, err), referenceVector, testVector)
		}
		if len(referenceGroup) != len(testGroup) {
			return orderedVectorDiff(fmt.Sprintf("instant vector ordered group %q sample count differs: reference %d, test %d", key, len(referenceGroup), len(testGroup)), referenceVector, testVector)
		}
		if diff := compareSampleMembership(referenceGroup, testGroup, policy.ValueTolerance); diff != "" {
			return orderedVectorDiff(fmt.Sprintf("instant vector ordered group %q %s", key, diff), referenceVector, testVector)
		}
	}
	return ""
}

func compareSampleMembership(reference, test []normalizedSample, tolerance *Tolerance) string {
	referenceSamples := append([]normalizedSample(nil), reference...)
	testSamples := append([]normalizedSample(nil), test...)
	sortNormalizedSamples(referenceSamples)
	sortNormalizedSamples(testSamples)
	return compareNormalized(
		normalizedValue{Type: "vector", Samples: referenceSamples},
		normalizedValue{Type: "vector", Samples: testSamples},
		ComparisonPolicy{ValueTolerance: tolerance},
	)
}

func orderedVectorDiff(reason string, reference, test model.Vector) string {
	return fmt.Sprintf("%s\nreference: %v\ntest: %v", reason, reference, test)
}

func orderedVectorGroups(vector model.Vector, grouping *OrderGrouping) (map[string][]normalizedSample, error) {
	groups := make(map[string][]normalizedSample)
	var currentKey string
	hasCurrentGroup := false
	for _, sample := range vector {
		key := orderGroupKey(sample.Metric, grouping)
		if !hasCurrentGroup || key != currentKey {
			if _, seen := groups[key]; seen {
				return nil, fmt.Errorf("group %q is not contiguous", key)
			}
			groups[key] = []normalizedSample{}
			currentKey = key
			hasCurrentGroup = true
		}
		groups[key] = append(groups[key], normalizedSample{Metric: metricString(sample.Metric), Timestamp: int64(sample.Timestamp), Value: float64(sample.Value)})
	}
	return groups, nil
}

func validateOrderedValues(samples []normalizedSample, direction string) error {
	for index := 1; index < len(samples); index++ {
		previous, current := samples[index-1], samples[index]
		if direction == instantOrderDescending && previous.Value < current.Value {
			return fmt.Errorf("values are not descending at sample %d", index)
		}
		if direction == instantOrderAscending && previous.Value > current.Value {
			return fmt.Errorf("values are not ascending at sample %d", index)
		}
	}
	return nil
}

func orderGroupKey(metric model.Metric, grouping *OrderGrouping) string {
	if grouping == nil {
		return ""
	}
	labels := make(model.Metric)
	if grouping.Mode == orderGroupingBy {
		for _, label := range grouping.Labels {
			labelName := model.LabelName(label)
			labels[labelName] = metric[labelName]
		}
		return metricString(labels)
	}
	excluded := make(map[model.LabelName]struct{}, len(grouping.Labels))
	excluded[model.MetricNameLabel] = struct{}{}
	for _, label := range grouping.Labels {
		excluded[model.LabelName(label)] = struct{}{}
	}
	for label, value := range metric {
		if _, skip := excluded[label]; !skip {
			labels[label] = value
		}
	}
	return metricString(labels)
}

type normalizedValue struct {
	Type    string             `json:"type"`
	Samples []normalizedSample `json:"samples,omitempty"`
	Scalar  *float64           `json:"scalar,omitempty"`
	String  *string            `json:"string,omitempty"`
}

type normalizedSample struct {
	Metric    string  `json:"metric"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

func normalize(value model.Value) (normalizedValue, error) {
	switch value := value.(type) {
	case model.Vector:
		return normalizedVector(value), nil
	case model.Matrix:
		return normalizedMatrix(value), nil
	case *model.Scalar:
		return normalizedValue{Type: "scalar", Scalar: floatPtr(float64(value.Value))}, nil
	case *model.String:
		return normalizedValue{Type: "string", String: stringPtr(value.Value)}, nil
	default:
		return normalizedValue{}, fmt.Errorf("unsupported Prometheus result type %T", value)
	}
}

func normalizeAt(value model.Value, timestamp time.Time) (normalizedValue, error) {
	requested := timestamp.UnixMilli()
	switch value := value.(type) {
	case model.Matrix:
		samples := make([]normalizedSample, 0)
		for _, stream := range value {
			for _, sample := range stream.Values {
				if int64(sample.Timestamp) == requested {
					samples = append(samples, normalizedSample{Metric: metricString(stream.Metric), Timestamp: requested, Value: float64(sample.Value)})
				}
			}
		}
		result := normalizedValue{Type: "vector", Samples: samples}
		sortNormalizedSamples(result.Samples)
		return result, nil
	case model.Vector:
		result := normalizedVector(value)
		for index := range result.Samples {
			result.Samples[index].Timestamp = requested
		}
		return result, nil
	default:
		return normalize(value)
	}
}

func normalizedVector(value model.Vector) normalizedValue {
	samples := make([]normalizedSample, 0, len(value))
	for _, sample := range value {
		samples = append(samples, normalizedSample{Metric: metricString(sample.Metric), Timestamp: int64(sample.Timestamp), Value: float64(sample.Value)})
	}
	sortNormalizedSamples(samples)
	return normalizedValue{Type: "vector", Samples: samples}
}

func normalizedMatrix(value model.Matrix) normalizedValue {
	samples := make([]normalizedSample, 0)
	for _, stream := range value {
		for _, sample := range stream.Values {
			samples = append(samples, normalizedSample{Metric: metricString(stream.Metric), Timestamp: int64(sample.Timestamp), Value: float64(sample.Value)})
		}
	}
	sortNormalizedSamples(samples)
	return normalizedValue{Type: "matrix", Samples: samples}
}

func compareNormalized(reference, test normalizedValue, tolerance ComparisonPolicy) string {
	if reference.Type != test.Type {
		return describeDiff(reference, test, "result type differs")
	}
	if reference.Scalar != nil || test.Scalar != nil {
		if reference.Scalar == nil || test.Scalar == nil || !equalFloat(*reference.Scalar, *test.Scalar, tolerance.ValueTolerance) {
			return describeDiff(reference, test, "scalar differs")
		}
		return ""
	}
	if reference.String != nil || test.String != nil {
		if reference.String == nil || test.String == nil || *reference.String != *test.String {
			return describeDiff(reference, test, "string differs")
		}
		return ""
	}
	if len(reference.Samples) != len(test.Samples) {
		return describeDiff(reference, test, "sample count differs")
	}
	for index := range reference.Samples {
		left, right := reference.Samples[index], test.Samples[index]
		if left.Metric != right.Metric || left.Timestamp != right.Timestamp {
			return describeDiff(reference, test, "metric or timestamp differs")
		}
		if !equalFloat(left.Value, right.Value, tolerance.ValueTolerance) {
			return describeDiff(reference, test, "sample value differs")
		}
	}
	return ""
}

func equalFloat(left, right float64, tolerance *Tolerance) bool {
	if math.IsNaN(left) || math.IsNaN(right) {
		return math.IsNaN(left) && math.IsNaN(right)
	}
	if math.IsInf(left, 0) || math.IsInf(right, 0) {
		return left == right
	}
	relative, absolute := 0.0, 0.0
	if tolerance != nil {
		if tolerance.Relative != nil {
			relative = *tolerance.Relative
		}
		if tolerance.Absolute != nil {
			absolute = *tolerance.Absolute
		}
	}
	limit := absolute + relative*math.Max(math.Abs(left), math.Abs(right))
	return math.Abs(left-right) <= limit
}

func sortNormalizedSamples(samples []normalizedSample) {
	sort.Slice(samples, func(left, right int) bool {
		if samples[left].Metric != samples[right].Metric {
			return samples[left].Metric < samples[right].Metric
		}
		return samples[left].Timestamp < samples[right].Timestamp
	})
}

func metricString(metric model.Metric) string {
	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+fmt.Sprintf("%q", metric[model.LabelName(key)]))
	}
	return strings.Join(parts, ",")
}

func describeDiff(reference, test normalizedValue, reason string) string {
	left, _ := json.Marshal(reference)
	right, _ := json.Marshal(test)
	return fmt.Sprintf("%s\nreference: %s\ntest: %s", reason, left, right)
}

func floatPtr(value float64) *float64 { return &value }
func stringPtr(value string) *string  { return &value }
