package runner

import (
	"testing"
	"time"
)

func TestLoadSuiteResolvesExplicitPerQueryTimesAndTolerance(t *testing.T) {
	suite, err := LoadSuite([]byte(`name: temporal
comparison_defaults:
  value_tolerance:
    relative: 0.01
queries:
  - name: gap
    expr: checkout_up
    instant_offsets_seconds: [300, 660, 900]
    range:
      start_offset_seconds: 0
      end_offset_seconds: 1200
      step_seconds: 60
  - name: exact
    expr: sum(up)
    instant_offsets_seconds: [600]
    comparison:
      value_tolerance:
        absolute: 0.000001
`))
	if err != nil {
		t.Fatalf("LoadSuite: %v", err)
	}

	base := time.UnixMilli(1_700_000_000_000).UTC()
	first := suite.Queries[0]
	times := first.InstantTimes(base)
	if got, want := len(times), 3; got != want {
		t.Fatalf("instant time count = %d, want %d", got, want)
	}
	if got, want := times[1], base.Add(660*time.Second); !got.Equal(want) {
		t.Fatalf("gap time = %s, want %s", got, want)
	}

	rng, err := first.RangeAt(base)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if got, want := rng.Start, base; !got.Equal(want) {
		t.Fatalf("range start = %s, want %s", got, want)
	}
	if got, want := rng.End, base.Add(1200*time.Second); !got.Equal(want) {
		t.Fatalf("range end = %s, want %s", got, want)
	}

	if got := first.EffectiveTolerance(suite.ComparisonDefaults); got.ValueTolerance == nil || got.ValueTolerance.Relative == nil || *got.ValueTolerance.Relative != 0.01 {
		t.Fatalf("global tolerance was not inherited: %#v", got)
	}
	if got := suite.Queries[1].EffectiveTolerance(suite.ComparisonDefaults); got.ValueTolerance == nil || got.ValueTolerance.Absolute == nil || *got.ValueTolerance.Absolute != 0.000001 {
		t.Fatalf("per-query tolerance was not applied: %#v", got)
	}
	if got := suite.Queries[1].EffectiveTolerance(suite.ComparisonDefaults); got.ValueTolerance == nil || got.ValueTolerance.Relative == nil || *got.ValueTolerance.Relative != 0.01 {
		t.Fatalf("per-query tolerance did not inherit global relative value: %#v", got)
	}
}

func TestLoadSuiteRejectsImplicitEvaluationWindow(t *testing.T) {
	_, err := LoadSuite([]byte(`name: incomplete
queries:
  - name: missing-times
    expr: up
`))
	if err == nil {
		t.Fatal("LoadSuite accepted a query with no instant times or range")
	}
}

func TestLoadSuiteRejectsInstantOutsideRange(t *testing.T) {
	_, err := LoadSuite([]byte(`name: outside
queries:
  - name: invalid-time
    expr: up
    instant_offsets_seconds: [120]
    range:
      start_offset_seconds: 0
      end_offset_seconds: 60
      step_seconds: 60
`))
	if err == nil {
		t.Fatal("LoadSuite accepted an instant time outside its range")
	}
}

func TestLoadSuiteRejectsInstantOffsetOffRangeGrid(t *testing.T) {
	_, err := LoadSuite([]byte(`name: off-grid
queries:
  - name: invalid-time
    expr: up
    instant_offsets_seconds: [330]
    range:
      start_offset_seconds: 300
      end_offset_seconds: 600
      step_seconds: 60
`))
	if err == nil {
		t.Fatal("LoadSuite accepted an instant offset that is not on the range grid")
	}
}

func TestLoadSuiteRejectsSubMillisecondRange(t *testing.T) {
	_, err := LoadSuite([]byte(`name: sub-millisecond
queries:
  - name: too-small
    expr: up
    range:
      start_offset_seconds: 0
      end_offset_seconds: 0.0005
      step_seconds: 0.0005
`))
	if err == nil {
		t.Fatal("LoadSuite accepted a sub-millisecond range")
	}
}
