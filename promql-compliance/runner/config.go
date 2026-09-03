package runner

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Suite is a data-independent collection of PromQL cases. All timestamps are
// offsets from the dataset base time selected for a run.
type Suite struct {
	Name               string           `yaml:"name" json:"name"`
	ComparisonDefaults ComparisonPolicy `yaml:"comparison_defaults" json:"comparisonDefaults"`
	Queries            []QueryCase      `yaml:"queries" json:"queries"`
}

type QueryCase struct {
	Name                  string            `yaml:"name" json:"name"`
	Expr                  string            `yaml:"expr" json:"expr"`
	InstantOffsetsSeconds []float64         `yaml:"instant_offsets_seconds" json:"instantOffsetsSeconds"`
	Range                 *RangeSpec        `yaml:"range" json:"range"`
	Comparison            *ComparisonPolicy `yaml:"comparison" json:"comparison"`
	ExpectError           bool              `yaml:"expect_error" json:"expectError"`
}

type RangeSpec struct {
	StartOffsetSeconds float64 `yaml:"start_offset_seconds" json:"startOffsetSeconds"`
	EndOffsetSeconds   float64 `yaml:"end_offset_seconds" json:"endOffsetSeconds"`
	StepSeconds        float64 `yaml:"step_seconds" json:"stepSeconds"`
}

// ComparisonPolicy is intentionally pointer-valued: omitted means exact
// comparison, while zero is a valid explicit tolerance.
type ComparisonPolicy struct {
	ValueTolerance *Tolerance `yaml:"value_tolerance" json:"valueTolerance"`
}

type Tolerance struct {
	Relative *float64 `yaml:"relative" json:"relative"`
	Absolute *float64 `yaml:"absolute" json:"absolute"`
}

// LoadSuite parses and validates a query suite. It rejects incomplete cases
// rather than silently selecting wall-clock defaults.
func LoadSuite(contents []byte) (Suite, error) {
	var suite Suite
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&suite); err != nil {
		return Suite{}, fmt.Errorf("parse query suite: %w", err)
	}
	if suite.Name == "" {
		return Suite{}, fmt.Errorf("query suite has no name")
	}
	if len(suite.Queries) == 0 {
		return Suite{}, fmt.Errorf("query suite %q has no queries", suite.Name)
	}
	for i := range suite.Queries {
		query := &suite.Queries[i]
		if query.Name == "" {
			return Suite{}, fmt.Errorf("query %d has no name", i)
		}
		if query.Expr == "" {
			return Suite{}, fmt.Errorf("query %q has no expr", query.Name)
		}
		if len(query.InstantOffsetsSeconds) == 0 && query.Range == nil {
			return Suite{}, fmt.Errorf("query %q has neither instant times nor a range", query.Name)
		}
		if query.Range != nil {
			if err := query.Range.validate(); err != nil {
				return Suite{}, fmt.Errorf("query %q: %w", query.Name, err)
			}
			for _, offset := range query.InstantOffsetsSeconds {
				if !finite(offset) || !query.Range.contains(offset) {
					return Suite{}, fmt.Errorf("query %q instant offset %v is outside its range", query.Name, offset)
				}
			}
		} else {
			for _, offset := range query.InstantOffsetsSeconds {
				if !finite(offset) {
					return Suite{}, fmt.Errorf("query %q has a non-finite instant offset", query.Name)
				}
			}
		}
		if err := validateTolerance(query.EffectiveTolerance(suite.ComparisonDefaults).ValueTolerance); err != nil {
			return Suite{}, fmt.Errorf("query %q: %w", query.Name, err)
		}
	}
	return suite, nil
}

// LoadSuiteFile reads a query suite from disk.
func LoadSuiteFile(path string) (Suite, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Suite{}, fmt.Errorf("read query suite %q: %w", path, err)
	}
	return LoadSuite(contents)
}

func (q QueryCase) InstantTimes(base time.Time) []time.Time {
	result := make([]time.Time, 0, len(q.InstantOffsetsSeconds))
	for _, offset := range q.InstantOffsetsSeconds {
		result = append(result, addSeconds(base, offset))
	}
	return result
}

func (q QueryCase) EffectiveTolerance(defaults ComparisonPolicy) ComparisonPolicy {
	if q.Comparison == nil || q.Comparison.ValueTolerance == nil {
		return defaults
	}
	result := defaults
	if result.ValueTolerance == nil {
		result.ValueTolerance = &Tolerance{}
	}
	merged := *result.ValueTolerance
	if q.Comparison.ValueTolerance.Relative != nil {
		merged.Relative = q.Comparison.ValueTolerance.Relative
	}
	if q.Comparison.ValueTolerance.Absolute != nil {
		merged.Absolute = q.Comparison.ValueTolerance.Absolute
	}
	result.ValueTolerance = &merged
	return result
}

func (r RangeSpec) validate() error {
	if !finite(r.StartOffsetSeconds) || !finite(r.EndOffsetSeconds) || !finite(r.StepSeconds) {
		return fmt.Errorf("range offsets and step must be finite")
	}
	if r.EndOffsetSeconds <= r.StartOffsetSeconds {
		return fmt.Errorf("range end must be after range start")
	}
	if r.EndOffsetSeconds-r.StartOffsetSeconds < float64(time.Millisecond)/float64(time.Second) {
		return fmt.Errorf("range duration must be at least 1ms")
	}
	if r.StepSeconds <= 0 {
		return fmt.Errorf("range step must be positive")
	}
	return nil
}

func (r RangeSpec) contains(offset float64) bool {
	return offset >= r.StartOffsetSeconds && offset <= r.EndOffsetSeconds
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validateTolerance(tolerance *Tolerance) error {
	if tolerance == nil {
		return nil
	}
	if tolerance.Relative != nil && (*tolerance.Relative < 0 || math.IsNaN(*tolerance.Relative) || math.IsInf(*tolerance.Relative, 0)) {
		return fmt.Errorf("relative tolerance must be a finite non-negative number")
	}
	if tolerance.Absolute != nil && (*tolerance.Absolute < 0 || math.IsNaN(*tolerance.Absolute) || math.IsInf(*tolerance.Absolute, 0)) {
		return fmt.Errorf("absolute tolerance must be a finite non-negative number")
	}
	return nil
}

func addSeconds(base time.Time, seconds float64) time.Time {
	return base.Add(time.Duration(seconds * float64(time.Second)))
}
