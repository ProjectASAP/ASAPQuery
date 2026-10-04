package runner

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	instantOrderAscending  = "ascending"
	instantOrderDescending = "descending"
	orderGroupingBy        = "by"
	orderGroupingWithout   = "without"
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
}

type RangeSpec struct {
	StartOffsetSeconds float64 `yaml:"start_offset_seconds" json:"startOffsetSeconds"`
	EndOffsetSeconds   float64 `yaml:"end_offset_seconds" json:"endOffsetSeconds"`
	StepSeconds        float64 `yaml:"step_seconds" json:"stepSeconds"`
}

// ComparisonPolicy is intentionally pointer-valued: omitted means exact
// comparison, while zero is a valid explicit tolerance.
type ComparisonPolicy struct {
	ValueTolerance     *Tolerance          `yaml:"value_tolerance" json:"valueTolerance"`
	InstantVectorOrder *InstantVectorOrder `yaml:"instant_vector_order" json:"instantVectorOrder"`
}

// InstantVectorOrder opts an instant-vector comparison into PromQL ordering
// rules. Range responses deliberately remain order-insensitive.
type InstantVectorOrder struct {
	Direction string         `yaml:"direction" json:"direction"`
	Grouping  *OrderGrouping `yaml:"grouping" json:"grouping"`
}

type OrderGrouping struct {
	Mode   string   `yaml:"mode" json:"mode"`
	Labels []string `yaml:"labels" json:"labels"`
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
				if !query.Range.containsGridOffset(offset) {
					return Suite{}, fmt.Errorf(
						"query %q instant offset %v is not aligned to the range start and step",
						query.Name,
						offset,
					)
				}
			}
		} else {
			for _, offset := range query.InstantOffsetsSeconds {
				if !finite(offset) {
					return Suite{}, fmt.Errorf("query %q has a non-finite instant offset", query.Name)
				}
			}
		}
		effective := query.EffectiveComparison(suite.ComparisonDefaults)
		if err := validateTolerance(effective.ValueTolerance); err != nil {
			return Suite{}, fmt.Errorf("query %q: %w", query.Name, err)
		}
		if err := validateInstantVectorOrder(effective.InstantVectorOrder); err != nil {
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

func (q QueryCase) EffectiveComparison(defaults ComparisonPolicy) ComparisonPolicy {
	if q.Comparison == nil {
		return defaults
	}
	result := defaults
	if q.Comparison.ValueTolerance != nil {
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
	}
	if q.Comparison.InstantVectorOrder != nil {
		result.InstantVectorOrder = q.Comparison.InstantVectorOrder
	}
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

func (r RangeSpec) containsGridOffset(offset float64) bool {
	steps := (offset - r.StartOffsetSeconds) / r.StepSeconds
	nearestStep := math.Round(steps)
	return math.Abs(steps-nearestStep) <= 1e-9
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

func validateInstantVectorOrder(order *InstantVectorOrder) error {
	if order == nil {
		return nil
	}
	if order.Direction != instantOrderAscending && order.Direction != instantOrderDescending {
		return fmt.Errorf("instant vector order direction must be ascending or descending")
	}
	if order.Grouping == nil {
		return nil
	}
	if order.Grouping.Mode != orderGroupingBy && order.Grouping.Mode != orderGroupingWithout {
		return fmt.Errorf("instant vector order grouping mode must be by or without")
	}
	if len(order.Grouping.Labels) == 0 {
		return fmt.Errorf("instant vector order grouping must include at least one label")
	}
	seen := make(map[string]struct{}, len(order.Grouping.Labels))
	for _, label := range order.Grouping.Labels {
		if label == "" {
			return fmt.Errorf("instant vector order grouping labels must be non-empty")
		}
		if _, duplicate := seen[label]; duplicate {
			return fmt.Errorf("instant vector order grouping label %q is duplicated", label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func addSeconds(base time.Time, seconds float64) time.Time {
	return base.Add(time.Duration(seconds * float64(time.Second)))
}
