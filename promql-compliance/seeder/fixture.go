package seeder

import (
	"bytes"
	"fmt"
	"os"

	"github.com/prometheus/prometheus/prompb"
	"gopkg.in/yaml.v3"
)

// Fixture is a versioned, human-authored dataset. Sample timestamps are
// offsets from the base timestamp selected for a run, so the same fixture can
// be replayed against Prometheus without becoming too old or too far ahead.
type Fixture struct {
	Name   string          `yaml:"name"`
	Series []FixtureSeries `yaml:"series"`
}

type FixtureSeries struct {
	Metric  string            `yaml:"metric"`
	Labels  map[string]string `yaml:"labels"`
	Samples []FixtureSample   `yaml:"samples"`
}

type FixtureSample struct {
	OffsetSeconds int64   `yaml:"offset_seconds"`
	Value         float64 `yaml:"value"`
}

// LoadFixture reads and validates a dataset fixture from YAML.
func LoadFixture(path string) (Fixture, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Fixture{}, fmt.Errorf("read dataset fixture %q: %w", path, err)
	}

	var fixture Fixture
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&fixture); err != nil {
		return Fixture{}, fmt.Errorf("parse dataset fixture %q: %w", path, err)
	}
	if fixture.Name == "" {
		return Fixture{}, fmt.Errorf("dataset fixture %q has no name", path)
	}
	if len(fixture.Series) == 0 {
		return Fixture{}, fmt.Errorf("dataset fixture %q has no series", path)
	}
	for i, series := range fixture.Series {
		if series.Metric == "" {
			return Fixture{}, fmt.Errorf("dataset fixture %q series %d has no metric", path, i)
		}
		if len(series.Samples) == 0 {
			return Fixture{}, fmt.Errorf("dataset fixture %q series %q has no samples", path, series.Metric)
		}
	}
	return fixture, nil
}

// BuildWriteRequestFromFixture converts a fixture into the same canonical
// remote-write representation used by the hand-authored Dataset.
func BuildWriteRequestFromFixture(baseTimeMs int64, fixture Fixture) *prompb.WriteRequest {
	series := make([]SeriesDef, 0, len(fixture.Series))
	for _, input := range fixture.Series {
		samples := make([]Sample, 0, len(input.Samples))
		for _, sample := range input.Samples {
			samples = append(samples, Sample{
				OffsetSeconds: sample.OffsetSeconds,
				Value:         sample.Value,
			})
		}
		series = append(series, SeriesDef{
			Name:    input.Metric,
			Labels:  input.Labels,
			Samples: samples,
		})
	}
	return BuildWriteRequest(baseTimeMs, series)
}
