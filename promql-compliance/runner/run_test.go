package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	"gopkg.in/yaml.v3"
)

func TestDataProbeTimeUsesConfiguredEvaluationTime(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	probe, err := dataProbeTime(QueryCase{
		InstantOffsetsSeconds: []float64{300, 600},
	}, base)
	if err != nil {
		t.Fatalf("dataProbeTime: %v", err)
	}
	if got, want := probe, base.Add(300*time.Second); !got.Equal(want) {
		t.Fatalf("probe time = %s, want %s", got, want)
	}
}

func TestGeneratedConfigsUseCurrentPlannerAndEngineSchema(t *testing.T) {
	directory := t.TempDir()
	fixture := seeder.Fixture{
		Name: "single-series",
		Series: []seeder.FixtureSeries{{
			Metric:  "up",
			Labels:  map[string]string{"job": "test"},
			Samples: []seeder.FixtureSample{{OffsetSeconds: 0, Value: 1}},
		}},
	}
	suite := Suite{
		Name:    "single-query",
		Queries: []QueryCase{{Name: "up", Expr: "up"}},
	}

	if err := writeGeneratedConfigs(directory, fixture, suite); err != nil {
		t.Fatalf("writeGeneratedConfigs: %v", err)
	}

	plannerContents, err := os.ReadFile(filepath.Join(directory, "controller-config.yaml"))
	if err != nil {
		t.Fatalf("read planner config: %v", err)
	}
	var planner struct {
		QueryGroups []struct {
			RepetitionDelayMS int `yaml:"repetition_delay_ms"`
		} `yaml:"query_groups"`
	}
	if err := yaml.Unmarshal(plannerContents, &planner); err != nil {
		t.Fatalf("parse planner config: %v", err)
	}
	if got, want := planner.QueryGroups[0].RepetitionDelayMS, defaultDataIngestionIntervalMS; got != want {
		t.Fatalf("planner repetition delay = %d, want %d", got, want)
	}

	engineContents, err := os.ReadFile(filepath.Join(directory, "engine_config.yaml"))
	if err != nil {
		t.Fatalf("read engine config: %v", err)
	}
	var engine struct {
		DataIngestionIntervalMS int `yaml:"data_ingestion_interval_ms"`
	}
	if err := yaml.Unmarshal(engineContents, &engine); err != nil {
		t.Fatalf("parse engine config: %v", err)
	}
	if got, want := engine.DataIngestionIntervalMS, 1000; got != want {
		t.Fatalf("engine ingestion interval = %d, want %d", got, want)
	}
}

func TestGeneratedPlannerGroupsUseCompatibleQueryTiming(t *testing.T) {
	directory := t.TempDir()
	fixture := seeder.Fixture{
		Name: "single-series",
		Series: []seeder.FixtureSeries{{
			Metric:  "up",
			Labels:  map[string]string{"job": "test"},
			Samples: []seeder.FixtureSample{{OffsetSeconds: 0, Value: 1}},
		}},
	}
	suite := Suite{
		Name: "mixed-query-timing",
		Queries: []QueryCase{
			{Name: "instant", Expr: "up", InstantOffsetsSeconds: []float64{0}},
			{
				Name:                  "short-rate",
				Expr:                  "rate(up[1500ms])",
				InstantOffsetsSeconds: []float64{60},
				Range:                 &RangeSpec{StartOffsetSeconds: 60, EndOffsetSeconds: 120, StepSeconds: 2},
			},
		},
	}

	if err := writeGeneratedConfigs(directory, fixture, suite); err != nil {
		t.Fatalf("writeGeneratedConfigs: %v", err)
	}

	plannerContents, err := os.ReadFile(filepath.Join(directory, "controller-config.yaml"))
	if err != nil {
		t.Fatalf("read planner config: %v", err)
	}
	var planner struct {
		QueryGroups []struct {
			Queries           []string `yaml:"queries"`
			RepetitionDelayMS int      `yaml:"repetition_delay_ms"`
			RangeDurationMS   *int     `yaml:"range_duration_ms"`
			StepMS            *int     `yaml:"step_ms"`
		} `yaml:"query_groups"`
	}
	if err := yaml.Unmarshal(plannerContents, &planner); err != nil {
		t.Fatalf("parse planner config: %v", err)
	}
	if got, want := len(planner.QueryGroups), 2; got != want {
		t.Fatalf("planner query groups = %d, want %d", got, want)
	}
	if got, want := planner.QueryGroups[0].RepetitionDelayMS, defaultDataIngestionIntervalMS; got != want {
		t.Fatalf("instant repetition delay = %d, want %d", got, want)
	}
	if got, want := planner.QueryGroups[1].RepetitionDelayMS, 1_000; got != want {
		t.Fatalf("short-rate repetition delay = %d, want %d", got, want)
	}
	if planner.QueryGroups[1].StepMS == nil || *planner.QueryGroups[1].StepMS != 2_000 {
		t.Fatalf("short-rate step_ms = %v, want 2000", planner.QueryGroups[1].StepMS)
	}
	if planner.QueryGroups[1].RangeDurationMS == nil || *planner.QueryGroups[1].RangeDurationMS != 60_000 {
		t.Fatalf("short-rate range_duration_ms = %v, want 60000", planner.QueryGroups[1].RangeDurationMS)
	}
}
