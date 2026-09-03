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
	if got, want := planner.QueryGroups[0].RepetitionDelayMS, defaultPlannerWindowMS; got != want {
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
