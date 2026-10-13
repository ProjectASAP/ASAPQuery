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

func TestLatestEvaluationIsMaxAcrossQueriesAndOwningQuery(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000).UTC()
	suite := Suite{
		Name: "mixed",
		Queries: []QueryCase{
			{Name: "early-instant", Expr: "early", InstantOffsetsSeconds: []float64{300, 600}},
			{Name: "late-range", Expr: "late", Range: &RangeSpec{StartOffsetSeconds: 0, EndOffsetSeconds: 1200, StepSeconds: 60}},
			{Name: "mid-instant", Expr: "mid", InstantOffsetsSeconds: []float64{900}},
		},
	}
	query, latest, ok := latestEvaluation(suite, base)
	if !ok {
		t.Fatal("latestEvaluation: expected a result")
	}
	if want := base.Add(1200 * time.Second); !latest.Equal(want) {
		t.Fatalf("latest evaluation time = %s, want %s (the range's end, not the highest instant offset)", latest, want)
	}
	if query.Name != "late-range" {
		t.Fatalf("latest evaluation query = %q, want %q (the query that timestamp actually belongs to)", query.Name, "late-range")
	}
}

func TestLatestEvaluationEmptySuite(t *testing.T) {
	if _, _, ok := latestEvaluation(Suite{Name: "empty"}, time.Now()); ok {
		t.Fatal("latestEvaluation: expected no result for a suite with no queries")
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

func TestGeneratedConfigsWithFixedPlannerOutputSkipPlanner(t *testing.T) {
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
		Name:             "fixed-output",
		Queries:          []QueryCase{{Name: "up", Expr: "up"}},
		PlannerOutputDir: "/fixed/planner/output",
	}

	if err := writeGeneratedConfigs(directory, fixture, suite); err != nil {
		t.Fatalf("writeGeneratedConfigs: %v", err)
	}

	if _, err := os.Stat(filepath.Join(directory, "controller-config.yaml")); !os.IsNotExist(err) {
		t.Fatalf("planner config was generated for a suite with fixed planner output (stat err: %v)", err)
	}

	overrideContents, err := os.ReadFile(filepath.Join(directory, fixedPlannerOutputComposeFile))
	if err != nil {
		t.Fatalf("read compose override: %v", err)
	}
	var override struct {
		Services struct {
			Planner struct {
				Profiles []string `yaml:"profiles"`
			} `yaml:"planner"`
			QueryEngine struct {
				DependsOn yaml.Node `yaml:"depends_on"`
				Volumes   []string  `yaml:"volumes"`
			} `yaml:"queryengine"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(overrideContents, &override); err != nil {
		t.Fatalf("parse compose override: %v", err)
	}
	if len(override.Services.Planner.Profiles) == 0 {
		t.Fatal("compose override leaves the planner service active")
	}
	dependsOn := override.Services.QueryEngine.DependsOn
	if dependsOn.Tag != "!override" {
		t.Fatalf("queryengine depends_on tag = %q, want !override so the planner dependency is dropped", dependsOn.Tag)
	}
	var dependencies map[string]any
	if err := dependsOn.Decode(&dependencies); err != nil {
		t.Fatalf("decode queryengine depends_on: %v", err)
	}
	if _, ok := dependencies["planner"]; ok {
		t.Fatal("queryengine still depends on the planner")
	}
	wantVolume := "${" + fixedPlannerOutputDirEnv + ":?" + fixedPlannerOutputDirEnv + " is required}:" + plannerOutputMountPath + ":ro"
	if got := override.Services.QueryEngine.Volumes; len(got) != 1 || got[0] != wantVolume {
		t.Fatalf("queryengine volumes = %v, want [%s]", got, wantVolume)
	}

	engineContents, err := os.ReadFile(filepath.Join(directory, "engine_config.yaml"))
	if err != nil {
		t.Fatalf("read engine config: %v", err)
	}
	var engine struct {
		InferenceConfig string `yaml:"inference_config"`
		StreamingConfig string `yaml:"streaming_config"`
	}
	if err := yaml.Unmarshal(engineContents, &engine); err != nil {
		t.Fatalf("parse engine config: %v", err)
	}
	if got, want := engine.InferenceConfig, plannerOutputMountPath+"/"+plannerInferenceConfigFile; got != want {
		t.Fatalf("engine inference_config = %q, want %q", got, want)
	}
	if got, want := engine.StreamingConfig, plannerOutputMountPath+"/"+plannerStreamingConfigFile; got != want {
		t.Fatalf("engine streaming_config = %q, want %q", got, want)
	}
}
