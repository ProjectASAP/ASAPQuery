package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"
)

const (
	defaultDatasetAge      = 30 * time.Minute
	serviceReadyTimeout    = 3 * time.Minute
	dataReadyTimeout       = 3 * time.Minute
	readinessPollInterval  = time.Second
	composeShutdownTimeout = time.Minute
)

type RunOptions struct {
	DatasetPath       string
	SuitePath         string
	ReferenceURL      string
	TestURL           string
	ReferenceWriteURL string
	TestWriteURL      string
	ComposeFiles      []string
	ComposeProject    string
	BaseTimeMS        int64
	KeepServices      bool
}

// Run executes one isolated dataset/suite run. If ComposeFiles is empty, the
// caller owns the target processes; otherwise this function starts and stops
// an isolated Docker Compose project around the run.
func Run(ctx context.Context, options RunOptions) (Report, error) {
	fixture, err := seeder.LoadFixture(options.DatasetPath)
	if err != nil {
		return Report{}, err
	}
	suite, err := LoadSuiteFile(options.SuitePath)
	if err != nil {
		return Report{}, err
	}
	if options.ReferenceURL == "" || options.TestURL == "" {
		return Report{}, fmt.Errorf("reference and test query URLs are required")
	}
	if options.ReferenceWriteURL == "" {
		options.ReferenceWriteURL = options.ReferenceURL
	}
	if options.TestWriteURL == "" {
		return Report{}, fmt.Errorf("test write URL is required")
	}

	base := runBaseTime(options.BaseTimeMS)
	runDirectory, err := os.MkdirTemp("", "asapquery-differential-")
	if err != nil {
		return Report{}, fmt.Errorf("create run directory: %w", err)
	}
	defer os.RemoveAll(runDirectory)
	if err := writeGeneratedConfigs(runDirectory, fixture, suite); err != nil {
		return Report{}, err
	}

	lifecycle := composeLifecycle{
		files:   options.ComposeFiles,
		project: options.ComposeProject,
		env:     []string{"ASAP_TEST_CONFIG_DIR=" + runDirectory},
	}
	if len(options.ComposeFiles) > 0 {
		if err := lifecycle.Start(ctx); err != nil {
			return Report{}, err
		}
		if !options.KeepServices {
			defer lifecycle.Stop()
		}
	}

	reference, err := NewHTTPQueryTarget(options.ReferenceURL)
	if err != nil {
		return Report{}, err
	}
	test, err := NewHTTPQueryTarget(options.TestURL)
	if err != nil {
		return Report{}, err
	}
	if err := waitForQueryEndpoint(ctx, reference, fixture.Series[0].Metric, base); err != nil {
		return Report{}, fmt.Errorf("reference target is not ready: %w", err)
	}
	if err := waitForQueryEndpoint(ctx, test, fixture.Series[0].Metric, base); err != nil {
		return Report{}, fmt.Errorf("test target is not ready: %w", err)
	}

	request := seeder.BuildWriteRequestFromFixture(base.UnixMilli(), fixture)
	body, err := seeder.EncodeSnappy(request)
	if err != nil {
		return Report{}, fmt.Errorf("encode dataset %q: %w", fixture.Name, err)
	}
	if err := seeder.PushEncoded(ctx, options.ReferenceWriteURL, body); err != nil {
		return Report{}, fmt.Errorf("seed reference target: %w", err)
	}
	if err := seeder.PushEncoded(ctx, options.TestWriteURL, body); err != nil {
		return Report{}, fmt.Errorf("seed test target: %w", err)
	}
	lastSample := base.Add(time.Duration(maxOffsetSeconds(fixture)) * time.Second)
	if err := waitForData(ctx, reference, fixture.Series[0].Metric, lastSample); err != nil {
		return Report{}, fmt.Errorf("reference target did not expose seeded data: %w", err)
	}
	if err := waitForData(ctx, test, fixture.Series[0].Metric, lastSample); err != nil {
		return Report{}, fmt.Errorf("test target did not expose seeded data: %w", err)
	}

	return CompareSuite(ctx, reference, test, suite, fixture.Name, base)
}

func runBaseTime(baseTimeMS int64) time.Time {
	if baseTimeMS != 0 {
		return time.UnixMilli(baseTimeMS).UTC()
	}
	return time.Now().UTC().Truncate(time.Minute).Add(-defaultDatasetAge)
}

func maxOffsetSeconds(fixture seeder.Fixture) int64 {
	var result int64
	for _, series := range fixture.Series {
		for _, sample := range series.Samples {
			if sample.OffsetSeconds > result {
				result = sample.OffsetSeconds
			}
		}
	}
	return result
}

func waitForQueryEndpoint(ctx context.Context, target QueryAPI, metric string, timestamp time.Time) error {
	deadline, cancel := context.WithTimeout(ctx, serviceReadyTimeout)
	defer cancel()
	for {
		_, _, err := target.Query(deadline, metric, timestamp)
		if err == nil {
			return nil
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-time.After(readinessPollInterval):
		}
	}
}

func waitForData(ctx context.Context, target QueryAPI, metric string, timestamp time.Time) error {
	deadline, cancel := context.WithTimeout(ctx, dataReadyTimeout)
	defer cancel()
	var lastErr error
	for {
		value, _, err := target.Query(deadline, metric, timestamp)
		if err != nil {
			lastErr = err
		} else if hasSamples(value) {
			return nil
		}
		select {
		case <-deadline.Done():
			if lastErr != nil {
				return fmt.Errorf("last query error: %w", lastErr)
			}
			return fmt.Errorf("metric %q is still empty", metric)
		case <-time.After(readinessPollInterval):
		}
	}
}

func hasSamples(value model.Value) bool {
	switch value := value.(type) {
	case model.Vector:
		return len(value) > 0
	case model.Matrix:
		for _, stream := range value {
			if len(stream.Values) > 0 {
				return true
			}
		}
		return false
	case *model.Scalar, *model.String:
		return true
	default:
		return false
	}
}

type composeLifecycle struct {
	files   []string
	project string
	env     []string
	started bool
}

func (l *composeLifecycle) Start(ctx context.Context) error {
	if l.project == "" {
		l.project = "asapquery-differential"
	}
	args := l.composeArgs()
	args = append(args, "up", "-d", "--build")
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = append(os.Environ(), l.env...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("start compose project %q: %w\n%s", l.project, err, output)
	}
	l.started = true
	return nil
}

func (l *composeLifecycle) Stop() {
	if !l.started {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), composeShutdownTimeout)
	defer cancel()
	args := l.composeArgs()
	args = append(args, "down", "--volumes", "--remove-orphans")
	command := exec.CommandContext(ctx, "docker", args...)
	command.Env = append(os.Environ(), l.env...)
	_ = command.Run()
}

func (l *composeLifecycle) composeArgs() []string {
	args := []string{"compose"}
	if l.project != "" {
		args = append(args, "--project-name", l.project)
	}
	for _, file := range l.files {
		args = append(args, "--file", file)
	}
	return args
}

type plannerConfig struct {
	QueryGroups []plannerQueryGroup `yaml:"query_groups"`
	Metrics     []plannerMetric     `yaml:"metrics"`
	Cleanup     plannerCleanup      `yaml:"aggregate_cleanup"`
}

type plannerQueryGroup struct {
	ID                int               `yaml:"id"`
	Queries           []string          `yaml:"queries"`
	RepetitionDelayMS int               `yaml:"repetition_delay_ms"`
	ControllerOptions plannerController `yaml:"controller_options"`
}

type plannerController struct {
	AccuracySLA float64 `yaml:"accuracy_sla"`
	LatencySLA  float64 `yaml:"latency_sla"`
}

type plannerMetric struct {
	Metric string   `yaml:"metric"`
	Labels []string `yaml:"labels"`
}

type plannerCleanup struct {
	Policy string `yaml:"policy"`
}

func writeGeneratedConfigs(directory string, fixture seeder.Fixture, suite Suite) error {
	metrics := make(map[string]map[string]struct{})
	for _, series := range fixture.Series {
		if metrics[series.Metric] == nil {
			metrics[series.Metric] = make(map[string]struct{})
		}
		for label := range series.Labels {
			metrics[series.Metric][label] = struct{}{}
		}
	}
	metricNames := make([]string, 0, len(metrics))
	for metric := range metrics {
		metricNames = append(metricNames, metric)
	}
	sort.Strings(metricNames)
	plannerMetrics := make([]plannerMetric, 0, len(metricNames))
	for _, metric := range metricNames {
		labels := make([]string, 0, len(metrics[metric]))
		for label := range metrics[metric] {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		plannerMetrics = append(plannerMetrics, plannerMetric{Metric: metric, Labels: labels})
	}
	queries := make([]string, 0, len(suite.Queries))
	for _, query := range suite.Queries {
		queries = append(queries, query.Expr)
	}
	config := plannerConfig{
		QueryGroups: []plannerQueryGroup{{
			ID: 1, Queries: queries, RepetitionDelayMS: 10_000,
			ControllerOptions: plannerController{AccuracySLA: 0.99, LatencySLA: 1},
		}},
		Metrics: plannerMetrics,
		Cleanup: plannerCleanup{Policy: "read_based"},
	}
	contents, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal generated planner config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "controller-config.yaml"), contents, 0o600); err != nil {
		return fmt.Errorf("write generated planner config: %w", err)
	}
	engineConfig := []byte(`output_dir: "/app/outputs"
log_level: "INFO"
data_ingestion_interval_ms: 1000
streaming_engine: "precompute"
http_server:
  port: 8088
backend:
  type: "prometheus"
  server: "http://prometheus:9090"
  forward_unsupported_queries: false
store:
  lock_strategy: "per-key"
ingest:
  type: "http_remote_write"
  port: 9091
inference_config: "/asap-planner-output/inference_config.yaml"
streaming_config: "/asap-planner-output/streaming_config.yaml"
`)
	if err := os.WriteFile(filepath.Join(directory, "engine_config.yaml"), engineConfig, 0o600); err != nil {
		return fmt.Errorf("write generated engine config: %w", err)
	}
	return nil
}
