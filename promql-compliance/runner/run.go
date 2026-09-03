package runner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ProjectASAP/ASAPQuery/promql-compliance/seeder"
	"github.com/prometheus/common/model"
	promqlparser "github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

const (
	defaultDatasetAge              = 30 * time.Minute
	defaultPlannerWindow           = 5 * time.Minute
	defaultPlannerWindowMS         = int(defaultPlannerWindow / time.Millisecond)
	defaultDataIngestionIntervalMS = 1000
	defaultPlannerStepMS           = 60_000
	serviceReadyTimeout            = 3 * time.Minute
	dataReadyTimeout               = 3 * time.Minute
	readinessPollInterval          = time.Second
	composeShutdownTimeout         = time.Minute
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
	probeQuery := suite.Queries[0].Expr
	if err := waitForHTTPReady(ctx, options.ReferenceURL); err != nil {
		return Report{}, fmt.Errorf("reference target is not ready: %w", err)
	}
	if err := waitForHTTPReady(ctx, options.TestURL); err != nil {
		return Report{}, fmt.Errorf("test target is not ready: %w", err)
	}

	requests := seeder.BuildWriteRequestsFromFixtureBatches(base.UnixMilli(), fixture)
	for index, request := range requests {
		body, err := seeder.EncodeSnappy(request)
		if err != nil {
			return Report{}, fmt.Errorf("encode dataset %q batch %d: %w", fixture.Name, index, err)
		}
		if err := seeder.PushEncoded(ctx, options.ReferenceWriteURL, body); err != nil {
			return Report{}, fmt.Errorf("seed reference target batch %d: %w", index, err)
		}
		if err := seeder.PushEncoded(ctx, options.TestWriteURL, body); err != nil {
			return Report{}, fmt.Errorf("seed test target batch %d: %w", index, err)
		}
	}
	probeTime, err := dataProbeTime(suite.Queries[0], base)
	if err != nil {
		return Report{}, err
	}
	if err := waitForData(ctx, reference, probeQuery, probeTime); err != nil {
		return Report{}, fmt.Errorf("reference target did not expose seeded data: %w", err)
	}
	if err := waitForData(ctx, test, probeQuery, probeTime); err != nil {
		return Report{}, fmt.Errorf("test target did not expose seeded data: %w", err)
	}

	return CompareSuite(ctx, reference, test, suite, fixture.Name, base)
}

func runBaseTime(baseTimeMS int64) time.Time {
	if baseTimeMS != 0 {
		return time.UnixMilli(baseTimeMS).UTC()
	}
	return time.Now().UTC().Truncate(defaultPlannerWindow).Add(-defaultDatasetAge)
}

func dataProbeTime(query QueryCase, base time.Time) (time.Time, error) {
	if offsets := query.InstantTimes(base); len(offsets) > 0 {
		return offsets[0], nil
	}
	interval, err := query.RangeAt(base)
	if err != nil {
		return time.Time{}, fmt.Errorf("select data probe time: %w", err)
	}
	return interval.End, nil
}

func waitForHTTPReady(ctx context.Context, baseURL string) error {
	deadline, cancel := context.WithTimeout(ctx, serviceReadyTimeout)
	defer cancel()
	healthURL := strings.TrimRight(baseURL, "/") + "/api/v1/status/runtimeinfo"
	client := &http.Client{}
	var lastErr error
	for {
		request, err := http.NewRequestWithContext(deadline, http.MethodGet, healthURL, nil)
		if err == nil {
			response, requestErr := client.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
					return nil
				}
				lastErr = fmt.Errorf("HTTP %s", response.Status)
			} else {
				lastErr = requestErr
			}
		} else {
			lastErr = err
		}
		select {
		case <-deadline.Done():
			if lastErr != nil {
				return fmt.Errorf("last query error: %w", lastErr)
			}
			return deadline.Err()
		case <-time.After(readinessPollInterval):
		}
	}
}

func waitForData(ctx context.Context, target QueryAPI, query string, timestamp time.Time) error {
	deadline, cancel := context.WithTimeout(ctx, dataReadyTimeout)
	defer cancel()
	var lastErr error
	for {
		value, _, err := target.Query(deadline, query, timestamp)
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
			return fmt.Errorf("query %q is still empty", query)
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
	StepMS            *int              `yaml:"step_ms,omitempty"`
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
	queryGroups := make([]plannerQueryGroup, 0, len(suite.Queries))
	for index, query := range suite.Queries {
		repetitionDelayMS, err := plannerRepetitionDelayMS(query)
		if err != nil {
			return fmt.Errorf("query %q: %w", query.Name, err)
		}
		group := plannerQueryGroup{
			ID: index + 1, Queries: []string{query.Expr}, RepetitionDelayMS: repetitionDelayMS,
			ControllerOptions: plannerController{AccuracySLA: 0.99, LatencySLA: 1},
		}
		if query.Range != nil {
			stepMS := int(query.Range.StepSeconds * float64(time.Second/time.Millisecond))
			group.StepMS = &stepMS
		}
		queryGroups = append(queryGroups, group)
	}
	config := plannerConfig{
		QueryGroups: queryGroups,
		Metrics:     plannerMetrics,
		Cleanup:     plannerCleanup{Policy: "read_based"},
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

func plannerRepetitionDelayMS(query QueryCase) (int, error) {
	lookbackMS := defaultDataIngestionIntervalMS
	if expr, err := promqlparser.NewParser(promqlparser.Options{}).ParseExpr(query.Expr); err == nil {
		maxRange := time.Duration(0)
		promqlparser.Inspect(expr, func(node promqlparser.Node, _ []promqlparser.Node) error {
			switch node := node.(type) {
			case *promqlparser.MatrixSelector:
				maxRange = maxDuration(maxRange, node.Range)
			case *promqlparser.SubqueryExpr:
				maxRange = maxDuration(maxRange, node.Range)
			}
			return nil
		})
		if maxRange > 0 {
			lookbackMS = int(maxRange / time.Millisecond)
		}
	}

	delayMS := minInt(defaultPlannerWindowMS, lookbackMS)
	if delayMS < defaultDataIngestionIntervalMS {
		delayMS = defaultDataIngestionIntervalMS
	}

	stepMS := defaultPlannerStepMS
	if query.Range != nil {
		stepMS = int(query.Range.StepSeconds * float64(time.Second/time.Millisecond))
	}
	if stepMS > 0 && delayMS < stepMS && stepMS%delayMS != 0 {
		delayMS = greatestCommonDivisor(delayMS, stepMS)
		if delayMS < defaultDataIngestionIntervalMS {
			return 0, fmt.Errorf(
				"no repetition delay at least %dms divides evaluation step %dms within lookback %dms",
				defaultDataIngestionIntervalMS, stepMS, lookbackMS,
			)
		}
	}
	return delayMS, nil
}

func maxDuration(left, right time.Duration) time.Duration {
	if right > left {
		return right
	}
	return left
}

func minInt(left, right int) int {
	if right < left {
		return right
	}
	return left
}

func greatestCommonDivisor(left, right int) int {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}
