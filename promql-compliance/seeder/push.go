// Package seeder builds Prometheus remote-write WriteRequests from a fixed,
// hand-authored dataset and pushes them to one or more remote-write
// endpoints. It exists to seed a real Prometheus and ASAPQuery's own
// remote-write ingest endpoint with the exact same bytes, so a differential
// PromQL compliance test (see GitHub issue #594) has no risk of the two
// ingestion mechanisms disagreeing and producing false diffs.
package seeder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// Sample is one (offset, value) point in a SeriesDef. OffsetSeconds is
// relative to a base time supplied at push time (see BuildWriteRequest),
// not an absolute Unix timestamp — this keeps the dataset's values fully
// deterministic while letting the actual wall-clock timestamps be chosen
// fresh on every run, which real Prometheus requires (it rejects samples
// that are too far in the past or future relative to "now").
type Sample struct {
	OffsetSeconds int64
	Value         float64
}

// SeriesDef is one time series: a metric name, a label set (NOT including
// __name__), and its samples.
type SeriesDef struct {
	Name    string
	Labels  map[string]string
	Samples []Sample
}

// BuildWriteRequest converts a set of SeriesDef into a prompb.WriteRequest,
// resolving each sample's absolute timestamp as baseTimeMs +
// sample.OffsetSeconds*1000.
func BuildWriteRequest(baseTimeMs int64, series []SeriesDef) *prompb.WriteRequest {
	wr := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(series)),
	}

	for _, s := range series {
		labels := make([]prompb.Label, 0, len(s.Labels)+1)
		labels = append(labels, prompb.Label{Name: "__name__", Value: s.Name})
		for k, v := range s.Labels {
			labels = append(labels, prompb.Label{Name: k, Value: v})
		}
		// Prometheus's remote-write receiver requires labels to be sorted
		// by name (excluding this, some implementations reject the write).
		sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })

		samples := make([]prompb.Sample, 0, len(s.Samples))
		for _, sm := range s.Samples {
			samples = append(samples, prompb.Sample{
				Value:     sm.Value,
				Timestamp: baseTimeMs + sm.OffsetSeconds*1000,
			})
		}

		wr.Timeseries = append(wr.Timeseries, prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		})
	}

	return wr
}

// EncodeSnappy protobuf-marshals a WriteRequest and snappy-compresses the
// result, i.e. produces exactly the body Prometheus remote-write expects.
func EncodeSnappy(wr *prompb.WriteRequest) ([]byte, error) {
	data, err := proto.Marshal(wr)
	if err != nil {
		return nil, fmt.Errorf("marshal WriteRequest: %w", err)
	}
	return snappy.Encode(nil, data), nil
}

// DecodeSnappy reverses EncodeSnappy: snappy-decompresses and
// protobuf-unmarshals a remote-write body back into a WriteRequest. It is
// primarily useful for tests that want to assert on what was actually sent.
func DecodeSnappy(body []byte) (*prompb.WriteRequest, error) {
	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	wr := &prompb.WriteRequest{}
	if err := proto.Unmarshal(decompressed, wr); err != nil {
		return nil, fmt.Errorf("unmarshal WriteRequest: %w", err)
	}
	return wr, nil
}

// Push POSTs a WriteRequest to url + "/api/v1/write" using the standard
// Prometheus remote-write wire format: snappy-compressed protobuf, with the
// headers a remote-write receiver expects.
func Push(ctx context.Context, url string, wr *prompb.WriteRequest) error {
	body, err := EncodeSnappy(wr)
	if err != nil {
		return err
	}
	return PushEncoded(ctx, url, body)
}

// PushEncoded sends an already-encoded remote-write body. The caller can
// encode once and send the exact same bytes to every target.
func PushEncoded(ctx context.Context, url string, body []byte) error {
	endpoint := strings.TrimRight(url, "/") + "/api/v1/write"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request for %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("POST %s: unexpected status %s: %s", url, resp.Status, string(respBody))
	}
	return nil
}

// PushDataset builds the write request for the fixed Dataset() at the given
// base time and pushes it to every URL in urls, stopping at the first
// error.
func PushDataset(ctx context.Context, baseTimeMs int64, urls ...string) error {
	return PushSeries(ctx, baseTimeMs, Dataset(), urls...)
}

// PushFixture sends a YAML fixture to every target using one encoded body.
func PushFixture(ctx context.Context, baseTimeMs int64, fixture Fixture, urls ...string) error {
	wr := BuildWriteRequestFromFixture(baseTimeMs, fixture)
	return PushEncodedRequest(ctx, wr, urls...)
}

// PushSeries sends one canonical encoded request to every target.
func PushSeries(ctx context.Context, baseTimeMs int64, series []SeriesDef, urls ...string) error {
	wr := BuildWriteRequest(baseTimeMs, series)
	return PushEncodedRequest(ctx, wr, urls...)
}

func PushEncodedRequest(ctx context.Context, wr *prompb.WriteRequest, urls ...string) error {
	body, err := EncodeSnappy(wr)
	if err != nil {
		return err
	}
	for _, u := range urls {
		if err := PushEncoded(ctx, u, body); err != nil {
			return err
		}
	}
	return nil
}
