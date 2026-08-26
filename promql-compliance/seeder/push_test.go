package seeder

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestBuildWriteRequest_TimestampsAndLabels(t *testing.T) {
	series := []SeriesDef{
		{
			Name:   "test_metric",
			Labels: map[string]string{"region": "us-east-1", "host": "a"},
			Samples: []Sample{
				{OffsetSeconds: 0, Value: 1.5},
				{OffsetSeconds: 60, Value: 2.5},
			},
		},
	}

	const base int64 = 1_700_000_000_000
	wr := BuildWriteRequest(base, series)

	if len(wr.Timeseries) != 1 {
		t.Fatalf("expected 1 timeseries, got %d", len(wr.Timeseries))
	}
	ts := wr.Timeseries[0]

	if len(ts.Samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(ts.Samples))
	}
	if ts.Samples[0].Timestamp != base {
		t.Errorf("sample 0 timestamp = %d, want %d", ts.Samples[0].Timestamp, base)
	}
	if ts.Samples[1].Timestamp != base+60_000 {
		t.Errorf("sample 1 timestamp = %d, want %d", ts.Samples[1].Timestamp, base+60_000)
	}
	if ts.Samples[0].Value != 1.5 || ts.Samples[1].Value != 2.5 {
		t.Errorf("unexpected sample values: %+v", ts.Samples)
	}

	// Labels must include __name__ and be sorted by name.
	names := make([]string, len(ts.Labels))
	for i, l := range ts.Labels {
		names[i] = l.Name
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("labels not sorted by name: %v", names)
	}

	got := map[string]string{}
	for _, l := range ts.Labels {
		got[l.Name] = l.Value
	}
	want := map[string]string{"__name__": "test_metric", "region": "us-east-1", "host": "a"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %q = %q, want %q", k, got[k], v)
		}
	}
}

func TestEncodeDecodeSnappyRoundTrip(t *testing.T) {
	wr := BuildWriteRequest(1000, []SeriesDef{
		{
			Name:   "roundtrip_metric",
			Labels: map[string]string{"env": "prod"},
			Samples: []Sample{
				{OffsetSeconds: 0, Value: 42},
				{OffsetSeconds: 5, Value: 43.5},
			},
		},
	})

	body, err := EncodeSnappy(wr)
	if err != nil {
		t.Fatalf("EncodeSnappy: %v", err)
	}

	decoded, err := DecodeSnappy(body)
	if err != nil {
		t.Fatalf("DecodeSnappy: %v", err)
	}

	if len(decoded.Timeseries) != 1 {
		t.Fatalf("expected 1 timeseries after roundtrip, got %d", len(decoded.Timeseries))
	}
	if !reflect.DeepEqual(decoded.Timeseries[0].Labels, wr.Timeseries[0].Labels) ||
		!reflect.DeepEqual(decoded.Timeseries[0].Samples, wr.Timeseries[0].Samples) {
		t.Errorf("roundtripped timeseries mismatch:\ngot:  %+v\nwant: %+v", decoded.Timeseries[0], wr.Timeseries[0])
	}
}

func TestDatasetBuildsAndEncodesCleanly(t *testing.T) {
	wr := BuildWriteRequest(1_700_000_000_000, Dataset())

	if len(wr.Timeseries) != 6 {
		t.Fatalf("expected 6 series in Dataset(), got %d", len(wr.Timeseries))
	}

	totalSamples := 0
	for _, ts := range wr.Timeseries {
		totalSamples += len(ts.Samples)
	}
	// 4 series x 21 samples + 2 series x 6 samples = 96.
	if want := 4*21 + 2*6; totalSamples != want {
		t.Errorf("total samples = %d, want %d", totalSamples, want)
	}

	if _, err := EncodeSnappy(wr); err != nil {
		t.Fatalf("EncodeSnappy(Dataset): %v", err)
	}
}

func TestDatasetHandComputedValues(t *testing.T) {
	series := Dataset()

	find := func(name string, labels map[string]string) SeriesDef {
		for _, s := range series {
			if s.Name != name || len(s.Labels) != len(labels) {
				continue
			}
			match := true
			for k, v := range labels {
				if s.Labels[k] != v {
					match = false
					break
				}
			}
			if match {
				return s
			}
		}
		t.Fatalf("series %s%v not found in Dataset()", name, labels)
		return SeriesDef{}
	}

	sampleAt := func(s SeriesDef, offset int64) (float64, bool) {
		for _, sm := range s.Samples {
			if sm.OffsetSeconds == offset {
				return sm.Value, true
			}
		}
		return 0, false
	}

	httpA := find("http_requests_total", map[string]string{"host": "a"})
	if v, ok := sampleAt(httpA, 1200); !ok || v != 1200 {
		t.Errorf("http_requests_total{host=a} at offset 1200 = %v (ok=%v), want 1200", v, ok)
	}

	httpB := find("http_requests_total", map[string]string{"host": "b"})
	if v, ok := sampleAt(httpB, 600); !ok || v != 2200 {
		t.Errorf("http_requests_total{host=b} at offset 600 = %v (ok=%v), want 2200", v, ok)
	}

	memA := find("node_memory_used_bytes", map[string]string{"host": "a"})
	if v, ok := sampleAt(memA, 600); !ok || v != 1000 {
		t.Errorf("node_memory_used_bytes{host=a} peak at offset 600 = %v (ok=%v), want 1000", v, ok)
	}
	if v, ok := sampleAt(memA, 1200); !ok || v != 500 {
		t.Errorf("node_memory_used_bytes{host=a} at offset 1200 = %v (ok=%v), want 500", v, ok)
	}

	memB := find("node_memory_used_bytes", map[string]string{"host": "b"})
	for _, sm := range memB.Samples {
		if sm.Value != 2000 {
			t.Errorf("node_memory_used_bytes{host=b} at offset %d = %v, want flat 2000", sm.OffsetSeconds, sm.Value)
		}
	}

	east := find("checkout_up", map[string]string{"service": "checkout", "region": "us-east"})
	if len(east.Samples) != 6 {
		t.Errorf("checkout_up{region=us-east} has %d samples, want 6", len(east.Samples))
	}
	for _, sm := range east.Samples {
		if sm.OffsetSeconds > 300 {
			t.Errorf("checkout_up{region=us-east} has sample at offset %d, want all <= 300", sm.OffsetSeconds)
		}
	}

	west := find("checkout_up", map[string]string{"service": "checkout", "region": "us-west"})
	if len(west.Samples) != 6 {
		t.Errorf("checkout_up{region=us-west} has %d samples, want 6", len(west.Samples))
	}
	for _, sm := range west.Samples {
		if sm.OffsetSeconds < 900 {
			t.Errorf("checkout_up{region=us-west} has sample at offset %d, want all >= 900", sm.OffsetSeconds)
		}
	}
}

func TestPushPostsSnappyProtobufWithExpectedHeaders(t *testing.T) {
	var gotContentType, gotContentEncoding, gotVersion, gotPath string
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotContentEncoding = r.Header.Get("Content-Encoding")
		gotVersion = r.Header.Get("X-Prometheus-Remote-Write-Version")
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("reading request body: %v", err)
		}
		gotBody = b
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	wr := BuildWriteRequest(1000, []SeriesDef{
		{
			Name:    "push_test_metric",
			Labels:  map[string]string{"k": "v"},
			Samples: []Sample{{OffsetSeconds: 0, Value: 7}},
		},
	})

	if err := Push(context.Background(), srv.URL, wr); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if gotPath != "/api/v1/write" {
		t.Errorf("path = %q, want /api/v1/write", gotPath)
	}
	if gotContentType != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", gotContentType)
	}
	if gotContentEncoding != "snappy" {
		t.Errorf("Content-Encoding = %q, want snappy", gotContentEncoding)
	}
	if gotVersion != "0.1.0" {
		t.Errorf("X-Prometheus-Remote-Write-Version = %q, want 0.1.0", gotVersion)
	}

	decoded, err := DecodeSnappy(gotBody)
	if err != nil {
		t.Fatalf("DecodeSnappy(received body): %v", err)
	}
	if len(decoded.Timeseries) != 1 ||
		!reflect.DeepEqual(decoded.Timeseries[0].Labels, wr.Timeseries[0].Labels) ||
		!reflect.DeepEqual(decoded.Timeseries[0].Samples, wr.Timeseries[0].Samples) {
		t.Errorf("received WriteRequest does not match what was built:\ngot:  %+v\nwant: %+v", decoded, wr)
	}
}

func TestPushReturnsErrorOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	wr := &prompb.WriteRequest{}
	if err := Push(context.Background(), srv.URL, wr); err == nil {
		t.Fatal("expected error on 400 response, got nil")
	}
}
