package seeder

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFixtureBuildsRelativeTimestamps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.yaml")
	contents := []byte(`name: sparse-checkout
series:
  - metric: checkout_up
    labels:
      service: checkout
      region: us-east
    samples:
      - offset_seconds: 0
        value: 1
      - offset_seconds: 300
        value: 0
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	fixture, err := LoadFixture(path)
	if err != nil {
		t.Fatalf("LoadFixture: %v", err)
	}
	if fixture.Name != "sparse-checkout" {
		t.Fatalf("fixture name = %q, want sparse-checkout", fixture.Name)
	}

	request := BuildWriteRequestFromFixture(1_700_000_000_000, fixture)
	if got := len(request.Timeseries); got != 1 {
		t.Fatalf("timeseries = %d, want 1", got)
	}
	if got := request.Timeseries[0].Samples[1].Timestamp; got != 1_700_000_300_000 {
		t.Fatalf("timestamp = %d, want 1700000300000", got)
	}
}

func TestLoadFixtureRejectsInvalidShape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dataset.yaml")
	if err := os.WriteFile(path, []byte(`name: missing-series`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadFixture(path); err == nil {
		t.Fatal("LoadFixture succeeded for a fixture without series")
	}
}
