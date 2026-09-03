package runner

import (
	"context"
	"fmt"
	"net/http"
	"time"

	client "github.com/prometheus/client_golang/api"
	clientv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// HTTPQueryTarget adapts any Prometheus-compatible HTTP query endpoint to the
// runner's small QueryAPI seam.
type HTTPQueryTarget struct {
	api clientv1.API
}

func NewHTTPQueryTarget(url string) (*HTTPQueryTarget, error) {
	if url == "" {
		return nil, fmt.Errorf("query target URL is empty")
	}
	apiClient, err := client.NewClient(client.Config{
		Address:      url,
		RoundTripper: http.DefaultTransport,
	})
	if err != nil {
		return nil, fmt.Errorf("create query target %q: %w", url, err)
	}
	return &HTTPQueryTarget{api: clientv1.NewAPI(apiClient)}, nil
}

func (t *HTTPQueryTarget) Query(ctx context.Context, query string, timestamp time.Time, options ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return t.api.Query(ctx, query, timestamp, options...)
}

func (t *HTTPQueryTarget) QueryRange(ctx context.Context, query string, interval clientv1.Range, options ...clientv1.Option) (model.Value, clientv1.Warnings, error) {
	return t.api.QueryRange(ctx, query, interval, options...)
}
