package seeder

// This file defines the seeder's one fixed, hand-authored dataset.
//
// # Shape
//
// The dataset spans a 20-minute window sampled every 60s: 21 timestamps at
// offsets (in seconds from a base time chosen at seed time, see BaseTimeMs
// in push.go) of 0, 60, 120, ..., 1200.
//
// It contains three metrics, six series total:
//
//  1. http_requests_total{host="a"|"b"}  — a counter-like metric (strictly
//     increasing), present for the whole window, to exercise rate()/increase().
//  2. node_memory_used_bytes{host="a"|"b"} — a gauge-like metric, present for
//     the whole window, to exercise min/max/avg_over_time().
//  3. checkout_up{service="checkout", region="us-east"|"us-west"} — a series
//     whose label set changes partway through the window: the us-east series
//     only has samples in the first 5 minutes, the us-west series only has
//     samples in the last 5 minutes, with a >5m silent gap in between. This
//     is deliberately built to exercise instant-vs-range divergence bugs
//     (see #589/#583/#584): an instant query evaluated inside the gap must
//     see an empty result (both series are stale/not-yet-started under the
//     default 5m lookback), while a range query covering the same window
//     returns raw matrix samples for both series.
//
// # Hand-computed expected values
//
// Let base = the base time (ms) the seeder used for this run (printed by
// cmd/seed on push). All timestamps below are "base + offset seconds".
//
//   - http_requests_total{host="a"}: value(offset) = offset (seconds).
//     So value(0)=0, value(600)=600, value(1200)=1200.
//     rate(http_requests_total{host="a"}[5m]) at any t in [base+300, base+1200]
//     = 1.0 exactly (1 unit/second).
//     increase(http_requests_total{host="a"}[5m]) at those same t = 300.
//
//   - http_requests_total{host="b"}: value(offset) = 1000 + 2*offset.
//     value(0)=1000, value(600)=2200, value(1200)=3400.
//     rate(http_requests_total{host="b"}[5m]) at any t in [base+300, base+1200]
//     = 2.0 exactly.
//
//   - sum(rate(http_requests_total[5m])) at t=base+600 = 1.0 + 2.0 = 3.0.
//
//   - node_memory_used_bytes{host="a"}: a triangle wave. Rises from 500 to
//     1000 in steps of 50 over offsets 0..600 (11 points), then falls back
//     from 950 to 500 in steps of 50 over offsets 660..1200 (10 points).
//     max_over_time(node_memory_used_bytes{host="a"}[20m]) at t=base+1200 = 1000.
//     min_over_time(node_memory_used_bytes{host="a"}[20m]) at t=base+1200 = 500.
//     Instant value at t=base+600 = 1000 (the peak).
//
//   - node_memory_used_bytes{host="b"}: flat 2000 for every sample.
//     avg_over_time(node_memory_used_bytes{host="b"}[20m]) = 2000 exactly.
//
//   - sum(node_memory_used_bytes) at t=base+600 (instant) = 1000 + 2000 = 3000.
//
//   - checkout_up{service="checkout",region="us-east"}: value=1 at offsets
//     0,60,120,180,240,300, then no more samples.
//     checkout_up{service="checkout",region="us-west"}: value=1 at offsets
//     900,960,1020,1080,1140,1200, no samples before that.
//     At t=base+660 (360s after the last us-east sample, i.e. > the 5m
//     default lookback, and 240s before the first us-west sample): an
//     instant query for checkout_up{service="checkout"} must return an
//     EMPTY result vector (both series are absent/stale). A range query
//     for checkout_up{service="checkout"}[20m] evaluated at t=base+1200
//     must return a matrix with two series: us-east with 6 samples
//     (offsets 0..300) and us-west with 6 samples (offsets 900..1200).
//     This is the instant-vs-range divergence case #594 is meant to catch.
//
// Point counts: http_requests_total and node_memory_used_bytes each have 21
// samples per series (offsets 0,60,...,1200). checkout_up has 6 samples per
// series (12 total), deliberately sparse and non-overlapping in time.

// Dataset returns the fixed set of series pushed by the seeder. It is a
// plain Go literal (built with small loops below for the repetitive parts)
// rather than data read from a file, so the values above are exactly what
// gets pushed — no external format to keep in sync.
func Dataset() []SeriesDef {
	offsets := make([]int64, 0, 21)
	for o := int64(0); o <= 1200; o += 60 {
		offsets = append(offsets, o)
	}

	httpRequestsA := SeriesDef{
		Name:   "http_requests_total",
		Labels: map[string]string{"host": "a"},
	}
	httpRequestsB := SeriesDef{
		Name:   "http_requests_total",
		Labels: map[string]string{"host": "b"},
	}
	for _, o := range offsets {
		httpRequestsA.Samples = append(httpRequestsA.Samples, Sample{
			OffsetSeconds: o,
			Value:         float64(o), // 1 unit/sec
		})
		httpRequestsB.Samples = append(httpRequestsB.Samples, Sample{
			OffsetSeconds: o,
			Value:         1000 + 2*float64(o), // 2 units/sec
		})
	}

	memA := SeriesDef{
		Name:   "node_memory_used_bytes",
		Labels: map[string]string{"host": "a"},
	}
	memB := SeriesDef{
		Name:   "node_memory_used_bytes",
		Labels: map[string]string{"host": "b"},
	}
	for _, o := range offsets {
		var v float64
		switch {
		case o <= 600:
			// Rising leg: 500 at o=0 up to 1000 at o=600, step 50 per 60s.
			v = 500 + 50*float64(o/60)
		default:
			// Falling leg: 950 at o=660 down to 500 at o=1200, step 50 per 60s.
			stepsPastPeak := (o - 600) / 60
			v = 1000 - 50*float64(stepsPastPeak)
		}
		memA.Samples = append(memA.Samples, Sample{OffsetSeconds: o, Value: v})
		memB.Samples = append(memB.Samples, Sample{OffsetSeconds: o, Value: 2000})
	}

	checkoutUpEast := SeriesDef{
		Name:   "checkout_up",
		Labels: map[string]string{"service": "checkout", "region": "us-east"},
	}
	for o := int64(0); o <= 300; o += 60 {
		checkoutUpEast.Samples = append(checkoutUpEast.Samples, Sample{OffsetSeconds: o, Value: 1})
	}

	checkoutUpWest := SeriesDef{
		Name:   "checkout_up",
		Labels: map[string]string{"service": "checkout", "region": "us-west"},
	}
	for o := int64(900); o <= 1200; o += 60 {
		checkoutUpWest.Samples = append(checkoutUpWest.Samples, Sample{OffsetSeconds: o, Value: 1})
	}

	return []SeriesDef{
		httpRequestsA,
		httpRequestsB,
		memA,
		memB,
		checkoutUpEast,
		checkoutUpWest,
	}
}
