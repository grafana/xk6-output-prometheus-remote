package remotewrite

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"go.k6.io/k6/metrics"
)

func MapTagSet(t *metrics.SampleTags) []prompb.Label {
	tags := t.CloneTags()

	labels := make([]prompb.Label, 0, len(tags))
	for k, v := range tags {
		labels = append(labels, prompb.Label{Name: k, Value: v})
	}
	return labels
}

func MapSeries(ts TimeSeries) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: append(MapTagSet(ts.Tags), prompb.Label{
			Name:  "__name__",
			Value: fmt.Sprintf("%s%s", defaultMetricPrefix, ts.Metric.Name),
		}),
	}
}

func MapTrend(series TimeSeries, t time.Time, sink *metrics.TrendSink) []prompb.TimeSeries {
	// Prometheus metric system does not support Trend so this mapping will
	// store a counter for the number of reported values and gauges to keep
	// track of aggregated values. Also store a sum of the values to allow
	// the calculation of moving averages.
	// TODO: when Prometheus implements support for sparse histograms, re-visit this implementation

	labels := MapTagSet(series.Tags)
	timestamp := timestamp.FromTime(t)

	return []prompb.TimeSeries{
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_count", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     float64(sink.Count),
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_sum", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.Sum,
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_min", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.Min,
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_max", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.Max,
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_avg", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.Avg,
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_med", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.Med,
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_p90", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.P(0.9),
					Timestamp: timestamp,
				},
			},
		},
		{
			Labels: append(labels, prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s_p95", defaultMetricPrefix, series.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{
					Value:     sink.P(0.95),
					Timestamp: timestamp,
				},
			},
		},
	}
}

// The following functions are an attempt to add ad-hoc optimization to TrendSink,
// and are a partial copy-paste from k6/metrics.
// TODO: re-write & refactor this once metrics refactoring progresses in k6.

func trendAdd(t *metrics.TrendSink, s metrics.Sample) {
	// insert into sorted array instead of sorting anew on each addition
	index := sort.Search(len(t.Values), func(i int) bool {
		return t.Values[i] > s.Value
	})
	t.Values = append(t.Values, 0)
	copy(t.Values[index+1:], t.Values[index:])
	t.Values[index] = s.Value

	t.Count += 1
	t.Sum += s.Value
	t.Avg = t.Sum / float64(t.Count)

	if s.Value > t.Max {
		t.Max = s.Value
	}
	if s.Value < t.Min || t.Count == 1 {
		t.Min = s.Value
	}

	if (t.Count & 0x01) == 0 {
		t.Med = (t.Values[(t.Count/2)-1] + t.Values[(t.Count/2)]) / 2
	} else {
		t.Med = t.Values[t.Count/2]
	}
}

func p(t *metrics.TrendSink, pct float64) float64 {
	switch t.Count {
	case 0:
		return 0
	case 1:
		return t.Values[0]
	default:
		// If percentile falls on a value in Values slice, we return that value.
		// If percentile does not fall on a value in Values slice, we calculate (linear interpolation)
		// the value that would fall at percentile, given the values above and below that percentile.
		i := pct * (float64(t.Count) - 1.0)
		j := t.Values[int(math.Floor(i))]
		k := t.Values[int(math.Ceil(i))]
		f := i - math.Floor(i)
		return j + (k-j)*f
	}
}
