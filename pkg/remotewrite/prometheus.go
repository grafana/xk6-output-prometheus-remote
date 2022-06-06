package remotewrite

import (
	"fmt"
	"time"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/tsdb"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
)

func tagsToLabels(config Config, tags tsdb.TagSet) []prompb.Label {
	if !config.KeepTags.Bool {
		return []prompb.Label{}
	}

	// Adding one more because with high probability
	// a __name__ label will be added after
	labels := make([]prompb.Label, 0, 1+len(tags))

	for _, tag := range tags {
		if len(tag.Key) < 1 || len(tag.Value) < 1 {
			continue
		}

		if !config.KeepNameTag.Bool && tag.Key == "name" {
			continue
		}

		if !config.KeepURLTag.Bool && tag.Key == "url" {
			continue
		}

		labels = append(labels, prompb.Label{
			Name:  tag.Key,
			Value: tag.Value,
		})
	}

	return labels
}

func appendNameLabel(labels []prompb.Label, name string) []prompb.Label {
	return append(labels, prompb.Label{
		Name:  "__name__",
		Value: fmt.Sprintf("%s%s", defaultMetricPrefix, name),
	})
}

func MapTrend(metricName string, s *tsdb.TrendSeries, labels []prompb.Label) []prompb.TimeSeries {
	// TODO: asssign in-place
	aggr := map[string]float64{
		"min":   s.Min(),
		"max":   s.Max(),
		"avg":   s.Avg(),
		"med":   s.Med(),
		"p(90)": s.P(0.90),
		"p(95)": s.P(0.95),
	}

	ts := time.Now()

	// Prometheus metric system does not support Trend so this mapping will store gauges
	// to keep track of key values.
	// TODO: when Prometheus implements support for sparse histograms, re-visit this implementation

	return []prompb.TimeSeries{
		{
			Labels: appendNameLabel(labels, metricName+"_min"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["min"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
		{
			Labels: appendNameLabel(labels, metricName+"_max"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["max"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
		{
			Labels: appendNameLabel(labels, metricName+"_avg"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["avg"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
		{
			Labels: appendNameLabel(labels, metricName+"_med"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["med"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
		{
			Labels: appendNameLabel(labels, metricName+"_p90"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["p(90)"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
		{
			Labels: appendNameLabel(labels, metricName+"_p95"),
			Samples: []prompb.Sample{
				{
					Value:     aggr["p(95)"],
					Timestamp: timestamp.FromTime(ts),
				},
			},
		},
	}
}
