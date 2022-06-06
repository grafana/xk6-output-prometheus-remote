package remotewrite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/lib/testutils"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

func TestOutputCollect(t *testing.T) {
	o, err := New(output.Params{
		Logger: testutils.NewLogger(t),
	})
	require.NoError(t, err)

	o.tsdb = tsdb.NewInMemoryRepository()
	metric := &metrics.Metric{
		Name: "myMetric",
		Type: metrics.Counter,
	}
	o.AddMetricSamples([]metrics.SampleContainer{
		metrics.Samples([]metrics.Sample{
			{
				Metric: metric,
				Time:   time.Now(),
				Tags:   metrics.NewSampleTags(map[string]string{"key1": "val1"}),
				Value:  3.14,
			},
		}),
	})
	o.AddMetricSamples([]metrics.SampleContainer{
		metrics.Samples([]metrics.Sample{
			{
				Metric: metric,
				Time:   time.Now(),
				Tags:   metrics.NewSampleTags(map[string]string{"key1": "val1"}),
				Value:  2.71,
			},
			{
				Metric: metric,
				Time:   time.Now(),
				Tags:   metrics.NewSampleTags(map[string]string{"key1": "val2"}),
				Value:  1.61,
			},
		}),
	})

	o.collect()

	serieses, err := o.tsdb.GetSeries()
	require.NoError(t, err)
	require.Len(t, serieses, 2)

	series, err := o.tsdb.GetSeriesByID(tsdb.HashKey(
		"myMetric",
		tsdb.TagSet{&tsdb.Tag{Key: "key1", Value: "val1"}},
	))
	require.NoError(t, err)

	cs, ok := series.Sink.(*tsdb.CountSeries)
	require.True(t, ok)

	assert.Equal(t, cs.Value(), 5.85)
}

type remoteWriteMock struct{}

// Store stores the given samples in the remote storage.
func (rwm remoteWriteMock) Store(_ context.Context, b []byte) error {
	if len(b) < 1 {
		return fmt.Errorf("received an empty body")
	}
	return nil
}

// Name uniquely identifies the remote storage.
func (rwm remoteWriteMock) Name() string {
	panic("Name not implemented")
}

// Endpoint is the remote read or write endpoint for the storage client.
func (rwm remoteWriteMock) Endpoint() string {
	panic("Endpoint not implemented")
}

func TestOutputFlushSeries(t *testing.T) {
	o, err := New(output.Params{
		Logger: testutils.NewLogger(t),
	})
	require.NoError(t, err)

	o.client = remoteWriteMock{}

	o.tsdb = tsdb.NewInMemoryRepository()
	metric := &metrics.Metric{
		Name: "myMetric",
		Type: metrics.Counter,
	}
	series := tsdb.NewCountSeries(metric.Name, tsdb.TagSet{{Key: "tag1", Value: "val1"}})
	require.NoError(t, o.tsdb.InsertSeries(series))

	series.AddPoint(42)
	o.flushSeries()
}
