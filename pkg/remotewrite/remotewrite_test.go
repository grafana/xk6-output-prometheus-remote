package remotewrite

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
)

func TestOutputConvertToPbSeries(t *testing.T) {
	registry := metrics.NewRegistry()
	metric1 := registry.MustNewMetric("metric1", metrics.Counter)
	tagset := metrics.NewSampleTags(map[string]string{"tagk1": "tagv1"})

	samples := []metrics.SampleContainer{
		metrics.Sample{
			Metric: metric1,
			Tags:   tagset,
			Time:   time.Date(2022, time.September, 1, 0, 0, 0, 0, time.UTC),
			Value:  3,
		},
		metrics.Sample{
			Metric: metric1,
			Tags:   tagset,
			Time:   time.Date(2022, time.August, 31, 0, 0, 0, 0, time.UTC),
			Value:  4,
		},
		metrics.Sample{
			Metric: registry.MustNewMetric("metric2", metrics.Counter),
			Tags:   tagset,
			Time:   time.Date(2022, time.September, 1, 0, 0, 0, 0, time.UTC),
			Value:  2,
		},
	}

	o := Output{
		tsdb: make(map[string]*seriesWithMeasure),
	}

	pbseries := o.convertToPbSeries(samples)
	require.Len(t, pbseries, 2)
	require.Len(t, o.tsdb, 2)

	unix1sept := int64(1661990400 * 1000) // in ms
	exp := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "tagk1", Value: "tagv1"},
				{Name: "__name__", Value: "k6_metric1"},
			},
			Samples: []prompb.Sample{
				{Value: 7, Timestamp: unix1sept},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: "tagk1", Value: "tagv1"},
				{Name: "__name__", Value: "k6_metric2"},
			},
			Samples: []prompb.Sample{
				{Value: 2, Timestamp: unix1sept},
			},
		},
	}

	assert.Equal(t, exp, pbseries)
}

func TestTimeSeriesKey(t *testing.T) {
	registry := metrics.NewRegistry()
	metric1 := registry.MustNewMetric("metric1", metrics.Counter)

	tagsmap := make(map[string]string)
	for i := 0; i < 8; i++ {
		is := strconv.Itoa(i)
		tagsmap["tagk"+is] = "tagv" + is
	}
	tagset := metrics.NewSampleTags(tagsmap)

	key := timeSeriesKey(metric1, tagset)

	expected := "metric1"
	sbytesep := string(bytesep)
	for i := 0; i < 8; i++ {
		is := strconv.Itoa(i)
		expected += sbytesep + "tagk" + is + sbytesep + "tagv" + is
	}

	assert.Equal(t, expected, key)
}
