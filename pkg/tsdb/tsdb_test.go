package tsdb_test

import (
	"math"
	"testing"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagSetAdd(t *testing.T) {
	tags := map[string]string{
		"group":             "",
		"method":            "GET",
		"expected_response": "true",
	}
	set := tsdb.NewTagSet(len(tags))
	for k, v := range tags {
		set.Add(&tsdb.Tag{Key: k, Value: v})
	}

	exp := []string{
		"expected_response",
		"group",
		"method",
	}
	keys := func() []string {
		s := []string{}
		for _, t := range set {
			s = append(s, t.Key)
		}
		return s
	}()
	assert.Equal(t, exp, keys)
}

func TestHashKey(t *testing.T) {
	tags := map[string]string{
		"expected_response": "true", "group": "",
		"method": "GET", "name": "https://test.k6.io",
		"proto": "HTTP/1.1", "scenario": "default",
		"status": "200", "tls_version": "tls1.3", "url": "https://test.k6.io",
	}

	var tagset tsdb.TagSet
	for k, v := range tags {
		tagset = append(tagset, &tsdb.Tag{Key: k, Value: v})
	}

	key := tsdb.HashKey("http_reqs", tagset)
	twice := tsdb.HashKey("http_reqs", tagset)
	assert.Equal(t, key, twice)
}

func TestCounterSeriesAddPoint(t *testing.T) {
	cs := tsdb.NewCountSeries("test", nil)
	cs.AddPoint(5.12)
	cs.AddPoint(42.1394)
	cs.AddPoint(-1) // it's discarded
	cs.AddPoint(64.8524)
	assert.Equal(t, 112.1118, cs.Sink.(*tsdb.CountSeries).Value())
}

func TestGaugeSeriesAddPoint(t *testing.T) {
	gs := tsdb.NewGaugeSeries("test", nil)
	gs.AddPoint(1)
	gs.AddPoint(1)
	gs.AddPoint(-5)
	assert.Equal(t, float64(-5), gs.Sink.(*tsdb.GaugeSeries).Value())
}

func TestRateSeriesAddPoint(t *testing.T) {
	rs := tsdb.NewRateSeries("test", nil)
	rs.AddPoint(1)
	rs.AddPoint(0)
	rs.AddPoint(7)

	rate, ok := rs.Sink.(*tsdb.RateSeries)
	require.True(t, ok)

	assert.Equal(t, float64(0.667), math.Round(rate.Value()*1000)/1000)
}

func TestTrendSeriesAddPoint(t *testing.T) {
	ts := tsdb.NewTrendSeries("test", nil)
	ts.AddPoint(3.14)
	ts.AddPoint(2.718)
	ts.AddPoint(1.618)

	trend, ok := ts.Sink.(*tsdb.TrendSeries)
	require.True(t, ok)

	assert.Equal(t, float64(1.618), trend.Min())
	assert.Equal(t, float64(3.14), trend.Max())
	assert.Equal(t, float64(2.492), math.Round(trend.Avg()*1000)/1000)
	assert.Equal(t, 2.75, trend.Med())    // 2.718
	assert.Equal(t, 3.17, trend.P(0.90))  // 3.2244
	assert.Equal(t, 3.185, trend.P(0.95)) // 3.2877
	assert.Equal(t, 3.197, trend.P(0.99)) // 3.2877
}
