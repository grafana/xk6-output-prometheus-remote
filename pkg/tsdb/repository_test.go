package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryGetSeries(t *testing.T) {
	r := NewInMemoryRepository()
	series, err := r.GetSeries()
	require.NoError(t, err)
	assert.Empty(t, series)

	// TODO: decoupling, they have direct dependency in this way
	// a bug on one side could impact the other side
	// or the bug could be hidden
	err = r.InsertSeries(NewCountSeries("metric-name", nil))
	require.NoError(t, err)

	series, err = r.GetSeries()
	require.NoError(t, err)
	assert.Len(t, series, 1)
}

func TestRepositoryGetSeriesByID(t *testing.T) {
	r := NewInMemoryRepository()
	counterSeries := NewCountSeries("metric-name", nil)

	series, err := r.GetSeriesByID(counterSeries.ID)
	assert.Equal(t, ErrSeriesNotFound, err)
	assert.Nil(t, series)

	err = r.InsertSeries(counterSeries)
	require.NoError(t, err)

	series, err = r.GetSeriesByID(counterSeries.ID)
	require.NoError(t, err)
	assert.Equal(t, "metric-name", series.MetricName)
}

func TestRepositoryInsertSeries(t *testing.T) {
	tags := []*Tag{
		{Key: "tag1", Value: "value1"},
		{Key: "tag2", Value: "value2"},
	}
	counterSeries := NewCountSeries("metric-name", tags)

	r := NewInMemoryRepository()
	err := r.InsertSeries(counterSeries)
	require.NoError(t, err)

	err = r.InsertSeries(counterSeries)
	assert.Equal(t, ErrSeriesAlreadyExists, err)

	counterSeries2 := NewCountSeries("metric-name", tags)
	err = r.InsertSeries(counterSeries2)
	assert.Equal(t, ErrSeriesAlreadyExists, err)

	series, err := r.GetSeries()
	require.NoError(t, err)
	assert.Len(t, series, 1)
}
