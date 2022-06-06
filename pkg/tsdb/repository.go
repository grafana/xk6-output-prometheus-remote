package tsdb

import (
	"errors"
	"sync"
)

var (
	ErrSeriesNotFound      = errors.New("series not found")
	ErrSeriesAlreadyExists = errors.New("series already exists")
)

type Repository interface {
	GetSeries() ([]*TimeSeries, error)
	GetSeriesByID(hash uint64) (*TimeSeries, error)
	InsertSeries(*TimeSeries) error
}

// InMemory is a basic in-memory time series storage.
type InMemory struct {
	rwm sync.RWMutex

	// series is the storage of the available series
	// we expect time series to be reused as much as possible
	// so it should be able to maintain the writes low
	// and instead reading most of the time.
	series map[uint64]*TimeSeries
}

func NewInMemoryRepository() *InMemory {
	return &InMemory{
		series: make(map[uint64]*TimeSeries),
		rwm:    sync.RWMutex{},
	}
}

func (inmem *InMemory) GetSeries() ([]*TimeSeries, error) {
	inmem.rwm.RLock()
	all := make([]*TimeSeries, 0, len(inmem.series))
	for _, s := range inmem.series {
		all = append(all, s)
	}
	inmem.rwm.RUnlock()

	return all, nil
}

func (inmem *InMemory) GetSeriesByID(id uint64) (*TimeSeries, error) {
	inmem.rwm.RLock()
	s, ok := inmem.series[id]
	inmem.rwm.RUnlock()
	if !ok {
		return nil, ErrSeriesNotFound
	}
	return s, nil
}

func (inmem *InMemory) InsertSeries(series *TimeSeries) error {
	inmem.rwm.Lock()
	defer inmem.rwm.Unlock()
	if _, ok := inmem.series[series.ID]; ok {
		return ErrSeriesAlreadyExists
	}

	inmem.series[series.ID] = series
	return nil
}
