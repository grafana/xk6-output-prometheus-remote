package tsdb

import (
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/openhistogram/circonusllhist"
	"go.k6.io/k6/metrics"
)

type Tag struct {
	Key, Value string
}

type TagSet []*Tag

func NewTagSet(ncap int) TagSet {
	return make([]*Tag, 0, ncap)
}

// Add adds a new item to the inner slice in an ordered way.
// Add is a not safe concurrent-operation.
func (set *TagSet) Add(t *Tag) {
	index := sort.Search(len(*set), func(i int) bool {
		return (*set)[i].Key > t.Key
	})

	if len(*set) == index {
		*set = append(*set, t)
		return
	}

	*set = append((*set)[:index], append(make([]*Tag, 1), (*set)[index:]...)...)
	(*set)[index] = t
}

type TagMap struct {
	m sync.Map
}

func (tagmap *TagMap) Get(hash uint64) *Tag {
	t, ok := tagmap.m.Load(hash)
	if !ok {
		return nil
	}
	return t.(*Tag)
}

func (tagmap *TagMap) Set(hash uint64, t *Tag) {
	// TODO: refactor, in this way it's hashing twice
	tagmap.m.Store(hash, t)
}

type TimeSeries struct {
	Sink       Sink
	MetricName string
	Tags       TagSet
	ID         uint64
}

func newTimeSeries(metric string, tags TagSet) *TimeSeries {
	return &TimeSeries{
		MetricName: metric,
		Tags:       tags,
		ID:         HashKey(metric, tags),
	}
}

func (s TimeSeries) AddPoint(v float64) {
	s.Sink.Add(v)
}

type Sink interface {
	Add(v float64)
	Value() float64
}

var bytesep = []byte{0xff}

func HashKey(metric string, tags TagSet) uint64 {
	hasher := xxhash.New()
	hasher.WriteString(metric)
	for i := 0; i < len(tags); i++ {
		hasher.Write(bytesep)
		hasher.WriteString(tags[i].Key)
		hasher.Write(bytesep)
		hasher.WriteString(tags[i].Value)
	}
	h := hasher.Sum64()
	hasher.Reset()
	return h
}

type CountSeries struct {
	rwm   sync.RWMutex
	value float64
}

func NewCountSeries(metric string, tags TagSet) *TimeSeries {
	s := newTimeSeries(metric, tags)
	s.Sink = &CountSeries{}
	return s
}

func (cs *CountSeries) Add(v float64) {
	if v < 0 {
		return
	}
	cs.rwm.Lock()
	cs.value += v
	cs.rwm.Unlock()
}

func (cs *CountSeries) Value() float64 {
	cs.rwm.RLock()
	defer cs.rwm.RUnlock()
	return cs.value
}

type GaugeSeries struct {
	rwm   sync.RWMutex
	value float64
}

func NewGaugeSeries(metric string, tags TagSet) *TimeSeries {
	s := newTimeSeries(metric, tags)
	s.Sink = &GaugeSeries{}
	return s
}

func (gs *GaugeSeries) Add(v float64) {
	gs.rwm.Lock()
	gs.value = v
	gs.rwm.Unlock()
}

func (gs *GaugeSeries) Value() float64 {
	gs.rwm.RLock()
	defer gs.rwm.RUnlock()
	return gs.value
}

type RateSeries struct {
	inner *metrics.RateSink
	rwm   sync.RWMutex
}

func NewRateSeries(metric string, tags TagSet) *TimeSeries {
	s := newTimeSeries(metric, tags)
	s.Sink = &RateSeries{
		inner: &metrics.RateSink{},
	}
	return s
}

func (rs *RateSeries) Add(v float64) {
	rs.rwm.Lock()
	rs.inner.Add(metrics.Sample{Value: v})
	rs.rwm.Unlock()
}

func (rs *RateSeries) Value() float64 {
	rs.rwm.RLock()
	defer rs.rwm.RUnlock()
	return float64(rs.inner.Trues) / float64(rs.inner.Total)
}

type TrendSeries struct {
	rwm sync.RWMutex

	histogram *circonusllhist.Histogram
	count     int
	sum       float64
	avg       float64
	max, min  float64
}

func NewTrendSeries(metric string, tags TagSet) *TimeSeries {
	s := newTimeSeries(metric, tags)
	s.Sink = &TrendSeries{
		histogram: circonusllhist.New(),
	}
	return s
}

func (t *TrendSeries) Add(v float64) {
	t.rwm.Lock()
	t.histogram.RecordValue(v)
	t.count += 1
	t.sum += v
	t.avg = t.sum / float64(t.count)
	if v > t.max {
		t.max = v
	}
	if t.count == 1 || v < t.min {
		t.min = v
	}
	t.rwm.Unlock()
}

func (t *TrendSeries) Value() float64 {
	// TODO: it's the  main p used by k6, maybe switch to 0.99
	return t.P(0.95)
}

func (t *TrendSeries) onRLock(f func() float64) float64 {
	t.rwm.RLock()
	v := f()
	t.rwm.RUnlock()
	return v
}

func (t *TrendSeries) Min() float64 { return t.onRLock(func() float64 { return t.min }) }
func (t *TrendSeries) Max() float64 { return t.onRLock(func() float64 { return t.max }) }
func (t *TrendSeries) Avg() float64 { return t.onRLock(func() float64 { return t.avg }) }
func (t *TrendSeries) Med() float64 { return t.P(0.5) }

func (t *TrendSeries) P(pct float64) float64 {
	return t.onRLock(func() float64 { return t.histogram.ValueAtQuantile(pct) })
}
