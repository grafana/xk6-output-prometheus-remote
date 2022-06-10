package remotewrite

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"time"

	"github.com/grafana/xk6-output-prometheus-remote/pkg/tsdb"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

type Output struct {
	output.SampleBuffer

	config            Config
	logger            logrus.FieldLogger
	client            remote.WriteClient
	tsdb              tsdb.Repository
	tagmap            tsdb.TagMap
	periodicCollector *output.PeriodicFlusher
	periodicFlusher   *output.PeriodicFlusher
}

var _ output.Output = new(Output)

func New(params output.Params) (*Output, error) {
	logger := params.Logger.WithFields(logrus.Fields{"output": "Prometheus Remote-Write"})
	config, err := GetConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return nil, err
	}

	remoteConfig, err := config.ConstructRemoteConfig()
	if err != nil {
		return nil, err
	}

	// name is used to differentiate clients in metrics
	client, err := remote.NewWriteClient("xk6-prwo", remoteConfig)
	if err != nil {
		return nil, err
	}

	return &Output{
		client: client,
		config: config,
		logger: logger,
		tsdb:   tsdb.NewInMemoryRepository(),
	}, nil
}

func (*Output) Description() string {
	return "Output k6 metrics to Prometheus remote-write endpoint"
}

func (o *Output) Start() error {
	collector, err := output.NewPeriodicFlusher(time.Duration(o.config.FlushPeriod.Duration), o.collect)
	if err != nil {
		return err
	}
	o.periodicCollector = collector

	flusher, err := output.NewPeriodicFlusher(time.Duration(o.config.FlushPeriod.Duration), o.flushSeries)
	if err != nil {
		return err
	}
	o.periodicFlusher = flusher

	o.logger.Debug("Periodic collector and series flusher have started")
	return nil
}

func (o *Output) Stop() error {
	o.logger.Debug("Stopping the output")
	o.periodicCollector.Stop()
	o.periodicFlusher.Stop()

	series, err := o.tsdb.GetSeries()
	if err != nil {
		return err
	}
	o.logger.WithField("unique-series-generated", len(series)).Debug("Output stopped")

	// TODO: remove before merge
	f, err := os.CreateTemp("/home/codebien/code/grafana/xk6-output-prometheus-remote", "k6-")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var line string
	for _, s := range series {
		line = fmt.Sprintf("%s{%+v} %f", s.MetricName, func() map[string]string {
			m := make(map[string]string)
			for _, t := range s.Tags {
				m[t.Key] = t.Value
			}
			return m
		}(), s.Sink.Value())
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func (o *Output) collect() {
	samplesContainers := o.GetBufferedSamples()

	if len(samplesContainers) < 1 {
		return
	}

	// Remote write endpoint accepts TimeSeries structure defined in gRPC. It must:
	// a) contain Labels array
	// b) have a __name__ label: without it, metric might be unquerable or even rejected
	// as a metric without a name. This behaviour depends on underlying storage used.
	// c) not have duplicate timestamps within 1 timeseries, see https://github.com/prometheus/prometheus/issues/9210
	// Prometheus write handler processes only some fields as of now, so here we'll add only them.

	series, err := o.sinkSeriesFromSamples(samplesContainers)
	if err != nil {
		o.logger.WithError(err).Error("Failed to convert the samples as time series")
		return
	}

	o.logger.WithField("series", series).Debug("Converting samples to time series and sink them")

	// TODO: sink here the series?
}

func (o *Output) flushSeries() {
	var (
		start = time.Now()
		nts   int
	)

	series, err := o.tsdb.GetSeries()
	if err != nil {
		o.logger.WithError(err).Error("Fetching time series")
		return
	}

	if len(series) < 1 {
		o.logger.Debug("Skipping the flush operation, any time series found")
		return
	}

	nts = len(series)

	defer func() {
		d := time.Since(start)
		if d > time.Duration(o.config.FlushPeriod.Duration) {
			// There is no intermediary storage so warn if writing to remote write endpoint becomes too slow
			o.logger.WithField("nts", nts).
				Warn(fmt.Sprintf("Remote write took %s while flush period is %s. Some samples may be dropped.",
					d.String(), o.config.FlushPeriod.String()))
		} else {
			o.logger.WithField("nts", nts).Debug(fmt.Sprintf("Remote write took %s.", d.String()))
		}
	}()

	o.logger.WithField("nts", nts).Debug("Preparing time series for flushing")

	// TODO: maybe some cache?
	var promSeries []prompb.TimeSeries
	for _, s := range series {
		promSeries = append(promSeries, o.formatSeriesAsProm(s)...)
	}

	req := prompb.WriteRequest{
		Timeseries: promSeries,
	}

	buf, err := proto.Marshal(&req)
	if err != nil {
		o.logger.WithError(err).Fatal("Failed to marshal timeseries")
		return
	}

	// TODO: this call can panic, find the source and fix it
	encoded := snappy.Encode(nil, buf)
	if err := o.client.Store(context.Background(), encoded); err != nil {
		o.logger.WithError(err).Error("Failed to store timeseries")
		return
	}
}

// TODO: cache the prom time series (name + tags)

func (o *Output) formatSeriesAsProm(series *tsdb.TimeSeries) []prompb.TimeSeries {
	labels := tagsToLabels(o.config, series.Tags)

	switch styp := series.Sink.(type) {
	case *tsdb.CountSeries, *tsdb.GaugeSeries, *tsdb.RateSeries:
		return []prompb.TimeSeries{
			{
				Labels: appendNameLabel(labels, series.MetricName),
				Samples: []prompb.Sample{
					{
						Value:     series.Sink.Value(),
						Timestamp: timestamp.FromTime(time.Now()),
					},
				},
			},
		}

	case *tsdb.TrendSeries:
		return MapTrend(series.MetricName, styp, labels)
	default:
		panic("the time series type is not supported")
	}
}

func (o *Output) sinkSeriesFromSamples(samplesContainers []metrics.SampleContainer) (uint, error) {
	sinked := make(map[uint64]*tsdb.TimeSeries)

	// TODO: aggregate same time series across containers (maybe caching?)
	// useless to fetch and update for each single sample
	for _, samplesContainer := range samplesContainers {
		samples := samplesContainer.GetSamples()

		for _, sample := range samples {
			tset, err := o.tagSetFromTags(sample.Tags.CloneTags())
			if err != nil {
				return 0, err
			}

			id := tsdb.HashKey(sample.Metric.Name, tset)
			series, ok := sinked[id]
			if ok {
				series.AddPoint(sample.Value)
				continue
			}
			series, err = o.tsdb.GetSeriesByID(id)
			if err != nil {
				if !errors.Is(err, tsdb.ErrSeriesNotFound) {
					return 0, err
				}

				// TODO: avoid to re-hash
				series = o.k6MetricToSeries(sample.Metric, tset)
				err = o.tsdb.InsertSeries(series)
				if err != nil {
					return 0, err
				}
			}

			series.AddPoint(sample.Value)
			sinked[id] = series
		}
	}
	return uint(len(sinked)), nil
}

var tagHasher maphash.Hash

func (o *Output) tagSetFromTags(tags map[string]string) (tsdb.TagSet, error) {
	tset := tsdb.NewTagSet(len(tags))
	for k, v := range tags {
		// TODO: define a better separator
		tagHasher.WriteString(k + "_" + v)
		h := tagHasher.Sum64()
		tagHasher.Reset()

		tag := o.tagmap.Get(h)
		if tag == nil {
			tag = &tsdb.Tag{Key: k, Value: v}
			o.tagmap.Set(h, tag)
		}
		tset.Add(tag)
	}
	return tset, nil
}

// TODO: test in case of sub-metric
func (o *Output) k6MetricToSeries(m *metrics.Metric, tset tsdb.TagSet) *tsdb.TimeSeries {
	switch m.Type {
	case metrics.Counter:
		return tsdb.NewCountSeries(m.Name, tset)
	case metrics.Gauge:
		return tsdb.NewGaugeSeries(m.Name, tset)
	case metrics.Trend:
		return tsdb.NewTrendSeries(m.Name, tset)
	case metrics.Rate:
		return tsdb.NewRateSeries(m.Name, tset)
	default:
		panic("the metric type is not supported")
	}
}
