package remotewrite

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
)

var _ output.Output = new(Output)

type Output struct {
	output.SampleBuffer

	config          Config
	logger          logrus.FieldLogger
	periodicFlusher *output.PeriodicFlusher

	client remote.WriteClient
	tsdb   map[string]*seriesWithMeasure
}

func New(params output.Params) (*Output, error) {
	logger := params.Logger.WithFields(logrus.Fields{"output": "Prometheus remote write"})

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
		tsdb:   make(map[string]*seriesWithMeasure),
	}, nil
}

func (*Output) Description() string {
	return "Prometheus remote write"
}

func (o *Output) Start() error {
	d := o.config.FlushPeriod.TimeDuration()
	periodicFlusher, err := output.NewPeriodicFlusher(d, o.flush)
	if err != nil {
		return err
	}
	o.periodicFlusher = periodicFlusher
	o.logger.WithField("flushtime", d).Debug("Output initialized")
	return nil
}

func (o *Output) Stop() error {
	o.logger.Debug("Stopping the output")
	o.periodicFlusher.Stop()
	o.logger.Debug("Output stopped")
	return nil
}

func (o *Output) flush() {
	var (
		start = time.Now()
		nts   int
	)

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

	samplesContainers := o.GetBufferedSamples()
	if len(samplesContainers) < 1 {
		o.logger.Debug("no buffered samples, skip the flushing operation")
		return
	}

	// Remote write endpoint accepts TimeSeries structure defined in gRPC. It must:
	// a) contain Labels array
	// b) have a __name__ label: without it, metric might be unquerable or even rejected
	// as a metric without a name. This behaviour depends on underlying storage used.
	// c) not have duplicate timestamps within 1 timeseries, see https://github.com/prometheus/prometheus/issues/9210
	// Prometheus write handler processes only some fields as of now, so here we'll add only them.

	promTimeSeries := o.convertToPbSeries(samplesContainers)
	nts = len(promTimeSeries)
	o.logger.WithField("nts", nts).Debug("Converted samples to Prometheus TimeSeries")

	buf, err := proto.Marshal(&prompb.WriteRequest{
		Timeseries: promTimeSeries,
	})
	if err != nil {
		o.logger.WithError(err).Fatal("Failed to encode time series as a Protobuf request")
		return
	}

	encoded := snappy.Encode(nil, buf) // TODO: this call can panic
	if err := o.client.Store(context.Background(), encoded); err != nil {
		o.logger.WithError(err).Error("Failed to send the time series data to the endpoint")
		return
	}
}

func (o *Output) convertToPbSeries(samplesContainers []metrics.SampleContainer) []prompb.TimeSeries {
	seen := make(map[string]struct{})

	for _, samplesContainer := range samplesContainers {
		samples := samplesContainer.GetSamples()

		for _, sample := range samples {
			timeSeriesKey := timeSeriesKey(sample.Metric, sample.Tags)
			swm, ok := o.tsdb[timeSeriesKey]
			if !ok {
				swm = &seriesWithMeasure{
					TimeSeries: TimeSeries{
						Metric: sample.Metric,
						Tags:   sample.Tags,
					},
					Measure: sinkByType(sample.Metric.Type),
					Latest:  sample.Time,
				}
				o.tsdb[timeSeriesKey] = swm
			}
			swm.Measure.Add(sample)
			if sample.Time.After(swm.Latest) {
				swm.Latest = sample.Time
			}
			seen[timeSeriesKey] = struct{}{}
		}
	}

	pbseries := make([]prompb.TimeSeries, 0, len(seen))
	now := time.Now()
	for s := range seen {
		pbseries = append(pbseries, o.tsdb[s].MapPrompb(now)...)
	}
	return pbseries
}

type seriesWithMeasure struct {
	Measure metrics.Sink
	Latest  time.Time

	// TImeSeries will be replaced with the native k6 version
	// when it will be available.
	TimeSeries

	// TODO: maybe add some caching for the mapping?
}

func (swm seriesWithMeasure) MapPrompb(t time.Time) []prompb.TimeSeries {
	var newts []prompb.TimeSeries

	mapMonoSeries := func(s TimeSeries, t time.Time) prompb.TimeSeries {
		return prompb.TimeSeries{
			Labels: append(MapTagSet(swm.Tags), prompb.Label{
				Name:  "__name__",
				Value: fmt.Sprintf("%s%s", defaultMetricPrefix, swm.Metric.Name),
			}),
			Samples: []prompb.Sample{
				{Timestamp: timestamp.FromTime(t)},
			},
		}
	}
	switch swm.Metric.Type {
	case metrics.Counter:
		ts := mapMonoSeries(swm.TimeSeries, swm.Latest)
		ts.Samples[0].Value = swm.Measure.(*metrics.CounterSink).Value
		newts = []prompb.TimeSeries{ts}

	case metrics.Gauge:
		ts := mapMonoSeries(swm.TimeSeries, swm.Latest)
		ts.Samples[0].Value = swm.Measure.(*metrics.GaugeSink).Value
		newts = []prompb.TimeSeries{ts}

	case metrics.Rate:
		ts := mapMonoSeries(swm.TimeSeries, swm.Latest)
		// pass zero duration here because time is useless for formatting rate
		rateVals := swm.Measure.(*metrics.RateSink).Format(time.Duration(0))
		ts.Samples[0].Value = rateVals["rate"]
		newts = []prompb.TimeSeries{ts}

	case metrics.Trend:
		newts = MapTrend(
			swm.TimeSeries, swm.Latest, swm.Measure.(*metrics.TrendSink))

	default:
		panic(fmt.Sprintf("Something is really off, as I cannot recognize the type of metric %s: `%s`", swm.Metric.Name, swm.Metric.Type))
	}

	return newts
}

func sinkByType(mt metrics.MetricType) metrics.Sink {
	var sink metrics.Sink
	switch mt {
	case metrics.Counter:
		sink = &metrics.CounterSink{}
	case metrics.Gauge:
		sink = &metrics.GaugeSink{}
	case metrics.Trend:
		sink = &metrics.TrendSink{}
	case metrics.Rate:
		sink = &metrics.RateSink{}
	default:
		panic(fmt.Sprintf("metric type %q unsupported", mt.String()))
	}
	return sink
}

// the code below will be removed
// when TimeSeries will be a native k6's concept.

type TimeSeries struct {
	Metric *metrics.Metric
	Tags   *metrics.SampleTags
}

var bytesep = []byte{0xff}

func timeSeriesKey(m *metrics.Metric, sampleTags *metrics.SampleTags) string {
	if sampleTags.IsEmpty() {
		return m.Name
	}

	tmap := sampleTags.CloneTags()
	keys := make([]string, 0, len(tmap))
	for k := range tmap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(m.Name)
	for i := 0; i < len(keys); i++ {
		b.Write(bytesep)
		b.WriteString(keys[i])
		b.Write(bytesep)
		b.WriteString(tmap[keys[i]])
	}
	return b.String()
}
