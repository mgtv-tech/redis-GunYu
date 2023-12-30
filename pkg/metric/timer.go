package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Timer interface {
	Timer() func(labels ...string)

	Observe(duration time.Duration, labels ...string)

	Close()
}

type timerOptions struct {
	buckets       []float64
	quantiles     map[float64]float64
	needSummary   bool
	needHistogram bool
	help          string
}

type TimerOption func(*timerOptions)

// WithHistogramBuckets set buckets of histogram
// default value is float64{.00001, .00005, .0001, .0002, .0005, .001, .005, .01, .025, .05, .1, .5, 1, 2.5, 5, 10, 60}
func WithHistogramBuckets(buckets []float64) TimerOption {
	return func(o *timerOptions) {
		o.buckets = buckets
	}
}

// WithSummaryQuantiles set quantile values
// default value is {0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
func WithSummaryQuantiles(quantiles map[float64]float64) TimerOption {
	return func(o *timerOptions) {
		o.quantiles = quantiles
	}
}

// summary metric
func WithSummary(need bool) TimerOption {
	return func(o *timerOptions) {
		o.needSummary = need
	}
}

// histogram metric
func WithHistogram(need bool) TimerOption {
	return func(o *timerOptions) {
		o.needHistogram = need
	}
}

// NewTimer
func NewTimer(cfg VectorOpts, opts ...TimerOption) Timer {
	timerOpts := timerOptions{
		needSummary:   false,
		needHistogram: true,
	}
	for _, opt := range opts {
		opt(&timerOpts)
	}

	if !timerOpts.needHistogram && !timerOpts.needSummary {
		timerOpts.needHistogram = true
	}

	tcm := &timerCombinedMetric{}

	// summary
	if timerOpts.needSummary {
		if len(timerOpts.quantiles) == 0 {
			timerOpts.quantiles = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
		}
		tcm.summary = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace:  cfg.Namespace,
				Subsystem:  cfg.Subsystem,
				Name:       cfg.Name + "_s",
				Help:       timerOpts.help + " (summary)",
				Objectives: timerOpts.quantiles,
			},
			cfg.Labels)
		prometheus.MustRegister(tcm.summary)
	}

	// histogram
	if timerOpts.needHistogram {
		if len(timerOpts.buckets) == 0 {
			timerOpts.buckets = []float64{.00001, .00005, .0001, .0002, .0005, .001, .005, .01, .025, .05, .1, .5, 1, 2.5, 5, 10, 60}
		}
		tcm.histogram = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: cfg.Namespace,
				Subsystem: cfg.Subsystem,
				Name:      cfg.Name,
				Help:      timerOpts.help + " (histogram)",
				Buckets:   timerOpts.buckets,
			}, cfg.Labels)
		prometheus.MustRegister(tcm.histogram)
	}

	return tcm
}

type timerCombinedMetric struct {
	summary   *prometheus.SummaryVec
	histogram *prometheus.HistogramVec
}

func (t *timerCombinedMetric) Timer() func(labels ...string) {
	if t == nil {
		return func(labels ...string) {}
	}

	now := time.Now()

	return func(labels ...string) {
		seconds := float64(time.Since(now)) / float64(time.Second)
		if t.summary != nil {
			t.summary.WithLabelValues(labels...).Observe(seconds)
		}
		if t.histogram != nil {
			t.histogram.WithLabelValues(labels...).Observe(seconds)
		}
	}
}

func (t *timerCombinedMetric) Observe(duration time.Duration, labels ...string) {
	if t == nil {
		return
	}

	seconds := float64(duration) / float64(time.Second)
	if t.summary != nil {
		t.summary.WithLabelValues(labels...).Observe(seconds)
	}
	if t.histogram != nil {
		t.histogram.WithLabelValues(labels...).Observe(seconds)
	}
}

func (t *timerCombinedMetric) Close() {
	if t == nil {
		return
	}

	if t.histogram != nil {
		prometheus.Unregister(t.histogram)
	}
	if t.summary != nil {
		prometheus.Unregister(t.summary)
	}
}
