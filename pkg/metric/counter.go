package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type (
	// A CounterVecOpts is an alias of VectorOpts.
	CounterVecOpts VectorOpts

	// CounterVec interface represents a counter vector.
	CounterVec interface {
		// Inc increments labels.
		Inc(labels ...string)
		// Add adds labels with v.
		Add(v float64, labels ...string)

		Close() bool
	}

	Counter interface {
		Inc()
		Add(v float64)
		Close() bool
		Value() float64
	}

	counterVec struct {
		counter *prom.CounterVec
	}
)

type counter struct {
	c   prom.Counter
	val atomic.Float64
}

type CounterOpts Opts

func NewCounter(opts CounterOpts) Counter {
	c := prom.NewCounter(prom.CounterOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Help:        opts.Help,
		ConstLabels: prom.Labels(opts.ConstLabels),
	})
	prom.MustRegister(c)
	return &counter{
		c: c,
	}
}

func (c *counter) Inc() {
	c.c.Inc()
}

func (c *counter) Add(v float64) {
	c.c.Add(v)
	c.val.Add(v)
}

func (c *counter) Value() float64 {
	return c.val.Load()
}

func (c *counter) Close() bool {
	return prom.Unregister(c.c)
}

// NewCounterVec returns a CounterVec.
func NewCounterVec(cfg CounterVecOpts) CounterVec {
	vec := prom.NewCounterVec(prom.CounterOpts{
		Namespace: cfg.Namespace,
		Subsystem: cfg.Subsystem,
		Name:      cfg.Name,
		Help:      cfg.Help,
	}, cfg.Labels)
	prom.MustRegister(vec)
	cv := &counterVec{
		counter: vec,
	}
	return cv
}

func (cv *counterVec) Inc(labels ...string) {
	cv.counter.WithLabelValues(labels...).Inc()
}

func (cv *counterVec) Add(v float64, labels ...string) {
	cv.counter.WithLabelValues(labels...).Add(v)
}

func (cv *counterVec) Close() bool {
	return prom.Unregister(cv.counter)
}
