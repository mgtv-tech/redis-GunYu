package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

type (
	// GaugeVecOpts is an alias of VectorOpts.
	GaugeVecOpts VectorOpts

	// GaugeVec represents a gauge vector.
	GaugeVec interface {
		// Set sets v to labels.
		Set(v float64, labels ...string)
		// Inc increments labels.
		Inc(labels ...string)
		// Add adds v to labels.
		Add(v float64, labels ...string)
		Close() bool
	}
	Gauge interface {
		Set(v float64)
		Inc()
		Add(v float64)
		Close() bool
	}

	gaugeVec struct {
		gauge *prom.GaugeVec
	}
	gauge struct {
		g prom.Gauge
	}
)

type GaugeOpts Opts

func NewGauge(o GaugeOpts) Gauge {
	g := prom.NewGauge(prom.GaugeOpts{
		Namespace:   o.Namespace,
		Subsystem:   o.Subsystem,
		Name:        o.Name,
		Help:        o.Help,
		ConstLabels: prom.Labels(o.ConstLabels),
	})
	prom.MustRegister(g)
	return &gauge{
		g: g,
	}
}

// NewGaugeVec returns a GaugeVec.
func NewGaugeVec(cfg *GaugeVecOpts) GaugeVec {
	if cfg == nil {
		return nil
	}

	vec := prom.NewGaugeVec(
		prom.GaugeOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      cfg.Name,
			Help:      cfg.Help,
		}, cfg.Labels)
	prom.MustRegister(vec)
	gv := &gaugeVec{
		gauge: vec,
	}

	return gv
}

func (g *gauge) Inc() {
	g.g.Inc()
}

func (g *gauge) Add(v float64) {
	g.g.Add(v)
}

func (g *gauge) Set(v float64) {
	g.g.Set(v)
}

func (g *gauge) Close() bool {
	return prom.Unregister(g.g)
}

func (gv *gaugeVec) Inc(labels ...string) {
	gv.gauge.WithLabelValues(labels...).Inc()
}

func (gv *gaugeVec) Add(v float64, labels ...string) {
	gv.gauge.WithLabelValues(labels...).Add(v)
}

func (gv *gaugeVec) Set(v float64, labels ...string) {
	gv.gauge.WithLabelValues(labels...).Set(v)
}

func (gv *gaugeVec) Close() bool {
	return prom.Unregister(gv.gauge)
}
