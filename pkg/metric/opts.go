package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

type VectorOpts struct {
	Namespace string
	Subsystem string
	Name      string
	Help      string
	Labels    []string
}

type Labels prom.Labels
type Opts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Help        string
	ConstLabels Labels
}
