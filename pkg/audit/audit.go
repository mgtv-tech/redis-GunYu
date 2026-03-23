package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/metric"
)

type FilteredEvent struct {
	Dt       string `json:"dt"`
	Input    string `json:"input"`
	Cmd      string `json:"cmd"`
	KeyValue string `json:"key_value"`
	Reason   string `json:"reason"`
	Stage    string `json:"stage"`
	Node     string `json:"node"`
}

type SentEvent struct {
	Dt       string `json:"dt"`
	Input    string `json:"input"`
	Target   string `json:"target"`
	Cmd      string `json:"cmd"`
	KeyValue string `json:"key_value"`
	Node     string `json:"node"`
}

type Writer interface {
	EnqueueFiltered(ev FilteredEvent)
	EnqueueSent(ev SentEvent)
}

type noopWriter struct{}

func (noopWriter) EnqueueFiltered(FilteredEvent) {}
func (noopWriter) EnqueueSent(SentEvent)         {}

type asyncWriter struct {
	logger log.Logger

	client   *http.Client
	endpoint string
	database string
	user     string
	password string

	filteredTable string
	sentTable     string
	batchSize     int

	filteredCh chan FilteredEvent
	sentCh     chan SentEvent
}

// gatedWriter forwards to inner only when allow is true (runtime + config intent).
type gatedWriter struct {
	mu    sync.RWMutex
	inner Writer
	allow atomic.Bool
}

func (g *gatedWriter) EnqueueFiltered(ev FilteredEvent) {
	if !g.allow.Load() {
		return
	}
	g.mu.RLock()
	w := g.inner
	g.mu.RUnlock()
	w.EnqueueFiltered(ev)
}

func (g *gatedWriter) EnqueueSent(ev SentEvent) {
	if !g.allow.Load() {
		return
	}
	g.mu.RLock()
	w := g.inner
	g.mu.RUnlock()
	w.EnqueueSent(ev)
}

func (g *gatedWriter) isNoopInner() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.inner.(noopWriter)
	return ok
}

var (
	initMu      sync.Mutex
	gate        *gatedWriter
	auditLogger log.Logger

	// channel: filtered | sync_cmd_filtered queue; sent | sync_cmd_sent queue
	auditEnqueueDroppedCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "audit",
		Name:      "enqueue_dropped_total",
		Help:      "Audit events dropped because the in-memory queue was full (non-blocking enqueue).",
		Labels:    []string{"channel"},
	})
	auditFlushFailedCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "audit",
		Name:      "flush_failed_total",
		Help:      "ClickHouse audit HTTP insert failures (batch discarded without retry).",
		Labels:    []string{"channel"},
	})
	auditFlushFailedRowsCounter = metric.NewCounterVec(metric.CounterVecOpts{
		Namespace: config.AppName,
		Subsystem: "audit",
		Name:      "flush_failed_rows_total",
		Help:      "Rows lost after a failed ClickHouse audit flush.",
		Labels:    []string{"channel"},
	})
)

// EnqueueAllowed reports whether audit events are accepted (runtime gate is open).
func EnqueueAllowed() bool {
	if gate == nil {
		return false
	}
	return gate.allow.Load()
}

// InitGlobal returns the process-wide audit writer. Safe to call from multiple outputs; config is reapplied each time.
func InitGlobal(cfg *config.AuditConfig, logger log.Logger) Writer {
	initMu.Lock()
	defer initMu.Unlock()
	applyAuditLocked(cfg, logger)
	return gate
}

// ApplyRuntimeConfig updates the in-process audit gate and lazily starts the async writer when enabling.
// Call after mutating config.Audit (e.g. audit.enabled). If cfg is nil, auditing is turned off.
func ApplyRuntimeConfig(cfg *config.AuditConfig, logger log.Logger) error {
	initMu.Lock()
	defer initMu.Unlock()
	if cfg != nil && cfg.Enabled && strings.TrimSpace(cfg.Endpoint) == "" {
		return fmt.Errorf("audit.enabled is true but audit.endpoint is empty")
	}
	applyAuditLocked(cfg, logger)
	return nil
}

func applyAuditLocked(cfg *config.AuditConfig, logger log.Logger) {
	if logger != nil {
		auditLogger = logger
	}
	if gate == nil {
		gate = &gatedWriter{inner: noopWriter{}}
		gate.allow.Store(false)
	}

	if cfg == nil || !cfg.Enabled {
		gate.allow.Store(false)
		return
	}

	if strings.TrimSpace(cfg.Endpoint) == "" {
		gate.allow.Store(false)
		return
	}

	if gate.isNoopInner() {
		if auditLogger == nil {
			auditLogger = log.WithLogger("[audit] ")
		}
		w, err := buildAsyncWriter(cfg, auditLogger)
		if err != nil {
			gate.allow.Store(false)
			return
		}
		gate.mu.Lock()
		gate.inner = w
		gate.mu.Unlock()
	}

	gate.allow.Store(true)
}

func buildAsyncWriter(cfg *config.AuditConfig, logger log.Logger) (Writer, error) {
	if cfg == nil || strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, fmt.Errorf("audit endpoint is required")
	}
	qs := cfg.QueueSize
	if qs <= 0 {
		qs = 50000
	}
	bs := cfg.BatchSize
	if bs <= 0 {
		bs = 10000
	}
	interval := cfg.FlushInterval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	db := cfg.Database
	if db == "" {
		db = "default"
	}
	ft := cfg.FilteredTable
	if ft == "" {
		ft = "sync_cmd_filtered"
	}
	st := cfg.SentTable
	if st == "" {
		st = "sync_cmd_sent"
	}

	w := &asyncWriter{
		logger:        logger,
		client:        &http.Client{Timeout: timeout},
		endpoint:      strings.TrimRight(cfg.Endpoint, "/"),
		database:      db,
		user:          cfg.User,
		password:      cfg.Password,
		filteredTable: ft,
		sentTable:     st,
		batchSize:     bs,
		filteredCh:    make(chan FilteredEvent, qs),
		sentCh:        make(chan SentEvent, qs),
	}
	w.run(interval)
	return w, nil
}

func (w *asyncWriter) EnqueueFiltered(ev FilteredEvent) {
	select {
	case w.filteredCh <- ev:
	default:
		auditEnqueueDroppedCounter.Inc("filtered")
	}
}

func (w *asyncWriter) EnqueueSent(ev SentEvent) {
	select {
	case w.sentCh <- ev:
	default:
		auditEnqueueDroppedCounter.Inc("sent")
	}
}

func (w *asyncWriter) run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		filteredBatch := make([]FilteredEvent, 0, w.batchSize)
		sentBatch := make([]SentEvent, 0, w.batchSize)
		flush := func() {
			if len(filteredBatch) > 0 {
				if err := w.flushFiltered(filteredBatch); err != nil {
					w.logger.Warnf("audit flush filtered failed: %v", err)
					auditFlushFailedCounter.Inc("filtered")
					auditFlushFailedRowsCounter.Add(float64(len(filteredBatch)), "filtered")
				}
				filteredBatch = filteredBatch[:0]
			}
			if len(sentBatch) > 0 {
				if err := w.flushSent(sentBatch); err != nil {
					w.logger.Warnf("audit flush sent failed: %v", err)
					auditFlushFailedCounter.Inc("sent")
					auditFlushFailedRowsCounter.Add(float64(len(sentBatch)), "sent")
				}
				sentBatch = sentBatch[:0]
			}
		}
		for {
			select {
			case ev := <-w.filteredCh:
				filteredBatch = append(filteredBatch, ev)
				if len(filteredBatch) >= w.batchSize {
					flush()
				}
			case ev := <-w.sentCh:
				sentBatch = append(sentBatch, ev)
				if len(sentBatch) >= w.batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
}

func (w *asyncWriter) flushFiltered(batch []FilteredEvent) error {
	query := fmt.Sprintf("INSERT INTO %s (dt,input,cmd,key_value,reason,stage,node) FORMAT JSONEachRow", w.filteredTable)
	return w.postJSONEachRow(query, batch)
}

func (w *asyncWriter) flushSent(batch []SentEvent) error {
	query := fmt.Sprintf("INSERT INTO %s (dt,input,target,cmd,key_value,node) FORMAT JSONEachRow", w.sentTable)
	return w.postJSONEachRow(query, batch)
}

func (w *asyncWriter) postJSONEachRow(query string, rows interface{}) error {
	var buf bytes.Buffer
	switch tt := rows.(type) {
	case []FilteredEvent:
		for _, row := range tt {
			b, err := json.Marshal(row)
			if err != nil {
				return err
			}
			buf.Write(b)
			buf.WriteByte('\n')
		}
	case []SentEvent:
		for _, row := range tt {
			b, err := json.Marshal(row)
			if err != nil {
				return err
			}
			buf.Write(b)
			buf.WriteByte('\n')
		}
	default:
		return fmt.Errorf("unsupported row type")
	}

	u, err := url.Parse(w.endpoint)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("database", w.database)
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, u.String(), &buf)
	if err != nil {
		return err
	}
	if w.user != "" {
		req.SetBasicAuth(w.user, w.password)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("clickhouse status code: %d", resp.StatusCode)
	}
	return nil
}
