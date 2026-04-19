package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
)

type workloadSummary struct {
	Scenario   string      `json:"scenario"`
	Prefix     string      `json:"prefix"`
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at"`
	Writer     sideSummary `json:"writer"`
}

type sideSummary struct {
	Commands           map[string]int `json:"commands"`
	UniqueKeys         int            `json:"unique_keys"`
	ApproxPayloadBytes int64          `json:"approx_payload_bytes"`
	Iterations         int            `json:"iterations"`
	TransientRetries   int            `json:"transient_retries"`
}

type sideRuntime struct {
	mu                 sync.Mutex
	commands           map[string]int
	keys               map[string]struct{}
	approxPayloadBytes int64
	iterations         int
	transientRetries   int
}

type clusterWriter struct {
	cli   redisclient.Redis
	stats *sideRuntime
}

const writeMaxAttempts = 6

func main() {
	var (
		scenario       string
		addrs          string
		prefix         string
		reportJSON     string
		soakDuration   time.Duration
		keySpace       int
		bigStringBytes int
		throttle       time.Duration
		boundaryEvery  int
		volatileEvery  int
		txnEvery       int
		scriptEvery    int
		targetQPS      int
		workers        int
	)

	flag.StringVar(&scenario, "scenario", "", "workload scenario: rich or soak")
	flag.StringVar(&addrs, "addrs", "", "comma-separated startup addresses for the source cluster")
	flag.StringVar(&prefix, "prefix", "", "key prefix used for this run")
	flag.StringVar(&reportJSON, "report-json", "", "optional path to write a JSON summary")
	flag.DurationVar(&soakDuration, "duration", 3*time.Minute, "duration for soak scenario")
	flag.IntVar(&keySpace, "key-space", 32, "rolling key space for soak scenario")
	flag.IntVar(&bigStringBytes, "big-string-bytes", 1<<20, "large string size in bytes")
	flag.DurationVar(&throttle, "throttle", 0, "optional sleep between soak iterations")
	flag.IntVar(&boundaryEvery, "boundary-every", 0, "write stable boundary data every N soak iterations; 0 disables it")
	flag.IntVar(&volatileEvery, "volatile-every", 0, "write volatile TTL boundary data every N soak iterations; 0 disables it")
	flag.IntVar(&txnEvery, "txn-every", 0, "write a same-slot MULTI/EXEC transaction every N soak iterations; 0 disables it")
	flag.IntVar(&scriptEvery, "script-every", 0, "write a script/function boundary set every N soak iterations; 0 disables it")
	flag.IntVar(&targetQPS, "target-qps", 0, "combined command-per-second cap for soak scenario; 0 disables rate limiting")
	flag.IntVar(&workers, "workers", 1, "number of concurrent source writers for soak scenario")
	flag.Parse()

	if scenario == "" || addrs == "" || prefix == "" {
		fmt.Fprintln(os.Stderr, "scenario, addrs and prefix are required")
		os.Exit(2)
	}
	if keySpace <= 0 {
		fmt.Fprintln(os.Stderr, "key-space must be > 0")
		os.Exit(2)
	}

	writer, err := newClusterWriter(splitAddrs(addrs))
	if err != nil {
		fmt.Fprintf(os.Stderr, "open source cluster failed: %v\n", err)
		os.Exit(1)
	}
	defer writer.close()

	startedAt := time.Now()
	switch scenario {
	case "rich":
		err = runRich(writer, prefix, bigStringBytes)
	case "soak":
		err = runSoakConcurrent(writer, splitAddrs(addrs), prefix, soakDuration, keySpace, bigStringBytes/4, throttle, boundaryEvery, volatileEvery, txnEvery, scriptEvery, targetQPS, workers)
	default:
		err = fmt.Errorf("unsupported scenario %q", scenario)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "run workload failed: %v\n", err)
		os.Exit(1)
	}

	summary := workloadSummary{
		Scenario:   scenario,
		Prefix:     prefix,
		StartedAt:  startedAt,
		FinishedAt: time.Now(),
		Writer:     writer.stats.snapshot(),
	}
	raw, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal summary failed: %v\n", err)
		os.Exit(1)
	}
	if reportJSON != "" {
		if err := os.WriteFile(reportJSON, raw, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write summary failed: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println(string(raw))
}

func newClusterWriter(addrs []string) (*clusterWriter, error) {
	return newClusterWriterWithStats(addrs, &sideRuntime{
		commands: make(map[string]int),
		keys:     make(map[string]struct{}),
	})
}

func newClusterWriterWithStats(addrs []string, stats *sideRuntime) (*clusterWriter, error) {
	cli, err := redisclient.NewRedis(config.RedisConfig{
		Addresses: addrs,
		Type:      config.RedisTypeCluster,
	})
	if err != nil {
		return nil, err
	}
	return &clusterWriter{
		cli:   cli,
		stats: stats,
	}, nil
}

func (w *clusterWriter) close() {
	if w.cli != nil {
		_ = w.cli.Close()
	}
}

func (s *sideRuntime) snapshot() sideSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.commands))
	for k, v := range s.commands {
		out[k] = v
	}
	return sideSummary{
		Commands:           out,
		UniqueKeys:         len(s.keys),
		ApproxPayloadBytes: s.approxPayloadBytes,
		Iterations:         s.iterations,
		TransientRetries:   s.transientRetries,
	}
}

func (s *sideRuntime) record(cmd string, key string, args ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key != "" {
		s.keys[key] = struct{}{}
		s.approxPayloadBytes += estimatePayloadBytes(args...)
	}
	s.commands[strings.ToLower(cmd)]++
}

func (s *sideRuntime) addIteration() {
	s.mu.Lock()
	s.iterations++
	s.mu.Unlock()
}

func (s *sideRuntime) addTransientRetry() {
	s.mu.Lock()
	s.transientRetries++
	s.mu.Unlock()
}

func (s *sideRuntime) commandCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int
	for _, count := range s.commands {
		total += count
	}
	return total
}

func splitAddrs(addrs string) []string {
	parts := strings.Split(addrs, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func runRich(w *clusterWriter, prefix string, bigStringBytes int) error {
	base := fmt.Sprintf("%s:stable:rich", prefix)
	if err := w.do("set", base+":string", "alpha"); err != nil {
		return err
	}
	if err := w.do("incrby", base+":counter", "9"); err != nil {
		return err
	}
	if err := w.do("hset", base+":hash", "f1", "v1", "f2", "v2"); err != nil {
		return err
	}
	if err := w.do("rpush", base+":list", "a", "b", "c"); err != nil {
		return err
	}
	if err := w.do("sadd", base+":set", "red", "blue", "green"); err != nil {
		return err
	}
	if err := w.do("zadd", base+":zset", "-1", "negative", "0", "zero", "9007199254740991", "large"); err != nil {
		return err
	}
	if err := w.do("xadd", base+":stream", "*", "side", "source", "seq", "1"); err != nil {
		return err
	}
	if err := w.do("xadd", base+":stream", "*", "side", "source", "seq", "2"); err != nil {
		return err
	}
	if err := w.do("set", base+":delete-me", "gone"); err != nil {
		return err
	}
	if err := w.do("del", base+":delete-me"); err != nil {
		return err
	}
	if err := writeBoundarySet(w, prefix, 0, bigStringBytes); err != nil {
		return err
	}
	if err := writeVolatileSet(w, prefix, 0); err != nil {
		return err
	}
	if err := writeTxnSet(w, prefix, 0); err != nil {
		return err
	}
	if err := writeScriptSet(w, prefix, 0); err != nil {
		return err
	}
	w.stats.addIteration()
	return nil
}

func runSoakConcurrent(baseWriter *clusterWriter, addrs []string, prefix string, duration time.Duration, keySpace int, largePayloadBytes int, throttle time.Duration, boundaryEvery, volatileEvery, txnEvery, scriptEvery int, targetQPS int, workers int) error {
	if workers <= 1 {
		return runSoak(baseWriter, prefix, duration, keySpace, largePayloadBytes, throttle, boundaryEvery, volatileEvery, txnEvery, scriptEvery, targetQPS)
	}

	writers := make([]*clusterWriter, 0, workers)
	writers = append(writers, baseWriter)
	for i := 1; i < workers; i++ {
		writer, err := newClusterWriterWithStats(addrs, baseWriter.stats)
		if err != nil {
			return err
		}
		defer writer.close()
		writers = append(writers, writer)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startedAt := time.Now()
	deadline := startedAt.Add(duration)
	errCh := make(chan error, workers)
	doneCh := make(chan struct{})
	var seq atomic.Int64
	var wg sync.WaitGroup

	for _, writer := range writers {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				current := int(seq.Add(1) - 1)
				if err := runSoakIteration(writer, prefix, keySpace, largePayloadBytes, boundaryEvery, volatileEvery, txnEvery, scriptEvery, current); err != nil {
					cancel()
					errCh <- err
					return
				}
				if throttle > 0 {
					time.Sleep(throttle)
				}
				if targetQPS > 0 {
					throttleToTargetQPS(startedAt, baseWriter.stats.commandCount(), targetQPS)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		cancel()
		<-doneCh
		return err
	case <-doneCh:
		return nil
	}
}

func runSoak(w *clusterWriter, prefix string, duration time.Duration, keySpace int, largePayloadBytes int, throttle time.Duration, boundaryEvery, volatileEvery, txnEvery, scriptEvery int, targetQPS int) error {
	startedAt := time.Now()
	deadline := startedAt.Add(duration)
	seq := 0
	for time.Now().Before(deadline) {
		if err := runSoakIteration(w, prefix, keySpace, largePayloadBytes, boundaryEvery, volatileEvery, txnEvery, scriptEvery, seq); err != nil {
			return err
		}
		seq++
		if throttle > 0 {
			time.Sleep(throttle)
		}
		if targetQPS > 0 {
			throttleToTargetQPS(startedAt, w.stats.commandCount(), targetQPS)
		}
	}
	return nil
}

func runSoakIteration(w *clusterWriter, prefix string, keySpace int, largePayloadBytes int, boundaryEvery, volatileEvery, txnEvery, scriptEvery int, seq int) error {
	base := fmt.Sprintf("%s:stable:soak:%02d", prefix, seq%keySpace)
	if err := w.do("set", base+":string", fmt.Sprintf("value:%06d", seq)); err != nil {
		return err
	}
	if err := w.do("incrby", base+":counter", strconv.Itoa((seq%7)+1)); err != nil {
		return err
	}
	if err := w.do("hset", base+":hash", fmt.Sprintf("field:%06d", seq), fmt.Sprintf("value:%06d", seq)); err != nil {
		return err
	}
	if err := w.do("rpush", base+":list", fmt.Sprintf("item:%06d", seq)); err != nil {
		return err
	}
	if err := w.do("ltrim", base+":list", "-256", "-1"); err != nil {
		return err
	}
	if err := w.do("sadd", base+":set", fmt.Sprintf("member:%03d", seq%512)); err != nil {
		return err
	}
	if err := w.do("zadd", base+":zset", strconv.Itoa(seq), fmt.Sprintf("member:%06d", seq)); err != nil {
		return err
	}
	if err := w.do("zremrangebyrank", base+":zset", "0", "-257"); err != nil {
		return err
	}
	if err := w.do("xadd", base+":stream", "maxlen", "~", "128", "*", "seq", strconv.Itoa(seq)); err != nil {
		return err
	}
	if seq%16 == 0 {
		if err := w.do("set", base+":big-string", buildPayload(largePayloadBytes, fmt.Sprintf("payload-%06d", seq))); err != nil {
			return err
		}
	}
	if seq%25 == 0 {
		if err := w.do("set", base+":delete-me", "tmp"); err != nil {
			return err
		}
		if err := w.do("del", base+":delete-me"); err != nil {
			return err
		}
	}
	if boundaryEvery > 0 && seq%boundaryEvery == 0 {
		if err := writeBoundarySet(w, prefix, seq, largePayloadBytes); err != nil {
			return err
		}
	}
	if volatileEvery > 0 && seq%volatileEvery == 0 {
		if err := writeVolatileSet(w, prefix, seq); err != nil {
			return err
		}
	}
	if txnEvery > 0 && seq%txnEvery == 0 {
		if err := writeTxnSet(w, prefix, seq); err != nil {
			return err
		}
	}
	if scriptEvery > 0 && seq%scriptEvery == 0 {
		if err := writeScriptSet(w, prefix, seq); err != nil {
			return err
		}
	}

	w.stats.addIteration()
	return nil
}

func writeBoundarySet(w *clusterWriter, prefix string, seq int, largePayloadBytes int) error {
	keyBase := fmt.Sprintf("%s:stable:boundary:%06d", prefix, seq)
	longKey := keyBase + ":" + buildPayload(256, "long-key-")
	if err := w.do("set", keyBase+":empty-string", ""); err != nil {
		return err
	}
	if err := w.do("set", keyBase+":binary-string", []byte{'b', 0, 'i', 0, 'n'}); err != nil {
		return err
	}
	if err := w.do("set", longKey, "long-key-value"); err != nil {
		return err
	}
	if err := w.do("set", keyBase+":large-string", buildPayload(largePayloadBytes, fmt.Sprintf("boundary-%06d", seq))); err != nil {
		return err
	}
	if err := w.do("hset", keyBase+":hash", "", "empty-field", "long-field-"+buildPayload(128, "f"), buildPayload(1024, "hash-value-")); err != nil {
		return err
	}
	if err := w.do("rpush", keyBase+":list", "", "first", buildPayload(1024, "list-value-"), "last"); err != nil {
		return err
	}
	if err := w.do("sadd", keyBase+":set", "", "member", buildPayload(512, "set-member-")); err != nil {
		return err
	}
	if err := w.do("zadd", keyBase+":zset", "-1", "negative", "0", "zero", "9007199254740991", "large"); err != nil {
		return err
	}
	return nil
}

func writeVolatileSet(w *clusterWriter, prefix string, seq int) error {
	keyBase := fmt.Sprintf("%s:volatile:%06d", prefix, seq)
	if err := w.do("set", keyBase+":ttl-short", "short-lived", "px", "1500"); err != nil {
		return err
	}
	if err := w.do("set", keyBase+":ttl-persist", "persist-me", "ex", "3600"); err != nil {
		return err
	}
	if err := w.do("persist", keyBase+":ttl-persist"); err != nil {
		return err
	}
	return nil
}

func writeTxnSet(w *clusterWriter, prefix string, seq int) error {
	tag := fmt.Sprintf("{txn-%04d}", seq%1024)
	keyBase := fmt.Sprintf("%s:stable:txn:%s:%06d", prefix, tag, seq)
	var err error
	for attempt := 0; attempt < writeMaxAttempts; attempt++ {
		if err = execTxnSet(w, keyBase, seq); err == nil {
			w.stats.record("multi", keyBase+":string")
			w.stats.record("set", keyBase+":string", keyBase+":string", fmt.Sprintf("txn:%06d", seq))
			w.stats.record("hincrby", keyBase+":hash", keyBase+":hash", "counter", "1")
			w.stats.record("rpush", keyBase+":list", keyBase+":list", fmt.Sprintf("txn-item:%06d", seq))
			w.stats.record("exec", keyBase+":string")
			return nil
		}
		if !isTransientWriteError(err) || attempt == writeMaxAttempts-1 {
			return err
		}
		w.stats.addTransientRetry()
		time.Sleep(writeRetryDelay(attempt))
	}
	return err
}

func execTxnSet(w *clusterWriter, keyBase string, seq int) error {
	batcher := w.cli.NewTxnBatcher()
	if err := batcher.Put("set", keyBase+":string", fmt.Sprintf("txn:%06d", seq)); err != nil {
		return err
	}
	if err := batcher.Put("hincrby", keyBase+":hash", "counter", "1"); err != nil {
		return err
	}
	if err := batcher.Put("rpush", keyBase+":list", fmt.Sprintf("txn-item:%06d", seq)); err != nil {
		return err
	}
	_, err := batcher.Exec()
	return err
}

func writeScriptSet(w *clusterWriter, prefix string, seq int) error {
	base := fmt.Sprintf("%s:stable:script:%06d", prefix, seq)
	if err := w.do("eval", "return redis.call('set', KEYS[1], ARGV[1])", "1", base+":eval", fmt.Sprintf("eval:%06d", seq)); err != nil {
		return err
	}
	return nil
}

func throttleToTargetQPS(startedAt time.Time, totalCommands int, targetQPS int) {
	if targetQPS <= 0 || totalCommands <= 0 {
		return
	}
	idealElapsed := time.Duration(float64(totalCommands) / float64(targetQPS) * float64(time.Second))
	if sleepFor := idealElapsed - time.Since(startedAt); sleepFor > 0 {
		time.Sleep(sleepFor)
	}
}

func (w *clusterWriter) do(cmd string, args ...interface{}) error {
	var key string
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			key = s
		}
	}
	return w.exec(cmd, key, args...)
}

func (w *clusterWriter) exec(cmd string, key string, args ...interface{}) error {
	var err error
	for attempt := 0; attempt < writeMaxAttempts; attempt++ {
		if _, err = w.cli.Do(cmd, args...); err == nil {
			w.stats.record(cmd, key, args...)
			return nil
		}
		if !isTransientWriteError(err) || attempt == writeMaxAttempts-1 {
			return err
		}
		w.stats.addTransientRetry()
		time.Sleep(writeRetryDelay(attempt))
	}
	return err
}

func isTransientWriteError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	transientParts := []string{
		"connection reset by peer",
		"connection refused",
		"broken pipe",
		"eof",
		"i/o timeout",
		"no route to host",
		"network is unreachable",
		"use of closed network connection",
		"cluster status fail",
		"cluster down",
		"loading redis is loading the dataset in memory",
		"try again",
	}
	for _, part := range transientParts {
		if strings.Contains(msg, part) {
			return true
		}
	}
	return false
}

func writeRetryDelay(attempt int) time.Duration {
	delays := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	if attempt < len(delays) {
		return delays[attempt]
	}
	return delays[len(delays)-1]
}

func estimatePayloadBytes(args ...interface{}) int64 {
	var total int64
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			total += int64(len(v))
		case []byte:
			total += int64(len(v))
		default:
			total += int64(len(fmt.Sprint(v)))
		}
	}
	return total
}

func buildPayload(size int, token string) string {
	if size <= 0 {
		return ""
	}
	if token == "" {
		token = "x"
	}
	var b strings.Builder
	b.Grow(size)
	for b.Len() < size {
		remaining := size - b.Len()
		if remaining >= len(token) {
			b.WriteString(token)
		} else {
			b.WriteString(token[:remaining])
		}
	}
	return b.String()
}
