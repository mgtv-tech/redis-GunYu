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
	Scenario   string                 `json:"scenario"`
	Prefix     string                 `json:"prefix"`
	StartedAt  time.Time              `json:"started_at"`
	FinishedAt time.Time              `json:"finished_at"`
	Sides      map[string]sideSummary `json:"sides"`
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
	name  string
	cli   redisclient.Redis
	stats *sideRuntime
}

const writeMaxAttempts = 6

func main() {
	var (
		scenario       string
		leftAddrs      string
		rightAddrs     string
		prefix         string
		reportJSON     string
		soakDuration   time.Duration
		keySpace       int
		bigStringBytes int
		bigHashFields  int
		bigListItems   int
		bigSetMembers  int
		bigZSetMembers int
		throttle       time.Duration
		boundaryEvery  int
		volatileEvery  int
		txnEvery       int
		targetQPS      int
		soakWorkers    int
	)

	flag.StringVar(&scenario, "scenario", "", "workload scenario: structures or soak")
	flag.StringVar(&leftAddrs, "left-addrs", "", "comma-separated startup addresses for the left cluster")
	flag.StringVar(&rightAddrs, "right-addrs", "", "comma-separated startup addresses for the right cluster")
	flag.StringVar(&prefix, "prefix", "", "key prefix used for this run")
	flag.StringVar(&reportJSON, "report-json", "", "optional path to write a JSON summary")
	flag.DurationVar(&soakDuration, "duration", 3*time.Minute, "duration for soak scenario")
	flag.IntVar(&keySpace, "key-space", 32, "rolling key space for soak scenario")
	flag.IntVar(&bigStringBytes, "big-string-bytes", 1<<20, "large string size in bytes")
	flag.IntVar(&bigHashFields, "big-hash-fields", 2048, "field count for the large hash scenario")
	flag.IntVar(&bigListItems, "big-list-items", 4096, "item count for the large list scenario")
	flag.IntVar(&bigSetMembers, "big-set-members", 2048, "member count for the large set scenario")
	flag.IntVar(&bigZSetMembers, "big-zset-members", 2048, "member count for the large zset scenario")
	flag.DurationVar(&throttle, "throttle", 0, "optional sleep between soak iterations")
	flag.IntVar(&boundaryEvery, "boundary-every", 0, "write stable boundary data every N soak iterations; 0 disables it")
	flag.IntVar(&volatileEvery, "volatile-every", 0, "write volatile TTL/stream boundary data every N soak iterations; 0 disables it")
	flag.IntVar(&txnEvery, "txn-every", 0, "write a same-slot MULTI/EXEC transaction every N soak iterations; 0 disables it")
	flag.IntVar(&targetQPS, "target-qps", 0, "combined command-per-second cap for soak scenario; 0 disables rate limiting")
	flag.IntVar(&soakWorkers, "workers", 1, "number of concurrent writer pairs for soak scenario")
	flag.Parse()

	if scenario == "" || leftAddrs == "" || rightAddrs == "" || prefix == "" {
		fmt.Fprintln(os.Stderr, "scenario, left-addrs, right-addrs and prefix are required")
		os.Exit(2)
	}
	if keySpace <= 0 {
		fmt.Fprintln(os.Stderr, "key-space must be > 0")
		os.Exit(2)
	}

	left, err := newClusterWriter("left", leftAddrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open left cluster failed: %v\n", err)
		os.Exit(1)
	}
	defer left.close()

	right, err := newClusterWriter("right", rightAddrs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open right cluster failed: %v\n", err)
		os.Exit(1)
	}
	defer right.close()

	startedAt := time.Now()
	switch scenario {
	case "structures":
		err = runStructures(left, right, prefix, bigStringBytes, bigHashFields, bigListItems, bigSetMembers, bigZSetMembers)
	case "soak":
		err = runSoakConcurrent(left, right, leftAddrs, rightAddrs, prefix, soakDuration, keySpace, bigStringBytes/4, throttle, boundaryEvery, volatileEvery, txnEvery, targetQPS, soakWorkers)
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
		Sides: map[string]sideSummary{
			"left":  left.stats.snapshot(),
			"right": right.stats.snapshot(),
		},
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

func newClusterWriter(name string, addrs string) (*clusterWriter, error) {
	return newClusterWriterWithStats(name, addrs, &sideRuntime{
		commands: make(map[string]int),
		keys:     make(map[string]struct{}),
	})
}

func newClusterWriterWithStats(name string, addrs string, stats *sideRuntime) (*clusterWriter, error) {
	cli, err := redisclient.NewRedis(config.RedisConfig{
		Addresses: splitAddrs(addrs),
		Type:      config.RedisTypeCluster,
	})
	if err != nil {
		return nil, err
	}
	return &clusterWriter{
		name:  name,
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

func runStructures(left, right *clusterWriter, prefix string, bigStringBytes, bigHashFields, bigListItems, bigSetMembers, bigZSetMembers int) error {
	if err := writeStructureSet(left, prefix, "left", bigStringBytes, bigHashFields, bigListItems, bigSetMembers, bigZSetMembers); err != nil {
		return err
	}
	if err := writeStructureSet(right, prefix, "right", bigStringBytes, bigHashFields, bigListItems, bigSetMembers, bigZSetMembers); err != nil {
		return err
	}
	return nil
}

func writeStructureSet(w *clusterWriter, prefix string, side string, bigStringBytes, bigHashFields, bigListItems, bigSetMembers, bigZSetMembers int) error {
	base := fmt.Sprintf("%s:structures:%s", prefix, side)
	if err := w.do("set", base+":string", "plain-"+side); err != nil {
		return err
	}
	if err := w.do("set", base+":big-string", buildPayload(bigStringBytes, side+"-payload")); err != nil {
		return err
	}
	if err := w.do("hset", base+":hash", "f1", "v1", "f2", "v2", "f3", "v3"); err != nil {
		return err
	}
	if err := writeBigHash(w, base+":big-hash", bigHashFields); err != nil {
		return err
	}
	if err := w.do("rpush", base+":list", "a", "b", "c", "d"); err != nil {
		return err
	}
	if err := writeBigList(w, base+":big-list", bigListItems); err != nil {
		return err
	}
	if err := w.do("sadd", base+":set", "red", "blue", "green"); err != nil {
		return err
	}
	if err := writeBigSet(w, base+":big-set", bigSetMembers); err != nil {
		return err
	}
	if err := w.do("zadd", base+":zset", "1", "one", "2", "two", "3", "three"); err != nil {
		return err
	}
	if err := writeBigZSet(w, base+":big-zset", bigZSetMembers); err != nil {
		return err
	}
	if err := w.do("set", base+":delete-me", "gone"); err != nil {
		return err
	}
	if err := w.do("del", base+":delete-me"); err != nil {
		return err
	}
	w.stats.iterations++
	return nil
}

func writeBigHash(w *clusterWriter, key string, fields int) error {
	const batchSize = 128
	for start := 0; start < fields; start += batchSize {
		end := min(start+batchSize, fields)
		args := make([]interface{}, 0, 1+2*(end-start))
		args = append(args, key)
		for i := start; i < end; i++ {
			args = append(args, fmt.Sprintf("field:%04d", i), fmt.Sprintf("value:%04d", i))
		}
		if err := w.exec("hset", key, args...); err != nil {
			return err
		}
	}
	return nil
}

func writeBigList(w *clusterWriter, key string, items int) error {
	const batchSize = 256
	for start := 0; start < items; start += batchSize {
		end := min(start+batchSize, items)
		args := make([]interface{}, 0, 1+(end-start))
		args = append(args, key)
		for i := start; i < end; i++ {
			args = append(args, fmt.Sprintf("item:%05d", i))
		}
		if err := w.exec("rpush", key, args...); err != nil {
			return err
		}
	}
	return nil
}

func writeBigSet(w *clusterWriter, key string, members int) error {
	const batchSize = 256
	for start := 0; start < members; start += batchSize {
		end := min(start+batchSize, members)
		args := make([]interface{}, 0, 1+(end-start))
		args = append(args, key)
		for i := start; i < end; i++ {
			args = append(args, fmt.Sprintf("member:%05d", i))
		}
		if err := w.exec("sadd", key, args...); err != nil {
			return err
		}
	}
	return nil
}

func writeBigZSet(w *clusterWriter, key string, members int) error {
	const batchSize = 128
	for start := 0; start < members; start += batchSize {
		end := min(start+batchSize, members)
		args := make([]interface{}, 0, 1+2*(end-start))
		args = append(args, key)
		for i := start; i < end; i++ {
			args = append(args, strconv.Itoa(i), fmt.Sprintf("member:%05d", i))
		}
		if err := w.exec("zadd", key, args...); err != nil {
			return err
		}
	}
	return nil
}

func runSoak(left, right *clusterWriter, prefix string, duration time.Duration, keySpace int, largePayloadBytes int, throttle time.Duration, boundaryEvery, volatileEvery, txnEvery int, targetQPS int) error {
	startedAt := time.Now()
	deadline := time.Now().Add(duration)
	seq := 0
	for time.Now().Before(deadline) {
		if err := runSoakIteration(left, right, prefix, keySpace, largePayloadBytes, boundaryEvery, volatileEvery, txnEvery, seq); err != nil {
			return err
		}
		seq++
		if throttle > 0 {
			time.Sleep(throttle)
		}
		if targetQPS > 0 {
			throttleToTargetQPS(startedAt, left.stats.commandCount()+right.stats.commandCount(), targetQPS)
		}
	}
	return nil
}

func runSoakConcurrent(left, right *clusterWriter, leftAddrs, rightAddrs string, prefix string, duration time.Duration, keySpace int, largePayloadBytes int, throttle time.Duration, boundaryEvery, volatileEvery, txnEvery int, targetQPS int, workers int) error {
	if workers <= 1 {
		return runSoak(left, right, prefix, duration, keySpace, largePayloadBytes, throttle, boundaryEvery, volatileEvery, txnEvery, targetQPS)
	}

	type writerPair struct {
		left  *clusterWriter
		right *clusterWriter
	}
	pairs := make([]writerPair, 0, workers)
	pairs = append(pairs, writerPair{left: left, right: right})
	for i := 1; i < workers; i++ {
		lw, err := newClusterWriterWithStats("left", leftAddrs, left.stats)
		if err != nil {
			return err
		}
		defer lw.close()
		rw, err := newClusterWriterWithStats("right", rightAddrs, right.stats)
		if err != nil {
			return err
		}
		defer rw.close()
		pairs = append(pairs, writerPair{left: lw, right: rw})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startedAt := time.Now()
	deadline := startedAt.Add(duration)
	errCh := make(chan error, workers)
	doneCh := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64

	for _, pair := range pairs {
		pair := pair
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
				if err := runSoakIteration(pair.left, pair.right, prefix, keySpace, largePayloadBytes, boundaryEvery, volatileEvery, txnEvery, current); err != nil {
					cancel()
					errCh <- err
					return
				}
				if throttle > 0 {
					time.Sleep(throttle)
				}
				if targetQPS > 0 {
					throttleToTargetQPS(startedAt, left.stats.commandCount()+right.stats.commandCount(), targetQPS)
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

func runSoakIteration(left, right *clusterWriter, prefix string, keySpace int, largePayloadBytes int, boundaryEvery, volatileEvery, txnEvery int, seq int) error {
	var w *clusterWriter
	if seq%2 == 0 {
		w = left
	} else {
		w = right
	}

	base := fmt.Sprintf("%s:stable:soak:%s:%02d", prefix, w.name, seq%keySpace)
	sideSeq := seq / 2
	memberID := sideSeq % 512
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
	if err := w.do("sadd", base+":set", fmt.Sprintf("member:%03d", memberID)); err != nil {
		return err
	}
	if err := w.do("zadd", base+":zset", strconv.Itoa(seq), fmt.Sprintf("member:%06d", seq)); err != nil {
		return err
	}
	if err := w.do("zremrangebyrank", base+":zset", "0", "-257"); err != nil {
		return err
	}
	if seq%16 == 0 {
		if err := w.do("set", base+":big-string", buildPayload(largePayloadBytes, fmt.Sprintf("%s-%06d", w.name, seq))); err != nil {
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
	if boundaryEvery > 0 && sideSeq%boundaryEvery == 0 {
		if err := writeBoundarySet(w, prefix, sideSeq, largePayloadBytes); err != nil {
			return err
		}
	}
	if volatileEvery > 0 && sideSeq%volatileEvery == 0 {
		if err := writeVolatileSet(w, prefix, sideSeq); err != nil {
			return err
		}
	}
	if txnEvery > 0 && sideSeq%txnEvery == 0 {
		if err := writeTxnSet(w, prefix, sideSeq); err != nil {
			return err
		}
	}

	w.stats.addIteration()
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

func writeBoundarySet(w *clusterWriter, prefix string, seq int, largePayloadBytes int) error {
	keyBase := fmt.Sprintf("%s:stable:boundary:%s:%06d", prefix, w.name, seq)
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
	if err := w.do("set", keyBase+":large-string", buildPayload(largePayloadBytes, fmt.Sprintf("%s-boundary-%06d", w.name, seq))); err != nil {
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
	keyBase := fmt.Sprintf("%s:volatile:%s:%06d", prefix, w.name, seq)
	if err := w.do("set", keyBase+":ttl-short", "short-lived", "px", "1500"); err != nil {
		return err
	}
	if err := w.do("set", keyBase+":ttl-persist", "persist-me", "ex", "3600"); err != nil {
		return err
	}
	if err := w.do("persist", keyBase+":ttl-persist"); err != nil {
		return err
	}
	if err := w.do("xadd", keyBase+":stream", "maxlen", "~", "128", "*", "side", w.name, "seq", strconv.Itoa(seq)); err != nil {
		return err
	}
	return nil
}

func writeTxnSet(w *clusterWriter, prefix string, seq int) error {
	tag := fmt.Sprintf("{txn-%s-%04d}", w.name, seq%1024)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
