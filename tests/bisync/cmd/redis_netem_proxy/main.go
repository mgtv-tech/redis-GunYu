package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type proxyConfig struct {
	listen   string
	upstream string
	latency  time.Duration
	jitter   time.Duration
	rewrite  map[string]string
}

type respValue interface{}

type respSimpleString struct {
	Value string
}

type respError struct {
	Value string
}

type respInteger struct {
	Value int64
}

type respBulkString struct {
	Null  bool
	Value []byte
}

type respArray struct {
	Null   bool
	Values []respValue
}

type delaySampler struct {
	latency time.Duration
	jitter  time.Duration
	rng     *rand.Rand
	mu      sync.Mutex
}

var connCounter atomic.Uint64

func main() {
	var (
		listen      string
		upstream    string
		latency     time.Duration
		jitter      time.Duration
		rewriteSpec string
		seed        int64
	)

	flag.StringVar(&listen, "listen", "", "proxy listen address")
	flag.StringVar(&upstream, "upstream", "", "upstream redis address")
	flag.DurationVar(&latency, "latency", 0, "base one-way latency per forwarded frame")
	flag.DurationVar(&jitter, "jitter", 0, "uniform one-way jitter added/subtracted from latency")
	flag.StringVar(&rewriteSpec, "rewrite-map", "", "comma-separated upstream=proxy address mappings")
	flag.Int64Var(&seed, "seed", time.Now().UnixNano(), "random seed for jitter")
	flag.Parse()

	if listen == "" || upstream == "" {
		fmt.Fprintln(os.Stderr, "--listen and --upstream are required")
		os.Exit(2)
	}
	if jitter < 0 {
		fmt.Fprintln(os.Stderr, "--jitter must be >= 0")
		os.Exit(2)
	}
	if latency < 0 {
		fmt.Fprintln(os.Stderr, "--latency must be >= 0")
		os.Exit(2)
	}

	cfg := proxyConfig{
		listen:   listen,
		upstream: upstream,
		latency:  latency,
		jitter:   jitter,
		rewrite:  parseRewriteMap(rewriteSpec),
	}

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatalf("listen %s failed: %v", cfg.listen, err)
	}
	defer ln.Close()

	log.Printf("redis_netem_proxy listening on %s -> %s latency=%s jitter=%s mappings=%d", cfg.listen, cfg.upstream, cfg.latency, cfg.jitter, len(cfg.rewrite))
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("accept failed: %v", err)
			continue
		}
		go handleClient(clientConn, cfg, seed+int64(connCounter.Add(1)))
	}
}

func parseRewriteMap(spec string) map[string]string {
	out := make(map[string]string)
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid rewrite pair %q", pair)
		}
		left := strings.TrimSpace(parts[0])
		right := strings.TrimSpace(parts[1])
		if left == "" || right == "" {
			log.Fatalf("invalid rewrite pair %q", pair)
		}
		out[left] = right
	}
	return out
}

func handleClient(clientConn net.Conn, cfg proxyConfig, seed int64) {
	upstreamConn, err := net.Dial("tcp", cfg.upstream)
	if err != nil {
		log.Printf("dial upstream %s failed: %v", cfg.upstream, err)
		_ = clientConn.Close()
		return
	}

	sampler := &delaySampler{
		latency: cfg.latency,
		jitter:  cfg.jitter,
		rng:     rand.New(rand.NewSource(seed)),
	}

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			_ = clientConn.Close()
			_ = upstreamConn.Close()
		})
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- copyRawWithDelay(upstreamConn, clientConn, sampler)
	}()
	go func() {
		errCh <- copyRedisReplies(clientConn, upstreamConn, sampler, cfg.rewrite)
	}()

	err = <-errCh
	closeBoth()
	<-errCh

	if err != nil && !errors.Is(err, io.EOF) && !isClosedConn(err) {
		log.Printf("proxy %s -> %s stopped with error: %v", cfg.listen, cfg.upstream, err)
	}
}

func copyRawWithDelay(dst net.Conn, src net.Conn, sampler *delaySampler) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if delay := sampler.next(); delay > 0 {
				time.Sleep(delay)
			}
			if writeErr := writeAll(dst, buf[:n]); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				closeWrite(dst)
			}
			return err
		}
	}
}

func copyRedisReplies(dst net.Conn, src net.Conn, sampler *delaySampler, rewrite map[string]string) error {
	br := bufio.NewReader(src)
	bw := bufio.NewWriter(dst)
	for {
		value, err := readRESP(br)
		if err != nil {
			if err == io.EOF {
				closeWrite(dst)
			}
			return err
		}
		value = rewriteRESP(value, rewrite)
		if delay := sampler.next(); delay > 0 {
			time.Sleep(delay)
		}
		if err := writeRESP(bw, value); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}
}

func (s *delaySampler) next() time.Duration {
	if s.latency <= 0 && s.jitter <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delay := s.latency
	if s.jitter > 0 {
		delta := time.Duration(s.rng.Int63n(int64(s.jitter)*2+1)) - s.jitter
		delay += delta
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func rewriteRESP(value respValue, mapping map[string]string) respValue {
	switch v := value.(type) {
	case respSimpleString:
		v.Value = rewriteStringPayload(v.Value, mapping)
		return v
	case respError:
		v.Value = rewriteErrorPayload(v.Value, mapping)
		return v
	case respBulkString:
		if !v.Null {
			v.Value = []byte(rewriteStringPayload(string(v.Value), mapping))
		}
		return v
	case respArray:
		if v.Null {
			return v
		}
		if looksLikeClusterSlots(v) {
			return rewriteClusterSlots(v, mapping)
		}
		for i, item := range v.Values {
			v.Values[i] = rewriteRESP(item, mapping)
		}
		return v
	default:
		return value
	}
}

func rewriteErrorPayload(msg string, mapping map[string]string) string {
	fields := strings.Fields(msg)
	if len(fields) != 3 {
		return msg
	}
	switch fields[0] {
	case "MOVED", "ASK":
		if mapped, ok := mapping[fields[2]]; ok {
			fields[2] = mapped
			return strings.Join(fields, " ")
		}
	}
	return msg
}

func rewriteStringPayload(s string, mapping map[string]string) string {
	for upstream, proxy := range mapping {
		s = strings.ReplaceAll(s, upstream, proxy)
	}
	return s
}

func looksLikeClusterSlots(arr respArray) bool {
	if arr.Null || len(arr.Values) == 0 {
		return false
	}
	for _, slotRange := range arr.Values {
		inner, ok := slotRange.(respArray)
		if !ok || inner.Null || len(inner.Values) < 3 {
			return false
		}
		if _, ok := inner.Values[0].(respInteger); !ok {
			return false
		}
		if _, ok := inner.Values[1].(respInteger); !ok {
			return false
		}
		nodeDesc, ok := inner.Values[2].(respArray)
		if !ok || nodeDesc.Null || len(nodeDesc.Values) < 2 {
			return false
		}
		if !respIsString(nodeDesc.Values[0]) {
			return false
		}
		if _, ok := nodeDesc.Values[1].(respInteger); !ok {
			return false
		}
	}
	return true
}

func rewriteClusterSlots(arr respArray, mapping map[string]string) respArray {
	for i, slotRange := range arr.Values {
		inner, ok := slotRange.(respArray)
		if !ok || inner.Null {
			continue
		}
		for nodeIdx := 2; nodeIdx < len(inner.Values); nodeIdx++ {
			nodeDesc, ok := inner.Values[nodeIdx].(respArray)
			if !ok || nodeDesc.Null || len(nodeDesc.Values) < 2 {
				continue
			}
			host, ok := respString(nodeDesc.Values[0])
			if !ok {
				continue
			}
			port, ok := respInt(nodeDesc.Values[1])
			if !ok {
				continue
			}
			mapped, exists := mapping[fmt.Sprintf("%s:%d", host, port)]
			if !exists {
				continue
			}
			mappedHost, mappedPort, err := splitMappedAddr(mapped)
			if err != nil {
				continue
			}
			nodeDesc.Values[0] = hostValueLike(nodeDesc.Values[0], mappedHost)
			nodeDesc.Values[1] = respInteger{Value: int64(mappedPort)}
			inner.Values[nodeIdx] = nodeDesc
		}
		arr.Values[i] = inner
	}
	return arr
}

func splitMappedAddr(addr string) (string, int, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

func respIsString(value respValue) bool {
	switch value.(type) {
	case respSimpleString, respBulkString:
		return true
	default:
		return false
	}
}

func respString(value respValue) (string, bool) {
	switch v := value.(type) {
	case respSimpleString:
		return v.Value, true
	case respBulkString:
		if v.Null {
			return "", false
		}
		return string(v.Value), true
	default:
		return "", false
	}
}

func respInt(value respValue) (int64, bool) {
	v, ok := value.(respInteger)
	if !ok {
		return 0, false
	}
	return v.Value, true
}

func hostValueLike(template respValue, host string) respValue {
	switch template.(type) {
	case respSimpleString:
		return respSimpleString{Value: host}
	default:
		return respBulkString{Value: []byte(host)}
	}
}

func readRESP(br *bufio.Reader) (respValue, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("invalid empty RESP frame")
	}

	switch line[0] {
	case '+':
		return respSimpleString{Value: string(line[1:])}, nil
	case '-':
		return respError{Value: string(line[1:])}, nil
	case ':':
		n, err := parseInt(line[1:])
		if err != nil {
			return nil, err
		}
		return respInteger{Value: n}, nil
	case '$':
		n, err := parseLen(line[1:])
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return respBulkString{Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return nil, errors.New("invalid bulk string terminator")
		}
		return respBulkString{Value: buf[:n]}, nil
	case '*':
		n, err := parseLen(line[1:])
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return respArray{Null: true}, nil
		}
		values := make([]respValue, n)
		for i := 0; i < n; i++ {
			item, err := readRESP(br)
			if err != nil {
				return nil, err
			}
			values[i] = item
		}
		return respArray{Values: values}, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", line[0])
	}
}

func writeRESP(w *bufio.Writer, value respValue) error {
	switch v := value.(type) {
	case respSimpleString:
		_, err := w.WriteString("+" + v.Value + "\r\n")
		return err
	case respError:
		_, err := w.WriteString("-" + v.Value + "\r\n")
		return err
	case respInteger:
		_, err := w.WriteString(":" + strconv.FormatInt(v.Value, 10) + "\r\n")
		return err
	case respBulkString:
		if v.Null {
			_, err := w.WriteString("$-1\r\n")
			return err
		}
		if _, err := w.WriteString("$" + strconv.Itoa(len(v.Value)) + "\r\n"); err != nil {
			return err
		}
		if _, err := w.Write(v.Value); err != nil {
			return err
		}
		_, err := w.WriteString("\r\n")
		return err
	case respArray:
		if v.Null {
			_, err := w.WriteString("*-1\r\n")
			return err
		}
		if _, err := w.WriteString("*" + strconv.Itoa(len(v.Values)) + "\r\n"); err != nil {
			return err
		}
		for _, item := range v.Values {
			if err := writeRESP(w, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported RESP value type %T", value)
	}
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("invalid RESP line terminator")
	}
	return line[:len(line)-2], nil
}

func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, errors.New("empty length")
	}
	if string(p) == "-1" {
		return -1, nil
	}
	n, err := strconv.Atoi(string(p))
	if err != nil {
		return 0, err
	}
	return n, nil
}

func parseInt(p []byte) (int64, error) {
	return strconv.ParseInt(string(p), 10, 64)
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

func closeWrite(conn net.Conn) {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func isClosedConn(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") || strings.Contains(msg, "broken pipe")
}
