package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	redisclient "github.com/mgtv-tech/redis-GunYu/pkg/redis/client"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
)

type clusterSnapshot struct {
	keys    map[string]string
	masters []string
}

func main() {
	var (
		leftAddrs  string
		rightAddrs string
		pattern    string
		maxKeys    int
		verbose    bool
	)

	flag.StringVar(&leftAddrs, "left-addrs", "", "comma-separated startup addresses for the left cluster")
	flag.StringVar(&rightAddrs, "right-addrs", "", "comma-separated startup addresses for the right cluster")
	flag.StringVar(&pattern, "pattern", "", "key pattern to compare")
	flag.IntVar(&maxKeys, "max-keys", 0, "limit snapshot size to the first N sorted matching keys; 0 keeps all keys")
	flag.BoolVar(&verbose, "verbose", false, "print every matching key")
	flag.Parse()

	if leftAddrs == "" || rightAddrs == "" || pattern == "" {
		fmt.Fprintln(os.Stderr, "left-addrs, right-addrs and pattern are required")
		os.Exit(2)
	}

	left, err := snapshotCluster(splitAddrs(leftAddrs), pattern, maxKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "snapshot left cluster failed: %v\n", err)
		os.Exit(1)
	}
	right, err := snapshotCluster(splitAddrs(rightAddrs), pattern, maxKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "snapshot right cluster failed: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		printSnapshot("left", left)
		printSnapshot("right", right)
	}

	if err := compareSnapshots(left, right); err != nil {
		fmt.Fprintf(os.Stderr, "compare failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("compare ok: pattern=%s keys=%d\n", pattern, len(left.keys))
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

func snapshotCluster(startAddrs []string, pattern string, maxKeys int) (*clusterSnapshot, error) {
	if len(startAddrs) == 0 {
		return nil, fmt.Errorf("empty startup addresses")
	}

	masters, err := discoverMasters(startAddrs)
	if err != nil {
		return nil, err
	}

	cli, err := redisclient.NewRedis(config.RedisConfig{
		Addresses: startAddrs,
		Type:      config.RedisTypeCluster,
	})
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	allKeys := make([]string, 0)
	for _, addr := range masters {
		nodeKeys, err := scanKeys(addr, pattern)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", addr, err)
		}
		allKeys = append(allKeys, nodeKeys...)
	}
	sort.Strings(allKeys)
	if maxKeys > 0 && len(allKeys) > maxKeys {
		allKeys = allKeys[:maxKeys]
	}

	keys := make(map[string]string, len(allKeys))
	for _, key := range allKeys {
		state, err := readKeyState(cli, key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		keys[key] = state
	}

	return &clusterSnapshot{
		keys:    keys,
		masters: masters,
	}, nil
}

func discoverMasters(addrs []string) ([]string, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		for _, addr := range addrs {
			masters, err := discoverMastersOnce(addr)
			if err == nil {
				return masters, nil
			}
			lastErr = fmt.Errorf("discover %s: %w", addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no startup addresses")
	}
	return nil, lastErr
}

func discoverMastersOnce(addr string) ([]string, error) {
	cli, err := conn.NewRedisConn(config.RedisConfig{
		Addresses: []string{addr},
		Type:      config.RedisTypeStandalone,
	})
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	raw, err := common.String(cli.Do("cluster", "nodes"))
	if err != nil {
		return nil, err
	}

	masters := make(map[string]struct{})
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		flags := strings.Split(fields[2], ",")
		if !contains(flags, "master") || contains(flags, "fail") || contains(flags, "handshake") || contains(flags, "noaddr") {
			continue
		}
		addrField := fields[1]
		hostPort := strings.Split(addrField, "@")[0]
		masters[hostPort] = struct{}{}
	}

	out := make([]string, 0, len(masters))
	for addr := range masters {
		out = append(out, addr)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("no master nodes found from %s", addr)
	}
	return out, nil
}

func scanKeys(addr string, pattern string) ([]string, error) {
	cli, err := conn.NewRedisConn(config.RedisConfig{
		Addresses: []string{addr},
		Type:      config.RedisTypeStandalone,
	})
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	var keys []string
	cursor := "0"
	for {
		reply, err := common.Values(cli.Do("scan", cursor, "match", pattern, "count", "1000"))
		if err != nil {
			return nil, err
		}
		if len(reply) != 2 {
			return nil, fmt.Errorf("unexpected scan reply length %d", len(reply))
		}
		cursor, err = common.String(reply[0], nil)
		if err != nil {
			return nil, err
		}
		part, err := common.Strings(reply[1], nil)
		if err != nil {
			return nil, err
		}
		keys = append(keys, part...)
		if cursor == "0" {
			break
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func compareSnapshots(left, right *clusterSnapshot) error {
	leftOnly := diffKeys(left.keys, right.keys)
	rightOnly := diffKeys(right.keys, left.keys)
	if len(leftOnly) > 0 || len(rightOnly) > 0 {
		return fmt.Errorf("key set mismatch: left_only=%v right_only=%v", leftOnly, rightOnly)
	}

	keys := make([]string, 0, len(left.keys))
	for key := range left.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if left.keys[key] != right.keys[key] {
			return fmt.Errorf("value mismatch on key %s: left=%s right=%s", key, left.keys[key], right.keys[key])
		}
	}
	return nil
}

func readKeyState(cli redisclient.Redis, key string) (string, error) {
	typ, err := common.String(cli.Do("type", key))
	if err != nil {
		return "", err
	}

	switch typ {
	case "string":
		val, err := common.String(cli.Do("get", key))
		if err != nil {
			return "", err
		}
		return "string:" + val, nil
	case "hash":
		items, err := common.StringMap(cli.Do("hgetall", key))
		if err != nil {
			return "", err
		}
		keys := make([]string, 0, len(items))
		for field := range items {
			keys = append(keys, field)
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString("hash:")
		for i, field := range keys {
			if i > 0 {
				b.WriteString("|")
			}
			b.WriteString(field)
			b.WriteString("=")
			b.WriteString(items[field])
		}
		return b.String(), nil
	case "list":
		items, err := common.Strings(cli.Do("lrange", key, 0, -1))
		if err != nil {
			return "", err
		}
		return "list:" + strings.Join(items, "|"), nil
	case "set":
		items, err := common.Strings(cli.Do("smembers", key))
		if err != nil {
			return "", err
		}
		sort.Strings(items)
		return "set:" + strings.Join(items, "|"), nil
	case "zset":
		items, err := common.Strings(cli.Do("zrange", key, 0, -1, "withscores"))
		if err != nil {
			return "", err
		}
		return "zset:" + strings.Join(items, "|"), nil
	case "stream":
		items, err := common.Values(cli.Do("xrange", key, "-", "+"))
		if err != nil {
			return "", err
		}
		return formatStreamState(items)
	case "none":
		return "none", nil
	default:
		return "", fmt.Errorf("unsupported redis type %s for key %s", typ, key)
	}
}

func formatStreamState(items []interface{}) (string, error) {
	var b strings.Builder
	b.WriteString("stream:")
	for i, item := range items {
		entry, err := common.Values(item, nil)
		if err != nil {
			return "", fmt.Errorf("stream entry %d: %w", i, err)
		}
		if len(entry) != 2 {
			return "", fmt.Errorf("stream entry %d: unexpected reply length %d", i, len(entry))
		}

		id, err := common.String(entry[0], nil)
		if err != nil {
			return "", fmt.Errorf("stream entry %d id: %w", i, err)
		}
		fields, err := common.Values(entry[1], nil)
		if err != nil {
			return "", fmt.Errorf("stream entry %s fields: %w", id, err)
		}
		if len(fields)%2 != 0 {
			return "", fmt.Errorf("stream entry %s has odd field/value list length %d", id, len(fields))
		}

		if i > 0 {
			b.WriteString("|")
		}
		b.WriteString(quoteStatePart(id))
		b.WriteString("[")
		for j := 0; j < len(fields); j += 2 {
			field, err := common.String(fields[j], nil)
			if err != nil {
				return "", fmt.Errorf("stream entry %s field %d: %w", id, j/2, err)
			}
			value, err := common.String(fields[j+1], nil)
			if err != nil {
				return "", fmt.Errorf("stream entry %s value %d: %w", id, j/2, err)
			}
			if j > 0 {
				b.WriteString(",")
			}
			b.WriteString(quoteStatePart(field))
			b.WriteString("=")
			b.WriteString(quoteStatePart(value))
		}
		b.WriteString("]")
	}
	return b.String(), nil
}

func quoteStatePart(value string) string {
	return strconv.QuoteToASCII(value)
}

func diffKeys(left, right map[string]string) []string {
	var out []string
	for key := range left {
		if _, ok := right[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func printSnapshot(name string, snap *clusterSnapshot) {
	fmt.Printf("%s masters=%v keys=%d\n", name, snap.masters, len(snap.keys))
	keys := make([]string, 0, len(snap.keys))
	for key := range snap.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s %s\n", name, key)
	}
}
