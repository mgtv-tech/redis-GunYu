package keyspec

import "strings"

// getkeysProc returns the indexes of key arguments for a command.
type getkeysProc func(args []string) []int

// redisKeyPosition describes the key position rule for a command.
// first/last/step follow the common key-spec semantics from the Redis COMMAND documentation:
// first is the 1-based position of the first key, last is the position of the last key,
// and step is the interval between keys.
type redisKeyPosition struct {
	first, last, step int
}

var genericKeyPos = redisKeyPosition{1, 1, 1}

// commandKeyPositions handles commands whose key positions are fixed.
// For these commands, all key arguments can be derived directly from first/last/step.
var commandKeyPositions = map[string]redisKeyPosition{
	"set":                  genericKeyPos,
	"setnx":                genericKeyPos,
	"setex":                genericKeyPos,
	"psetex":               genericKeyPos,
	"getdel":               genericKeyPos,
	"getex":                genericKeyPos,
	"append":               genericKeyPos,
	"setbit":               genericKeyPos,
	"bitfield":             genericKeyPos,
	"setrange":             genericKeyPos,
	"move":                 genericKeyPos,
	"incr":                 genericKeyPos,
	"decr":                 genericKeyPos,
	"rpush":                genericKeyPos,
	"lpush":                genericKeyPos,
	"rpushx":               genericKeyPos,
	"lpushx":               genericKeyPos,
	"linsert":              genericKeyPos,
	"rpop":                 genericKeyPos,
	"lpop":                 genericKeyPos,
	"brpop":                {1, -2, 1},
	"brpoplpush":           {1, 2, 1},
	"blpop":                {1, -2, 1},
	"lset":                 genericKeyPos,
	"ltrim":                genericKeyPos,
	"lrem":                 genericKeyPos,
	"rpoplpush":            {1, 2, 1},
	"lmove":                {1, 2, 1},
	"blmove":               {1, 2, 1},
	"sadd":                 genericKeyPos,
	"srem":                 genericKeyPos,
	"smove":                {1, 2, 1},
	"spop":                 genericKeyPos,
	"sinterstore":          {1, -1, 1},
	"sunionstore":          {1, -1, 1},
	"sdiffstore":           {1, -1, 1},
	"zadd":                 genericKeyPos,
	"zincrby":              genericKeyPos,
	"zrem":                 genericKeyPos,
	"zremrangebyscore":     genericKeyPos,
	"zremrangebyrank":      genericKeyPos,
	"zremrangebylex":       genericKeyPos,
	"hset":                 genericKeyPos,
	"hsetnx":               genericKeyPos,
	"hsetex":               genericKeyPos,
	"hmset":                genericKeyPos,
	"hgetdel":              genericKeyPos,
	"hgetex":               genericKeyPos,
	"hincrby":              genericKeyPos,
	"hincrbyfloat":         genericKeyPos,
	"hdel":                 genericKeyPos,
	"incrby":               genericKeyPos,
	"decrby":               genericKeyPos,
	"incrbyfloat":          genericKeyPos,
	"getset":               genericKeyPos,
	"delex":                genericKeyPos,
	"mset":                 {1, -1, 2},
	"msetnx":               {1, -1, 2},
	"rename":               {1, 2, 1},
	"renamenx":             {1, 2, 1},
	"copy":                 {1, 2, 1},
	"expire":               genericKeyPos,
	"expireat":             genericKeyPos,
	"pexpire":              genericKeyPos,
	"pexpireat":            genericKeyPos,
	"persist":              genericKeyPos,
	"restore":              genericKeyPos,
	"restore-asking":       genericKeyPos,
	"bitop":                {2, -1, 1},
	"geoadd":               genericKeyPos,
	"geosearchstore":       {1, 2, 1},
	"pfadd":                genericKeyPos,
	"pfmerge":              {1, -1, 1},
	"xadd":                 genericKeyPos,
	"xdel":                 genericKeyPos,
	"xtrim":                genericKeyPos,
	"xack":                 genericKeyPos,
	"xackdel":              genericKeyPos,
	"xclaim":               genericKeyPos,
	"xautoclaim":           genericKeyPos,
	"xdelex":               genericKeyPos,
	"xsetid":               genericKeyPos,
	"zrangestore":          {1, 2, 1},
	"zpopmin":              genericKeyPos,
	"zpopmax":              genericKeyPos,
	"bzpopmin":             {1, -2, 1},
	"bzpopmax":             {1, -2, 1},
	"hexpire":              genericKeyPos,
	"hpexpire":             genericKeyPos,
	"hexpireat":            genericKeyPos,
	"hpexpireat":           genericKeyPos,
	"hpersist":             genericKeyPos,
	"json.arrappend":       genericKeyPos,
	"json.arrinsert":       genericKeyPos,
	"json.arrpop":          genericKeyPos,
	"json.arrtrim":         genericKeyPos,
	"json.clear":           genericKeyPos,
	"json.del":             genericKeyPos,
	"json.forget":          genericKeyPos,
	"json.merge":           genericKeyPos,
	"json.mset":            {1, -1, 3},
	"json.numincrby":       genericKeyPos,
	"json.nummultby":       genericKeyPos,
	"json.set":             genericKeyPos,
	"json.strappend":       genericKeyPos,
	"json.toggle":          genericKeyPos,
	"bf.add":               genericKeyPos,
	"bf.madd":              genericKeyPos,
	"bf.insert":            genericKeyPos,
	"cf.add":               genericKeyPos,
	"cf.addnx":             genericKeyPos,
	"cf.insert":            genericKeyPos,
	"cf.insertnx":          genericKeyPos,
	"cms.incrby":           genericKeyPos,
	"cms.initbydim":        genericKeyPos,
	"cms.initbyprob":       genericKeyPos,
	"tdigest.add":          genericKeyPos,
	"tdigest.byrevrank":    genericKeyPos,
	"tdigest.byrevscore":   genericKeyPos,
	"tdigest.create":       genericKeyPos,
	"tdigest.cdf":          genericKeyPos,
	"tdigest.incrby":       genericKeyPos,
	"tdigest.max":          genericKeyPos,
	"tdigest.min":          genericKeyPos,
	"tdigest.quantile":     genericKeyPos,
	"tdigest.rank":         genericKeyPos,
	"tdigest.reset":        genericKeyPos,
	"tdigest.revrank":      genericKeyPos,
	"tdigest.trimmed_mean": genericKeyPos,
	"topk.add":             genericKeyPos,
	"topk.incrby":          genericKeyPos,
	"topk.list":            genericKeyPos,
	"topk.reserve":         genericKeyPos,
	"ft.create":            genericKeyPos,
	"ft.search":            genericKeyPos,
	"ft.dropindex":         genericKeyPos,
	"del":                  {1, 0, 1},
	"unlink":               {1, -1, 1},
}

// commandKeyExtractors handles commands whose key positions cannot be described by a fixed rule.
// For example, EVAL depends on numkeys, XREADGROUP depends on the STREAMS marker,
// and SORT/GEORADIUS depend on optional sub-arguments.
var commandKeyExtractors = map[string]getkeysProc{
	"eval":              numkeysExtractor(1, 2),
	"evalsha":           numkeysExtractor(1, 2),
	"fcall":             numkeysExtractor(1, 2),
	"fcall_ro":          numkeysExtractor(1, 2),
	"msetex":            numkeysStepExtractor(0, 1, 2),
	"zunionstore":       numkeysExtractor(1, 2, 0),
	"zinterstore":       numkeysExtractor(1, 2, 0),
	"zdiffstore":        numkeysExtractor(1, 2, 0),
	"cms.merge":         fixedKeyExtractor(0),
	"tdigest.merge":     fixedKeyExtractor(0),
	"georadius":         geoRadiusStoreExtractor,
	"georadiusbymember": geoRadiusStoreExtractor,
	"xgroup":            xgroupExtractor,
	"xreadgroup":        streamsExtractor,
	"sort":              sortExtractor,
	"zmpop":             numkeysExtractor(0, 1),
	"bzmpop":            numkeysExtractor(1, 2),
	"lmpop":             numkeysExtractor(0, 1),
	"blmpop":            numkeysExtractor(1, 2),
}

// numkeysExtractor is used for commands that specify the number of keys first,
// followed by a contiguous key list.
// fixedKeys can be used to include additional fixed key positions, such as a STORE destination key.
func numkeysExtractor(numkeysIdx int, firstKeyIdx int, fixedKeys ...int) getkeysProc {
	return numkeysStepExtractor(numkeysIdx, firstKeyIdx, 1, fixedKeys...)
}

// fixedKeyExtractor returns a stable list of 0-based key argument indexes.
// It is used for commands where Redis only treats selected arguments as keys
// even if other key-like arguments follow later in the command.
func fixedKeyExtractor(indexes ...int) getkeysProc {
	return func(args []string) []int {
		keys := make([]int, 0, len(indexes))
		for _, idx := range indexes {
			if idx < 0 || idx >= len(args) {
				return nil
			}
			keys = append(keys, idx)
		}
		return keys
	}
}

// numkeysStepExtractor extends numkeysExtractor to support keys distributed with a fixed step.
// It first validates numkeys and argument bounds, then combines fixed keys and dynamic keys.
// Any out-of-range index causes extraction to fail immediately.
func numkeysStepExtractor(numkeysIdx int, firstKeyIdx int, keyStep int, fixedKeys ...int) getkeysProc {
	return func(args []string) []int {
		if numkeysIdx < 0 || numkeysIdx >= len(args) {
			return nil
		}
		if keyStep <= 0 {
			return nil
		}
		numkeys := parseCommandInt(args[numkeysIdx])
		if numkeys <= 0 {
			return nil
		}
		lastKeyIdx := firstKeyIdx + (numkeys-1)*keyStep
		if firstKeyIdx < 0 || lastKeyIdx >= len(args) {
			return nil
		}
		keys := make([]int, 0, len(fixedKeys)+numkeys)
		for _, idx := range fixedKeys {
			if idx < 0 || idx >= len(args) {
				return nil
			}
			keys = append(keys, idx)
		}
		for idx := firstKeyIdx; idx <= lastKeyIdx; idx += keyStep {
			keys = append(keys, idx)
		}
		return keys
	}
}

// parseCommandInt parses a non-negative integer from a command argument.
// It only accepts digit-only strings and returns -1 on any invalid character,
// to avoid misinterpreting non-key arguments as count fields.
func parseCommandInt(arg string) int {
	v := 0
	for i := 0; i < len(arg); i++ {
		if arg[i] < '0' || arg[i] > '9' {
			return -1
		}
		v = v*10 + int(arg[i]-'0')
	}
	return v
}

// xgroupExtractor extracts the stream key from XGROUP subcommands.
// Supported subcommands always place the key at the second argument.
func xgroupExtractor(args []string) []int {
	if len(args) < 2 {
		return nil
	}
	switch strings.ToLower(args[0]) {
	case "create", "setid", "destroy", "createconsumer", "delconsumer":
		return []int{1}
	default:
		return nil
	}
}

// streamsExtractor extracts the key list that follows the STREAMS marker in
// commands such as XREADGROUP/XREAD.
// These commands arrange arguments after STREAMS as "keys + ids", so the number
// of keys is half of the remaining arguments.
func streamsExtractor(args []string) []int {
	marker := -1
	for i, arg := range args {
		if strings.EqualFold(arg, "streams") {
			marker = i
			break
		}
	}
	if marker == -1 || marker+2 > len(args) {
		return nil
	}
	// STREAMS must be followed by at least one key and its matching id.
	keyStart := marker + 1
	keyCount := (len(args) - keyStart) / 2
	if keyCount <= 0 || keyStart+keyCount > len(args) {
		return nil
	}
	keys := make([]int, 0, keyCount)
	for idx := keyStart; idx < keyStart+keyCount; idx++ {
		keys = append(keys, idx)
	}
	return keys
}

// sortExtractor extracts the source key and optional STORE destination key from SORT.
// It only accepts BY/GET forms that do not introduce additional keys, namely # or nosort.
// Otherwise it returns nil to avoid incorrect key projection.
func sortExtractor(args []string) []int {
	if len(args) == 0 {
		return nil
	}
	keys := []int{0}
	hasStore := false
	for i := 1; i < len(args); i++ {
		switch {
		case strings.EqualFold(args[i], "store"):
			if i+1 >= len(args) {
				return nil
			}
			// The STORE destination is also a key accessed by the command.
			keys = append(keys, i+1)
			hasStore = true
			i++
		case strings.EqualFold(args[i], "by"):
			if i+1 >= len(args) {
				return nil
			}
			pattern := args[i+1]
			if !strings.EqualFold(pattern, "#") && !strings.EqualFold(pattern, "nosort") {
				return nil
			}
			i++
		case strings.EqualFold(args[i], "get"):
			if i+1 >= len(args) {
				return nil
			}
			if !strings.EqualFold(args[i+1], "#") {
				return nil
			}
			i++
		}
	}
	// Only return keys when STORE is present, so callers can decide whether
	// partial projection is allowed.
	if !hasStore {
		return nil
	}
	return keys
}

// geoRadiusStoreExtractor extracts the source key and STORE/STOREDIST destination key
// from GEORADIUS/GEORADIUSBYMEMBER.
// These commands only access multiple keys when a store target is present,
// so it returns nil if no STORE-related argument is found.
func geoRadiusStoreExtractor(args []string) []int {
	if len(args) == 0 {
		return nil
	}
	keys := []int{0}
	hasStore := false
	for i := 1; i < len(args)-1; i++ {
		if strings.EqualFold(args[i], "store") || strings.EqualFold(args[i], "storedist") {
			keys = append(keys, i+1)
			hasStore = true
			break
		}
	}
	if !hasStore {
		return nil
	}
	return keys
}

// CommandKeys returns the list of key strings accessed by the command.
// It reuses CommandKeyIndexes to compute indexes first, then converts [][]byte
// into strings so both interfaces share the same extraction logic.
func CommandKeys(cmd string, args [][]byte) ([]string, bool) {
	indexes, ok := CommandKeyIndexes(cmd, args)
	if !ok || len(indexes) == 0 {
		return nil, false
	}
	keys := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		if idx < 0 || idx >= len(args) {
			return nil, false
		}
		keys = append(keys, string(args[idx]))
	}
	return keys, true
}

// CommandKeyIndexes returns the 0-based indexes of keys accessed by the command in args.
// It first tries a custom extractor, then falls back to the fixed position table.
// Any invalid argument layout causes it to return false.
func CommandKeyIndexes(cmd string, args [][]byte) ([]int, bool) {
	lc := strings.ToLower(cmd)
	if len(args) == 0 {
		return nil, false
	}

	if proc, ok := commandKeyExtractors[lc]; ok {
		// Custom extractors operate on string arguments for case-insensitive matching
		// and numeric parsing.
		strArgs := make([]string, 0, len(args))
		for _, arg := range args {
			strArgs = append(strArgs, string(arg))
		}
		indexes := proc(strArgs)
		if len(indexes) == 0 {
			return nil, false
		}
		return indexes, true
	}

	cmdPos, ok := commandKeyPositions[lc]
	if !ok {
		return nil, false
	}

	var lastkey int
	switch {
	case cmdPos.last > 0:
		// A positive value means a fixed 1-based position.
		lastkey = cmdPos.last - 1
	case cmdPos.last == 0:
		// Zero means the last argument.
		lastkey = len(args) - 1
	default:
		// A negative value means an offset from the end, for example -1 means the last argument.
		lastkey = len(args) + cmdPos.last
	}
	if lastkey < 0 || lastkey >= len(args) || cmdPos.first <= 0 || cmdPos.step <= 0 {
		return nil, false
	}

	// first/step are described in 1-based form, so convert them to 0-based indexes
	// before collecting all keys in order.
	indexes := make([]int, 0, (lastkey-cmdPos.first+cmdPos.step)/cmdPos.step)
	for firstkey := cmdPos.first - 1; firstkey <= lastkey; firstkey += cmdPos.step {
		if firstkey < 0 || firstkey >= len(args) {
			return nil, false
		}
		indexes = append(indexes, firstkey)
	}
	if len(indexes) == 0 {
		return nil, false
	}
	return indexes, true
}

// CommandAllowsPartialProjection reports whether a command can be projected to a subset of keys.
// This is currently enabled only for multi-key write commands whose per-key effects are independent,
// to avoid splitting commands that require whole-command semantics.
func CommandAllowsPartialProjection(cmd string) bool {
	switch strings.ToLower(cmd) {
	case "mset", "del", "unlink":
		return true
	default:
		return false
	}
}
