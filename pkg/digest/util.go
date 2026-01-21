package digest

import "fmt"

const KClusterSlots = 16384

func Hash(key string) uint16 {
	var s, e int
	for s = 0; s < len(key); s++ {
		if key[s] == '{' {
			break
		}
	}

	if s == len(key) {
		return Crc16(key) & (KClusterSlots - 1)
	}

	for e = s + 1; e < len(key); e++ {
		if key[e] == '}' {
			break
		}
	}

	if e == len(key) || e == s+1 {
		return Crc16(key) & (KClusterSlots - 1)
	}

	return Crc16(key[s+1:e]) & (KClusterSlots - 1)
}

func GenerateKeyForSlot(prefix string, slot uint16) string {
	// 尝试不同的数字后缀，直到找到一个合适的键
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("%s_%d", prefix, i)
		if Hash(candidate) == slot {
			return candidate
		}
	}
}

var SlotKey map[uint16]string
