package util

import "hash/fnv"

func FnvHash(data []byte) uint32 {
	hash := fnv.New32a()
	hash.Write(data)
	return hash.Sum32()
}
