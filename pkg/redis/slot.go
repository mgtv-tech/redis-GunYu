package redis

import "github.com/mgtv-tech/redis-GunYu/pkg/digest"

type SlotOwner struct {
	Master            string
	Slave             []string
	SlotLeftBoundary  int
	SlotRightBoundary int
}

func KeyToSlot(key string) uint16 {
	hashtag := ""
	for i, s := range key {
		if s == '{' {
			for k := i; k < len(key); k++ {
				if key[k] == '}' {
					hashtag = key[i+1 : k]
					break
				}
			}
		}
	}
	if len(hashtag) > 0 {
		return digest.Crc16(hashtag) & 0x3fff
	}
	return digest.Crc16(key) & 0x3fff
}
