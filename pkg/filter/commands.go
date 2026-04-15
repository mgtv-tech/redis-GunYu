package filter

import "github.com/mgtv-tech/redis-GunYu/pkg/redis/keyspec"

func CommandKeys(cmd string, args [][]byte) ([]string, bool) {
	return keyspec.CommandKeys(cmd, args)
}

func CommandKeyIndexes(cmd string, args [][]byte) ([]int, bool) {
	return keyspec.CommandKeyIndexes(cmd, args)
}

func CommandAllowsPartialProjection(cmd string) bool {
	return keyspec.CommandAllowsPartialProjection(cmd)
}
