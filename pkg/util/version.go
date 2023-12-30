package util

import (
	"strconv"
	"strings"
)

type VersionSemantic int

const (
	VersionMajor VersionSemantic = 0
	VersionMinor VersionSemantic = 1
	VersionPatch VersionSemantic = 2
)

// >=
func VersionGE(a, b string, semantic VersionSemantic) bool {
	return compareVersion(a, b, semantic) >= 0
}

func VersionLE(a, b string, semantic VersionSemantic) bool {
	return compareVersion(a, b, semantic) <= 0
}

func VersionEq(a, b string, semantic VersionSemantic) bool {
	return compareVersion(a, b, semantic) == 0
}

// = returns 0
// > returns 1
// < returns -1
func compareVersion(a, b string, semantic VersionSemantic) int {
	var err error
	as := strings.Split(a, ".")
	bs := strings.Split(b, ".")
	var av, bv int
	for i := 0; i < int(semantic)+1; i++ {
		if i >= len(as) {
			av = 0
		} else {
			av, err = strconv.Atoi(as[i])
			if err != nil {
				av = 0
			}
		}
		if i >= len(bs) {
			bv = 0
		} else {
			bv, err = strconv.Atoi(bs[i])
			if err != nil {
				bv = 0
			}
		}
		if av > bv {
			return 1
		} else if av < bv {
			return -1
		}
	}
	return 0
}
