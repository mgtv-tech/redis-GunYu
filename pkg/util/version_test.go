package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionCompare(t *testing.T) {
	cases := [][]struct {
		a     string
		b     string
		expGE bool
		expLE bool
		expEq bool
	}{
		{ // major
			{"4", "4.0", true, true, true}, {"4.0", "4", true, true, true}, {"4.0", "4.1", true, true, true},
			{"4.1", "4.0", true, true, true}, {"4", "4", true, true, true},
			{"4.0", "5.0", false, true, false}, {"4.1", "5.0", false, true, false},
			{"5.0", "4.0", true, false, false}, {"5.0", "4.1", true, false, false},
			{"a.0", "4.1", false, true, false}, {"4.a", "4.1", true, true, true},
		},
		{ // minor
			{"4", "4.0", true, true, true}, {"4.0", "4", true, true, true}, {"4.0", "4.0", true, true, true},
			{"4.0.0", "4.0.1", true, true, true},
			{"4.1", "4.0", true, false, false}, {"4.1.0", "4.0.1", true, false, false},
			{"4.0", "4.1", false, true, false}, {"4.0.1", "4.1.0", false, true, false},
			{"4.a.1", "4.0.1", true, true, true}, {"4.0.2", "4.b.1", true, true, true},
			{"4.a.1", "4.1.1", false, true, false},
		},
		{ // patch
			{"4", "4.0.0", true, true, true}, {"4.0.0", "4", true, true, true}, {"4.0.0", "4.0", true, true, true},
			{"4.0.0", "4.0.0", true, true, true},
			{"4.0.1", "4.0", true, false, false},
			{"4.0.0", "4.0.1", false, true, false}, {"4.0.a", "4.0.1", false, true, false}, {"4.0.1", "4.0.b", true, false, false},
		},
	}

	for i, sematic := range cases {
		for _, c := range sematic {
			assert.Equal(t, c.expGE, VersionGE(c.a, c.b, VersionSemantic(i)))
			if c.expLE != VersionLE(c.a, c.b, VersionSemantic(i)) {
				fmt.Println(VersionLE(c.a, c.b, VersionSemantic(i)))
			}
			assert.Equal(t, c.expLE, VersionLE(c.a, c.b, VersionSemantic(i)))
			assert.Equal(t, c.expEq, VersionEq(c.a, c.b, VersionSemantic(i)))
		}
	}

}
