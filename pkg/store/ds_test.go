package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrimLastEmptyAof(t *testing.T) {

	t.Run("trim last aof", func(t *testing.T) {
		ds := testMakeDataSet(0, 10)
		lastAof := ds.aofSegs[len(ds.aofSegs)-1]
		lastAof.rtSize.Store(0)
		ds.trimLastEmptyAof()
		lastAof2 := ds.aofSegs[len(ds.aofSegs)-1]
		assert.True(t, lastAof.Left() != lastAof2.Left())
	})

	t.Run("empty ds", func(t *testing.T) {
		ds := testMakeDataSet(0, 0)
		ds.trimLastEmptyAof()
		assert.Len(t, ds.aofSegs, 0)
	})
}
