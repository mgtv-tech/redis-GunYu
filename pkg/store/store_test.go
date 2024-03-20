package store

import (
	"testing"

	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRdbAofSuite(t *testing.T) {
	suite.Run(t, new(rdbAofTestSuite))
}

type rdbAofTestSuite struct {
	suite.Suite
}

func (ts *rdbAofTestSuite) createdGap(gaps []int) *dataSet {
	ds := &dataSet{
		rdb: &dataSetRdb{},
	}
	left := int64(0)
	for _, g := range gaps {
		aof := &dataSetAof{left: left}
		aof.rtSize.Store(100 - int64(g))
		ds.AppendAof(aof)
		left += 100
	}
	return ds
}

func (ts *rdbAofTestSuite) TestTruncateGap() {

	cases := []struct {
		gaps []int
		exp  []int64
	}{
		{
			gaps: []int{0, 2, 0, 0, 2, 0},
			exp:  []int64{500, 600},
		},
		{
			gaps: []int{0, 0, 0, 0, 2, 0},
			exp:  []int64{500, 600},
		},
		{
			gaps: []int{0, 0, 0, 0, 0, 0},
			exp:  []int64{0, 600},
		},
		{
			gaps: []int{2, 0, 0, 0, 0, 0},
			exp:  []int64{100, 600},
		},
		{
			gaps: []int{2},
			exp:  []int64{0, 98},
		},
	}

	for _, ca := range cases {
		rdb := ts.createdGap(ca.gaps)
		rdb.TruncateGap()
		ts.Equal(ca.exp[0], rdb.aofSegs[0].left)
		ts.Equal(ca.exp[1], rdb.Right())
	}
}

/*
func (ts *rdbAofTestSuite) TestSplitRdbAof() {

	cases := []struct {
		gaps []int
		exp  []int
	}{
		{
			gaps: []int{0, 0, 0, 0, 2, 0},
			exp:  []int{498, 600},
		},
		{
			gaps: []int{0, 0, 2, 0, 2, 0},
			exp:  []int{298, 498, 600},
		},
		{
			gaps: []int{2, 0, 0, 0, 2, 2},
			exp:  []int{98, 498, 598},
		},
		{
			gaps: []int{2, 2, 2, 2, 2, 2},
			exp:  []int{98, 198, 298, 398, 498, 598},
		},
		{
			gaps: []int{0, 0, 0, 0, 0, 0},
			exp:  []int{600},
		},
		{
			gaps: []int{0},
			exp:  []int{100},
		},
		{
			gaps: []int{2},
			exp:  []int{98},
		},
	}

	for _, ca := range cases {
		rdb := ts.createdGap(ca.gaps)
		res := splitRdbAof(rdb)
		ts.Equal(len(ca.exp), len(res))
		//fmt.Println("-----")
		for i, re := range res {
			ts.Equal(i == 0, re.rdb != nil)
			//fmt.Println(re.rdb, re.dir, re.Left(), re.Right())
			ts.Equal(int64(ca.exp[i]), res[i].Right())
		}
	}

}
*/

func TestGcLog(t *testing.T) {

	makeRdb := func(left int64, size int64) *dataSet {
		ds := &dataSet{
			rdb: &dataSetRdb{rdbSize: 2, left: left},
		}
		for i := int64(0); i < size-2; i++ {
			aof := &dataSetAof{
				left: left + 2 + i,
			}
			aof.rtSize.Store(1)
			ds.AppendAof(aof)
		}
		return ds
	}

	makeStorer := func(max int64, size int64) *Storer {
		storer := &Storer{
			logger: log.WithLogger(""),
		}
		storer.maxSize = max
		storer.dataSet = makeRdb(0, size)
		return storer
	}

	t.Run("normal case", func(t *testing.T) {
		storer := makeStorer(5, 10)
		// [0, 2] ([2,3]...[9,10])
		storer.gcDataSet()
		assert.Nil(t, storer.dataSet.rdb)
		assert.Equal(t, 5, len(storer.dataSet.aofSegs))
		assert.Equal(t, int64(5), storer.dataSet.aofSegs[0].left)
	})

	t.Run("ref case", func(t *testing.T) {
		storer := makeStorer(5, 10)
		// [0, 2] ([2,3]...[9,10])
		storer.dataSet.aofSegs[1].rwRef.Add(1)
		storer.gcDataSet()
		assert.Nil(t, storer.dataSet.rdb)
		assert.Equal(t, 7, len(storer.dataSet.aofSegs))
		assert.Equal(t, int64(3), storer.dataSet.aofSegs[0].left)
	})

}
