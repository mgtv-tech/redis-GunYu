package store

import (
	"testing"

	"github.com/ikenchina/redis-GunYu/pkg/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRdbAofSuite(t *testing.T) {
	suite.Run(t, new(rdbAofTestSuite))
}

type rdbAofTestSuite struct {
	suite.Suite
}

func (ts *rdbAofTestSuite) createdGap(gaps []int) *RdbAof {
	rdb := &RdbAof{
		dir: "dir",
		rdb: &Rdb{},
	}
	left := int64(0)
	for _, g := range gaps {
		aof := &Aof{Left: left}
		aof.rtSize.Store(100 - int64(g))
		rdb.aofs = append(rdb.aofs, aof)
		left += 100
	}
	return rdb
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
		st := &Storer{}
		st.truncateGap(rdb)
		ts.Equal(ca.exp[0], rdb.aofs[0].Left)
		ts.Equal(ca.exp[1], rdb.Right())
	}
}

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

func TestGcLog(t *testing.T) {

	makeRdb := func(left int64) *RdbAof {
		rdbaof := &RdbAof{
			rdb: &Rdb{RdbSize: 2, Left: left},
		}
		for i := int64(0); i < 8; i++ {
			aof := &Aof{
				Left: left + 2 + i,
			}
			aof.rtSize.Store(1)
			rdbaof.aofs = append(rdbaof.aofs, aof)
		}
		return rdbaof
	}
	makeRdbs := func(c int) (ra []*RdbAof) {
		for i := 0; i < c; i++ {
			ra = append(ra, makeRdb(int64(i*10)))
		}
		return
	}
	makeStorer := func(max int64, cur int) *Storer {
		storer := &Storer{
			logger: log.WithLogger(""),
		}
		storer.maxSize = max
		storer.rdbs = makeRdbs(cur)
		return storer
	}

	t.Run("redundant rdbaof", func(t *testing.T) {
		storer := makeStorer(20, 30)
		last := storer.rdbs[len(storer.rdbs)-1]
		storer.gcRedundantRdbs()
		assert.Equal(t, 1, len(storer.rdbs))
		assert.Equal(t, last, storer.rdbs[0])
	})

	t.Run("normal case", func(t *testing.T) {
		storer := makeStorer(15, 3)
		// [0, (2-9)], [10,(12-19)], [20,(22-29)]
		storer.gcRdbs()
		assert.Equal(t, 2, len(storer.rdbs))
		// [,(15-19)], [20,(22-29)]
		assert.True(t, storer.rdbs[1].rdb.Left == 20)
		assert.Nil(t, storer.rdbs[0].rdb)
		assert.True(t, storer.rdbs[0].aofs[0].Left == 15)
	})

	t.Run("ref case", func(t *testing.T) {
		storer := makeStorer(15, 3)
		// [0, (2-9)], [10,(12-19)], [20,(22-29)]
		storer.rdbs[1].aofs[5].ref = 1
		storer.gcRdbs()
		assert.Equal(t, 2, len(storer.rdbs))
		// [,(15-19)], [20,(22-29)]
		assert.True(t, storer.rdbs[1].rdb.Left == 20)
		assert.Nil(t, storer.rdbs[0].rdb)
		assert.True(t, storer.rdbs[0].aofs[0].Left == 15)
	})

}
