package store

import (
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ikenchina/redis-GunYu/pkg/log"
)

// dataSet
type dataSetRdb struct {
	mux     sync.RWMutex
	left    int64
	rdbSize int64
	rwRef   atomic.Int32
	writer  *RdbWriter
	readers []*RdbReader
}

func (r *dataSetRdb) Left() int64 {
	r.mux.RLock()
	defer r.mux.RUnlock()
	return r.left
}

func (r *dataSetRdb) Size() int64 {
	r.mux.RLock()
	defer r.mux.RUnlock()
	return r.rdbSize
}

func (r *dataSetRdb) AddReader(rd *RdbReader) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rwRef.Add(1)
	r.readers = append(r.readers, rd)
}

func (r *dataSetRdb) AddWriter(wr *RdbWriter) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rwRef.Add(1)
	r.writer = wr
}

func (r *dataSetRdb) DelReader(rd *RdbReader) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rwRef.Add(-1)
	for i, t := range r.readers {
		if t == rd {
			r.readers = append(r.readers[:i], r.readers[i+1:]...)
			return
		}
	}
	log.Errorf("dataSetRdb.DelReader : self(%d), rd(%d)", r.left, rd.offset)
}

func (r *dataSetRdb) DelWriter(wr *RdbWriter) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rwRef.Add(-1)
	if r.writer != wr {
		log.Errorf("dataSetRdb.DelWriter : self(%d), wr(%d)", r.left, wr.left)
		return
	}
	r.writer = nil
}

func (r *dataSetRdb) SetWriter(wr *RdbWriter) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rwRef.Add(1)
	r.writer = wr
}

func (r *dataSetRdb) Ref() int32 {
	return r.rwRef.Load()
}

func (r *dataSetRdb) Close() {
	if r.rwRef.Load() == 0 {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	if r.writer != nil {
		r.writer.Close()
	}
	for _, r := range r.readers {
		r.Close()
	}
}

type dataSetAof struct {
	mux    sync.RWMutex
	left   int64
	size   int64        // -1 means the file is writing, or the size without header of file
	rtSize atomic.Int64 // real time size of aof without header of file
	rwRef  atomic.Int32

	writer  *AofWriter
	readers []*AofRotateReader
}

func (a *dataSetAof) Size() int64 {
	a.mux.RLock()
	defer a.mux.RUnlock()
	return a.size
}

func (a *dataSetAof) AddReader(rd *AofRotateReader) {
	a.mux.Lock()
	defer a.mux.Unlock()
	a.readers = append(a.readers, rd)
	a.rwRef.Add(1)
}

func (a *dataSetAof) DelReader(rdr *AofRotateReader) {
	a.mux.Lock()
	defer a.mux.Unlock()
	for i, rd := range a.readers {
		if rd == rdr {
			a.readers = append(a.readers[:i], a.readers[i+1:]...)
			a.rwRef.Add(-1)
			return
		}
	}
	log.Errorf("dataSetAof.DelReader : self(%d), rd(%d)", a.left, rdr.left)
}

func (a *dataSetAof) SetSize(s int64) {
	a.mux.Lock()
	defer a.mux.Unlock()
	a.size = s
}

func (a *dataSetAof) DelWriter(wr *AofWriter) {
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.writer != wr {
		log.Errorf("dataSetAof.DelWriter : self(%d), wr(%d)", a.writer.left, wr.left)
	}
	a.writer = nil
	a.rwRef.Add(-1)
}

func (a *dataSetAof) SetWriter(wr *AofWriter) {
	a.mux.Lock()
	defer a.mux.Unlock()
	a.writer = wr
	a.rwRef.Add(1)
}

func (a *dataSetAof) Ref() int32 {
	return a.rwRef.Load()
}

func (a *dataSetAof) Close() {
	if a.rwRef.Load() == 0 { //fast path
		return
	}
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.writer != nil {
		a.writer.Close()
	}
	for _, r := range a.readers {
		r.Close()
	}
}

func (a *dataSetAof) CloseWriter() {
	if a.rwRef.Load() == 0 { //fast path
		return
	}
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.writer != nil {
		a.writer.Close()
	}
}

func (a *dataSetAof) Right() int64 {
	a.mux.Lock()
	defer a.mux.Unlock()
	return a.left + a.rtSize.Load()
}

func (a *dataSetAof) Left() int64 {
	a.mux.Lock()
	defer a.mux.Unlock()
	return a.left
}

func (a *dataSetAof) incrSize(delta int64) {
	a.rtSize.Add(delta)
}

func newDataSet(rdb *dataSetRdb, aofs []*dataSetAof) *dataSet {
	ds := &dataSet{
		rdb:     rdb,
		aofSegs: aofs,
		aofMap:  make(map[int64]*dataSetAof),
	}
	if len(aofs) == 0 {
		return ds
	}
	for _, a := range aofs {
		ds.aofMap[a.left] = a
	}

	sort.Slice(ds.aofSegs, func(i, j int) bool {
		return ds.aofSegs[i].left < ds.aofSegs[j].left
	})

	ds.lastAofSeg.Store(ds.aofSegs[len(ds.aofSegs)-1].Left())
	return ds
}

type dataSet struct {
	mux        sync.RWMutex
	rdb        *dataSetRdb
	aofSegs    []*dataSetAof // aof segments
	aofMap     map[int64]*dataSetAof
	lastAofSeg atomic.Int64
}

func (ds *dataSet) TruncateGap() (*dataSetRdb, []*dataSetAof) {
	ds.mux.RLock()
	defer ds.mux.RUnlock()

	var rdb *dataSetRdb
	var aofs []*dataSetAof

	for i := len(ds.aofSegs) - 1; i > 0; i-- {
		if ds.aofSegs[i].Left() != ds.aofSegs[i-1].Right() {
			uaofs := ds.aofSegs[:i]
			aofs = append(aofs, uaofs...)
			if ds.rdb != nil {
				rdb = ds.rdb
			}
			ds.aofSegs = ds.aofSegs[i:]
			ds.rdb = nil
			break
		}
	}

	ds.aofMap = make(map[int64]*dataSetAof)
	for _, a := range ds.aofSegs {
		ds.aofMap[a.left] = a
	}

	return rdb, aofs
}

func (ds *dataSet) LastAofSeg() int64 {
	return ds.lastAofSeg.Load()
}

func (ds *dataSet) RdbSize() int64 {
	ds.mux.RLock()
	defer ds.mux.RUnlock()
	if ds.rdb == nil {
		return -1
	}
	return ds.rdb.Size()
}

func (ds *dataSet) SetRdb(rdb *dataSetRdb) {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	ds.rdb = rdb
}

func (ds *dataSet) GetRdb() *dataSetRdb {
	ds.mux.RLock()
	defer ds.mux.RUnlock()
	return ds.rdb
}

func (ds *dataSet) AppendAof(a *dataSetAof) {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	ds.aofSegs = append(ds.aofSegs, a)
	ds.aofMap[a.Left()] = a
	ds.lastAofSeg.Store(a.left)
}

func (ds *dataSet) InRange(offset int64) bool {
	ds.mux.RLock()
	defer ds.mux.RUnlock()

	ll, rr := ds.getRange()
	// left <= offset <= right
	if offset != -1 && ll <= offset && rr >= offset {
		return true
	} else if ll >= offset && ds.rdb != nil { // offset <= rdb.left
		return true
	}
	return false
}

func (ds *dataSet) Close() {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	if ds.rdb != nil {
		ds.rdb.Close()
	}
	for _, a := range ds.aofSegs {
		a.Close()
	}
}

func (ds *dataSet) CloseAofWriter() {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	for _, a := range ds.aofSegs {
		a.Close()
	}
}

func (ds *dataSet) FindAof(left int64) *dataSetAof {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	return ds.aofMap[left]
}

func (ds *dataSet) IndexAof(offset int64) *dataSetAof {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	for i := len(ds.aofSegs) - 1; i >= 0; i-- {
		aof := ds.aofSegs[i]
		if aof.Left() <= offset && aof.Right() >= offset {
			return aof
		}
	}
	return nil
}

func (ds *dataSet) Range() (ll int64, rr int64) {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	return ds.getRange()
}

func (ds *dataSet) getRange() (ll int64, rr int64) {
	if ds.rdb == nil && len(ds.aofSegs) == 0 {
		return -1, -1
	}

	ll = math.MaxInt64
	rr = 0
	if ds.rdb != nil {
		ll = ds.rdb.left
		rr = ds.rdb.left
	}
	if len(ds.aofSegs) != 0 && ll > ds.aofSegs[0].left {
		ll = ds.aofSegs[0].left
	}

	if len(ds.aofSegs) != 0 && rr < ds.aofSegs[len(ds.aofSegs)-1].Right() {
		rr = ds.aofSegs[len(ds.aofSegs)-1].Right()
	}
	return
}

func (ds *dataSet) Left() int64 {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	l := int64(-1)
	if ds.rdb != nil {
		l = ds.rdb.Left()
	}
	if len(ds.aofSegs) != 0 && l > ds.aofSegs[0].Left() {
		l = ds.aofSegs[0].Left()
	}
	return l
}

func (ds *dataSet) Right() int64 {
	ds.mux.Lock()
	defer ds.mux.Unlock()
	if len(ds.aofSegs) != 0 {
		return ds.aofSegs[len(ds.aofSegs)-1].Right()
	}
	if ds.rdb != nil {
		return ds.rdb.Left()
	}
	return int64(-1)
}

func (ds *dataSet) gcLogs(dir string, maxSize int64) {
	ds.mux.Lock()
	defer ds.mux.Unlock()

	size := int64(0)

	rdb := ds.rdb

	// from newest to oldest

	// iterate AOF files first
	aofLast := len(ds.aofSegs) - 1
	for ; aofLast >= 0; aofLast-- {
		aofSize := ds.aofSegs[aofLast].rtSize.Load()
		if aofSize == 0 {
			log.Warnf("aof rtsize is 0 : aof(%d), size(%d)", ds.aofSegs[aofLast].Left(), ds.aofSegs[aofLast].Size())
		}
		size += aofSize
		if size > maxSize {
			break
		}
	}

	ref0 := true

	if rdb != nil {
		size += rdb.rdbSize
		rdb.mux.Lock()
		if size > maxSize {
			if rdb.rwRef.Load() == 0 {
				rdbfn := rdbFilePath(dir, rdb.left, rdb.rdbSize)
				if err := os.RemoveAll(rdbfn); err != nil {
					log.Errorf("GC Logs, remove rdb file error : file(%s), error(%v)", rdbfn, err)
				} else {
					log.Infof("GC Logs, remove rdb file : file(%s)", rdbfn)
					size -= rdb.rdbSize
				}
				ds.rdb = nil
			} else {
				ref0 = false
				log.Warnf("storage size exceeds limitation, but rdb reference is not zero : size(%d), maxSize(%d), rdb(%d)",
					size, maxSize, rdb.left)
			}
		}
		rdb.mux.Unlock()
	}

	if ref0 {
		// oldest to newest
		for z := 0; z <= aofLast; z++ {
			aof := ds.aofSegs[z]
			aof.mux.Lock()
			if aof.Ref() > 0 {
				ref0 = false
				aof.mux.Unlock()
				if size > maxSize {
					log.Warnf("GC Logs, size exceeded limitation, but reference is not zero : size(%d), maxSize(%d), left(%d)",
						size, maxSize, aof.Left())
				}
				break
			} else {
				aoffn := aofFilePath(dir, aof.left)
				if err := os.RemoveAll(aoffn); err != nil {
					log.Errorf("GC Logs, remove aof file error : file(%s), error(%v)", aoffn, err)
				} else {
					log.Infof("GC Logs, remove aof file : file(%s)", aoffn)
				}
				size -= aof.rtSize.Load()
				ds.aofSegs = ds.aofSegs[z+1:]
				aofLast--
				z--
				aof.mux.Unlock()
			}
		}
	}

}
