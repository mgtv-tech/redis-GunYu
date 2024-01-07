package store

import (
	"math"
	"os"
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

type dataSet struct {
	mux        sync.RWMutex
	rdb        *dataSetRdb
	aofSegs    []*dataSetAof // aof segments
	lastAofSeg atomic.Int64
}

func (r *dataSet) LastAofSeg() int64 {
	return r.lastAofSeg.Load()
}

func (r *dataSet) RdbSize() int64 {
	r.mux.RLock()
	defer r.mux.RUnlock()
	if r.rdb == nil {
		return -1
	}
	return r.rdb.Size()
}

func (r *dataSet) SetRdb(rdb *dataSetRdb) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.rdb = rdb
}

func (r *dataSet) GetRdb() *dataSetRdb {
	r.mux.RLock()
	defer r.mux.RUnlock()
	return r.rdb
}

func (r *dataSet) AppendAof(a *dataSetAof) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.aofSegs = append(r.aofSegs, a)
	r.lastAofSeg.Store(a.left)
}

func (r *dataSet) InRange(offset int64) bool {
	r.mux.RLock()
	defer r.mux.RUnlock()

	ll, rr := r.getRange()
	// left <= offset <= right
	if offset != -1 && ll <= offset && rr >= offset {
		return true
	} else if ll >= offset && r.rdb != nil { // offset <= rdb.left
		return true
	}
	return false
}

func (r *dataSet) Close() {
	r.mux.Lock()
	defer r.mux.Unlock()
	if r.rdb != nil {
		r.rdb.Close()
	}
	for _, a := range r.aofSegs {
		a.Close()
	}
}

func (r *dataSet) CloseAofWriter() {
	r.mux.Lock()
	defer r.mux.Unlock()
	for _, a := range r.aofSegs {
		a.Close()
	}
}

func (r *dataSet) IndexAof(offset int64) *dataSetAof {
	r.mux.Lock()
	defer r.mux.Unlock()
	for i := len(r.aofSegs) - 1; i >= 0; i-- {
		aof := r.aofSegs[i]
		if aof.Left() <= offset && aof.Right() >= offset {
			return aof
		}
	}
	return nil
}

func (r *dataSet) Range() (ll int64, rr int64) {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.getRange()
}

func (r *dataSet) getRange() (ll int64, rr int64) {
	if r.rdb == nil && len(r.aofSegs) == 0 {
		return -1, -1
	}

	ll = math.MaxInt64
	rr = 0
	if r.rdb != nil {
		ll = r.rdb.left
		rr = r.rdb.left
	}
	if len(r.aofSegs) != 0 && ll > r.aofSegs[0].left {
		ll = r.aofSegs[0].left
	}

	if len(r.aofSegs) != 0 && rr < r.aofSegs[len(r.aofSegs)-1].Right() {
		rr = r.aofSegs[len(r.aofSegs)-1].Right()
	}
	return
}

func (r *dataSet) Left() int64 {
	r.mux.Lock()
	defer r.mux.Unlock()
	l := int64(-1)
	if r.rdb != nil {
		l = r.rdb.Left()
	}
	if len(r.aofSegs) != 0 && l > r.aofSegs[0].Left() {
		l = r.aofSegs[0].Left()
	}
	return l
}

func (r *dataSet) Right() int64 {
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.aofSegs) != 0 {
		return r.aofSegs[len(r.aofSegs)-1].Right()
	}
	if r.rdb != nil {
		return r.rdb.Left()
	}
	return int64(-1)
}

func (ra *dataSet) gcLogs(dir string, maxSize int64) {
	ra.mux.Lock()
	defer ra.mux.Unlock()

	size := int64(0)

	rdb := ra.rdb

	// from newest to oldest

	// iterate AOF files first
	aofLast := len(ra.aofSegs) - 1
	for ; aofLast >= 0; aofLast-- {
		size += ra.aofSegs[aofLast].rtSize.Load()
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
				ra.rdb = nil
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
			aof := ra.aofSegs[z]
			aof.mux.Lock()
			if aof.Ref() > 0 {
				ref0 = false
				aof.mux.Unlock()
				break
			} else {
				aoffn := aofFilePath(dir, aof.left)
				if err := os.RemoveAll(aoffn); err != nil {
					log.Errorf("GC Logs, remove aof file error : file(%s), error(%v)", aoffn, err)
				} else {
					log.Infof("GC Logs, remove aof file : file(%s)", aoffn)
				}

				ra.aofSegs = ra.aofSegs[z+1:]
				aofLast--
				z--
				aof.mux.Unlock()
			}
		}
	}

}
