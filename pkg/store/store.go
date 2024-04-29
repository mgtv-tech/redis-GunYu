package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/io/pipe"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	usync "github.com/mgtv-tech/redis-GunYu/pkg/sync"
)

type Storer struct {
	Id          string
	mux         sync.RWMutex // private functions aren't thread safe
	baseDir     string       // storage directory
	dir         string       // baseDir + runId
	maxSize     int64
	logSize     int64
	runId       string
	dataSetMux  sync.RWMutex
	dataSet     *dataSet
	readBufSize int
	closer      usync.WaitCloser
	logger      log.Logger
	flush       config.FlushPolicy
}

func NewStorer(id string, baseDir string, maxSize, logSize int64, flush config.FlushPolicy) *Storer {
	ss := &Storer{
		Id:          id,
		baseDir:     baseDir,
		maxSize:     maxSize,
		logSize:     logSize,
		readBufSize: 10 * 1024 * 1024,
		closer:      usync.NewWaitCloser(nil),
		logger:      log.WithLogger(config.LogModuleName(fmt.Sprintf("[Storer(%s)] ", id))),
		dataSet:     newDataSet(nil, nil),
		flush:       flush,
	}

	usync.SafeGo(func() {
		ss.gcLogJob()
	}, nil)

	return ss
}

func (s *Storer) getDataSet() *dataSet {
	s.dataSetMux.RLock()
	defer s.dataSetMux.RUnlock()
	return s.dataSet
}

func (s *Storer) VerifyRunId(ids []string) (offset int64, err error) {
	for _, id := range ids {
		if id == "" || id == "?" {
			continue
		}
		dir := filepath.Join(s.baseDir, id)
		_, err = os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				return
			}
			err = nil
			continue
		}
		err = s.SetRunId(id)
		if err != nil {
			return
		}
		newest := s.LatestOffset()
		if newest == 0 {
			continue
		}
		return newest, nil
	}
	return
}

func (s *Storer) newRunId(id string) error {
	if id == "" || id == "?" {
		return nil
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	dir := filepath.Join(s.baseDir, id)
	_, err := os.Stat(dir)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0777)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}

	s.runId = id
	s.dir = dir

	rdbAof := s.initDataSet()
	if rdbAof != nil {
		s.dataSetMux.Lock()
		s.dataSet = rdbAof
		s.dataSetMux.Unlock()
	}

	return nil
}

func (s *Storer) SetRunId(new string) error {
	old := s.runId
	if old == "" || !ExistReplId(s.baseDir, old) {
		return s.newRunId(new)
	}
	if new == "" || ExistReplId(s.baseDir, new) {
		return s.newRunId(new)
	}
	err := changeReplId(s.baseDir, old, new)
	if err != nil {
		return err
	}
	return s.newRunId(new)
}

func (s *Storer) DelRunId(id string) error {
	if id == "" {
		return nil
	}
	if id == "?" {
		// @TODO clear all  locals ?
		return nil
	}

	s.mux.Lock()
	defer s.mux.Unlock()

	dir := filepath.Join(s.baseDir, id)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	err = os.RemoveAll(dir)
	if err != nil {
		return err
	}

	s.dir = ""
	s.runId = ""
	s.resetDataSet()

	return nil
}

func (s *Storer) resetDataSet() {
	s.logger.Debugf("Storer reset dataset : %s", s.dir)

	s.dataSetMux.Lock()
	defer s.dataSetMux.Unlock()
	ra := s.dataSet
	if ra != nil {
		ra.Close()
	}

	filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if path == s.dir {
			return nil
		}
		s.logger.Infof("remove path : %s", path)
		err = os.RemoveAll(path)
		if err != nil {
			s.logger.Errorf("remove : path(%s), error(%v)", path, err)
		}
		return nil
	})

	s.dataSet = newDataSet(nil, nil)
}

func (s *Storer) Close() error {
	s.closer.Close(nil)
	return s.closer.Error()
}

func (s *Storer) findAof(left int64) *dataSetAof {
	ra := s.getDataSet()
	return ra.FindAof(left)
}

func (s *Storer) hasWriter(left int64) bool {
	aof := s.findAof(left)
	if aof != nil {
		return aof.Size() == -1
	}
	return false
}

func (s *Storer) RunId() string {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.runId
}

func (s *Storer) LatestOffset() int64 {
	return s.getDataSet().Right()
}

func (s *Storer) IsValidOffset(offset int64) bool {
	ds := s.getDataSet()
	return ds.InRange(offset)
}

func (s *Storer) GetOffsetRange() (int64, int64) {
	ds := s.getDataSet()
	return ds.Range()
}

func (s *Storer) GetRdb() (int64, int64) {
	ds := s.getDataSet()
	rdb := ds.GetRdb()
	if rdb == nil {
		return -1, -1
	}
	return rdb.Left(), rdb.Size()
}

// for a rdb writer, Reader returns io.EOF when data has been drained
// for a aof writer, it's endless unless encounter an error
func (s *Storer) GetReader(offset int64, verifyCrc bool) (*Reader, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	s.dataSetMux.Lock()
	defer s.dataSetMux.Unlock()

	ds := s.dataSet
	if !ds.InRange(offset) {
		return nil, os.ErrNotExist
	}

	rd := &Reader{
		runId: s.runId,
	}
	piper, pipew := pipe.NewSize(s.readBufSize)
	reader := bufio.NewReaderSize(piper, s.readBufSize)

	aof := ds.IndexAof(offset) // try get AOF reader first
	if aof == nil {
		rdb := ds.GetRdb()
		if rdb != nil {
			left := rdb.Left()
			if offset <= left {
				rr, err := NewRdbReader(pipew, s.dir, left, rdb.Size(), verifyCrc)
				if err != nil {
					return nil, err
				}
				rd.rdb = rr
				rd.reader = reader
				rd.size = rdb.Size()
				rd.left = left
				rd.logger = log.WithLogger(config.LogModuleName("[Reader(rdb)] "))
				rdb.AddReader(rr)
				rr.SetObserver(&observerProxy{
					close: s.newRdbRCloseObserver(rr, rdb),
				})
				return rd, nil
			}
		}
		return nil, os.ErrNotExist
	}

	rr, err := NewAofRotateReader(s.dir, aof.Left(), s, pipew, verifyCrc)
	if err != nil {
		return nil, err
	}

	rd.left = offset
	rd.aof = rr
	rd.reader = reader
	rd.size = -1
	rd.logger = log.WithLogger(config.LogModuleName("[Reader(aof)] "))

	err = rr.Seek(offset)
	if err != nil {
		rr.Close()
		return nil, err
	}

	aof.AddReader(rr)
	rr.SetObserver(&observerProxy{
		open:  s.newAofROpenObserver(rr, ds),
		close: s.newAofRCloseObserver(rr, ds),
	})

	return rd, nil
}

func (s *Storer) newAofROpenObserver(reader *AofRotateReader, ra *dataSet) func(args ...interface{}) {
	return func(args ...interface{}) {
		offset := args[0].(int64)
		aof := ra.FindAof(offset)
		if aof != nil {
			aof.AddReader(reader)
		}
	}
}

func (s *Storer) newAofRCloseObserver(reader *AofRotateReader, ra *dataSet) func(args ...interface{}) {
	return func(args ...interface{}) {
		offset := args[0].(int64)
		aof := ra.FindAof(offset)
		if aof != nil {
			aof.DelReader(reader)
		}
	}
}

func (s *Storer) newRdbRCloseObserver(r *RdbReader, rdb *dataSetRdb) func(args ...interface{}) {
	return func(args ...interface{}) {
		rdb.DelReader(r)
	}
}

func (s *Storer) GetRdbWriter(r io.Reader, offset int64, rdbSize int64) (*RdbWriter, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.resetDataSet()

	w, err := NewRdbWriter(s.Id, r, s.dir, offset, rdbSize)
	if err != nil {
		return nil, err
	}

	s.dataSetMux.Lock()
	rdb := &dataSetRdb{
		left:    offset,
		rdbSize: rdbSize,
	}
	s.dataSet = newDataSet(rdb, nil)
	rdb.AddWriter(w)
	s.dataSetMux.Unlock()

	obr := &observerProxy{
		close: s.newRdbWCloseObserver(w, rdb),
	}
	w.SetObserver(obr)

	return w, nil
}

func (s *Storer) newRdbWCloseObserver(w *RdbWriter, rdb *dataSetRdb) func(args ...interface{}) {
	return func(args ...interface{}) {
		rdb.DelWriter(w)
	}
}

func (s *Storer) GetAofWritter(r io.Reader, offset int64) (*AofWriter, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	// allows only one writer
	ds := s.getDataSet()
	ds.CloseAofWriter()

	w, err := NewAofWriter(s.Id, s.dir, offset, r, s.logSize, s.flush)
	if err != nil {
		return nil, err
	}

	aofSeg := &dataSetAof{
		left: offset,
	}
	s.dataSetMux.Lock()
	s.dataSet.AppendAof(aofSeg)
	s.dataSetMux.Unlock()

	aofSeg.SetWriter(w)

	proxy := &observerProxy{
		open:  s.newAofWOpenObserver(w, ds),
		close: s.newAofWCloseObserver(w, ds),
		write: s.newAofWriteObserver(),
	}
	w.SetObserver(proxy)

	return w, nil
}

func (s *Storer) lastSeg() int64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.getDataSet().LastAofSeg()
}

func (s *Storer) newAofWriteObserver() func(args ...interface{}) {
	return func(args ...interface{}) {
		left := args[0].(int64)
		size := args[1].(int64)
		ds := s.getDataSet()
		aof := ds.FindAof(left)
		if aof != nil {
			aof.incrSize(size)
		} else {
			s.logger.Warnf("aof doesnot exist : aof(%d)", left)
		}
	}
}

func (s *Storer) newAofWOpenObserver(w *AofWriter, ds *dataSet) func(args ...interface{}) {
	return func(args ...interface{}) {
		left := args[0].(int64)

		// always create new aof, avoid calculating CRC
		aof := &dataSetAof{
			left: left,
			size: -1,
		}
		aof.SetWriter(w)
		ds.AppendAof(aof)
	}
}

func (s *Storer) newAofWCloseObserver(w *AofWriter, ds *dataSet) func(args ...interface{}) {
	return func(args ...interface{}) {
		offset := args[0].(int64)
		size := args[1].(int64)

		aof := ds.FindAof(offset)
		if aof == nil {
			s.logger.Errorf("aofClose file does not exist : %d", offset)
			return
		}
		aof.SetSize(size)
		aof.DelWriter(w)
	}
}

func (s *Storer) initDataSet() *dataSet {
	dir := s.dir
	// template variable needn't mutex
	aofSegs := []*dataSetAof{}
	var rdb *dataSetRdb
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			if strings.HasSuffix(info.Name(), ".aof") {
				fn := strings.TrimSuffix(info.Name(), ".aof")
				ofs, err := strconv.ParseInt(fn, 10, 64)
				if err != nil {
					s.logger.Errorf("wrong aof file : name(%s)", info.Name())
					return nil
				}
				a := &dataSetAof{
					left: ofs,
					size: info.Size() - headerSize,
				}
				if a.size > 0 { // size must greater than zero
					a.rtSize.Store(a.size)
					aofSegs = append(aofSegs, a)
				}
			} else {
				rf := ParseRdbFile(info.Name(), false)
				if rf.IsValid() {
					rdb = &dataSetRdb{
						left:    rf.offset,
						rdbSize: rf.size,
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		s.logger.Errorf("%v", err)
		return nil
	}

	ds := newDataSet(rdb, aofSegs)
	dRdb, dAofs := ds.TruncateGap()
	if dRdb != nil {
		opath := rdbFilePath(s.dir, dRdb.Left(), dRdb.Size())
		err := os.Remove(opath)
		if err != nil {
			s.logger.Errorf("remove rdb file : rdb(%s), error(%v)", opath, err)
		} else {
			s.logger.Infof("remove rdb file : rdb(%s)", opath)
		}
	}
	for _, a := range dAofs {
		opath := aofFilePath(s.dir, a.Left())
		err := os.Remove(opath)
		if err != nil {
			s.logger.Errorf("remove aof file : aof(%s), error(%v)", opath, err)
		} else {
			s.logger.Infof("remove aof file : aof(%s)", opath)
		}
	}

	return ds
}

func (s *Storer) gcLogJob() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closer.Context().Done():
			return
		case <-ticker.C:
		}
		s.gcLog()
	}
}

func (s *Storer) gcLog() {
	if s.maxSize <= 0 {
		return
	}

	// remove redundant rdbAof
	//s.gcRedundantRdbs() // doesn't support multi rdb now

	// remove unused rdb and aof
	s.gcDataSet()

}

// func (s *Storer) gcRedundantRdbs() {
// 	remove := false
// 	for i := len(s.rdbs) - 1; i >= 0; i-- {
// 		ra := s.rdbs[i]
// 		if remove && ra.ref() == 0 {
// 			os.RemoveAll(ra.dir)
// 			s.rdbs = append(s.rdbs[:i], s.rdbs[i+1:]...)
// 			continue
// 		}
// 		if ra.rdb != nil {
// 			remove = true
// 		}
// 	}
// }

func (s *Storer) gcDataSet() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.getDataSet().gcLogs(s.dir, s.maxSize)
}
