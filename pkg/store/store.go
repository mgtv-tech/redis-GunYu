package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikenchina/redis-GunYu/pkg/io/pipe"
	"github.com/ikenchina/redis-GunYu/pkg/log"
	usync "github.com/ikenchina/redis-GunYu/pkg/sync"
)

type Storer struct {
	baseDir       string // storage directory
	dir           string // baseDir + runId
	maxSize       int64
	logSize       int64
	runId         string
	rdbs          []*RdbAof
	rdbLocker     sync.RWMutex // private functions aren't thread safe
	readBufSize   int
	closer        usync.WaitCloser
	latestAofLeft atomic.Int64
	logger        log.Logger
}

func NewStorer(baseDir string, maxSize, logSize int64) *Storer {
	ss := &Storer{
		baseDir:     baseDir,
		maxSize:     maxSize,
		logSize:     logSize,
		readBufSize: 10 * 1024 * 1024,
		closer:      usync.NewWaitCloser(nil),
		logger:      log.WithLogger("[Storer] "),
	}

	usync.SafeGo(func() {
		ss.gcLogJob()
	}, nil)

	return ss
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

	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

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

	s.initFiles()

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
	s.rdbLocker.Lock()
	s.rdbs = []*RdbAof{}
	s.rdbLocker.Unlock()

	return nil
}

func (s *Storer) Close() error {
	s.closer.Close(nil)
	return s.closer.Error()
}

func (s *Storer) newAofCallBack(dir string, offset int64) updateAofSizeFunc {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	var rdbaof *RdbAof
	for _, rdb := range s.rdbs {
		if rdb.dir == dir {
			rdbaof = rdb
			break
		}
	}

	// always create new aof, avoid calculating CRC
	aof := &Aof{
		Left: offset,
		Size: -1,
		ref:  1,
	}

	if rdbaof == nil {
		s.rdbs = append(s.rdbs, &RdbAof{
			aofs: []*Aof{aof},
			dir:  dir,
		})
	} else {
		rdbaof.aofs = append(rdbaof.aofs, aof)
	}

	s.latestAofLeft.Store(offset)

	return aof.incrSize
}

func (s *Storer) endAof(dir string, offset int64, size int64) {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	for _, rdb := range s.rdbs {
		if rdb.dir == dir {
			for _, aof := range rdb.aofs {
				if offset == aof.Left {
					aof.Size = size
					aof.ref--
					return
				}
			}
		}
	}
	s.logger.Errorf("endAof file does not exist : %s, %d", dir, offset)
}

func (s *Storer) isWritingAof(dir string, offset int64) bool {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	for i := len(s.rdbs) - 1; i >= 0; i-- {
		rdb := s.rdbs[i]
		if rdb.dir == dir {
			for j := len(rdb.aofs) - 1; j >= 0; j-- {
				if offset == rdb.aofs[j].Left {
					return rdb.aofs[j].Size == -1
				}
			}
		}
	}

	return false
}

func (s *Storer) delAof(dir string, offset int64, size int64) {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	for _, rdb := range s.rdbs {
		if rdb.dir == dir {
			for i, aof := range rdb.aofs {
				if offset == aof.Left {
					rdb.aofs = append(rdb.aofs[:i], rdb.aofs[i+1:]...)
					return
				}
			}
		}
	}
	s.logger.Errorf("endAof file does not exist : %s, %d", dir, offset)
}

func (s *Storer) RunId() string {
	s.rdbLocker.RLock()
	defer s.rdbLocker.RUnlock()
	return s.runId
}

func (s *Storer) LatestOffset() int64 {
	s.rdbLocker.RLock()
	defer s.rdbLocker.RUnlock()

	if len(s.rdbs) == 0 {
		return 0
	}
	rdb := s.rdbs[len(s.rdbs)-1]
	if len(rdb.aofs) == 0 {
		if rdb.rdb == nil {
			return 0
		}
		return rdb.rdb.Left
	}
	return rdb.aofs[len(rdb.aofs)-1].Right()
}

func (s *Storer) IsValidOffset(offset int64) bool {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	rdb := s.getRdb(offset)
	if rdb != nil {
		return true
	}
	return false
}

func (s *Storer) GetOffsetRange(offset int64) (int64, int64) {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	rdb := s.getRdb(offset)
	if rdb == nil {
		return -1, -1
	}
	return rdb.Left(), rdb.Right()
}

func (s *Storer) GetRdbSize(offset int64) int64 {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	rdb := s.getRdb(offset)
	if rdb.rdb != nil {
		return rdb.rdb.RdbSize
	}
	return -1
}

// for a rdb writer, Reader returns io.EOF when data has been drained
// for a aof writer, it's endless unless encounter an error
func (s *Storer) GetReader(offset int64, verifyCrc bool) (*Reader, error) {
	s.rdbLocker.RLock()
	defer s.rdbLocker.RUnlock()

	rdb := s.getRdb(offset)
	if rdb == nil {
		return nil, os.ErrNotExist
	}

	rd := &Reader{
		runId: s.runId,
	}
	piper, pipew := pipe.NewSize(s.readBufSize)
	reader := bufio.NewReaderSize(piper, s.readBufSize)

	aof := rdb.indexAof(offset) // try get AOF reader first
	if aof == nil {
		if rdb.rdb != nil && offset <= rdb.rdb.Left {
			rr, err := s.getRdbReader(pipew, rdb.rdb.Left, rdb.rdb.RdbSize, verifyCrc)
			if err != nil {
				return nil, err
			}
			rdb.rdb.ref++
			rd.rdb = rr
			rd.reader = reader
			rd.size = rdb.rdb.RdbSize
			rd.left = rdb.rdb.Left
			rd.logger = log.WithLogger("[Reader(rdb)] ")
			return rd, nil
		}
		return nil, os.ErrNotExist
	}

	rr, err := s.getAofReader(pipew, rdb.dir, aof.Left, verifyCrc)
	if err != nil {
		return nil, err
	}
	aof.ref++

	err = rr.Seek(offset)
	if err != nil {
		rr.Close()
		return nil, err
	}

	rd.left = offset
	rd.aof = rr
	rd.reader = reader
	rd.size = -1
	rd.logger = log.WithLogger("[Reader(aof)] ")
	return rd, nil
}

func (s *Storer) ensureDir(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0777)
	}
	return err
}

func (s *Storer) releaseRdbAof(dir string, offset int64, size int64, isRdb bool) {
	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()
	s.addRdbAofRef(dir, offset, size, isRdb, -1)
}

func (s *Storer) addRdbAofRef(dir string, offset int64, size int64, isRdb bool, counter int) {
	for _, rdb := range s.rdbs {
		if rdb.dir == dir {
			if isRdb {
				if rdb.rdb != nil {
					rdb.rdb.ref += counter
				}
			} else {
				for _, aof := range rdb.aofs {
					if offset == aof.Left {
						aof.ref += counter
						return
					}
				}
			}
		}
	}
}

func (s *Storer) GetRdbWriter(r io.Reader, offset int64, rdbSize int64) (*RdbWriter, error) {
	rdbDir := filepath.Join(s.dir, strconv.FormatInt(offset, 10))
	err := s.ensureDir(rdbDir)
	if err != nil {
		return nil, err
	}
	w, err := NewRdbWriter(s, r, rdbDir, offset, rdbSize)
	if err != nil {
		return nil, err
	}

	s.rdbLocker.Lock()
	defer s.rdbLocker.Unlock()

	s.rdbs = append(s.rdbs, &RdbAof{
		rdb: &Rdb{
			Left:    offset,
			RdbSize: rdbSize,
			ref:     1,
		},
		dir: rdbDir,
	})

	return w, nil
}

func (s *Storer) getRdbReader(w pipe.Writer, offset int64, rdbSize int64, verifyCrc bool) (*RdbReader, error) {
	rdbDir := filepath.Join(s.dir, strconv.FormatInt(offset, 10))
	return NewRdbReader(s, w, rdbDir, offset, rdbSize, verifyCrc)
}

// create a new aof file
func (s *Storer) GetAofWritter(r io.Reader, offset int64) (*AofWriter, error) {
	s.rdbLocker.Lock()

	dir := fmt.Sprintf("%s%c%d", s.dir, os.PathSeparator, offset)
	newDir := true
	// no gap and no overlap, reuse the directory
	for i := len(s.rdbs) - 1; i >= 0; i-- {
		if s.rdbs[i].Right() == offset {
			dir = s.rdbs[i].dir
			newDir = false
		}
	}

	if newDir {
		err := s.ensureDir(dir)
		if err != nil {
			s.rdbLocker.Unlock()
			return nil, err
		}
		s.rdbs = append(s.rdbs, &RdbAof{
			dir: dir,
		})
	}
	s.rdbLocker.Unlock()

	// NewAofWriter will hold the locker
	w, err := NewAofWriter(dir, offset, r, s, s.logSize)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Storer) getAofReader(w io.Writer, dir string, offset int64, verifyCrc bool) (*AofReader, error) {
	return NewAofReader(dir, offset, w, s, verifyCrc)
}

type Rdb struct {
	//Right   int64
	Left    int64
	RdbSize int64
	ref     int
}

type Aof struct {
	Left   int64
	Size   int64        // -1 means the file is writing, or the size without header of file
	rtSize atomic.Int64 // real time size of aof without header of file
	ref    int
}

func (a *Aof) Right() int64 {
	return a.Left + a.rtSize.Load()
}

func (a *Aof) incrSize(delta int64) {
	a.rtSize.Add(delta)
}

type RdbAof struct {
	rdb  *Rdb
	dir  string
	aofs []*Aof
}

func (r *RdbAof) ref() int {
	ref := 0
	if r.rdb != nil {
		ref += r.rdb.ref
	}
	for _, a := range r.aofs {
		ref += a.ref
	}
	return ref
}

func (r *RdbAof) indexAof(offset int64) *Aof {
	for i := len(r.aofs) - 1; i >= 0; i-- {
		aof := r.aofs[i]
		if aof.Left <= offset && aof.Right() >= offset {
			return aof
		}
	}
	return nil
}

func (r *RdbAof) Left() int64 {
	if r.rdb != nil {
		return r.rdb.Left
	}
	if len(r.aofs) != 0 {
		return r.aofs[0].Left
	}
	return -1
}

func (r *RdbAof) Right() int64 {
	if len(r.aofs) != 0 {
		return r.aofs[len(r.aofs)-1].Right()
	}
	if r.rdb != nil {
		return r.rdb.Left
	}
	return int64(-1)
}

func (s *Storer) initFiles() {
	s.rdbs = s.getRdbs()
}

func (s *Storer) getRdbs() []*RdbAof {
	rdbs := []*RdbAof{}
	if !dirExist(s.dir) {
		return nil
	}

	dirs := []string{}
	es, err := getSubDirs(s.dir)
	if err != nil {
		s.logger.Errorf("%v", err)
		return nil
	}
	for _, e := range es {
		ofs, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil {
			s.logger.Errorf("%v", err)
			continue
		}
		if ofs > 0 {
			dirs = append(dirs, e.Name())
		}
	}

	if err != nil {
		s.logger.Errorf("%v", err)
	}

	for _, rdbDir := range dirs {
		dir := filepath.Join(s.dir, rdbDir)
		aofs := []*Aof{}
		var rdb *Rdb
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				if strings.HasSuffix(info.Name(), ".aof") {
					fn := strings.TrimSuffix(info.Name(), ".aof")
					ofs, err := strconv.ParseInt(fn, 10, 64)
					if err != nil {
						s.logger.Errorf("wrong aof file : name(%s)", info.Name())
						return nil
					}
					a := &Aof{
						Left: ofs,
						Size: info.Size() - headerSize,
					}
					if a.Size > 0 { // size must greater than zero
						a.rtSize.Store(a.Size)
						aofs = append(aofs, a)
					}
				} else {
					rf := ParseRdbFile(info.Name(), false)
					if rf.IsValid() {
						rdb = &Rdb{
							Left:    rf.offset,
							RdbSize: rf.size,
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			s.logger.Errorf("%v", err)
			continue
		}

		ra := &RdbAof{
			rdb:  rdb,
			aofs: aofs,
			dir:  dir,
		}

		if len(ra.aofs) > 0 {
			sort.Slice(ra.aofs, func(i, j int) bool {
				return ra.aofs[i].Left < ra.aofs[j].Left
			})

			// remove gap
			s.truncateGap(ra)
		}

		rdbs = append(rdbs, ra)
	}

	sort.Slice(rdbs, func(i, j int) bool {
		return rdbs[i].Left() < rdbs[j].Left()
	})

	return rdbs
}

func (s *Storer) truncateGap(rdbAof *RdbAof) {
	for i := len(rdbAof.aofs) - 1; i > 0; i-- {
		if rdbAof.aofs[i].Left != rdbAof.aofs[i-1].Right() {
			// rename
			uaofs := rdbAof.aofs[:i]
			for _, a := range uaofs {
				opath := aofFilePath(rdbAof.dir, a.Left)
				err := os.Remove(opath)
				if err != nil {
					s.logger.Errorf("remove aof file : aof(%s), error(%v)", opath, err)
				} else {
					s.logger.Infof("remove aof file : aof(%s)", opath)
				}
			}
			if rdbAof.rdb != nil {
				opath := rdbFilePath(rdbAof.dir, rdbAof.rdb.Left, rdbAof.rdb.RdbSize)
				err := os.Remove(opath)
				if err != nil {
					s.logger.Errorf("remove rdb file : rdb(%s), error(%v)", opath, err)
				} else {
					s.logger.Infof("remove rdb file : rdb(%s)", opath)
				}
			}
			rdbAof.aofs = rdbAof.aofs[i:]
			rdbAof.rdb = nil
			return
		}
	}
}

func splitRdbAof(rdbAof *RdbAof) (res []*RdbAof) {
	pos := 0
	for i := 1; i < len(rdbAof.aofs); i++ {
		if rdbAof.aofs[i-1].Right() != rdbAof.aofs[i].Left {
			res = append(res, &RdbAof{
				rdb:  rdbAof.rdb,
				aofs: rdbAof.aofs[pos:i],
				dir:  rdbAof.dir,
			})
			rdbAof.rdb = nil
			pos = i
		}
	}
	if pos < len(rdbAof.aofs) {
		res = append(res, &RdbAof{
			rdb:  rdbAof.rdb,
			aofs: rdbAof.aofs[pos:],
			dir:  rdbAof.dir,
		})
	}
	return
}

func (s *Storer) getRdb(offset int64) *RdbAof {
	for i := len(s.rdbs) - 1; i >= 0; i-- {
		// left <= offset <= right
		if offset != -1 && s.rdbs[i].Left() <= offset && s.rdbs[i].Right() >= offset {
			return s.rdbs[i]
		} else if s.rdbs[i].Left() >= offset && s.rdbs[i].rdb != nil { // offset <= rdb.left
			return s.rdbs[i]
		}
	}
	return nil
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

	s.rdbLocker.RLock()
	defer s.rdbLocker.RUnlock()

	// remove redundant rdbAof
	s.gcRedundantRdbs()

	// remove unused rdb and aof
	s.gcRdbs()

}

func (s *Storer) gcRedundantRdbs() {
	remove := false
	for i := len(s.rdbs) - 1; i >= 0; i-- {
		ra := s.rdbs[i]
		if remove && ra.ref() == 0 {
			os.RemoveAll(ra.dir)
			s.rdbs = append(s.rdbs[:i], s.rdbs[i+1:]...)
			continue
		}
		if ra.rdb != nil {
			remove = true
		}
	}
}

func (s *Storer) gcRdbs() {

	size := int64(0)
	// walk from newest to oldest
	for i := len(s.rdbs) - 1; i >= 0; i-- {
		ra := s.rdbs[i]

		// iterate AOF files first
		aofLast := len(ra.aofs) - 1
		for ; aofLast >= 0; aofLast-- {
			size += ra.aofs[aofLast].rtSize.Load()
			if size > s.maxSize {
				break
			}
		}

		ref0 := true
		if ra.rdb != nil {
			size += ra.rdb.RdbSize
			if size > s.maxSize { // @TODO i == 0 and len(ra.aof) == 0
				if ra.rdb.ref == 0 {
					rdbfn := rdbFilePath(ra.dir, ra.rdb.Left, ra.rdb.RdbSize)
					if err := os.RemoveAll(rdbfn); err != nil {
						s.logger.Errorf("GC Logs, remove rdb file error : file(%s), error(%v)", rdbfn, err)
					} else {
						s.logger.Infof("GC Logs, remove rdb file : file(%s)", rdbfn)
					}
					ra.rdb = nil
				} else {
					ref0 = false
				}
			}
		}

		if ref0 {
			// oldest to newest
			for z := 0; z <= aofLast; z++ {
				if ra.aofs[z].ref > 0 {
					ref0 = false
					break
				} else {
					aoffn := aofFilePath(ra.dir, ra.aofs[z].Left)
					if err := os.RemoveAll(aoffn); err != nil {
						s.logger.Errorf("GC Logs, remove aof file error : file(%s), error(%v)", aoffn, err)
					} else {
						s.logger.Infof("GC Logs, remove aof file : file(%s)", aoffn)
					}
					ra.aofs = ra.aofs[z+1:]
					aofLast--
					z--
				}
			}
		}

		if size > s.maxSize { // try to remove an directory if it is empty
			os.Remove(ra.dir)
		}
		if ra.rdb == nil && len(ra.aofs) == 0 {
			s.rdbs = append(s.rdbs[:i], s.rdbs[i+1:]...)
		}
	}
}
