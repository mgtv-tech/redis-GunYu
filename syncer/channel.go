package syncer

import (
	"fmt"
	"io"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
	"github.com/mgtv-tech/redis-GunYu/pkg/store"
)

var _ Channel = &StoreChannel{}

type Channel interface {
	StartPoint([]string) (StartPoint, error)
	SetRunId(string) error
	DelRunId(string) error
	RunId() string
	IsValidOffset(Offset) bool
	GetOffsetRange(string) (int64, int64)
	GetRdb(string) (int64, int64)
	NewRdbWriter(io.Reader, int64, int64) (*store.RdbWriter, error)
	NewAofWritter(r io.Reader, offset int64) (*store.AofWriter, error)
	NewReader(Offset) (*store.Reader, error)
	Close() error
}

type StoreChannel struct {
	storer    *store.Storer
	StorerCfg StorerConf
	logger    log.Logger
}

func NewStoreChannel(cfg StorerConf) *StoreChannel {
	storer := store.NewStorer(cfg.Id, cfg.Dir, cfg.MaxSize, cfg.LogSize, cfg.flush)
	return &StoreChannel{
		storer: storer,
		logger: log.WithLogger(config.LogModuleName(fmt.Sprintf("[StoreChannel(%d)] ", cfg.Id))),
	}
}

func (sc *StoreChannel) StartPoint(ids []string) (StartPoint, error) {
	if len(ids) == 0 {
		return StartPoint{
			RunId:  sc.storer.RunId(),
			Offset: sc.storer.LatestOffset(),
		}, nil
	}
	offset, err := sc.storer.VerifyRunId(ids)
	if err != nil {
		return StartPoint{}, err
	}
	if offset < 0 {
		return StartPoint{
			RunId:  "?",
			Offset: -1,
		}, nil
	}

	return StartPoint{
		RunId:  sc.storer.RunId(),
		Offset: offset,
	}, nil
}

func (sc *StoreChannel) IsValidOffset(off Offset) bool {
	if off.RunId == "?" {
		return !sc.storer.IsValidOffset(-1)
	}
	if off.RunId != sc.storer.RunId() {
		return false
	}
	return sc.storer.IsValidOffset(off.Offset)
}

func (sc *StoreChannel) GetOffsetRange(runId string) (int64, int64) {
	if runId != sc.storer.RunId() {
		return -1, -1
	}
	return sc.storer.GetOffsetRange()
}

func (sc *StoreChannel) GetRdb(runId string) (int64, int64) {
	if runId != sc.storer.RunId() {
		return -1, -1
	}
	return sc.storer.GetRdb()
}

func (sc *StoreChannel) NewReader(offset Offset) (*store.Reader, error) {
	// if offset.RunId != sc.storer.RunId() {
	// 	err := sc.storer.SetRunId(offset.RunId)
	// 	if err != nil {
	// 		sc.logger.Errorf("storer.SetRunId error : offset(%v), err(%v)", offset, err)
	// 		return nil, err
	// 	}
	// }
	r, err := sc.storer.GetReader(offset.Offset, config.Get().Channel.VerifyCrc)
	if err != nil {
		sc.logger.Errorf("storer.GetReader error : offset(%v), err(%v)", offset, err)
	}
	return r, err
}

func (sc *StoreChannel) NewRdbWriter(reader io.Reader, offset int64, size int64) (*store.RdbWriter, error) {
	w, err := sc.storer.GetRdbWriter(reader, offset, size)
	if err != nil {
		sc.logger.Errorf("get rdb writer error : offset(%d), size(%d), err(%w)", offset, size, err)
	}
	return w, err
}

func (sc *StoreChannel) NewAofWritter(r io.Reader, offset int64) (*store.AofWriter, error) {
	w, err := sc.storer.GetAofWritter(r, offset)
	if err != nil {
		sc.logger.Errorf("get aof writer error : offset(%d), err(%w)", offset, err)
	}
	return w, err
}

func (sc *StoreChannel) SetRunId(runId string) error {
	err := sc.storer.SetRunId(runId)
	if err != nil {
		sc.logger.Errorf("SetRunId : runId(%s), err(%v)", runId, err)
	} else {
		sc.logger.Debugf("SetRunId : runId(%s)", runId)
	}
	return err
}

func (sc *StoreChannel) DelRunId(runId string) error {
	err := sc.storer.DelRunId(runId)
	if err != nil {
		sc.logger.Errorf("DelRunId : runId(%s), err(%v)", runId, err)
	} else {
		sc.logger.Infof("DelRunId : runId(%s)", runId)
	}
	return err
}

func (sc *StoreChannel) RunId() string {
	return sc.storer.RunId()
}

func (sc *StoreChannel) Close() error {
	err := sc.storer.Close()
	if err != nil {
		sc.logger.Errorf("close storer : err(%v)", err)
	}
	return err
}
