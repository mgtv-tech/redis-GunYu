package store

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ikenchina/redis-GunYu/pkg/log"
)

func newNopWriteCloser(writer io.Writer) io.WriteCloser {
	wc := &writeCloser{}
	wc.Writer = writer
	return wc
}

type writeCloser struct {
	io.Writer
	close func() error
}

func (wc *writeCloser) Close() error {
	if wc.close == nil {
		return nil
	}
	return wc.close()
}

func getSubDirs(dirname string) ([]fs.DirEntry, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	dirs, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return dirs, nil
}

func getAllRunIds(dir string) ([]string, error) {
	dirs := []string{}

	entries, err := getSubDirs(dir)
	if err != nil {
		return dirs, err
	}

	for _, en := range entries {
		dirs = append(dirs, en.Name())
	}
	return dirs, err
}

func getLatestRunId(dir string) (string, error) {
	ids, err := getAllRunIds(dir)
	if err != nil {
		return "", err
	}
	maxOffset := int64(0)
	rid := ""
	for _, id := range ids {
		subDirs, err := getSubDirs(filepath.Join(dir, id))
		if err != nil {
			return rid, err
		}
		for _, sub := range subDirs {
			offset, err := strconv.ParseInt(sub.Name(), 10, 64)
			if err != nil {
				log.Error(err)
				continue
			}
			if offset > int64(maxOffset) {
				maxOffset = offset
				rid = id
			}
		}
		// err := filepath.WalkDir(dir, func(path string, info fs.DirEntry, err error) error {
		// 	offset, err := strconv.ParseInt(info.Name(), 10, 64)
		// 	if err != nil {
		// 		log.Error(err)
		// 		return nil
		// 	}
		// 	if offset > int64(maxOffset) {
		// 		maxOffset = offset
		// 		rid = id
		// 	}
		// 	return nil
		// })
	}
	return rid, nil
}

func EnsureRunIdStore(root, runId string) error {
	return MkdirIfNoExist(filepath.Join(root, runId))
}

func MkdirIfNoExist(dir string) error {
	if dirExist(dir) {
		return nil
	}
	return os.MkdirAll(dir, 0777)
}

func ExistReplId(root, id string) bool {
	return dirExist(filepath.Join(root, id))
}

func fileExist(fn string) bool {
	_, err := os.Stat(fn)
	return err == nil
}

func dirExist(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil
}

// func CreateReplId(dir string, replId string) error {
// 	return os.Mkdir(filepath.Join(dir, replId), 0777)
// }

func changeReplId(dir string, replId1, replId2 string) error {
	cdir1 := filepath.Join(dir, replId1)
	cdir2 := filepath.Join(dir, replId2)
	_, err := os.Stat(cdir1)
	if err != nil {
		return err
	}
	_, err = os.Stat(cdir2)
	if err != nil && os.IsNotExist(err) {
		return os.Rename(cdir1, cdir2)
	}
	err = os.RemoveAll(cdir2)
	if err != nil {
		return err
	}
	return os.MkdirAll(cdir1, 0777)
}
