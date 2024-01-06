package file

import (
	"os"
	"path/filepath"
	"time"
)

func GetDirectorySize(path string) (int64, time.Time, error) {
	var size int64
	modtime := time.Now()

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
			modTs := info.ModTime()
			if modtime.After(modTs) {
				modtime = modTs
			}
		}
		return nil
	})

	if err != nil {
		return 0, modtime, err
	}
	return size, modtime, nil
}
