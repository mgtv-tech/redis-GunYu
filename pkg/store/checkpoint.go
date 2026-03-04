package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/log"
)

// FileCheckpointInfo represents checkpoint information stored in file
type FileCheckpointInfo struct {
	RunId   string `json:"runId"`
	Offset  int64  `json:"offset"`
	Version string `json:"version"`
	Mtime   int64  `json:"mtime"`
	DbId    int    `json:"dbId"`
}

// FileCheckpoint manages checkpoint data in local files
type FileCheckpoint struct {
	baseDir string
	inputId string
	logger  log.Logger
	mu      sync.RWMutex
	cache   *FileCheckpointInfo
}

// NewFileCheckpoint creates a new file-based checkpoint manager
func NewFileCheckpoint(baseDir, inputId string) *FileCheckpoint {
	return &FileCheckpoint{
		baseDir: baseDir,
		inputId: inputId,
		logger:  log.WithLogger(config.LogModuleName(fmt.Sprintf("[FileCheckpoint(%s)] ", inputId))),
	}
}

// checkpointFilePath returns the path to the checkpoint file
func (fc *FileCheckpoint) checkpointFilePath() string {
	return filepath.Join(fc.baseDir, fc.inputId, "checkpoint.json")
}

// tempCheckpointFilePath returns the path to the temporary checkpoint file
func (fc *FileCheckpoint) tempCheckpointFilePath() string {
	return filepath.Join(fc.baseDir, fc.inputId, "checkpoint.json.tmp")
}

// Get reads checkpoint from file, returns nil if not found
func (fc *FileCheckpoint) Get(runIds []string) (*FileCheckpointInfo, error) {
	fc.mu.RLock()
	defer fc.mu.RUnlock()

	// Try to read from cache first
	if fc.cache != nil {
		for _, id := range runIds {
			if id == fc.cache.RunId {
				return fc.cache, nil
			}
		}
	}

	// Read from file
	filePath := fc.checkpointFilePath()
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint file error: %w", err)
	}

	var cp FileCheckpointInfo
	if err := json.Unmarshal(data, &cp); err != nil {
		fc.logger.Errorf("unmarshal checkpoint error: %v", err)
		return nil, fmt.Errorf("unmarshal checkpoint error: %w", err)
	}

	// Verify runId matches
	for _, id := range runIds {
		if id == cp.RunId || id == "" {
			return &cp, nil
		}
	}

	// RunId doesn't match, return nil (need full sync)
	return nil, nil
}

// Set writes checkpoint to file atomically
func (fc *FileCheckpoint) Set(cp *FileCheckpointInfo) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	cp.Mtime = time.Now().UnixNano()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint error: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(fc.checkpointFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir error: %w", err)
	}

	// Write to temp file first
	tempPath := fc.tempCheckpointFilePath()
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp checkpoint file error: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, fc.checkpointFilePath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename checkpoint file error: %w", err)
	}

	// Update cache
	fc.cache = cp
	return nil
}

// SetOffset updates only the offset field
func (fc *FileCheckpoint) SetOffset(runId string, offset int64) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	cp := fc.cache
	if cp == nil {
		cp = &FileCheckpointInfo{
			RunId:   runId,
			Version: config.Version,
		}
	}
	cp.Offset = offset
	cp.Mtime = time.Now().UnixNano()
	if cp.RunId == "" || cp.RunId == "?" {
		cp.RunId = runId
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint error: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(fc.checkpointFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir error: %w", err)
	}

	// Write to temp file first
	tempPath := fc.tempCheckpointFilePath()
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp checkpoint file error: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, fc.checkpointFilePath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename checkpoint file error: %w", err)
	}

	fc.cache = cp
	return nil
}

// Delete removes the checkpoint file
func (fc *FileCheckpoint) Delete() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.cache = nil

	filePath := fc.checkpointFilePath()
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete checkpoint file error: %w", err)
	}
	return nil
}

// SetRunId updates the runId in checkpoint
func (fc *FileCheckpoint) SetRunId(runId string) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	cp := fc.cache
	if cp == nil {
		cp = &FileCheckpointInfo{
			RunId:   runId,
			Offset:  -1,
			Version: config.Version,
		}
	} else {
		cp.RunId = runId
	}
	cp.Mtime = time.Now().UnixNano()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint error: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(fc.checkpointFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir error: %w", err)
	}

	tempPath := fc.tempCheckpointFilePath()
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("write temp checkpoint file error: %w", err)
	}

	if err := os.Rename(tempPath, fc.checkpointFilePath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("rename checkpoint file error: %w", err)
	}

	fc.cache = cp
	return nil
}

// Validate checks if the checkpoint is valid for the given runIds
func (fc *FileCheckpoint) Validate(runIds []string) bool {
	cp, err := fc.Get(runIds)
	if err != nil {
		return false
	}
	if cp == nil || cp.RunId == "" || cp.RunId == "?" {
		return false
	}
	for _, id := range runIds {
		if id == cp.RunId {
			return true
		}
	}
	return false
}

// GetOffset returns the current offset, or -1 if not found
func (fc *FileCheckpoint) GetOffset(runIds []string) (int64, error) {
	cp, err := fc.Get(runIds)
	if err != nil {
		return -1, err
	}
	if cp == nil {
		return -1, nil
	}
	return cp.Offset, nil
}

// ErrCheckpointNotFound is returned when checkpoint is not found
var ErrCheckpointNotFound = errors.New("checkpoint not found")

// CheckpointProvider is an interface for providing checkpoint functionality
type CheckpointProvider interface {
	Get(runIds []string) (*FileCheckpointInfo, error)
	Set(cp *FileCheckpointInfo) error
	SetOffset(runId string, offset int64) error
	SetRunId(runId string) error
	Delete() error
	GetOffset(runIds []string) (int64, error)
	Validate(runIds []string) bool
}

// Ensure FileCheckpoint implements CheckpointProvider
var _ CheckpointProvider = (*FileCheckpoint)(nil)
