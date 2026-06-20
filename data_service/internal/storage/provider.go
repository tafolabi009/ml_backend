package storage

import (
	"encoding/json"
	"path/filepath"
)

// DatasetEntry is a lightweight dataset listing record
type DatasetEntry struct {
	DatasetID  string `json:"dataset_id"`
	Filename   string `json:"filename"`
	FileSize   int64  `json:"file_size"`
	FileType   string `json:"file_type"`
	UploadedAt string `json:"uploaded_at"`
}

// StorageProvider defines filesystem-backed storage operations used by DataService
type StorageProvider interface {
	// DatasetFilePath returns the absolute path where a dataset file should be stored
	DatasetFilePath(userID, datasetID, filename string) (string, error)
	// EnsureDatasetDir creates the dataset directory and returns its path
	EnsureDatasetDir(userID, datasetID string) (string, error)
	// WriteMetadata writes metadata.json into the dataset directory
	WriteMetadata(datasetDir string, meta map[string]interface{}) error
	// ReadMetadata reads metadata.json if present
	ReadMetadata(datasetDir string) (map[string]interface{}, error)
	// ListDatasets lists datasets for a user
	ListDatasets(userID string) ([]DatasetEntry, error)
	// DeleteDataset deletes the dataset directory
	DeleteDataset(userID, datasetID string) error
}

// Helper to marshal metadata
func marshalMeta(meta map[string]interface{}) ([]byte, error) {
	return json.MarshalIndent(meta, "", "  ")
}

func unmarshalMeta(b []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// safeAbs joins base and rel and returns absolute path
func safeAbs(base, rel string) (string, error) {
	p := filepath.Join(base, rel)
	ap, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return ap, nil
}
