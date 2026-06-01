package storage

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "time"
)

// LocalProvider implements StorageProvider using local filesystem under basePath
type LocalProvider struct {
    basePath string
}

func NewLocalProvider(basePath string) (*LocalProvider, error) {
    if err := os.MkdirAll(basePath, 0755); err != nil {
        return nil, err
    }
    return &LocalProvider{basePath: basePath}, nil
}

func (l *LocalProvider) EnsureDatasetDir(userID, datasetID string) (string, error) {
    dir := filepath.Join(l.basePath, userID, datasetID)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return "", err
    }
    return dir, nil
}

func (l *LocalProvider) DatasetFilePath(userID, datasetID, filename string) (string, error) {
    dir := filepath.Join(l.basePath, userID, datasetID)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return "", err
    }
    return filepath.Join(dir, filename), nil
}

func (l *LocalProvider) WriteMetadata(datasetDir string, meta map[string]interface{}) error {
    b, err := json.MarshalIndent(meta, "", "  ")
    if err != nil {
        return err
    }
    path := filepath.Join(datasetDir, "metadata.json")
    return os.WriteFile(path, b, 0644)
}

func (l *LocalProvider) ReadMetadata(datasetDir string) (map[string]interface{}, error) {
    path := filepath.Join(datasetDir, "metadata.json")
    b, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var m map[string]interface{}
    if err := json.Unmarshal(b, &m); err != nil {
        return nil, err
    }
    return m, nil
}

func (l *LocalProvider) ListDatasets(userID string) ([]DatasetEntry, error) {
    userDir := filepath.Join(l.basePath, userID)
    var result []DatasetEntry
    entries, err := os.ReadDir(userDir)
    if err != nil {
        if os.IsNotExist(err) {
            return result, nil
        }
        return nil, err
    }
    for _, e := range entries {
        if !e.IsDir() {
            continue
        }
        dsDir := filepath.Join(userDir, e.Name())
        // prefer metadata.json
        metaPath := filepath.Join(dsDir, "metadata.json")
        if b, err := os.ReadFile(metaPath); err == nil {
            var m map[string]interface{}
            if jerr := json.Unmarshal(b, &m); jerr == nil {
                fe := DatasetEntry{
                    DatasetID: toString(m["dataset_id"]),
                    Filename:  toString(m["filename"]),
                    FileType:  toString(m["file_type"]),
                    UploadedAt: toString(m["uploaded_at"]),
                }
                if v, ok := m["file_size"]; ok {
                    switch t := v.(type) {
                    case float64:
                        fe.FileSize = int64(t)
                    case int64:
                        fe.FileSize = t
                    }
                }
                result = append(result, fe)
                continue
            }
        }

        // fallback: pick first file
        files, _ := os.ReadDir(dsDir)
        for _, f := range files {
            if f.IsDir() {
                continue
            }
            fi, err := f.Info()
            if err != nil {
                continue
            }
            result = append(result, DatasetEntry{
                DatasetID: e.Name(),
                Filename:  f.Name(),
                FileSize:  fi.Size(),
                FileType:  strings.TrimPrefix(strings.ToLower(filepath.Ext(f.Name())), "."),
                UploadedAt: fi.ModTime().Format(time.RFC3339),
            })
            break
        }
    }
    return result, nil
}

func (l *LocalProvider) DeleteDataset(userID, datasetID string) error {
    dsPath := filepath.Join(l.basePath, userID, datasetID)
    return os.RemoveAll(dsPath)
}

// helper to convert interface{} to string
func toString(v interface{}) string {
    if v == nil {
        return ""
    }
    switch t := v.(type) {
    case string:
        return t
    default:
        return fmt.Sprintf("%v", t)
    }
}
