package service

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	repo "github.com/synthos/data-service/internal/repository"
	stor "github.com/synthos/data-service/internal/storage"
	pb "github.com/tafolabi009/backend/proto/data"
)

// DataServiceServer implements the DataService gRPC service
type DataServiceServer struct {
	pb.UnimplementedDataServiceServer
	storagePath  string
	provider     stor.StorageProvider
	metadataRepo *repo.MetadataRepository
}

// NewDataServiceServer creates a new data service server
func NewDataServiceServer(storagePath string) *DataServiceServer {
	// Ensure storage directory exists
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// initialize local storage provider
	lp, err := stor.NewLocalProvider(storagePath)
	if err != nil {
		log.Fatalf("Failed to initialize storage provider: %v", err)
	}

	return &DataServiceServer{
		storagePath: storagePath,
		provider:    lp,
	}
}

// NewDataServiceServerWithRepo creates a data service server backed by storage and Postgres metadata.
func NewDataServiceServerWithRepo(storagePath string, metadataRepo *repo.MetadataRepository) *DataServiceServer {
	server := NewDataServiceServer(storagePath)
	server.metadataRepo = metadataRepo
	return server
}

// UploadDataset handles streaming dataset upload
func (s *DataServiceServer) UploadDataset(stream pb.DataService_UploadDatasetServer) error {
	log.Println("UploadDataset: Stream started")

	var metadata *pb.DatasetMetadata
	var file *os.File
	var bytesReceived int64

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Upload complete
			if file != nil {
				file.Close()
			}

			// A well-formed upload must begin with a metadata message; without it
			// there is nothing to persist and the response below would nil-panic.
			if metadata == nil {
				return fmt.Errorf("upload stream closed before metadata was received")
			}

			// persist metadata.json beside file for compatibility
			if metadata != nil {
				metadata.FileSize = bytesReceived
				metaObj := map[string]interface{}{
					"dataset_id":   metadata.DatasetId,
					"user_id":      metadata.UserId,
					"filename":     metadata.Filename,
					"file_size":    bytesReceived,
					"file_type":    metadata.FileType,
					"storage_path": metadata.StoragePath,
					"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
					"metadata":     metadata.CustomMetadata,
				}
				_ = s.provider.WriteMetadata(filepath.Dir(metadata.StoragePath), metaObj)

				if s.metadataRepo != nil {
					custom := make(map[string]any, len(metadata.CustomMetadata))
					for k, v := range metadata.CustomMetadata {
						custom[k] = v
					}
					meta := repo.DatasetMetadata{
						ID:          metadata.DatasetId,
						UserID:      metadata.UserId,
						Filename:    metadata.Filename,
						FileSize:    bytesReceived,
						FileType:    metadata.FileType,
						StoragePath: metadata.StoragePath,
						Status:      "ready",
						Metadata:    custom,
					}
					_ = s.metadataRepo.UpsertDataset(context.Background(), meta)
				}
			}

			response := &pb.UploadDatasetResponse{
				DatasetId:     metadata.DatasetId,
				Status:        "success",
				StoragePath:   metadata.StoragePath,
				BytesUploaded: bytesReceived,
			}

			log.Printf("Upload completed: %d bytes for dataset %s", bytesReceived, metadata.DatasetId)
			return stream.SendAndClose(response)
		}

		if err != nil {
			log.Printf("Error receiving upload chunk: %v", err)
			return err
		}

		switch data := req.Data.(type) {
		case *pb.UploadDatasetRequest_Metadata:
			// First message contains metadata
			metadata = data.Metadata
			log.Printf("Received metadata for dataset: %s", metadata.DatasetId)

			// Create file path
			filePath, err := s.provider.DatasetFilePath(metadata.UserId, metadata.DatasetId, metadata.Filename)
			if err != nil {
				return fmt.Errorf("failed to compute dataset path: %w", err)
			}

			// Open file for writing
			file, err = os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}
			metadata.StoragePath = filePath
		case *pb.UploadDatasetRequest_Chunk:
			// Subsequent messages contain data chunks
			if file == nil {
				return fmt.Errorf("received chunk before metadata")
			}

			n, err := file.Write(data.Chunk)
			if err != nil {
				return fmt.Errorf("failed to write chunk: %w", err)
			}
			bytesReceived += int64(n)
		}
	}
}

// GetDatasetMetadata retrieves metadata for a specific dataset
func (s *DataServiceServer) GetDatasetMetadata(ctx context.Context, req *pb.GetDatasetRequest) (*pb.GetDatasetResponse, error) {
	log.Printf("GetDatasetMetadata: dataset_id=%s, user_id=%s", req.DatasetId, req.UserId)
	if s.metadataRepo != nil {
		if meta, err := s.metadataRepo.GetDataset(ctx, req.UserId, req.DatasetId); err == nil && meta != nil {
			ds := &pb.Dataset{
				Id:          meta.ID,
				UserId:      meta.UserID,
				Filename:    meta.Filename,
				FileSize:    meta.FileSize,
				FileType:    meta.FileType,
				Status:      meta.Status,
				StoragePath: meta.StoragePath,
				UploadedAt:  meta.CreatedAt.UTC().Format(time.RFC3339),
			}
			if meta.ProcessedAt != nil {
				ds.ProcessedAt = meta.ProcessedAt.UTC().Format(time.RFC3339)
			}
			return &pb.GetDatasetResponse{Dataset: ds}, nil
		}
	}

	// Look for dataset under storagePath/userId/datasetId
	datasetDir := filepath.Join(s.storagePath, req.UserId, req.DatasetId)
	info, err := os.Stat(datasetDir)
	if err != nil || !info.IsDir() {
		return &pb.GetDatasetResponse{ErrorMessage: "dataset not found"}, nil
	}

	// Try provider metadata read
	if m, err := s.provider.ReadMetadata(datasetDir); err == nil {
		ds := &pb.Dataset{
			Id:          req.DatasetId,
			UserId:      req.UserId,
			Filename:    toString(m["filename"]),
			FileType:    toString(m["file_type"]),
			StoragePath: toString(m["storage_path"]),
			Status:      "ready",
			UploadedAt:  toString(m["uploaded_at"]),
			ProcessedAt: toString(m["uploaded_at"]),
		}
		if v, ok := m["file_size"]; ok {
			switch t := v.(type) {
			case float64:
				ds.FileSize = int64(t)
			case int64:
				ds.FileSize = t
			}
		}
		return &pb.GetDatasetResponse{Dataset: ds}, nil
	}

	// fallback: find first file inside datasetDir
	files, err := os.ReadDir(datasetDir)
	if err != nil || len(files) == 0 {
		return &pb.GetDatasetResponse{ErrorMessage: "no files for dataset"}, nil
	}

	// prefer the first regular file entry
	var fileName string
	var filePath string
	var fileInfo os.FileInfo
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fi, ferr := f.Info()
		if ferr != nil {
			continue
		}
		fileName = f.Name()
		filePath = filepath.Join(datasetDir, fileName)
		fileInfo = fi
		break
	}

	if fileInfo == nil {
		return &pb.GetDatasetResponse{ErrorMessage: "no files for dataset"}, nil
	}

	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(fileName)), ".")
	if ext == "" {
		ext = "binary"
	}
	dataset := &pb.Dataset{
		Id:          req.DatasetId,
		UserId:      req.UserId,
		Filename:    fileName,
		FileSize:    fileInfo.Size(),
		FileType:    ext,
		Status:      "ready",
		StoragePath: filePath,
		UploadedAt:  fileInfo.ModTime().Format(time.RFC3339),
		ProcessedAt: fileInfo.ModTime().Format(time.RFC3339),
	}

	return &pb.GetDatasetResponse{Dataset: dataset}, nil
}

// ListDatasets returns a paginated list of datasets
func (s *DataServiceServer) ListDatasets(ctx context.Context, req *pb.ListDatasetsRequest) (*pb.ListDatasetsResponse, error) {
	log.Printf("ListDatasets: user_id=%s, page=%d, page_size=%d", req.UserId, req.Page, req.PageSize)
	if s.metadataRepo != nil {
		page := int(req.Page)
		if page <= 0 {
			page = 1
		}
		pageSize := int(req.PageSize)
		if pageSize <= 0 {
			pageSize = 20
		}
		offset := (page - 1) * pageSize
		metas, err := s.metadataRepo.ListDatasets(ctx, req.UserId, pageSize, offset)
		if err == nil {
			totalCount, countErr := s.metadataRepo.CountDatasets(ctx, req.UserId)
			if countErr != nil {
				totalCount = int64(len(metas))
			}
			var datasets []*pb.Dataset
			for _, meta := range metas {
				ds := &pb.Dataset{
					Id:          meta.ID,
					UserId:      meta.UserID,
					Filename:    meta.Filename,
					FileSize:    meta.FileSize,
					FileType:    meta.FileType,
					Status:      meta.Status,
					StoragePath: meta.StoragePath,
					UploadedAt:  meta.CreatedAt.UTC().Format(time.RFC3339),
				}
				if meta.ProcessedAt != nil {
					ds.ProcessedAt = meta.ProcessedAt.UTC().Format(time.RFC3339)
				}
				datasets = append(datasets, ds)
			}
			return &pb.ListDatasetsResponse{
				Datasets: datasets,
				Pagination: &pb.Pagination{
					Page:       int32(page),
					PageSize:   int32(pageSize),
					TotalCount: totalCount,
					TotalPages: int32((int(totalCount) + pageSize - 1) / pageSize),
				},
			}, nil
		}
	}

	// Use storage provider to list datasets for user
	providerEntries, err := s.provider.ListDatasets(req.UserId)
	if err == nil && len(providerEntries) > 0 {
		var datasets []*pb.Dataset
		for _, pe := range providerEntries {
			datasets = append(datasets, &pb.Dataset{
				Id:         pe.DatasetID,
				UserId:     req.UserId,
				Filename:   pe.Filename,
				FileSize:   pe.FileSize,
				FileType:   pe.FileType,
				Status:     "ready",
				UploadedAt: pe.UploadedAt,
			})
		}

		// pagination
		page := int(req.Page)
		if page <= 0 {
			page = 1
		}
		pageSize := int(req.PageSize)
		if pageSize <= 0 {
			pageSize = 20
		}
		total := int64(len(datasets))
		totalPages := int32((len(datasets) + pageSize - 1) / pageSize)

		start := (page - 1) * pageSize
		end := start + pageSize
		if start > len(datasets) {
			start = len(datasets)
		}
		if end > len(datasets) {
			end = len(datasets)
		}

		paged := datasets[start:end]

		pagination := &pb.Pagination{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalCount: total,
			TotalPages: totalPages,
		}

		return &pb.ListDatasetsResponse{Datasets: paged, Pagination: pagination}, nil
	}

	userDir := filepath.Join(s.storagePath, req.UserId)
	entries, err := os.ReadDir(userDir)
	if err != nil {
		return &pb.ListDatasetsResponse{ErrorMessage: "failed to read user directory"}, nil
	}

	var datasets []*pb.Dataset
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dsId := e.Name()
		dsDir := filepath.Join(userDir, dsId)
		// prefer metadata.json if present
		metaPath := filepath.Join(dsDir, "metadata.json")
		if b, err := os.ReadFile(metaPath); err == nil {
			var m map[string]interface{}
			if jerr := json.Unmarshal(b, &m); jerr == nil {
				// build dataset from metadata
				ds := &pb.Dataset{
					Id:         dsId,
					UserId:     req.UserId,
					Filename:   toString(m["filename"]),
					FileType:   toString(m["file_type"]),
					Status:     "ready",
					UploadedAt: toString(m["uploaded_at"]),
				}
				if v, ok := m["file_size"]; ok {
					switch t := v.(type) {
					case float64:
						ds.FileSize = int64(t)
					case int64:
						ds.FileSize = t
					}
				}
				datasets = append(datasets, ds)
				continue
			}
		}

		// fallback: pick first regular file
		files, _ := os.ReadDir(dsDir)
		if len(files) == 0 {
			continue
		}
		var fileName string
		var fileInfo os.FileInfo
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			fileName = f.Name()
			if fi, err := os.Stat(filepath.Join(dsDir, fileName)); err == nil {
				fileInfo = fi
				break
			}
		}
		if fileName == "" || fileInfo == nil {
			continue
		}

		ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(fileName)), ".")
		if ext == "" {
			ext = "binary"
		}

		datasets = append(datasets, &pb.Dataset{
			Id:         dsId,
			UserId:     req.UserId,
			Filename:   fileName,
			FileSize:   fileInfo.Size(),
			FileType:   ext,
			Status:     "ready",
			UploadedAt: fileInfo.ModTime().Format(time.RFC3339),
		})
	}

	// pagination
	page := int(req.Page)
	if page <= 0 {
		page = 1
	}
	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 20
	}
	total := int64(len(datasets))
	totalPages := int32((len(datasets) + pageSize - 1) / pageSize)

	start := (page - 1) * pageSize
	end := start + pageSize
	if start > len(datasets) {
		start = len(datasets)
	}
	if end > len(datasets) {
		end = len(datasets)
	}

	paged := datasets[start:end]

	pagination := &pb.Pagination{
		Page:       int32(page),
		PageSize:   int32(pageSize),
		TotalCount: total,
		TotalPages: totalPages,
	}

	return &pb.ListDatasetsResponse{Datasets: paged, Pagination: pagination}, nil
}

// DeleteDataset removes a dataset
func (s *DataServiceServer) DeleteDataset(ctx context.Context, req *pb.DeleteDatasetRequest) (*pb.DeleteDatasetResponse, error) {
	log.Printf("DeleteDataset: dataset_id=%s, user_id=%s", req.DatasetId, req.UserId)
	if s.metadataRepo != nil {
		if err := s.metadataRepo.DeleteDataset(ctx, req.UserId, req.DatasetId); err != nil {
			return &pb.DeleteDatasetResponse{Success: false, Message: fmt.Sprintf("failed to delete metadata: %v", err)}, nil
		}
	}
	// Delegate deletion to storage provider
	if err := s.provider.DeleteDataset(req.UserId, req.DatasetId); err != nil {
		return &pb.DeleteDatasetResponse{Success: false, Message: fmt.Sprintf("failed to delete: %v", err)}, nil
	}

	return &pb.DeleteDatasetResponse{
		Success:   true,
		Message:   fmt.Sprintf("Dataset %s deleted successfully", req.DatasetId),
		DeletedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// helper: path to index directory
func (s *DataServiceServer) storage_path_index() string {
	return filepath.Join(s.storagePath, "_index")
}

// updateUserIndex appends or updates a dataset entry in the per-user index
func (s *DataServiceServer) updateUserIndex(userId, datasetId string, meta map[string]interface{}) error {
	idxDir := s.storage_path_index()
	if err := os.MkdirAll(idxDir, 0755); err != nil {
		return err
	}
	idxPath := filepath.Join(idxDir, userId+".json")

	var entries []map[string]interface{}
	if b, err := os.ReadFile(idxPath); err == nil {
		_ = json.Unmarshal(b, &entries)
	}

	// remove any existing entry for datasetId
	filtered := []map[string]interface{}{}
	for _, e := range entries {
		if toString(e["dataset_id"]) != datasetId {
			filtered = append(filtered, e)
		}
	}

	// append new entry
	filtered = append(filtered, meta)

	if b, err := json.MarshalIndent(filtered, "", "  "); err == nil {
		return os.WriteFile(idxPath, b, 0644)
	}
	return nil
}

// removeFromUserIndex removes a dataset entry from the per-user index
func (s *DataServiceServer) removeFromUserIndex(userId, datasetId string) error {
	idxDir := s.storage_path_index()
	idxPath := filepath.Join(idxDir, userId+".json")
	if _, err := os.Stat(idxPath); err != nil {
		return nil
	}

	var entries []map[string]interface{}
	if b, err := os.ReadFile(idxPath); err == nil {
		_ = json.Unmarshal(b, &entries)
	}

	filtered := []map[string]interface{}{}
	for _, e := range entries {
		if toString(e["dataset_id"]) != datasetId {
			filtered = append(filtered, e)
		}
	}

	if len(filtered) == 0 {
		// remove file
		_ = os.Remove(idxPath)
		return nil
	}

	if b, err := json.MarshalIndent(filtered, "", "  "); err == nil {
		return os.WriteFile(idxPath, b, 0644)
	}
	return nil
}

// ProfileDataset analyzes dataset and returns profiling information
func (s *DataServiceServer) ProfileDataset(ctx context.Context, req *pb.ProfileDatasetRequest) (*pb.ProfileDatasetResponse, error) {
	log.Printf("ProfileDataset: dataset_id=%s, format=%s", req.DatasetId, req.DataFormat)
	// locate file
	var filePath string
	if req.DatasetPath != "" {
		filePath = req.DatasetPath
	} else {
		// try to find file under storagePath
		users, _ := os.ReadDir(s.storagePath)
		for _, u := range users {
			if !u.IsDir() {
				continue
			}
			candidate := filepath.Join(s.storagePath, u.Name(), req.DatasetId)
			if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
				// prefer metadata.json if present
				metaPath := filepath.Join(candidate, "metadata.json")
				if b, err := os.ReadFile(metaPath); err == nil {
					var m map[string]interface{}
					if jerr := json.Unmarshal(b, &m); jerr == nil {
						if sp, ok := m["storage_path"].(string); ok && sp != "" {
							filePath = sp
							break
						}
					}
				}

				entries, _ := os.ReadDir(candidate)
				if len(entries) > 0 {
					filePath = filepath.Join(candidate, entries[0].Name())
					break
				}
			}
		}
	}

	if filePath == "" {
		return &pb.ProfileDatasetResponse{ErrorMessage: "dataset file not found"}, nil
	}

	f, err := os.Open(filePath)
	if err != nil {
		return &pb.ProfileDatasetResponse{ErrorMessage: fmt.Sprintf("failed to open file: %v", err)}, nil
	}
	defer f.Close()

	// Basic CSV profiling: count rows and columns (sample or full)
	sampleSize := int(req.Config.SampleSize)
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	scanner := bufio.NewScanner(f)
	rowCount := int64(0)
	var header string
	var colCount int32
	completenessSum := 0.0
	scanned := 0

	for scanner.Scan() {
		line := scanner.Text()
		if rowCount == 0 {
			header = line
			cols := csv.NewReader(strings.NewReader(header))
			rec, err := cols.Read()
			if err == nil {
				colCount = int32(len(rec))
			} else {
				// fallback split
				colCount = int32(len(strings.Split(header, ",")))
			}
			rowCount++
			continue
		}

		rowCount++
		if scanned < sampleSize {
			// simple completeness heuristic: fraction of non-empty fields
			parts := strings.Split(line, ",")
			nonEmpty := 0
			for _, p := range parts {
				if strings.TrimSpace(p) != "" {
					nonEmpty++
				}
			}
			if len(parts) > 0 {
				completenessSum += float64(nonEmpty) / float64(len(parts))
			}
			scanned++
		}
	}

	completeness := 1.0
	if scanned > 0 {
		completeness = completenessSum / float64(scanned)
	}

	profile := &pb.DatasetProfile{
		RowCount:    rowCount,
		ColumnCount: colCount,
		Quality: &pb.DataQuality{
			Completeness: float32(completeness),
			Uniqueness:   0.0,
			Validity:     1.0,
		},
	}

	return &pb.ProfileDatasetResponse{
		DatasetId: req.DatasetId,
		Profile:   profile,
		Status:    "completed",
	}, nil
}

// StreamDataset streams dataset chunks for processing
func (s *DataServiceServer) StreamDataset(req *pb.StreamDatasetRequest, stream pb.DataService_StreamDatasetServer) error {
	log.Printf("StreamDataset: dataset_id=%s, chunk_size=%d", req.DatasetId, req.ChunkSize)
	// Determine file path
	var filePath string
	if req.DatasetPath != "" {
		filePath = req.DatasetPath
	} else {
		// try to find file under storagePath
		users, err := os.ReadDir(s.storagePath)
		if err != nil {
			return fmt.Errorf("failed to read storage path: %w", err)
		}
		for _, u := range users {
			if !u.IsDir() {
				continue
			}
			candidate := filepath.Join(s.storagePath, u.Name(), req.DatasetId)
			if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
				entries, _ := os.ReadDir(candidate)
				if len(entries) > 0 {
					filePath = filepath.Join(candidate, entries[0].Name())
					break
				}
			}
		}
	}

	if filePath == "" {
		return errors.New("dataset file not found")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open dataset file: %w", err)
	}
	defer f.Close()

	chunkSize := int(req.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	startRow := int(req.StartRow)
	endRow := int(req.EndRow)

	scanner := bufio.NewScanner(f)
	current := 0
	chunkIdx := 0
	buffer := make([]string, 0, chunkSize)

	for scanner.Scan() {
		line := scanner.Text()
		if current < startRow {
			current++
			continue
		}
		if endRow > 0 && current >= endRow {
			break
		}

		buffer = append(buffer, line)
		current++

		if len(buffer) >= chunkSize {
			data := strings.Join(buffer, "\n")
			chunk := &pb.DatasetChunk{
				ChunkIndex: int32(chunkIdx),
				StartRow:   int64(current - len(buffer)),
				EndRow:     int64(current - 1),
				Data:       []byte(data),
				IsLast:     false,
			}
			if err := stream.Send(chunk); err != nil {
				return fmt.Errorf("failed to send chunk: %w", err)
			}
			buffer = buffer[:0]
			chunkIdx++
		}
	}

	// send remaining buffer
	if len(buffer) > 0 {
		data := strings.Join(buffer, "\n")
		chunk := &pb.DatasetChunk{
			ChunkIndex: int32(chunkIdx),
			StartRow:   int64(current - len(buffer)),
			EndRow:     int64(current - 1),
			Data:       []byte(data),
			IsLast:     true,
		}
		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send final chunk: %w", err)
		}
	} else {
		// if no leftover chunk but we sent at least one earlier, mark last sent as last
		if chunkIdx > 0 {
			// nothing to do; client will have received all chunks
		} else {
			// empty file: send empty final chunk
			chunk := &pb.DatasetChunk{
				ChunkIndex: 0,
				StartRow:   int64(current),
				EndRow:     int64(current),
				Data:       []byte{},
				IsLast:     true,
			}
			if err := stream.Send(chunk); err != nil {
				return fmt.Errorf("failed to send empty chunk: %w", err)
			}
		}
	}

	log.Println("Dataset streaming completed")
	return nil
}

// toString converts interface{} to string safely
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case float64:
		// JSON numbers are float64
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
