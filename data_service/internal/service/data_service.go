package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	pb "github.com/tafolabi009/backend/proto/data"
)

// DataServiceServer implements the DataService gRPC service
type DataServiceServer struct {
	pb.UnimplementedDataServiceServer
	storagePath string
}

// NewDataServiceServer creates a new data service server
func NewDataServiceServer(storagePath string) *DataServiceServer {
	// Ensure storage directory exists
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	return &DataServiceServer{
		storagePath: storagePath,
	}
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

			response := &pb.UploadDatasetResponse{
				DatasetId:      metadata.DatasetId,
				Status:         "success",
				StoragePath:    metadata.StoragePath,
				BytesUploaded:  bytesReceived,
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
			filePath := filepath.Join(s.storagePath, metadata.UserId, metadata.DatasetId, metadata.Filename)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
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

	// TODO: Fetch from database
	// For now, return mock data
	dataset := &pb.Dataset{
		Id:         req.DatasetId,
		UserId:     req.UserId,
		Filename:   "sample_data.csv",
		FileSize:   52428800,
		FileType:   "csv",
		Status:     "ready",
		UploadedAt: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		ProcessedAt: time.Now().Add(-23 * time.Hour).Format(time.RFC3339),
	}

	return &pb.GetDatasetResponse{
		Dataset: dataset,
	}, nil
}

// ListDatasets returns a paginated list of datasets
func (s *DataServiceServer) ListDatasets(ctx context.Context, req *pb.ListDatasetsRequest) (*pb.ListDatasetsResponse, error) {
	log.Printf("ListDatasets: user_id=%s, page=%d, page_size=%d", req.UserId, req.Page, req.PageSize)

	// TODO: Fetch from database with pagination
	// For now, return mock data
	datasets := []*pb.Dataset{
		{
			Id:         "ds_abc123",
			UserId:     req.UserId,
			Filename:   "training_data.csv",
			FileSize:   104857600,
			FileType:   "csv",
			Status:     "ready",
			UploadedAt: time.Now().Add(-48 * time.Hour).Format(time.RFC3339),
			Profile: &pb.DatasetProfile{
				RowCount:    500000,
				ColumnCount: 25,
			},
		},
	}

	pagination := &pb.Pagination{
		Page:       req.Page,
		PageSize:   req.PageSize,
		TotalCount: 1,
		TotalPages: 1,
	}

	return &pb.ListDatasetsResponse{
		Datasets:   datasets,
		Pagination: pagination,
	}, nil
}

// DeleteDataset removes a dataset
func (s *DataServiceServer) DeleteDataset(ctx context.Context, req *pb.DeleteDatasetRequest) (*pb.DeleteDatasetResponse, error) {
	log.Printf("DeleteDataset: dataset_id=%s, user_id=%s", req.DatasetId, req.UserId)

	// TODO: Verify ownership
	// TODO: Delete from storage
	// TODO: Delete from database

	return &pb.DeleteDatasetResponse{
		Success:   true,
		Message:   fmt.Sprintf("Dataset %s deleted successfully", req.DatasetId),
		DeletedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// ProfileDataset analyzes dataset and returns profiling information
func (s *DataServiceServer) ProfileDataset(ctx context.Context, req *pb.ProfileDatasetRequest) (*pb.ProfileDatasetResponse, error) {
	log.Printf("ProfileDataset: dataset_id=%s, format=%s", req.DatasetId, req.DataFormat)

	// TODO: Implement actual profiling logic
	// For now, return mock profile
	profile := &pb.DatasetProfile{
		RowCount:    500000,
		ColumnCount: 25,
		Columns: []*pb.ColumnInfo{
			{
				Name:        "user_id",
				DataType:    "int64",
				NullCount:   0,
				UniqueCount: 450000,
			},
			{
				Name:        "age",
				DataType:    "int32",
				NullCount:   1200,
				UniqueCount: 80,
				MinValue:    "18",
				MaxValue:    "95",
				MeanValue:   42.5,
				StdDev:      15.3,
			},
		},
		Quality: &pb.DataQuality{
			Completeness:  0.97,
			Uniqueness:    0.90,
			Validity:      0.95,
			DuplicateRows: 2500,
			OutlierCount:  8750,
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

	// TODO: Implement actual streaming from storage
	// For now, send mock chunks
	for i := 0; i < 5; i++ {
		chunk := &pb.DatasetChunk{
			ChunkIndex: int32(i),
			StartRow:   int64(i * 1000),
			EndRow:     int64((i + 1) * 1000),
			Data:       []byte(fmt.Sprintf("mock data chunk %d", i)),
			IsLast:     i == 4,
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}

		log.Printf("Sent chunk %d", i)
	}

	log.Println("Dataset streaming completed")
	return nil
}
