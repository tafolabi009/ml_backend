package models

import (
	"time"
)

// Dataset represents a customer's uploaded dataset
type Dataset struct {
	ID          string    `json:"dataset_id" db:"id"`
	UserID      string    `json:"user_id" db:"user_id"`
	Filename    string    `json:"filename" db:"filename"`
	FileSize    int64     `json:"file_size" db:"file_size"`
	FileType    string    `json:"file_type" db:"file_type"`
	Status      string    `json:"status" db:"status"` // uploaded, processing, processed, failed
	S3Path      string    `json:"-" db:"s3_path"`
	RowCount    *int64    `json:"row_count,omitempty" db:"row_count"`
	ColumnCount *int      `json:"column_count,omitempty" db:"column_count"`
	Description string    `json:"description" db:"description"`
	UploadedAt  time.Time `json:"uploaded_at" db:"uploaded_at"`
	ProcessedAt *time.Time `json:"processed_at,omitempty" db:"processed_at"`
}

// InitiateUploadRequest is the request to start a dataset upload
type InitiateUploadRequest struct {
	Filename    string `json:"filename" binding:"required"`
	FileSize    int64  `json:"file_size" binding:"required"`
	FileType    string `json:"file_type" binding:"required"`
	Description string `json:"description"`
}

// InitiateUploadResponse contains the signed upload URL
type InitiateUploadResponse struct {
	DatasetID    string `json:"dataset_id"`
	UploadURL    string `json:"upload_url"`
	UploadMethod string `json:"upload_method"`
	ChunkSize    int    `json:"chunk_size"`
	ExpiresIn    int    `json:"expires_in"`
}

// CompleteUploadRequest marks upload as complete
type CompleteUploadRequest struct {
	ETag string `json:"etag" binding:"required"`
}

// CompleteUploadResponse confirms processing has started
type CompleteUploadResponse struct {
	DatasetID            string                `json:"dataset_id"`
	Status               string                `json:"status"`
	EstimatedCompletion  time.Time             `json:"estimated_completion"`
	ProcessingStages     []ProcessingStage     `json:"processing_stages"`
}

// ProcessingStage represents a stage in dataset processing
type ProcessingStage struct {
	Stage    string  `json:"stage"`
	Status   string  `json:"status"`
	Progress float64 `json:"progress"`
}

// DatasetListResponse is the paginated list of datasets
type DatasetListResponse struct {
	Datasets   []Dataset  `json:"datasets"`
	Pagination Pagination `json:"pagination"`
}

// Pagination contains pagination metadata
type Pagination struct {
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalCount int `json:"total_count"`
	TotalPages int `json:"total_pages"`
}
