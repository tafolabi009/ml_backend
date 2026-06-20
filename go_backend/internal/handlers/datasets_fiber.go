package handlers

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/grpcclient"
	"github.com/tafolabi009/backend/go_backend/pkg/storage"
	validationpb "github.com/tafolabi009/backend/proto/validation"
)

// DatasetHandler holds dependencies for dataset handlers
type DatasetHandler struct {
	S3Client         *storage.S3Client
	GCSClient        *storage.GCSProvider
	Repo             *repository.DatasetRepository
	ValidationClient *grpcclient.ValidationClient
}

// NewDatasetHandler creates a new DatasetHandler with injected dependencies
func NewDatasetHandler(s3Client *storage.S3Client, repo *repository.DatasetRepository, validationClient *grpcclient.ValidationClient) *DatasetHandler {
	return &DatasetHandler{
		S3Client:         s3Client,
		Repo:             repo,
		ValidationClient: validationClient,
	}
}

// NewDatasetHandlerGCS creates a DatasetHandler using GCS storage
func NewDatasetHandlerGCS(gcsClient *storage.GCSProvider, repo *repository.DatasetRepository, validationClient *grpcclient.ValidationClient) *DatasetHandler {
	return &DatasetHandler{
		GCSClient:        gcsClient,
		Repo:             repo,
		ValidationClient: validationClient,
	}
}

func getS3Bucket() string {
	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		return bucket
	}
	return "synthos-datasets-570116615008"
}

// InitiateUploadFiber starts a dataset upload and returns signed URL - Fiber version
func (h *DatasetHandler) InitiateUploadFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	var req models.InitiateUploadRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	// Sanitize the client-supplied filename to its base component so it cannot
	// contain path separators or "../" sequences that would let the upload escape
	// the user's {userID}/ storage namespace.
	req.Filename = filepath.Base(req.Filename)
	if req.Filename == "." || req.Filename == ".." || req.Filename == "/" || req.Filename == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{"code": "INVALID_REQUEST", "message": "Invalid filename"},
		})
	}

	// Generate dataset ID
	datasetID := "ds_" + uuid.New().String()[:8]

	// Generate storage key path
	objectKey := fmt.Sprintf("%s/%s/%s", userID, datasetID, req.Filename)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Generate upload URL (resumable for GCS, presigned for S3)
	var uploadURL string
	var uploadMethod string
	var err error
	if h.GCSClient != nil {
		// Use resumable upload for reliable large file transfers
		uploadURL, err = h.GCSClient.InitiateResumableUpload(ctx, objectKey, req.FileType)
		uploadMethod = "resumable"
		if err != nil {
			log.Printf("Resumable upload failed, falling back to signed URL: %v", err)
			uploadURL, _, err = h.GCSClient.GeneratePresignedUploadURL(ctx, objectKey, req.FileType, 1440)
			uploadMethod = "direct"
		}
	} else if h.S3Client != nil {
		uploadURL, err = h.S3Client.GeneratePresignedURL(ctx, objectKey, "PUT", 24*time.Hour, req.FileType)
		uploadMethod = "direct"
	} else {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "STORAGE_ERROR",
				"message": "No storage client configured",
			},
		})
	}
	if err != nil {
		log.Printf("Failed to generate upload URL: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "URL_GENERATION_ERROR",
				"message": "Failed to generate upload URL",
			},
		})
	}

	// Save dataset metadata to database
	dataset := models.Dataset{
		ID:          datasetID,
		UserID:      userID,
		Filename:    req.Filename,
		FileSize:    req.FileSize,
		FileType:    req.FileType,
		Status:      "uploading",
		S3Path:      objectKey,
		Description: req.Description,
		UploadedAt:  time.Now().UTC(),
	}

	if err := h.Repo.Create(ctx, &dataset); err != nil {
		log.Printf("Failed to create dataset record: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to save dataset metadata",
			},
		})
	}

	response := models.InitiateUploadResponse{
		DatasetID:    datasetID,
		UploadURL:    uploadURL,
		UploadMethod: uploadMethod,
		ChunkSize:    8388608, // 8MB chunks for resumable uploads
		ExpiresIn:    86400,   // 24 hours
	}

	return c.JSON(response)
}

// CompleteUploadFiber marks an upload as complete and triggers processing - Fiber version
func (h *DatasetHandler) CompleteUploadFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	var req models.CompleteUploadRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "INVALID_REQUEST",
				"message": err.Error(),
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify dataset belongs to user
	dataset, err := h.Repo.GetByID(ctx, datasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Dataset not found",
			},
		})
	}

	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this dataset",
			},
		})
	}

	// Update dataset status to "processing"
	dataset.Status = "processing"
	dataset.ProcessedAt = &[]time.Time{time.Now().UTC()}[0]

	if err := h.Repo.Update(ctx, dataset); err != nil {
		log.Printf("Failed to update dataset status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to update dataset status",
			},
		})
	}

	// Trigger ML backend via gRPC and update status when done
	go func() {
		db := database.GetDB()
		updateCtx := context.Background()

		if h.ValidationClient != nil {
			grpcCtx, grpcCancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer grpcCancel()

			cascadeConfig := &validationpb.CascadeConfig{
				NumEpochs:               10,
				BatchSize:               32,
				LearningRate:            0.001,
				EarlyStoppingPatience:   3,
				ValidationSplit:         0.2,
				Tiers:                   []string{"light", "medium", "heavy"},
				EnableSpectralAnalysis:  true,
				EnableFrequencyAnalysis: true,
			}

			_, err := h.ValidationClient.TrainCascade(grpcCtx, datasetID, dataset.S3Path, cascadeConfig)
			if err != nil {
				log.Printf("⚠️ ML processing failed for dataset %s: %v - marking as ready anyway", datasetID, err)
			} else {
				log.Printf("✅ ML processing completed for dataset %s", datasetID)
			}
		} else {
			log.Printf("⚠️ No ML client - marking dataset %s as ready directly", datasetID)
		}

		// Always mark dataset as ready so validations can proceed
		_, err := db.Exec(updateCtx, `UPDATE datasets SET status = 'ready', updated_at = NOW() WHERE id = $1`, datasetID)
		if err != nil {
			log.Printf("❌ Failed to update dataset %s status to ready: %v", datasetID, err)
		} else {
			log.Printf("✅ Dataset %s marked as ready", datasetID)
		}
	}()

	response := models.CompleteUploadResponse{
		DatasetID:           datasetID,
		Status:              "processing",
		EstimatedCompletion: time.Now().Add(30 * time.Minute),
		ProcessingStages: []models.ProcessingStage{
			{Stage: "ingestion", Status: "in_progress", Progress: 0},
			{Stage: "profiling", Status: "pending", Progress: 0},
		},
	}

	return c.JSON(response)
}

// ListDatasetsFiber returns paginated list of datasets - Fiber version
func (h *DatasetHandler) ListDatasetsFiber(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)

	page, _ := strconv.Atoi(c.Query("page", "1"))
	if page < 1 {
		page = 1
	}

	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	status := c.Query("status")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	datasets, totalCount, err := h.Repo.List(ctx, userID, page, pageSize)
	if err != nil {
		log.Printf("Failed to list datasets: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to retrieve datasets",
			},
		})
	}

	// Filter by status if provided
	if status != "" {
		filtered := []models.Dataset{}
		for _, ds := range datasets {
			if ds.Status == status {
				filtered = append(filtered, ds)
			}
		}
		datasets = filtered
	}

	totalPages := (totalCount + pageSize - 1) / pageSize

	response := models.DatasetListResponse{
		Datasets: datasets,
		Pagination: models.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalCount: totalCount,
			TotalPages: totalPages,
		},
	}

	return c.JSON(response)
}

// GetDatasetFiber returns details for a specific dataset - Fiber version
func (h *DatasetHandler) GetDatasetFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataset, err := h.Repo.GetByID(ctx, datasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Dataset not found",
			},
		})
	}

	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this dataset",
			},
		})
	}

	return c.JSON(dataset)
}

// DeleteDatasetFiber removes a dataset - Fiber version
func (h *DatasetHandler) DeleteDatasetFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataset, err := h.Repo.GetByID(ctx, datasetID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "NOT_FOUND",
				"message": "Dataset not found",
			},
		})
	}

	if dataset.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "FORBIDDEN",
				"message": "You do not have access to this dataset",
			},
		})
	}

	// Delete from storage if path exists
	if dataset.S3Path != "" {
		if h.GCSClient != nil {
			if err := h.GCSClient.Delete(ctx, dataset.S3Path); err != nil {
				log.Printf("Failed to delete file from GCS: %v", err)
			}
		} else if h.S3Client != nil {
			if err := h.S3Client.Delete(ctx, dataset.S3Path); err != nil {
				log.Printf("Failed to delete file from S3: %v", err)
			}
		}
	}

	if err := h.Repo.Delete(ctx, datasetID); err != nil {
		log.Printf("Failed to delete dataset from database: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to delete dataset",
			},
		})
	}

	return c.JSON(fiber.Map{
		"dataset_id": datasetID,
		"status":     "deleted",
		"deleted_at": time.Now().UTC().Format(time.RFC3339),
	})
}
