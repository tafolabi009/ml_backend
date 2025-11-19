package handlers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/tafolabi009/backend/go_backend/internal/models"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/storage"
)

// InitiateUploadFiber starts a dataset upload and returns signed URL - Fiber version
func InitiateUploadFiber(c *fiber.Ctx) error {
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

	// Generate dataset ID
	datasetID := "ds_" + uuid.New().String()[:8]

	// Generate S3 key path
	s3Key := fmt.Sprintf("%s/%s/%s", userID, datasetID, req.Filename)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s3Config := storage.S3Config{
		Region:          "us-east-1",
		Bucket:          "synthos-uploads",
		AccessKeyID:     "",
		SecretAccessKey: "",
	}

	s3Client, err := storage.NewS3Client(ctx, s3Config)
	if err != nil {
		log.Printf("Failed to initialize S3 client: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "STORAGE_ERROR",
				"message": "Failed to initialize storage service",
			},
		})
	}

	// Generate presigned URL for upload (valid for 1 hour)
	uploadURL, err := s3Client.GeneratePresignedURL(ctx, s3Key, "PUT", time.Hour)
	if err != nil {
		log.Printf("Failed to generate presigned URL: %v", err)
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
		S3Path:      s3Key,
		Description: req.Description,
		UploadedAt:  time.Now().UTC(),
	}

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	if err := datasetRepo.Create(ctx, &dataset); err != nil {
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
		UploadMethod: "multipart",
		ChunkSize:    10485760,
		ExpiresIn:    3600,
	}

	return c.JSON(response)
}

// CompleteUploadFiber marks an upload as complete and triggers processing - Fiber version
func CompleteUploadFiber(c *fiber.Ctx) error {
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
	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	dataset, err := datasetRepo.GetByID(ctx, datasetID)
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

	if err := datasetRepo.Update(ctx, dataset); err != nil {
		log.Printf("Failed to update dataset status: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fiber.Map{
				"code":    "DATABASE_ERROR",
				"message": "Failed to update dataset status",
			},
		})
	}

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
func ListDatasetsFiber(c *fiber.Ctx) error {
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

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	datasets, totalCount, err := datasetRepo.List(ctx, userID, page, pageSize)
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
func GetDatasetFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	dataset, err := datasetRepo.GetByID(ctx, datasetID)
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
func DeleteDatasetFiber(c *fiber.Ctx) error {
	datasetID := c.Params("id")
	userID := c.Locals("user_id").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	datasetRepo := repository.NewDatasetRepository(database.GetDB())

	dataset, err := datasetRepo.GetByID(ctx, datasetID)
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

	// Delete from S3 if S3 path exists
	if dataset.S3Path != "" {
		s3Config := storage.S3Config{
			Region:          "us-east-1",
			Bucket:          "synthos-uploads",
			AccessKeyID:     "",
			SecretAccessKey: "",
		}

		s3Client, err := storage.NewS3Client(ctx, s3Config)
		if err != nil {
			log.Printf("Failed to initialize S3 client for deletion: %v", err)
		} else {
			if err := s3Client.Delete(ctx, dataset.S3Path); err != nil {
				log.Printf("Failed to delete file from S3: %v", err)
			}
		}
	}

	if err := datasetRepo.Delete(ctx, datasetID); err != nil {
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
