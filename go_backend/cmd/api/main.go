package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tafolabi009/backend/go_backend/internal/handlers"
	"github.com/tafolabi009/backend/go_backend/internal/middleware"
	"github.com/tafolabi009/backend/go_backend/pkg/config"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Set Gin mode
	if cfg.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	// Initialize router
	router := gin.New()

	// Global middleware
	router.Use(gin.Recovery())
	router.Use(middleware.Logger())
	router.Use(middleware.CORS())
	router.Use(middleware.ErrorHandler())

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": "synthos-api-gateway",
			"version": "1.0.0",
		})
	})

	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// Auth routes (public)
		auth := v1.Group("/auth")
		{
			auth.POST("/register", handlers.Register)
			auth.POST("/login", handlers.Login)
			auth.POST("/refresh", handlers.RefreshToken)
		}

		// Protected routes (require authentication)
		protected := v1.Group("")
		protected.Use(middleware.AuthRequired())
		{
			// Dataset management
			datasets := protected.Group("/datasets")
			{
				datasets.POST("/upload", handlers.InitiateUpload)
				datasets.POST("/:id/complete", handlers.CompleteUpload)
				datasets.GET("", handlers.ListDatasets)
				datasets.GET("/:id", handlers.GetDataset)
				datasets.DELETE("/:id", handlers.DeleteDataset)
			}

			// Validation jobs
			validations := protected.Group("/validations")
			{
				validations.POST("/create", handlers.CreateValidation)
				validations.GET("", handlers.ListValidations)
				validations.GET("/:id", handlers.GetValidation)
				validations.GET("/:id/report", handlers.GetValidationReport)
				validations.GET("/:id/certificate", handlers.GetValidationCertificate)
				validations.GET("/:id/collapse-details", handlers.GetCollapseDetails)
				validations.GET("/:id/recommendations", handlers.GetRecommendations)
			}

			// Warranty management
			warranties := protected.Group("/warranties")
			{
				warranties.POST("/:validation_id/request", handlers.RequestWarranty)
				warranties.GET("", handlers.ListWarranties)
				warranties.GET("/:id", handlers.GetWarranty)
				warranties.POST("/:id/claim", handlers.FileWarrantyClaim)
			}

			// Analytics
			analytics := protected.Group("/analytics")
			{
				analytics.GET("/usage", handlers.GetUsageAnalytics)
				analytics.GET("/validation-history", handlers.GetValidationHistory)
			}
		}
	}

	// Create server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Starting Synthos API Gateway on port %d", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Graceful shutdown with 5 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exited")
}
