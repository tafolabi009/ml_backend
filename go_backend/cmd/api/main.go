package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/handlers"
	"github.com/tafolabi009/backend/go_backend/internal/middleware"
	"github.com/tafolabi009/backend/go_backend/pkg/config"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/grpcclient"
	"github.com/tafolabi009/backend/go_backend/pkg/monitoring"
	"github.com/tafolabi009/backend/go_backend/pkg/tracing"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize JWT
	auth.InitJWT(cfg.JWTSecret)

	// Initialize database
	if err := database.Init(cfg.DatabaseURL); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	// Initialize gRPC clients for ML services
	grpcClients, err := grpcclient.NewClients(
		cfg.ValidationServiceAddr,
		cfg.CollapseServiceAddr,
		cfg.DataServiceAddr,
	)
	if err != nil {
		log.Printf("Warning: Failed to initialize gRPC clients: %v (some features may be unavailable)", err)
	} else {
		defer grpcClients.Close()
		handlers.SetGRPCClients(grpcClients)
		log.Println("gRPC clients initialized successfully")
	}

	// Initialize Jaeger tracing (optional)
	if os.Getenv("ENABLE_TRACING") == "true" {
		jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
		if jaegerEndpoint == "" {
			jaegerEndpoint = "localhost:6831"
		}

		tracer, closer, err := tracing.InitJaeger("synthos-api-gateway", jaegerEndpoint)
		if err != nil {
			log.Printf("Warning: Failed to initialize Jaeger tracer: %v", err)
		} else {
			defer closer.Close()
			log.Printf("Jaeger tracer initialized: %s", jaegerEndpoint)
			_ = tracer // Use tracer to avoid unused warning
		}
	}

	// Create Fiber app
	app := fiber.New(fiber.Config{
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		BodyLimit:    100 * 1024 * 1024, // 100MB
		ErrorHandler: customErrorHandler,
		AppName:      "Synthos API Gateway v1.0.0",
	})

	// Global middleware
	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format:     "${time} ${status} - ${method} ${path} ${latency}\n",
		TimeFormat: "2006-01-02 15:04:05",
		TimeZone:   "UTC",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Content-Type,Authorization",
	}))

	// Prometheus metrics middleware (if enabled)
	if os.Getenv("ENABLE_METRICS") == "true" {
		app.Use(monitoring.PrometheusMiddleware())
		log.Println("Prometheus metrics enabled at /metrics")
	}

	// Jaeger tracing middleware (if enabled)
	if os.Getenv("ENABLE_TRACING") == "true" {
		app.Use(tracing.TracingMiddleware())
		log.Println("Jaeger distributed tracing enabled")
	}

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "healthy",
			"service": "synthos-api-gateway",
			"version": "1.0.0",
			"time":    time.Now().Unix(),
		})
	})

	// Prometheus metrics endpoint (if enabled)
	if os.Getenv("ENABLE_METRICS") == "true" {
		app.Get("/metrics", monitoring.MetricsHandler())
	}

	// API v1 routes
	v1 := app.Group("/api/v1")
	{
		// Auth routes (public)
		auth := v1.Group("/auth")
		{
			auth.Post("/register", handlers.RegisterFiber)
			auth.Post("/login", handlers.LoginFiber)
			auth.Post("/refresh", handlers.RefreshTokenFiber)
		}

		// Protected routes (require authentication)
		protected := v1.Group("", middleware.AuthRequiredFiber())
		{
			// Dataset management
			datasets := protected.Group("/datasets")
			{
				datasets.Post("/upload", handlers.InitiateUploadFiber)
				datasets.Post("/:id/complete", handlers.CompleteUploadFiber)
				datasets.Get("", handlers.ListDatasetsFiber)
				datasets.Get("/:id", handlers.GetDatasetFiber)
				datasets.Delete("/:id", handlers.DeleteDatasetFiber)
			}

			// Validation jobs
			validations := protected.Group("/validations")
			{
				validations.Post("/create", handlers.CreateValidationFiber)
				validations.Get("", handlers.ListValidationsFiber)
				validations.Get("/:id", handlers.GetValidationFiber)
				validations.Get("/:id/report", handlers.GetValidationReportFiber)
				validations.Get("/:id/certificate", handlers.GetValidationCertificateFiber)
				validations.Get("/:id/collapse-details", handlers.GetCollapseDetailsFiber)
				validations.Get("/:id/recommendations", handlers.GetRecommendationsFiber)
			}

			// Warranty management
			warranties := protected.Group("/warranties")
			{
				warranties.Post("/:validation_id/request", handlers.RequestWarrantyFiber)
				warranties.Get("", handlers.ListWarrantiesFiber)
				warranties.Get("/:id", handlers.GetWarrantyFiber)
				warranties.Post("/:id/claim", handlers.FileWarrantyClaimFiber)
			}

			// Analytics
			analytics := protected.Group("/analytics")
			{
				analytics.Get("/usage", handlers.GetUsageAnalyticsFiber)
				analytics.Get("/validation-history", handlers.GetValidationHistoryFiber)
			}
		}
	}

	// Start server
	port := cfg.Port
	if port == 0 {
		port = 8000
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Fatal("Server forced to shutdown:", err)
		}

		log.Println("Server exited")
	}()

	log.Printf("Starting Synthos API Gateway on port %d", port)
	if err := app.Listen(fmt.Sprintf(":%d", port)); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

// customErrorHandler handles errors in a consistent format
func customErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError
	errorCode := "INTERNAL_SERVER_ERROR"
	message := "An unexpected error occurred"

	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
		message = e.Message

		switch code {
		case fiber.StatusBadRequest:
			errorCode = "BAD_REQUEST"
		case fiber.StatusUnauthorized:
			errorCode = "UNAUTHORIZED"
		case fiber.StatusForbidden:
			errorCode = "FORBIDDEN"
		case fiber.StatusNotFound:
			errorCode = "NOT_FOUND"
		case fiber.StatusConflict:
			errorCode = "CONFLICT"
		}
	}

	return c.Status(code).JSON(fiber.Map{
		"error": fiber.Map{
			"code":    errorCode,
			"message": message,
		},
	})
}
