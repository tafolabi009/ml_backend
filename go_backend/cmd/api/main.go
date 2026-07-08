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
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"

	"github.com/tafolabi009/backend/go_backend/internal/auth"
	"github.com/tafolabi009/backend/go_backend/internal/handlers"
	"github.com/tafolabi009/backend/go_backend/internal/middleware"
	"github.com/tafolabi009/backend/go_backend/internal/repository"
	configpkg "github.com/tafolabi009/backend/go_backend/pkg/config"
	"github.com/tafolabi009/backend/go_backend/pkg/database"
	"github.com/tafolabi009/backend/go_backend/pkg/email"
	"github.com/tafolabi009/backend/go_backend/pkg/grpcclient"
	"github.com/tafolabi009/backend/go_backend/pkg/monitoring"
	"github.com/tafolabi009/backend/go_backend/pkg/storage"
	"github.com/tafolabi009/backend/go_backend/pkg/tracing"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load configuration
	cfg, err := configpkg.LoadProduction(ctx)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	defer cfg.Close()

	log.Printf("Starting Synthos API Gateway in %s mode", cfg.Environment)

	// Initialize JWT with secret from config
	auth.InitJWT(cfg.JWTSecret)

	// Initialize database
	if err := database.Init(cfg.DatabaseURL); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	// Initialize email client
	email.Init()

	// Initialize gRPC clients for ML services
	grpcCfg := grpcclient.DefaultProductionConfig()
	grpcCfg.ValidationAddr = cfg.ValidationServiceAddr
	grpcCfg.CollapseAddr = cfg.CollapseServiceAddr

	grpcClients, err := grpcclient.NewProductionClients(ctx, grpcCfg)
	if err != nil {
		log.Printf("⚠️ Failed to initialize gRPC clients (ML services may be unavailable): %v", err)
	} else {
		defer grpcClients.Close()
	}

	// Initialize storage client based on cloud provider
	var s3Client *storage.S3Client
	var gcsClient *storage.GCSProvider

	if cfg.CloudProvider == "gcp" {
		gcsBucket := cfg.GCSBucket
		if gcsBucket == "" {
			gcsBucket = getEnvOrDefault("GCS_BUCKET", fmt.Sprintf("synthos-datasets-%s", cfg.GCPProjectID))
		}
		gcsClient, err = storage.NewGCSProvider(ctx, gcsBucket, "")
		if err != nil {
			log.Fatalf("Failed to initialize GCS client: %v", err)
		}
		defer gcsClient.Close()
		log.Printf("✅ GCS client initialized for bucket: %s", gcsBucket)
	} else {
		s3Config := storage.S3Config{
			Region:          getEnvOrDefault("AWS_REGION", "us-east-1"),
			Bucket:          getEnvOrDefault("S3_BUCKET", "synthos-datasets-570116615008"),
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			UsePathStyle:    os.Getenv("S3_USE_PATH_STYLE") == "true",
		}
		s3Client, err = storage.NewS3Client(ctx, s3Config)
		if err != nil {
			log.Fatalf("Failed to initialize S3 client: %v", err)
		}
		log.Printf("✅ S3 client initialized for bucket: %s", s3Config.Bucket)
	}

	// Initialize ValidationClient for ML backend communication (optional - may not be available)
	var validationClient *grpcclient.ValidationClient
	validationAddr := getEnvOrDefault("VALIDATION_SERVICE_ADDR", cfg.ValidationServiceAddr)
	if validationAddr != "" {
		validationClient, err = grpcclient.NewValidationClient(validationAddr)
		if err != nil {
			log.Printf("⚠️ Failed to initialize ValidationClient (ML backend may be unavailable): %v", err)
		} else {
			defer validationClient.Close()
			log.Printf("✅ ValidationClient connected to: %s", validationAddr)
		}
	}

	// Set package-level validation client for standalone handler functions
	handlers.SetValidationClient(validationClient)

	// Shared storage client for sidecar reads (privacy reports)
	if s3Client != nil {
		handlers.SetStorageClient(s3Client)
	}

	// Reports bucket client for generated PDF artifacts (report/certificate)
	if cfg.CloudProvider != "gcp" {
		reportsCfg := storage.S3Config{
			Region:          getEnvOrDefault("AWS_REGION", "us-east-1"),
			Bucket:          getEnvOrDefault("REPORTS_BUCKET", "synthos-reports-us-east-1"),
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			UsePathStyle:    os.Getenv("S3_USE_PATH_STYLE") == "true",
		}
		if reportsClient, rerr := storage.NewS3Client(ctx, reportsCfg); rerr == nil {
			handlers.SetReportsStorage(reportsClient)
			log.Printf("✅ Reports storage initialized for bucket: %s", reportsCfg.Bucket)
		} else {
			log.Printf("⚠️ Reports storage unavailable (artifact links disabled): %v", rerr)
		}
	}

	// Background scheduler: drift monitors, failed-validation refunds,
	// idempotency-key purge. Multi-instance safe; disable with MONITOR_SCHEDULER=off.
	handlers.StartMonitorScheduler(ctx)

	// Initialize DatasetHandler with dependencies (Dependency Injection)
	datasetRepo := repository.NewDatasetRepository(database.GetDB())
	var datasetHandler *handlers.DatasetHandler
	if cfg.CloudProvider == "gcp" {
		datasetHandler = handlers.NewDatasetHandlerGCS(gcsClient, datasetRepo, validationClient)
	} else {
		datasetHandler = handlers.NewDatasetHandler(s3Client, datasetRepo, validationClient)
	}

	// Initialize Jaeger tracing (if enabled)
	if cfg.EnableTracing {
		tracer, closer, err := tracing.InitJaeger("synthos-api-gateway", cfg.JaegerEndpoint)
		if err != nil {
			log.Printf("Warning: Failed to initialize Jaeger tracer: %v", err)
		} else {
			defer closer.Close()
			log.Printf("✅ Jaeger tracer initialized: %s", cfg.JaegerEndpoint)
			_ = tracer
		}
	}

	// Create Fiber app with production settings
	app := fiber.New(fiber.Config{
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           120 * time.Second,
		BodyLimit:             100 * 1024 * 1024, // 100MB
		ErrorHandler:          productionErrorHandler,
		AppName:               "Synthos API Gateway v1.0.0",
		DisableStartupMessage: cfg.Environment == "production",
		EnablePrintRoutes:     cfg.Environment != "production",
		Prefork:               false, // Disable prefork for Kubernetes/ECS compatibility
	})

	// Request ID middleware
	app.Use(requestid.New())

	// Recover from panics
	app.Use(recover.New(recover.Config{
		EnableStackTrace: cfg.Environment != "production",
	}))

	// Compression
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
	}))

	// Logger
	app.Use(logger.New(logger.Config{
		Format:     "${time} ${status} - ${method} ${path} ${latency}\n",
		TimeFormat: "2006-01-02 15:04:05",
		TimeZone:   "UTC",
	}))

	// CORS configuration - allow all frontend headers
	corsOrigins := joinOrigins(cfg.AllowedOrigins)
	// Ensure we never use wildcard with credentials
	if corsOrigins == "" || corsOrigins == "*" {
		corsOrigins = "https://synthos.dev,https://www.synthos.dev,https://app.synthos.dev"
	}
	log.Printf("CORS Origins configured: %s", corsOrigins)

	app.Use(cors.New(cors.Config{
		AllowOrigins:     corsOrigins,
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS,PATCH",
		AllowHeaders:     "Content-Type,Authorization,X-Request-ID,X-Trace-ID,X-Requested-With,Accept,Origin,Cache-Control",
		AllowCredentials: true,
		ExposeHeaders:    "X-Request-ID,X-Trace-ID,Content-Length",
		MaxAge:           86400,
	}))

	// Rate limiting
	app.Use(limiter.New(limiter.Config{
		Max:               cfg.RateLimitRPS,
		Expiration:        time.Second,
		LimiterMiddleware: limiter.SlidingWindow{},
		KeyGenerator: func(c *fiber.Ctx) string {
			userID := c.Locals("user_id")
			if userID != nil {
				return fmt.Sprintf("%s:%s", c.IP(), userID)
			}
			return c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "RATE_LIMIT_EXCEEDED",
					"message": "Too many requests, please try again later",
				},
			})
		},
	}))

	// Prometheus metrics middleware
	if cfg.EnableMetrics {
		app.Use(monitoring.PrometheusMiddleware())
		log.Println("✅ Prometheus metrics enabled at /metrics")
	}

	// Jaeger tracing middleware
	if cfg.EnableTracing {
		app.Use(tracing.TracingMiddleware())
		log.Println("✅ Jaeger distributed tracing enabled")
	}

	// Health check endpoints
	app.Get("/health", healthHandler(grpcClients))
	app.Get("/health/live", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "alive"})
	})
	app.Get("/health/ready", readinessHandler(grpcClients))
	app.Get("/health/uptime", handlers.GetUptimeFiber)

	// Paddle billing webhook (public; HMAC signature-verified in the handler)
	app.Post("/webhooks/paddle", handlers.PaddleWebhookFiber)

	// Prometheus metrics endpoint
	if cfg.EnableMetrics {
		app.Get("/metrics", monitoring.MetricsHandler())
	}

	// API v1 routes
	v1 := app.Group("/api/v1")
	{
		// Auth routes (public)
		authRoutes := v1.Group("/auth")
		{
			authRoutes.Post("/register", handlers.RegisterFiber)
			authRoutes.Post("/login", handlers.LoginFiber)
			authRoutes.Post("/logout", middleware.AuthRequiredFiber(), handlers.LogoutFiber)
			authRoutes.Post("/refresh", handlers.RefreshTokenFiber)
			authRoutes.Post("/forgot-password", handlers.ForgotPasswordFiber)
			authRoutes.Post("/reset-password", handlers.ResetPasswordFiber)
			authRoutes.Post("/verify-email", handlers.VerifyEmailFiber)
			authRoutes.Post("/resend-otp", handlers.ResendOTPFiber)
		}

		// Promo code validation (public - for signup flow)
		v1.Get("/promo/validate", handlers.ValidatePromoCodeFiber)

		// Shared report links (public — the token IS the credential)
		v1.Get("/shared/reports/:token", handlers.GetSharedReportFiber)

		// Certificate verification (public — third parties verify authenticity)
		v1.Get("/certificates/public-key", handlers.GetCertificatePublicKeyFiber)
		v1.Post("/certificates/verify", handlers.VerifyCertificateFiber)
		v1.Get("/certificates/:id/verify", handlers.VerifyCertificateByIDFiber)

		// Bootstrap: promote authenticated user to admin (only works if no admins exist)
		v1.Post("/bootstrap/admin", middleware.AuthRequiredFiber(), func(c *fiber.Ctx) error {
			userID := c.Locals("user_id").(string)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db := database.GetDB()
			var adminCount int
			db.QueryRow(ctx, `SELECT COUNT(*) FROM users WHERE role = 'admin'`).Scan(&adminCount)
			if adminCount > 0 {
				return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
					"error": fiber.Map{"code": "FORBIDDEN", "message": "Admin already exists. Use admin panel to manage roles."},
				})
			}
			_, err := db.Exec(ctx, `UPDATE users SET role = 'admin' WHERE id = $1`, userID)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fiber.Map{"code": "UPDATE_FAILED", "message": "Failed to bootstrap admin"},
				})
			}
			log.Printf("🔑 Bootstrap: User %s promoted to admin", userID)
			return c.JSON(fiber.Map{"message": "You are now an admin. Please re-login to get updated permissions.", "role": "admin"})
		})

		// Protected auth routes
		authProtected := v1.Group("/auth", middleware.AuthRequiredFiber())
		{
			authProtected.Get("/me", handlers.GetMeFiber)
			authProtected.Put("/me", handlers.UpdateProfileFiber)
			authProtected.Patch("/me", handlers.UpdateProfileFiber)
			authProtected.Post("/change-password", handlers.ChangePasswordFiber)

			// Notification preferences
			authProtected.Get("/notification-preferences", handlers.GetNotificationPreferencesFiber)
			authProtected.Put("/notification-preferences", handlers.UpdateNotificationPreferencesFiber)

			// 2FA routes
			authProtected.Post("/2fa/setup", handlers.TwoFactorSetupFiber)
			authProtected.Post("/2fa/verify", handlers.TwoFactorVerifyFiber)
			authProtected.Post("/2fa/disable", handlers.TwoFactorDisableFiber)
		}

		// 2FA contract endpoints (status/enroll/activate)
		authProtected.Get("/2fa/status", handlers.TwoFactorStatusFiber)
		authProtected.Post("/2fa/enroll", handlers.TwoFactorEnrollFiber)
		authProtected.Post("/2fa/activate", handlers.TwoFactorActivateFiber)

		// Session management
		authProtected.Get("/sessions", handlers.ListSessionsFiber)
		authProtected.Delete("/sessions/:id", handlers.RevokeSessionFiber)

		// API Keys (protected)
		apiKeys := v1.Group("/api-keys", middleware.AuthRequiredFiber())
		{
			apiKeys.Post("", handlers.CreateAPIKeyFiber)
			apiKeys.Get("", handlers.ListAPIKeysFiber)
			apiKeys.Delete("/:id", handlers.DeleteAPIKeyFiber)
		}

		// Notifications (protected)
		notifications := v1.Group("/notifications", middleware.AuthRequiredFiber())
		{
			notifications.Get("", handlers.GetNotificationsFiber)
			notifications.Post("/read", handlers.MarkNotificationsReadFiber)
			notifications.Delete("/:id", handlers.DeleteNotificationFiber)
		}

		// Protected routes (require authentication)
		protected := v1.Group("", middleware.AuthRequiredFiber())
		{
			// Dataset management - using injected handler
			datasets := protected.Group("/datasets")
			{
				datasets.Post("/upload", middleware.RequireScopes("write:datasets"), datasetHandler.InitiateUploadFiber)
				datasets.Post("/:id/complete", middleware.RequireScopes("write:datasets"), datasetHandler.CompleteUploadFiber)
				datasets.Get("", middleware.RequireScopes("read:datasets"), datasetHandler.ListDatasetsFiber)
				datasets.Get("/:id", middleware.RequireScopes("read:datasets"), datasetHandler.GetDatasetFiber)
				datasets.Get("/:id/history", middleware.RequireScopes("read:datasets"), handlers.GetDatasetHistoryFiber)
				datasets.Get("/:id/preview", middleware.RequireScopes("read:datasets"), handlers.GetDatasetPreviewFiber)
				datasets.Get("/:id/stats", middleware.RequireScopes("read:datasets"), handlers.GetDatasetStatsFiber)
				datasets.Post("/:id/schedule", middleware.RequireScopes("write:validations"), handlers.UpsertDatasetScheduleFiber)
				datasets.Get("/:id/schedule", middleware.RequireScopes("read:validations"), handlers.GetDatasetScheduleFiber)
				datasets.Delete("/:id/schedule", middleware.RequireScopes("write:validations"), handlers.DeleteDatasetScheduleFiber)
				datasets.Delete("/:id", middleware.RequireScopes("write:datasets"), datasetHandler.DeleteDatasetFiber)
			}

			// Validation jobs
			validations := protected.Group("/validations")
			{
				validations.Post("/create", middleware.RequireScopes("write:validations"), handlers.CreateValidationFiber)
				validations.Get("", middleware.RequireScopes("read:validations"), handlers.ListValidationsFiber)
				validations.Get("/compare", middleware.RequireScopes("read:validations"), handlers.CompareValidationsFiber)
				validations.Get("/:id", middleware.RequireScopes("read:validations"), handlers.GetValidationFiber)
				validations.Patch("/:id", middleware.RequireScopes("write:validations"), handlers.RenameValidationFiber)
				validations.Get("/:id/progress", middleware.RequireScopes("read:validations"), handlers.GetValidationProgressFiber)
				validations.Get("/:id/findings", middleware.RequireScopes("read:validations"), handlers.GetValidationFindingsFiber)
				validations.Get("/:id/report", middleware.RequireScopes("read:validations"), handlers.GetValidationReportFiber)
				validations.Get("/:id/certificate", middleware.RequireScopes("read:validations"), handlers.GetValidationCertificateFiber)
				validations.Get("/:id/certificate.json", middleware.RequireScopes("read:validations"), handlers.GetSignedCertificateFiber)
				validations.Get("/:id/collapse-details", middleware.RequireScopes("read:validations"), handlers.GetCollapseDetailsFiber)
				validations.Get("/:id/recommendations", middleware.RequireScopes("read:validations"), handlers.GetRecommendationsFiber)
				validations.Get("/:id/datasheet", middleware.RequireScopes("read:validations"), handlers.GetValidationDatasheetFiber)
				validations.Get("/:id/privacy", middleware.RequireScopes("read:validations"), handlers.GetValidationPrivacyFiber)
				validations.Post("/:id/outcome", middleware.RequireScopes("write:validations"), handlers.RecordValidationOutcomeFiber)
				validations.Get("/:id/outcome", middleware.RequireScopes("read:validations"), handlers.GetValidationOutcomeFiber)
				validations.Post("/:id/share", middleware.RequireScopes("write:validations"), handlers.CreateReportShareFiber)
				validations.Get("/:id/shares", middleware.RequireScopes("read:validations"), handlers.ListReportSharesFiber)
				validations.Delete("/:id/share/:token", middleware.RequireScopes("write:validations"), handlers.RevokeReportShareFiber)
				validations.Post("/:id/cancel", middleware.RequireScopes("write:validations"), handlers.CancelValidationFiber)
			}

			// Continuous drift monitors
			monitors := protected.Group("/monitors")
			{
				monitors.Post("", middleware.RequireScopes("write:validations"), handlers.CreateMonitorFiber)
				monitors.Get("", middleware.RequireScopes("read:validations"), handlers.ListMonitorsFiber)
				monitors.Get("/:id", middleware.RequireScopes("read:validations"), handlers.GetMonitorFiber)
				monitors.Patch("/:id", middleware.RequireScopes("write:validations"), handlers.UpdateMonitorFiber)
				monitors.Delete("/:id", middleware.RequireScopes("write:validations"), handlers.DeleteMonitorFiber)
			}

			// Effective quota + usage for the caller
			protected.Get("/quota", handlers.GetMyQuotaFiber)

			// Dataset groups
			datasetGroups := protected.Group("/dataset-groups")
			{
				datasetGroups.Get("", middleware.RequireScopes("read:datasets"), handlers.ListDatasetGroupsFiber)
				datasetGroups.Delete("/:id", middleware.RequireScopes("write:datasets"), handlers.DeleteDatasetGroupFiber)
			}

			// Warranty management
			warranties := protected.Group("/warranties")
			{
				warranties.Post("/:validation_id/request", middleware.RequireScopes("write:warranties"), handlers.RequestWarrantyFiber)
				warranties.Get("", middleware.RequireScopes("read:warranties"), handlers.ListWarrantiesFiber)
				warranties.Get("/:id", middleware.RequireScopes("read:warranties"), handlers.GetWarrantyFiber)
				warranties.Post("/:id/claim", middleware.RequireScopes("write:warranties"), handlers.FileWarrantyClaimFiber)
			}

			// Credits
			credits := protected.Group("/credits")
			{
				credits.Get("/balance", handlers.GetCreditBalanceFiber)
				credits.Get("/packages", handlers.GetCreditPackagesFiber)
				credits.Post("/purchase", handlers.PurchaseCreditsFiber)
				credits.Get("/history", handlers.GetCreditHistoryFiber)
				credits.Get("/usage-series", handlers.GetCreditUsageSeriesFiber)
				credits.Post("/redeem", handlers.RedeemPromoCodeFiber)
			}

			// Analytics
			analytics := protected.Group("/analytics")
			{
				analytics.Get("/usage", middleware.RequireScopes("read:analytics"), handlers.GetUsageAnalyticsFiber)
				analytics.Get("/validation-history", middleware.RequireScopes("read:analytics"), handlers.GetValidationHistoryFiber)
				analytics.Get("/benchmarks", middleware.RequireScopes("read:analytics"), handlers.GetBenchmarksFiber)
				analytics.Get("/quality-trends", middleware.RequireScopes("read:analytics"), handlers.GetQualityTrendsFiber)
				analytics.Get("/calibration", middleware.RequireScopes("read:analytics"), handlers.GetCalibrationFiber)
			}

			// Webhook management (any authenticated user)
			webhookRoutes := protected.Group("/webhooks")
			{
				webhookRoutes.Post("", handlers.CreateWebhookFiber)
				webhookRoutes.Get("", handlers.ListWebhooksFiber)
				webhookRoutes.Get("/:id", handlers.GetWebhookFiber)
				webhookRoutes.Patch("/:id", handlers.UpdateWebhookFiber)
				webhookRoutes.Delete("/:id", handlers.DeleteWebhookFiber)
				webhookRoutes.Get("/:id/deliveries", handlers.ListWebhookDeliveriesFiber)
			}

			// Customer ticket routes (any authenticated user)
			ticketRoutes := protected.Group("/tickets")
			{
				ticketRoutes.Post("", handlers.CreateTicketFiber)
				ticketRoutes.Get("", handlers.ListMyTicketsFiber)
				ticketRoutes.Get("/:id", handlers.GetMyTicketFiber)
				ticketRoutes.Post("/:id/reply", handlers.ReplyToMyTicketFiber)
			}
		}

		// Admin routes (admin role required)
		adminRoutes := v1.Group("/admin", middleware.AuthRequiredFiber(), middleware.RequireRole("admin"))
		{
			adminRoutes.Get("/overview", handlers.GetSystemOverviewFiber)
			adminRoutes.Get("/users", handlers.ListUsersFiber)
			adminRoutes.Get("/users/:id", handlers.GetUserDetailFiber)
			adminRoutes.Patch("/users/:id/role", handlers.UpdateUserRoleFiber)
			adminRoutes.Patch("/users/:id/status", handlers.UpdateUserStatusFiber)
			adminRoutes.Post("/promo-codes", handlers.CreatePromoCodeFiber)
			adminRoutes.Get("/promo-codes", handlers.ListPromoCodesFiber)
			adminRoutes.Patch("/promo-codes/:id", handlers.UpdatePromoCodeFiber)
			adminRoutes.Post("/invites", handlers.CreateInviteFiber)
			adminRoutes.Get("/invites", handlers.ListInvitesFiber)
			adminRoutes.Delete("/invites/:id", handlers.DeleteInviteFiber)
			adminRoutes.Get("/validations", handlers.ListAllValidationsFiber)
			adminRoutes.Get("/datasets", handlers.ListAllDatasetsFiber)
			adminRoutes.Delete("/users/:id", handlers.DeleteUserFiber)
			adminRoutes.Get("/warranties", handlers.ListAllWarrantiesFiber)
			adminRoutes.Patch("/warranties/:id/approve", handlers.ApproveWarrantyFiber)
			adminRoutes.Patch("/warranties/:id/reject", handlers.RejectWarrantyFiber)
			adminRoutes.Get("/audit-log", handlers.GetAuditLogFiber)
			adminRoutes.Get("/settings", handlers.GetPlatformSettingsFiber)
			adminRoutes.Patch("/settings", handlers.UpdatePlatformSettingsFiber)
			adminRoutes.Get("/calibration", handlers.GetCalibrationAdminFiber)
			adminRoutes.Get("/analytics/growth", handlers.GetAdminGrowthFiber)
			adminRoutes.Get("/metrics", handlers.GetAdminMetricsFiber)
			adminRoutes.Post("/impersonate", handlers.ImpersonateUserFiber)
			adminRoutes.Get("/users/:id/quotas", handlers.GetUserQuotaAdminFiber)
			adminRoutes.Patch("/users/:id/quotas", handlers.UpdateUserQuotaAdminFiber)
		}

		// Support routes (support role required - admin/developer also pass via hierarchy)
		supportRoutes := v1.Group("/support", middleware.AuthRequiredFiber(), middleware.RequireRole("support"))
		{
			supportRoutes.Get("/overview", handlers.GetSupportOverviewFiber)
			supportRoutes.Get("/tickets", handlers.ListTicketsFiber)
			supportRoutes.Get("/tickets/:id", handlers.GetTicketFiber)
			supportRoutes.Post("/tickets/:id/reply", handlers.ReplyToTicketFiber)
			supportRoutes.Patch("/tickets/:id/status", handlers.UpdateTicketStatusFiber)
			supportRoutes.Patch("/tickets/:id/assign", handlers.AssignTicketFiber)
			supportRoutes.Patch("/tickets/:id/priority", handlers.UpdateTicketPriorityFiber)
			supportRoutes.Patch("/warranties/:id/claim/:claim_id/process", handlers.ProcessWarrantyClaimFiber)
			supportRoutes.Get("/users/:id", handlers.GetUserForSupportFiber)
			supportRoutes.Get("/users/:id/validations", handlers.GetUserValidationsFiber)
		}

		// Developer routes (developer role required - admin also passes)
		devRoutes := v1.Group("/developer", middleware.AuthRequiredFiber(), middleware.RequireRole("developer"))
		{
			devRoutes.Get("/overview", handlers.GetDevOverviewFiber)
			devRoutes.Get("/services", handlers.GetServicesStatusFiber)
			devRoutes.Get("/api-docs", handlers.GetAPIDocsFiber)
			devRoutes.Get("/logs", handlers.GetRecentLogsFiber)
			devRoutes.Get("/metrics", handlers.GetMetricsFiber)
		}
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := app.ShutdownWithContext(shutdownCtx); err != nil {
			log.Fatal("Server forced to shutdown:", err)
		}

		log.Println("Server exited")
	}()

	// Start server
	addr := fmt.Sprintf(":%d", cfg.Port)
	log.Printf("🚀 Starting Synthos API Gateway on %s", addr)

	if cfg.TLSEnabled && cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		if err := app.ListenTLS(addr, cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
			log.Fatal("Server failed to start:", err)
		}
	} else {
		if err := app.Listen(addr); err != nil {
			log.Fatal("Server failed to start:", err)
		}
	}
}

// healthHandler returns comprehensive health status
func healthHandler(grpcClients *grpcclient.ProductionClients) fiber.Handler {
	return func(c *fiber.Ctx) error {
		health := fiber.Map{
			"status":  "healthy",
			"service": "synthos-api-gateway",
			"version": "1.0.0",
			"time":    time.Now().Unix(),
		}

		// Check gRPC services health
		if grpcClients != nil {
			health["services"] = fiber.Map{
				"validation": grpcClients.GetValidationHealth(),
				"collapse":   grpcClients.GetCollapseHealth(),
			}
		}

		// Check database health
		if database.IsHealthy() {
			health["database"] = "healthy"
		} else {
			health["database"] = "unhealthy"
			health["status"] = "degraded"
		}

		// Maintenance flag from admin platform settings (best effort).
		maintenance := false
		if db := database.GetDB(); db != nil {
			hctx, hcancel := context.WithTimeout(context.Background(), 2*time.Second)
			var v string
			if err := db.QueryRow(hctx, `SELECT value FROM platform_settings WHERE key = 'maintenance_mode'`).Scan(&v); err == nil {
				maintenance = v == "true" || v == `"true"`
			}
			hcancel()
		}
		health["maintenance_mode"] = maintenance

		return c.JSON(health)
	}
}

// readinessHandler checks if the service is ready to accept traffic
func readinessHandler(grpcClients *grpcclient.ProductionClients) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if !database.IsHealthy() {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"ready":  false,
				"reason": "database not ready",
			})
		}

		return c.JSON(fiber.Map{
			"ready": true,
		})
	}
}

// productionErrorHandler handles errors in production-safe manner
func productionErrorHandler(c *fiber.Ctx, err error) error {
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
		case fiber.StatusTooManyRequests:
			errorCode = "RATE_LIMIT_EXCEEDED"
		case fiber.StatusServiceUnavailable:
			errorCode = "SERVICE_UNAVAILABLE"
		}
	}

	// Log the error
	requestID := c.Locals("requestid")
	log.Printf("Error [%s] %d: %s (request_id: %v)", errorCode, code, err.Error(), requestID)

	return c.Status(code).JSON(fiber.Map{
		"error": fiber.Map{
			"code":       errorCode,
			"message":    message,
			"request_id": requestID,
		},
	})
}

// joinOrigins joins origins with comma for Fiber CORS
func joinOrigins(origins []string) string {
	// Production fallback - never return wildcard when AllowCredentials is true
	if len(origins) == 0 {
		return "https://www.synthos.dev,https://synthos.dev,https://app.synthos.dev"
	}
	result := ""
	for i, origin := range origins {
		if i > 0 {
			result += ","
		}
		result += origin
	}
	return result
}

// getEnvOrDefault returns the environment variable value or a default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
