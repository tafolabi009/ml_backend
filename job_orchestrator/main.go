package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/tafolabi009/backend/job_orchestrator/internal/api"
	"github.com/tafolabi009/backend/job_orchestrator/internal/service"
	"github.com/tafolabi009/backend/proto/orchestrator"
)

func main() {
	// Get configuration from environment
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50053"
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	workers, _ := strconv.Atoi(os.Getenv("WORKERS"))
	if workers == 0 {
		workers = 10
	}

	// Service addresses
	validationAddr := os.Getenv("VALIDATION_SERVICE_ADDR")
	if validationAddr == "" {
		validationAddr = "localhost:50051"
	}

	collapseAddr := os.Getenv("COLLAPSE_SERVICE_ADDR")
	if collapseAddr == "" {
		collapseAddr = "localhost:50052"
	}

	dataAddr := os.Getenv("DATA_SERVICE_ADDR")
	if dataAddr == "" {
		dataAddr = "localhost:50054"
	}

	// Initialize orchestrator service with gRPC clients
	orchestratorService, err := service.NewOrchestratorService(workers, validationAddr, collapseAddr, dataAddr)
	if err != nil {
		log.Fatalf("Failed to create orchestrator service: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              10 * time.Second,
			Timeout:           3 * time.Second,
		}),
		grpc.MaxRecvMsgSize(100*1024*1024), // 100MB
		grpc.MaxSendMsgSize(100*1024*1024),
	)

	// Initialize gRPC server wrapper
	grpcOrchestratorServer := service.NewOrchestratorServer(orchestratorService)
	orchestrator.RegisterJobOrchestratorServer(grpcServer, grpcOrchestratorServer)

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	go func() {
		log.Printf("gRPC server starting on port %s...", grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Create REST API server
	router := mux.NewRouter()
	restHandler := api.NewRESTHandler(orchestratorService)
	restHandler.SetupRoutes(router)

	// Add CORS middleware
	router.Use(corsMiddleware)
	router.Use(loggingMiddleware)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%s", httpPort),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start HTTP server
	go func() {
		log.Printf("HTTP REST API server starting on port %s...", httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to serve HTTP: %v", err)
		}
	}()

	log.Printf("Job Orchestrator service started successfully")
	log.Printf("- gRPC endpoint: :%s", grpcPort)
	log.Printf("- REST API endpoint: :%s", httpPort)
	log.Printf("- Workers: %d", workers)
	log.Printf("- Connected to validation service: %s", validationAddr)
	log.Printf("- Connected to collapse service: %s", collapseAddr)
	log.Printf("- Connected to data service: %s", dataAddr)

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Job Orchestrator service...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Shutdown gRPC server
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout, forcing stop...")
		grpcServer.Stop()
	case <-stopped:
		log.Println("gRPC server stopped gracefully")
	}

	// Stop orchestrator service
	orchestratorService.Stop()

	log.Println("Job Orchestrator service stopped successfully")
}

// Middleware
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s - %v", r.Method, r.URL.Path, time.Since(start))
	})
}
