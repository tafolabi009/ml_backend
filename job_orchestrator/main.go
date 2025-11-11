package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	
	pb "github.com/synthos/job-orchestrator/proto"
	"github.com/synthos/job-orchestrator/internal/config"
	"github.com/synthos/job-orchestrator/internal/database"
	"github.com/synthos/job-orchestrator/internal/server"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	db, err := database.NewDatabase(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Run migrations
	if err := db.RunMigrations(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Create gRPC server with mTLS
	var grpcServer *grpc.Server
	if cfg.Environment == "production" {
		// Load TLS credentials
		creds, err := credentials.NewServerTLSFromFile(
			cfg.TLSCertFile,
			cfg.TLSKeyFile,
		)
		if err != nil {
			log.Fatalf("Failed to load TLS credentials: %v", err)
		}

		grpcServer = grpc.NewServer(
			grpc.Creds(creds),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle: 5 * time.Minute,
				Time:              10 * time.Second,
				Timeout:           3 * time.Second,
			}),
			grpc.MaxRecvMsgSize(100 * 1024 * 1024), // 100MB
			grpc.MaxSendMsgSize(100 * 1024 * 1024),
		)
	} else {
		// Development mode without TLS
		grpcServer = grpc.NewServer(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle: 5 * time.Minute,
				Time:              10 * time.Second,
				Timeout:           3 * time.Second,
			}),
			grpc.MaxRecvMsgSize(100 * 1024 * 1024),
			grpc.MaxSendMsgSize(100 * 1024 * 1024),
		)
	}

	// Initialize job orchestrator server
	orchestratorServer := server.NewJobOrchestratorServer(db, cfg)
	
	// Register gRPC service
	pb.RegisterJobOrchestratorServer(grpcServer, orchestratorServer)

	// Start listening
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Job Orchestrator service starting on port %s...", cfg.Port)
	log.Printf("Environment: %s", cfg.Environment)
	log.Printf("TLS Enabled: %v", cfg.Environment == "production")

	// Start gRPC server in goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Job Orchestrator service...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutdown timeout, forcing stop...")
		grpcServer.Stop()
	case <-stopped:
		log.Println("Job Orchestrator service stopped gracefully")
	}
}
