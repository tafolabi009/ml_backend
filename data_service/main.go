package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/synthos/data-service/internal/repository"
	"github.com/synthos/data-service/internal/service"
	pb "github.com/tafolabi009/backend/proto/data"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50054"
	}

	storagePath := os.Getenv("STORAGE_PATH")
	if storagePath == "" {
		storagePath = "/tmp/synthos_datasets"
	}

	databaseURL := os.Getenv("DATA_SERVICE_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}

	var metadataRepo *repository.MetadataRepository
	if databaseURL != "" {
		pool, err := pgxpool.New(context.Background(), databaseURL)
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		metadataRepo = repository.NewMetadataRepository(pool)
		if err := metadataRepo.EnsureSchema(context.Background()); err != nil {
			log.Fatalf("Failed to ensure datasets schema: %v", err)
		}
		defer pool.Close()
		log.Printf("  - Metadata DB enabled")
	} else {
		log.Printf("  - Metadata DB disabled (set DATA_SERVICE_DATABASE_URL or DATABASE_URL)")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server with options
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(100*1024*1024), // 100MB
		grpc.MaxSendMsgSize(100*1024*1024), // 100MB
	)

	// Create and register data service
	dataService := service.NewDataServiceServerWithRepo(storagePath, metadataRepo)
	pb.RegisterDataServiceServer(grpcServer, dataService)

	// Enable reflection for grpcurl/grpcui
	reflection.Register(grpcServer)

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down Data Service...")
		grpcServer.GracefulStop()
	}()

	log.Printf("🚀 Data Service starting on port %s...", port)
	log.Printf("  - Storage Path: %s", storagePath)
	if databaseURL != "" {
		log.Printf("  - Database URL: configured")
	}
	log.Printf("  - UploadDataset: Ready for streaming uploads")
	log.Printf("  - GetDatasetMetadata: Ready")
	log.Printf("  - ListDatasets: Ready")
	log.Printf("  - DeleteDataset: Ready")
	log.Printf("  - ProfileDataset: Ready")
	log.Printf("  - StreamDataset: Ready for streaming downloads")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
