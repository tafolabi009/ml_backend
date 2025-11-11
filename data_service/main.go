package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	
	pb "github.com/tafolabi009/backend/proto/data"
	"github.com/tafolabi009/backend/data_service/internal/service"
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

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server with options
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(100 * 1024 * 1024), // 100MB
		grpc.MaxSendMsgSize(100 * 1024 * 1024), // 100MB
	)

	// Create and register data service
	dataService := service.NewDataServiceServer(storagePath)
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
