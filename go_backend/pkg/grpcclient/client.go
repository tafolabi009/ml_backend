package grpcclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	
	validationpb "github.com/tafolabi009/backend/proto/gen/go"
	collapsepb "github.com/tafolabi009/backend/proto/gen/go"
	datapb "github.com/tafolabi009/backend/proto/gen/go"
)

// Clients holds all gRPC client connections
type Clients struct {
	Validation validationpb.ValidationServiceClient
	Collapse   collapsepb.CollapseServiceClient
	Data       datapb.DataServiceClient
	
	validationConn *grpc.ClientConn
	collapseConn   *grpc.ClientConn
	dataConn       *grpc.ClientConn
}

// NewClients creates and connects all gRPC clients
func NewClients(validationAddr, collapseAddr, dataAddr string) (*Clients, error) {
	clients := &Clients{}
	
	// Common dial options
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100 * 1024 * 1024), // 100MB
			grpc.MaxCallSendMsgSize(100 * 1024 * 1024), // 100MB
		),
	}
	
	// Connect to validation service
	log.Printf("Connecting to Validation Service at %s", validationAddr)
	validationConn, err := grpc.NewClient(validationAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to validation service: %w", err)
	}
	clients.validationConn = validationConn
	clients.Validation = validationpb.NewValidationServiceClient(validationConn)
	
	// Connect to collapse service
	log.Printf("Connecting to Collapse Service at %s", collapseAddr)
	collapseConn, err := grpc.NewClient(collapseAddr, dialOpts...)
	if err != nil {
		validationConn.Close()
		return nil, fmt.Errorf("failed to connect to collapse service: %w", err)
	}
	clients.collapseConn = collapseConn
	clients.Collapse = collapsepb.NewCollapseServiceClient(collapseConn)
	
	// Connect to data service
	log.Printf("Connecting to Data Service at %s", dataAddr)
	dataConn, err := grpc.NewClient(dataAddr, dialOpts...)
	if err != nil {
		validationConn.Close()
		collapseConn.Close()
		return nil, fmt.Errorf("failed to connect to data service: %w", err)
	}
	clients.dataConn = dataConn
	clients.Data = datapb.NewDataServiceClient(dataConn)
	
	log.Println("✅ All gRPC clients connected successfully")
	return clients, nil
}

// Close closes all gRPC connections
func (c *Clients) Close() {
	if c.validationConn != nil {
		c.validationConn.Close()
		log.Println("Validation service connection closed")
	}
	if c.collapseConn != nil {
		c.collapseConn.Close()
		log.Println("Collapse service connection closed")
	}
	if c.dataConn != nil {
		c.dataConn.Close()
		log.Println("Data service connection closed")
	}
}

// Health checks all gRPC services
func (c *Clients) Health(ctx context.Context) error {
	// Check validation service
	_, err := c.Validation.GetTrainingProgress(ctx, &validationpb.ProgressRequest{JobId: "health-check"})
	if err != nil && err.Error() != "rpc error: code = NotFound desc = Job health-check not found" {
		return fmt.Errorf("validation service health check failed: %w", err)
	}
	
	// Additional health checks can be added here
	return nil
}
