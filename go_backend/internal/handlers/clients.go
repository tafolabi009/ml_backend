package handlers

import (
	"github.com/tafolabi009/backend/go_backend/pkg/grpcclient"
)

var (
	// Global gRPC clients
	grpcClients *grpcclient.Clients
)

// SetGRPCClients sets the global gRPC clients
func SetGRPCClients(clients *grpcclient.Clients) {
	grpcClients = clients
}

// GetGRPCClients returns the global gRPC clients
func GetGRPCClients() *grpcclient.Clients {
	return grpcClients
}
