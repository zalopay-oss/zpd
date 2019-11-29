package service

import (
	"context"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// HealthImpl implement grpc_health_v1.HealthServer
type HealthImpl struct{}

// NewHealthService create HealthService
func NewHealthService() grpc_health_v1.HealthServer {
	return &HealthImpl{}
}

// Check healthy service
func (h *HealthImpl) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// Watch implement soon
func (h *HealthImpl) Watch(req *grpc_health_v1.HealthCheckRequest, w grpc_health_v1.Health_WatchServer) error {
	return nil
}
