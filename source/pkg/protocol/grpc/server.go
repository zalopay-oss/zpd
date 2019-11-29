package grpc

import (
	"context"
	"net"
	"os"
	"os/signal"
	zpd_proto "zpd/pkg/public-api"
	"zpd/pkg/service"

	zpd_internal_proto "zpd/pkg/internal-api"

	zpdcore "zpd/pkg/zpdcore"
	zpdcore_internal "zpd/pkg/zpdcore-internal"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

//RunServer run gRPC service
func RunServer(ctx context.Context, zpdCore zpdcore.ZPDCore, zpdCoreInternal zpdcore_internal.ZPDCoreInternal, port string) error {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	zpdservice := service.NewZPDService(zpdCore)
	healthyService := service.NewHealthService()
	zpdServiceInternal := service.NewZPDInternalService(zpdCoreInternal)

	server := grpc.NewServer()

	zpd_proto.RegisterZPDServiceServer(server, zpdservice)
	grpc_health_v1.RegisterHealthServer(server, healthyService)
	zpd_internal_proto.RegisterZPDInternalServiceServer(server, zpdServiceInternal)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(zpdCore zpdcore.ZPDCore, zpdCoreInternal zpdcore_internal.ZPDCoreInternal) {
		for range c {
			log.Info("shutting down gRPC server...")

			server.GracefulStop()
			zpdCore.Close()
			// zpdCoreInternal.Close()

			<-ctx.Done()
		}
	}(zpdCore, zpdCoreInternal)

	log.Info("Start ZPD service port " + port + " ...")
	return server.Serve(listen)
}
