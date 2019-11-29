package service

import (
	"context"
	"strings"
	zpd_proto "zpd/pkg/public-api"
	"zpd/pkg/zpdcore"

	error_zpd "zpd/pkg/error"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/peer"
)

//ZPDService mtidb service
type ZPDService struct {
	core zpdcore.ZPDCore
}

//NewZPDService new mtidb service
func NewZPDService(core zpdcore.ZPDCore) zpd_proto.ZPDServiceServer {
	return &ZPDService{
		core: core,
	}
}

func (service *ZPDService) getConnID(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", error_zpd.ErrGetPeerFailed
	}

	s := strings.Split(p.Addr.String(), ":")

	return s[len(s)-1], nil
}

//Ping api
func (service *ZPDService) Ping(ctx context.Context, msgReq *zpd_proto.Ping) (*zpd_proto.Pong, error) {
	timestamp := service.core.Ping(msgReq.Timestamp)
	code := 1
	err := ""

	status := &zpd_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_proto.Pong{
		Timestamp:   timestamp,
		ServiceName: "MTiDB Service",
		Status:      status,
	}, nil
}

//ConnectDatabase api
func (service *ZPDService) ConnectDatabase(ctx context.Context, msgReq *zpd_proto.ConnectionDBRequest) (*zpd_proto.MessageResponse, error) {
	code := 1
	err := ""

	connID, ero := service.getConnID(ctx)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPDService] Get connID failed with error: ", err)
	} else {
		cc, ero := service.core.HandleConnection(connID, msgReq.Dbname)
		if cc == nil {
			code = 0
			err = ero.Error()

			log.Error("[ZPDService] Handle connection failed with error: ", err)
		} else if ero != nil {
			err = ero.Error()
		}
	}

	status := &zpd_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_proto.MessageResponse{
		Status: status,
	}, nil
}

//CloseConnectionDatabase close connectionDB api
func (service *ZPDService) CloseConnectionDatabase(ctx context.Context, msgReq *zpd_proto.CloseConnectionDBRequest) (*zpd_proto.MessageResponse, error) {
	code := 1
	err := ""

	connID, ero := service.getConnID(ctx)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPDService] Get connID failed with error: ", err)
	} else {
		ero = service.core.HandleCloseConnection(connID)
		if ero != nil {
			code = 0
			err = ero.Error()

			log.Error("[ZPDService] Handle close clientConn failed with error: ", err)
		}
	}

	status := &zpd_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_proto.MessageResponse{
		Status: status,
	}, nil
}

//ExecuteStatement execute statement
func (service *ZPDService) ExecuteStatement(ctx context.Context, msgReq *zpd_proto.StatementRequest) (*zpd_proto.StatementResponse, error) {
	var data []byte
	code := 1
	err := ""

	connID, ero := service.getConnID(ctx)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPDService] Get connID failed with error: ", err)
	} else {
		data, ero = service.core.HandleStatement(ctx, connID, msgReq)
		if ero != nil {
			code = 0
			err = ero.Error()

			log.Error("[ZPDService] Handle statement failed with error: ", err)
		}
	}

	status := &zpd_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_proto.StatementResponse{
		Type:   msgReq.Type,
		Data:   data,
		Status: status,
	}, nil
}
