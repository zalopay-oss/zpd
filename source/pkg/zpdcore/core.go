package zpdcore

import (
	"context"
	"zpd/pkg/conn"
	zpd_proto "zpd/pkg/public-api"
)

//ZPDCore interface
type ZPDCore interface {
	Ping(timestamp int64) int64
	HandleConnection(connID string, dbName string) (*conn.ClientConn, error)
	HandleCloseConnection(connID string) error
	HandleStatement(ctx context.Context, connID string, msgReq *zpd_proto.StatementRequest) ([]byte, error)
	Close()
}
