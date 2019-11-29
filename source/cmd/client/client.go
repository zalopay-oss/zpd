package client

import (
	"context"
	"fmt"
	"log"
	"time"
	"zpd/configs"
	zpd_proto "zpd/pkg/public-api"

	"google.golang.org/grpc"
)

//ClientZPD client MTiDB struct
type ClientZPD struct {
	client zpd_proto.ZPDServiceClient
	conn   *grpc.ClientConn
	ctx    context.Context
}

//NewClient new client
func NewClient(config *configs.ZPDServiceConfig) *ClientZPD {
	ctx := context.Background()

	client, conn := connectServer(config.GRPCHost, config.GRPCPort)

	return &ClientZPD{
		client: client,
		conn:   conn,
		ctx:    ctx,
	}
}

//connectServer connect server
func connectServer(host string, port int) (zpd_proto.ZPDServiceClient, *grpc.ClientConn) {
	address := fmt.Sprintf("%s:%d", host, port)

	log.Println("[Client] Connect to: ", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect server: %v", err)
	}

	return zpd_proto.NewZPDServiceClient(conn), conn
}

func getTimestame() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

//Close close connection
func (c *ClientZPD) Close() error {
	return c.conn.Close()
}

//Ping api
func (c *ClientZPD) Ping() (*zpd_proto.Pong, error) {
	msgPing := &zpd_proto.Ping{
		Timestamp: getTimestame(),
	}

	return c.client.Ping(c.ctx, msgPing)
}

//ConnectDatabase connecton DB api
func (c *ClientZPD) ConnectDatabase(dbName string) (*zpd_proto.MessageResponse, error) {
	msgConnectioDB := &zpd_proto.ConnectionDBRequest{
		Dbname: dbName,
	}

	return c.client.ConnectDatabase(c.ctx, msgConnectioDB)
}

//CloseConnectionDatabase connecton DB api
func (c *ClientZPD) CloseConnectionDatabase() (*zpd_proto.MessageResponse, error) {
	msgConnectioDBClose := &zpd_proto.CloseConnectionDBRequest{}

	return c.client.CloseConnectionDatabase(c.ctx, msgConnectioDBClose)
}

//ExecuteStatement execute stmt api
func (c *ClientZPD) ExecuteStatement(typeExec zpd_proto.SQLType, sql string) (*zpd_proto.StatementResponse, error) {
	msgExecuteStmt := &zpd_proto.StatementRequest{
		Type: typeExec,
		Sql:  sql,
	}

	return c.client.ExecuteStatement(c.ctx, msgExecuteStmt)
}
