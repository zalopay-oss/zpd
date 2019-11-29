package benchmark

import (
	"context"
	"math/rand"
	"time"
	config_benchmark "zpd/benchmark/config"
	zpd_proto "zpd/pkg/public-api"

	grpcpool "github.com/processout/grpc-go-pool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type FactoryManagerClient struct {
	config         *config_benchmark.ZPDBenchmarkConfig
	numberElement  int
	managerClients []*ManagerClient
}

//ManagerClient mansger client struct
type ManagerClient struct {
	pool     *grpcpool.Pool
	host     string
	poolSize int
	timeOut  int
}

//ZPDClient client ping struct
type ZPDClient struct {
	client zpd_proto.ZPDServiceClient
	conn   *grpcpool.ClientConn
	ctx    context.Context
}

// NewFactoryManagerClient new factory
func NewFactoryManagerClient(config *config_benchmark.ZPDBenchmarkConfig) *FactoryManagerClient {
	managerClients := make([]*ManagerClient, len(config.ZPDService))
	rand.Seed(time.Now().UTC().UnixNano())

	for i := 0; i < len(config.ZPDService); i++ {
		managerClients[i] = newManagerClient(config, i)
	}

	return &FactoryManagerClient{
		config:         config,
		numberElement:  len(config.ZPDService),
		managerClients: managerClients,
	}
}

//NewManagerClient creat manager client
func newManagerClient(config *config_benchmark.ZPDBenchmarkConfig, ID int) *ManagerClient {
	manager := &ManagerClient{
		host:     config.ZPDService[ID].Host,
		poolSize: config.Bridge.PoolSize,
		timeOut:  config.Bridge.TimeOut,
	}

	p, err := grpcpool.New(manager.NewFactoryClient, manager.poolSize, manager.poolSize, time.Duration(manager.timeOut)*time.Second)
	if err != nil {
		log.Fatal("Do not init connection pool")
	}

	manager.pool = p

	return manager
}

//NewFactoryClient create factory client
func (manager *ManagerClient) NewFactoryClient() (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(manager.host, grpc.WithInsecure())
	if err != nil {
		log.WithFields(log.Fields{"Error": err}).Fatal("Did not connect server")
		return nil, err
	}

	return conn, nil
}

// NewClient new client
func (fManagerClient *FactoryManagerClient) NewClient() *ZPDClient {
	index := rand.Intn(fManagerClient.numberElement)
	mangerClient := fManagerClient.managerClients[index]
	// log.Info(index)
	return mangerClient.newClient()
}

func (fManagerClient *FactoryManagerClient) prepareConnectionToZPD(dbName string) {
	for _, v := range fManagerClient.managerClients {
		for i := 0; i < fManagerClient.config.Bridge.PoolSize; i++ {
			client := v.newClient()
			client.ConnectDatabase(dbName)
			client.Close()
		}
	}
}

// CloseAllConnection close all
func (fManagerClient *FactoryManagerClient) CloseAllConnection() {
	for _, item := range fManagerClient.managerClients {
		for i := 0; i < fManagerClient.config.Bridge.PoolSize; i++ {
			client := item.newClient()
			client.CloseConnectionDatabase()
		}
	}
}

//NewClient new client
func (manager *ManagerClient) newClient() *ZPDClient {
	ctx := context.Background()

	conn, _ := manager.pool.Get(ctx)
	return &ZPDClient{
		client: zpd_proto.NewZPDServiceClient(conn.ClientConn),
		conn:   conn,
		ctx:    ctx,
	}
}

//ConnectDatabase connecton DB api
func (c *ZPDClient) ConnectDatabase(dbName string) (*zpd_proto.MessageResponse, error) {
	msgConnectioDB := &zpd_proto.ConnectionDBRequest{
		Dbname: dbName,
	}

	return c.client.ConnectDatabase(c.ctx, msgConnectioDB)
}

//CloseConnectionDatabase connecton DB api
func (c *ZPDClient) CloseConnectionDatabase() (*zpd_proto.MessageResponse, error) {
	msgConnectioDBClose := &zpd_proto.CloseConnectionDBRequest{}

	return c.client.CloseConnectionDatabase(c.ctx, msgConnectioDBClose)
}

//ExecuteStatement execute stmt api
func (c *ZPDClient) ExecuteStatement(typeExec zpd_proto.SQLType, sql string) (*zpd_proto.StatementResponse, error) {
	msgExecuteStmt := &zpd_proto.StatementRequest{
		Type: typeExec,
		Sql:  sql,
	}

	return c.client.ExecuteStatement(c.ctx, msgExecuteStmt)
}

//Close close conn
func (c *ZPDClient) Close() error {
	return c.conn.Close()
}
