package bridge

import (
	"context"
	"sync"
	"time"

	"zpd/configs"

	zpd_internal_proto "zpd/pkg/internal-api"

	error_zpd "zpd/pkg/error"

	cmap "github.com/orcaman/concurrent-map"
	grpcpool "github.com/processout/grpc-go-pool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	IDLE              = "IDLE"
	CONNECTING        = "CONNECTING"
	READY             = "READY"
	TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
	SHUTDOWN          = "SHUTDOWN"
	INVALID           = "Invalid-State"
)

// ManagerClient interface
type ManagerClient interface {
	GetZPDClient(host string) (ZPDClient, error)
	Close()
}

// ManagerClientImpl manager client
type ManagerClientImpl struct {
	maxPoolSize int
	timeOut     int
	// poolClients map[string]*PoolClient
	poolClients cmap.ConcurrentMap
	mux         sync.RWMutex
}

// PoolClient mansger client struct
type PoolClient struct {
	pool *grpcpool.Pool
	host string
}

// NewManagerClient new factory manager client
func NewManagerClient(config *configs.Bridge) ManagerClient {

	return &ManagerClientImpl{
		maxPoolSize: config.PoolSize,
		timeOut:     config.TimeOut,
		poolClients: cmap.New(),
	}
}

// NewManagerClient creat manager client
func (managerClient *ManagerClientImpl) newPoolClient(host string) (*PoolClient, error) {
	poolClient := &PoolClient{
		host: host,
	}

	p, err := grpcpool.New(poolClient.newFactoryClient, managerClient.maxPoolSize, managerClient.maxPoolSize, time.Duration(managerClient.timeOut)*time.Second)
	if err != nil {
		return nil, err
	}
	poolClient.pool = p

	return poolClient, nil
}

// AddPoolClient add manager client
func (managerClient *ManagerClientImpl) addPoolClient(host string) (*PoolClient, error) {
	log.Info("[Manager client] Add poolClient with host: " + host)
	poolClient, err := managerClient.newPoolClient(host)
	if err != nil {
		return nil, err
	}
	managerClient.poolClients.Set(host, poolClient)

	return poolClient, nil
}

func (managerClient *ManagerClientImpl) getPoolClient(host string) *PoolClient {
	pool, ok := managerClient.poolClients.Get(host)
	if !ok {
		return nil
	}

	return pool.(*PoolClient)
}

func (managerClient *ManagerClientImpl) removePoolClient(host string) {
	log.Info("[Manager client] Remove poolclient with host: " + host)

	managerClient.poolClients.Remove(host)
}

// GetZPDClient get pool client
func (managerClient *ManagerClientImpl) GetZPDClient(host string) (ZPDClient, error) {
	pool := managerClient.getPoolClient(host)

	if pool == nil {
		poolTmp, err := managerClient.addPoolClient(host)
		if err != nil {
			log.Error("[Manager Client] Get client with host " + host + " error: " + err.Error())

			return nil, error_zpd.ErrServerDoesNotReady
		}
		pool = poolTmp
	}

	client, err := pool.newClient()

	if err != nil {
		managerClient.removePoolClient(host)
		log.Error("[Manager Client] Remove poolClient with host: " + host + " error: " + err.Error())

		return nil, err
	}

	return client, nil
}

// Close close manager client
func (managerClient *ManagerClientImpl) Close() {
	for item := range managerClient.poolClients.Iter() {
		poolClient := item.Val.(*PoolClient)
		poolClient.closePool()
	}
}

// NewFactoryClient create factory client
func (poolClient *PoolClient) newFactoryClient() (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(poolClient.host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// NewClient new client
func (poolClient *PoolClient) newClient() (ZPDClient, error) {
	ctx := context.Background()

	conn, err := poolClient.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	// check state conn
	state := conn.GetState().String()

	if state == TRANSIENT_FAILURE || state == SHUTDOWN || state == INVALID {
		log.Error("[PoolClient] State connection " + state)
		return nil, error_zpd.ErrConnectionDoesNotReady
	}

	return &ZPDClientImpl{
		client: zpd_internal_proto.NewZPDInternalServiceClient(conn.ClientConn),
		conn:   conn,
		ctx:    ctx,
	}, nil
}

// ClosePool close pool
func (poolClient *PoolClient) closePool() {
	poolClient.pool.Close()
}
