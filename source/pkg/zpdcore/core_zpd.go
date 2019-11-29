package zpdcore

import (
	"context"
	"sync"
	"time"
	configs "zpd/configs"
	"zpd/pkg/conn"
	consul_agent "zpd/pkg/consul-agent"
	error_zpd "zpd/pkg/error"
	"zpd/pkg/global"
	zpd_proto "zpd/pkg/public-api"

	log "github.com/sirupsen/logrus"
)

// ZPDCoreImpl implement PingCore
type ZPDCoreImpl struct {
	clients     map[string]*conn.ClientConn
	globalVar   *global.GlobalVar
	config      *configs.ZPDServiceConfig
	rwlock      sync.RWMutex
	consulAgent consul_agent.ConsulAgent
}

// NewZPDCore create ping core
func NewZPDCore(config *configs.ZPDServiceConfig, globalVar *global.GlobalVar, consulAgent consul_agent.ConsulAgent) (ZPDCore, error) {
	return &ZPDCoreImpl{
		clients:     make(map[string]*conn.ClientConn),
		globalVar:   globalVar,
		config:      config,
		consulAgent: consulAgent,
	}, nil
}

func (core *ZPDCoreImpl) getTimestame() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// Ping ping api
func (core *ZPDCoreImpl) Ping(timestamp int64) int64 {
	return core.getTimestame()
}

// create clientConn
func (core *ZPDCoreImpl) addClientConn(conn *conn.ClientConn) {
	core.rwlock.Lock()
	defer core.rwlock.Unlock()

	core.clients[conn.ConnectionID] = conn
}

// HandleConnection handle connection to MTiDB
func (core *ZPDCoreImpl) HandleConnection(connID string, dbName string) (*conn.ClientConn, error) {

	cc, err := conn.NewClientConn(connID, core.globalVar, dbName, core.config)
	if cc == nil {
		return nil, err
	}

	core.addClientConn(cc)
	log.Info("[zpdcore] Create new connection with connID: ", cc.ConnectionID, " and dbname: ", dbName)
	return cc, err
}

// get clientConn
func (core *ZPDCoreImpl) getClientConn(connID string) *conn.ClientConn {
	core.rwlock.RLock()
	defer core.rwlock.RUnlock()

	return core.clients[connID]
}

func (core *ZPDCoreImpl) removeClientConn(connID string) {
	core.rwlock.Lock()
	defer core.rwlock.Unlock()

	delete(core.clients, connID)
}

// HandleCloseConnection handle close connection to MTiDB
func (core *ZPDCoreImpl) HandleCloseConnection(connID string) error {
	cc := core.getClientConn(connID)
	if cc == nil {
		return error_zpd.ErrClientNoExists
	}

	core.removeClientConn(connID)
	log.Info("[zpdcore] Remove connetion with connID: ", connID)
	return cc.Close()
}

// HandleStatement handle statement
// update value return
func (core *ZPDCoreImpl) HandleStatement(ctx context.Context, connID string, msgReq *zpd_proto.StatementRequest) ([]byte, error) {
	cc := core.getClientConn(connID)
	if cc == nil {
		return nil, error_zpd.ErrClientNoExists
	}

	return cc.Handle(ctx, msgReq)
}

// Close ZPDCore
func (core *ZPDCoreImpl) Close() {
	core.globalVar.Close()
	for key, value := range core.clients {
		value.Close()
		delete(core.clients, key)
	}
}
