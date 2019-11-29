package conn

import (
	"context"
	"zpd/configs"
	"zpd/pkg/global"
	zpd_proto "zpd/pkg/public-api"

	log "github.com/sirupsen/logrus"
)

//ClientConn struct
type ClientConn struct {
	ConnectionID string
	Session      Session
	Ctx          context.Context
}

//NewClientConn create new clientConn
func NewClientConn(connID string, globalVar *global.GlobalVar, dbName string, config *configs.ZPDServiceConfig) (*ClientConn, error) {
	ctx := context.Background()
	ss, err := newSession(ctx, globalVar, dbName, config)

	if ss == nil {
		return nil, err
	} else if err != nil {
		return &ClientConn{
			ConnectionID: connID,
			Session:      ss,
			Ctx:          ctx,
		}, err
	}

	return &ClientConn{
		ConnectionID: connID,
		Session:      ss,
		Ctx:          ctx,
	}, nil
}

//Close close ClientConn
func (cc *ClientConn) Close() error {
	return cc.Session.close()
}

//Handle handle statement sql
func (cc *ClientConn) Handle(ctx context.Context, msgReq *zpd_proto.StatementRequest) ([]byte, error) {
	data, err := cc.Session.execute(ctx, msgReq.Sql)

	if err != nil {
		log.Error("[ClientConn] Handle sql `"+msgReq.Sql+"` error: ", err.Error())
		return nil, err
	}

	return data, nil
}
