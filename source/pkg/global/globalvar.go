package global

import (
	"zpd/configs"

	"zpd/pkg/bridge"
	consul_agent "zpd/pkg/consul-agent"

	log "github.com/sirupsen/logrus"
)

// GlobalVar global variable for service
type GlobalVar struct {
	DDLDB         DDLDB
	ManagerClient bridge.ManagerClient
}

// NewGlobalVar new GlobalVar struct
func NewGlobalVar(consulAgent consul_agent.ConsulAgent, config *configs.ZPDServiceConfig) (*GlobalVar, error) {
	managerClient := bridge.NewManagerClient(config.Bridge)

	ddlDB, err := NewDDLDB(consulAgent, config.Database, managerClient)
	if err != nil {
		log.Error("[Global] New DDLDB error: ", err)
		return nil, err
	}

	return &GlobalVar{
		DDLDB:         ddlDB,
		ManagerClient: managerClient,
	}, nil
}

// Close close GlobalVar
func (gv *GlobalVar) Close() {
	gv.DDLDB.Close()
	gv.ManagerClient.Close()
}
