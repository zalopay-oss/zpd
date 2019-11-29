package consul_agent

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"zpd/configs"

	error_zpd "zpd/pkg/error"

	"github.com/hashicorp/consul/api"
)

// ConsulAgentImpl consul agent
type ConsulAgentImpl struct {
	client                         *api.Client
	addressConsul                  string
	ID                             string
	name                           string
	tag                            []string
	gRPCPort                       int
	gRPCHost                       string
	interval                       time.Duration
	deregisterCriticalServiceAfter time.Duration
	sessionTimeout                 string
	keyLeader                      string
	sessionID                      string
	doneChan                       chan struct{}
}

// NewConsulAgent new consul agent
func NewConsulAgent(config *configs.Consul) ConsulAgent {
	tags := strings.Split(config.Tag, ",")

	return &ConsulAgentImpl{
		addressConsul:                  config.AddressConsul,
		ID:                             config.ID,
		name:                           config.Name,
		tag:                            tags,
		gRPCPort:                       config.GRPCPort,
		gRPCHost:                       config.GRPCHost,
		interval:                       time.Duration(config.Interval),
		deregisterCriticalServiceAfter: time.Duration(config.DeregisterCriticalServiceAfter) * time.Minute,
		sessionTimeout:                 config.SessionTimeout,
		keyLeader:                      config.KeyLeader,
		doneChan:                       make(chan struct{}),
	}
}

// Register registe servire
func (agent *ConsulAgentImpl) Register() error {
	// create sample client
	client, err := api.NewClient(&api.Config{Address: agent.addressConsul})
	if err != nil {
		return err
	}

	reg := &api.AgentServiceRegistration{
		ID:      agent.ID,
		Name:    agent.name,
		Tags:    agent.tag,
		Port:    agent.gRPCPort,
		Address: agent.gRPCHost,
		Check: &api.AgentServiceCheck{
			Interval:                       agent.interval.String(),
			GRPC:                           fmt.Sprintf("%v:%v/%v", agent.gRPCHost, agent.gRPCPort, agent.name),
			DeregisterCriticalServiceAfter: agent.deregisterCriticalServiceAfter.String(),
		},
	}

	if err := client.Agent().ServiceRegister(reg); err != nil {
		return err
	}
	agent.client = client
	return nil
}

// CreateSession creates a session in consul with especified TTL and behavior set to delete
func (agent *ConsulAgentImpl) CreateSession() error {
	sessionConf := &api.SessionEntry{
		TTL:       agent.sessionTimeout,
		Behavior:  "delete",
		LockDelay: 1 * time.Millisecond,
		Name:      agent.name,
	}

	sessionID, _, err := agent.client.Session().Create(sessionConf, nil)
	if err != nil {
		return err
	}
	agent.sessionID = sessionID

	return nil
}

// AcquireSession basically creates the mutual exclusion lock
func (agent *ConsulAgentImpl) AcquireSession() (bool, error) {
	KVpair := &api.KVPair{
		Key:     agent.keyLeader,
		Value:   []byte(fmt.Sprintf("%v:%v", agent.gRPCHost, agent.gRPCPort)),
		Session: agent.sessionID,
	}

	aquired, _, err := agent.client.KV().Acquire(KVpair, nil)
	return aquired, err
}

// RenewSession We need to renew the session because the TTL will destroy
// the session if its not renewed and the task is taking too long
func (agent *ConsulAgentImpl) RenewSession() error {
	err := agent.client.Session().RenewPeriodic(agent.sessionTimeout, agent.sessionID, nil, agent.doneChan)
	if err != nil {
		erroMsg := fmt.Sprintf("ERROR RenewSession delete key %s: %s", agent.keyLeader, err)
		return errors.New(erroMsg)
	}
	return nil
}

// DestroySession destroys the session by triggering the behavior. So it will delete de Key as well
func (agent *ConsulAgentImpl) DestroySession() error {
	_, err := agent.client.Session().Destroy(agent.sessionID, nil)
	if err != nil {
		erroMsg := fmt.Sprintf("ERROR cannot delete key %s: %s", agent.keyLeader, err)
		return errors.New(erroMsg)
	}

	return nil
}

// GetAddressLeader get host leader to consul
func (agent *ConsulAgentImpl) GetAddressLeader() (string, error) {
	kv, _, err := agent.client.KV().Get(agent.keyLeader, nil)
	if err != nil {
		return "", err
	}

	if kv != nil && kv.Session != "" {
		leaderHostname := string(kv.Value)
		if leaderHostname == (fmt.Sprintf("%v:%v", agent.gRPCHost, agent.gRPCPort)) {
			return "", error_zpd.ErrAddressServer
		}

		return leaderHostname, nil
	}

	return "", error_zpd.ErrDoNotFindLeader
}

// CloseAgent agent close session
func (agent *ConsulAgentImpl) CloseAgent() error {
	close(agent.doneChan)
	agent.DestroySession()

	return nil
}
