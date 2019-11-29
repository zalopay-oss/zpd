package consul_agent

type ConsulAgent interface {
	Register() error
	CreateSession() error
	AcquireSession() (bool, error)
	RenewSession() error
	DestroySession() error
	GetAddressLeader() (string, error)
	CloseAgent() error
}
