package configs

import (
	"github.com/spf13/viper"
)

const configFilePath = "."
const configFileName = "config"

// ZPDServiceConfig config
type ZPDServiceConfig struct {
	GRPCPort int
	GRPCHost string
	ID       uint64
	Log      *ZPDLog
	Database []*Database
	Consul   *Consul
	Bridge   *Bridge
}

// ZPDLog config
type ZPDLog struct {
	Level       string
	IsLogFile   bool
	PathLogFile string
}

// Database config
type Database struct {
	HostPD string
}

// Consul config
type Consul struct {
	AddressConsul                  string
	ID                             string
	Name                           string
	Tag                            string
	GRPCPort                       int
	GRPCHost                       string
	Interval                       int
	DeregisterCriticalServiceAfter int
	SessionTimeout                 string
	KeyLeader                      string
}

type Bridge struct {
	PoolSize int
	TimeOut  int
}

// LoadConfig load config
func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yaml")

	return viper.ReadInConfig()
}
