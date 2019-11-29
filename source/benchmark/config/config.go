package config_benchmark

import (
	"github.com/spf13/viper"
)

const configFilePath = "."
const configFileName = "config"

type Bridge struct {
	PoolSize int
	TimeOut  int
}

// ZPDBenchmarkConfig ZPDBenchmarkConfig
type ZPDBenchmarkConfig struct {
	ZPDService []*ZPDService
	Bridge     *Bridge
	DBName     string
	TestName   string
}

// ZPDLog config
type ZPDLog struct {
	Level       string
	IsLogFile   bool
	PathLogFile string
}

// ZPDService ZPDService
type ZPDService struct {
	Host string
}

// LoadConfig load config
func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yaml")

	return viper.ReadInConfig()
}
