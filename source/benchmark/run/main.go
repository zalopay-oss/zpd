package main

import (
	"zpd/benchmark"

	config_benchmark "zpd/benchmark/config"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func run() {
	//load config
	config := &config_benchmark.ZPDBenchmarkConfig{}
	config_benchmark.LoadConfig()
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal("load config: ", err)
	}

	log.Info("Load test: ", config.TestName)

	managerClient := benchmark.NewFactoryManagerClient(config)

	boomerClient := benchmark.BoomerClient{}
	boomerClient.LoadFactoryManagerClient(managerClient)
	tasks, _ := boomerClient.LoadTask(config.TestName, 1)

	boomerClient.RunTask(tasks)
}

func main() {
	run()
}
