package cmd

import (
	"context"
	"flag"
	"os"
	"strconv"
	"zpd/configs"
	"zpd/pkg/global"
	grpc "zpd/pkg/protocol/grpc"
	zpdcore "zpd/pkg/zpdcore"
	zpdcore_internal "zpd/pkg/zpdcore-internal"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	consul_agent "zpd/pkg/consul-agent"
)

func initLogFile(config *configs.ZPDLog) error {
	if !config.IsLogFile {
		return nil
	}

	// open a file
	f, err := os.OpenFile(config.PathLogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(f)

	// Only log the warning severity or above.
	log.SetLevel(
		getLogLevel(config.Level),
	)

	return nil
}

func getLogLevel(logLevel string) log.Level {
	switch logLevel {
	case "PANNIC":
		return log.PanicLevel
	case "FATAL":
		return log.FatalLevel
	case "ERROR":
		return log.ErrorLevel
	case "WARN":
		return log.WarnLevel
	case "INFO":
		return log.InfoLevel
	case "DEBUG":
		return log.TraceLevel
	default:
		return log.InfoLevel
	}
}

//RunServer run gRPC server
func RunServer() error {
	ctx := context.Background()

	//load config
	config := &configs.ZPDServiceConfig{}
	configs.LoadConfig()
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal("load config: ", err)
	}

	//init logfile
	if err := initLogFile(config.Log); err != nil {
		return err
	}

	GRPCHost := flag.String("host", "zpd-service-1", "host service")
	GRPCPort := flag.Int("port", 10001, "port service")
	ID := flag.String("id", "ZPD_1", "id service")
	Name := flag.String("name", "SQLServer_1", "name service")

	flag.Parse()

	config.GRPCHost = *GRPCHost
	config.GRPCPort = *GRPCPort
	config.Consul.GRPCHost = *GRPCHost
	config.Consul.GRPCPort = *GRPCPort
	config.Consul.ID = *ID
	config.Consul.Name = *Name

	// new consul agent
	consulAgent := consul_agent.NewConsulAgent(config.Consul)

	err := consulAgent.Register()
	if err != nil {
		return err
	}

	err = consulAgent.CreateSession()
	if err != nil {
		return err
	}

	go func() {
		err := consulAgent.RenewSession()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// new global var
	globalVar, err := global.NewGlobalVar(consulAgent, config)
	if err != nil {
		return err
	}

	zpdCore, err := zpdcore.NewZPDCore(config, globalVar, consulAgent)
	if err != nil {
		return err
	}

	// new service internal
	zpdCoreInternal := zpdcore_internal.NewZPDCoreInternal(globalVar)

	return grpc.RunServer(ctx, zpdCore, zpdCoreInternal, strconv.Itoa(config.GRPCPort))
}
