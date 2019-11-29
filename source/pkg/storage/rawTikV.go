package storage

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

//RawTiKV struct
type RawTiKV struct {
	HostPD    []string
	ClusterID uint64
	client    *rawkv.Client
}

//NewRawTiKV new rawTiKVClient
func NewRawTiKV(ctx context.Context, hostPD []string) (Storage, error) {
	rawTiKV := &RawTiKV{
		HostPD: hostPD,
	}

	if err := rawTiKV.ConnectDB(ctx); err != nil {
		return nil, err
	}

	return rawTiKV, nil
}

//ConnectDB connectDB
func (rawTiKV *RawTiKV) ConnectDB(ctx context.Context) error {
	cli, err := rawkv.NewClient(ctx, rawTiKV.HostPD, config.Default())
	if err != nil {
		return err
	}

	rawTiKV.ClusterID = cli.ClusterID()
	rawTiKV.client = cli
	// log.Info("[Storage] Connected TiKV with address PD: ", rawTiKV.HostPD)

	return nil
}

//CloseDB close rawClientTiKV
func (rawTiKV *RawTiKV) CloseDB() error {
	if err := rawTiKV.client.Close(); err != nil {
		log.Error("[Storage] Close connect TiKV error: ", err)
		return err
	}

	// log.Info("[Storage] Close connect TikV success")

	return nil
}

//GetClient get rawClientTiKV
func (rawTiKV *RawTiKV) GetClient() interface{} {
	return rawTiKV.client
}
