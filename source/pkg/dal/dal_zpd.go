package dal

import (
	"context"
	"zpd/configs"
	"zpd/pkg/storage"

	"github.com/tikv/client-go/rawkv"
)

//DataAccessLayerImpl dal implement dal interface
type DataAccessLayerImpl struct {
	rawKV storage.Storage
}

func getHostPD(configDB []*configs.Database) []string {
	host := make([]string, len(configDB))
	for index, value := range configDB {
		host[index] = value.HostPD
	}

	return host
}

//NewDataAccessLayer new dalimpl
func NewDataAccessLayer(ctx context.Context, config []*configs.Database) (DataAccessLayer, error) {
	cfg := getHostPD(config)
	storage, err := storage.NewRawTiKV(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &DataAccessLayerImpl{
		rawKV: storage,
	}, nil
}

//DisconnectStorage disconnect to storagego
func (dal DataAccessLayerImpl) DisconnectStorage() error {
	return dal.rawKV.CloseDB()
}

//Get queries value with the key
func (dal DataAccessLayerImpl) Get(ctx context.Context, key []byte) ([]byte, error) {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.Get(ctx, key)
}

//Put stores key-value pair to TiKV
func (dal DataAccessLayerImpl) Put(ctx context.Context, key []byte, val []byte) error {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.Put(ctx, key, val)
}

//Delete delete key-value pair from TiKV
func (dal DataAccessLayerImpl) Delete(ctx context.Context, key []byte) error {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.Delete(ctx, key)
}

//BatchGet queries values with the keys
func (dal DataAccessLayerImpl) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.BatchGet(ctx, keys)
}

//BatchPut stores key-value pairs to TiKV
func (dal DataAccessLayerImpl) BatchPut(ctx context.Context, keys, values [][]byte) error {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.BatchPut(ctx, keys, values)
}

//BatchDelete deletes key-value pairs from TiKV
func (dal DataAccessLayerImpl) BatchDelete(ctx context.Context, keys [][]byte) error {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.BatchDelete(ctx, keys)
}

//Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs
func (dal DataAccessLayerImpl) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.Scan(ctx, startKey, endKey, limit)
}

//DeleteRange deletes all key-value pairs in a range from TiKV
func (dal DataAccessLayerImpl) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	cli := dal.rawKV.GetClient().(*rawkv.Client)
	return cli.DeleteRange(ctx, startKey, endKey)
}
