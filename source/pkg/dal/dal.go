package dal

import (
	"context"
)

//DataAccessLayer dal struct
type DataAccessLayer interface {
	DisconnectStorage() error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, val []byte) error
	Delete(ctx context.Context, key []byte) error
	BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error)
	BatchPut(ctx context.Context, keys, value [][]byte) error
	BatchDelete(ctx context.Context, keys [][]byte) error
	Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
}
