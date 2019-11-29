package storage

import "context"

//Storage interface
type Storage interface {
	ConnectDB(ctx context.Context) error
	CloseDB() error
	GetClient() interface{}
}
