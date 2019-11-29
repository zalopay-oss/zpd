package conn

import (
	"context"
)

//Session interface
type Session interface {
	execute(ctx context.Context, sql string) ([]byte, error)
	close() error
}
