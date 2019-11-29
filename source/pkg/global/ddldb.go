package global

import (
	"zpd/pkg/util"
)

// DDLDB data definition language
type DDLDB interface {
	CreateDatabase(dbName string) error
	GetSchema(dbName string) (*util.Schema, error)
	Close() error
	DropDatabase(dbName string) error
	GetDatabases() ([]string, error)

	CreateTable(DBID uint64, infoTable *util.Table) error
	GetNameTablesOfDatabse(dbName string) ([]string, error)
	DropTable(DBID uint64, tbName string) error
	GetTable(DBID uint64, tbName string) (*util.Table, error)
}
