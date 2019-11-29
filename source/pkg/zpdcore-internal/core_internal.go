package zpdcore_internal

import (
	zpd_internal_proto "zpd/pkg/internal-api"
	"zpd/pkg/util"
)

// ZPDCoreInternal interface
type ZPDCoreInternal interface {
	GetSchema(schemaReq *zpd_internal_proto.SchemaRequest) (*zpd_internal_proto.Schema, error)
	CreateDatabase(createDBReq *zpd_internal_proto.CreateDatabaseRequest) error
	DropDatabase(dropDBReq *zpd_internal_proto.DropDatabaseRequest) error
	GetDatabases() ([]string, error)

	CreateTable(createTbReq *zpd_internal_proto.CreateTableRequest) error
	GetNameTables(dbName string) ([]string, error)
	DropTable(DBID uint64, tbName string) error
	GetTable(DBID uint64, tbName string) (*util.Table, error)
}
