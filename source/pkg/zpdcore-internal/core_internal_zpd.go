package zpdcore_internal

import (
	"zpd/pkg/global"
	zpd_internal_proto "zpd/pkg/internal-api"
	"zpd/pkg/util"
)

// ZPDCoreInternalImpl zpd core internal impl
type ZPDCoreInternalImpl struct {
	globalVar *global.GlobalVar
}

// NewZPDCoreInternal implenment ZPDCoreInternal
func NewZPDCoreInternal(globalVar *global.GlobalVar) ZPDCoreInternal {
	return &ZPDCoreInternalImpl{
		globalVar: globalVar,
	}
}

// GetSchema get schema by name
// update field table later
func (coreInternal *ZPDCoreInternalImpl) GetSchema(schemaReq *zpd_internal_proto.SchemaRequest) (*zpd_internal_proto.Schema, error) {
	schema, err := coreInternal.globalVar.DDLDB.GetSchema(schemaReq.Dbname)
	if err != nil {
		return nil, err
	}

	return util.MappingSchemaProto(schema), nil
}

// CreateDatabase create database
func (coreInternal *ZPDCoreInternalImpl) CreateDatabase(createDBReq *zpd_internal_proto.CreateDatabaseRequest) error {
	return coreInternal.globalVar.DDLDB.CreateDatabase(createDBReq.Dbname)
}

// DropDatabase drop database
func (coreInternal *ZPDCoreInternalImpl) DropDatabase(dropDBReq *zpd_internal_proto.DropDatabaseRequest) error {
	return coreInternal.globalVar.DDLDB.DropDatabase(dropDBReq.Dbname)
}

// GetDatabases drop database
func (coreInternal *ZPDCoreInternalImpl) GetDatabases() ([]string, error) {
	return coreInternal.globalVar.DDLDB.GetDatabases()
}

// CreateTable create table
func (coreInternal *ZPDCoreInternalImpl) CreateTable(createTbReq *zpd_internal_proto.CreateTableRequest) error {
	table := util.MappingTable(createTbReq.Table)
	return coreInternal.globalVar.DDLDB.CreateTable(createTbReq.DBID, table)
}

// GetNameTables get name tables
func (coreInternal *ZPDCoreInternalImpl) GetNameTables(dbName string) ([]string, error) {
	return coreInternal.globalVar.DDLDB.GetNameTablesOfDatabse(dbName)
}

// DropTable drop table
func (coreInternal *ZPDCoreInternalImpl) DropTable(DBID uint64, tbName string) error {
	return coreInternal.globalVar.DDLDB.DropTable(DBID, tbName)
}

// GetTable get table
func (coreInternal *ZPDCoreInternalImpl) GetTable(DBID uint64, tbName string) (*util.Table, error) {
	return coreInternal.globalVar.DDLDB.GetTable(DBID, tbName)
}
