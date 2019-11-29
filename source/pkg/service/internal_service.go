package service

import (
	"context"
	zpd_internal_proto "zpd/pkg/internal-api"
	zpdcore_internal "zpd/pkg/zpdcore-internal"

	"zpd/pkg/util"

	log "github.com/sirupsen/logrus"
)

// ZPDInternalService struct
type ZPDInternalService struct {
	coreInternal zpdcore_internal.ZPDCoreInternal
}

// NewZPDInternalService new zpd internal service
func NewZPDInternalService(core zpdcore_internal.ZPDCoreInternal) zpd_internal_proto.ZPDInternalServiceServer {
	return &ZPDInternalService{
		coreInternal: core,
	}
}

// GetSchema get schema
func (zpdInternal *ZPDInternalService) GetSchema(ctx context.Context, schemaReq *zpd_internal_proto.SchemaRequest) (*zpd_internal_proto.SchemaResponse, error) {
	code := 1
	err := ""

	schema, ero := zpdInternal.coreInternal.GetSchema(schemaReq)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Get schema ", schemaReq.Dbname, " error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.SchemaResponse{
		Schema: schema,
		Status: status,
	}, nil
}

// CreateDatabase create database
func (zpdInternal *ZPDInternalService) CreateDatabase(ctx context.Context, createDBReq *zpd_internal_proto.CreateDatabaseRequest) (*zpd_internal_proto.CreateDatabaseResponse, error) {
	code := 1
	err := ""

	ero := zpdInternal.coreInternal.CreateDatabase(createDBReq)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Create database ", createDBReq.Dbname, " error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.CreateDatabaseResponse{
		Status: status,
	}, nil
}

// DropDatabase drop database
func (zpdInternal *ZPDInternalService) DropDatabase(ctx context.Context, dropDBReq *zpd_internal_proto.DropDatabaseRequest) (*zpd_internal_proto.DropDatabaseResponse, error) {
	code := 1
	err := ""

	ero := zpdInternal.coreInternal.DropDatabase(dropDBReq)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Drop database ", dropDBReq.Dbname, " error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.DropDatabaseResponse{
		Status: status,
	}, nil
}

// GetDatabases drop database
func (zpdInternal *ZPDInternalService) GetDatabases(ctx context.Context, getDBReq *zpd_internal_proto.GetDatabasesRequest) (*zpd_internal_proto.GetDatabasesResponse, error) {
	code := 1
	err := ""

	dbs, ero := zpdInternal.coreInternal.GetDatabases()
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Gety databases error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.GetDatabasesResponse{
		Databases: dbs,
		Status:    status,
	}, nil
}

// CreateTable create table
func (zpdInternal *ZPDInternalService) CreateTable(ctx context.Context, createTbReq *zpd_internal_proto.CreateTableRequest) (*zpd_internal_proto.CreateTableResponse, error) {
	code := 1
	err := ""

	ero := zpdInternal.coreInternal.CreateTable(createTbReq)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Gety databases error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.CreateTableResponse{
		Status: status,
	}, nil
}

// GetNameTables get nam tables of database
func (zpdInternal *ZPDInternalService) GetNameTables(ctx context.Context, getTBReq *zpd_internal_proto.GetNameTablesRequest) (*zpd_internal_proto.GetNameTablesResponse, error) {
	code := 1
	err := ""

	nameTables, ero := zpdInternal.coreInternal.GetNameTables(getTBReq.Dbname)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Gety databases error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.GetNameTablesResponse{
		Nametables: nameTables,
		Status:     status,
	}, nil
}

// DropTable drop table
func (zpdInternal *ZPDInternalService) DropTable(ctx context.Context, dropTBReq *zpd_internal_proto.DropTableRequest) (*zpd_internal_proto.DropTableResponse, error) {
	code := 1
	err := ""

	ero := zpdInternal.coreInternal.DropTable(dropTBReq.DBID, dropTBReq.Tbname)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Gety databases error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	return &zpd_internal_proto.DropTableResponse{
		Status: status,
	}, nil
}

// GetTable get table
func (zpdInternal *ZPDInternalService) GetTable(ctx context.Context, getTBReq *zpd_internal_proto.GetTableRequest) (*zpd_internal_proto.GetTableResponse, error) {
	code := 1
	err := ""
	tbProto := &zpd_internal_proto.Table{}

	tb, ero := zpdInternal.coreInternal.GetTable(getTBReq.DBID, getTBReq.Tbname)
	if ero != nil {
		code = 0
		err = ero.Error()

		log.Error("[ZPD Internal] Gety databases error: ", err)
	}

	status := &zpd_internal_proto.Status{
		Code:  int32(code),
		Error: err,
	}

	if code == 1 {
		tbProto = util.MappingTableProto(tb)
	}

	return &zpd_internal_proto.GetTableResponse{
		Table:  tbProto,
		Status: status,
	}, nil
}
