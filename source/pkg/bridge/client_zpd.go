package bridge

import (
	"context"
	"errors"
	zpd_internal_proto "zpd/pkg/internal-api"
	"zpd/pkg/util"

	error_zpd "zpd/pkg/error"

	grpcpool "github.com/processout/grpc-go-pool"
	log "github.com/sirupsen/logrus"
)

// ZPDClient interface ZPDClient
type ZPDClient interface {
	GetSchema(dbName string) (*util.Schema, error)
	CreateDatabase(dbName string) error
	DropDatabase(dbName string) error
	GetDatabases() ([]string, error)

	CreateTable(DBID uint64, table *util.Table) error
	GetNameTables(dbName string) ([]string, error)
	DropTable(DBID uint64, tbName string) error
	GetTable(DBID uint64, tbNname string) (*util.Table, error)
}

// ZPDClientImpl client wrapper ZPDInternalServiceClient
type ZPDClientImpl struct {
	client zpd_internal_proto.ZPDInternalServiceClient
	conn   *grpcpool.ClientConn
	ctx    context.Context
}

// Close close conn
func (zpdClient *ZPDClientImpl) close() error {
	return zpdClient.conn.Close()
}

// GetSchema get schema
// update field table
func (zpdClient *ZPDClientImpl) GetSchema(dbName string) (*util.Schema, error) {
	schemaReq := &zpd_internal_proto.SchemaRequest{
		Dbname: dbName,
	}

	res, err := zpdClient.client.GetSchema(zpdClient.ctx, schemaReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Get schema " + dbName + " error: " + err.Error())

		return nil, error_zpd.ErrServerDoesNotReady
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		return nil, errors.New(res.Status.Error)
	}

	return util.MappingSchema(res.Schema), nil
}

// CreateDatabase create database
func (zpdClient *ZPDClientImpl) CreateDatabase(dbName string) error {
	createDBReq := &zpd_internal_proto.CreateDatabaseRequest{
		Dbname: dbName,
	}

	res, err := zpdClient.client.CreateDatabase(zpdClient.ctx, createDBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Create database " + dbName + " error: " + err.Error())

		return error_zpd.ErrServerDoesNotReady
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Create database " + dbName + " error: " + res.Status.Error)

		return errors.New(res.Status.Error)
	}

	return nil
}

// DropDatabase create database
func (zpdClient *ZPDClientImpl) DropDatabase(dbName string) error {
	dropDBReq := &zpd_internal_proto.DropDatabaseRequest{
		Dbname: dbName,
	}

	res, err := zpdClient.client.DropDatabase(zpdClient.ctx, dropDBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Drop database " + dbName + " error: " + err.Error())

		return error_zpd.ErrServerDoesNotReady
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Drop database " + dbName + " error: " + res.Status.Error)

		return errors.New(res.Status.Error)
	}

	return nil
}

// GetDatabases create database
func (zpdClient *ZPDClientImpl) GetDatabases() ([]string, error) {
	getDBReq := &zpd_internal_proto.GetDatabasesRequest{}

	res, err := zpdClient.client.GetDatabases(zpdClient.ctx, getDBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Get databases  error: " + err.Error())

		return nil, error_zpd.ErrServerDoesNotReady
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Get databases error: " + res.Status.Error)

		return nil, errors.New(res.Status.Error)
	}

	return res.Databases, nil
}

// CreateTable create table
func (zpdClient *ZPDClientImpl) CreateTable(DBID uint64, table *util.Table) error {
	tableProto := util.MappingTableProto(table)

	createTBReq := &zpd_internal_proto.CreateTableRequest{
		DBID:  DBID,
		Table: tableProto,
	}

	res, err := zpdClient.client.CreateTable(zpdClient.ctx, createTBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Create table  error: " + err.Error())

		return err
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Create table " + table.TBName + " error: " + res.Status.Error)

		return errors.New(res.Status.Error)
	}

	return nil
}

// GetNameTables get name tables
func (zpdClient *ZPDClientImpl) GetNameTables(dbName string) ([]string, error) {
	getNameTBReq := &zpd_internal_proto.GetNameTablesRequest{
		Dbname: dbName,
	}

	res, err := zpdClient.client.GetNameTables(zpdClient.ctx, getNameTBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Get name tables of DB " + dbName + " error: " + err.Error())

		return nil, err
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Get name tables of DB " + dbName + " error: " + res.Status.Error)

		return nil, errors.New(res.Status.Error)
	}

	return res.Nametables, nil
}

// DropTable drop table
func (zpdClient *ZPDClientImpl) DropTable(DBID uint64, tbName string) error {
	dropTBReq := &zpd_internal_proto.DropTableRequest{
		DBID:   DBID,
		Tbname: tbName,
	}

	res, err := zpdClient.client.DropTable(zpdClient.ctx, dropTBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Drop tables of DB " + tbName + " error: " + err.Error())

		return err
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Get name tables of DB " + tbName + " error: " + res.Status.Error)

		return errors.New(res.Status.Error)
	}

	return nil
}

// GetTable get table
func (zpdClient *ZPDClientImpl) GetTable(DBID uint64, tbName string) (*util.Table, error) {
	getTBReq := &zpd_internal_proto.GetTableRequest{
		DBID:   DBID,
		Tbname: tbName,
	}

	res, err := zpdClient.client.GetTable(zpdClient.ctx, getTBReq)
	if err != nil {
		log.Error("[ZPDClientImpl] Drop tables of DB " + tbName + " error: " + err.Error())

		return nil, err
	}
	zpdClient.close()

	if res.Status.Code == 0 {
		log.Error("[ZPDClientImpl] Get name tables of DB " + tbName + " error: " + res.Status.Error)

		return nil, errors.New(res.Status.Error)
	}

	return util.MappingTable(res.Table), nil
}
