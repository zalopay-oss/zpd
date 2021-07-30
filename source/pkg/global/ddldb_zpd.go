package global

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"zpd/configs"
	"zpd/pkg/bridge"
	consul_agent "zpd/pkg/consul-agent"
	"zpd/pkg/dal"
	error_zpd "zpd/pkg/error"
	"zpd/pkg/util"

	log "github.com/sirupsen/logrus"
)

var (
	mDBPrefix    = "DB"
	mDBIDMax     = []byte("DB:99999999999999999999")
	mDBIDMin     = []byte("DB:0")
	mNextDBIDKey = []byte("nextDBID")

	mTBPrefix    = "TB"
	minTBID      = "0"
	maxTBID      = "99999999999999999999"
	mAllTBIDMax  = []byte("TB:99999999999999999999:99999999999999999999")
	mAllTBIDMin  = []byte("DB:0:0")
	mNextTBIDKey = "nextTBID"
	limitScan    = 10240
	maxInt64     = 10240
)

// DDLDBImpl handle statments related to schema
type DDLDBImpl struct {
	managerClient bridge.ManagerClient
	consulAgent   consul_agent.ConsulAgent
	dal           dal.DataAccessLayer
	ctx           context.Context
	mux           sync.RWMutex
	muxLeader     sync.RWMutex
	leader        bool
}

// NewDDLDB new DDL struct
func NewDDLDB(consulAgent consul_agent.ConsulAgent, config []*configs.Database, managerClient bridge.ManagerClient) (DDLDB, error) {
	ctx := context.Background()
	dalDDLDB, err := dal.NewDataAccessLayer(ctx, config)
	if err != nil {
		return nil, err
	}

	ddlDBImpl := &DDLDBImpl{
		managerClient: managerClient,
		dal:           dalDDLDB,
		ctx:           ctx,
		consulAgent:   consulAgent,
	}

	// If it is leader, loading cache
	isLeader, err := consulAgent.AcquireSession()
	if err != nil {
		return nil, err
	}

	ddlDBImpl.leader = isLeader

	return ddlDBImpl, nil
}

func (ddl *DDLDBImpl) getStateLeader() bool {
	ddl.muxLeader.RLock()
	defer ddl.muxLeader.RUnlock()

	return ddl.leader
}

func (ddl *DDLDBImpl) updateStateLeader(state bool) {
	ddl.muxLeader.Lock()
	defer ddl.muxLeader.Unlock()

	if ddl.leader != state {
		log.Info("[DDLDBImpl] Update state leader: ", state)
		ddl.leader = state
	}
}

func (ddl *DDLDBImpl) checkLeader() error {
	isLeader, err := ddl.consulAgent.AcquireSession()
	if err != nil {
		return err
	}

	if ddl.getStateLeader() != isLeader {
		ddl.updateStateLeader(isLeader)
	}

	return nil
}

// Close close DDLDBImpl
func (ddl *DDLDBImpl) Close() error {
	return ddl.dal.DisconnectStorage()
}

// genDBKey gen DB key
func (ddl *DDLDBImpl) genDBKey(DBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, DBID))
}

// gen TB key
func (ddl *DDLDBImpl) genTBKey(DBID uint64, TBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d", mTBPrefix, DBID, TBID))
}

func (ddl *DDLDBImpl) genTBKeyMin(DBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%s", mTBPrefix, DBID, minTBID))
}

func (ddl *DDLDBImpl) genTBKeyMax(DBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%s", mTBPrefix, DBID, maxTBID))
}

func (ddl *DDLDBImpl) genNextIDTBKey(DBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mNextTBIDKey, DBID))
}

// CreateSchema implement CreateSchema of interface DDL
// needing use mux.lock
func (ddl *DDLDBImpl) buildSchema(dbName string) (*util.Schema, error) {
	// lowercase dbname
	lDBName := strings.ToLower(dbName)

	// gen GlobalID
	id, err := ddl.genGlobalSchemaID()
	if err != nil {
		return nil, err
	}

	return &util.Schema{
		ID:     id,
		DBName: lDBName,
		Tables: nil,
	}, nil
}

// needing use mux.lock
func (ddl *DDLDBImpl) loadDatabaseID() (uint64, error) {
	var DBID uint64
	// read metadata DBID from TiKV
	curDBID, err := ddl.dal.Get(ddl.ctx, mNextDBIDKey)
	if err != nil {
		return 1, err
	}

	if curDBID == nil {
		DBID = 1
		if err := ddl.dal.Put(ddl.ctx, mNextDBIDKey, util.ConvertUint64ToBytes(DBID)); err != nil {
			return 1, err
		}
	} else {
		DBID = util.ConvertBytesToUint64(curDBID)
	}

	return DBID, nil
}

// needing use mux.lock
func (ddl *DDLDBImpl) getSchemas() (map[string]*util.Schema, error) {
	var count int
	currID, err := ddl.loadDatabaseID()
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]*util.Schema)

	if int(currID) < limitScan {
		count = int(currID)
	} else {
		count = limitScan
	}

	for {
		_, values, err := ddl.dal.Scan(ddl.ctx, mDBIDMin, mDBIDMax, count)
		if err != nil {
			log.Error("[DDLDBImpl] Scan error: ", err)
			return nil, err
		}

		for _, value := range values {
			var schema util.Schema
			err := util.Decode(value, &schema)
			if err != nil {
				log.Error("[DDLDBImpl] Decode Schema error: ", err)
				return nil, err
			}

			schemas[schema.DBName] = &schema
		}

		currID = currID - uint64(count)
		if currID <= 0 {
			break
		}
	}

	return schemas, nil
}

// needing use mux.lock
func (ddl *DDLDBImpl) getDatabases() ([]string, error) {
	var count int
	currID, err := ddl.loadDatabaseID()
	if err != nil {
		return nil, err
	}

	schemas := make([]string, 0)

	if int(currID) < limitScan {
		count = int(currID)
	} else {
		count = limitScan
	}

	for {
		_, values, err := ddl.dal.Scan(ddl.ctx, mDBIDMin, mDBIDMax, count)
		if err != nil {
			log.Error("[DDLDBImpl] Scan error: ", err)
			return nil, err
		}

		for _, value := range values {
			var schema util.Schema
			err := util.Decode(value, &schema)
			if err != nil {
				log.Error("[DDLDBImpl] Decode Schema error: ", err)
				return nil, err
			}

			schemas = append(schemas, schema.DBName)
		}

		currID = currID - uint64(count)
		if currID <= 0 {
			break
		}
	}

	return schemas, nil
}

// GenGlobalSchemaID gen database id
// needing use mux.lock
func (ddl *DDLDBImpl) genGlobalSchemaID() (uint64, error) {
	DBID, err := ddl.loadDatabaseID()
	if err != nil {
		return 0, err
	}

	currDBID := DBID + 1
	if err := ddl.dal.Put(ddl.ctx, mNextDBIDKey, util.ConvertUint64ToBytes(currDBID)); err != nil {
		return 0, err
	}

	return currDBID, nil
}

// checkDatabaseExists check Database exists
// needing use mux.lock
func (ddl *DDLDBImpl) checkDatabaseExists(dbName string) (bool, error) {
	schemas, err := ddl.getSchemas()
	if err != nil {
		return false, err
	}

	schema := schemas[dbName]
	if schema == nil {
		return false, nil
	}

	return true, nil
}

// needing use mux.lock
func (ddl *DDLDBImpl) getSchema(dbName string) (*util.Schema, error) {
	schemas, err := ddl.getSchemas()
	if err != nil {
		return nil, err
	}

	schema := schemas[dbName]
	if schema == nil {
		return nil, error_zpd.ErrSchemaNoExists
	}

	return schema, nil
}

// GetSchema get schema
func (ddl *DDLDBImpl) GetSchema(dbName string) (*util.Schema, error) {
	err := ddl.checkLeader()
	if err != nil {
		return nil, err
	}

	if ddl.getStateLeader() {
		ddl.mux.RLock()
		defer ddl.mux.RUnlock()

		schema, err := ddl.getSchema(dbName)
		if err != nil {
			return nil, error_zpd.ErrSchemaNoExists
		}

		tables, err := ddl.getTablesOfDatabaseToArray(schema.ID)
		if err != nil {
			return nil, err
		}

		schema.Tables = tables
		return schema, nil
	}

	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return nil, err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return nil, err
	}

	return client.GetSchema(dbName)
}

func (ddl *DDLDBImpl) createDatabase(dbName string) error {
	ddl.mux.Lock()
	defer ddl.mux.Unlock()

	check, err := ddl.checkDatabaseExists(dbName)
	if err != nil {
		return err
	}

	if check {
		log.Error("[DDLDBImpl] Create database " + dbName + " is exists")
		return error_zpd.ErrDBNExists
	}

	// create schema
	schema, err := ddl.buildSchema(dbName)
	if err != nil {
		return err
	}

	// encode data
	data, err := util.Encode(schema)
	if err != nil {
		return err
	}

	// gen key
	dbKey := ddl.genDBKey(schema.ID)

	// put TiKV
	err = ddl.dal.Put(ddl.ctx, dbKey, data)
	if err != nil {
		log.Error("[DDLDBImpl] Put schema "+dbName+" to TiKV error: ", err)
		return err
	}

	keyNextTBID := ddl.genNextIDTBKey(schema.ID)

	err = ddl.dal.Put(ddl.ctx, keyNextTBID, util.ConvertUint64ToBytes(uint64(0)))
	if err != nil {
		log.Error("[DDLDBImpl] Put KeyNextTBID of "+dbName+" to TiKV error: ", err)
		return err
	}

	return nil
}

// CreateDatabase create database
func (ddl *DDLDBImpl) CreateDatabase(dbName string) error {
	err := ddl.checkLeader()
	if err != nil {
		return err
	}

	if ddl.getStateLeader() {
		return ddl.createDatabase(dbName)
	}

	// call api
	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return err
	}

	return client.CreateDatabase(dbName)
}

// update drop database: drop table, data table
func (ddl *DDLDBImpl) dropDatabase(dbName string) error {
	ddl.mux.Lock()
	defer ddl.mux.Unlock()
	// log.Info("[DDLDBImpl] Start drop database with name ", dbName)

	schema, err := ddl.getSchema(dbName)
	if err != nil {
		return err
	}

	if schema == nil {
		log.Error("[DDLDBImpl] Drop database " + dbName + " is not exists")
		return error_zpd.ErrSchemaNoExists
	}

	// gen key
	dbKey := ddl.genDBKey(schema.ID)

	// delete TiKV
	err = ddl.dal.Delete(ddl.ctx, dbKey)
	if err != nil {
		log.Error("[DDLDBImpl] Drop database "+dbName+" to TiKV error: ", err)
		return err
	}

	// log.Info("[DDLDBImpl] Drop database with name " + dbName + " done")

	return nil
}

// DropDatabase drop database
func (ddl *DDLDBImpl) DropDatabase(dbName string) error {
	err := ddl.checkLeader()
	if err != nil {
		return err
	}

	if ddl.getStateLeader() {
		return ddl.dropDatabase(dbName)
	}

	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return err
	}

	log.Info("[DDLDBImpl] Call api DropDatabase to node leader with host: ", addressLeader)

	return client.DropDatabase(dbName)
}

// GetDatabases get name list database
func (ddl *DDLDBImpl) GetDatabases() ([]string, error) {
	err := ddl.checkLeader()
	if err != nil {
		return nil, err
	}

	if ddl.getStateLeader() {
		ddl.mux.RLock()
		defer ddl.mux.RUnlock()

		return ddl.getDatabases()
	}

	// call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return nil, err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return nil, err
	}

	// log.Info("[DDLDBImpl] Call api GetDatabases to node leader with host: ", addressLeader)

	return client.GetDatabases()
}

// needing use mux.lock
func (ddl *DDLDBImpl) loadTableID(DBID uint64) (uint64, error) {
	nextIDTBKey := ddl.genNextIDTBKey(DBID)
	curTBID, err := ddl.dal.Get(ddl.ctx, nextIDTBKey)
	if err != nil {
		return 0, err
	}

	if curTBID == nil {
		DBID = 0
		if err := ddl.dal.Put(ddl.ctx, nextIDTBKey, util.ConvertUint64ToBytes(DBID)); err != nil {
			return 0, err
		}
	} else {
		DBID = util.ConvertBytesToUint64(curTBID)
	}

	return DBID, nil
}

func (ddl *DDLDBImpl) genGlobalTBID(DBID uint64) (uint64, error) {
	nextIDTBKey := ddl.genNextIDTBKey(DBID)

	TBID, err := ddl.loadTableID(DBID)
	if err != nil {
		return 0, err
	}

	currTBID := TBID + 1
	if err := ddl.dal.Put(ddl.ctx, nextIDTBKey, util.ConvertUint64ToBytes(currTBID)); err != nil {
		return 0, err
	}

	return currTBID, nil
}

func (ddl *DDLDBImpl) getTablesOfDatabase(DBID uint64) (map[string]*util.Table, error) {
	var count int

	currID, err := ddl.loadTableID(DBID)
	if err != nil {
		return nil, err
	}

	tables := make(map[string]*util.Table)

	if int(currID) < limitScan {
		count = int(currID)
	} else {
		count = limitScan
	}

	for {
		_, values, err := ddl.dal.Scan(ddl.ctx, ddl.genTBKeyMin(DBID), ddl.genTBKeyMax(DBID), count)
		if err != nil {
			log.Error("[DDLDBImpl] Scan error: ", err)
			return nil, err
		}

		for _, value := range values {
			var table util.Table
			err := util.Decode(value, &table)
			if err != nil {
				log.Error("[DDLDBImpl] Decode Table error: ", err)
				return nil, err
			}

			tables[table.TBName] = &table
		}

		currID = currID - uint64(count)
		if currID <= 0 {
			break
		}
	}

	return tables, nil
}

func (ddl *DDLDBImpl) getTablesOfDatabaseToArray(DBID uint64) ([]*util.Table, error) {
	var count int

	currID, err := ddl.loadTableID(DBID)
	if err != nil {
		return nil, err
	}

	tables := make([]*util.Table, 0)

	if int(currID) < limitScan {
		count = int(currID)
	} else {
		count = limitScan
	}

	for {
		_, values, err := ddl.dal.Scan(ddl.ctx, ddl.genTBKeyMin(DBID), ddl.genTBKeyMax(DBID), count)
		if err != nil {
			log.Error("[DDLDBImpl] Scan error: ", err)
			return nil, err
		}

		for _, value := range values {
			var table util.Table
			err := util.Decode(value, &table)
			if err != nil {
				log.Error("[DDLDBImpl] Decode Table error: ", err)
				return nil, err
			}

			tables = append(tables, &table)
		}

		currID = currID - uint64(count)
		if currID <= 0 {
			break
		}
	}

	return tables, nil
}

func (ddl *DDLDBImpl) getNameTablesOfDatabase(DBID uint64) ([]string, error) {
	var count int

	currID, err := ddl.loadTableID(DBID)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)

	if int(currID) < limitScan {
		count = int(currID)
	} else {
		count = limitScan
	}

	for {
		_, values, err := ddl.dal.Scan(ddl.ctx, ddl.genTBKeyMin(DBID), ddl.genTBKeyMax(DBID), count)
		if err != nil {
			log.Error("[DDLDBImpl] Scan error: ", err)
			return nil, err
		}

		for _, value := range values {
			var table util.Table
			err := util.Decode(value, &table)
			if err != nil {
				log.Error("[DDLDBImpl] Decode Table error: ", err)
				return nil, err
			}

			tables = append(tables, table.TBName)
		}

		currID = currID - uint64(count)
		if currID <= 0 {
			break
		}
	}

	return tables, nil
}

func (ddl *DDLDBImpl) getTable(DBID uint64, tbName string) (*util.Table, error) {
	tbs, err := ddl.getTablesOfDatabase(DBID)
	if err != nil {
		return nil, err
	}

	tb, exists := tbs[tbName]
	if !exists {
		return nil, error_zpd.ErrTableNoExists
	}

	return tb, nil
}

// handle later
func (ddl *DDLDBImpl) getAllTablesOfService() (map[string]*util.Table, error) {
	tables := make(map[string]*util.Table)

	_, values, err := ddl.dal.Scan(ddl.ctx, mAllTBIDMin, mAllTBIDMax, maxInt64)
	if err != nil {
		log.Error("[DDLDBImpl] Scan error: ", err)
		return nil, err
	}

	for _, value := range values {
		var table util.Table
		err := util.Decode(value, &table)
		if err != nil {
			log.Error("[DDLDBImpl] Decode Table error: ", err)
			return nil, err
		}

		tables[table.TBName] = &table
	}

	return tables, nil
}

func (ddl *DDLDBImpl) checkTableExists(DBID uint64, tbName string) (bool, error) {
	tables, err := ddl.getTablesOfDatabase(DBID)
	if err != nil {
		return false, err
	}

	table := tables[tbName]
	if table == nil {
		return false, nil
	}

	return true, nil
}

func (ddl *DDLDBImpl) createTable(DBID uint64, infoTable *util.Table) error {
	ddl.mux.Lock()
	defer ddl.mux.Unlock()
	// log.Info("[DDLDBImpl] Start create table with name ", infoTable.TBName)

	check, err := ddl.checkTableExists(DBID, infoTable.TBName)
	if err != nil {
		return err
	}

	if check {
		log.Error("[DDLDBImpl] Create database " + infoTable.TBName + " is exists")
		return error_zpd.ErrTBNNExists
	}

	// gen TBID
	ID, err := ddl.genGlobalTBID(DBID)
	if err != nil {
		return err
	}
	infoTable.ID = ID

	// encode data
	data, err := util.Encode(infoTable)
	if err != nil {
		return err
	}

	// gen key
	dbKey := ddl.genTBKey(DBID, infoTable.ID)

	// put TiKV
	err = ddl.dal.Put(ddl.ctx, dbKey, data)
	if err != nil {
		log.Error("[DDLDBImpl] Put table "+infoTable.TBName+" to TiKV error: ", err)
		return err
	}

	// log.Info("[DDLDBImpl] Create table with name " + infoTable.TBName + " done")
	return nil
}

// CreateTable create table
func (ddl *DDLDBImpl) CreateTable(DBID uint64, infoTable *util.Table) error {
	err := ddl.checkLeader()
	if err != nil {
		return err
	}

	if ddl.getStateLeader() {
		return ddl.createTable(DBID, infoTable)
	}
	// call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return err
	}

	// log.Info("[DDLDBImpl] Call api CreateTable to node leader with host: ", addressLeader)

	return client.CreateTable(DBID, infoTable)
}

// GetNameTablesOfDatabse get list name table of database
func (ddl *DDLDBImpl) GetNameTablesOfDatabse(dbName string) ([]string, error) {
	err := ddl.checkLeader()
	if err != nil {
		return nil, err
	}

	if ddl.getStateLeader() {
		ddl.mux.RLock()
		defer ddl.mux.RUnlock()

		schema, err := ddl.getSchema(dbName)
		if err != nil {
			return nil, error_zpd.ErrSchemaNoExists
		}

		return ddl.getNameTablesOfDatabase(schema.ID)
	}

	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return nil, err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return nil, err
	}

	// log.Info("[DDLDBImpl] Call api GetTable to node leader with host: ", addressLeader)

	return client.GetNameTables(dbName)
}

// update drop database: drop table, data table
func (ddl *DDLDBImpl) dropTable(DBID uint64, tbName string) error {
	ddl.mux.Lock()
	defer ddl.mux.Unlock()
	// log.Info("[DDLDBImpl] Start drop table with name ", tbName)

	tb, err := ddl.getTable(DBID, tbName)
	if err != nil {
		return err
	}

	if tb == nil {
		log.Error("[DDLDBImpl] Drop table " + tbName + " is not exists")
		return error_zpd.ErrTableNoExists
	}

	// gen key
	dbKey := ddl.genTBKey(DBID, tb.ID)

	// delete TiKV
	err = ddl.dal.Delete(ddl.ctx, dbKey)
	if err != nil {
		log.Error("[DDLDBImpl] Drop table "+tbName+" to TiKV error: ", err)
		return err
	}

	// log.Info("[DDLDBImpl] Drop Table with name " + tbName + " done")

	return nil
}

// DropTable drop table
// update drop rows of table late
func (ddl *DDLDBImpl) DropTable(DBID uint64, tbName string) error {
	err := ddl.checkLeader()
	if err != nil {
		return err
	}

	if ddl.getStateLeader() {
		return ddl.dropTable(DBID, tbName)
	}

	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return err
	}

	// log.Info("[DDLDBImpl] Call api DropTable to node leader with host: ", addressLeader)

	return client.DropTable(DBID, tbName)
}

// GetTable get table
func (ddl *DDLDBImpl) GetTable(DBID uint64, tbName string) (*util.Table, error) {
	err := ddl.checkLeader()
	if err != nil {
		return nil, err
	}

	if ddl.getStateLeader() {
		ddl.mux.Lock()
		defer ddl.mux.Unlock()

		return ddl.getTable(DBID, tbName)
	}

	//call api to node leader
	addressLeader, err := ddl.consulAgent.GetAddressLeader()
	if err != nil {
		return nil, err
	}

	client, err := ddl.managerClient.GetZPDClient(addressLeader)
	if err != nil {
		return nil, err
	}

	// log.Info("[DDLDBImpl] Call api DropTable to node leader with host: ", addressLeader)

	return client.GetTable(DBID, tbName)
}
