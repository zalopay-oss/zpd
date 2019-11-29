package conn

import (
	"context"
	"sync"
	"zpd/configs"
	"zpd/pkg/dal"
	"zpd/pkg/executor"
	"zpd/pkg/global"
	"zpd/pkg/parser"
	"zpd/pkg/util"

	zpd_proto "zpd/pkg/public-api"

	error_zpd "zpd/pkg/error"

	log "github.com/sirupsen/logrus"
)

const (
	DBNAME_DEFAULT = "*"
)

//SessionImpl struct
type SessionImpl struct {
	parser          parser.Parser
	builderExecutor *executor.BuilderExecutor
	dal             dal.DataAccessLayer
	DBName          string
	Schema          *util.Schema
	ctx             context.Context
	mux             sync.RWMutex
	generate        util.Generate
}

//newSession create session
func newSession(ctx context.Context, globalVar *global.GlobalVar, dbName string, config *configs.ZPDServiceConfig) (Session, error) {
	var mux sync.RWMutex
	dal, err := dal.NewDataAccessLayer(ctx, config.Database)
	if err != nil {
		return nil, err
	}

	parser := parser.NewParser()

	builderExecutor, err := executor.NewBuilderExecutor(dal, nil, mux, globalVar, config.ID)
	if err != nil {
		return nil, err
	}

	if dbName == DBNAME_DEFAULT {
		// log.Info("[Session] New session with dbName: ", dbName)

		return &SessionImpl{
			parser:          parser,
			dal:             dal,
			builderExecutor: builderExecutor,
			DBName:          dbName,
			ctx:             ctx,
			generate:        util.NewGenerate(),
			mux:             mux,
		}, nil
	}

	// get schema from cache
	schema, err := globalVar.DDLDB.GetSchema(dbName)
	if err != nil {
		log.Error("[Session] New session with: ", dbName, " err ", err.Error())

		return &SessionImpl{
			parser:          parser,
			dal:             dal,
			builderExecutor: builderExecutor,
			DBName:          DBNAME_DEFAULT,
			ctx:             ctx,
			generate:        util.NewGenerate(),
			mux:             mux,
		}, err
	}

	builderExecutor.Schema = schema

	return &SessionImpl{
		parser:          parser,
		dal:             dal,
		builderExecutor: builderExecutor,
		DBName:          dbName,
		ctx:             ctx,
		Schema:          schema,
		generate:        util.NewGenerate(),
		mux:             mux,
	}, nil
}

func (ss *SessionImpl) close() error {
	return ss.dal.DisconnectStorage()
}

// execute handle sql
func (ss *SessionImpl) execute(ctx context.Context, sql string) ([]byte, error) {
	// parse SQL statement to AST
	stmt, err := ss.parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	// build executor
	exec, err := ss.builderExecutor.Build(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if exec == nil {
		return nil, error_zpd.ErrBuildExecutorFail
	}

	return ss.next(exec)
}

func (ss *SessionImpl) next(exec executor.Executor) ([]byte, error) {
	// executor execute
	result, err := exec.Next()
	if err == nil {
		return ss.handleResult(exec, result)
	}

	return nil, err
}

func (ss *SessionImpl) handleResult(exec executor.Executor, data interface{}) ([]byte, error) {
	if data == nil {
		return nil, nil
	}

	switch exec.(type) {
	case *executor.DBDDLExec:
		if (exec.(*executor.DBDDLExec).Action == executor.DropStr) && (data.(*util.Schema).ID == ss.Schema.ID) {
			ss.updateSchema(nil)
		}

		return nil, nil
	case *executor.UseExec:
		ss.updateSchema(data.(*util.Schema))

		return nil, nil
	case *executor.ShowDatabaseExec:
		data := &zpd_proto.Databases{
			Databases: data.([]string),
		}

		return ss.generate.EncodeDataProto(data)
	case *executor.ShowTableExec:
		data := &zpd_proto.NameTables{
			Nametables: data.([]string),
		}

		return ss.generate.EncodeDataProto(data)
	case *executor.SelectExec:
		data := util.MappingRowsProto(data.([]*util.Row))

		return ss.generate.EncodeDataProto(data)
	}

	return nil, nil
}

func (ss *SessionImpl) updateSchema(schema *util.Schema) {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	if schema == nil {
		log.Info("[SessionImpl] Update schema with dbName: *")

		ss.DBName = "*"
		ss.Schema = nil
		ss.builderExecutor.Schema = nil
	} else {
		log.Info("[SessionImpl] Update schema with dbName: ", schema.DBName)

		ss.DBName = schema.DBName
		ss.Schema = schema
		ss.builderExecutor.Schema = schema
	}
}

func (ss *SessionImpl) getSchema() *util.Schema {
	ss.mux.RLock()
	defer ss.mux.RUnlock()

	return ss.Schema
}

func (ss *SessionImpl) updateTableToSchema(table *util.Table) {
	ss.mux.Lock()
	defer ss.mux.Unlock()

	if ss.Schema != nil {
		ss.Schema.Tables = append(ss.Schema.Tables, table)
	}
}
