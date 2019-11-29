package executor

import (
	"github.com/xwb1989/sqlparser"
)

// DBDDLExec DBDDL execute
// create, drop database.
type DBDDLExec struct {
	stmt         *sqlparser.DBDDL
	baseExecutor *BaseExecutor
	Action       string
}

// Next implements the Executor Next interface.
func (dbddlExec *DBDDLExec) Next() (interface{}, error) {
	switch dbddlExec.Action {
	case CreateStr:
		return nil, dbddlExec.createDatabase()
	case DropStr:
		return nil, dbddlExec.dropDatabase()
	}

	return nil, nil
}

func (dbddlExec *DBDDLExec) createDatabase() error {
	return dbddlExec.baseExecutor.globalVar.DDLDB.CreateDatabase(dbddlExec.stmt.DBName)
}

func (dbddlExec *DBDDLExec) dropDatabase() error {
	return dbddlExec.baseExecutor.globalVar.DDLDB.DropDatabase(dbddlExec.stmt.DBName)
}
