package executor

import (
	"github.com/xwb1989/sqlparser"
)

// ShowTableExec showdatabase executor
type ShowTableExec struct {
	stmt         *sqlparser.Show
	baseExecutor *BaseExecutor
}

// Next implement Next executor
func (showTBExec *ShowTableExec) Next() (interface{}, error) {
	dbName := ""
	if showTBExec.baseExecutor.schema != nil && showTBExec.stmt.ShowTablesOpt.DbName == "" {
		dbName = showTBExec.baseExecutor.schema.DBName
	} else {
		dbName = showTBExec.stmt.ShowTablesOpt.DbName
	}

	return showTBExec.baseExecutor.globalVar.DDLDB.GetNameTablesOfDatabse(dbName)
}
