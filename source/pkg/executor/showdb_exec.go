package executor

import (
	"github.com/xwb1989/sqlparser"
)

// ShowDatabaseExec showdatabase executor
type ShowDatabaseExec struct {
	stmt         *sqlparser.Show
	baseExecutor *BaseExecutor
}

// Next implement Next executor
func (showdbExec *ShowDatabaseExec) Next() (interface{}, error) {
	return showdbExec.baseExecutor.globalVar.DDLDB.GetDatabases()
}
