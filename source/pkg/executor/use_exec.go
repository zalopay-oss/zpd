package executor

import (
	"github.com/xwb1989/sqlparser"
)

// UseExec executor execute sql USE DB
type UseExec struct {
	stmt         *sqlparser.Use
	baseExecutor *BaseExecutor
}

// Next implement Next executor
func (useExec *UseExec) Next() (interface{}, error) {
	dbName, err := useExec.baseExecutor.childrenExec[0].Next()
	if err != nil {
		return nil, err
	}

	return useExec.baseExecutor.globalVar.DDLDB.GetSchema(dbName.(string))
}
