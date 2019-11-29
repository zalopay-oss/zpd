package executor

import (
	"github.com/xwb1989/sqlparser"
)

// TableIdentExec tableIdent executor
type TableIdentExec struct {
	stmt         *sqlparser.TableIdent
	baseExecutor *BaseExecutor
}

// Next implement Next executor
func (tbIdentExec *TableIdentExec) Next() (interface{}, error) {
	return tbIdentExec.stmt.String(), nil
}
