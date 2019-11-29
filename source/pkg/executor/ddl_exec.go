package executor

import (
	"zpd/pkg/util"

	"github.com/xwb1989/sqlparser"
)

// DDLExec DDLExec executor DDL
// create, drop, show table
type DDLExec struct {
	stmt         *sqlparser.DDL
	baseExecutor *BaseExecutor
	Action       string
}

// Next implements the Executor Next interface.
func (ddlExec *DDLExec) Next() (interface{}, error) {
	switch ddlExec.Action {
	case CreateStr:
		return nil, ddlExec.createTable()
	case DropStr:
		return nil, ddlExec.dropTable()
	}
	return nil, nil
}

func (ddlExec *DDLExec) createTable() error {
	table, err := util.BuildTable(ddlExec.stmt)
	if err != nil {
		return err
	}

	return ddlExec.baseExecutor.globalVar.DDLDB.CreateTable(ddlExec.baseExecutor.schema.ID, table)
}

func (ddlExec *DDLExec) dropTable() error {
	return ddlExec.baseExecutor.globalVar.DDLDB.DropTable(ddlExec.baseExecutor.schema.ID, ddlExec.stmt.Table.Name.String())
}
