package executor

import (
	"bytes"
	"fmt"
	error_zpd "zpd/pkg/error"
	"zpd/pkg/util"

	log "github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
)

// DeleteExec delete executor
type DeleteExec struct {
	stmt         *sqlparser.Delete
	baseExecutor *BaseExecutor
	tbNames      []string
	where        *Where
}

// Next implement Next executor
func (deleteExec *DeleteExec) Next() (interface{}, error) {
	tbs, err := deleteExec.getTables()
	if err != nil {
		return nil, err
	}

	if len(tbs) == 0 {
		return nil, error_zpd.ErrHaveNotTable
	}

	return deleteExec.deleteRow(tbs)
}

func (deleteExec *DeleteExec) getTables() ([]*util.Table, error) {
	tbs := make([]*util.Table, len(deleteExec.tbNames))

	// get table to check
	for i, tbName := range deleteExec.tbNames {
		tb, err := deleteExec.baseExecutor.globalVar.DDLDB.GetTable(deleteExec.baseExecutor.schema.ID, tbName)
		if err != nil {
			return nil, err
		}

		if tb == nil {
			return nil, error_zpd.ErrTableNoExists
		}

		tbs[i] = tb
	}

	return tbs, nil
}

func (deleteExec *DeleteExec) deleteRow(tables []*util.Table) (interface{}, error) {
	for _, tb := range tables {
		return deleteExec.deleteRowTable(tb), nil
	}

	return nil, nil
}

func (deleteExec *DeleteExec) deleteRowTable(table *util.Table) error {
	if deleteExec.where != nil {
		deleteExec.deleteRowTableHaveWhere(table)
	}

	return deleteExec.deleteAll(table)
}

func (deleteExec *DeleteExec) getIndexTable(table *util.Table) (map[string][]byte, []*util.IndexInfo) {
	indexNames := make(map[string][]byte, 0)
	indexInfos := make([]*util.IndexInfo, 0)

	for _, index := range table.Indexes {
		for _, name := range index.Columns {
			if name.Name == deleteExec.where.Expr.Left.Name {
				indexNames[name.Name] = deleteExec.where.Expr.Right.Val
				indexInfos = append(indexInfos, index.Info)
			}
		}
	}

	return indexNames, indexInfos
}

// implement later
func (deleteExec *DeleteExec) deleteAll(table *util.Table) error {
	return nil
}

func (deleteExec *DeleteExec) deleteRowTableHaveWhere(table *util.Table) error {
	indexNames, _ := deleteExec.getIndexTable(table)

	if len(indexNames) != 0 {
		return deleteExec.handleDeleteHaveIndex(table, indexNames)
	} else {
		deleteExec.handleDeleteNoIndex(table)
	}

	return nil
}

func (deleteExec *DeleteExec) genRowKeyIndex(TBID uint64, nameCol string, value []byte) []byte {
	key := []byte(fmt.Sprintf("%s:%d:%d:%s:", mIndexPrefix, deleteExec.baseExecutor.schema.ID, TBID, nameCol))

	return append(key, value...)
}

func (deleteExec *DeleteExec) genRowKey(TBID uint64, RowID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%d", mRowPrefix, deleteExec.baseExecutor.schema.ID, TBID, RowID))
}

func (deleteExec *DeleteExec) genRowKeyMin(TBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%s", mRowPrefix, deleteExec.baseExecutor.schema.ID, TBID, minRowID))
}

func (deleteExec *DeleteExec) genRowKeyMax(TBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%s", mRowPrefix, deleteExec.baseExecutor.schema.ID, TBID, maxRowID))
}

func (deleteExec *DeleteExec) handleOperatorEqual(TBID uint64, nameCol string, val []byte) error {
	keyIndex := deleteExec.genRowKeyIndex(TBID, nameCol, val)

	v, err := deleteExec.baseExecutor.dal.Get(deleteExec.baseExecutor.ctx, keyIndex)
	if err != nil {
		return err
	}
	if v == nil {
		return error_zpd.ErrRowIsNotExists
	}

	var tmp uint64
	util.Decode(v, &tmp)
	rowID := deleteExec.genRowKey(TBID, tmp)
	err = deleteExec.baseExecutor.dal.Delete(deleteExec.baseExecutor.ctx, rowID)
	if err != nil {
		return err
	}

	// delete index
	err = deleteExec.baseExecutor.dal.Delete(deleteExec.baseExecutor.ctx, keyIndex)
	if err != nil {
		return err
	}

	return nil
}

func (deleteExec *DeleteExec) handleDeleteHaveIndex(table *util.Table, indexNames map[string][]byte) error {
	for key, val := range indexNames {
		switch deleteExec.where.Expr.Operator {
		case OPERATOR_EQUAL:
			return deleteExec.handleOperatorEqual(table.ID, key, val)
		default:
			break
		}
	}

	return nil
}

func (deleteExec *DeleteExec) getIndexColumnInRow(nameCol string, columns []*util.Column) int {
	for i, col := range columns {
		if col.Name == nameCol {
			return i
		}
	}

	return -1
}

func (deleteExec *DeleteExec) handleDeleteNoIndex(table *util.Table) error {
	keyMin := deleteExec.genRowKeyMin(table.ID)
	keyMax := deleteExec.genRowKeyMax(table.ID)

	keys, values, err := deleteExec.baseExecutor.dal.Scan(deleteExec.baseExecutor.ctx, keyMin, keyMax, limit)
	if err != nil {
		log.Error("[DeleteExec] Scan error: ", err)
		return err
	}

	index := deleteExec.getIndexColumnInRow(deleteExec.where.Expr.Left.Name, table.Columns)
	if index == -1 {
		return error_zpd.ErrRowIsNotExists
	}

	for i, value := range values {
		var row util.Row
		err := util.Decode(value, &row)
		if err != nil {
			log.Error("[DeleteExec] Decode Row error: ", err)
			return err
		}

		if &row != nil {
			if bytes.Equal(deleteExec.where.Expr.Right.Val, row.Items[index].Val) {
				return deleteExec.baseExecutor.dal.Delete(deleteExec.baseExecutor.ctx, keys[i])
			}
		}
	}

	return nil
}
