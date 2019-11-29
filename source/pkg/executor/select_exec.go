package executor

import (
	"bytes"
	"fmt"
	error_zpd "zpd/pkg/error"
	"zpd/pkg/util"

	log "github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
)

// SelectExec insert executor
type SelectExec struct {
	stmt         *sqlparser.Select
	baseExecutor *BaseExecutor
	cols         []string
	tbNames      []string
	where        *Where
}

// Next implement Next executor
func (selectExec *SelectExec) Next() (interface{}, error) {
	tbs, err := selectExec.getTables()
	if err != nil {
		return nil, err
	}

	if len(tbs) == 0 {
		return nil, error_zpd.ErrHaveNotTable
	}

	return selectExec.selectRow(tbs, selectExec.cols)
}

func (selectExec *SelectExec) getTables() ([]*util.Table, error) {
	tbs := make([]*util.Table, len(selectExec.tbNames))

	// get table to check
	for i, tbName := range selectExec.tbNames {
		tb, err := selectExec.baseExecutor.globalVar.DDLDB.GetTable(selectExec.baseExecutor.schema.ID, tbName)
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

func (selectExec *SelectExec) selectRow(tables []*util.Table, cols []string) ([]*util.Row, error) {
	// verify cols
	// update later
	// update return multiple row of multiple table
	for _, tb := range tables {
		return selectExec.selectRowOfTable(tb)
	}

	return nil, nil
}

func (selectExec *SelectExec) checkSelectStar() bool {
	for _, nameCol := range selectExec.cols {
		if nameCol == "*" {
			return true
		}
	}

	return false
}

func (selectExec *SelectExec) genRowKeyMin(TBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%s", mRowPrefix, selectExec.baseExecutor.schema.ID, TBID, minRowID))
}

func (selectExec *SelectExec) genRowKeyMax(TBID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%s", mRowPrefix, selectExec.baseExecutor.schema.ID, TBID, maxRowID))
}

func (selectExec *SelectExec) genRowKey(TBID uint64, RowID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%d", mRowPrefix, selectExec.baseExecutor.schema.ID, TBID, RowID))
}

func (selectExec *SelectExec) genRowKeyIndex(TBID uint64, nameCol string, value []byte) []byte {
	key := []byte(fmt.Sprintf("%s:%d:%d:%s:", mIndexPrefix, selectExec.baseExecutor.schema.ID, TBID, nameCol))

	return append(key, value...)
}

func (selectExec *SelectExec) mappingIndexCol(colTb []*util.Column) map[string]uint64 {
	m := make(map[string]uint64)
	for _, name := range selectExec.cols {
		for _, item := range colTb {
			if item.Name == name {
				m[name] = item.ID
			}
		}
	}

	return m
}

func (selectExec *SelectExec) getIndexColumnInRow(nameCol string, columns []*util.Column) int {
	for i, col := range columns {
		if col.Name == nameCol {
			return i
		}
	}

	return -1
}

func (selectExec *SelectExec) prepareRow(m map[string]uint64, row *util.Row) *util.Row {
	items := make([]*util.Item, len(selectExec.cols))

	for i, name := range selectExec.cols {
		items[i] = row.Items[m[name]]
	}

	return &util.Row{
		ID:    row.ID,
		Items: items,
	}
}

func (selectExec *SelectExec) prepareRows(colTb []*util.Column, rows []*util.Row) []*util.Row {
	rs := make([]*util.Row, len(rows))
	m := selectExec.mappingIndexCol(colTb)

	for i, row := range rows {
		rs[i] = selectExec.prepareRow(m, row)
	}

	return rs
}

func (selectExec *SelectExec) getIndexTable(table *util.Table) (map[string][]byte, []*util.IndexInfo) {
	indexNames := make(map[string][]byte, 0)
	indexInfos := make([]*util.IndexInfo, 0)

	for _, index := range table.Indexes {
		for _, name := range index.Columns {
			if name.Name == selectExec.where.Expr.Left.Name {
				indexNames[name.Name] = selectExec.where.Expr.Right.Val
				indexInfos = append(indexInfos, index.Info)
			}
		}
	}

	return indexNames, indexInfos
}

func (selectExec *SelectExec) handleOperatorEqual(TBID uint64, nameCol string, val []byte) (*util.Row, error) {
	keyIndex := selectExec.genRowKeyIndex(TBID, nameCol, val)

	v, err := selectExec.baseExecutor.dal.Get(selectExec.baseExecutor.ctx, keyIndex)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, error_zpd.ErrRowIsNotExists
	}

	var tmp uint64
	util.Decode(v, &tmp)
	rowID := selectExec.genRowKey(TBID, tmp)
	r, err := selectExec.baseExecutor.dal.Get(selectExec.baseExecutor.ctx, rowID)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, error_zpd.ErrRowIsNotExists
	}

	var row util.Row
	err = util.Decode(r, &row)
	if err != nil {
		log.Error("[SelectExec] Decode Row error: ", err)
		return nil, err
	}

	return &row, nil
}

func (selectExec *SelectExec) handleSelectWhereHaveIndex(table *util.Table, indexNames map[string][]byte) (*util.Row, error) {
	for key, val := range indexNames {
		switch selectExec.where.Expr.Operator {
		case OPERATOR_EQUAL:
			return selectExec.handleOperatorEqual(table.ID, key, val)
		default:
			break
		}
	}

	return nil, nil
}

func (selectExec *SelectExec) handleSelectWhereNoIndex(table *util.Table) ([]*util.Row, error) {
	rows := make([]*util.Row, 0)

	keyMin := selectExec.genRowKeyMin(table.ID)
	keyMax := selectExec.genRowKeyMax(table.ID)
	_, values, err := selectExec.baseExecutor.dal.Scan(selectExec.baseExecutor.ctx, keyMin, keyMax, limit)
	if err != nil {
		log.Error("[SelectExec] Scan error: ", err)
		return nil, err
	}

	index := selectExec.getIndexColumnInRow(selectExec.where.Expr.Left.Name, table.Columns)
	if index == -1 {
		return nil, error_zpd.ErrRowIsNotExists
	}

	for _, value := range values {
		var row util.Row
		err := util.Decode(value, &row)
		if err != nil {
			log.Error("[SelectExec] Decode Row error: ", err)
			return nil, err
		}

		if &row != nil {
			if bytes.Equal(selectExec.where.Expr.Right.Val, row.Items[index].Val) {
				rows = append(rows, &row)
			}
		}
	}

	if len(rows) == 0 {
		return nil, error_zpd.ErrRowIsNotExists
	}

	return rows, nil
}

func (selectExec *SelectExec) handleSelectHaveWhere(table *util.Table) ([]*util.Row, error) {
	rows := make([]*util.Row, 0)

	indexNames, _ := selectExec.getIndexTable(table)
	if len(indexNames) != 0 {
		// return single row
		row, err := selectExec.handleSelectWhereHaveIndex(table, indexNames)
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
	} else {
		rs, err := selectExec.handleSelectWhereNoIndex(table)
		if err != nil {
			return nil, err
		}

		rows = append(rows, rs...)
	}

	return rows, nil
}

func (selectExec *SelectExec) handleSelect(TBID uint64) ([]*util.Row, error) {
	rows := make([]*util.Row, 0)

	keyMin := selectExec.genRowKeyMin(TBID)
	keyMax := selectExec.genRowKeyMax(TBID)
	_, values, err := selectExec.baseExecutor.dal.Scan(selectExec.baseExecutor.ctx, keyMin, keyMax, limit)
	if err != nil {
		log.Error("[SelectExec] Scan error: ", err)
		return nil, err
	}

	for _, value := range values {
		var row util.Row
		err := util.Decode(value, &row)
		if err != nil {
			log.Error("[SelectExec] Decode Row error: ", err)
			return nil, err
		}

		if &row != nil {
			rows = append(rows, &row)
		}
	}

	return rows, nil
}

// SelectRow select row
// update select row with number row more than 10240 later
func (selectExec *SelectExec) selectRowOfTable(table *util.Table) ([]*util.Row, error) {
	// log.Info("[InsertExec] Start select row for table: ", table.ID)
	rows := make([]*util.Row, 0)

	if selectExec.where != nil {
		rs, err := selectExec.handleSelectHaveWhere(table)
		if err != nil {
			return nil, err
		}

		rows = append(rows, rs...)
	} else {
		rs, err := selectExec.handleSelect(table.ID)
		if err != nil {
			return nil, err
		}

		rows = append(rows, rs...)
	}

	checkSelectStar := selectExec.checkSelectStar()

	if checkSelectStar {
		// log.Info("[InsertExec] Select row for table done: ", table.ID)
		return rows, nil
	} else {
		// log.Info("[InsertExec] Select row for table done: ", table.ID)
		return selectExec.prepareRows(table.Columns, rows), nil
	}

	return nil, nil
}
