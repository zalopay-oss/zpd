package executor

import (
	"fmt"
	"strings"
	"zpd/pkg/util"

	error_zpd "zpd/pkg/error"

	log "github.com/sirupsen/logrus"

	"github.com/xwb1989/sqlparser"
)

// InsertExec insert executor
type InsertExec struct {
	stmt         *sqlparser.Insert
	baseExecutor *BaseExecutor
	Action       string
	Columns      []string
	Rows         []*util.Row
}

// Next implement Next executor
func (insertExec *InsertExec) Next() (interface{}, error) {
	switch insertExec.Action {
	case InsertStr:
		return insertExec.insertRow()
	case ReplaceStr:
		return nil, nil
	}

	return nil, nil
}

func (insertExec *InsertExec) genRowKey(TBID uint64, rowID uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d:%d:%d", mRowPrefix, insertExec.baseExecutor.schema.ID, TBID, rowID))
}

func (insertExec *InsertExec) genKeyIndex(TBID uint64, nameCol string, value []byte) []byte {
	key := []byte(fmt.Sprintf("%s:%d:%d:%s:", mIndexPrefix, insertExec.baseExecutor.schema.ID, TBID, nameCol))

	return append(key, value...)
}

func (insertExec *InsertExec) getIndexColumnInRow(nameCol string, columns []*util.Column) int {
	for i, col := range columns {
		if strings.ToLower(nameCol) == col.Name {
			return i
		}
	}

	return -1
}

func (insertExec *InsertExec) getValueColumnInRow(nameCol string, row *util.Row) []byte {
	for i, name := range insertExec.Columns {
		if name == nameCol {
			return row.Items[i].Val
		}
	}

	return nil
}

func (insertExec *InsertExec) verifyTypeColumn(column *util.Column, item *util.Item) error {
	if column.Type.Type != item.Type {
		return error_zpd.ErrDifferentTypeColumn
	}

	//verify lenght update later
	return nil
}

func (insertExec *InsertExec) verifyNotNull(columns []*util.Column, items []*util.Item) error {
	for i, col := range columns {
		if col.Type.NotNull == true && items[i] == nil {
			return error_zpd.ErrNotNull
		}
	}

	return nil
}

func (insertExec *InsertExec) verifyRow(table *util.Table, items []*util.Item) error {
	// implement more verify row later
	return insertExec.verifyNotNull(table.Columns, items)
}

func (insertExec *InsertExec) prepareRow(table *util.Table, row *util.Row) (*util.Row, error) {
	items := make([]*util.Item, len(table.Columns))

	for i, nameCol := range insertExec.Columns {
		index := insertExec.getIndexColumnInRow(nameCol, table.Columns)
		if index == -1 {
			return nil, error_zpd.ErrColumnNotExist
		} else {
			err := insertExec.verifyTypeColumn(table.Columns[index], row.Items[i])
			if err != nil {
				return nil, err
			}

			items[index] = row.Items[i]
		}
	}

	err := insertExec.verifyRow(table, items)
	if err != nil {
		return nil, err
	}

	return &util.Row{
		Items: items,
	}, nil
}

func (insertExec *InsertExec) prepareIndexs(TBID uint64, rowID uint64, indexs []*util.Index, row *util.Row) ([][]byte, [][]byte, error) {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for _, index := range indexs {
		for _, indexCol := range index.Columns {
			value, err := util.Encode(rowID)
			if err != nil {
				return nil, nil, err
			}
			valCol := insertExec.getValueColumnInRow(indexCol.Name, row)
			key := insertExec.genKeyIndex(TBID, indexCol.Name, valCol)

			// check unique value
			if index.Info.Unique {
				v, err := insertExec.baseExecutor.dal.Get(insertExec.baseExecutor.ctx, key)
				if err != nil {
					return nil, nil, err
				}
				if v != nil {
					return nil, nil, error_zpd.ErrUnique
				}
			}

			keys = append(keys, key)
			values = append(values, value)
		}
	}

	return keys, values, nil
}

func (insertExec *InsertExec) prepareData(table *util.Table) ([][]byte, [][]byte, error) {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for _, row := range insertExec.Rows {
		data, err := insertExec.prepareRow(table, row)
		if err != nil {
			return nil, nil, err
		}

		rowID := uint64(insertExec.baseExecutor.generateID.Generate().Int64())
		rowKey := insertExec.genRowKey(table.ID, rowID)

		data.ID = rowID

		dataInsert, err := util.Encode(data)
		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, rowKey)
		values = append(values, dataInsert)

		iKey, iVal, err := insertExec.prepareIndexs(table.ID, rowID, table.Indexes, row)
		if err != nil {
			return nil, nil, err
		}

		keys = append(keys, iKey...)
		values = append(values, iVal...)

	}

	return keys, values, nil
}

func (insertExec *InsertExec) insertRow() (interface{}, error) {
	// get table to check
	tb, err := insertExec.baseExecutor.globalVar.DDLDB.GetTable(insertExec.baseExecutor.schema.ID,
		insertExec.stmt.Table.Name.String())
	if err != nil {
		return nil, err
	}

	if tb == nil {
		return nil, error_zpd.ErrTableNoExists
	}

	// log.Info("[InsertExec] Start insert row for table: ", tb.ID)

	keys, values, err := insertExec.prepareData(tb)
	if err != nil {
		return nil, err
	}

	// insert one rows
	if len(keys) == 1 {
		err = insertExec.baseExecutor.dal.Put(insertExec.baseExecutor.ctx, keys[0], values[0])
		if err != nil {
			log.Error("[InsertExec] Put row " + util.ConvertBytesToString(keys[0]) + " to TiKV error: " + err.Error())
			return nil, err
		}
	} else { // insert batch row
		err := insertExec.baseExecutor.dal.BatchPut(insertExec.baseExecutor.ctx, keys, values)
		if err != nil {
			log.Error("[InsertExec] Put row to TiKV error: " + err.Error())
			return nil, err
		}
	}

	// log.Info("[InsertExec] Insert row for table done: ", tb.ID)
	return nil, nil
}
