package util

import (
	"strings"
	zpd_internal_proto "zpd/pkg/internal-api"

	error_zpd "zpd/pkg/error"
	zpd_proto "zpd/pkg/public-api"

	"github.com/xwb1989/sqlparser"
)

const (
	MAX_COLUMN = 1000
	StrVal     = "varchar"
	IntVal     = "int"
	STRVAL     = 0
	INTVAL     = 1
	DEFAULT    = -1
)

// Schema info Schema
type Schema struct {
	ID     uint64
	DBName string
	Tables []*Table
}

// Table info Table
type Table struct {
	ID      uint64
	TBName  string
	Columns []*Column
	Indexes []*Index
}

// Column info Column
type Column struct {
	ID   uint64
	Name string
	Type *ColumnType
}

// ColumnType info Column
type ColumnType struct {
	Type    int32
	NotNull bool
	Lenght  uint64
}

// Item of row
type Item struct {
	Type int32
	Val  []byte
	Bool bool
}

// Row row
type Row struct {
	ID    uint64
	Items []*Item
}

// Index info
type Index struct {
	ID      uint64
	Info    *IndexInfo
	Columns []*IndexColumn
}

// IndexInfo info index
type IndexInfo struct {
	Type    string
	Name    string
	Primary bool
	Unique  bool
}

// IndexColumn info IndexColumn
type IndexColumn struct {
	Name string
}

// MappingItemProto maping object item to item proto
func MappingItemProto(item *Item) *zpd_proto.Item {
	return &zpd_proto.Item{
		Type: int32(item.Type),
		Val:  item.Val,
		Bool: item.Bool,
	}
}

// MappingRowProto mapping row object to row proto
func MappingRowProto(row *Row) *zpd_proto.Row {
	items := make([]*zpd_proto.Item, len(row.Items))

	for i, item := range row.Items {
		tmp := MappingItemProto(item)
		items[i] = tmp
	}

	return &zpd_proto.Row{
		Items: items,
	}
}

// MappingRowsProto mapping rows object to rows proto
func MappingRowsProto(rows []*Row) *zpd_proto.Rows {
	rs := make([]*zpd_proto.Row, len(rows))

	for i, r := range rows {
		tmp := MappingRowProto(r)
		rs[i] = tmp
	}

	return &zpd_proto.Rows{
		Rows: rs,
	}
}

// MappingColumnType build column type
func MappingColumnType(colType *zpd_internal_proto.ColumnType) *ColumnType {
	return &ColumnType{
		Type:    colType.Type,
		NotNull: colType.Notnull,
		Lenght:  colType.Lenght,
	}
}

// MappingColumns build column
func MappingColumns(columns []*zpd_internal_proto.Column) []*Column {
	cols := make([]*Column, len(columns))

	for i := 0; i < len(columns); i++ {
		col := &Column{
			ID:   uint64(i),
			Name: columns[i].Name,
			Type: MappingColumnType(columns[i].Type),
		}

		cols[i] = col
	}

	return cols
}

// MappingIndexInfo mapping index info
func MappingIndexInfo(info *zpd_internal_proto.IndexInfo) *IndexInfo {
	return &IndexInfo{
		Type:    info.Type,
		Name:    info.Name,
		Primary: info.Primary,
		Unique:  info.Unique,
	}
}

// MappingIndexColumn build index column
func MappingIndexColumn(indexCols []*zpd_internal_proto.IndexColumn) []*IndexColumn {
	idxCols := make([]*IndexColumn, len(indexCols))

	for i := 0; i < len(indexCols); i++ {
		idxCol := &IndexColumn{
			Name: indexCols[i].Name,
		}

		idxCols[i] = idxCol
	}

	return idxCols
}

// MappingIndexes build indexes
func MappingIndexes(indexes []*zpd_internal_proto.Index) []*Index {
	idxs := make([]*Index, len(indexes))

	for i := 0; i < len(indexes); i++ {
		idx := &Index{
			ID:      uint64(i),
			Info:    MappingIndexInfo(indexes[i].Info),
			Columns: MappingIndexColumn(indexes[i].Columns),
		}

		idxs[i] = idx
	}

	return idxs
}

// MappingTable mapping table proto to table object
// update later
func MappingTable(table *zpd_internal_proto.Table) *Table {
	return &Table{
		ID:      table.ID,
		TBName:  table.Tbname,
		Columns: MappingColumns(table.Columns),
		Indexes: MappingIndexes(table.Indexes),
	}
}

// MappingColumnTypeProto build column type
func MappingColumnTypeProto(colType *ColumnType) *zpd_internal_proto.ColumnType {
	return &zpd_internal_proto.ColumnType{
		Type:    colType.Type,
		Notnull: colType.NotNull,
		Lenght:  colType.Lenght,
	}
}

// MappingColumnsProto build column
func MappingColumnsProto(columns []*Column) []*zpd_internal_proto.Column {
	cols := make([]*zpd_internal_proto.Column, len(columns))

	for i := 0; i < len(columns); i++ {
		col := &zpd_internal_proto.Column{
			ID:   uint64(i),
			Name: columns[i].Name,
			Type: MappingColumnTypeProto(columns[i].Type),
		}

		cols[i] = col
	}

	return cols
}

// MappingIndexInfoProto mapping index info
func MappingIndexInfoProto(info *IndexInfo) *zpd_internal_proto.IndexInfo {
	return &zpd_internal_proto.IndexInfo{
		Type:    info.Type,
		Name:    info.Name,
		Primary: info.Primary,
		Unique:  info.Unique,
	}
}

// MappingIndexColumnProto build index column
func MappingIndexColumnProto(indexCols []*IndexColumn) []*zpd_internal_proto.IndexColumn {
	idxCols := make([]*zpd_internal_proto.IndexColumn, len(indexCols))

	for i := 0; i < len(indexCols); i++ {
		idxCol := &zpd_internal_proto.IndexColumn{
			Name: indexCols[i].Name,
		}

		idxCols[i] = idxCol
	}

	return idxCols
}

// MappingIndexesProto build indexes
func MappingIndexesProto(indexes []*Index) []*zpd_internal_proto.Index {
	idxs := make([]*zpd_internal_proto.Index, len(indexes))

	for i := 0; i < len(indexes); i++ {
		idx := &zpd_internal_proto.Index{
			ID:      uint64(i),
			Info:    MappingIndexInfoProto(indexes[i].Info),
			Columns: MappingIndexColumnProto(indexes[i].Columns),
		}

		idxs[i] = idx
	}

	return idxs
}

// MappingTableProto mapping table object to table proto
// update later
func MappingTableProto(table *Table) *zpd_internal_proto.Table {
	return &zpd_internal_proto.Table{
		ID:      table.ID,
		Tbname:  table.TBName,
		Columns: MappingColumnsProto(table.Columns),
		Indexes: MappingIndexesProto(table.Indexes),
	}
}

// MappingSchema mapping schema proto to schema object
func MappingSchema(schema *zpd_internal_proto.Schema) *Schema {
	tables := make([]*Table, len(schema.Tables))

	for index, table := range schema.Tables {
		tables[index] = MappingTable(table)
	}

	return &Schema{
		ID:     schema.ID,
		DBName: schema.Dbname,
		Tables: tables,
	}
}

// MappingSchemaProto mapping schema object to schema proto
func MappingSchemaProto(schema *Schema) *zpd_internal_proto.Schema {
	tables := make([]*zpd_internal_proto.Table, len(schema.Tables))

	for index, table := range schema.Tables {
		tables[index] = MappingTableProto(table)
	}

	return &zpd_internal_proto.Schema{
		ID:     schema.ID,
		Dbname: schema.DBName,
		Tables: tables,
	}
}

// BuildColumnType build column type
func BuildColumnType(colType sqlparser.ColumnType) (*ColumnType, error) {
	col := &ColumnType{}

	switch colType.Type {
	case StrVal:
		col.Type = STRVAL
		break
	case IntVal:
		col.Type = INTVAL
		break
	default:
		col.Type = DEFAULT
		break
	}

	if colType.NotNull == true {
		col.NotNull = true
	} else {
		col.NotNull = false
	}

	len, err := ConvertStringToUInt64(ConvertBytesToString(colType.Length.Val))
	if err != nil {
		return nil, err
	}
	col.Lenght = len

	return col, nil
}

// BuildColumns build column
func BuildColumns(columns []*sqlparser.ColumnDefinition) ([]*Column, error) {
	cols := make([]*Column, len(columns))

	for i := 0; i < len(columns); i++ {
		col := &Column{
			ID:   uint64(i),
			Name: columns[i].Name.String(),
		}

		typeCol, err := BuildColumnType(columns[i].Type)
		if err != nil {
			return nil, err
		}
		col.Type = typeCol

		cols[i] = col
	}

	return cols, nil
}

// BuildIndexInfo build index info
func BuildIndexInfo(info *sqlparser.IndexInfo) *IndexInfo {
	return &IndexInfo{
		Type:    info.Type,
		Name:    info.Name.String(),
		Primary: info.Primary,
		Unique:  info.Unique,
	}
}

// BuildIndexColumn build index column
func BuildIndexColumn(indexCols []*sqlparser.IndexColumn) []*IndexColumn {
	idxCols := make([]*IndexColumn, len(indexCols))

	for i := 0; i < len(indexCols); i++ {
		idxCol := &IndexColumn{
			Name: indexCols[i].Column.String(),
		}

		idxCols[i] = idxCol
	}

	return idxCols
}

// BuildIndexes build indexes
func BuildIndexes(indexes []*sqlparser.IndexDefinition) []*Index {
	idxs := make([]*Index, len(indexes))

	for i := 0; i < len(indexes); i++ {
		idx := &Index{
			ID:      uint64(i),
			Info:    BuildIndexInfo(indexes[i].Info),
			Columns: BuildIndexColumn(indexes[i].Columns),
		}

		idxs[i] = idx
	}

	return idxs
}

// checkDuplicate check duplicate columns
func checkDuplicate(columns []*sqlparser.ColumnDefinition) error {
	cols := make(map[string]bool)

	for i := 0; i < len(columns); i++ {
		if cols[columns[i].Name.String()] {
			return error_zpd.ErrDuplicateColumn
		}
		cols[columns[i].Name.String()] = true
	}

	return nil
}

func checkTooManyColumns(numberCol int) error {
	if numberCol > MAX_COLUMN {
		return error_zpd.ErrAddressServer
	}
	return nil
}

func checkInfoTable(infoTable *sqlparser.DDL) error {
	if err := checkDuplicate(infoTable.TableSpec.Columns); err != nil {
		return err
	}
	if err := checkTooManyColumns(len(infoTable.TableSpec.Columns)); err != nil {
		return err
	}
	// add rule to check table

	return nil
}

// BuildTable build table
// ID will be added later
func BuildTable(infoTable *sqlparser.DDL) (*Table, error) {
	if err := checkInfoTable(infoTable); err != nil {
		return nil, err
	}

	columns, err := BuildColumns(infoTable.TableSpec.Columns)

	table := &Table{
		TBName:  strings.ToLower(infoTable.NewName.Name.String()),
		Indexes: BuildIndexes(infoTable.TableSpec.Indexes),
	}

	if err != nil {
		return nil, err
	}
	table.Columns = columns

	return table, nil
}
