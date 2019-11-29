package executor

import (
	"context"
	"strings"
	"sync"
	"zpd/pkg/dal"
	error_zpd "zpd/pkg/error"
	"zpd/pkg/global"
	"zpd/pkg/util"

	"github.com/bwmarrin/snowflake"
	"github.com/xwb1989/sqlparser"
)

//Executor executor interface
type Executor interface {
	Next() (interface{}, error)
}

// BaseExecutor base executor
type BaseExecutor struct {
	globalVar    *global.GlobalVar
	schema       *util.Schema
	childrenExec []Executor
	dal          dal.DataAccessLayer
	generateID   *snowflake.Node
	ctx          context.Context
}

// BuilderExecutor base executor
type BuilderExecutor struct {
	dal        dal.DataAccessLayer
	globalVar  *global.GlobalVar
	Schema     *util.Schema
	generateID *snowflake.Node
	mux        sync.RWMutex
}

// NewBuilderExecutor new NewBuilderExecutor
func NewBuilderExecutor(dal dal.DataAccessLayer, schema *util.Schema, mux sync.RWMutex, globalVar *global.GlobalVar, ID uint64) (*BuilderExecutor, error) {
	node, err := snowflake.NewNode(int64(ID))
	if err != nil {

		return nil, err
	}

	return &BuilderExecutor{
		dal:        dal,
		globalVar:  globalVar,
		Schema:     schema,
		generateID: node,
		mux:        mux,
	}, nil
}

func (builder *BuilderExecutor) newBaseExecutor(ctx context.Context, childrenExec ...Executor) *BaseExecutor {
	return &BaseExecutor{
		globalVar:    builder.globalVar,
		schema:       builder.Schema,
		childrenExec: childrenExec,
		dal:          builder.dal,
		generateID:   builder.generateID,
		ctx:          ctx,
	}
}

// Build build executor
func (builder *BuilderExecutor) Build(ctx context.Context, stmt sqlparser.SQLNode) (Executor, error) {
	switch stmt.(type) {
	case *sqlparser.DBDDL:
		return builder.buildDBDDLExec(ctx, stmt.(*sqlparser.DBDDL))
	case *sqlparser.Use:
		return builder.buildUseExec(ctx, stmt.(*sqlparser.Use))
	case *sqlparser.Show:
		if stmt.(*sqlparser.Show).Type == DATABASES {
			return builder.buildShowDatabaseExec(ctx, stmt.(*sqlparser.Show))
		} else {
			return builder.buildShowTableExec(ctx, stmt.(*sqlparser.Show))
		}
	case *sqlparser.TableIdent:
		return nil, nil
	case *sqlparser.DDL:
		return builder.buildDDLExec(ctx, stmt.(*sqlparser.DDL))
	case *sqlparser.Insert:
		return builder.buildInsertRowExec(ctx, stmt.(*sqlparser.Insert))
	case *sqlparser.Select:
		return builder.buildSelectRowExec(ctx, stmt.(*sqlparser.Select))
	case *sqlparser.Delete:
		return builder.buildDeleteExec(ctx, stmt.(*sqlparser.Delete))
	case *sqlparser.Update:
		return nil, nil
	}
	return nil, nil
}

// NewDBDDLExec new DBDDLExec
func (builder *BuilderExecutor) buildDBDDLExec(ctx context.Context, stmt *sqlparser.DBDDL) (*DBDDLExec, error) {
	baseExec := builder.newBaseExecutor(ctx)

	return &DBDDLExec{
		stmt:         stmt,
		baseExecutor: baseExec,
		Action:       stmt.Action,
	}, nil
}

func (builder *BuilderExecutor) buildUseExec(ctx context.Context, stmt *sqlparser.Use) (*UseExec, error) {
	tableIdentExec, err := builder.buildTableIdentExec(ctx, &stmt.DBName)
	if err != nil {
		return nil, err
	}

	baseExec := builder.newBaseExecutor(ctx, tableIdentExec)

	return &UseExec{
		stmt:         stmt,
		baseExecutor: baseExec,
	}, nil
}

func (builder *BuilderExecutor) buildTableIdentExec(ctx context.Context, stmt *sqlparser.TableIdent) (*TableIdentExec, error) {
	baseExec := builder.newBaseExecutor(ctx)

	return &TableIdentExec{
		stmt:         stmt,
		baseExecutor: baseExec,
	}, nil
}

func (builder *BuilderExecutor) buildShowDatabaseExec(ctx context.Context, stmt *sqlparser.Show) (*ShowDatabaseExec, error) {
	baseExec := builder.newBaseExecutor(ctx)
	return &ShowDatabaseExec{
		stmt:         stmt,
		baseExecutor: baseExec,
	}, nil
}

func (builder *BuilderExecutor) checkSchemaIsExists() error {
	builder.mux.RLock()
	defer builder.mux.RUnlock()

	if builder.Schema == nil {
		return error_zpd.ErrDoNotUseDatabase
	}

	return nil
}

func (builder *BuilderExecutor) buildDDLExec(ctx context.Context, stmt *sqlparser.DDL) (*DDLExec, error) {
	err := builder.checkSchemaIsExists()
	if err != nil {
		return nil, err
	}
	baseExec := builder.newBaseExecutor(ctx)

	return &DDLExec{
		stmt:         stmt,
		baseExecutor: baseExec,
		Action:       stmt.Action,
	}, nil
}

func (builder *BuilderExecutor) buildShowTableExec(ctx context.Context, stmt *sqlparser.Show) (*ShowTableExec, error) {
	err := builder.checkSchemaIsExists()
	if err != nil {
		return nil, err
	}

	if stmt.ShowTablesOpt.DbName == "" && builder.Schema == nil {
		return nil, error_zpd.ErrDoNotUseDatabase
	}

	baseExec := builder.newBaseExecutor(ctx)

	return &ShowTableExec{
		stmt:         stmt,
		baseExecutor: baseExec,
	}, nil
}

func (builder *BuilderExecutor) buildInsertRowExec(ctx context.Context, stmt *sqlparser.Insert) (*InsertExec, error) {
	err := builder.checkSchemaIsExists()
	if err != nil {
		return nil, err
	}

	baseExec := builder.newBaseExecutor(ctx)

	cols, err := builder.buildColumns(stmt.Columns)
	if err != nil {
		return nil, err
	}

	tmp := InsertExec{
		stmt:         stmt,
		baseExecutor: baseExec,
		Action:       stmt.Action,
		Columns:      cols,
		Rows:         builder.buildRows(stmt.Rows.(sqlparser.Values)),
	}

	return &tmp, nil
}

func (builder *BuilderExecutor) checkDuplicateColumn(nameCol string, cols []string) bool {
	for _, item := range cols {
		if item == nameCol {
			return true
		}
	}

	return false
}

func (builder *BuilderExecutor) buildColumns(columns []sqlparser.ColIdent) ([]string, error) {
	cols := make([]string, len(columns))
	for i, colName := range columns {
		if builder.checkDuplicateColumn(colName.String(), cols) {
			return nil, error_zpd.ErrDuplicateColumn
		}

		cols[i] = strings.ToLower(colName.String())
	}

	return cols, nil
}

func (builder *BuilderExecutor) buildRows(rowTuples []sqlparser.ValTuple) []*util.Row {
	rowTps := make([]*util.Row, len(rowTuples))

	for i, tuple := range rowTuples {
		rowTmp := &util.Row{}
		items := make([]*util.Item, len(tuple))

		for j, r := range tuple {
			item := &util.Item{}

			switch r.(sqlparser.Expr).(type) {
			case *sqlparser.SQLVal:
				switch r.(*sqlparser.SQLVal).Type {
				case sqlparser.StrVal:
					item.Type = STRVAL
					break
				case sqlparser.IntVal:
					item.Type = INTVAL
					break
				default:
					item.Type = DEFAULT
					break
				}
				item.Val = r.(*sqlparser.SQLVal).Val
				break
			case sqlparser.BoolVal:
				item.Type = BOOLVAL
				if r.(sqlparser.BoolVal) {
					item.Bool = true
				} else {
					item.Bool = false
				}
				break
			default:
				item.Type = DEFAULT
				item.Val = nil
				break
			}
			items[j] = item
		}

		rowTmp.Items = items
		rowTps[i] = rowTmp
	}

	return rowTps
}

func (builder *BuilderExecutor) buildColumnsSelect(selectExprs sqlparser.SelectExprs) []string {
	cols := make([]string, len(selectExprs))

	for i, col := range selectExprs {
		switch col.(type) {
		case *sqlparser.AliasedExpr:
			cols[i] = col.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.String()
			break
		case *sqlparser.StarExpr:
			cols[i] = "*"
			break
		}
	}

	return cols
}

func (builder *BuilderExecutor) buildTableExprs(tableExprs sqlparser.TableExprs) []string {
	tbs := make([]string, len(tableExprs))

	for i, tb := range tableExprs {
		switch tb.(type) {
		case *sqlparser.AliasedTableExpr:
			tbs[i] = tb.(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
			break
		}
	}

	return tbs
}

func (builder *BuilderExecutor) buildComparisonExpr(expr *sqlparser.ComparisonExpr) *ComparisonExpr {
	comExpr := &ComparisonExpr{}
	comExpr.Operator = expr.Operator
	comExpr.Left = &LeftSide{
		Name: strings.ToLower(expr.Left.(*sqlparser.ColName).Name.String()),
	}

	rightSide := &RightSide{}
	switch expr.Right.(*sqlparser.SQLVal).Type {
	case sqlparser.StrVal:
		rightSide.Type = STRVAL
		break
	case sqlparser.IntVal:
		rightSide.Type = INTVAL
		break
	}

	comExpr.Right = rightSide
	comExpr.Right.Val = expr.Right.(*sqlparser.SQLVal).Val

	return comExpr
}

func (builder *BuilderExecutor) buildExprWhere(where *sqlparser.Where) *Where {
	w := &Where{}
	w.Type = where.Type

	switch where.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		w.Expr = builder.buildComparisonExpr(where.Expr.(*sqlparser.ComparisonExpr))
		break
	default:
		return nil
	}

	return w
}

func (builder *BuilderExecutor) buildSelectRowExec(ctx context.Context, stmt *sqlparser.Select) (*SelectExec, error) {
	err := builder.checkSchemaIsExists()
	if err != nil {
		return nil, err
	}

	baseExec := builder.newBaseExecutor(ctx)

	if stmt.Where != nil {
		return &SelectExec{
			stmt:         stmt,
			baseExecutor: baseExec,
			cols:         builder.buildColumnsSelect(stmt.SelectExprs),
			tbNames:      builder.buildTableExprs(stmt.From),
			where:        builder.buildExprWhere(stmt.Where),
		}, nil
	}
	return &SelectExec{
		stmt:         stmt,
		baseExecutor: baseExec,
		cols:         builder.buildColumnsSelect(stmt.SelectExprs),
		tbNames:      builder.buildTableExprs(stmt.From),
		where:        nil,
	}, nil
}

func (builder *BuilderExecutor) buildDeleteExec(ctx context.Context, stmt *sqlparser.Delete) (*DeleteExec, error) {
	err := builder.checkSchemaIsExists()
	if err != nil {
		return nil, err
	}

	baseExec := builder.newBaseExecutor(ctx)

	if stmt.Where != nil {
		return &DeleteExec{
			stmt:         stmt,
			baseExecutor: baseExec,
			tbNames:      builder.buildTableExprs(stmt.TableExprs),
			where:        builder.buildExprWhere(stmt.Where),
		}, nil
	}

	return &DeleteExec{
		stmt:         stmt,
		baseExecutor: baseExec,
		tbNames:      builder.buildTableExprs(stmt.TableExprs),
		where:        nil,
	}, nil
}
