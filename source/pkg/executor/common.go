package executor

const (
	CreateStr = "create"
	DropStr   = "drop"
)

const (
	DATABASES = "databases"
	TABLES    = "tables"
	STRVAL    = 0
	INTVAL    = 1
	BOOLVAL   = 2
	DEFAULT   = -1
)

const (
	InsertStr  = "insert"
	ReplaceStr = "replace"
)

var (
	mRowPrefix = "row"
	minRowID   = "0"
	maxRowID   = "99999999999999999999"
	limitScan  = 10240

	mIndexPrefix = "i"
)

var (
	limit          = 10240
	OPERATOR_EQUAL = "="
)

// LeftSide left side
type LeftSide struct {
	Name string
}

// RightSide right side
type RightSide struct {
	Type int32
	Val  []byte
}

// ComparisonExpr comparisonExpr
type ComparisonExpr struct {
	Operator string
	Left     *LeftSide
	Right    *RightSide
}

// Where struct
type Where struct {
	Type string
	Expr *ComparisonExpr
}
