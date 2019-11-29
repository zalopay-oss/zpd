package parser

import (
	"github.com/xwb1989/sqlparser"
)

//Parser interface
type Parser interface {
	Parse(sql string) (sqlparser.Statement, error)
}
