package parser

import "github.com/xwb1989/sqlparser"

//ParserImpl struct implement Parser interface
type ParserImpl struct {
}

//NewParser create parser
func NewParser() Parser {
	return &ParserImpl{}
}

//Parse parse SQL to AST
func (p *ParserImpl) Parse(sql string) (sqlparser.Statement, error) {
	return sqlparser.Parse(sql)
}
