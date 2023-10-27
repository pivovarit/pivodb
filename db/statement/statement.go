package statement

import (
	"github.com/pivovarit/pivodb/db/storage"
	"strings"
)

type Type string

const (
	InsertStatement      Type = "insert into"
	CreateTableStatement Type = "create table"
	SelectStatement      Type = "select"
	TablesStatement      Type = "tables"
)

func Types() []Type {
	return []Type{InsertStatement, CreateTableStatement, SelectStatement, TablesStatement}
}

func (t Type) Value() string {
	return string(t)
}

func ParseStatementType(str string) *Type {
	for _, statement := range Types() {
		if strings.HasPrefix(str, statement.Value()) {
			return &statement
		}
	}
	return nil
}

type Statement struct {
	StatementType Type
	RowToInsert   storage.Row
	TableName     string
}

func CreateTable(table string) *Statement {
	return &Statement{
		StatementType: CreateTableStatement,
		TableName:     table,
	}
}

func Insert(row storage.Row, table string) *Statement {
	return &Statement{
		StatementType: InsertStatement,
		RowToInsert:   row,
		TableName:     table,
	}
}

func Select(table string) *Statement {
	return &Statement{
		StatementType: SelectStatement,
		TableName:     table,
	}
}

func Tables() *Statement {
	return &Statement{StatementType: TablesStatement}
}
