package statement

import "github.com/pivovarit/pivodb/db/storage"

type Type string

const (
	InsertStatement      Type = "insert into"
	CreateTableStatement Type = "create table"
	SelectStatement      Type = "select"
)

func (t Type) Value() string {
	return string(t)
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
