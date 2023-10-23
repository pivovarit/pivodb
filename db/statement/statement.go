package statement

import "github.com/pivovarit/pivodb/db/storage"

type Type string

const (
	InsertStatement Type = "insert into"
	SelectStatement Type = "select"
)

func (t Type) Value() string {
	return string(t)
}

type Statement struct {
	StatementType Type
	RowToInsert   storage.Row
}

func Insert(row storage.Row) *Statement {
	return &Statement{
		StatementType: InsertStatement,
		RowToInsert:   row,
	}
}

func Select() *Statement {
	return &Statement{
		StatementType: SelectStatement,
	}
}
