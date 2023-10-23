package statement

import "github.com/pivovarit/pivodb/db/storage"

const InsertStatement = "insert into"
const SelectStatement = "select"

type Statement struct {
	StatementType Type
	RowToInsert   storage.Row
}

type Type string

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
