package statement

import "github.com/pivovarit/pivodb/db/column"

const Insert = "insert"
const Select = "select"

type Statement struct {
	StatementType Type
	RowToInsert column.Row
}

type Type string
