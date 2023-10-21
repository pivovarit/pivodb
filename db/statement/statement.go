package statement

import "github.com/pivovarit/pivodb/db/storage"

const Insert = "insert"
const Select = "select"

type Statement struct {
	StatementType Type
	RowToInsert   storage.Row
}

type Type string
