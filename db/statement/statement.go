package statement

const Insert = "insert"
const Select = "select"

type Statement struct {
	StatementType Type
}

type Type string
