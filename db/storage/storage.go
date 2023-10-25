package storage

const (
	PageSize      = 4096
	TableMaxPages = 100

	IdSize         = 4
	UsernameSize   = 32
	EmailSize      = 255
	RowSize        = IdSize + UsernameSize + EmailSize
	IdOffset       = 0
	UsernameOffset = IdOffset + IdSize
	EmailOffset    = UsernameOffset + UsernameSize

	RowsPerPage  = PageSize / (IdSize + UsernameSize + EmailSize)
	TableMaxRows = RowsPerPage * TableMaxPages
)

type Page struct {
	Rows [RowsPerPage]*[RowSize]byte
}

type Row struct {
	Id       uint32
	Username string
	Email    string
}

type Table struct {
	RowCount uint32
	Pager    *Pager
}

func NewPage() *Page {
	return &Page{Rows: [RowsPerPage]*[RowSize]byte{}}
}

func NewTable(table string) *Table {
	pager := New(table)
	return &Table{
		RowCount: pager.RowCount(),
		Pager:    pager,
	}
}
