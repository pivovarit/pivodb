package storage

const TableName = "users"

const (
	PageSize      = 4096
	TableMaxPages = 100

	IdSize         = 4
	UsernameSize   = 32
	EmailSize      = 255
	IdOffset       = 0
	UsernameOffset = IdOffset + IdSize
	EmailOffset    = UsernameOffset + UsernameSize

	RowsPerPage  = PageSize / (IdSize + UsernameSize + EmailSize)
	TableMaxRows = RowsPerPage * TableMaxPages
)

type Page struct {
	Rows [RowsPerPage]*Row
}

type Row struct {
	Id       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}

type Table struct {
	PageCount uint32
	RowCount uint32
	Pages [TableMaxPages]*Page
}
