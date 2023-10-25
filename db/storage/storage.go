package storage

import "github.com/pivovarit/pivodb/db/serializer"

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
	Rows [RowsPerPage]*SerializedRow
}

type Row struct {
	Id       uint32
	Username string
	Email    string
}

type SerializedRow [RowSize]byte

func (r SerializedRow) Raw() []byte {
	return r[:]
}

func Serialize(row Row) SerializedRow {
	var serialized [RowSize]byte
	copy(serialized[IdOffset:UsernameOffset], serializer.WriteUint32(row.Id))
	copy(serialized[UsernameOffset:EmailOffset], serializer.WriteString(row.Username))
	copy(serialized[EmailOffset:], serializer.WriteString(row.Email))
	return serialized
}

func Deserialize(row *SerializedRow) Row {
	var id [IdSize]byte
	var username [UsernameSize]byte
	var email [EmailSize]byte

	copy(id[:], row[IdOffset:UsernameOffset])
	copy(username[:], row[UsernameOffset:EmailOffset])
	copy(email[:], row[EmailOffset:])

	return Row{
		Id:       serializer.ReadUint32(id),
		Username: serializer.ReadString(username[:]),
		Email:    serializer.ReadString(email[:]),
	}
}

type Table struct {
	RowCount  uint32
	Pager     *Pager
}

func NewTable() *Table {
	return &Table{Pager: &Pager{pages: [TableMaxPages]*Page{}}}
}
