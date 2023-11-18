package storage

import "github.com/pivovarit/pivodb/db/storage/layout"

type Page struct {
	Dirty bool
	Rows  [RowsPerPage]*[layout.RowSize]byte
}

type Row struct {
	Id       uint32
	Username string
	Email    string
}

type Table struct {
	RowCount uint32
	Pager    Pager
}

func NewPage() *Page {
	return &Page{
		Dirty: false,
		Rows:  [RowsPerPage]*[layout.RowSize]byte{}}
}

func Open(table string) *Table {
	pager := New(table)
	numRows := pager.FileLength() / layout.RowSize
	return &Table{
		RowCount: uint32(numRows),
		Pager:    pager,
	}
}
