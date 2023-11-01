package storage

import "github.com/pivovarit/pivodb/db/storage/layout"

type Cursor struct {
	Table      *Table
	RowNum     uint32
	EndOfTable bool
}

func StartOf(table *Table) *Cursor {
	return &Cursor{
		Table:      table,
		RowNum:     0,
		EndOfTable: false,
	}
}

func EndOf(table *Table) *Cursor {
	return &Cursor{
		Table:      table,
		RowNum:     table.RowCount,
		EndOfTable: true,
	}
}

func (c *Cursor) Next() {
	c.RowNum++
	if c.RowNum == c.Table.RowCount {
		c.EndOfTable = true
	}
}

func (c *Cursor) Offset() uint32 {
	return c.RowNum * layout.RowSize
}
