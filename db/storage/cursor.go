package storage

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

func (c *Cursor) Offset() uint32 {
	return c.RowNum * RowSize
}
