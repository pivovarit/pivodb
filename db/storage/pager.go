package storage

import (
	"github.com/pivovarit/pivodb/db/storage/layout"
)

type Pager interface {
	FileLength() uint64
	FlushToDisk()
	GetPages() []*Page
	GetRow(RowNum uint32) (*Row, error)
	GetRowAt(cursor *Cursor) (Row, error)
	SaveAt(bytes [layout.RowSize]byte, cursor *Cursor)
}
