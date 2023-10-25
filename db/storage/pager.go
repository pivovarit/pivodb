package storage

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

const DbFileNamePrefix = ".pivodb"

type Pager struct {
	pages [TableMaxPages]*Page
	file  *os.File
}

func New(table string) *Pager {
	fileName := fmt.Sprintf("%s_%s", DbFileNamePrefix, table)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	if err != nil {
		file, err = os.Create(fileName)
		if err != nil {
			panic("could not open/create db file")
		}
	}

	return &Pager{
		pages: [TableMaxPages]*Page{},
		file:  file,
	}
}

func (p *Pager) RowCount() uint32 {
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}

	return uint32(stat.Size() / RowSize)
}

func (p *Pager) PageCount() uint32 {
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}
	return uint32(math.Ceil((float64(stat.Size()) / float64(RowSize)) / float64(RowsPerPage)))
}

func (p *Pager) GetPages() []*Page {
	var result []*Page
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}

	for i := 0; i < int(stat.Size()); i = i + RowsPerPage*RowSize {
		page := make([]byte, RowsPerPage*RowSize)
		_, err = p.file.ReadAt(page, int64(i))
		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
		result = append(result, DeserializePage(page))
	}

	return result
}

func (p *Pager) SaveAt(bytes [RowSize]byte, cursor *Cursor) error {
	_, err := p.file.WriteAt(bytes[:], int64(cursor.Offset()))
	return err
}
