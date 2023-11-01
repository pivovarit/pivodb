package storage

import (
	"errors"
	"fmt"
	"github.com/pivovarit/pivodb/db/storage/layout"
	"github.com/rs/zerolog/log"
	"io"
	"os"
)

const DbFileNamePrefix = ".pivodb_"

const (
	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / (layout.IdSize + layout.UsernameSize + layout.EmailSize)
	TableMaxRows  = RowsPerPage * TableMaxPages
)

type Pager struct {
	pageCache [TableMaxPages]*Page
	file      *os.File
}

func New(table string) *Pager {
	fileName := DbFileNamePrefix + table
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	if err != nil {
		file, err = os.Create(fileName)
		if err != nil {
			log.Error().Err(err).Msg("Could not open db files")
			panic("could not open/create db file")
		}
	}

	return &Pager{
		pageCache: [TableMaxPages]*Page{},
		file:      file,
	}
}

func (p *Pager) FileLength() uint64 {
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}

	return uint64(stat.Size())
}

func (p *Pager) FlushToDisk() {
	for pageId, page := range p.pageCache {
		if page != nil && page.Dirty {
			log.Debug().Int("pageId", pageId).Msg("Flushing page to disk")

			offset := int64(pageId * (RowsPerPage * layout.RowSize))
			_, err := p.file.WriteAt(SerializePage(page), offset)
			if err != nil {
				panic("could not flush to disk: " + err.Error())
			}
		}
	}
}

func (p *Pager) GetPages() []*Page {
	var result []*Page
	stat, err := p.file.Stat()
	if err != nil {
		panic(err)
	}

	for i := 0; i < int(stat.Size()); i = i + RowsPerPage*layout.RowSize {
		page := make([]byte, RowsPerPage*layout.RowSize)
		_, err = p.file.ReadAt(page, int64(i))
		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
		result = append(result, DeserializePage(page))
	}

	copy(p.pageCache[:], result)

	return result
}

func (p *Pager) GetRow(RowNum uint32) (*Row, error) {
	pageId := RowNum / RowsPerPage
	page := p.loadPage(pageId)
	row := page.Rows[RowNum%RowsPerPage]
	if row != nil {
		deserialized := Deserialize(*row)
		return &deserialized, nil
	}

	return nil, nil
}

func (p *Pager) readPageFromDisk(pageId uint32) *Page {
	log.Debug().Uint32("pageId", pageId).Msg("Loading page from disk")

	page := make([]byte, RowsPerPage*layout.RowSize)
	readBytes, err := p.file.ReadAt(page, int64(pageId*RowsPerPage*layout.RowSize))
	if err != nil && !errors.Is(err, io.EOF) {
		panic("page ended: " + err.Error())
	}

	if readBytes == 0 {
		return nil
	}
	return DeserializePage(page)
}

func (p *Pager) GetRowAt(cursor *Cursor) (Row, error) {
	if cursor.EndOfTable {
		return Row{}, fmt.Errorf("end of table cursor")
	}
	row, err := p.GetRow(cursor.RowNum)
	return *row, err
}

func (p *Pager) SaveAt(bytes [layout.RowSize]byte, cursor *Cursor) {
	pageId := cursor.RowNum / RowsPerPage
	page := p.loadPage(pageId)
	page.Rows[cursor.RowNum%RowsPerPage] = &bytes
	page.Dirty = true
}

func (p *Pager) loadPage(pageId uint32) *Page {
	page := p.pageCache[pageId]
	if page == nil {
		loaded := p.readPageFromDisk(pageId)
		if loaded != nil {
			p.pageCache[pageId] = loaded
			page = loaded
		} else {
			page = NewPage()
			p.pageCache[pageId] = page
		}
	}
	return page
}
