package storage

type Pager struct {
	pageCount uint32
	pages     [TableMaxPages]*Page
}

func (p *Pager) ResolvePage(pageId uint32) *Page {
	page := p.pages[pageId]
	if page == nil {
		p.pageCount++
		p.pages[pageId] = &Page{Rows: [RowsPerPage]*SerializedRow{}}
		page = p.pages[pageId]
	}
	return page
}

func (p *Pager) PageCount() uint32 {
	return p.pageCount
}

func (p *Pager) GetPages() [TableMaxPages]*Page {
	return p.pages
}
