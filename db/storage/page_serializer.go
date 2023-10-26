package storage

func SerializePage(page *Page) []byte {
	var result []byte
	for _, row := range page.Rows {
		if row != nil {
			for _, b := range row {
				result = append(result, b)
			}
		}
	}
	return result
}

func DeserializePage(bytes []byte) *Page {
	page := NewPage()
	idx := 0
	for i := 0; i < len(bytes); i = i + RowSize {
		var row = [RowSize]byte(bytes[i : i+RowSize])
		page.Rows[idx] = &row
		idx++
	}
	return page
}