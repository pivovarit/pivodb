package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
)

const DefaultTableName = "users"

type DB struct {
	Table storage.Table
}

func New() *DB {
	return &DB{Table: storage.Table{Pages: [storage.TableMaxPages]*storage.Page{}}}
}

func (db *DB) Execute(stmt *statement.Statement) ([]string, error) {
	switch stmt.StatementType {
	case statement.Insert:
		if db.Table.RowCount == storage.TableMaxRows {
			return []string{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		pageId := db.Table.RowCount / storage.RowsPerPage
		page := db.resolvePage(pageId)
		page.Rows[(db.Table.RowCount % storage.RowsPerPage)] = &storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		}
		db.Table.RowCount++
		return []string{}, nil
	case statement.Select:
		var results []string
		for _, page := range db.Table.Pages {
			if page == nil {
				break
			}
			for _, row := range page.Rows {
				if row == nil {
					break
				}
				results = append(results, fmt.Sprintf("(%d,%s,%s)", row.Id, row.Username, row.Email))
			}
		}

		return results, nil
	}
	return []string{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
}

func (db *DB) resolvePage(pageId uint32) *storage.Page {
	page := db.Table.Pages[pageId]
	if page == nil {
		db.Table.PageCount++
		db.Table.Pages[pageId] = &storage.Page{Rows: [storage.RowsPerPage]*storage.Row{}}
		page = db.Table.Pages[pageId]
	}
	return page
}
