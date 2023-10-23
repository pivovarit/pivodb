package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
)

const DefaultTableName = "users"

type Result struct {
	Id       uint32
	Username string
	Email    string
}

func (r Result) ToString() string {
	return fmt.Sprintf("(%d,%s,%s)", r.Id, r.Username, r.Email)
}

type DB struct {
	Table storage.Table
}

func New() *DB {
	return &DB{Table: storage.Table{Pages: [storage.TableMaxPages]*storage.Page{}}}
}

func (db *DB) Execute(stmt *statement.Statement) ([]Result, error) {
	switch stmt.StatementType {
	case statement.InsertStatement:
		if db.Table.RowCount == storage.TableMaxRows {
			return []Result{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		pageId := db.Table.RowCount / storage.RowsPerPage
		page := db.resolvePage(pageId)
		page.Rows[(db.Table.RowCount % storage.RowsPerPage)] = &storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		}
		db.Table.RowCount++
		return []Result{}, nil
	case statement.SelectStatement:
		var results []Result
		for _, page := range db.Table.Pages {
			if page == nil {
				break
			}
			for _, row := range page.Rows {
				if row == nil {
					break
				}
				results = append(results, Result{
					Id:       row.Id,
					Username: string(row.Username[:]),
					Email:    string(row.Email[:]),
				})
			}
		}

		return results, nil
	}
	return []Result{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
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
