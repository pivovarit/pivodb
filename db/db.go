package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
)

type Result struct {
	Id       uint32
	Username string
	Email    string
}

func (r *Result) ToString() string {
	return fmt.Sprintf("(%d,%s,%s)", r.Id, r.Username, r.Email)
}

type DB struct {
	Tables map[string]*storage.Table
}

func New() *DB {
	return &DB{Tables: map[string]*storage.Table{}}
}

func (db *DB) Execute(stmt *statement.Statement) ([]Result, error) {
	switch stmt.StatementType {
	case statement.CreateTableStatement:
		if db.Tables[stmt.TableName] != nil {
			return []Result{}, fmt.Errorf("table [%s] already exists", stmt.TableName)
		}

		db.Tables[stmt.TableName] = &storage.Table{Pages: [storage.TableMaxPages]*storage.Page{}}
		return []Result{}, nil
	case statement.InsertStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		if db.Tables[stmt.TableName].RowCount == storage.TableMaxRows {
			return []Result{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		pageId := table.RowCount / storage.RowsPerPage
		page := db.resolvePage(pageId, table)
		serialized := storage.Serialize(storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		})
		page.Rows[(table.RowCount % storage.RowsPerPage)] = &serialized
		table.RowCount++
		return []Result{}, nil
	case statement.SelectStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}
		var results []Result
		for _, page := range table.Pages {
			if page == nil {
				break
			}
			for _, serializedRow := range page.Rows {
				if serializedRow == nil {
					break
				}

				row := storage.Deserialize(serializedRow)
				results = append(results, Result{
					Id:       row.Id,
					Username: row.Username,
					Email:    row.Email,
				})
			}
		}

		return results, nil
	}
	return []Result{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
}

func (db *DB) resolvePage(pageId uint32, table *storage.Table) *storage.Page {
	page := table.Pages[pageId]
	if page == nil {
		table.PageCount++
		table.Pages[pageId] = &storage.Page{Rows: [storage.RowsPerPage]*storage.SerializedRow{}}
		page = table.Pages[pageId]
	}
	return page
}
