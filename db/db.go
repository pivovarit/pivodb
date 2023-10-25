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
	PageId   uint32
	RowId    uint32
}

func (r *Result) ToString() string {
	return fmt.Sprintf("(%d,%s,%s) [rowId: %d, pageId: %d]", r.Id, r.Username, r.Email, r.RowId, r.PageId)
}

type DB struct {
	Tables map[string]*storage.Table
}

func New() *DB {
	return &DB{Tables: map[string]*storage.Table{}}
}

func (db *DB) Close() {
}

func (db *DB) Execute(stmt *statement.Statement) ([]Result, error) {
	switch stmt.StatementType {
	case statement.CreateTableStatement:
		if db.Tables[stmt.TableName] != nil {
			return []Result{}, fmt.Errorf("table [%s] already exists", stmt.TableName)
		}

		db.Tables[stmt.TableName] = storage.NewTable(stmt.TableName)
		return []Result{}, nil
	case statement.InsertStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		if db.Tables[stmt.TableName].RowCount == storage.TableMaxRows {
			return []Result{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		err := table.Pager.Save(storage.Serialize(storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		}))
		if err != nil {
			return []Result{}, fmt.Errorf("could not insert: %s", err)
		}
		table.RowCount++
		return []Result{}, nil
	case statement.SelectStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		var results []Result
		for pageId, page := range table.Pager.GetPages() {
			if page == nil {
				break
			}
			for rowId, serializedRow := range page.Rows {
				if serializedRow == nil {
					break
				}

				row := storage.Deserialize(serializedRow)
				results = append(results, Result{
					Id:       row.Id,
					Username: row.Username,
					Email:    row.Email,
					PageId:   uint32(pageId),
					RowId:    uint32((pageId * storage.RowsPerPage) + rowId),
				})
			}
		}

		return results[:table.RowCount], nil
	}
	return []Result{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
}

func (db *DB) pageId(rowId uint32) uint32 {
	return rowId % storage.RowsPerPage
}
