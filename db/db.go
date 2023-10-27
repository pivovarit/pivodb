package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"log"
	"os"
	"strings"
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
	return &DB{Tables: openExistingTables()}
}

func (db *DB) Close() {
	for _, table := range db.Tables {
		table.Pager.FlushToDisk()
	}
}

func (db *DB) Execute(stmt *statement.Statement) ([]Result, error) {
	switch stmt.StatementType {
	case statement.CreateTableStatement:
		if db.Tables[stmt.TableName] != nil {
			return []Result{}, fmt.Errorf("table [%s] already exists", stmt.TableName)
		}

		db.Tables[stmt.TableName] = storage.Open(stmt.TableName)
		return []Result{}, nil
	case statement.InsertStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		if db.Tables[stmt.TableName].RowCount == storage.TableMaxRows {
			return []Result{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		table.Pager.SaveAt(storage.Serialize(storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		}), storage.EndOf(table))
		table.RowCount++
		return []Result{}, nil
	case statement.SelectStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []Result{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		var results []Result
		cursor := storage.StartOf(table)
		for !cursor.EndOfTable {
			row, err := table.Pager.GetRow(cursor.RowNum)
			if err != nil {
				panic("could not fetch row: " + err.Error())
			}
			if row == nil {
				break
			}
			results = append(results, Result{
				Id:       row.Id,
				Username: row.Username,
				Email:    row.Email,
				PageId:   cursor.RowNum / storage.RowsPerPage,
				RowId:    cursor.RowNum,
			})

			cursor.Next()
		}

		return results[:], nil
	}
	return []Result{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
}

func openExistingTables() map[string]*storage.Table {
	entries, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("could not read db file: %s\n", err)
	}
	tables := map[string]*storage.Table{}
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), storage.DbFileNamePrefix) {
			tableName := strings.SplitAfter(entry.Name(), storage.DbFileNamePrefix)[1]
			tables[tableName] = storage.Open(tableName)
		}
	}
	return tables
}
