package pivo

import (
	"fmt"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"log"
	"os"
	"strconv"
	"strings"
)

type ResultSet struct {
	columns map[string]string
	keys    []string
	PageId  uint32
	RowId   uint32
}

func (r *ResultSet) GetString(column string) string {
	return r.columns[column]
}

func (r *ResultSet) ToString() string {
	var b strings.Builder

	for _, key := range r.keys {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		_, err := fmt.Fprintf(&b, "%s=%s", key, r.columns[key])
		if err != nil {
			return ""
		}
	}
	return fmt.Sprintf("(%s) [rowId: %d, pageId: %d]", b.String(), r.RowId, r.PageId)
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

func (db *DB) Execute(stmt *statement.Statement) ([]ResultSet, error) {
	switch stmt.StatementType {
	case statement.TablesStatement:
		var result []ResultSet

		for tableName := range db.Tables {
			result = append(result, ResultSet{columns: map[string]string{
				"name": tableName,
			}, keys: []string{"name"}})
		}

		return result, nil
	case statement.CreateTableStatement:
		if db.Tables[stmt.TableName] != nil {
			return []ResultSet{}, fmt.Errorf("table [%s] already exists", stmt.TableName)
		}

		db.Tables[stmt.TableName] = storage.Open(stmt.TableName)
		return []ResultSet{}, nil
	case statement.InsertStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []ResultSet{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		if db.Tables[stmt.TableName].RowCount == storage.TableMaxRows {
			return []ResultSet{}, fmt.Errorf("max row count reached: %d", storage.TableMaxRows)
		}
		table.Pager.SaveAt(storage.Serialize(storage.Row{
			Id:       stmt.RowToInsert.Id,
			Username: stmt.RowToInsert.Username,
			Email:    stmt.RowToInsert.Email,
		}), storage.EndOf(table))
		table.RowCount++
		return []ResultSet{}, nil
	case statement.SelectStatement:
		table := db.Tables[stmt.TableName]
		if table == nil {
			return []ResultSet{}, fmt.Errorf("table [%s] does not exist", stmt.TableName)
		}

		var results []ResultSet
		cursor := storage.StartOf(table)
		for !cursor.EndOfTable {
			row, err := table.Pager.GetRow(cursor.RowNum)
			if err != nil {
				panic("could not fetch row: " + err.Error())
			}
			if row == nil {
				break
			}
			results = append(results, ResultSet{
				columns: map[string]string{
					"id":       strconv.Itoa(int(row.Id)),
					"username": row.Username,
					"email":    row.Email,
				},
				keys:   []string{"id", "username", "email"},
				PageId: cursor.RowNum / storage.RowsPerPage,
				RowId:  cursor.RowNum,
			})

			cursor.Next()
		}

		return results[:], nil
	}
	return []ResultSet{}, fmt.Errorf("unrecognized statement: %s", stmt.StatementType)
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
