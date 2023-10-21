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
	return &DB{Table: storage.Table{Rows: []storage.Row{}}}
}

func (db *DB) Execute(s *statement.Statement) {
	switch s.StatementType {
	case statement.Insert:
		db.Table.Rows = append(db.Table.Rows, s.RowToInsert)
	case statement.Select:
		for _, row := range db.Table.Rows {
			fmt.Printf("(%d,%s,%s)\n", row.Id, row.Username, row.Email)
		}
	}
}
