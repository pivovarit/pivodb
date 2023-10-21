package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"os"
	"strconv"
	"strings"
)

const commandExit = "exit"
const defaultTableName = "users"

func executeStatement(s *statement.Statement, table *storage.Table) {
	//fmt.Printf("%+v\n", s)
	switch s.StatementType {
	case statement.Insert:
		table.Rows = append(table.Rows, s.RowToInsert)
	case statement.Select:
		for _, row := range table.Rows {
			fmt.Printf("Id: %d, Username: %s, Email: %s\n", row.Id, row.Username, row.Email)
		}
	}
	s = nil
}

var db = storage.Table{Rows: []storage.Row{}}

func main() {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	var stmt *statement.Statement = nil
	root := prompt.New(
		func(s string) {
			if s == commandExit {
				os.Exit(0)
			} else if strings.HasPrefix(s, statement.Insert) {
				params := strings.Fields(s)
				if params[2] != defaultTableName {
					fmt.Println("Unrecognized table name: " + params[2])
					return
				}
				if (len(params)) != 6 {
					fmt.Println("Invalid statement: '" + s + "', try: 'insert into users {id} {username} {email}" )
					return
				}
				id, err := strconv.Atoi(params[3])
				if err != nil {
					fmt.Printf("Id: [%s] needs to be numeric\n", params[3])
				}

				if len(params[5]) > storage.EmailSize {
					fmt.Println("Exceeded length for email column")
					return
				}

				var email [storage.EmailSize]byte
				copy(email[:], params[5])

				if len(params[4]) > storage.UsernameSize {
					fmt.Println("Exceeded length for username column")
					return
				}

				var username [storage.UsernameSize]byte
				copy(username[:], params[4])

				stmt = &statement.Statement{
					StatementType: statement.Insert,
					RowToInsert: storage.Row{
						Id:       uint32(id),
						Username: username,
						Email:    email,
					},
				}
				executeStatement(stmt, &db)
			} else if strings.HasPrefix(s, statement.Select) {
				params := strings.Fields(s)
				if (len(params)) != 4 {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from users")
					return
				}

				if params[1] != "*" || params[2] != "from" {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from users")
					return
				}

				if params[3] != defaultTableName {
					fmt.Println("Unrecognized table name: " + params[3])
					return
				}

				stmt = &statement.Statement{StatementType: statement.Select}
				executeStatement(stmt, &db)
			} else {
				fmt.Println("Unrecognized command: " + s)
			}
		},
		func(document prompt.Document) []prompt.Suggest {
			s := []prompt.Suggest{
				{
					Text:        commandExit,
					Description: "Exit PivoDB"},
				{
					Text:        statement.Select,
					Description: "SELECT SQL statement"},
				{
					Text:        statement.Insert,
					Description: "INSERT SQL statement"},
			}
			return prompt.FilterHasPrefix(s, document.GetWordBeforeCursor(), true)
		},
		prompt.OptionTitle("pivodb: golang-based SQLite-inspired database"),
		prompt.OptionPrefix("> "),
		prompt.OptionInputTextColor(prompt.Blue),
		prompt.OptionCompletionWordSeparator(completer.FilePathCompletionSeparator),
	)
	root.Run()
}
