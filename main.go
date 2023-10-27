package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
	"github.com/pivovarit/pivodb/db"
	"github.com/pivovarit/pivodb/db/statement"
	"github.com/pivovarit/pivodb/db/storage"
	"os"
	"strconv"
	"strings"
)

const commandExit = "exit"

var db = pivo.New()

func main() {
	defer db.Close()
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	var stmt *statement.Statement = nil
	root := prompt.New(
		func(s string) {
			if s == commandExit {
				db.Close()
				os.Exit(0)
			}

			stmtType := statement.ParseStatementType(s)
			if stmtType == nil {
				fmt.Printf("Unrecognized command: %s\n", s)
				return
			}

			switch *stmtType {
			case statement.TablesStatement:
				result, err := db.Execute(statement.Tables())
				if err != nil {
					fmt.Printf("%s\n", err)
				}

				for _, row := range result {
					fmt.Println(row.GetString("name"))
				}
			case statement.InsertStatement:
				params := strings.Fields(s)
				if (len(params)) != 6 {
					fmt.Println("Invalid statement: '" + s + "', try: 'insert into users {id} {username} {email}")
					return
				}

				tableName := params[2]
				id, err := strconv.Atoi(params[3])
				if err != nil {
					fmt.Printf("Id: [%s] needs to be numeric\n", params[3])
					return
				}

				if len(params[5]) > storage.EmailSize {
					fmt.Println("Exceeded length for email column")
					return
				}

				email := params[5]

				if len(params[4]) > storage.UsernameSize {
					fmt.Println("Exceeded length for username column")
					return
				}

				username := params[4]

				stmt = statement.Insert(storage.Row{
					Id:       uint32(id),
					Username: username,
					Email:    email,
				}, tableName)
				_, err = db.Execute(stmt)
				if err != nil {
					fmt.Printf("%s\n", err)
					return
				}
			case statement.SelectStatement:
				params := strings.Fields(s)
				if (len(params)) != 4 {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from <table>")
					return
				}

				tableName := params[3]

				if params[1] != "*" || params[2] != "from" {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from <table>")
					return
				}

				stmt = statement.Select(tableName)
				result, err := db.Execute(stmt)
				for _, row := range result {
					fmt.Println(row.ToString())
				}
				if err != nil {
					fmt.Printf("%s\n", err)
				}
			case statement.CreateTableStatement:
				params := strings.Fields(s)
				if (len(params)) != 3 {
					fmt.Println("Invalid statement: '" + s + "', try: 'create table <table>")
					return
				}

				tableName := params[2]
				stmt = statement.CreateTable(tableName)
				_, err := db.Execute(stmt)
				if err != nil {
					fmt.Printf("%s\n", err)
				}
			default:
				panic(fmt.Errorf("no implemented support for %s", *stmtType))
			}
		},
		func(document prompt.Document) []prompt.Suggest {
			s := []prompt.Suggest{
				{
					Text:        commandExit,
					Description: "Exit PivoDB"},
				{
					Text:        statement.TablesStatement.Value(),
					Description: "List all tables"},
				{
					Text:        statement.CreateTableStatement.Value(),
					Description: "CREATE TABLE SQL statement"},
				{
					Text:        statement.SelectStatement.Value(),
					Description: "SELECT SQL statement"},
				{
					Text:        statement.InsertStatement.Value(),
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
