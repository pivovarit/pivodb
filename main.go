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
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	var stmt *statement.Statement = nil
	root := prompt.New(
		func(s string) {
			if s == commandExit {
				os.Exit(0)
			} else if strings.HasPrefix(s, statement.InsertStatement) {
				params := strings.Fields(s)
				if params[2] != pivo.DefaultTableName {
					fmt.Println("Unrecognized table name: " + params[2])
					return
				}
				if (len(params)) != 6 {
					fmt.Println("Invalid statement: '" + s + "', try: 'insert into users {id} {username} {email}")
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

				stmt = statement.Insert(storage.Row{
					Id:       uint32(id),
					Username: username,
					Email:    email,
				})
				_, err = db.Execute(stmt)
				if err != nil {
					fmt.Printf("%s\n", err)
					return
				}
			} else if strings.HasPrefix(s, statement.SelectStatement) {
				params := strings.Fields(s)
				if (len(params)) != 4 {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from users")
					return
				}

				if params[1] != "*" || params[2] != "from" {
					fmt.Println("Invalid statement: '" + s + "', try: 'select * from users")
					return
				}

				if params[3] != pivo.DefaultTableName {
					fmt.Println("Unrecognized table name: " + params[3])
					return
				}

				stmt = statement.Select()
				result, err := db.Execute(stmt)
				for _, row := range result {
					fmt.Println(row.ToString())
				}
				if err != nil {
					fmt.Printf("%s\n", err)
				}
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
					Text:        statement.SelectStatement,
					Description: "SELECT SQL statement"},
				{
					Text:        statement.InsertStatement,
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
