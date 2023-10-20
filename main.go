package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
	"github.com/pivovarit/pivodb/db/statement"
	"os"
	"strings"
)

const commandExit = "exit"

func executeStatement(s *statement.Statement) {
	fmt.Printf("%+v\n", s)
	switch s.StatementType {
	case statement.Insert:
		fmt.Println("Parsed INSERT statement")
	case statement.Select:
		fmt.Println("Parsed SELECT statement")
	}
	s = nil
}

func main() {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	var stmt *statement.Statement = nil
	root := prompt.New(
		func(s string) {
			if s == commandExit {
				os.Exit(0)
			} else if strings.HasPrefix(s, statement.Insert) {
				stmt = &statement.Statement{StatementType: statement.Insert}
				executeStatement(stmt)
			} else if strings.HasPrefix(s, statement.Select) {
				stmt = &statement.Statement{StatementType: statement.Select}
				executeStatement(stmt)
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
