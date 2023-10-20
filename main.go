package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
	"os"
	"strings"
)

const commandExit = "exit"
const statementInsert = "insert"
const statementSelect = "select"

type StatementType string
type Statement struct {
	StatementType StatementType
}

func executeStatement(s *Statement) {
	fmt.Printf("%+v\n", s)
	switch s.StatementType {
	case statementInsert:
		fmt.Println("Parsed INSERT statement")
	case statementSelect:
		fmt.Println("Parsed SELECT statement")
	}
	s = nil
}

func main() {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	var statement *Statement = nil
	root := prompt.New(
		func(s string) {
			if s == commandExit {
				os.Exit(0)
			} else if strings.HasPrefix(s, statementInsert) {
				statement = &Statement{StatementType: statementInsert}
				executeStatement(statement)
			} else if strings.HasPrefix(s, statementSelect) {
				statement = &Statement{StatementType: statementSelect}
				executeStatement(statement)
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
					Text:        statementSelect,
					Description: "SELECT SQL statement"},
				{
					Text:        statementInsert,
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
