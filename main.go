package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
	"os"
)

const commandExit = "exit"

func main() {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program")
	root := prompt.New(
		func(s string) {
			if s != commandExit {
				fmt.Println("Unrecognized command: " + s)
			} else {
				os.Exit(0)
			}
		},
		func(document prompt.Document) []prompt.Suggest {
			s := []prompt.Suggest{{Text: commandExit, Description: "Exit PivoDB"}}
			return prompt.FilterHasPrefix(s, document.GetWordBeforeCursor(), true)
		},
		prompt.OptionTitle("pivodb: golang-based SQLite-inspired database"),
		prompt.OptionPrefix(">>> "),
		prompt.OptionInputTextColor(prompt.Blue),
		prompt.OptionCompletionWordSeparator(completer.FilePathCompletionSeparator),
	)
	root.Run()
}
