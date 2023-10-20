package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func main() {
	cmdRoot :=
		&cobra.Command{
			Use:     "pivo",
			Short:   "A sample of pivo usage.",
			Example: "",
			Version: "0.0.1-SNAPSHOT",
		}

	err := cmdRoot.Execute()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
		return
	}
}
