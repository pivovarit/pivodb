package main

import (
	pivo "github.com/pivovarit/pivodb/db"
	"github.com/pivovarit/pivodb/db/layout"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

func main() {
	layout.VerifyLayout()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	pivo.NewRepl().Run()
}
