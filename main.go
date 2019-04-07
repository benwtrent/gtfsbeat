package main

import (
	"os"

	"github.com/benwtrent/gtfsbeat/cmd"

	_ "github.com/benwtrent/gtfsbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
