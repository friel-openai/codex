package main

import (
	"context"
	"log"
	"os"

	"github.com/friel-openai/codex/layerctl/internal/cli"
)

func main() {
	logger := log.New(os.Stderr, "layerctl: ", 0)
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}
