package main

import (
	"fmt"
	"os"

	"github.com/max-trifonov/letopis/internal/cli"
	"github.com/max-trifonov/letopis/pkg/ext"
)

func main() {
	if err := cli.Main(os.Args[1:], ext.Defaults()); err != nil {
		fmt.Fprintln(os.Stderr, "letopis:", err)
		os.Exit(1)
	}
}
