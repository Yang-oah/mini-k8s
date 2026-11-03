package main

import (
	"context"
	"fmt"
	"os"

	"mini-k8s/internal/cli"
)

func main() {
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mini-k8s:", err)
		os.Exit(1)
	}
}
