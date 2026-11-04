// Command g4-probe provides bounded evidence collection for G4 acceptance.
// It does not determine whether the complete G4 gate has passed.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: g4-probe serve|check|observe|create [options]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "check":
		err = check(os.Args[2:])
	case "observe":
		err = observe(os.Args[2:])
	case "create":
		err = create(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "g4-probe:", err)
		os.Exit(1)
	}
}
