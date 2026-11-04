//go:build !linux

package runtime

import (
	"errors"
	"os"
)

func acquireLock(string) (*os.File, error) { return nil, errors.New("CRI runtime requires Linux") }
func releaseLock(f *os.File) error         { return f.Close() }
