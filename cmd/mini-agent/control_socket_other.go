//go:build !linux

package main

import (
	"errors"
	"net"
)

func prepareControlSocket(string, string) (net.Listener, func(), error) {
	return nil, nil, errors.New("Agent control socket requires Linux")
}
