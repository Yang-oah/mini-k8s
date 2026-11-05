//go:build !linux

package proxy

import (
	"context"
	"errors"
)

// IPTables 仅能在 Linux 节点执行。
type IPTables struct{}

func NewIPTables(string) (*IPTables, error) { return nil, errors.New("iptables proxy requires Linux") }
func (*IPTables) Apply(context.Context, []Route) error {
	return errors.New("iptables proxy requires Linux")
}
func (*IPTables) Close() error { return nil }
