//go:build unix

package main

import (
	"context"
	"net"
	"syscall"
)

func listenUDPReusePort(addr string) (*net.UDPConn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var setErr error
			if err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				// SO_REUSEPORT = 15 on Linux, 0x200 on Darwin; try best-effort via 15 / SO_REUSEADDR.
				setErr = setsockoptReusePort(fd)
			}); err != nil {
				return err
			}
			_ = setErr
			return nil
		},
	}
	pc, err := lc.ListenPacket(context.Background(), "udp", addr)
	if err != nil {
		return nil, err
	}
	return pc.(*net.UDPConn), nil
}
