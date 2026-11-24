//go:build !unix

package main

import "net"

func listenUDPReusePort(addr string) (*net.UDPConn, error) {
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp", uaddr)
}
