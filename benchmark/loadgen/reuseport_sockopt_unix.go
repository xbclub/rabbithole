//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package main

import "syscall"

// setsockoptReusePort enables SO_REUSEPORT where defined in x/ or OS headers,
// keeping zero external Go dependencies.
func setsockoptReusePort(fd uintptr) error {
	// On Linux amd64/arm64 SO_REUSEPORT is 15; on Darwin/BSD it is 0x200.
	// Using a tiny OS-split file avoids any mismatch. See reuseport_sockopt_*.go.
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePortConst, 1)
}
