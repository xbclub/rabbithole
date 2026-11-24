//go:build darwin || freebsd || openbsd || netbsd || dragonfly

package main

import "syscall"

const soReusePortConst = syscall.SO_REUSEPORT
