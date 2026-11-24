//go:build unix

package main

import (
	"errors"
	"syscall"
)

func raiseFDLimit() (soft, hard uint64) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, 0
	}
	soft = uint64(rl.Cur)
	hard = uint64(rl.Max)
	if rl.Cur < rl.Max {
		try := rl
		try.Cur = rl.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &try); err == nil {
			soft = uint64(try.Cur)
		}
	}
	return soft, hard
}

func isEMFile(err error) (int, bool) {
	if errors.Is(err, syscall.EMFILE) {
		return int(syscall.EMFILE), true
	}
	if errors.Is(err, syscall.ENFILE) {
		return int(syscall.ENFILE), true
	}
	return 0, false
}
