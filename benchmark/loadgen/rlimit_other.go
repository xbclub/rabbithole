//go:build !unix

package main

import "math"

func raiseFDLimit() (soft, hard uint64) {
	return math.MaxUint64, math.MaxUint64
}

func isEMFile(error) (int, bool) {
	return 0, false
}
