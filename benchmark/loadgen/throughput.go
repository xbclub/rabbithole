package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// 256 KiB per-connection buffer keeps memory cache-hot and eliminates cross-goroutine
// data races / false sharing during concurrent multi-stream runs.
const thpBufSize = 256 << 10

const dialTimeout = 10 * time.Second

// Zero-filled read-only chunk shared solely for outbound writes (no mutations).
var readonlyZeroChunk = make([]byte, thpBufSize)

type thpResult struct {
	Role     string  `json:"role"`
	Dir      string  `json:"dir,omitempty"`
	Conns    int     `json:"conns"`
	OK       int     `json:"ok"`
	Failed   int     `json:"failed"`
	Bytes    int64   `json:"bytes"`
	Gbps     float64 `json:"gbps"`
	ElapsedS float64 `json:"elapsed_s"`
}

func emitThp(r thpResult) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

// runSink accepts throughput connections up to maxConns until seconds elapse.
//
// Deliberately leaves OS socket buffers (SO_RCVBUF / SO_SNDBUF) untouched so
// Linux tcp_moderate_rcvbuf auto-tunes cwnd naturally (calling SetReadBuffer(4MiB)
// previously triggered Linux autotuning disablement and cwnd collapse to ~94,
// capping single-stream throughput at ~1.15-1.6 Gbps).
//
// Each connection begins with an optional 1-byte direction header:
//   - 'U' (0x55): client uploads -> sink drains into a private per-conn buffer
//   - 'D' (0x44): client downloads -> sink writes continuous payload until EOF/stop
//   - any other byte: backwards-compatible raw upload fallback (first byte counted)
func runSink(port int, seconds float64, maxConns int) int {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		return 1
	}
	defer ln.Close()

	var total atomic.Int64
	var accepted atomic.Int32
	var wg sync.WaitGroup

	deadline := time.Now().Add(time.Duration(seconds * float64(time.Second)))
	stop := make(chan struct{})
	timer := time.AfterFunc(time.Until(deadline), func() {
		close(stop)
		if tl, ok := ln.(*net.TCPListener); ok {
			_ = tl.SetDeadline(time.Now())
		} else {
			_ = ln.Close()
		}
	})
	defer timer.Stop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
			default:
			}
			break
		}
		if int(accepted.Load()) >= maxConns {
			_ = conn.Close()
			continue
		}
		accepted.Add(1)
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()

			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}

			// Cancel blocking read/write immediately when sink duration ends.
			doneConn := make(chan struct{})
			defer close(doneConn)
			go func() {
				select {
				case <-stop:
					_ = c.SetDeadline(time.Now())
				case <-doneConn:
				}
			}()

			// Read 1-byte direction tag ('U' or 'D').
			var hdr [1]byte
			_ = c.SetReadDeadline(deadline)
			n, err := io.ReadFull(c, hdr[:])
			if err != nil || n == 0 {
				return
			}
			_ = c.SetReadDeadline(time.Time{})

			switch hdr[0] {
			case 'D':
				// Reverse (download) mode: sink streams continuously to src.
				for {
					select {
					case <-stop:
						return
					default:
					}
					nw, werr := c.Write(readonlyZeroChunk)
					if nw > 0 {
						total.Add(int64(nw))
					}
					if werr != nil {
						return
					}
				}
			case 'U':
				// Forward (upload) mode: drain into per-goroutine buffer.
				buf := make([]byte, thpBufSize)
				for {
					nr, rerr := c.Read(buf)
					if nr > 0 {
						total.Add(int64(nr))
					}
					if rerr != nil {
						return
					}
				}
			default:
				// Backwards-compatible raw stream: first byte already belongs to payload.
				total.Add(int64(n))
				buf := make([]byte, thpBufSize)
				for {
					nr, rerr := c.Read(buf)
					if nr > 0 {
						total.Add(int64(nr))
					}
					if rerr != nil {
						return
					}
				}
			}
		}(conn)
	}

	wg.Wait()
	el := time.Until(deadline)
	elapsed := seconds - el.Seconds()
	if elapsed <= 0 {
		elapsed = seconds
	}
	n := int(accepted.Load())
	emitThp(thpResult{
		Role:     "sink",
		Conns:    n,
		OK:       n,
		Bytes:    total.Load(),
		Gbps:     float64(total.Load()) * 8 / elapsed / 1e9,
		ElapsedS: elapsed,
	})
	return 0
}

// runSrc dials conns independent TCP connections to addr and measures upload
// (default) or download (reverse == true, like `iperf3 -R`) for seconds.
//
// All connections dial before the timing window starts, wait on a barrier so dial/
// tunnel setup latency does not skew short windows, then exchange the 1-byte tag
// ('U' or 'D') and transfer for the exact deadline.
func runSrc(addr string, seconds float64, conns int, reverse bool) int {
	var total atomic.Int64
	var okCount, failCount atomic.Int32
	var readyWg sync.WaitGroup
	var wg sync.WaitGroup
	readyWg.Add(conns)

	startCh := make(chan struct{})
	var startMu sync.Mutex
	var deadline time.Time

	dirTag := byte('U')
	dirName := "upload"
	if reverse {
		dirTag = 'D'
		dirName = "download"
	}

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := net.Dialer{Timeout: dialTimeout}
			conn, err := d.Dial("tcp", addr)
			if err != nil {
				failCount.Add(1)
				readyWg.Done()
				return
			}
			defer conn.Close()
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			okCount.Add(1)
			readyWg.Done()

			<-startCh

			startMu.Lock()
			dl := deadline
			startMu.Unlock()
			if time.Now().After(dl) {
				return
			}

			// Send 1-byte direction header inside timing window so sink does not
			// pre-fill tcp buffers for download before t0.
			_ = conn.SetDeadline(dl)
			if _, err := conn.Write([]byte{dirTag}); err != nil {
				return
			}
			_ = conn.SetDeadline(time.Time{})

			if reverse {
				_ = conn.SetReadDeadline(dl)
				buf := make([]byte, thpBufSize)
				var recvd int64
				for {
					nr, rerr := conn.Read(buf)
					if nr > 0 {
						recvd += int64(nr)
					}
					if rerr != nil {
						total.Add(recvd)
						return
					}
				}
			}

			_ = conn.SetWriteDeadline(dl)
			var sent int64
			for {
				nw, werr := conn.Write(readonlyZeroChunk)
				if nw > 0 {
					sent += int64(nw)
				}
				if werr != nil {
					total.Add(sent)
					return
				}
			}
		}()
	}

	readyWg.Wait()
	t0 := time.Now()
	startMu.Lock()
	deadline = t0.Add(time.Duration(seconds * float64(time.Second)))
	startMu.Unlock()
	close(startCh)

	wg.Wait()
	elapsed := time.Since(t0).Seconds()
	if elapsed <= 0 {
		elapsed = seconds
	}
	ok := int(okCount.Load())
	bytesTotal := total.Load()
	emitThp(thpResult{
		Role:     "src",
		Dir:      dirName,
		Conns:    conns,
		OK:       ok,
		Failed:   int(failCount.Load()),
		Bytes:    bytesTotal,
		Gbps:     float64(bytesTotal) * 8 / elapsed / 1e9,
		ElapsedS: elapsed,
	})
	if ok == 0 || bytesTotal == 0 {
		return 1
	}
	return 0
}

func cmdSrc(args []string) int {
	fs := flag.NewFlagSet("src", flag.ContinueOnError)
	rev := fs.Bool("R", false, "reverse mode (download from sink)")
	revLong := fs.Bool("reverse", false, "reverse mode (download from sink)")

	// Accept -R / --reverse anywhere (before or after positional args, matching iperf3).
	var flagArgs, posArgs []string
	for _, a := range args {
		if a == "-R" || a == "--reverse" {
			flagArgs = append(flagArgs, a)
		} else if len(a) > 0 && a[0] == '-' {
			flagArgs = append(flagArgs, a)
		} else {
			posArgs = append(posArgs, a)
		}
	}
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	posArgs = append(posArgs, fs.Args()...)
	if len(posArgs) < 3 {
		fmt.Fprintln(os.Stderr, "src needs [-R] <host:port> <seconds> <conns>")
		return 2
	}
	sec, err := parsePosFloat(posArgs[1], "seconds")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	conns, err := parsePosInt(posArgs[2], "conns")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return runSrc(posArgs[0], sec, conns, *rev || *revLong)
}

func cmdSink(args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "sink needs <port> <seconds> [maxConns]")
		return 2
	}
	port, err := parsePosInt(args[0], "port")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	sec, err := parsePosFloat(args[1], "seconds")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	maxConns := 4096
	if len(args) >= 3 {
		if v, err := parsePosInt(args[2], "maxConns"); err == nil {
			maxConns = v
		}
	}
	return runSink(port, sec, maxConns)
}
