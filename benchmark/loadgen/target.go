package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const patternChunk = 1 << 20

var patternBlock = func() []byte {
	b := make([]byte, patternChunk)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}()

var httpBody4KiB = bytes.Repeat([]byte{'x'}, 4096)

func fillPatternSlice(dst []byte, offset int64) {
	n := len(dst)
	off := 0
	for off < n {
		pos := int((offset + int64(off)) % patternChunk)
		take := n - off
		if avail := patternChunk - pos; take > avail {
			take = avail
		}
		copy(dst[off:off+take], patternBlock[pos:pos+take])
		off += take
	}
}

func sendAllPattern(w io.Writer, total int64, mode string) error {
	buf := make([]byte, 64<<10)
	var sent int64
	var rngBlk int64 = -1
	var rngBuf []byte
	if mode == "random" {
		rngBuf = make([]byte, patternChunk)
	}

	for sent < total {
		n := int64(len(buf))
		if remain := total - sent; n > remain {
			n = remain
		}
		chunk := buf[:n]
		switch mode {
		case "zeros":
			// buf stays all zeros
		case "random":
			blk := sent / patternChunk
			if blk != rngBlk {
				rng := rand.New(rand.NewSource(blk))
				for i := range rngBuf {
					rngBuf[i] = byte(rng.Intn(256))
				}
				rngBlk = blk
			}
			off := 0
			for off < int(n) {
				bidx := (sent + int64(off)) / patternChunk
				if bidx != rngBlk {
					rng := rand.New(rand.NewSource(bidx))
					for i := range rngBuf {
						rngBuf[i] = byte(rng.Intn(256))
					}
					rngBlk = bidx
				}
				pos := int((sent + int64(off)) % patternChunk)
				take := int(n) - off
				if avail := patternChunk - pos; take > avail {
					take = avail
				}
				copy(chunk[off:off+take], rngBuf[pos:pos+take])
				off += take
			}
		default:
			fillPatternSlice(chunk, sent)
		}
		if _, err := w.Write(chunk); err != nil {
			return err
		}
		sent += n
	}
	return nil
}

func handleTargetTCPConn(conn net.Conn) {
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	r := bufio.NewReaderSize(conn, 64<<10)
	for {
		line, err := r.ReadString('\n')
		if len(line) > 256 && err != nil {
			return
		}
		if err != nil && len(line) == 0 {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			if err != nil {
				return
			}
			continue
		}
		op := strings.ToUpper(fields[0])
		switch op {
		case "PING":
			if _, werr := conn.Write([]byte("PONG\n")); werr != nil {
				return
			}
		case "SEND":
			if len(fields) < 2 {
				return
			}
			total, perr := strconv.ParseInt(fields[1], 10, 64)
			if perr != nil || total < 0 {
				return
			}
			mode := "pattern"
			if len(fields) > 2 {
				mode = fields[2]
			}
			_ = sendAllPattern(conn, total, mode)
			return
		case "RECV":
			if len(fields) < 2 {
				return
			}
			total, perr := strconv.ParseInt(fields[1], 10, 64)
			if perr != nil || total < 0 {
				return
			}
			buf := make([]byte, 64<<10)
			exp := make([]byte, 64<<10)
			var got int64
			for got < total {
				n := int64(len(buf))
				if remain := total - got; n > remain {
					n = remain
				}
				if _, rerr := io.ReadFull(r, buf[:n]); rerr != nil {
					return
				}
				fillPatternSlice(exp[:n], got)
				if !bytes.Equal(buf[:n], exp[:n]) {
					_, _ = conn.Write([]byte("ERR corrupt\n"))
					return
				}
				got += n
			}
			if _, werr := fmt.Fprintf(conn, "OK %d\n", total); werr != nil {
				return
			}
		case "SLEEP":
			if len(fields) < 2 {
				return
			}
			sec, perr := strconv.ParseFloat(fields[1], 64)
			if perr != nil || sec < 0 {
				return
			}
			time.Sleep(time.Duration(sec * float64(time.Second)))
			return
		case "ECHO":
			buf := make([]byte, 65536)
			for {
				n, rerr := r.Read(buf)
				if n > 0 {
					if _, werr := conn.Write(buf[:n]); werr != nil {
						return
					}
				}
				if rerr != nil {
					return
				}
			}
		default:
			if _, werr := conn.Write([]byte("ERR unknown\n")); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

type targetServer struct {
	tcpLn    net.Listener
	udpConns []*net.UDPConn
	httpSrv  *http.Server
	httpLn   net.Listener
	wg       sync.WaitGroup
}

func startTargetServer(tcpPort, udpPort, httpPort, udpWorkers int) (*targetServer, []string, error) {
	ts := &targetServer{}
	var started []string

	if tcpPort > 0 {
		ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", tcpPort))
		if err != nil {
			ts.close()
			return nil, nil, err
		}
		ts.tcpLn = ln
		_, realPort, _ := net.SplitHostPort(ln.Addr().String())
		started = append(started, "tcp:"+realPort)
		ts.wg.Add(1)
		go func() {
			defer ts.wg.Done()
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go handleTargetTCPConn(c)
			}
		}()
	}

	if udpPort > 0 {
		if udpWorkers < 1 {
			udpWorkers = 1
		}
		bindAddr := fmt.Sprintf("0.0.0.0:%d", udpPort)
		var actualAddr string
		for i := 0; i < udpWorkers; i++ {
			addrToBind := bindAddr
			if i > 0 && udpPort == 0 && actualAddr != "" {
				addrToBind = actualAddr
			}
			uc, err := listenUDPReusePort(addrToBind)
			if err != nil {
				ts.close()
				return nil, nil, err
			}
			if i == 0 {
				actualAddr = uc.LocalAddr().String()
				_, realPort, _ := net.SplitHostPort(actualAddr)
				started = append(started, "udp:"+realPort)
			}
			ts.udpConns = append(ts.udpConns, uc)
			ts.wg.Add(1)
			go func(sock *net.UDPConn) {
				defer ts.wg.Done()
				buf := make([]byte, 65536)
				for {
					n, raddr, rerr := sock.ReadFromUDP(buf)
					if n > 0 && raddr != nil {
						_, _ = sock.WriteToUDP(buf[:n], raddr)
					}
					if rerr != nil {
						return
					}
				}
			}(uc)
		}
	}

	if httpPort > 0 {
		ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", httpPort))
		if err != nil {
			ts.close()
			return nil, nil, err
		}
		ts.httpLn = ln
		_, realPort, _ := net.SplitHostPort(ln.Addr().String())
		started = append(started, "http:"+realPort)

		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(httpBody4KiB)
		})
		ts.httpSrv = &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 30 * time.Second,
		}
		ts.wg.Add(1)
		go func() {
			defer ts.wg.Done()
			if err := ts.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintf(os.Stderr, "http target error: %v\n", err)
			}
		}()
	}

	return ts, started, nil
}

func (ts *targetServer) close() {
	if ts.tcpLn != nil {
		_ = ts.tcpLn.Close()
	}
	for _, uc := range ts.udpConns {
		_ = uc.Close()
	}
	if ts.httpSrv != nil {
		_ = ts.httpSrv.Close()
	} else if ts.httpLn != nil {
		_ = ts.httpLn.Close()
	}
	ts.wg.Wait()
}

func cmdTarget(args []string) int {
	fs := flag.NewFlagSet("target", flag.ContinueOnError)
	tcpPort := fs.Int("tcp-port", 0, "TCP line-protocol port")
	udpPort := fs.Int("udp-port", 0, "UDP echo port")
	httpPort := fs.Int("http-port", 0, "HTTP/1.1 keep-alive target port")
	udpWorkers := fs.Int("udp-workers", 4, "UDP worker goroutines")
	readyFile := fs.String("ready-file", "", "write readiness marker to file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *tcpPort == 0 && *udpPort == 0 && *httpPort == 0 {
		fmt.Fprintln(os.Stderr, "未指定任何监听端口")
		return 2
	}

	ts, started, err := startTargetServer(*tcpPort, *udpPort, *httpPort, *udpWorkers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "target listen error: %v\n", err)
		return 1
	}
	defer ts.close()

	readyLine := "target ready " + strings.Join(started, " ")
	fmt.Println(readyLine)
	if *readyFile != "" {
		if werr := os.WriteFile(*readyFile, []byte(strings.Join(started, " ")), 0o644); werr != nil {
			fmt.Fprintf(os.Stderr, "ready-file error: %v\n", werr)
		}
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	return 0
}

// Keep unused encoding/binary import away if scenarios.go uses it separately;
// target.go uses net/http and pattern helpers cleanly.
var _ = binary.BigEndian
