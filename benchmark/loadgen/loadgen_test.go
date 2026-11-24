package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func buildTestBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "loadgen")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	return bin
}

func TestThroughputUploadAndDownload(t *testing.T) {
	// Start sink on ephemeral port by resolving an available port first.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	var sinkErr int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sinkErr = runSink(port, 4.0, 64)
	}()

	// Wait for sink to listen
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, derr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if derr == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sink never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if rc := runSrc(addr, 0.5, 2, false); rc != 0 {
		t.Fatalf("upload runSrc returned %d", rc)
	}
	if rc := runSrc(addr, 0.5, 2, true); rc != 0 {
		t.Fatalf("download runSrc returned %d", rc)
	}
	wg.Wait()
	if sinkErr != 0 {
		t.Fatalf("runSink returned %d", sinkErr)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()
	return port
}

func TestTargetAndScenariosEndToEnd(t *testing.T) {
	tcpPort := freeTCPPort(t)
	udpPort := freeUDPPort(t)
	httpPort := freeTCPPort(t)
	ts, _, err := startTargetServer(tcpPort, udpPort, httpPort, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.close()

	// 1) Verify TCP line protocol PING / SEND / RECV
	conn, err := dialTCPNoDelay(net.JoinHostPort("127.0.0.1", strconv.Itoa(tcpPort)), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	if _, err := conn.Write([]byte("PING\n")); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil || line != "PONG\n" {
		t.Fatalf("unexpected PING resp: %q err=%v", line, err)
	}
	exp := make([]byte, 4096)
	fillPatternSlice(exp, 0)
	if _, err := fmt.Fprintf(conn, "RECV %d\n", len(exp)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(exp); err != nil {
		t.Fatal(err)
	}
	line, err = r.ReadString('\n')
	if err != nil || line != "OK 4096\n" {
		t.Fatalf("unexpected RECV resp: %q err=%v", line, err)
	}
	_ = conn.Close()

	// 2) Exercise all CLI subcommands via compiled binary to also test flag order
	bin := buildTestBinary(t)
	cases := [][]string{
		{"--host", "127.0.0.1", "--port", strconv.Itoa(tcpPort), "latency", "--count", "20", "--warmup", "5"},
		{"latency", "--host", "127.0.0.1", "--port", strconv.Itoa(tcpPort), "--newconn", "--count", "10"},
		{"--host", "127.0.0.1", "--port", strconv.Itoa(tcpPort), "connect", "--connections", "20", "--parallel", "4"},
		{"--host", "127.0.0.1", "--port", strconv.Itoa(udpPort), "udp", "--sources", "2", "--size", "1200", "--duration", "0.5", "--window", "8", "--drain", "0.3"},
		{"--host", "127.0.0.1", "--port", strconv.Itoa(httpPort), "http", "--connections", "4", "--duration", "0.5", "--host-header", "bench.test"},
		{"--host", "127.0.0.1", "--port", strconv.Itoa(tcpPort), "concurrent", "--connections", "16", "--hold", "1"},
	}
	for _, argv := range cases {
		out, err := exec.Command(bin, argv...).CombinedOutput()
		if err != nil {
			t.Fatalf("cmd %v failed: %v\n%s", argv, err, out)
		}
		if len(bytes.TrimSpace(out)) == 0 || !bytes.Contains(out, []byte("{")) {
			t.Fatalf("cmd %v empty/non-json output: %s", argv, out)
		}
	}
}

func TestAcceptOnlyConcurrentZeroVerified(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Keep open without replying to test timeout/verify failure cleanly
			_ = c
		}
	}()

	bin := buildTestBinary(t)
	port := ln.Addr().(*net.TCPAddr).Port
	out, err := exec.Command(bin,
		"--host", "127.0.0.1", "--port", strconv.Itoa(port), "--timeout", "1",
		"concurrent", "--connections", "8", "--hold", "1",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("concurrent failed: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte(`"established":8`)) || !bytes.Contains(out, []byte(`"verified":0`)) {
		t.Fatalf("expected established=8 verified=0, got: %s", out)
	}
}

func TestUDPDelayedAndDupAccounting(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	go func() {
		buf := make([]byte, 65536)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 8 {
				continue
			}
			seq := binary.BigEndian.Uint64(buf[:8])
			pkt := append([]byte(nil), buf[:n]...)
			if seq < 5 {
				// Hold first 5 packets past send window, release during drain
				time.AfterFunc(450*time.Millisecond, func() {
					_, _ = pc.WriteTo(pkt, addr)
				})
				continue
			}
			_, _ = pc.WriteTo(pkt, addr)
			if seq == 6 {
				_, _ = pc.WriteTo(pkt, addr) // duplicate
			}
		}
	}()

	bin := buildTestBinary(t)
	port := pc.LocalAddr().(*net.UDPAddr).Port
	out, err := exec.Command(bin,
		"--host", "127.0.0.1", "--port", strconv.Itoa(port),
		"udp", "--size", "1200", "--duration", "0.3", "--window", "16", "--drain", "1.0",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("udp failed: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte(`"lost":0`)) || !bytes.Contains(out, []byte(`"dup":1`)) {
		t.Fatalf("expected lost=0 dup=1, got: %s", out)
	}
	_ = io.Discard
}
