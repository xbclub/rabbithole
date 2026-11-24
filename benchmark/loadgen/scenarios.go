package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func pct(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	idx := float64(n-1) * q
	lo := int(idx)
	hi := lo + 1
	if hi >= n {
		hi = n - 1
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

type summaryStats struct {
	Count    int     `json:"count"`
	MinMs    float64 `json:"min_ms,omitempty"`
	P50Ms    float64 `json:"p50_ms,omitempty"`
	P90Ms    float64 `json:"p90_ms,omitempty"`
	P99Ms    float64 `json:"p99_ms,omitempty"`
	P999Ms   float64 `json:"p999_ms,omitempty"`
	MaxMs    float64 `json:"max_ms,omitempty"`
	MeanMs   float64 `json:"mean_ms,omitempty"`
	StddevMs float64 `json:"stddev_ms,omitempty"`
}

func summarize(samples []float64) summaryStats {
	n := len(samples)
	if n == 0 {
		return summaryStats{Count: 0}
	}
	s := make([]float64, n)
	copy(s, samples)
	sort.Float64s(s)

	var sum float64
	for _, v := range s {
		sum += v
	}
	mean := sum / float64(n)
	var sq float64
	for _, v := range s {
		d := v - mean
		sq += d * d
	}
	stddev := math.Sqrt(sq / float64(n))

	return summaryStats{
		Count:    n,
		MinMs:    round4(s[0]),
		P50Ms:    round4(pct(s, 0.50)),
		P90Ms:    round4(pct(s, 0.90)),
		P99Ms:    round4(pct(s, 0.99)),
		P999Ms:   round4(pct(s, 0.999)),
		MaxMs:    round4(s[n-1]),
		MeanMs:   round4(mean),
		StddevMs: round4(stddev),
	}
}

func dialTCPNoDelay(addr string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return conn, nil
}

func emitJSON(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func emitError(msg string) {
	emitJSON(map[string]any{"error": msg})
}

// ---------- latency ----------

func cmdLatency(g globalOpts, args []string) int {
	fs := flag.NewFlagSet("latency", flag.ContinueOnError)
	registerGlobalFlags(fs, &g)
	count := fs.Int("count", 2000, "number of RTT samples")
	warmup := fs.Int("warmup", 50, "warmup pings on persistent mode")
	newconn := fs.Bool("newconn", false, "new connection per PING")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if g.port <= 0 {
		emitError("missing --port")
		return 1
	}

	addr := net.JoinHostPort(g.host, strconv.Itoa(g.port))
	timeout := time.Duration(g.timeout * float64(time.Second))
	samples := make([]float64, 0, *count)
	errorsCount := 0

	if *newconn {
		for i := 0; i < *count; i++ {
			t0 := time.Now()
			conn, err := dialTCPNoDelay(addr, timeout)
			if err != nil {
				errorsCount++
				continue
			}
			_ = conn.SetDeadline(time.Now().Add(timeout))
			if _, err := conn.Write([]byte("PING\n")); err != nil {
				_ = conn.Close()
				errorsCount++
				continue
			}
			var buf [64]byte
			n, rerr := conn.Read(buf[:])
			dtMs := time.Since(t0).Seconds() * 1000.0
			_ = conn.Close()
			if rerr == nil && bytes.HasPrefix(buf[:n], []byte("PONG")) {
				samples = append(samples, dtMs)
			} else {
				errorsCount++
			}
		}
	} else {
		conn, err := dialTCPNoDelay(addr, timeout)
		if err != nil {
			emitError(err.Error())
			return 1
		}
		defer conn.Close()
		r := bufio.NewReaderSize(conn, 4096)

		for i := 0; i < *warmup; i++ {
			_ = conn.SetDeadline(time.Now().Add(timeout))
			if _, err := conn.Write([]byte("PING\n")); err != nil {
				emitError(err.Error())
				return 1
			}
			if _, err := r.ReadString('\n'); err != nil {
				emitError(err.Error())
				return 1
			}
		}
		for i := 0; i < *count; i++ {
			t0 := time.Now()
			_ = conn.SetDeadline(time.Now().Add(timeout))
			if _, err := conn.Write([]byte("PING\n")); err != nil {
				errorsCount++
				break
			}
			line, rerr := r.ReadBytes('\n')
			dtMs := time.Since(t0).Seconds() * 1000.0
			if rerr == nil && bytes.HasPrefix(line, []byte("PONG")) {
				samples = append(samples, dtMs)
			} else {
				errorsCount++
			}
		}
	}

	st := summarize(samples)
	out := map[string]any{
		"count":  st.Count,
		"errors": errorsCount,
		"mode":   map[bool]string{true: "newconn", false: "persistent"}[*newconn],
	}
	if st.Count > 0 {
		out["min_ms"] = st.MinMs
		out["p50_ms"] = st.P50Ms
		out["p90_ms"] = st.P90Ms
		out["p99_ms"] = st.P99Ms
		out["p999_ms"] = st.P999Ms
		out["max_ms"] = st.MaxMs
		out["mean_ms"] = st.MeanMs
		out["stddev_ms"] = st.StddevMs
	}
	emitJSON(out)
	return 0
}

// ---------- connect ----------

func cmdConnect(g globalOpts, args []string) int {
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	registerGlobalFlags(fs, &g)
	connections := fs.Int("connections", 2000, "total short connections")
	parallel := fs.Int("parallel", 32, "concurrent worker goroutines")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if g.port <= 0 {
		emitError("missing --port")
		return 1
	}

	addr := net.JoinHostPort(g.host, strconv.Itoa(g.port))
	timeout := time.Duration(g.timeout * float64(time.Second))

	var claimed atomic.Int64
	var errorsCount atomic.Int64
	var mu sync.Mutex
	lats := make([]float64, 0, *connections)

	t0 := time.Now()
	var wg sync.WaitGroup
	workers := *parallel
	if workers < 1 {
		workers = 1
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLat := make([]float64, 0, (*connections/workers)+8)
			var localErr int64
			var buf [64]byte
			for {
				idx := claimed.Add(1)
				if int(idx) > *connections {
					break
				}
				c0 := time.Now()
				conn, err := dialTCPNoDelay(addr, timeout)
				if err != nil {
					localErr++
					continue
				}
				_ = conn.SetDeadline(time.Now().Add(timeout))
				if _, err := conn.Write([]byte("PING\n")); err != nil {
					_ = conn.Close()
					localErr++
					continue
				}
				n, rerr := conn.Read(buf[:])
				_ = conn.Close()
				if rerr != nil || !bytes.HasPrefix(buf[:n], []byte("PONG")) {
					localErr++
					continue
				}
				localLat = append(localLat, time.Since(c0).Seconds()*1000.0)
			}
			errorsCount.Add(localErr)
			mu.Lock()
			lats = append(lats, localLat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	dt := time.Since(t0).Seconds()
	rate := 0.0
	if dt > 0 {
		rate = round1(float64(len(lats)) / dt)
	}

	st := summarize(lats)
	out := map[string]any{
		"requested":  *connections,
		"completed":  len(lats),
		"errors":     errorsCount.Load(),
		"seconds":    round4(dt),
		"rate_per_s": rate,
		"lat_count":  st.Count,
	}
	if st.Count > 0 {
		out["lat_min_ms"] = st.MinMs
		out["lat_p50_ms"] = st.P50Ms
		out["lat_p90_ms"] = st.P90Ms
		out["lat_p99_ms"] = st.P99Ms
		out["lat_p999_ms"] = st.P999Ms
		out["lat_max_ms"] = st.MaxMs
		out["lat_mean_ms"] = st.MeanMs
		out["lat_stddev_ms"] = st.StddevMs
	}
	emitJSON(out)
	return 0
}

// ---------- http ----------

func cmdHTTP(g globalOpts, args []string) int {
	fs := flag.NewFlagSet("http", flag.ContinueOnError)
	registerGlobalFlags(fs, &g)
	connections := fs.Int("connections", 16, "concurrent HTTP/1.1 keep-alive clients")
	duration := fs.Float64("duration", 10.0, "test duration seconds")
	path := fs.String("path", "/", "request path")
	hostHeader := fs.String("host-header", "", "Host header override")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if g.port <= 0 {
		emitError("missing --port")
		return 1
	}

	addr := net.JoinHostPort(g.host, strconv.Itoa(g.port))
	urlStr := "http://" + addr + *path
	reqHost := *hostHeader
	timeout := time.Duration(g.timeout * float64(time.Second))

	stop := make(chan struct{})
	var mu sync.Mutex
	var totalOK int64
	var totalErr int64
	lats := make([]float64, 0, 16384)

	var wg sync.WaitGroup
	t0 := time.Now()
	timer := time.AfterFunc(time.Duration(*duration*float64(time.Second)), func() {
		close(stop)
	})
	defer timer.Stop()

	for w := 0; w < *connections; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr := &http.Transport{
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				DisableCompression:  true,
				DisableKeepAlives:   false,
				ForceAttemptHTTP2:   false,
				DialContext: (&net.Dialer{
					Timeout: timeout,
				}).DialContext,
			}
			defer tr.CloseIdleConnections()
			client := &http.Client{
				Transport: tr,
				Timeout:   timeout,
			}

			localLat := make([]float64, 0, 4096)
			var okCnt int64
			for {
				select {
				case <-stop:
					mu.Lock()
					totalOK += okCnt
					lats = append(lats, localLat...)
					mu.Unlock()
					return
				default:
				}
				req, err := http.NewRequest(http.MethodGet, urlStr, nil)
				if err != nil {
					mu.Lock()
					totalErr++
					totalOK += okCnt
					lats = append(lats, localLat...)
					mu.Unlock()
					return
				}
				if reqHost != "" {
					req.Host = reqHost
				}
				reqT0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					select {
					case <-stop:
					default:
						mu.Lock()
						totalErr++
						mu.Unlock()
					}
					continue
				}
				_, rerr := io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				reqDtMs := time.Since(reqT0).Seconds() * 1000.0
				if rerr != nil || resp.StatusCode != http.StatusOK {
					mu.Lock()
					totalErr++
					mu.Unlock()
					continue
				}
				okCnt++
				if len(localLat) < 200000 {
					localLat = append(localLat, reqDtMs)
				}
			}
		}()
	}

	wg.Wait()
	dt := time.Since(t0).Seconds()
	qps := 0.0
	if dt > 0 {
		qps = round1(float64(totalOK) / dt)
	}
	st := summarize(lats)
	out := map[string]any{
		"connections": *connections,
		"requests":    totalOK,
		"errors":      totalErr,
		"seconds":     round3(dt),
		"qps":         qps,
		"lat_count":   st.Count,
	}
	if st.Count > 0 {
		out["lat_min_ms"] = st.MinMs
		out["lat_p50_ms"] = st.P50Ms
		out["lat_p90_ms"] = st.P90Ms
		out["lat_p99_ms"] = st.P99Ms
		out["lat_p999_ms"] = st.P999Ms
		out["lat_max_ms"] = st.MaxMs
		out["lat_mean_ms"] = st.MeanMs
		out["lat_stddev_ms"] = st.StddevMs
	}
	emitJSON(out)
	return 0
}

// ---------- udp ----------

type udpRx struct {
	data []byte
	now  time.Time
}

type udpInflight struct {
	seq uint64
	ts  time.Time
}

func cmdUDP(g globalOpts, args []string) int {
	fs := flag.NewFlagSet("udp", flag.ContinueOnError)
	registerGlobalFlags(fs, &g)
	sources := fs.Int("sources", 1, "number of independent UDP source sockets")
	size := fs.Int("size", 1200, "UDP datagram size including 8B sequence header")
	duration := fs.Float64("duration", 10.0, "send duration seconds")
	window := fs.Int("window", 32, "max in-flight datagrams")
	drain := fs.Float64("drain", 5.0, "post-send drain window seconds (A8)")
	rto := fs.Float64("rto", 2.0, "in-flight pacing timeout seconds")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if g.port <= 0 {
		emitError("missing --port")
		return 1
	}

	const hdrLen = 8
	payloadSize := *size
	bodyLen := payloadSize - hdrLen
	if bodyLen <= 0 {
		emitJSON(map[string]any{
			"error":       fmt.Sprintf("--size 必须 > %d 字节(含 8B 序号头)", hdrLen),
			"packet_size": payloadSize,
		})
		return 0
	}

	nSrc := *sources
	if nSrc < 1 {
		nSrc = 1
	}
	raddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(g.host, strconv.Itoa(g.port)))
	if err != nil {
		emitError(err.Error())
		return 1
	}

	socks := make([]*net.UDPConn, 0, nSrc)
	closeSocks := func() {
		for _, sk := range socks {
			_ = sk.Close()
		}
	}
	for i := 0; i < nSrc; i++ {
		sk, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			closeSocks()
			emitError(err.Error())
			return 1
		}
		socks = append(socks, sk)
	}

	rxCh := make(chan udpRx, 8192)
	var rxWg sync.WaitGroup
	for _, sk := range socks {
		rxWg.Add(1)
		go func(s *net.UDPConn) {
			defer rxWg.Done()
			buf := make([]byte, 65536)
			for {
				n, rerr := s.Read(buf)
				if n > 0 {
					pkt := make([]byte, n)
					copy(pkt, buf[:n])
					rxCh <- udpRx{data: pkt, now: time.Now()}
				}
				if rerr != nil {
					return
				}
			}
		}(sk)
	}

	var sent, sendErrors, recvInTime, lateRecv, dup, corrupt uint64
	seen := make(map[uint64]struct{}, 4096)
	pending := make(map[uint64]time.Time, 4096)
	unacked := make([]udpInflight, 0, 256)
	var headIdx int
	lat := make([]float64, 0, 4096)

	rtoDur := time.Duration(*rto * float64(time.Second))
	sendDur := time.Duration(*duration * float64(time.Second))
	drainDur := time.Duration(*drain * float64(time.Second))

	expectedBody := make([]byte, bodyLen)
	txBuf := make([]byte, payloadSize)

	handlePkt := func(pkt []byte, rxNow time.Time, inWindow bool) {
		if len(pkt) != payloadSize {
			corrupt++
			return
		}
		seq := binary.BigEndian.Uint64(pkt[:hdrLen])
		fillPatternSlice(expectedBody, int64(seq)*4096)
		if !bytes.Equal(pkt[hdrLen:], expectedBody) {
			corrupt++
			return
		}
		if _, exists := seen[seq]; exists {
			dup++
			return
		}
		seen[seq] = struct{}{}
		if inWindow {
			recvInTime++
		} else {
			lateRecv++
		}
		if sendTS, ok := pending[seq]; ok {
			lat = append(lat, rxNow.Sub(sendTS).Seconds()*1000.0)
			delete(pending, seq)
		}
	}

	pruneUnacked := func(now time.Time) {
		for headIdx < len(unacked) {
			e := unacked[headIdx]
			if _, ok := seen[e.seq]; ok || now.Sub(e.ts) > rtoDur {
				headIdx++
				continue
			}
			break
		}
		if headIdx > 1024 && headIdx*2 >= len(unacked) {
			remaining := copy(unacked, unacked[headIdx:])
			unacked = unacked[:remaining]
			headIdx = 0
		}
	}

	drainReadyRx := func(inWindow bool) {
		for {
			select {
			case msg := <-rxCh:
				handlePkt(msg.data, msg.now, inWindow)
			default:
				return
			}
		}
	}

	startedUs := time.Now().UnixMicro()
	t0 := time.Now()
	deadline := t0.Add(sendDur)

	for {
		now := time.Now()
		if !now.Before(deadline) {
			break
		}
		drainReadyRx(true)
		now = time.Now()
		pruneUnacked(now)
		outstanding := len(unacked) - headIdx

		for outstanding < *window && time.Now().Before(deadline) {
			binary.BigEndian.PutUint64(txBuf[:hdrLen], sent)
			fillPatternSlice(txBuf[hdrLen:], int64(sent)*4096)
			_, werr := socks[int(sent%uint64(nSrc))].Write(txBuf)
			if werr != nil {
				sendErrors++
				if sent == 0 {
					closeSocks()
					rxWg.Wait()
					emitJSON(map[string]any{
						"error":       fmt.Sprintf("sendto 失败(报文 %dB 可能超过本机 MTU;Linux 隧道下建议 ≤ 60000): %v", payloadSize, werr),
						"packet_size": payloadSize,
						"send_errors": sendErrors,
					})
					return 0
				}
				break
			}
			ts := time.Now()
			pending[sent] = ts
			unacked = append(unacked, udpInflight{seq: sent, ts: ts})
			sent++
			outstanding++
		}

		drainReadyRx(true)
		now = time.Now()
		if !now.Before(deadline) {
			break
		}
		pruneUnacked(now)
		if len(unacked)-headIdx < *window {
			continue
		}

		// Window full: block on rxCh until either a reply arrives, oldest packet
		// hits RTO, or send deadline expires (zero 1ms polling overhead).
		waitUntil := deadline
		if headIdx < len(unacked) {
			if expiry := unacked[headIdx].ts.Add(rtoDur); expiry.Before(waitUntil) {
				waitUntil = expiry
			}
		}
		waitDur := time.Until(waitUntil)
		if waitDur <= 0 {
			continue
		}
		timer := time.NewTimer(waitDur)
		select {
		case msg := <-rxCh:
			timer.Stop()
			handlePkt(msg.data, msg.now, true)
			drainReadyRx(true)
		case <-timer.C:
		}
	}

	sendEnd := time.Now()
	drainUntil := sendEnd.Add(drainDur)
	for {
		remain := time.Until(drainUntil)
		if remain <= 0 {
			break
		}
		timer := time.NewTimer(remain)
		select {
		case msg := <-rxCh:
			timer.Stop()
			handlePkt(msg.data, msg.now, false)
			drainReadyRx(false)
		case <-timer.C:
		}
	}

	closeSocks()
	rxWg.Wait()
	drainReadyRx(false)

	endedUs := time.Now().UnixMicro()
	recvUnique := uint64(len(seen))
	lost := sent - recvUnique
	sendDt := sendEnd.Sub(t0).Seconds()
	if sendDt < 1e-9 {
		sendDt = 1e-9
	}
	lossPct := 0.0
	if sent > 0 {
		lossPct = round3(float64(lost) * 100.0 / float64(sent))
	}

	st := summarize(lat)
	out := map[string]any{
		"sent":          sent,
		"received":      recvInTime,
		"late_recv":     lateRecv,
		"dup":           dup,
		"corrupt":       corrupt,
		"recv_unique":   recvUnique,
		"lost":          lost,
		"loss_pct":      lossPct,
		"send_errors":   sendErrors,
		"packet_size":   payloadSize,
		"sources":       nSrc,
		"seconds":       round3(*duration),
		"drain_seconds": round3(*drain),
		"mbps":          round2(float64(recvUnique*uint64(payloadSize)*8) / sendDt / 1e6),
		"pps":           round1(float64(recvUnique) / sendDt),
		"started_us":    startedUs,
		"ended_us":      endedUs,
	}
	if st.Count > 0 {
		out["rtt_count"] = st.Count
		out["rtt_min_ms"] = st.MinMs
		out["rtt_p50_ms"] = st.P50Ms
		out["rtt_p90_ms"] = st.P90Ms
		out["rtt_p99_ms"] = st.P99Ms
		out["rtt_p999_ms"] = st.P999Ms
		out["rtt_max_ms"] = st.MaxMs
		out["rtt_mean_ms"] = st.MeanMs
		out["rtt_stddev_ms"] = st.StddevMs
	} else {
		out["rtt_count"] = 0
	}
	emitJSON(out)
	return 0
}

// ---------- concurrent ----------

type concFailure struct {
	Idx          int      `json:"idx"`
	LocalPort    int      `json:"local_port"`
	State        string   `json:"state"`
	Reason       string   `json:"reason"`
	Detail       string   `json:"detail"`
	ConnectAgeMs float64  `json:"connect_age_ms"`
	AwaitAgeMs   *float64 `json:"await_age_ms"`
	InbufHead    string   `json:"inbuf_head"`
}

func cmdConcurrent(g globalOpts, args []string) int {
	fs := flag.NewFlagSet("concurrent", flag.ContinueOnError)
	registerGlobalFlags(fs, &g)
	connections := fs.Int("connections", 256, "target concurrent connections")
	hold := fs.Float64("hold", 5.0, "hold duration in seconds (1 PING/PONG per round)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if g.port <= 0 {
		emitError("missing --port")
		return 1
	}

	n := *connections
	soft, hard := raiseFDLimit()
	if soft != math.MaxUint64 && uint64(n)+64 > soft {
		emitJSON(map[string]any{
			"env_error": map[string]any{
				"kind": "fd_limit",
				"hint": fmt.Sprintf("fd 上限不足:需要 ~%d,可用 %d(硬上限 %d);请提高 ulimit -n 后重试", n, soft, hard),
			},
			"connections": n,
			"fd_soft":     soft,
			"fd_hard":     hard,
		})
		return 1
	}

	addr := net.JoinHostPort(g.host, strconv.Itoa(g.port))
	connTimeout := time.Duration(g.timeout * float64(time.Second))
	verifyTimeout := 10 * time.Second
	holdTick := 1 * time.Second
	totalRounds := int(math.Round(*hold))
	if totalRounds < 1 {
		totalRounds = 1
	}

	var established atomic.Int64
	var initialVerified atomic.Int64
	var holdAlive atomic.Int64
	var verified atomic.Int64
	var connectFailed atomic.Int64
	var verifyFailed atomic.Int64
	var holdLost atomic.Int64
	var emfileErrno atomic.Int64
	aliveByRound := make([]atomic.Int64, totalRounds)

	var mu sync.Mutex
	connectLats := make([]float64, 0, n)
	verifyRtts := make([]float64, 0, n)
	failures := make([]concFailure, 0, 16)

	noteFailure := func(idx, lport int, state, reason, detail string, connT0, pingT0 time.Time, head string) {
		now := time.Now()
		if len(detail) > 120 {
			detail = detail[:120]
		}
		var awaitMs *float64
		if !pingT0.IsZero() {
			v := round1(now.Sub(pingT0).Seconds() * 1000.0)
			awaitMs = &v
		}
		mu.Lock()
		if len(failures) < 64 {
			failures = append(failures, concFailure{
				Idx:          idx,
				LocalPort:    lport,
				State:        state,
				Reason:       reason,
				Detail:       detail,
				ConnectAgeMs: round1(now.Sub(connT0).Seconds() * 1000.0),
				AwaitAgeMs:   awaitMs,
				InbufHead:    head,
			})
		}
		mu.Unlock()
	}

	abortCh := make(chan struct{})
	var abortOnce sync.Once
	triggerAbort := func(errno int) {
		emfileErrno.CompareAndSwap(0, int64(errno))
		abortOnce.Do(func() { close(abortCh) })
	}

	startedUs := time.Now().UnixMicro()
	t0 := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			connT0 := time.Now()
			d := net.Dialer{Timeout: connTimeout}
			conn, err := d.Dial("tcp", addr)
			if err != nil {
				if eno, ok := isEMFile(err); ok {
					triggerAbort(eno)
					return
				}
				connectFailed.Add(1)
				return
			}
			defer conn.Close()

			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			lport := 0
			if ta, ok := conn.LocalAddr().(*net.TCPAddr); ok {
				lport = ta.Port
			}

			established.Add(1)
			connLatMs := time.Since(connT0).Seconds() * 1000.0
			mu.Lock()
			connectLats = append(connectLats, connLatMs)
			mu.Unlock()

			// Watch abort signal so an EMFILE abort closes open conns cleanly.
			done := make(chan struct{})
			defer close(done)
			go func() {
				select {
				case <-abortCh:
					_ = conn.Close()
				case <-done:
				}
			}()

			r := bufio.NewReaderSize(conn, 4096)

			pingPong := func(state string) (float64, bool) {
				pingT0 := time.Now()
				_ = conn.SetDeadline(pingT0.Add(verifyTimeout))
				if _, werr := conn.Write([]byte("PING\n")); werr != nil {
					noteFailure(idx, lport, state, "senderr", werr.Error(), connT0, pingT0, "")
					return 0, false
				}
				line, rerr := r.ReadString('\n')
				if rerr != nil {
					reason := "readerr"
					if errorsIsEOF(rerr) {
						reason = "EOF"
					} else if isTimeoutErr(rerr) {
						reason = "timeout"
					}
					noteFailure(idx, lport, state, reason, rerr.Error(), connT0, pingT0, line)
					return 0, false
				}
				trimmed := bytes.TrimSpace([]byte(line))
				if !bytes.Equal(trimmed, []byte("PONG")) {
					noteFailure(idx, lport, state, "badline", string(trimmed), connT0, pingT0, line)
					return 0, false
				}
				return time.Since(pingT0).Seconds() * 1000.0, true
			}

			rttMs, ok := pingPong("ver_r")
			if !ok {
				verifyFailed.Add(1)
				return
			}
			initialVerified.Add(1)
			mu.Lock()
			verifyRtts = append(verifyRtts, rttMs)
			mu.Unlock()

			for roundIdx := 0; roundIdx < totalRounds; roundIdx++ {
				t := time.NewTimer(holdTick)
				select {
				case <-abortCh:
					t.Stop()
					holdLost.Add(1)
					return
				case <-t.C:
				}
				if _, ok := pingPong("hold"); !ok {
					holdLost.Add(1)
					return
				}
				aliveByRound[roundIdx].Add(1)
			}

			holdAlive.Add(1)
			verified.Add(1)
		}(i)
	}

	wg.Wait()
	endedUs := time.Now().UnixMicro()
	setupDt := time.Since(t0).Seconds()

	if eno := emfileErrno.Load(); eno != 0 {
		emitJSON(map[string]any{
			"env_error": map[string]any{
				"kind":        "emfile",
				"errno":       eno,
				"connections": n,
				"fd_soft":     soft,
				"fd_hard":     hard,
				"hint":        "压测端 fd 不足:请提高 ulimit -n(EMFILE 不得计入产品并发失败)",
			},
		})
		return 1
	}

	aliveSlice := make([]int64, totalRounds)
	for i := range aliveByRound {
		aliveSlice[i] = aliveByRound[i].Load()
	}

	est := established.Load()
	rate := 0.0
	if setupDt > 0 {
		rate = round1(float64(est) / setupDt)
	}
	totalFail := connectFailed.Load() + verifyFailed.Load() + holdLost.Load()
	stConn := summarize(connectLats)

	out := map[string]any{
		"connections":        n,
		"established":        est,
		"initial_verified":   initialVerified.Load(),
		"hold_alive":         holdAlive.Load(),
		"verified":           verified.Load(),
		"failed":             totalFail,
		"connect_failed":     connectFailed.Load(),
		"verify_failed":      verifyFailed.Load(),
		"hold_lost":          holdLost.Load(),
		"hold_seconds":       *hold,
		"hold_rounds":        totalRounds,
		"alive_by_round":     aliveSlice,
		"setup_seconds":      round4(setupDt),
		"connect_rate_per_s": rate,
		"started_us":         startedUs,
		"ended_us":           endedUs,
		"connect_count":      stConn.Count,
		"failures":           failures,
	}
	if stConn.Count > 0 {
		out["connect_min_ms"] = stConn.MinMs
		out["connect_p50_ms"] = stConn.P50Ms
		out["connect_p90_ms"] = stConn.P90Ms
		out["connect_p99_ms"] = stConn.P99Ms
		out["connect_p999_ms"] = stConn.P999Ms
		out["connect_max_ms"] = stConn.MaxMs
		out["connect_mean_ms"] = stConn.MeanMs
		out["connect_stddev_ms"] = stConn.StddevMs
	}
	if len(verifyRtts) > 0 {
		out["verify_rtt_ms"] = summarize(verifyRtts)
	}
	emitJSON(out)
	return 0
}

func errorsIsEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}

func isTimeoutErr(err error) bool {
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}
