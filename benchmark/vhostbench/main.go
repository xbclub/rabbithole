// rabbithole benchmark —— vhost 专用压测器(vhostbench)
//
// ## 为什么要单开一个工具
//
// 既有 benchmark/py/loadgen.py 的 http 子命令只覆盖一个场景:16 连接 × 8s 的
// 明文 HTTP QPS,响应体由 benchmark/py/target.py 固定为 4 KiB。它既测不出
// 大响应体的吞吐,也完全没有 HTTPS vhost 路径(loadgen.py 不支持 TLS 客户端,
// target.py 没有 TLS 监听),更无法把「控制面 tcp vs tls」与「vhost http vs
// https」两个维度分开度量。
//
// 本工具补的正是这些缺口:
//
//	vhostbench serve --port P        可选 TLS 的源站(原始 socket 固定响应)
//	vhostbench qps   --addr H:P      keep-alive 串行请求 → QPS + 延迟分位
//	vhostbench tput  --addr H:P      大响应体 → Gbps
//	vhostbench lat   --addr H:P      单连接串行往返 → p50/p99/p999(可 --newconn)
//	vhostbench conc  --addr H:P      N 连接建立 + 保持 + 存活复验
//
// 全部是 --flag 形式(不混位置参数),避免 Go flag 包「遇首个非 flag 即停」的坑。
//
// ## 口径
//
// 与 benchmark/loadgen/main.go、benchmark/py/loadgen.py 一致:字节只计
// **应用层载荷**(HTTP 响应体),不含 TCP/IP/TLS 头。所有子命令输出单行 JSON
// 到 stdout,诊断信息到 stderr,便于编排脚本用 jq 取值。
//
// ## 为什么 serve 用原始 socket 而不是 net/http
//
// 源站不能成为天花板。net/http 每个请求要做 header map 分配 + 状态机推进,
// 当被测隧道的 QPS 到十万级时源站自身就先饱和了 —— 那时测到的是源站而不是
// 隧道,而两者在结果里无法区分(这个坑在 benchmark/loadgen/main.go 顶部已
// 记录过一次:Python 压测器把 rh 与 frp 一起压到 2 Gbps)。
//
// 原始 socket 只做两件事:读掉请求头到空行、写一份预构建好的响应字节。
// 单核可跑百万级 QPS,不会成为瓶颈。请求解析只做最小正确性(读到空行结束,
// 识别 Connection: close),足以支撑 GET + keep-alive 的压测。
package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// dialTimeout 建连超时。隧道冷启动(尤其 TLS/多载体协商)可能到秒级,
// 给 10s 以免把「启动慢」误判为「连不上」。
const dialTimeout = 10 * time.Second

// sockBuf TCP 收发缓冲(4 MiB)。压测器必须比被测系统「更能吃」,否则它
// 通告的窗口会成为被测系统的天花板(loadgen 里实测未设时 rcv_space 仅 14KB)。
const sockBuf = 4 << 20

// ioBufSize 读响应体的缓冲。1 MiB 让 io.CopyBuffer 走大块路径。
const ioBufSize = 1 << 20

// verifyParallel 并发存活复验的并发度。
//
// 复验阶段源站侧已经有 N 个连接读 goroutine 常驻,复验再叠加 N 个会让
// 8 核机器在 4096 档整体超出 deadline(实测直连存活率跌到 1%~5%)。
// 限到 256:4096 条分批 16 轮,每轮开销毫秒级,总耗时仍在秒级内。
const verifyParallel = 256

// ---------------------------------------------------------------- 通用

func emit(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(b))
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}

// raiseNofile 提高 fd 软限到硬限。并发档(4096)需要远超默认 1024。
// 失败不致命:调用方用 --connections 与本机上限比较后决定是否 env_error。
func raiseNofile() (uint64, uint64) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, 0
	}
	soft, hard := lim.Cur, lim.Max
	if soft < hard {
		nl := lim
		nl.Cur = nl.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &nl); err == nil {
			soft = nl.Cur
		}
	}
	return soft, hard
}

// pct 对**已排序**切片取分位。空切片返回 0。
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func r3(v float64) float64 { return float64(int64(v*1000+0.5)) / 1000 }
func r1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }

// ---------------------------------------------------------------- serve

// buildResponse 预构建整份响应字节(状态行 + 头 + 体)。
//
// Connection: keep-alive 显式声明:HTTP/1.1 下本已默认,但显式写能让
// 「谁终止了长连接」的排查少一层猜测。
func buildResponse(bodySize int) []byte {
	body := make([]byte, bodySize)
	// 非零填充:避免内核/压缩层对大段零页做特殊处理影响可比性
	for i := range body {
		body[i] = byte(i)
	}
	head := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"Content-Type: application/octet-stream\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: keep-alive\r\n"+
			"Server: vhostbench\r\n"+
			"\r\n", len(body))
	out := make([]byte, 0, len(head)+len(body))
	out = append(out, head...)
	out = append(out, body...)
	return out
}

// serveConn 一条连接上循环:读掉请求头 → 写死响应。
//
// 只读到空行,不解析方法/路径:压测请求是固定形态(GET / + Host 头),
// 解析它们除了引入开销没有收益。识别 Connection: close —— 不识别的话
// 客户端关闭后这里会读到一个已关闭的 socket,循环自然退出,行为正确;
// 识别它可以让「半关闭后继续写」的噪音更早消失。
func serveConn(c net.Conn, resp []byte, counter *atomic.Int64) {
	defer c.Close()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	br := bufio.NewReaderSize(c, 16<<10)
	for {
		closeAfter := false
		hdrBytes := 0
		for {
			line, err := br.ReadSlice('\n')
			if err != nil {
				return // 对端关闭 / 超时 / 头部超长
			}
			hdrBytes += len(line)
			if hdrBytes > 1<<20 {
				return // 异常请求,保护自己
			}
			if len(line) == 2 && line[0] == '\r' && line[1] == '\n' {
				break // 头部结束
			}
			// 小写化比较:头部名大小写不敏感
			if len(line) > 11 && strings.EqualFold(string(line[:11]), "connection:") {
				rest := string(line[11:])
				if strings.Contains(strings.ToLower(rest), "close") {
					closeAfter = true
				}
			}
		}
		if _, err := c.Write(resp); err != nil {
			return
		}
		counter.Add(1)
		if closeAfter {
			return
		}
	}
}

// selfSigned 生成自签证书(ECDSA P-256)。
//
// 用 ECDSA 而非 RSA:握手计算量小一个数量级,避免 TLS 握手成本混淆
// 「隧道开销」的度量。CN/SAN 固定 bench.test,与隧道侧的 vhost 域名一致。
// 客户端一律 InsecureSkipVerify —— 这里度量的是隧道,不是证书校验。
func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bench.test"},
		DNSNames:              []string{"bench.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

func loadCert(certPath, keyPath string) (tls.Certificate, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.Int("port", 0, "监听端口(必填)")
	bind := fs.String("bind", "0.0.0.0", "监听地址")
	body := fs.Int("body", 4096, "响应体字节数")
	useTLS := fs.Bool("tls", false, "启用 TLS(HTTPS 源站)")
	certPath := fs.String("cert", "", "证书 PEM(--tls 时可省,缺省自签)")
	keyPath := fs.String("key", "", "私钥 PEM")
	dur := fs.Float64("duration", 0, "运行秒数(0=直到 SIGTERM)")
	fs.Parse(args)
	if *port == 0 {
		fatalf("serve 需要 --port")
	}

	resp := buildResponse(*body)
	var counter atomic.Int64

	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *bind, *port))
	if err != nil {
		fatalf("listen: %v", err)
	}
	scheme := "http"
	if *useTLS {
		var cert tls.Certificate
		if *certPath != "" && *keyPath != "" {
			cert, err = loadCert(*certPath, *keyPath)
		} else {
			cert, err = selfSigned()
		}
		if err != nil {
			fatalf("tls cert: %v", err)
		}
		ln = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			// 明确只提供 http/1.1:被测两端(rh vhost / frp vhost)都是
			// HTTP/1.1,协商出 h2 会让对照失去意义。
			NextProtos: []string{"http/1.1"},
		})
		scheme = "https"
	}
	fmt.Fprintf(os.Stderr, "vhostbench serve %s://%s:%d body=%d\n",
		scheme, *bind, *port, *body)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
				}
				// TLS 握手中的错误也走这里:不终止 accept 循环
				if errors.Is(err, net.ErrClosed) {
					return
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				serveConn(c, resp, &counter)
			}()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	if *dur > 0 {
		select {
		case <-time.After(time.Duration(*dur * float64(time.Second))):
		case <-sig:
		}
	} else {
		<-sig
	}
	close(stop)
	ln.Close()
	// 给在途连接 200ms 收尾(不等满 wg:keep-alive 连接可能一直不关)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
	}
	emit(map[string]any{
		"role":     "serve",
		"port":     *port,
		"tls":      *useTLS,
		"body":     *body,
		"requests": counter.Load(),
	})
}

// ---------------------------------------------------------------- 客户端公共

type clientOpts struct {
	addr    string
	tls     bool
	sni     string
	host    string
	path    string
	timeout time.Duration
}

// newClient 构造与压测场景匹配的 http.Client。
//
// 关键约束:
//   - ForceAttemptHTTP2=false 且 TLSNextProto 置空 → 只走 HTTP/1.1,
//     与被测两端(rh hyper http1 / frp http1)一致。
//   - DisableCompression=true → 不发 Accept-Encoding,避免响应被压缩后
//     字节口径失真(也避免 gzip CPU 混入)。
//   - MaxConnsPerHost=conns → 并发连接数受控,不会因排队额外开连接。
func newClient(o clientOpts, conns int) *http.Client {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: dialTimeout,
		}).DialContext,
		MaxIdleConns:          conns,
		MaxIdleConnsPerHost:   conns,
		MaxConnsPerHost:       conns,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		ResponseHeaderTimeout: o.timeout,
		ExpectContinueTimeout: time.Second,
	}
	if o.tls {
		tr.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, // 度量隧道,不度量证书校验
			ServerName:         o.sni,
			NextProtos:         []string{"http/1.1"},
			MinVersion:         tls.VersionTLS12,
		}
	}
	return &http.Client{Transport: tr, Timeout: 0}
}

func (o clientOpts) request() (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+o.addr+o.path, nil)
	if err != nil {
		return nil, err
	}
	// URL 里用 http:// 只是占位:实际协议由 Transport 的 TLSClientConfig 决定,
	// scheme 要在下面显式改成 https,否则 Transport 不会走 TLS 分支。
	if o.tls {
		req.URL.Scheme = "https"
	}
	if o.host != "" {
		req.Host = o.host
	}
	return req, nil
}

func addClientFlags(fs *flag.FlagSet, o *clientOpts, defaultPath string) {
	fs.StringVar(&o.addr, "addr", "", "目标 host:port(必填)")
	fs.BoolVar(&o.tls, "tls", false, "用 TLS 连接")
	fs.StringVar(&o.sni, "sni", "", "TLS SNI(--tls 时用,vhost 路由依据)")
	fs.StringVar(&o.host, "host", "", "HTTP Host 头(vhost 路由依据)")
	fs.StringVar(&o.path, "path", defaultPath, "请求路径")
	fs.DurationVar(&o.timeout, "timeout", 30*time.Second, "单请求超时")
}

// ---------------------------------------------------------------- qps

func cmdQPS(args []string) {
	fs := flag.NewFlagSet("qps", flag.ExitOnError)
	var o clientOpts
	addClientFlags(fs, &o, "/")
	conns := fs.Int("conns", 16, "并发连接数")
	dur := fs.Float64("dur", 10, "压测秒数")
	fs.Parse(args)
	if o.addr == "" {
		fatalf("qps 需要 --addr")
	}

	cli := newClient(o, *conns)
	// 预热:先各发一个请求,把连接建立(含 TLS 握手)排除在计时窗口外,
	// 否则短窗口下握手成本会污染 QPS。
	warm := make([]*http.Response, 0)
	var wmu sync.Mutex
	var wwg sync.WaitGroup
	for i := 0; i < *conns; i++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			req, err := o.request()
			if err != nil {
				return
			}
			resp, err := cli.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			wmu.Lock()
			warm = append(warm, resp)
			wmu.Unlock()
		}()
	}
	wwg.Wait()
	_ = warm

	var (
		requests atomic.Int64
		badStat  atomic.Int64
		errs     atomic.Int64
	)
	var latMu sync.Mutex
	var lats []float64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, 4096)
			for {
				select {
				case <-stop:
					latMu.Lock()
					lats = append(lats, local...)
					latMu.Unlock()
					return
				default:
				}
				req, err := o.request()
				if err != nil {
					errs.Add(1)
					continue
				}
				start := time.Now()
				resp, err := cli.Do(req)
				if err != nil {
					errs.Add(1)
					continue
				}
				n, rerr := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				d := float64(time.Since(start).Microseconds()) / 1000.0
				if rerr != nil || resp.StatusCode != 200 || n == 0 {
					badStat.Add(1)
					continue
				}
				requests.Add(1)
				if len(local) < 400000 {
					local = append(local, d)
				}
			}
		}()
	}
	time.Sleep(time.Duration(*dur * float64(time.Second)))
	close(stop)
	wg.Wait()
	elapsed := time.Since(t0).Seconds()

	sort.Float64s(lats)
	out := map[string]any{
		"role":       "qps",
		"addr":       o.addr,
		"tls":        o.tls,
		"sni":        o.sni,
		"host":       o.host,
		"conns":      *conns,
		"requests":   requests.Load(),
		"errors":     errs.Load(),
		"bad_status": badStat.Load(),
		"seconds":    r3(elapsed),
		"samples":    len(lats),
	}
	qps := float64(requests.Load()) / elapsed
	out["qps"] = r1(qps)
	if len(lats) > 0 {
		var sum float64
		for _, v := range lats {
			sum += v
		}
		out["lat_mean_ms"] = r3(sum / float64(len(lats)))
		out["lat_p50_ms"] = r3(pct(lats, 0.50))
		out["lat_p99_ms"] = r3(pct(lats, 0.99))
		out["lat_p999_ms"] = r3(pct(lats, 0.999))
		out["lat_max_ms"] = r3(lats[len(lats)-1])
	}
	emit(out)
}

// ---------------------------------------------------------------- tput

func cmdTput(args []string) {
	fs := flag.NewFlagSet("tput", flag.ExitOnError)
	var o clientOpts
	addClientFlags(fs, &o, "/")
	conns := fs.Int("conns", 16, "并发连接数")
	dur := fs.Float64("dur", 10, "压测秒数")
	fs.Parse(args)
	if o.addr == "" {
		fatalf("tput 需要 --addr")
	}

	cli := newClient(o, *conns)
	var (
		totalBytes atomic.Int64
		requests   atomic.Int64
		errs       atomic.Int64
	)

	// 缓冲池:每连接一个 1 MiB 缓冲,避免每次 io.Copy 重新分配。
	pool := sync.Pool{New: func() any { return make([]byte, ioBufSize) }}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := pool.Get().([]byte)
			defer pool.Put(buf)
			for {
				select {
				case <-stop:
					return
				default:
				}
				req, err := o.request()
				if err != nil {
					errs.Add(1)
					if err != nil && errs.Load() <= 3 {
						fmt.Fprintf(os.Stderr, "tput request: %v\n", err)
					}
					continue
				}
				resp, err := cli.Do(req)
				if err != nil {
					errs.Add(1)
					if errs.Load() <= 3 {
						fmt.Fprintf(os.Stderr, "tput do: %v\n", err)
					}
					continue
				}
				n, rerr := io.CopyBuffer(io.Discard, resp.Body, buf)
				resp.Body.Close()
				if rerr != nil || resp.StatusCode != 200 {
					errs.Add(1)
					continue
				}
				totalBytes.Add(n)
				requests.Add(1)
			}
		}()
	}
	time.Sleep(time.Duration(*dur * float64(time.Second)))
	close(stop)
	wg.Wait()
	elapsed := time.Since(t0).Seconds()

	b := totalBytes.Load()
	emit(map[string]any{
		"role":     "tput",
		"addr":     o.addr,
		"tls":      o.tls,
		"sni":      o.sni,
		"host":     o.host,
		"conns":    *conns,
		"bytes":    b,
		"requests": requests.Load(),
		"errors":   errs.Load(),
		"seconds":  r3(elapsed),
		"gbps":     r3(float64(b) * 8 / elapsed / 1e9),
	})
}

// ---------------------------------------------------------------- lat

func cmdLat(args []string) {
	fs := flag.NewFlagSet("lat", flag.ExitOnError)
	var o clientOpts
	addClientFlags(fs, &o, "/")
	count := fs.Int("count", 5000, "往返次数")
	warmup := fs.Int("warmup", 200, "预热次数(不计入)")
	newconn := fs.Bool("newconn", false, "每次往返新建连接(测建连开销)")
	fs.Parse(args)
	if o.addr == "" {
		fatalf("lat 需要 --addr")
	}

	cli := newClient(o, 1)
	lats := make([]float64, 0, *count)

	doOne := func(cl *http.Client) (float64, error) {
		req, err := o.request()
		if err != nil {
			return 0, err
		}
		start := time.Now()
		resp, err := cl.Do(req)
		if err != nil {
			return 0, err
		}
		_, rerr := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if rerr != nil {
			return 0, rerr
		}
		if resp.StatusCode != 200 {
			return 0, fmt.Errorf("status %d", resp.StatusCode)
		}
		return float64(time.Since(start).Microseconds()) / 1000.0, nil
	}

	// newconn 模式:每次请求一个全新 client,用完关掉空闲连接,
	// 这样才能真正测到「建连 + 握手」而不会被连接池复用掉。
	for i := 0; i < *warmup; i++ {
		if *newconn {
			c := newClient(o, 1)
			doOne(c)
			c.CloseIdleConnections()
		} else {
			doOne(cli)
		}
	}

	var errs int64
	for i := 0; i < *count; i++ {
		var (
			d   float64
			err error
		)
		if *newconn {
			c := newClient(o, 1)
			d, err = doOne(c)
			c.CloseIdleConnections()
		} else {
			d, err = doOne(cli)
		}
		if err != nil {
			errs++
			continue
		}
		lats = append(lats, d)
	}
	sort.Float64s(lats)

	out := map[string]any{
		"role":     "lat",
		"addr":     o.addr,
		"tls":      o.tls,
		"sni":      o.sni,
		"host":     o.host,
		"newconn":  *newconn,
		"count":    len(lats),
		"errors":   errs,
		"samples":  len(lats),
	}
	if len(lats) > 0 {
		var sum float64
		for _, v := range lats {
			sum += v
		}
		out["p50_ms"] = r3(pct(lats, 0.50))
		out["p90_ms"] = r3(pct(lats, 0.90))
		out["p99_ms"] = r3(pct(lats, 0.99))
		out["p999_ms"] = r3(pct(lats, 0.999))
		out["min_ms"] = r3(lats[0])
		out["max_ms"] = r3(lats[len(lats)-1])
		out["mean_ms"] = r3(sum / float64(len(lats)))
	}
	emit(out)
}

// ---------------------------------------------------------------- conc

func cmdConc(args []string) {
	fs := flag.NewFlagSet("conc", flag.ExitOnError)
	var o clientOpts
	addClientFlags(fs, &o, "/")
	conns := fs.Int("connections", 1024, "并发连接数")
	parallel := fs.Int("parallel", 64, "建连并发度")
	hold := fs.Float64("hold", 5, "建成后保持秒数")
	fs.Parse(args)
	if o.addr == "" {
		fatalf("conc 需要 --addr")
	}

	soft, hard := raiseNofile()
	// fd 不足是**环境错误**,不是产品并发失败(与 loadgen.py 同口径:
	// 绝不能把 EMFILE 折算成隧道建不起连接)
	if uint64(*conns) > soft-64 {
		emit(map[string]any{
			"role": "conc",
			"env_error": map[string]any{
				"kind":   "fd_limit",
				"hint":   fmt.Sprintf("fd 上限不足:需要 ~%d,可用 %d(硬上限 %d)", *conns, soft, hard),
				"fd_soft": soft,
				"fd_hard": hard,
			},
			"connections": *conns,
			"fd_soft":     soft,
			"fd_hard":     hard,
		})
		os.Exit(1)
	}

	// 每个 worker 自己拨号并保持:不复用 http.Client 的连接池,
	// 否则「N 条并发连接」会退化成「池里几条连接」。
	//
	// br 必须与连接一起保存:建立阶段读到的是**响应头**,响应体还在
	// bufio 缓冲里。存活复验若直接从 conn 读,拿到的是上一次响应体的
	// 残留字节而非状态行 —— 这会让 hold_alive 恒为 0。
	type held struct {
		c  net.Conn
		br *bufio.Reader
	}
	var (
		mu          sync.Mutex
		established int
		failed      int
		connsHeld   []held
		connectLat  []float64
		readErr     int64
		diagN       int64
	)

	// readResponse 读一份完整响应(头 + 体),返回状态码。
	// bodyN < 0 表示未取到 Content-Length:读到 EOF 或超时为止。
	readResponse := func(br *bufio.Reader, timeout time.Duration) (int, error) {
		var (
			status  int
			bodyLen = -1
		)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return 0, err
			}
			if status == 0 && strings.HasPrefix(line, "HTTP/1.") {
				// "HTTP/1.1 200 OK"
				f := strings.Fields(line)
				if len(f) >= 2 {
					if n, e := strconv.Atoi(f[1]); e == nil {
						status = n
					}
				}
			}
			if strings.HasPrefix(strings.ToLower(line), "content-length:") {
				if n, e := strconv.Atoi(strings.TrimSpace(line[15:])); e == nil {
					bodyLen = n
				}
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		if bodyLen > 0 {
			// io.CopyN 到 Discard:响应体可能很大,不整块驻留内存
			if _, err := io.CopyN(io.Discard, br, int64(bodyLen)); err != nil {
				return status, err
			}
		} else if bodyLen < 0 {
			// 无 Content-Length:读到 EOF(源站用 Connection: close)
			_, _ = io.Copy(io.Discard, br)
		}
		return status, nil
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, *parallel)
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			d := net.Dialer{Timeout: dialTimeout}
			start := time.Now()
			raw, err := d.Dial("tcp", o.addr)
			if err != nil {
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			c := raw
			if o.tls {
				tc := tls.Client(raw, &tls.Config{
					InsecureSkipVerify: true,
					ServerName:         o.sni,
					NextProtos:         []string{"http/1.1"},
					MinVersion:         tls.VersionTLS12,
				})
				if err := tc.Handshake(); err != nil {
					raw.Close()
					mu.Lock()
					failed++
					mu.Unlock()
					return
				}
				c = tc
			}
			if tc, ok := raw.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			// 发一个请求并读到 200:证明这条连接**数据面可用**,
			// 而不只是内核里建好了 socket(verified 语义)
			hostHdr := ""
			if o.host != "" {
				hostHdr = "Host: " + o.host + "\r\n"
			}
			req := "GET " + o.path + " HTTP/1.1\r\n" + hostHdr +
				"Connection: keep-alive\r\n\r\n"
			_ = c.SetDeadline(time.Now().Add(o.timeout))
			if _, err := c.Write([]byte(req)); err != nil {
				c.Close()
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			br := bufio.NewReaderSize(c, 4096)
			status, err := readResponse(br, o.timeout)
			_ = c.SetDeadline(time.Time{}) // 取消超时,进入保持期
			if err != nil || status != 200 {
				c.Close()
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			lat := float64(time.Since(start).Microseconds()) / 1000.0
			mu.Lock()
			established++
			connectLat = append(connectLat, lat)
			connsHeld = append(connsHeld, held{c: c, br: br})
			mu.Unlock()
		}()
	}
	wg.Wait()

	// 保持期:期间连接必须存活(被隧道主动断开 = 并发能力不成立)
	time.Sleep(time.Duration(*hold * float64(time.Second)))

	// 存活复验:再发一个请求,能拿到 200 才算 hold_alive。
	//
	// ★总超时必须给足(实测 15s;5s 会假失败):
	// 4096 连接时源站侧每连接一个读 goroutine,复验阶段 8 核上争用明显,
	// 5s deadline 下直连基线只有 4~322 条"存活",15s 下 4096/4096。
	// 那是压测器自己制造的超时,不是隧道断开 —— 若直接拿 5s 的结果,
	// 会把"压测器不够快"误读成"隧道的并发能力差"。
	// 归因经负向验证:单独把 deadline 改回 5s 即复发(4096→322),
	// 单独把 verifyParallel 调到 4096 不复发 —— 所以根因是超时,不是并发度。
	// 限流保留:它让复验有序推进、总耗时可控,但不是修好这个 bug 的原因。
	verifySem := make(chan struct{}, verifyParallel)
	var aliveWG sync.WaitGroup
	var alive atomic.Int64
	for _, h := range connsHeld {
		aliveWG.Add(1)
		verifySem <- struct{}{}
		go func(c net.Conn, br *bufio.Reader) {
			defer aliveWG.Done()
			defer func() { <-verifySem }()
			// 单条复验超时给足:限流后总耗时可控,不必靠短超时压
			_ = c.SetDeadline(time.Now().Add(15 * time.Second))
			req := "GET " + o.path + " HTTP/1.1\r\n"
			if o.host != "" {
				req += "Host: " + o.host + "\r\n"
			}
			req += "Connection: close\r\n\r\n"
			if _, err := c.Write([]byte(req)); err != nil {
				atomic.AddInt64(&readErr, 1)
				return
			}
			status, err := readResponse(br, 15*time.Second)
			if err != nil || status != 200 {
				atomic.AddInt64(&readErr, 1)
				// 诊断:前若干次失败把原因打出来,否则"存活为 0"无从归因
				if atomic.AddInt64(&diagN, 1) <= 8 {
					fmt.Fprintf(os.Stderr, "hold verify: status=%d err=%v\n", status, err)
				}
				return
			}
			alive.Add(1)
		}(h.c, h.br)
	}
	aliveWG.Wait()
	for _, h := range connsHeld {
		h.c.Close()
	}

	sort.Float64s(connectLat)
	out := map[string]any{
		"role":        "conc",
		"addr":        o.addr,
		"tls":         o.tls,
		"sni":         o.sni,
		"host":        o.host,
		"connections": *conns,
		"established": established,
		"failed":      failed,
		"hold_s":      *hold,
		"hold_alive":  alive.Load(),
		"hold_dead":   readErr,
	}
	if len(connectLat) > 0 {
		out["connect_p50_ms"] = r3(pct(connectLat, 0.50))
		out["connect_p99_ms"] = r3(pct(connectLat, 0.99))
		out["connect_max_ms"] = r3(connectLat[len(connectLat)-1])
	}
	emit(out)
}

// ---------------------------------------------------------------- main

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage:")
		fmt.Fprintln(os.Stderr, "  vhostbench serve --port P [--tls] [--body N] [--duration S]")
		fmt.Fprintln(os.Stderr, "  vhostbench qps   --addr H:P [--tls --sni D --host D] [--conns N --dur S]")
		fmt.Fprintln(os.Stderr, "  vhostbench tput  --addr H:P [--tls --sni D --host D] [--conns N --dur S]")
		fmt.Fprintln(os.Stderr, "  vhostbench lat   --addr H:P [--tls --sni D --host D] [--count N --newconn]")
		fmt.Fprintln(os.Stderr, "  vhostbench conc  --addr H:P [--tls --sni D --host D] [--connections N --hold S]")
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]
	// 环境变量兜底:某些编排场景下 fd 由父进程给,这里再抬一次无害
	raiseNofile()
	switch sub {
	case "serve":
		cmdServe(args)
	case "qps":
		cmdQPS(args)
	case "tput":
		cmdTput(args)
	case "lat":
		cmdLat(args)
	case "conc":
		cmdConc(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", sub)
		os.Exit(2)
	}
	_ = strconv.Itoa // 保留 strconv 导入以便后续扩展数值参数
}
