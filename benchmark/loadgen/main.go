// rabbithole benchmark —— Go 压测器(sink / src)。
//
// ## 为什么必须是 Go 而不是 Python
//
// 压测器本身会成为被测系统的天花板。Python 版(旧 benchmark/py/loadgen.py)
// 在 2 Gbps 量级时解释器与 GIL 已经饱和:压测器跑不动 ≠ 隧道跑不动,而
// 两者在结果里无法区分。曾据此得出「rh 与 frp 都卡在 2.0~2.2G、都不随
// 并发扩展」——那个结论是压测器造成的,不是隧道。
//
// Go 版本:每连接一个 goroutine,零解释器开销,收发走 io.Copy 的大缓冲
// 快路径;可稳定压满 10GbE。
//
// ## 两个子命令
//
//	sink  <port> <seconds> <conns>   接收端:接受最多 conns 条连接并读空
//	src   <host:port> <seconds> <conns>  发送端:开 conns 条连接持续写
//
// 两者都输出单行 JSON,便于脚本收集:
//
//	{"role":"src","conns":16,"ok":16,"bytes":123,"gbps":1.23,"elapsed_s":10.0}
//
// bytes 只计**有效载荷**(应用层字节),不含 TCP/IP 头 —— 与 iperf3 的
// 口径一致,便于与既有基准数据对照。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ioBufSize 单次读写的缓冲:1 MiB 足以让 io.Copy 走大块路径,
// 又不会因过大的分配拖慢启动。
const ioBufSize = 1 << 20

// dialTimeout 建连超时。隧道冷启动(尤其 TLS/多载体协商)可能到秒级,
// 给 10s 以免把"启动慢"误判为"连不上"。
const dialTimeout = 10 * time.Second

type result struct {
	Role     string  `json:"role"`
	Conns    int     `json:"conns"`
	OK       int     `json:"ok"`
	Failed   int     `json:"failed"`
	Bytes    int64   `json:"bytes"`
	Gbps     float64 `json:"gbps"`
	ElapsedS float64 `json:"elapsed_s"`
}

func emit(r result) {
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

// ---------------------------------------------------------------- sink

// runSink 接受连接并读空,直到 seconds 到期。
//
// conns 是**上限**:连接数达到上限后不再 accept,但已建立的连接继续读
// 到结束。这样发送端可以用任意并发度压测,而 sink 不会无限吃 fd。
func runSink(port int, seconds float64, maxConns int) {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		os.Exit(1)
	}
	defer ln.Close()

	var total atomic.Int64
	var accepted atomic.Int32
	var wg sync.WaitGroup

	deadline := time.Now().Add(time.Duration(seconds * float64(time.Second)))

	// accept 循环独立跑,到期后用 SetDeadline 打断 Accept 阻塞。
	stop := make(chan struct{})
	go func() {
		time.Sleep(time.Until(deadline))
		close(stop)
		_ = ln.(*net.TCPListener).SetDeadline(time.Now())
	}()

	buf := make([]byte, ioBufSize)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
				// 到期正常退出
			default:
			}
			break
		}
		if int(accepted.Load()) >= maxConns {
			conn.Close()
			continue
		}
		accepted.Add(1)
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			// 到期后强制打断读阻塞:读端是长连接,发送端不会主动关。
			go func() {
				<-stop
				_ = c.SetReadDeadline(time.Now())
			}()
			for {
				n, err := c.Read(buf)
				if n > 0 {
					total.Add(int64(n))
				}
				if err != nil {
					return
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
	emit(result{
		Role:     "sink",
		Conns:    n,
		OK:       n,
		Bytes:    total.Load(),
		Gbps:     float64(total.Load()) * 8 / elapsed / 1e9,
		ElapsedS: elapsed,
	})
}

// ---------------------------------------------------------------- src

// runSrc 向 host:port 开 conns 条独立 TCP 连接,各自持续写到 seconds 到期。
//
// 每条连接独立建连 = 独立四元组。这与 iperf3 的 -P(同一条 yamux 会话内
// 多流)语义不同,正是测「隧道的真实并发能力」所需要的:多流在国内多路
// 复用实现里可能全挤在一条物理连接上,而多连接会真正开出多条。
func runSrc(addr string, seconds float64, conns int) {
	var total atomic.Int64
	var okCount, failCount atomic.Int32
	var wg sync.WaitGroup

	deadline := time.Now().Add(time.Duration(seconds * float64(time.Second)))
	chunk := make([]byte, ioBufSize) // 全零内容,内容本身不影响吞吐

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := net.Dialer{Timeout: dialTimeout}
			conn, err := d.Dial("tcp", addr)
			if err != nil {
				failCount.Add(1)
				return
			}
			defer conn.Close()
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			okCount.Add(1)

			// 到期打断写阻塞(对端不读时 Write 会一直阻塞,不能只靠检查时间)。
			done := make(chan struct{})
			go func() {
				time.Sleep(time.Until(deadline))
				_ = conn.SetWriteDeadline(time.Now())
				close(done)
			}()

			var sent int64
			for {
				select {
				case <-done:
					total.Add(sent)
					return
				default:
				}
				n, err := conn.Write(chunk)
				if n > 0 {
					sent += int64(n)
				}
				if err != nil {
					total.Add(sent)
					return
				}
			}
		}()
	}

	wg.Wait()
	elapsed := seconds
	ok := int(okCount.Load())
	emit(result{
		Role:     "src",
		Conns:    conns,
		OK:       ok,
		Failed:   int(failCount.Load()),
		Bytes:    total.Load(),
		Gbps:     float64(total.Load()) * 8 / elapsed / 1e9,
		ElapsedS: elapsed,
	})
}

// ---------------------------------------------------------------- main

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage:")
		fmt.Fprintln(os.Stderr, "  loadgen sink <port> <seconds> [maxConns]")
		fmt.Fprintln(os.Stderr, "  loadgen src  <host:port> <seconds> <conns>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sink":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "sink needs <port> <seconds> [maxConns]")
			os.Exit(2)
		}
		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad port: %v\n", err)
			os.Exit(2)
		}
		sec, err := strconv.ParseFloat(os.Args[3], 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad seconds: %v\n", err)
			os.Exit(2)
		}
		maxConns := 4096
		if len(os.Args) >= 5 {
			if v, err := strconv.Atoi(os.Args[4]); err == nil {
				maxConns = v
			}
		}
		runSink(port, sec, maxConns)
	case "src":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "src needs <host:port> <seconds> <conns>")
			os.Exit(2)
		}
		sec, err := strconv.ParseFloat(os.Args[3], 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad seconds: %v\n", err)
			os.Exit(2)
		}
		conns, err := strconv.Atoi(os.Args[4])
		if err != nil || conns < 1 {
			fmt.Fprintln(os.Stderr, "bad conns")
			os.Exit(2)
		}
		runSrc(os.Args[2], sec, conns)
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q\n", os.Args[1])
		os.Exit(2)
	}
	_ = io.Discard // 保留 io 导入,便于后续扩展 io.Copy 路径
}
