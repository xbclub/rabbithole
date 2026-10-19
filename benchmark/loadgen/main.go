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

// sockBuf TCP 收发缓冲(4 MiB)。压测器必须比被测系统"更能吃",否则
// 它通告的窗口会成为被测系统的天花板(实测未设时 rcv_space 仅 14KB)。
const sockBuf = 4 << 20

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
			// 放大接收缓冲(2026-10-20 修正)。
			//
			// 不设时内核自动调节的通告窗口偏低:实测一端 `rcv_space`
			// 只有 14480 字节、`rcvmss` 只有 536,**发送方被对面通告的
			// 窗口卡死**,测出来的是压测器的接收能力而不是被测系统。
			// 这是本项目第二次被压测端反噬(Python GIL 那次之外)。
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(sockBuf)
			}
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
				// 放大发送缓冲:避免阻塞在较小的内核默认值上。
				_ = tc.SetWriteBuffer(sockBuf)
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

// ---------------------------------------------------------------- reverse
//
// 上行(反向)测量需要一个**主动拨号**的一端和一个**被动接受**的一端,
// 与 sink/src 的角色刚好对调:
//
//	隧道发布端(test2)        隧道接入端(test3)
//	revsink <addr> ...  ──拨号──►  revsrc <port> ...
//	   (持续写)                      (接受并读空)
//
// 命名按"数据流向"而非"谁拨号":revsink = 反向的接收端角色(它其实是
// 发送方)。这样 `revsrc`/`revsink` 与正向的 sink/src 在脚本里语义对称。

// runRevSink 拨号到 addr 并持续写。它是上行方向的**发送方**。
func runRevSink(addr string, seconds float64, conns int) {
	runSrc(addr, seconds, conns) // 与正向 src 的发送逻辑完全一致
}

// runRevSrc 监听 port,接受连接并读空。它是上行方向的**接收方**。
//
// 与 runSink 的差别:连接由对端(隧道发布端)主动拨入,sink 侧不限制
// 连接数上限(隧道发布端自己决定开几条),但同样在到期时打断读阻塞。
func runRevSrc(port int, seconds float64, maxConns int) {
	runSink(port, seconds, maxConns)
}

// ---------------------------------------------------------------- main

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage:")
		fmt.Fprintln(os.Stderr, "  loadgen sink    <port> <seconds> [maxConns]   # 正向:接受并读空")
		fmt.Fprintln(os.Stderr, "  loadgen src     <host:port> <seconds> <conns> # 正向:拨号并写")
		fmt.Fprintln(os.Stderr, "  loadgen revsrc  <port> <seconds> [maxConns]   # 上行:接受并读空(接收方)")
		fmt.Fprintln(os.Stderr, "  loadgen revsink <host:port> <seconds> <conns> # 上行:拨号并写(发送方)")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "revsrc":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "revsrc needs <port> <seconds> [maxConns]")
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
		runRevSrc(port, sec, maxConns)
	case "revsink":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "revsink needs <host:port> <seconds> <conns>")
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
		runRevSink(os.Args[2], sec, conns)
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
