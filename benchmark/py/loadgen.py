#!/usr/bin/env python3
"""rabbithole benchmark —— 压测引擎。

纯标准库实现(机器上无 iperf3),直接测代理层,输出 JSON 供汇总。

子命令:
  latency    往返延迟(PING/PONG 或 UDP 回显);百分位 + 抖动
  throughput 单流吞吐(上行/下行/双向)
  concurrent N 并发连接,测连接建立能力与稳定性
  soak       持续压测,按时间桶输出吞吐/错误曲线,可检出衰减
  udp        UDP 吞吐与丢包
  http       HTTP 请求吞吐(QPS)
  connect    连接建立速率(每连接一次短交互)

所有测量都记录墙钟时间,吞吐按有效载荷字节计算(不含协议开销)。
"""
from __future__ import annotations

import argparse
import json
import resource
import socket
import sys
import threading
import time


def raise_fd_limit() -> tuple[int, int]:
    """把本进程 fd 软上限提到硬上限。

    高并发场景下压测端自身要持有 N 个 socket,默认软上限(常为 1024)
    会让压测先于被测系统失败,污染结果。返回 (软上限, 硬上限)。"""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        soft = hard
    except (ValueError, OSError):
        pass
    return soft, hard


CHUNK = 1 << 20
PATTERN = bytes(range(256)) * (CHUNK // 256)


def pattern_slice(offset: int, n: int) -> bytes:
    pos = offset % CHUNK
    if pos + n <= CHUNK:
        return PATTERN[pos:pos + n]
    out = bytearray()
    while len(out) < n:
        pos = (offset + len(out)) % CHUNK
        take = min(n - len(out), CHUNK - pos)
        out += PATTERN[pos:pos + take]
    return bytes(out)


_RAND_CACHE: dict = {}


def random_slice(offset: int, n: int) -> bytes:
    """确定性伪随机(不可压缩),与 target.py 保持一致。"""
    import random
    blk = offset // CHUNK
    base = _RAND_CACHE.get(blk)
    if base is None:
        rng = random.Random(blk)
        base = bytes(rng.getrandbits(8) for _ in range(CHUNK))
        _RAND_CACHE.clear()
        _RAND_CACHE[blk] = base
    pos = offset % CHUNK
    if pos + n <= CHUNK:
        return base[pos:pos + n]
    out = bytearray()
    while len(out) < n:
        p = (offset + len(out)) % CHUNK
        take = min(n - len(out), CHUNK - p)
        out += base[p:p + take]
    return bytes(out)


def payload_slice(mode: str, offset: int, n: int) -> bytes:
    if mode == "zeros":
        return bytes(n)
    if mode == "random":
        return random_slice(offset, n)
    return pattern_slice(offset, n)


def connect(host: str, port: int, timeout: float = 10.0) -> socket.socket:
    s = socket.create_connection((host, port), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def pct(sorted_vals: list[float], q: float) -> float:
    """已排序列表的分位数(线性插值)。"""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = (len(sorted_vals) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def summarize(samples: list[float]) -> dict:
    if not samples:
        return {"count": 0}
    s = sorted(samples)
    mean = sum(s) / len(s)
    var = sum((x - mean) ** 2 for x in s) / len(s)
    return {
        "count": len(s),
        "min_ms": round(s[0], 4),
        "p50_ms": round(pct(s, 0.50), 4),
        "p90_ms": round(pct(s, 0.90), 4),
        "p99_ms": round(pct(s, 0.99), 4),
        "p999_ms": round(pct(s, 0.999), 4),
        "max_ms": round(s[-1], 4),
        "mean_ms": round(mean, 4),
        "stddev_ms": round(var ** 0.5, 4),
    }


# ---------- latency ----------

def run_latency(args) -> dict:
    """TCP 往返延迟:单连接内串行 PING,或每请求新建连接。"""
    n = args.count
    samples: list[float] = []
    errors = 0

    if args.newconn:
        for _ in range(n):
            try:
                t0 = time.perf_counter()
                s = connect(args.host, args.port, args.timeout)
                s.sendall(b"PING\n")
                resp = s.recv(64)
                dt = (time.perf_counter() - t0) * 1e3
                s.close()
                if resp.startswith(b"PONG"):
                    samples.append(dt)
                else:
                    errors += 1
            except OSError:
                errors += 1
    else:
        s = connect(args.host, args.port, args.timeout)
        s.settimeout(args.timeout)
        try:
            for _ in range(args.warmup):
                s.sendall(b"PING\n")
                s.recv(64)
            for _ in range(n):
                t0 = time.perf_counter()
                s.sendall(b"PING\n")
                resp = s.recv(64)
                dt = (time.perf_counter() - t0) * 1e3
                if resp.startswith(b"PONG"):
                    samples.append(dt)
                else:
                    errors += 1
        finally:
            s.close()

    out = summarize(samples)
    out["errors"] = errors
    out["mode"] = "newconn" if args.newconn else "persistent"
    return out


# ---------- throughput ----------

def run_throughput(args) -> dict:
    """单流吞吐:download(服务端发)/ upload(客户端发)/ bidir(双向同时)。"""
    duration = args.duration
    total = 0
    errors = 0
    result: dict = {}

    if args.direction == "download":
        s = connect(args.host, args.port, args.timeout)
        s.settimeout(args.timeout)
        mode = getattr(args, "mode", "pattern")
        s.sendall(b"SEND %d %s\n" % ((1 << 62), mode.encode()))  # 极大值:由时长截断
        deadline = time.perf_counter() + duration
        t0 = time.perf_counter()
        expected = 0
        try:
            while True:
                remain = deadline - time.perf_counter()
                if remain <= 0:
                    break
                s.settimeout(max(remain, 0.5))
                d = s.recv(CHUNK)
                if not d:
                    break
                if mode == "pattern" and d != pattern_slice(expected, len(d)):
                    errors += 1
                    break
                expected += len(d)
                total += len(d)
        except socket.timeout:
            pass
        except OSError:
            errors += 1
        dt = time.perf_counter() - t0
        s.close()

    elif args.direction == "upload":
        s = connect(args.host, args.port, args.timeout)
        s.settimeout(args.timeout)
        nbytes = args.size
        s.sendall(b"RECV %d\n" % nbytes)
        deadline = time.perf_counter() + duration
        t0 = time.perf_counter()
        sent = 0
        try:
            while sent < nbytes and time.perf_counter() < deadline:
                n = min(CHUNK, nbytes - sent)
                s.sendall(pattern_slice(sent, n))
                sent += n
            total = sent
            resp = s.recv(64)
            if not resp.startswith(b"OK"):
                errors += 1
        except OSError:
            errors += 1
        dt = time.perf_counter() - t0
        s.close()

    else:  # bidir
        s = connect(args.host, args.port, args.timeout)
        s.settimeout(args.timeout)
        s.sendall(b"ECHO\n")
        nbytes = args.size
        recv_total = [0]
        stop = threading.Event()

        def reader():
            expected = 0
            try:
                while not stop.is_set():
                    s.settimeout(1.0)
                    d = s.recv(CHUNK)
                    if not d:
                        break
                    if d != pattern_slice(expected, len(d)):
                        errors_local[0] += 1
                        break
                    expected += len(d)
                    recv_total[0] += len(d)
            except (socket.timeout, OSError):
                pass

        errors_local = [0]
        th = threading.Thread(target=reader, daemon=True)
        t0 = time.perf_counter()
        th.start()
        sent = 0
        deadline = t0 + duration
        try:
            while sent < nbytes and time.perf_counter() < deadline:
                n = min(CHUNK, nbytes - sent)
                s.sendall(pattern_slice(sent, n))
                sent += n
        except OSError:
            errors += 1
        # 等回显排空
        drain_deadline = time.perf_counter() + 5
        while recv_total[0] < sent and time.perf_counter() < drain_deadline:
            time.sleep(0.05)
        dt = time.perf_counter() - t0
        stop.set()
        s.close()
        total = sent + recv_total[0]
        errors += errors_local[0]

    mb = total / (1024 * 1024)
    result = {
        "direction": args.direction,
        "bytes": total,
        "seconds": round(dt, 4),
        "mbps": round(mb * 8 / dt, 2) if dt > 0 else 0,
        "mb_per_s": round(mb / dt, 2) if dt > 0 else 0,
        "errors": errors,
    }
    return result


# ---------- concurrent ----------

def run_concurrent(args) -> dict:
    """N 条并发长连接:建立 + **应用层往返校验** + 保持期持续存活。

    ★goal-repair A12:旧版只数 TCP 握手(established),4096 个"established"
    不等于 4096 条真实可用的隧道。现在每条连接必须完成一次 PING/PONG 往返
    (initial_verified),并在整个保持期内按秒持续 PING/PONG 存活
    (hold_alive);verified = 完成初次往返且保持到期末 —— G5 用这个数。

    非阻塞 + selectors,避免「一连接一线程」的 Python 线程上限。

    环境错误(EMFILE/ENFILE)返回带 env_error 的 dict(不折算成产品失败)。
    """
    import errno
    import selectors

    n = args.connections
    hold = args.hold
    verify_timeout = 10.0            # 单轮 PING/PONG 超时
    hold_tick = 1.0                  # 保持期每秒一轮存活探测
    total_rounds = max(1, int(round(hold)))

    soft, hard = raise_fd_limit()

    class Conn:
        __slots__ = ("s", "state", "outbuf", "inbuf", "connect_t0",
                     "await_deadline", "next_ping_at", "rounds_done",
                     "alive", "finished", "idx", "ping_t0", "err_note")

        def __init__(self, s):
            self.s = s
            self.state = "conn"      # conn → ver_r(初次 PING/PONG) → hold(周期存活)
            self.outbuf = b""
            self.inbuf = bytearray()
            self.connect_t0 = 0.0
            self.await_deadline = 0.0
            self.next_ping_at = 0.0
            self.rounds_done = 0
            self.alive = True
            self.finished = False
            self.idx = -1            # ★c84-diag:连接序号(失败可观测性)
            self.ping_t0 = 0.0       # ★c84-diag:最近一次 PING 发出时刻
            self.err_note = ""       # ★c84-diag:失败详情(EOF/timeout/badline/senderr)

    # ★c84-diag:逐连接失败原因留痕(只观测,不改变任何 deadline/口径;
    # 上限 64 条防膨胀;原因分类与 verify_failed/hold_lost 计数一一对应)
    failures: list[dict] = []
    verify_rtts: list[float] = []

    def note_failure(c: Conn, reason: str, extra: str = "") -> None:
        now = time.perf_counter()
        try:
            lport = c.s.getsockname()[1]
        except OSError:
            lport = 0
        rec = {
            "idx": c.idx, "local_port": lport, "state": c.state,
            "reason": reason, "detail": extra[:120],
            "connect_age_ms": round((now - c.connect_t0) * 1e3, 1),
            "await_age_ms": round((now - c.ping_t0) * 1e3, 1)
            if c.ping_t0 else None,
            "inbuf_head": bytes(c.inbuf[:16]).decode("latin1"),
        }
        if len(failures) < 64:
            failures.append(rec)

    sel = selectors.DefaultSelector()
    conns: list[Conn] = []
    lat: list[float] = []
    established = 0
    initial_verified = 0
    hold_alive = 0
    verified = 0
    connect_failed = 0
    verify_failed = 0
    hold_lost = 0
    alive_by_round = [0] * total_rounds

    def close_conn(c: Conn) -> None:
        c.finished = True
        try:
            sel.unregister(c.s)
        except (KeyError, ValueError):
            pass
        try:
            c.s.close()
        except OSError:
            pass

    def set_interest(c: Conn) -> None:
        events = selectors.EVENT_READ
        if c.outbuf:
            events |= selectors.EVENT_WRITE
        try:
            sel.modify(c.s, events, c)
        except (KeyError, ValueError):
            pass

    def send_pending(c: Conn) -> None:
        nonlocal connect_failed, verify_failed, hold_lost
        while c.outbuf:
            try:
                k = c.s.send(c.outbuf)
            except BlockingIOError:
                return
            except OSError as exc:
                c.alive = False
                note_failure(c, "senderr", f"errno={exc.errno} {exc}")
                if c.state == "conn":
                    connect_failed += 1
                elif c.state in ("ver_w", "ver_r"):
                    verify_failed += 1
                else:
                    hold_lost += 1
                close_conn(c)
                return
            c.outbuf = c.outbuf[k:]
        set_interest(c)

    def queue_ping(c: Conn, now: float) -> None:
        c.outbuf += b"PING\n"
        c.ping_t0 = now            # ★c84-diag:RTT/await 计时锚
        c.await_deadline = now + verify_timeout
        set_interest(c)

    def on_readable(c: Conn, now: float) -> None:
        nonlocal initial_verified, connect_failed, verify_failed, hold_lost
        try:
            data = c.s.recv(4096)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:               # 对端关闭
            c.alive = False
            note_failure(c, "EOF")
            if c.state in ("ver_w", "ver_r"):
                verify_failed += 1
            else:
                hold_lost += 1
            close_conn(c)
            return
        c.inbuf += data
        while b"\n" in c.inbuf:
            line, _, rest = c.inbuf.partition(b"\n")
            c.inbuf = bytearray(rest)
            if line.strip() != b"PONG":
                c.alive = False
                note_failure(c, "badline", f"line={bytes(line[:24])!r}")
                if c.state == "ver_r":
                    verify_failed += 1
                else:
                    hold_lost += 1
                close_conn(c)
                return
            # 收到合法 PONG
            c.await_deadline = 0.0
            if c.state == "ver_r":
                if c.ping_t0:
                    verify_rtts.append((now - c.ping_t0) * 1e3)
                initial_verified += 1
                c.state = "hold"
                c.next_ping_at = now + hold_tick
            elif c.state == "hold":
                c.rounds_done += 1
                if c.rounds_done <= total_rounds:
                    alive_by_round[c.rounds_done - 1] += 1
                c.next_ping_at = now + hold_tick
            set_interest(c)
            return   # 一次事件处理一轮,状态推进后重新计算兴趣

    t0 = time.perf_counter()
    started_us = int(time.time() * 1e6)

    # ---- 发起全量非阻塞连接;EMFILE/ENFILE 属环境错误 ----
    for _ in range(n):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            rc = s.connect_ex((args.host, args.port))
        except OSError as exc:
            rc = exc.errno or errno.EIO
        if rc in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
            c = Conn(s)
            c.idx = len(conns)
            c.connect_t0 = time.perf_counter()
            conns.append(c)
            sel.register(s, selectors.EVENT_WRITE, c)
        else:
            if rc in (errno.EMFILE, errno.ENFILE):
                for c in conns:
                    close_conn(c)
                sel.close()
                return {
                    "env_error": {
                        "kind": "emfile",
                        "errno": rc,
                        "connections": n,
                        "fd_soft": soft,
                        "fd_hard": hard,
                        "hint": "压测端 fd 不足:请提高 ulimit -n(EMFILE 不得计入产品并发失败)",
                    }
                }
            connect_failed += 1
            s.close()

    connect_deadline = t0 + args.timeout
    # 验证 + 保持期总时限:连接超时 + 每轮超时 × (1 + 轮数) + 余量
    overall = t0 + args.timeout + verify_timeout * (total_rounds + 2) + 10

    open_conns = list(conns)
    while open_conns and time.perf_counter() < overall:
        now = time.perf_counter()
        events = sel.select(timeout=0.1)
        for key, mask in events:
            c = key.data
            if c.finished:
                continue
            if mask & selectors.EVENT_WRITE:
                if c.state == "conn":
                    err = c.s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                    if err != 0:
                        if err in (errno.EMFILE, errno.ENFILE):
                            for cc in open_conns:
                                if not cc.finished:
                                    close_conn(cc)
                            sel.close()
                            return {"env_error": {"kind": "emfile", "errno": err,
                                                  "connections": n,
                                                  "fd_soft": soft, "fd_hard": hard}}
                        connect_failed += 1
                        close_conn(c)
                        continue
                    established += 1
                    lat.append((time.perf_counter() - c.connect_t0) * 1e3)
                    c.state = "ver_r"
                    queue_ping(c, time.perf_counter())
                    continue
                send_pending(c)
            if mask & selectors.EVENT_READ and not c.finished:
                on_readable(c, time.perf_counter())
        # ---- 周期维护:超时判定 + 保持期轮转 ----
        now = time.perf_counter()
        for c in open_conns:
            if c.finished:
                continue
            if c.state == "conn" and now > connect_deadline:
                connect_failed += 1
                close_conn(c)
            elif c.await_deadline and now > c.await_deadline:
                # 期望的 PONG 未按期到达
                c.alive = False
                note_failure(c, "timeout")
                if c.state == "ver_r":
                    verify_failed += 1
                elif c.state == "hold":
                    hold_lost += 1
                close_conn(c)
            elif c.state == "hold" and not c.await_deadline:
                if c.rounds_done >= total_rounds:
                    # 保持到期末仍活:结算
                    if c.alive:
                        hold_alive += 1
                        verified += 1
                    close_conn(c)
                elif now >= c.next_ping_at:
                    queue_ping(c, now)
        open_conns = [c for c in open_conns if not c.finished]

    # 整体超时仍未结束的连接按保持丢失计
    for c in open_conns:
        if not c.finished:
            c.alive = False
            note_failure(c, "overall_timeout")
            if c.state == "conn":
                connect_failed += 1
            elif c.state in ("ver_w", "ver_r"):
                verify_failed += 1
            else:
                hold_lost += 1
            close_conn(c)

    ended_us = int(time.time() * 1e6)
    setup_dt = time.perf_counter() - t0
    sel.close()

    out = {
        "connections": n,
        "established": established,
        "initial_verified": initial_verified,
        "hold_alive": hold_alive,
        "verified": verified,
        "failed": connect_failed + verify_failed + hold_lost,
        "connect_failed": connect_failed,
        "verify_failed": verify_failed,
        "hold_lost": hold_lost,
        "hold_seconds": hold,
        "hold_rounds": total_rounds,
        "alive_by_round": alive_by_round,
        "setup_seconds": round(setup_dt, 4),
        "connect_rate_per_s": round(established / setup_dt, 1) if setup_dt > 0 else 0,
        "started_us": started_us,
        "ended_us": ended_us,
    }
    out.update({f"connect_{k}": v for k, v in summarize(lat).items()})
    # ★c84-diag:逐连接失败留痕 + 初次往返 RTT 分布(观测字段;
    # verify_goal 的 G5 判定字段完全不变)
    out["failures"] = failures
    if verify_rtts:
        out["verify_rtt_ms"] = summarize(verify_rtts)
    return out


# ---------- soak ----------

def run_soak(args) -> dict:
    """持续压测:按秒分桶记录吞吐与错误,暴露衰减/抖动。"""
    duration = args.duration
    conns = args.connections
    size = args.size
    buckets: list[dict] = []
    lock = threading.Lock()
    stop = threading.Event()

    def worker(wid: int):
        while not stop.is_set():
            try:
                s = connect(args.host, args.port, args.timeout)
                s.settimeout(args.timeout)
                for _ in range(args.rounds_per_conn):
                    if stop.is_set():
                        break
                    s.sendall(b"PING\n")
                    if not s.recv(64).startswith(b"PONG"):
                        with lock:
                            errors[0] += 1
                        break
                    with lock:
                        rounds[0] += 1
                s.close()
            except OSError:
                with lock:
                    errors[0] += 1
                time.sleep(0.2)

    rounds = [0]
    errors = [0]
    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(conns)]
    for t in threads:
        t.start()

    last_rounds = 0
    while time.perf_counter() - t0 < duration:
        time.sleep(1.0)
        with lock:
            cur = rounds[0]
            errs = errors[0]
        buckets.append({
            "t": round(time.perf_counter() - t0, 2),
            "rps": cur - last_rounds,
            "rounds": cur,
            "errors": errs,
        })
        last_rounds = cur

    stop.set()
    for t in threads:
        t.join(timeout=5)
    dt = time.perf_counter() - t0

    rps_vals = [b["rps"] for b in buckets]
    out = {
        "duration": round(dt, 2),
        "connections": conns,
        "total_rounds": rounds[0],
        "errors": errors[0],
        "rps_mean": round(sum(rps_vals) / len(rps_vals), 1) if rps_vals else 0,
        "rps_min": min(rps_vals) if rps_vals else 0,
        "rps_max": max(rps_vals) if rps_vals else 0,
        "buckets": buckets,
    }
    return out


# ---------- udp ----------

def run_udp(args) -> dict:
    """UDP 吞吐与丢包(★goal-repair A8 修复:序号 + 去重 + 到时排空)。

    数据报 = 8 字节大端序号 + 以序号为键的确定性载荷(总长 = --size)。
    旧版到时立即停收,window=32 时每个样本恰好"丢"31 —— 那是终止污染,
    不是链路损失。现在:
      * 每个序号只记一次首达(重复回包计 dup);
      * deadline 后进入**固定时长排空期**(--drain,默认 5s),在途回包
        继续接收,首达计入 late_recv(单列,不与期内回包混算);
      * 损失口径 = sent - 去重后的唯一回包数(排空后),RTT 按各自发送时刻。
    """
    payload_size = args.size
    hdr = 8
    body = payload_size - hdr
    if body <= 0:
        return {"error": f"--size 必须 > {hdr} 字节(含 8B 序号头)", "packet_size": payload_size}
    duration = args.duration
    drain_s = float(getattr(args, "drain", 5.0))
    rto = float(getattr(args, "rto", 2.0))
    # ★多源:每个源一个独立 UDP socket,绑定不同本地端口。
    # 隧道侧据此产生多个并发"用户会话"(代理侧按用户源地址建会话+回写任务),
    # 这正是多会话写路径的压测场景。sources=1 时与旧行为逐字节等价。
    n_src = max(1, int(getattr(args, "sources", 1)))
    socks: list[socket.socket] = []
    for _ in range(n_src):
        sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sk.bind(("0.0.0.0", 0))  # 系统分配本地端口 → 独特的源地址
        socks.append(sk)
    # 回包可能落在任一源 socket 上;统一非阻塞轮询
    for sk in socks:
        sk.setblocking(False)
    sock = socks[0]  # 兼容下面的错误路径引用(仅用于首包失败判定)
    addr = (args.host, args.port)

    sent = 0
    send_errors = 0
    recv_in_time = 0     # 发送期内首次到达的唯一序号数
    late_recv = 0        # 排空期才首次到达的唯一序号数
    dup = 0
    corrupt = 0
    seen: set[int] = set()
    pending: dict[int, float] = {}
    lat: list[float] = []
    started_us = int(time.time() * 1e6)

    def expected(seq: int) -> bytes:
        return seq.to_bytes(hdr, "big") + pattern_slice(seq * 4096, body)

    def handle(data: bytes, now: float, in_window: bool) -> None:
        nonlocal dup, corrupt, recv_in_time, late_recv
        if len(data) != payload_size:
            corrupt += 1
            return
        seq = int.from_bytes(data[:hdr], "big")
        if data[hdr:] != pattern_slice(seq * 4096, body):
            corrupt += 1
            return
        if seq in seen:
            dup += 1
            return
        seen.add(seq)
        if in_window:
            recv_in_time += 1
        else:
            late_recv += 1
        t0 = pending.pop(seq, None)
        if t0 is not None:
            lat.append((now - t0) * 1e3)

    t0 = time.perf_counter()
    deadline = t0 + duration
    try:
        while True:
            now = time.perf_counter()
            if now >= deadline:
                break
            # 窗口口径:未确认且未过 RTO 的报文数(过 RTO 视为疑似丢失,继续发)
            outstanding = sum(1 for ts in pending.values() if now - ts <= rto)
            while outstanding < args.window:
                dgram = expected(sent)
                try:
                    # 轮转源:保证每个 source 都有稳定流量,且同一 source
                    # 的序号严格递增(隧道侧会话内保序可验证)。
                    socks[sent % n_src].sendto(dgram, addr)
                except OSError:
                    send_errors += 1
                    if sent == 0:
                        return {
                            "error": (f"sendto 失败(报文 {payload_size}B 可能超过本机 MTU;"
                                      f"Linux 隧道下建议 ≤ 60000)"),
                            "packet_size": payload_size,
                            "send_errors": send_errors,
                        }
                    break
                pending[sent] = time.perf_counter()
                sent += 1
                outstanding += 1
                if time.perf_counter() >= deadline:
                    break
            got = False
            for sk in socks:
                try:
                    data, _ = sk.recvfrom(65535)
                except OSError:
                    continue
                handle(data, time.perf_counter(), in_window=True)
                got = True
                break
            if not got:
                time.sleep(0.001)

        send_end = time.perf_counter()
        # ---- 排空期(固定 drain_s):在途回包继续收,迟到首达单列 ----
        drain_until = send_end + drain_s
        while time.perf_counter() < drain_until:
            got = False
            for sk in socks:
                try:
                    data, _ = sk.recvfrom(65535)
                except OSError:
                    continue
                handle(data, time.perf_counter(), in_window=False)
                got = True
                break
            if not got:
                time.sleep(0.001)
    finally:
        for sk in socks:
            sk.close()

    ended_us = int(time.time() * 1e6)
    recv_unique = len(seen)
    lost = sent - recv_unique
    send_dt = send_end - t0
    out = {
        "sent": sent,
        "received": recv_in_time,        # 期内唯一回包
        "late_recv": late_recv,          # 排空期到达的唯一回包(A8 口径单列)
        "dup": dup,
        "corrupt": corrupt,
        "recv_unique": recv_unique,
        "lost": lost,
        "loss_pct": round(lost * 100.0 / sent, 3) if sent else 0.0,
        "send_errors": send_errors,
        "packet_size": payload_size,
        "sources": n_src,
        "seconds": round(duration, 3),
        "drain_seconds": round(drain_s, 3),
        "mbps": round(recv_unique * payload_size * 8 / max(send_dt, 1e-9) / 1e6, 2),
        "pps": round(recv_unique / max(send_dt, 1e-9), 1),
        "started_us": started_us,
        "ended_us": ended_us,
    }
    out.update({f"rtt_{k}": v for k, v in summarize(lat).items()})
    return out


# ---------- http ----------

def run_http(args) -> dict:
    """HTTP 吞吐:keep-alive 长连接串行请求,测 QPS。"""
    import http.client

    duration = args.duration
    conns = args.connections
    path = args.path
    lock = threading.Lock()
    counts = [0]
    errors = [0]
    lats: list[float] = []
    stop = threading.Event()

    def worker():
        local_lat: list[float] = []
        local_ok = 0
        try:
            conn = http.client.HTTPConnection(args.host, args.port, timeout=args.timeout)
            conn.connect()
            while not stop.is_set():
                t0 = time.perf_counter()
                conn.request("GET", path, headers={"Host": args.host_header or args.host})
                resp = conn.getresponse()
                body = resp.read()
                dt = (time.perf_counter() - t0) * 1e3
                if resp.status != 200:
                    raise OSError(f"status {resp.status}")
                local_ok += 1
                if len(local_lat) < 200000:
                    local_lat.append(dt)
            conn.close()
        except Exception:
            with lock:
                errors[0] += 1
        finally:
            with lock:
                counts[0] += local_ok
                lats.extend(local_lat)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(conns)]
    for t in threads:
        t.start()
    time.sleep(duration)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    dt = time.perf_counter() - t0

    out = {
        "connections": conns,
        "requests": counts[0],
        "errors": errors[0],
        "seconds": round(dt, 3),
        "qps": round(counts[0] / dt, 1) if dt > 0 else 0,
    }
    out.update({f"lat_{k}": v for k, v in summarize(lats).items()})
    return out


# ---------- connect ----------

def run_connect(args) -> dict:
    """连接建立速率:每连接一次 PING 往返后关闭(测 accept/握手开销)。"""
    n = args.connections
    conns = args.parallel
    lock = threading.Lock()
    claimed = [0]        # 已认领的任务数(全局配额,避免超发)
    errors = [0]
    lats: list[float] = []

    def worker():
        local_lat: list[float] = []
        local_ok = 0
        local_err = 0
        while True:
            with lock:
                if claimed[0] >= n:
                    break
                claimed[0] += 1
            try:
                t0 = time.perf_counter()
                s = connect(args.host, args.port, args.timeout)
                s.sendall(b"PING\n")
                if not s.recv(64).startswith(b"PONG"):
                    raise OSError("bad resp")
                local_lat.append((time.perf_counter() - t0) * 1e3)
                local_ok += 1
                s.close()
            except OSError:
                local_err += 1
        with lock:
            errors[0] += local_err
            lats.extend(local_lat)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(conns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=args.timeout * 10)
    dt = time.perf_counter() - t0

    out = {
        "requested": n,
        "completed": len(lats),
        "errors": errors[0],
        "seconds": round(dt, 4),
        "rate_per_s": round(len(lats) / dt, 1) if dt > 0 else 0,
    }
    out.update({f"lat_{k}": v for k, v in summarize(lats).items()})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="rabbithole benchmark 压测引擎")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--timeout", type=float, default=15.0)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("latency")
    p.add_argument("--count", type=int, default=2000)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--newconn", action="store_true")

    p = sub.add_parser("throughput")
    p.add_argument("--direction", choices=["download", "upload", "bidir"], default="download")
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--size", type=int, default=1 << 30)
    p.add_argument("--mode", choices=["pattern", "zeros", "random"], default="pattern")

    p = sub.add_parser("concurrent")
    p.add_argument("--connections", type=int, default=256)
    p.add_argument("--hold", type=float, default=5.0)

    p = sub.add_parser("soak")
    p.add_argument("--connections", type=int, default=64)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--rounds-per-conn", type=int, default=50)
    p.add_argument("--size", type=int, default=0)

    p = sub.add_parser("udp")
    p.add_argument("--sources", type=int, default=1,
                   help="并发源地址数:每源一个独立 UDP socket(= 隧道侧一个用户会话)。"
                        "默认 1 = 旧行为。用于压测多会话写路径。")
    p.add_argument("--size", type=int, default=1200,
                   help="数据报总字节数(含 8B 序号头;G4 口径 1200)")
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--window", type=int, default=32)
    p.add_argument("--drain", type=float, default=5.0,
                   help="到时后的固定排空秒数(A8:在途回包继续收,迟到单列)")
    p.add_argument("--rto", type=float, default=2.0,
                   help="报文视为疑似丢失的等待秒数(仅影响窗口节奏,不判定损失)")

    p = sub.add_parser("http")
    p.add_argument("--connections", type=int, default=16)
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--path", default="/")
    p.add_argument("--host-header", default="")

    p = sub.add_parser("connect")
    p.add_argument("--connections", type=int, default=2000)
    p.add_argument("--parallel", type=int, default=32)

    args = ap.parse_args()

    soft, hard = raise_fd_limit()
    # 并发场景需要的 fd 数超过可用上限时:环境错误(env_error),不是产品失败
    if args.cmd == "concurrent" and args.connections > soft - 64:
        print(json.dumps({
            "env_error": {
                "kind": "fd_limit",
                "hint": (f"fd 上限不足:需要 ~{args.connections},可用 {soft}"
                         f"(硬上限 {hard});请提高 ulimit -n 后重试"),
            },
            "connections": args.connections, "fd_soft": soft, "fd_hard": hard,
        }))
        return 1

    runners = {
        "latency": run_latency,
        "throughput": run_throughput,
        "concurrent": run_concurrent,
        "soak": run_soak,
        "udp": run_udp,
        "http": run_http,
        "connect": run_connect,
    }
    try:
        result = runners[args.cmd](args)
    except Exception as exc:  # 压测失败也要输出结构化结果
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1
    print(json.dumps(result))
    # 环境错误(EMFILE 等)单独非零退出:驱动脚本据此中止本轮
    if isinstance(result, dict) and "env_error" in result:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
