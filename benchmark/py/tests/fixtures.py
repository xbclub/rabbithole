"""benchmark/py/tests 共享测试基建(标准库 unittest)。

红→绿口径:
  * 绿 = 当前 benchmark 实现(py/*.py、peer.sh、lib/common.sh)上的断言;
  * 红 = 同一断言在 benchmark/runs/goal-repair/harness/before-fix/ 的
    修复前快照上必须失败(快照在本轮修复开始前留档,SHA256SUMS.txt 可核)。
"""
from __future__ import annotations

import atexit
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

BENCH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PY_DIR = os.path.join(BENCH, "py")
BEFORE_FIX = os.path.join(BENCH, "runs", "goal-repair", "harness", "before-fix")
GO_LOADGEN_SRC = os.path.join(BENCH, "loadgen")

sys.path.insert(0, PY_DIR)

_GO_LOADGEN_BIN: str | None = None
_GO_LOADGEN_TMPDIR: str | None = None


def go_loadgen_bin() -> str:
    """Build benchmark/loadgen once per test process and return its binary path."""
    global _GO_LOADGEN_BIN, _GO_LOADGEN_TMPDIR
    if _GO_LOADGEN_BIN and os.path.isfile(_GO_LOADGEN_BIN):
        return _GO_LOADGEN_BIN
    _GO_LOADGEN_TMPDIR = tempfile.mkdtemp(prefix="rh-go-loadgen-test-")
    atexit.register(lambda d=_GO_LOADGEN_TMPDIR: shutil.rmtree(d, ignore_errors=True))
    bin_path = os.path.join(_GO_LOADGEN_TMPDIR, "loadgen")
    res = subprocess.run(
        ["go", "build", "-o", bin_path, "."],
        cwd=GO_LOADGEN_SRC,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"failed to build Go loadgen: {res.stderr}\n{res.stdout}")
    _GO_LOADGEN_BIN = bin_path
    return bin_path


def run_go_loadgen(*args: str, timeout: float = 60) -> dict:
    """Run compiled Go loadgen subcommand and decode stdout JSON."""
    proc = subprocess.run(
        [go_loadgen_bin(), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0 and not proc.stdout.strip():
        raise RuntimeError(f"loadgen {args} failed ({proc.returncode}): {proc.stderr}")
    return json.loads(proc.stdout.strip())


def load_before_fix(module_filename: str):
    """以独立模块名加载修复前快照(与当前实现互不污染)。"""
    path = os.path.join(BEFORE_FIX, module_filename)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"修复前快照缺失:{path}")
    modname = "rh_before_fix_" + module_filename.replace(".", "_").replace("-", "_")
    spec = importlib.util.spec_from_file_location(modname, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_bash(script: str, *, env_extra: dict | None = None, timeout: float = 120):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True,
        cwd=BENCH, env=env, timeout=timeout)


class DelayedUdpEcho(threading.Thread):
    """可控 UDP 回显:支持扣押(hold)、延迟释放、重复回包、篡改。

    行为按键空间(报文前 8 字节是大端序号,与 loadgen.run_udp 对齐):
      seq < hold_n            → 扣押,release_after 秒后统一回发(模拟在途)
      seq in dup range        → 回发两次(去重口径)
      seq in corrupt range    → 回发篡改包(损坏检测)
    """

    def __init__(self, hold_n=0, release_after=0.0, dup_n=0, dup_from=0,
                 corrupt_n=0, corrupt_from=0):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.hold_n = hold_n
        self.release_after = release_after
        self.dup_n = dup_n
        self.dup_from = dup_from
        self.corrupt_n = corrupt_n
        self.corrupt_from = corrupt_from
        self.stop = threading.Event()
        self.received = 0

    def run(self):
        held: list[tuple[bytes, tuple]] = []
        t0 = time.perf_counter()
        while not self.stop.is_set():
            self.sock.settimeout(0.05)
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                if held and time.perf_counter() - t0 >= self.release_after:
                    for d, a in held:
                        self.sock.sendto(d, a)
                    held = []
                continue
            except OSError:
                break
            self.received += 1
            seq = int.from_bytes(data[:8], "big")
            if seq < self.hold_n and time.perf_counter() - t0 < self.release_after:
                held.append((data, addr))
                continue
            if self.corrupt_from <= seq < self.corrupt_from + self.corrupt_n:
                # 篡改包替代原文(只回坏包 → 客户端必须判 corrupt 且计损失)
                bad = data[:-1] + bytes([data[-1] ^ 0xFF])
                self.sock.sendto(bad, addr)
                continue
            self.sock.sendto(data, addr)
            if self.dup_from <= seq < self.dup_from + self.dup_n:
                self.sock.sendto(data, addr)
        for d, a in held:
            try:
                self.sock.sendto(d, a)
            except OSError:
                pass

    def close(self):
        self.stop.set()
        self.join(timeout=2)
        self.sock.close()


class AcceptOnlyServer(threading.Thread):
    """接受 TCP 连接但永不响应(检测"仅握手"误判)。"""

    def __init__(self):
        super().__init__(daemon=True)
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(256)
        self.port = self.sock.getsockname()[1]
        self.stop = threading.Event()
        self.conns: list[socket.socket] = []

    def run(self):
        self.sock.settimeout(0.1)
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
                self.conns.append(conn)
            except socket.timeout:
                continue
            except OSError:
                break

    def close(self):
        self.stop.set()
        self.join(timeout=2)
        for c in self.conns:
            try:
                c.close()
            except OSError:
                pass
        self.sock.close()


def write_results(dir_path: str, name: str, records: list[dict]) -> str:
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, name)
    with open(path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    return path


def thp(label, path, direction, streams, sample, mbps, *, valid=True,
        proto="tcp", mux="true", relay="splice", run_id="r1"):
    rec = {
        "metric": "throughput", "path": path, "dir": direction, "streams": streams,
        "sample": sample, "proto": proto, "mux": mux, "relay": relay,
        "run_id": run_id, "label": label,
        "valid": valid, "mbps": mbps if valid else None,
        "exit": 0 if valid else 1,
        "window_start_us": 1800000000000000, "window_end_us": 1800000002000000,
        "raw_json": f"raw/thp-{label}-s{sample}-{path}-{direction}-{streams}s.json",
        "raw_stderr": f"raw/thp-{label}-s{sample}-{path}-{direction}-{streams}s.stderr",
        "raw_exit": f"raw/thp-{label}-s{sample}-{path}-{direction}-{streams}s.exit",
    }
    if not valid:
        rec["error"] = {"iperf_exit": 1, "stderr_tail": "synthetic"}
    return rec


def tmp_dir():
    return tempfile.mkdtemp(prefix="rh-bench-test-")
