"""A12 回归:并发 = 应用层往返校验 + 保持期存活(红→绿)。

红:修复前 loadgen.run_concurrent 只数 TCP 握手(established),
    "4096 established"≠4096 条真实可用隧道。
绿:现行实现要求每条连接完成 PING/PONG(initial_verified)并在保持期
    按秒存活(hold_alive);verified = 两者皆备。
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import fixtures as fx  # noqa: E402

old_loadgen = fx.load_before_fix("loadgen.py")


def run_go_conc(host, port, *, connections=48, hold=1.5, timeout=10.0):
    return fx.run_go_loadgen(
        "--host", host, "--port", str(port), "--timeout", str(timeout),
        "concurrent",
        "--connections", str(connections),
        "--hold", str(hold),
        timeout=60,
    )


class RealTarget:
    """起 Go loadgen target 的 PING/PONG 行协议服务。"""

    def __init__(self):
        self.port = fx.free_port()
        self._tmp = tempfile.mkdtemp(prefix="rh-target-test-")
        ready = os.path.join(self._tmp, "ready")
        self.proc = subprocess.Popen(
            [fx.go_loadgen_bin(), "target", "--tcp-port", str(self.port), "--ready-file", ready],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if os.path.isfile(ready):
                return
            if self.proc.poll() is not None:
                err = self.proc.stderr.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"go loadgen target exited early: {err}")
            time.sleep(0.02)
        self.close()
        raise RuntimeError("go loadgen target readiness timeout")

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self.proc.stderr:
            self.proc.stderr.close()
        shutil.rmtree(self._tmp, ignore_errors=True)


class TestConcurrentVerified(unittest.TestCase):
    def test_verified_equals_connections_on_real_target(self):
        t = RealTarget()
        try:
            out = run_go_conc("127.0.0.1", t.port)
        finally:
            t.close()
        self.assertEqual(out["established"], 48, out)
        self.assertEqual(out["initial_verified"], 48, out)
        self.assertEqual(out["hold_alive"], 48, out)
        self.assertEqual(out["verified"], 48, out)
        self.assertEqual(out["failed"], 0, out)
        self.assertIsInstance(out["started_us"], int)
        self.assertIsInstance(out["ended_us"], int)
        self.assertEqual(len(out["alive_by_round"]), out["hold_rounds"], out)

    def test_accept_only_server_fails_verification(self):
        """只握手不回 PONG:established>0 但 verified=0(旧口径会把这当成功)。"""
        srv = fx.AcceptOnlyServer()
        srv.start()
        try:
            out = run_go_conc("127.0.0.1", srv.port, connections=24, hold=1.0)
        finally:
            srv.close()
        self.assertEqual(out["established"], 24, out)
        self.assertEqual(out["initial_verified"], 0, out)
        self.assertEqual(out["verified"], 0, out)
        self.assertEqual(out["verify_failed"], 24, out)

    def test_red_old_concurrent_has_no_verified_semantics(self):
        """修复前快照:同一"只握手"场景被计为成功(established=24)且无 verified 字段。"""
        srv = fx.AcceptOnlyServer()
        srv.start()
        try:
            out = old_loadgen.run_concurrent(argparse.Namespace(
                host="127.0.0.1", port=srv.port, connections=24,
                hold=0.5, timeout=10.0))
        finally:
            srv.close()
        self.assertEqual(out["established"], 24, out)
        self.assertNotIn("verified", out,
                         "旧实现没有应用层校验字段(握手成功≠隧道可用,这是缺陷)")

    def test_old_concurrent_real_target_also_lacks_verified(self):
        """红证补充:即使对端真实可用,旧实现也不产生任何应用层校验计数。"""
        t = RealTarget()
        try:
            out = old_loadgen.run_concurrent(argparse.Namespace(
                host="127.0.0.1", port=t.port, connections=16,
                hold=0.5, timeout=10.0))
        finally:
            t.close()
        self.assertEqual(out["established"], 16, out)
        for field in ("verified", "initial_verified", "hold_alive"):
            self.assertNotIn(field, out)


if __name__ == "__main__":
    unittest.main()
