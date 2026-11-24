"""A8 回归:UDP 序号去重 + 到时排空(红→绿)。

红:修复前 loadgen(benchmark/runs/goal-repair/harness/before-fix/loadgen.py)
    到时立即停收,扣押的在途回包被计为"丢包" —— REVIEW.md 记录的
    "32 个样本恰好丢 31"终止污染。
绿:现行 loadgen.run_udp 在排空期收回在途回包(late_recv 单列),
    重复回包去重(dup),篡改包计入 corrupt,损失 = sent - recv_unique。
"""
from __future__ import annotations

import argparse
import sys
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import fixtures as fx  # noqa: E402

old_loadgen = fx.load_before_fix("loadgen.py")


def run_go_udp(host, port, *, duration=1.0, drain=3.0, window=32, size=1200):
    return fx.run_go_loadgen(
        "--host", host, "--port", str(port),
        "udp",
        "--size", str(size),
        "--duration", str(duration),
        "--window", str(window),
        "--drain", str(drain),
        "--rto", "0.5",
        timeout=30,
    )


class TestUdpDrain(unittest.TestCase):
    def test_inflight_replies_counted_after_drain(self):
        """扣押 20 个在途回包:排空期全部收回 → lost=0,late_recv≥20。"""
        srv = fx.DelayedUdpEcho(hold_n=20, release_after=1.6)
        srv.start()
        try:
            out = run_go_udp("127.0.0.1", srv.port)
        finally:
            srv.close()
        # late_recv 含被扣押的 20 个 + 截止时刻仍在途的自然存量;
        # 关键不变量:排空后 lost == 0(终止污染消除)
        self.assertGreaterEqual(out["late_recv"], 20, out)
        self.assertEqual(out["lost"], 0, out)
        self.assertEqual(out["loss_pct"], 0.0, out)
        self.assertGreater(out["sent"], 20)

    def test_red_old_loadgen_counts_inflight_as_lost(self):
        """同一场景在修复前快照上:在途回包被计为丢失(红证)。

        旧实现到时即停,扣押的 20 个 + 窗口内自然在途(≈window-1)全部计损;
        REVIEW.md 的"window=32 恰好丢 31"正是这个形态。"""
        srv = fx.DelayedUdpEcho(hold_n=20, release_after=1.6)
        srv.start()
        try:
            out = old_loadgen.run_udp(argparse.Namespace(
                host="127.0.0.1", port=srv.port, size=1200,
                duration=1.0, window=32))
        finally:
            srv.close()
        self.assertGreaterEqual(out["lost"], 20, out)
        self.assertNotIn("late_recv", out,
                         "旧实现没有排空口径字段(这本身就是缺陷)")

    def test_duplicate_replies_deduped(self):
        """5 个重复回包只计一次:dup=5,recv_unique==sent,lost=0。"""
        srv = fx.DelayedUdpEcho(dup_n=5, dup_from=30)
        srv.start()
        try:
            out = run_go_udp("127.0.0.1", srv.port)
        finally:
            srv.close()
        self.assertEqual(out["dup"], 5, out)
        self.assertEqual(out["lost"], 0, out)
        self.assertEqual(out["recv_unique"], out["sent"], out)

    def test_corrupt_detected_and_loss_consistent(self):
        """3 个篡改回包:corrupt=3,lost=3,loss_pct 与 (sent-recv_unique)/sent 一致。"""
        srv = fx.DelayedUdpEcho(corrupt_n=3, corrupt_from=40)
        srv.start()
        try:
            out = run_go_udp("127.0.0.1", srv.port)
        finally:
            srv.close()
        self.assertEqual(out["corrupt"], 3, out)
        self.assertEqual(out["lost"], 3, out)
        self.assertAlmostEqual(out["loss_pct"], 3 * 100.0 / out["sent"], places=2)

    def test_drain_fields_present(self):
        """A8 口径字段必须在(late_recv/dup/recv_unique/drain_seconds)。"""
        srv = fx.DelayedUdpEcho()
        srv.start()
        try:
            out = run_go_udp("127.0.0.1", srv.port)
        finally:
            srv.close()
        for field in ("late_recv", "dup", "recv_unique", "drain_seconds"):
            self.assertIn(field, out)


if __name__ == "__main__":
    unittest.main()
