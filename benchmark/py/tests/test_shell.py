"""shell 层回归:lib/common.sh 助手与 peer.sh 的 run_iperf(本机 iperf3,不连远端)。

覆盖:
  * require_run_env 显式环境(A9:无隐式默认值)
  * mux_bool 严格值域
  * record_nofile / check_nofile(软限不足 = 环境错误,rc=1)
  * record_identity SHA-256(带回落路径)
  * epoch_us
  * run_iperf 留痕(A11:raw json/stderr/exit;失败不造 0)—— 本机 iperf3 实测
  * 新 peer.sh 不再以 `// 0` + `2>/dev/null` 掩盖 iperf 失败(旧版红证)
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import fixtures as fx  # noqa: E402


def sh(script: str, **env) -> subprocess.CompletedProcess:
    return fx.run_bash(script, env_extra=env)


class TestCommonSh(unittest.TestCase):
    def test_require_run_env_missing_fails(self):
        r = sh('source lib/common.sh; require_run_env test-role')
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIn("缺少显式环境变量", r.stderr)

    def test_require_run_env_explicit_ok(self):
        r = sh('source lib/common.sh; require_run_env drive '
               'PROTO=tls RELAY=copy MUX=1 PEER_RUN_ID=run-x',
               PROTO="tls", RELAY="copy", MUX="1", PEER_RUN_ID="run-x")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("PROTO=tls", r.stdout)
        self.assertIn("RELAY=copy", r.stdout)

    def test_mux_bool_strict(self):
        r = sh('source lib/common.sh; MUX=0 mux_bool')
        self.assertEqual(r.stdout.strip(), "false")
        r = sh('source lib/common.sh; MUX=1 mux_bool')
        self.assertEqual(r.stdout.strip(), "true")
        for bad in ("2", "yes"):
            r = sh('source lib/common.sh; MUX="$MUX" mux_bool', MUX=bad)
            self.assertNotEqual(r.returncode, 0,
                                f"MUX={bad!r} 必须被拒绝(值域只有 0/1)")
        r = sh('source lib/common.sh; unset MUX; mux_bool')
        self.assertNotEqual(r.returncode, 0, "MUX 未设置必须失败(无隐式默认)")

    def test_record_nofile_writes_json(self):
        r = sh('PEER_DIR=$(mktemp -d); source lib/common.sh; '
               'raise_nofile 65535; record_nofile drive 65535 >/dev/null; '
               'cat "$PEER_DIR/nofile.json"')
        self.assertEqual(r.returncode, 0, r.stderr)
        doc = json.loads(r.stdout.strip().splitlines()[-1])
        self.assertEqual(doc["role"], "drive")
        self.assertIn("soft", doc)
        self.assertIn("ok", doc)
        self.assertTrue(isinstance(doc["soft"], str))

    def test_check_nofile_below_need_is_env_error(self):
        """软限压到 256 后要求 4608:必须以环境错误失败。"""
        r = sh('ulimit -n 256 2>/dev/null || true; PEER_DIR=$(mktemp -d); '
               'source lib/common.sh; check_nofile drive 4608; echo rc=$?')
        self.assertIn("环境错误", r.stdout + r.stderr)
        self.assertNotIn("rc=0", r.stdout)

    def test_record_identity_sha256_with_fallback_path(self):
        r = sh('PEER_DIR=$(mktemp -d); source lib/common.sh; '
               'tmp=$(mktemp); printf "#!/bin/sh\\necho v9.9.9\\n" > "$tmp"; chmod +x "$tmp"; '
               'record_identity rh-server 999999999 "$tmp" >/dev/null; '
               'echo "$tmp"; cat "$PEER_DIR/identity.jsonl"')
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = r.stdout.strip().splitlines()
        exe_path = lines[0]
        rec = json.loads(lines[1])
        want = hashlib.sha256(open(exe_path, "rb").read()).hexdigest()
        self.assertEqual(rec["exe_sha256"], want)
        self.assertEqual(rec["version"], "v9.9.9")
        self.assertEqual(rec["source"], "fallback_path")

    def test_epoch_us(self):
        r = sh('source lib/common.sh; epoch_us')
        val = int(r.stdout.strip())
        self.assertGreater(val, 1_700_000_000_000_000)


class TestRunIperf(unittest.TestCase):
    """A11:iperf 原始 JSON/stderr/退出码留痕;失败不得记 0(本机 iperf3)。"""

    @classmethod
    def setUpClass(cls):
        # 本机没有 iperf3 时跳过(三机验收机上必测)
        if subprocess.run(["bash", "-c", "command -v iperf3"], capture_output=True).returncode:
            raise unittest.SkipTest("本机无 iperf3(验收机上运行)")

    def _run_iperf(self, port: int, raw_id: str) -> dict:
        # 注意先 source 再设 RAW_DIR(peer.sh 顶层会重置这些全局变量)
        script = (
            'source peer.sh; RAW_DIR=$(mktemp -d); PEER_DURATION=2; '
            f'echo "$RAW_DIR"; run_iperf 127.0.0.1 {port} 1 "" {raw_id}'
        )
        r = fx.run_bash(script, env_extra={"THP_TOOL": "iperf3", "IPERF3_BIN": "iperf3"}, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
        raw_dir = lines[0]
        return json.loads(lines[1]), raw_dir

    def test_valid_run_saves_raw_triple(self):
        port = fx.free_port()
        srv = subprocess.Popen(["iperf3", "-s", "-1", "-p", str(port)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            out, raw_dir = self._run_iperf(port, "t-valid")
        finally:
            srv.terminate()
            srv.wait(timeout=5)
        self.assertTrue(out["valid"], out)
        self.assertGreater(out["mbps"], 0)
        self.assertEqual(out["exit"], 0)
        for suffix in ("json", "stderr", "exit"):
            p = os.path.join(raw_dir, f"t-valid.{suffix}")
            self.assertTrue(os.path.isfile(p), p)
        raw = json.load(open(os.path.join(raw_dir, "t-valid.json")))
        bps = raw["end"]["sum_received"]["bits_per_second"]
        self.assertEqual(out["mbps"], int(bps / 1e6))   # 记录值可由原始数据重算
        self.assertEqual(open(os.path.join(raw_dir, "t-valid.exit")).read().strip(), "0")
        self.assertIn("window_start_us", out)
        self.assertIn("window_end_us", out)

    def test_failure_recorded_not_zero(self):
        port = fx.free_port()   # 无人监听 → iperf3 连接失败
        out, raw_dir = self._run_iperf(port, "t-fail")
        self.assertFalse(out["valid"], out)
        self.assertIsNone(out["mbps"], "失败样本 mbps 必须为 null,不得记 0")
        self.assertNotEqual(out["exit"], 0)
        self.assertIn("error", out)
        # 失败也留痕:三个 raw 文件都在
        for suffix in ("json", "stderr", "exit"):
            self.assertTrue(os.path.isfile(os.path.join(raw_dir, f"t-fail.{suffix}")))


class TestLoadgenFailureAccounting(unittest.TestCase):
    """run_loadgen 在 $() 子 shell 里执行:失败计数必须在父 shell 补记。

    回归背景:run_loadgen 内部的 FAILS++ 会随子 shell 丢失,曾导致
    loadgen 失败完全不影响 drive 退出码(违反"失败非零退出")。"""

    def test_loadgen_failed_counts_in_parent(self):
        r = sh('source peer.sh; FAILS=0; '
               'loadgen_failed \'{"ok":false,"loadgen_exit":1}\'; '
               'loadgen_failed \'{"mbps":123}\'; echo "FAILS=$FAILS"')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("FAILS=1", r.stdout)

    def test_failed_run_loadgen_output_counted_end_to_end(self):
        """端到端:真实调用一个必失败的 loadgen 子命令 → ok:false + 父 shell FAILS=1。

        用 udp --size 4(小于 8 字节序号头)制造确定性失败。"""
        r = sh('source peer.sh; RAW_DIR=$(mktemp -d); FAILS=0; '
               'out=$(run_loadgen t-x udp --host 127.0.0.1 --port 1 '
               '--size 4 --duration 0.2 --window 4 --drain 0.2); '
               'okval=$(printf \'%s\' "$out" | jq -r \'.ok\'); '
               'loadgen_failed "$out" || true; echo "OK=$okval FAILS=$FAILS"',
               LOADGEN_BIN=fx.go_loadgen_bin())
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("OK=false", r.stdout, r.stdout)
        self.assertIn("FAILS=1", r.stdout, r.stdout)


class TestNoMaskedFailures(unittest.TestCase):
    """静态红→绿:旧 peer.sh 用 `2>/dev/null` + `// 0` 吞掉 iperf 失败;新版禁止。"""

    def test_red_old_peer_sh_masked_iperf_failures(self):
        old = open(os.path.join(fx.BEFORE_FIX, "peer.sh"), encoding="utf-8").read()
        self.assertIn("2>/dev/null", old)
        self.assertIn("// 0", old)

    def test_new_peer_sh_does_not_mask(self):
        src = open(os.path.join(fx.BENCH, "peer.sh"), encoding="utf-8").read()
        # 吞失败的两个形态都必须消失:iperf 行的 2>/dev/null 与 jq // 0
        bad_lines = [ln for ln in src.splitlines()
                     if "-J 2>/dev/null" in ln or "// 0'" in ln or '// 0"' in ln]
        self.assertEqual(bad_lines, [], f"新 peer.sh 仍存在掩盖失败的行:{bad_lines}")

    def test_new_peer_sh_fails_nonzero_on_invalid(self):
        src = open(os.path.join(fx.BENCH, "peer.sh"), encoding="utf-8").read()
        self.assertIn("FAILS=$((FAILS+1))", src)
        self.assertIn("return 1", src)


if __name__ == "__main__":
    unittest.main()
