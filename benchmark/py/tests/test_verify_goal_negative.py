"""verify_goal r2 负例回归(主代理独立复现的 6 个误通过 + r2 新增硬检查)。

背景:主代理用 /tmp/rh-main-independent-review/check_verifier_negative.py 对
r1 版核验器复现出 6 个误通过(正对照 PASS,六反例全部错误 PASS):
  1) UDP packet_size=1 而非 1200;
  2) 删掉 ws/wss/quic/kcp 全部 concurrent 记录;
  3) sources.sha256 只有伪造 hash 和 missing-current-source.rs;
  4) concurrent 只留 verified/established,删 hold_alive/initial_verified/
     hold_rounds/alive_by_round;
  5) raw iperf start.test_start 写 UDP+reverse=1+num_streams=99;
  6) 端点与 drive nofile=64 且 ok=false。
本文件按同样变异在当前核验器上断言全部 FAIL(方法独立重写,不 import 主代
理脚本),另含 r2 新增的:重复时间窗、NaN、跨目录拼装、伪造保持、时间戳
偏移、目标错配、未排空 UDP、下行 CPU 超限(方向分别判定)。
正向对照(good set)必须 PASS,防止核验器退化成"永远 FAIL"。
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import types as types_mod
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import fixtures as fx  # noqa: E402
import verify_goal  # noqa: E402


def mutate_results(root, edit):
    """对 root 下所有 results-*.jsonl 逐行应用 edit(dirname, rows) → rows。"""
    import glob
    for path in glob.glob(os.path.join(root, "*", "results-*.jsonl")):
        rows = [json.loads(x) for x in open(path, encoding="utf-8") if x.strip()]
        rows = edit(os.path.basename(os.path.dirname(path)), rows)
        with open(path, "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")


class VerifierNegativeBase(unittest.TestCase):
    """公共:构造变异 fixture 并断言 verify_path 结果。"""

    def assert_fails(self, build_and_mutate, need_substr=""):
        with tempfile.TemporaryDirectory(prefix="rh-neg-") as tmp:
            build_and_mutate(tmp)
            ok, report = verify_goal.verify_path(tmp)
        self.assertFalse(ok, f"必须 FAIL:\n{report}")
        if need_substr:
            self.assertIn(need_substr, report)

    def assert_passes(self):
        with tempfile.TemporaryDirectory(prefix="rh-pos-") as tmp:
            verify_goal.build_good_set(tmp)
            ok, report = verify_goal.verify_path(tmp)
        self.assertTrue(ok, report)


class TestMainAgentNegatives(VerifierNegativeBase):
    """主代理复现的 6 个误通过负例(r2 后必须全部 FAIL)。"""

    def test_positive_control_passes(self):
        self.assert_passes()

    def test_wrong_udp_size(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_name, rows):
                for row in rows:
                    if row.get("metric") == "udp":
                        row["data"]["packet_size"] = 1
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "1200")

    def test_missing_four_protocol_concurrency(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)

            def edit(name, rows):
                if name in ("ws-mux", "wss-mux", "quic-mux", "kcp-mux"):
                    return [r for r in rows if r.get("metric") != "concurrent"]
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "G5")

    def test_fabricated_source_manifest(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            for p in glob.glob(os.path.join(tmp, "*", "build", "sources.sha256")):
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write("a" * 64 + "  missing-current-source.rs\n")
        self.assert_fails(fn, "missing-current-source.rs")

    def test_no_hold_proof(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_name, rows):
                for r in rows:
                    if r.get("metric") == "concurrent":
                        for field in ("hold_alive", "initial_verified",
                                      "hold_rounds", "alive_by_round"):
                            r.pop(field, None)
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "concurrent 记录缺")

    def test_raw_shape_mismatch(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            for p in glob.glob(os.path.join(tmp, "*", "raw", "*.json")):
                doc = json.loads(open(p, encoding="utf-8").read())
                doc["start"] = {"test_start": {"protocol": "UDP", "reverse": 1,
                                               "num_streams": 99}}
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)
        self.assert_fails(fn, "test_start.protocol")

    def test_insufficient_nofile(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            for p in glob.glob(os.path.join(tmp, "*", "endpoints", "*", "endpoint.json")):
                doc = json.loads(open(p, encoding="utf-8").read())
                doc["nofile"] = {"soft": "64", "hard": "64", "ok": False}
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)

            def edit(_name, rows):
                for row in rows:
                    if row.get("metric") == "meta":
                        row["drive_nofile_soft"] = "64"
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "fd 软限")


class TestR2HardeningNegatives(VerifierNegativeBase):
    """r2 新增的 fail-closed 面(同样必须 FAIL)。"""

    def test_duplicate_window_samples(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            d = os.path.join(tmp, "tcp-nomux")
            f = [x for x in os.listdir(d) if x.startswith("results-")][0]
            fp = os.path.join(d, f)
            rows = [json.loads(x) for x in open(fp, encoding="utf-8") if x.strip()]
            src = next(dict(r) for r in rows
                       if r.get("metric") == "throughput" and r.get("dir") == "upload"
                       and r.get("streams") == 1 and r.get("path") == "tunnel"
                       and r.get("sample") == 1)
            clones = []
            for sample in (2, 3):
                clone = dict(src)
                clone["sample"] = sample
                for field in ("raw_json", "raw_stderr", "raw_exit"):
                    dst_name = os.path.basename(src[field]).replace("-s1-", f"-s{sample}-")
                    with open(os.path.join(d, src[field]), "rb") as i, \
                            open(os.path.join(d, "raw", dst_name), "wb") as o:
                        o.write(i.read())
                    clone[field] = f"raw/{dst_name}"
                clones.append(clone)
            rows = [r for r in rows if not (r.get("metric") == "throughput"
                                            and r.get("dir") == "upload"
                                            and r.get("streams") == 1
                                            and r.get("path") == "tunnel"
                                            and r.get("sample") in (2, 3))]
            rows.extend(clones)
            with open(fp, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r) + "\n")
        self.assert_fails(fn, "重复")

    def test_nan_mbps_rejected(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            d = os.path.join(tmp, "tcp-nomux")
            f = [x for x in os.listdir(d) if x.startswith("results-")][0]
            text = open(os.path.join(d, f), encoding="utf-8").read()
            text = text.replace('"mbps": 3500', '"mbps": NaN', 1)
            if '"mbps": NaN' not in text:
                text = text.replace('"mbps":3500', '"mbps":NaN', 1)
            with open(os.path.join(d, f), "w", encoding="utf-8") as fh:
                fh.write(text)
        self.assert_fails(fn, "NaN")

    def test_cross_dir_mixed_builds(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            d = os.path.join(tmp, "kcp-mux")
            fake = "f" * 64
            bj = os.path.join(d, "build", "binaries.json")
            doc = json.load(open(bj, encoding="utf-8"))
            doc["binaries"]["rh-server"]["sha256"] = fake
            json.dump(doc, open(bj, "w", encoding="utf-8"))
            ep_path = os.path.join(d, "endpoints", "server", "endpoint.json")
            ep = json.load(open(ep_path, encoding="utf-8"))
            ep["identity"]["exe_sha256"] = fake
            json.dump(ep, open(ep_path, "w", encoding="utf-8"))
            os.remove(os.path.join(d, "build", "bin", "rh-server"))
        self.assert_fails(fn, "跨目录构建不一致")

    def test_fabricated_hold_numbers(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_name, rows):
                for r in rows:
                    if r.get("metric") == "concurrent":
                        r["alive_by_round"] = [1] * r.get("hold_rounds", 5)
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "矛盾")

    def test_raw_timestamp_skew(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            # 只改第一条 tunnel 单流 raw 的时间戳(标准对象,偏离 60s)
            d = os.path.join(tmp, "tcp-nomux")
            for p in sorted(glob.glob(os.path.join(d, "raw", "*tunnel-upload-1s.json"))):
                doc = json.loads(open(p, encoding="utf-8").read())
                doc["start"]["timestamp"]["timesecs"] += 60
                doc["start"]["timestamp"]["timemillisecs"] += 60_000
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)
                break
        self.assert_fails(fn, "timestamp")

    def test_raw_target_mismatch(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            d = os.path.join(tmp, "tcp-nomux")
            for p in sorted(glob.glob(os.path.join(d, "raw", "*tunnel-upload-1s.json"))):
                doc = json.loads(open(p, encoding="utf-8").read())
                doc["start"]["connected"][0] = {"remote_host": "10.0.0.2",
                                                "remote_port": 19101}
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)
                break
        self.assert_fails(fn, "remote_port")

    def test_undrained_udp(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_name, rows):
                for r in rows:
                    if r.get("metric") == "udp":
                        r["data"]["drain_seconds"] = 0
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(fn, "drain_seconds")

    def test_download_cpu_above_limit(self):
        """下行 CPU 拉满(全部下行单流样本窗口 ~80% 单核)→ 方向分别判定必须 FAIL。"""
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            d = os.path.join(tmp, "tcp-nomux")
            f = [x for x in os.listdir(d) if x.startswith("results-")][0]
            rows = [json.loads(x) for x in open(os.path.join(d, f), encoding="utf-8")
                    if x.strip()]
            wins = [(r["window_start_us"], r["window_end_us"]) for r in rows
                    if (r.get("metric") == "throughput" and r.get("dir") == "download"
                        and r.get("path") == "tunnel" and r.get("streams") == 1)]
            for role in ("server", "client"):
                p = os.path.join(d, "endpoints", role, "cpu.jsonl")
                lines = [json.loads(x) for x in open(p, encoding="utf-8") if x.strip()]
                header, samples = lines[0], lines[1:]
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write(json.dumps(header) + "\n")
                    for s in samples:
                        if any(w0 <= s["t_us"] <= w1 for w0, w1 in wins):
                            s["ticks"] = s["ticks"] * 4
                        fh.write(json.dumps(s) + "\n")
        self.assert_fails(fn, "download")


class TestCpuNoCrossDirMixing(VerifierNegativeBase):
    """CPU 必须按样本所在目录匹配:同配置两个目录,其一 CPU 证据坏了,
    不能被另一目录的好 CPU 稀释成 PASS。"""

    def test_second_dir_bad_cpu_fails(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            # 再造一个同配置目录(不同 run_id,模拟另一轮会话),CPU 采样抽稀
            verify_goal.build_fixture_dir(tmp, "tcp-nomux-b", "tcp", False, "splice",
                                          samples=3, tunnel_mbps=3500.0,
                                          label="tcp-nomux-b",
                                          # 独立一轮:时间线整体后移(r4 独立性
                                          # 要求同配置目录窗口不重叠)
                                          base_us=verify_goal.FIX_BASE_US + 600_000_000)
            p = os.path.join(tmp, "tcp-nomux-b", "endpoints", "client", "cpu.jsonl")
            lines = [json.loads(x) for x in open(p, encoding="utf-8") if x.strip()]
            with open(p, "w", encoding="utf-8") as fh:
                fh.write("\n".join(json.dumps(x) for x in lines[:6]) + "\n")
        self.assert_fails(fn, "覆盖率")


class TestR3ManifestCoverage(VerifierNegativeBase):
    """r3:清单必须覆盖枚举规则全集(遗漏/重复/符号链接/规则外条目/伪 repo)。"""

    def test_manifest_omits_actual_source(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            repo = os.path.join(tmp, "fx-repo")
            with open(os.path.join(repo, "crates", "actual-unlisted-source.rs"),
                      "w", encoding="utf-8") as fh:
                fh.write("// source changes are not covered by the submitted manifest\n")
        self.assert_fails(fn, "遗漏")

    def test_manifest_duplicate_entry(self):
        def fn(tmp):
            verify_goal.build_good_set(tmp)
            p = os.path.join(tmp, "tcp-nomux", "build", "sources.sha256")
            text = open(p, encoding="utf-8").read()
            with open(p, "w", encoding="utf-8") as fh:
                fh.write(text + text.splitlines()[0] + "\n")
        self.assert_fails(fn, "重复条目")

    def test_manifest_symlink_entry(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            repo = os.path.join(tmp, "fx-repo")
            link = os.path.join(repo, "crates", "synthetic", "link.rs")
            os.symlink(os.path.join(repo, "crates", "synthetic", "a.rs"), link)
            import hashlib
            for p in glob.glob(os.path.join(tmp, "*", "build", "sources.sha256")):
                text = open(p, encoding="utf-8").read()
                h = hashlib.sha256(open(link, "rb").read()).hexdigest()
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write(text + f"{h}  crates/synthetic/link.rs\n")
        self.assert_fails(fn, "符号链接")

    def test_manifest_extra_entry_outside_rules(self):
        def fn(tmp):
            import glob
            import hashlib
            verify_goal.build_good_set(tmp)
            with open(os.path.join(tmp, "fx-repo", "notes.txt"), "w",
                      encoding="utf-8") as fh:
                fh.write("not a build input\n")
            for p in glob.glob(os.path.join(tmp, "*", "build", "sources.sha256")):
                text = open(p, encoding="utf-8").read()
                h = hashlib.sha256(b"not a build input\n").hexdigest()
                with open(p, "w", encoding="utf-8") as fh:
                    fh.write(text + f"{h}  notes.txt\n")
        self.assert_fails(fn, "枚举规则之外")

    def test_fake_repo_escape_rejected(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            fake = os.path.join(tmp, "fake-repo")
            os.makedirs(fake, exist_ok=True)
            for p in glob.glob(os.path.join(tmp, "*", "build", "binaries.json")):
                doc = json.load(open(p, encoding="utf-8"))
                doc["repo_root"] = fake
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)
        self.assert_fails(fn, "不像 Cargo 工作区")


class TestR4Independence(VerifierNegativeBase):
    """r4 负例:独立性/克隆/混装(独立审查二轮实证的 3 个高严重度绕过)。"""

    def test_whole_dir_clone_bypasses_g7(self):
        """samples=1 + 每目录整树克隆 2 份 → 修前全门槛 PASS,必须 FAIL。"""
        import shutil

        def build(tmp):
            verify_goal.build_good_set(tmp, samples=1)
            for name in sorted(os.listdir(tmp)):
                full = os.path.join(tmp, name)
                if name == "fx-repo" or not os.path.isdir(full):
                    continue
                for k in (2, 3):
                    shutil.copytree(full, os.path.join(tmp, f"{name}-clone{k}"))
        self.assert_fails(build, "独立性被破坏")

    def test_udp_dup_same_window(self):
        """UDP 记录复制改 sample 号、时间窗原样 → 必须FAIL(修前计入 G4)。"""
        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(dirname, rows):
                if dirname != "tls-mux":
                    return rows
                out = []
                for r in rows:
                    out.append(r)
                    if r.get("metric") == "udp" and r.get("path") == "tunnel":
                        for new_sample in (4, 5):
                            c = json.loads(json.dumps(r))
                            c["sample"] = new_sample
                            out.append(c)
                return out
            mutate_results(tmp, edit)
        self.assert_fails(build, "时间窗重叠")

    def test_concurrent_dup_same_window(self):
        """concurrent 记录复制改 sample 号、时间窗原样 → 必须FAIL(修前计入 G5)。"""
        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(dirname, rows):
                if dirname != "tls-mux":
                    return rows
                out = []
                for r in rows:
                    out.append(r)
                    if r.get("metric") == "concurrent":
                        for new_sample in (4, 5):
                            c = json.loads(json.dumps(r))
                            c["sample"] = new_sample
                            out.append(c)
                return out
            mutate_results(tmp, edit)
        self.assert_fails(build, "时间窗重叠")

    def test_schema_run_id_mismatch(self):
        """schema.json run_id 与 meta.run_id 不一致(目录混装)→ FAIL。"""
        def build(tmp):
            verify_goal.build_good_set(tmp)
            p = os.path.join(tmp, "tcp-nomux", "schema.json")
            doc = json.loads(open(p, encoding="utf-8").read())
            doc["run_id"] = "fx-other-round"
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(doc, fh)
        self.assert_fails(build, "目录混装")

    def test_udp_missing_window_fields(self):
        """UDP 缺 started_us/ended_us(无法做独立性查重)→ FAIL。"""
        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_dirname, rows):
                for r in rows:
                    if r.get("metric") == "udp":
                        r["data"].pop("started_us", None)
                        r["data"].pop("ended_us", None)
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(build, "started_us")

    def test_in_dir_udp_partial_window_overlap(self):
        """UDP 同目录非等值部分重叠(sample2 前移一半)→ FAIL(锁 overlap 谓词)。"""
        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(dirname, rows):
                if dirname != "tcp-nomux":
                    return rows
                by_key = {}
                for r in rows:
                    if r.get("metric") == "udp" and r.get("path") == "tunnel":
                        by_key[r.get("sample")] = r
                if 1 in by_key and 2 in by_key:
                    w0 = by_key[1]["data"]["started_us"]
                    # sample=2 窗口整体改到 sample=1 窗口中段(部分重叠,非等值)
                    by_key[2]["data"]["started_us"] = w0 + 4_000_000
                    by_key[2]["data"]["ended_us"] = w0 + 12_000_000
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(build, "时间窗重叠")


class TestR5TimeAnchors(VerifierNegativeBase):
    """r5 负例:克隆+全局时间平移(独立审查三轮实验 D)与绝对时间锚。"""

    @staticmethod
    def _shift_dir_time(d, delta_us):
        """把一个证据目录的全部时间字段机械平移(窗口/raw 时间戳/cpu)。"""
        import glob
        for path in glob.glob(os.path.join(d, "results-*.jsonl")):
            rows = [json.loads(x) for x in open(path, encoding="utf-8") if x.strip()]
            for r in rows:
                data = r.get("data", r)
                for f in ("window_start_us", "window_end_us",
                          "started_us", "ended_us"):
                    if f in data and verify_goal.is_num(data[f]):
                        data[f] += delta_us
                    elif f in r and verify_goal.is_num(r[f]):
                        r[f] += delta_us
            with open(path, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r) + "\n")
        for path in glob.glob(os.path.join(d, "raw", "*.json")):
            doc = json.loads(open(path, encoding="utf-8").read())
            ts = ((doc.get("start") or {}).get("timestamp")) or {}
            if isinstance(ts.get("timemillisecs"), int):
                ts["timemillisecs"] += delta_us // 1000
            if isinstance(ts.get("timesecs"), int):
                ts["timesecs"] += delta_us // 1_000_000
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(doc, fh)
        for path in glob.glob(os.path.join(d, "endpoints", "*", "cpu.jsonl")):
            lines = [json.loads(x) for x in open(path, encoding="utf-8") if x.strip()]
            for s in lines:
                if verify_goal.is_num(s.get("t_us")):
                    s["t_us"] += delta_us
            with open(path, "w", encoding="utf-8") as fh:
                for s in lines:
                    fh.write(json.dumps(s) + "\n")

    def test_clone_shift_out_of_anchor(self):
        """克隆+整体平移 +24/48h(run_id 原样):绝对时间锚必须 FAIL。"""
        import shutil

        def build(tmp):
            verify_goal.build_good_set(tmp, samples=1)
            for name in sorted(os.listdir(tmp)):
                full = os.path.join(tmp, name)
                if name == "fx-repo" or not os.path.isdir(full):
                    continue
                for k, delta in ((2, 24 * 3600 * 1_000_000),
                                 (3, 48 * 3600 * 1_000_000)):
                    dst = os.path.join(tmp, f"{name}-clone{k}")
                    shutil.copytree(full, dst)
                    self._shift_dir_time(dst, delta)
        self.assert_fails(build, "绝对时间越界")

    def test_clone_shift_within_anchor_same_rid(self):
        """克隆+锚窗内平移(+20min,不与兄弟重叠):run_id 全局互异必须 FAIL。"""
        import shutil

        def build(tmp):
            verify_goal.build_good_set(tmp, samples=1)
            for name in sorted(os.listdir(tmp)):
                full = os.path.join(tmp, name)
                if name == "fx-repo" or not os.path.isdir(full):
                    continue
                dst = os.path.join(tmp, f"{name}-clone2")
                shutil.copytree(full, dst)
                self._shift_dir_time(dst, 20 * 60 * 1_000_000)
        self.assert_fails(build, "run_id 重复")

    def test_clone_shift_rewrite_rid_anchor_catches(self):
        """克隆+平移+改写全部 run_id(schema/results/endpoints 一致):
        仅 meta.started_at 未随之平移 → 锚必须 FAIL。"""
        import glob as _glob
        import shutil

        def build(tmp):
            verify_goal.build_good_set(tmp, samples=1)
            for name in sorted(os.listdir(tmp)):
                full = os.path.join(tmp, name)
                if name == "fx-repo" or not os.path.isdir(full):
                    continue
                for k, delta in ((2, 24 * 3600 * 1_000_000),
                                 (3, 48 * 3600 * 1_000_000)):
                    dst = os.path.join(tmp, f"{name}-clone{k}")
                    shutil.copytree(full, dst)
                    self._shift_dir_time(dst, delta)
                    new_rid = f"fx-{name}-c{k}"
                    sp = os.path.join(dst, "schema.json")
                    doc = json.loads(open(sp, encoding="utf-8").read())
                    doc["run_id"] = new_rid
                    with open(sp, "w", encoding="utf-8") as fh:
                        json.dump(doc, fh)
                    for path in _glob.glob(os.path.join(dst, "results-*.jsonl")):
                        rows = [json.loads(x) for x in open(path, encoding="utf-8")
                                if x.strip()]
                        for r in rows:
                            r["run_id"] = new_rid
                        with open(path, "w", encoding="utf-8") as fh:
                            for r in rows:
                                fh.write(json.dumps(r) + "\n")
                    for ep in ("server", "client"):
                        epath = os.path.join(dst, "endpoints", ep, "endpoint.json")
                        doc = json.loads(open(epath, encoding="utf-8").read())
                        doc["run_id"] = new_rid
                        with open(epath, "w", encoding="utf-8") as fh:
                            json.dump(doc, fh)
        self.assert_fails(build, "绝对时间越界")

    def test_started_at_missing(self):
        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(_d, rows):
                for r in rows:
                    if r.get("metric") == "meta":
                        r.pop("started_at", None)
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(build, "started_at")

    def test_started_at_redated_after_windows(self):
        import datetime as dt

        def build(tmp):
            verify_goal.build_good_set(tmp)

            def edit(dirname, rows):
                if dirname != "tcp-nomux":
                    return rows
                for r in rows:
                    if r.get("metric") == "meta":
                        r["started_at"] = dt.datetime.fromtimestamp(
                            (verify_goal.FIX_BASE_US + 99 * 3_600_000_000) / 1e6,
                            tz=dt.timezone.utc).isoformat()
                return rows
            mutate_results(tmp, edit)
        self.assert_fails(build, "绝对时间越界")


class TestR3RealIperfTimestamp(VerifierNegativeBase):
    """r3:timestamp 用真实 iperf 对象形态;强转伪数值必须 FAIL。"""

    def test_numeric_timestamp_rejected(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            p = sorted(glob.glob(os.path.join(
                tmp, "tcp-nomux", "raw", "*tunnel-upload-1s.json")))[0]
            doc = json.loads(open(p, encoding="utf-8").read())
            doc["start"]["timestamp"] = doc["start"]["timestamp"]["timesecs"]
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(doc, fh)
        self.assert_fails(fn, "非标准对象形态")

    def test_timesecs_only_still_passes(self):
        """只有 timesecs(秒精度,老版本 iperf 常见)是合法回退:必须 PASS。"""
        import glob
        with tempfile.TemporaryDirectory(prefix="rh-ts-only-") as tmp:
            verify_goal.build_good_set(tmp)
            for p in glob.glob(os.path.join(tmp, "*", "raw", "*.json")):
                doc = json.loads(open(p, encoding="utf-8").read())
                doc["start"]["timestamp"].pop("timemillisecs", None)
                with open(p, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh)
            ok, report = verify_goal.verify_path(tmp)
        self.assertTrue(ok, report)

    def test_timesecs_skew_rejected(self):
        def fn(tmp):
            import glob
            verify_goal.build_good_set(tmp)
            p = sorted(glob.glob(os.path.join(
                tmp, "tcp-nomux", "raw", "*tunnel-upload-1s.json")))[0]
            doc = json.loads(open(p, encoding="utf-8").read())
            doc["start"]["timestamp"]["timesecs"] += 60
            doc["start"]["timestamp"].pop("timemillisecs", None)
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(doc, fh)
        self.assert_fails(fn, "timestamp")


class TestR3LiveIperfCompat(unittest.TestCase):
    """真实 iperf3(本机 3.22)实采 raw JSON 必须原样通过 _check_raw_shape。"""

    def test_live_capture_shape_ok(self):
        if subprocess.run(["bash", "-c", "command -v iperf3"],
                          capture_output=True).returncode:
            self.skipTest("本机无 iperf3")
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        srv = subprocess.Popen(["iperf3", "-s", "-1", "-p", str(port)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        tmp = tempfile.mkdtemp(prefix="rh-live-iperf-")
        try:
            script = (f'cd {fx.BENCH} && export THP_TOOL=iperf3 IPERF3_BIN=iperf3 && source peer.sh; RAW_DIR={tmp}; PEER_DURATION=2; '
                      f'run_iperf 127.0.0.1 {port} 1 "" live322')
            r = subprocess.run(["bash", "-c", script], capture_output=True,
                               text=True, timeout=60)
            lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
            rec = json.loads(lines[-1])
            self.assertTrue(rec["valid"], rec)
            check_rec = {"streams": 1, "dir": "upload", "dst": "127.0.0.1",
                         "dst_port": port,
                         "window_start_us": rec["window_start_us"],
                         "window_end_us": rec["window_end_us"]}
            issues: list[str] = []
            ns = types_mod.SimpleNamespace(duration_s=2, _issue=issues.append)
            raw = json.loads(open(os.path.join(tmp, "live322.json")).read())
            verify_goal.EvidenceDir._check_raw_shape(ns, raw, check_rec, "live322")
            self.assertEqual(issues, [], f"真实 3.22 输出必须兼容:{issues}")
            # 对象形态与字段存在性
            self.assertIsInstance(raw["start"]["timestamp"], dict)
            self.assertIn("timesecs", raw["start"]["timestamp"])
        finally:
            srv.terminate()
            srv.wait(timeout=5)


class TestRealSourceBinding(unittest.TestCase):
    """sources.sha256 必须绑定真实源树:当前文件被改动后哈希不匹配也要 FAIL。"""

    def test_source_tamper_detected(self):
        with tempfile.TemporaryDirectory(prefix="rh-srcbind-") as tmp:
            verify_goal.build_good_set(tmp)
            repo = os.path.join(tmp, "fx-repo")
            victim = os.path.join(repo, "crates", "synthetic", "a.rs")
            with open(victim, "ab") as fh:
                fh.write(b"\n// tampered after build\n")
            ok, report = verify_goal.verify_path(tmp)
        self.assertFalse(ok, report)
        self.assertIn("源哈希不符", report)


if __name__ == "__main__":
    unittest.main()
