"""closure_c84 回归(标准库 unittest,本地运行,不碰远端)。

锁定的关键面:
  * 构建输入清单口径:migrations/rust-toolchain 收录、console/dist 排除、
    符号链接跳过;清单缺 migrations 直接拒(write_build_inputs fail closed);
  * 工作区确定性快照:同树同清单、内容变哈希变、产物/缓存不进快照;
  * 外层 deadline:超时清理自己进程组并抛 CommandTimeout;
  * 脱敏:secret/token/密码不出现在输出;
  * targeted 旋钮红线:SO_*BUF/sysctl 类拒收;
  * verify 作用域:baseline 形态目录不做 accept 全矩阵声明(缺协议自然 FAIL),
    verify 本身不因"E2E 未跑"把 baseline 误判崩(但跑过且失败仍 FAIL)。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import unittest.mock

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import fixtures as fx  # noqa: E402  (约定共享基建:把 benchmark/py 挂上 sys.path)
sys.path.insert(0, fx.BENCH)      # closure_c84.py 在 benchmark/ 下

import closure_c84 as cc  # noqa: E402


def _mk_tree(root: str) -> str:
    tree = os.path.join(root, "tree")
    for rel, content in (
        ("Cargo.toml", "[workspace]\nmembers = ['crates/x']\n"),
        ("Cargo.lock", "lock\n"),
        ("rust-toolchain.toml", '[toolchain]\nchannel = "1.98.1"\n'),
        ("crates/x/Cargo.toml", "x\n"),
        ("crates/x/src/lib.rs", "fn a(){}\n"),
        ("migrations/0001_init.sql", "-- 1\n"),
        ("migrations/0002_client_quotas.sql", "-- 2\n"),
        ("migrations/0003_certs.sql", "-- 3\n"),
        ("benchmark/peer.sh", "# peer\n"),
        ("benchmark/lib/common.sh", "# common\n"),
        ("benchmark/closure_c84.py", "# tool\n"),
        ("benchmark/loadgen/main.go", "// lg\n"),
        ("tests/e2e.sh", "# e2e\n"),
        ("console/dist/index.html", "<i>\n"),
        ("target/release/rh-server", "bin\n"),
    ):
        p = os.path.join(tree, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(content)
    return tree


class TestBuildInputs(unittest.TestCase):
    def test_migrations_and_toolchain_in_dist_out(self):
        with tempfile.TemporaryDirectory() as td:
            tree = _mk_tree(td)
            got = cc.enumerate_build_inputs(tree)
            for m in cc.MIN_MIGRATIONS:
                self.assertIn(m, got)
            self.assertIn("rust-toolchain.toml", got)
            self.assertIn("crates/x/src/lib.rs", got)
            self.assertIn("Cargo.lock", got)
            self.assertNotIn("benchmark/peer.sh", got)   # harness 不是编译输入
            self.assertFalse(any(x.startswith("console/") for x in got))

    def test_tree_hash_content_sensitive(self):
        with tempfile.TemporaryDirectory() as td:
            tree = _mk_tree(td)
            h1 = cc.tree_hash(tree)
            with open(os.path.join(tree, "crates/x/src/lib.rs"), "a") as fh:
                fh.write("// touched\n")
            h2 = cc.tree_hash(tree)
            self.assertNotEqual(h1, h2)

    def test_write_build_inputs_rejects_missing_migrations(self):
        with tempfile.TemporaryDirectory() as td:
            tree = _mk_tree(td)
            os.remove(os.path.join(tree, "migrations/0002_client_quotas.sql"))
            with self.assertRaises(SystemExit):
                cc.write_build_inputs(tree, os.path.join(td, "out"), {})

    def test_manifest_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            tree = _mk_tree(td)
            out = os.path.join(td, "out")
            doc = cc.write_build_inputs(tree, out, {"snapshot_rule": "unit"})
            self.assertFalse(doc["embedded_ui"])
            man = cc.read_manifest_lines(os.path.join(out, "build-inputs.sha256"))
            self.assertEqual(len(man), doc["file_count"])
            for rel, h in man.items():
                self.assertEqual(h, cc.sha256_file(os.path.join(tree, rel)))


class TestWorkspaceSnapshot(unittest.TestCase):
    def test_deterministic_and_excludes(self):
        with tempfile.TemporaryDirectory() as td:
            tree = _mk_tree(td)
            snap = os.path.join(td, "snap")
            files = cc.enumerate_workspace_files(tree)
            for rel in files:
                dst = os.path.join(snap, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with open(os.path.join(tree, rel), "rb") as s, open(dst, "wb") as d:
                    d.write(s.read())
            self.assertIn("benchmark/closure_c84.py", files)
            self.assertIn("tests/e2e.sh", files)
            self.assertTrue(any(f.startswith("migrations/") for f in files))
            self.assertFalse(any(f.startswith(("target/", "console/")) for f in files))
            self.assertFalse(any("__pycache__" in f for f in files))
            # 同一快照规则两次枚举结果一致(顺序稳定)
            self.assertEqual(files, cc.enumerate_workspace_files(tree))
            # 快照树重算 build-inputs 与原树一致(内容决定,路径无关)
            self.assertEqual(cc.tree_hash(tree), cc.tree_hash(snap))


class TestDeadline(unittest.TestCase):
    def test_timeout_kills_own_process_group(self):
        marker = f"rhc84t{os.getpid()}"      # 唯一标记,避免匹配其他运行残留
        sleeper = f"import time; time.sleep(30)  # {marker}"
        t0 = time.time()
        with self.assertRaises(cc.CommandTimeout):
            # 两个 sleeper 子进程;必须整组被清理(标记进真实命令行才可 pgrep)
            cc.run_local(["bash", "-c",
                          f"python3 -c {sleeper!r} & python3 -c {sleeper!r}"],
                         deadline_s=2)
        self.assertLess(time.time() - t0, 15)
        leftover = subprocess.run(["pgrep", "-f", marker], capture_output=True)
        self.assertEqual(leftover.returncode, 1,
                         f"超时后仍有残留进程: {leftover.stdout!r}")

    def test_normal_return(self):
        rc, out, _ = cc.run_local(["echo", "hi"], deadline_s=10)
        self.assertEqual(rc, 0)
        self.assertIn(b"hi", out)


class TestRedact(unittest.TestCase):
    def test_secrets_never_leak(self):
        s = ('token=eyJhbGciOi.J9.abc "secret":"S3cr3t" secret = "toml-secret"\n'
             "Authorization: Bearer aaaa.bbbb.cccc password peer-Pass-1")
        r = cc.redact(s)
        for leak in ("S3cr3t", "toml-secret", "aaaa.bbbb.cccc", "peer-Pass-1"):
            self.assertNotIn(leak, r)
        self.assertIn("<redacted>", r)


class TestTargetedGuard(unittest.TestCase):
    def test_forbidden_knobs_rejected(self):
        for knob in ("SO_RCVBUF", "SO_SNDBUF", "net.core.rmem_max", "sysctl_wmagic"):
            self.assertRegex(knob, cc.FORBIDDEN_KNOB_PAT)
        self.assertNotRegex("RH_NOTSENT_LOWAT", cc.FORBIDDEN_KNOB_PAT)
        self.assertNotRegex("RH_QUIC_CC", cc.FORBIDDEN_KNOB_PAT)


class TestVerifyScope(unittest.TestCase):
    def test_baseline_shaped_dir_gates_fail_not_crash(self):
        """baseline 只有 4 配置:verify 必须非零(缺协议等门槛 FAIL),
        且不因 'E2E 未跑' 之外的原因崩溃;note 而非冒充。"""
        with tempfile.TemporaryDirectory() as td:
            out = cc._fx_lab(td)
            idx = cc.index_load(out)
            idx["mode"] = "baseline"
            # baseline 只有 targeted 4 配置
            idx["rounds"] = [r for r in idx["rounds"]
                             if (r["proto"], r["mux"]) in cc.BASELINE_TARGETED]
            for d in ("ws-mux", "wss-mux"):
                import shutil
                shutil.rmtree(os.path.join(out, "evidence", d), ignore_errors=True)
            idx["e2e"] = {}
            cc.index_save(out, idx)
            rc = cc.cmd_verify(out)
            self.assertNotEqual(rc, 0, "baseline 缺协议必须门槛 FAIL(非零)")

    def test_failed_e2e_in_any_mode_fails(self):
        with tempfile.TemporaryDirectory() as td:
            out = cc._fx_lab(td)
            idx = cc.index_load(out)
            idx["mode"] = "targeted"
            idx["e2e"] = {"e2e": {"exit": 2, "pass_fail": [[5, 1]]}}
            cc.index_save(out, idx)
            self.assertNotEqual(cc.cmd_verify(out), 0)


class FakeTm:
    """最小远端假体:按 name 前缀回放输出,记录全部命令(路径构造测试用)。
    download 把预置内容写到本地目标路径(模拟原件回收)。"""

    def __init__(self, out_by_prefix=None, files_by_remote=None, dirs_by_remote=None,
                 raise_on=()):
        self.cmds = []                     # (name, cmd)
        self.out_by_prefix = out_by_prefix or {}
        self.files_by_remote = files_by_remote or {}
        self.dirs_by_remote = dirs_by_remote or {}   # remote_dir → {name: content}
        self.raise_on = tuple(raise_on)    # name 前缀 → exec 抛 TmError(通道失败)
        self.progress = cc.Progress(tempfile.mkdtemp(prefix="fake-prog-"))

    def exec(self, asset_id, cmd, deadline=None, name=None, check=True):
        self.cmds.append((name or "exec", cmd))
        name = name or "exec"
        if name.startswith(self.raise_on):
            raise cc.TmError(f"远端命令非零(-1): (fake 通道超时) {cmd[:80]}")
        for prefix, out in self.out_by_prefix.items():
            if name.startswith(prefix):
                return 0, out
        return 0, ""

    def exec_sh(self, asset_id, cmd, **kw):
        return self.exec(asset_id, cmd, **kw)[1]

    def upload(self, asset_id, local, remote, deadline=None, name=None):
        self.cmds.append((name or "upload", f"{local}->{remote}"))

    def download(self, asset_id, remote, local, deadline=None, name=None):
        self.cmds.append((name or "download", f"{remote}->{local}"))
        files = self.dirs_by_remote.get(remote)
        if files is not None:              # 目录下载:逐文件落到 local 下
            os.makedirs(local, exist_ok=True)
            for fname, content in files.items():
                fp = os.path.join(local, fname)
                os.makedirs(os.path.dirname(fp), exist_ok=True)
                with open(fp, "w", encoding="utf-8") as fh:
                    fh.write(content)
            return
        content = self.files_by_remote.get(remote)
        if content is not None:
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, "w", encoding="utf-8") as fh:
                fh.write(content)


class TestPreflightRegex(unittest.TestCase):
    """★precheck:端口集合必须拼成合法 | 交替(空格拼接永不匹配→冲突漏检)。"""

    ASSET = {"id": "fx", "name": "fx"}

    def _tm(self, out):
        return FakeTm(out_by_prefix={"preflight": out})

    def test_regex_uses_pipe_alternation(self):
        tm = self._tm("")
        cc.preflight_ports(tm, self.ASSET, [17100, 18080, 20001], "t")
        cmd = next(c for n, c in tm.cmds if n.startswith("preflight"))
        self.assertIn("(17100|18080|20001)", cmd)
        self.assertNotIn("17100 18080", cmd)

    def test_single_busy_port_rejected(self):
        tm = self._tm("17100\n")
        with self.assertRaises(SystemExit):
            cc.preflight_ports(tm, self.ASSET, [17100, 18080], "t")

    def test_free_ports_pass(self):
        cc.preflight_ports(self._tm(""), self.ASSET, [17100, 18080], "t")

    def test_no_ss_fails_closed(self):
        with self.assertRaises(SystemExit):
            cc.preflight_ports(self._tm("NO_SS\n"), self.ASSET, [17100], "t")


class TestRemoteNohupPaths(unittest.TestCase):
    """★precheck+attempt1:pid/exit/log 绝对路径;launch 通道超时以 pid 文件
    为准恢复(termark 60s 硬超时实测,远端可能实际已拉起)。"""

    ASSET = {"id": "fx", "name": "fx"}

    def test_launch_uses_absolute_paths_and_own_pid(self):
        tm = FakeTm(out_by_prefix={
            "launch": "LAUNCHED",
            "recover-pid": "4242",
            "ident": "exe=/usr/bin/bash\n12345\npgid=4242",
            "poll": "0",
        })
        rc = cc.remote_nohup(tm, self.ASSET, "echo x", "/tmp/rh-closure-c84-accept-x",
                             "probe", deadline=5, poll_interval=0.05)
        self.assertEqual(rc, 0)
        launch = next(c for n, c in tm.cmds if n == "launch-probe")
        self.assertIn("/tmp/rh-closure-c84-accept-x/probe.pid", launch)
        self.assertIn("/tmp/rh-closure-c84-accept-x/probe.exit", launch)
        self.assertIn("/tmp/rh-closure-c84-accept-x/probe.log", launch)
        self.assertIn("echo $$ >", launch)          # 后台包装自记会话组长 PID
        self.assertIn("disown", launch)             # 前台立即返回,不留 job
        self.assertIn("rm -f", launch)              # 清旧 pid,防恢复读到旧值
        poll = next(c for n, c in tm.cmds if n == "poll-probe")
        self.assertIn("/tmp/rh-closure-c84-accept-x/probe.exit", poll)

    def test_launch_channel_timeout_recovers_via_pid_file(self):
        """★attempt1 事故回归:launch 通道超时(-1)不判死,pid 文件出现即继续。"""
        tm = FakeTm(out_by_prefix={
            "recover-pid": "555",
            "ident": "exe=/usr/bin/bash\n9\npgid=555",
            "poll": "0",
        }, raise_on=("launch-",))
        rc = cc.remote_nohup(tm, self.ASSET, "cargo test --workspace --locked",
                             "/tmp/rh-closure-c84-baseline-fx", "test-workspace",
                             deadline=5, poll_interval=0.05)
        self.assertEqual(rc, 0)
        self.assertTrue(any(n == "recover-pid-test-workspace" for n, _ in tm.cmds))
        self.assertTrue(any(n == "poll-test-workspace" for n, _ in tm.cmds))

    def test_launch_failure_without_pid_file_raises(self):
        tm = FakeTm(out_by_prefix={"recover-pid": "NONE"},
                    raise_on=("launch-",))
        with unittest.mock.patch.object(cc.time, "sleep", lambda s: None):
            with self.assertRaises(cc.TmError):
                cc.remote_nohup(tm, self.ASSET, "x", "/tmp/rh-fx", "job",
                                deadline=5, poll_interval=0.05)

    def test_governed_kill_group_and_recheck(self):
        tm = FakeTm(out_by_prefix={
            "govcheck": "/usr/bin/bash|12345|4242",
        })
        ok = cc.governed_kill(tm, self.ASSET, 4242,
                              "exe=/usr/bin/bash\n12345\npgid=4242", "k",
                              expect_names=("bash",))
        self.assertTrue(ok)
        killcmd = next(c for n, c in tm.cmds if n == "govkill-k")
        self.assertIn("kill -TERM -4242", killcmd)          # PGID==PID → 整组
        self.assertIn("kill -KILL -4242", killcmd)
        # KILL 前重验同 starttime(防 PID 复用)
        self.assertIn("awk '{print $22}' /proc/4242/stat", killcmd)
        self.assertIn("\"12345\"", killcmd)

    def test_governed_kill_pid_not_group_when_not_leader(self):
        tm = FakeTm(out_by_prefix={"govcheck": "/usr/bin/python3.10|77|1"})
        ok = cc.governed_kill(tm, self.ASSET, 77, "exe=/usr/bin/python3.10\n77",
                              "k", expect_names=("python3",))
        self.assertTrue(ok)
        killcmd = next(c for n, c in tm.cmds if n == "govkill-k")
        self.assertIn("kill -TERM 77 ", killcmd)
        self.assertNotIn("kill -TERM -77", killcmd)


class TestFdPreflight(unittest.TestCase):
    """★linux-baseline attempt1 实测:默认软限 1024 → cargo test EMFILE
    exit 101。runner 必须 fail-closed 环境错误,不得折算产品/测试失败。"""

    ASSET = {"id": "fx", "name": "fx"}

    def _tm(self, out):
        return FakeTm(out_by_prefix={"fd-preflight": out})

    def test_fd_ok(self):
        info = cc.remote_fd_preflight(self._tm(
            "nofile_soft=65535 nofile_hard=1048576\n"), self.ASSET)
        self.assertEqual(info["soft"], "65535")

    def test_fd_insufficient_fails_closed(self):
        with self.assertRaises(SystemExit):
            cc.remote_fd_preflight(self._tm(
                "nofile_soft=1024 nofile_hard=1048576\n"), self.ASSET)

    def test_fd_unlimited_ok(self):
        info = cc.remote_fd_preflight(self._tm(
            "nofile_soft=unlimited nofile_hard=unlimited\n"), self.ASSET)
        self.assertEqual(info["soft"], "unlimited")

    def test_fd_unparseable_fails_closed(self):
        with self.assertRaises(SystemExit):
            cc.remote_fd_preflight(self._tm("garbage\n"), self.ASSET)


class TestE2EAndPullPaths(unittest.TestCase):
    """★precheck:E2E 相对 BIN 目录;manifest 比较先于拷贝。"""

    def _fake_for_e2e(self):
        out = tempfile.mkdtemp(prefix="fake-e2e-")
        os.makedirs(out, exist_ok=True)
        idx = cc.index_load(out)
        idx["e2e"] = {}
        tm = FakeTm(out_by_prefix={
            "fd-preflight": "nofile_soft=65535 nofile_hard=1048576\n",
            "recover-pid": "111",
            "ident": "exe=/usr/bin/bash\n9\npgid=111",
            "poll": "0",
        })
        # 每个 e2e 下载写本地原件(模拟远端回收)
        return tm, out, idx

    def test_e2e_uses_relative_bin_and_downloads_originals(self):
        import types
        tm, out, idx = self._fake_for_e2e()
        rr = "/tmp/rh-closure-c84-accept-fx"
        tm.files_by_remote = {
            f"{rr}/e2e-{k}.{s}": ("PASS: a\n\n== 结果: PASS=3 FAIL=0 ==\n"
                                  if s == "log" else "0\n")
            for k, _ in cc.E2E_SUITES for s in ("log", "exit")
        }
        cc.run_e2e_all(tm, cc.Progress(out), out, rr, idx)
        launches = [c for n, c in tm.cmds if n.startswith("launch-e2e-")]
        self.assertEqual(len(launches), 4)
        for c in launches:
            self.assertRegex(c, r"bash tests/e2e[a-z-]*\.sh target/release")
            self.assertNotRegex(c, r"sh /tmp/\S*target/release")  # 不得拼绝对 BIN
        for k, _ in cc.E2E_SUITES:
            self.assertTrue(os.path.isfile(os.path.join(out, "logs", f"e2e-{k}.exit")))
            self.assertTrue(os.path.isfile(os.path.join(out, "logs", f"e2e-{k}.log")))
            self.assertTrue(os.path.isfile(os.path.join(out, "logs",
                                                        f"e2e-{k}.log.redacted")))
        for k, e in idx["e2e"].items():
            self.assertEqual(e["exit"], 0)
            self.assertEqual(e["pass_fail"], [[3, 0]])

    def test_pull_build_artifacts_compares_before_copy(self):
        out = tempfile.mkdtemp(prefix="fake-pull-")
        bld = os.path.join(out, "build")
        os.makedirs(bld, exist_ok=True)
        local_manifest = "aa…  Cargo.toml\nbb  migrations/0002_client_quotas.sql\n"
        open(os.path.join(bld, "build-inputs.sha256"), "w").write(local_manifest)
        rr = "/tmp/rh-closure-c84-accept-fx2"
        tm = FakeTm(dirs_by_remote={
            f"{rr}/build": {"build-inputs.sha256": "DIFFERENT  Cargo.toml\n"},
        })
        prog = cc.Progress(out)
        with self.assertRaises(SystemExit):   # 远端清单不一致 → 拒绝
            cc.pull_build_artifacts(tm, prog, out, rr)
        # 本地初始清单未被覆盖
        self.assertEqual(open(os.path.join(bld, "build-inputs.sha256")).read(),
                         local_manifest)

    def test_pull_build_artifacts_copies_after_equal(self):
        out = tempfile.mkdtemp(prefix="fake-pull2-")
        bld = os.path.join(out, "build")
        os.makedirs(bld, exist_ok=True)
        local_manifest = "aa  Cargo.toml\nbb  migrations/0002_client_quotas.sql\n"
        open(os.path.join(bld, "build-inputs.sha256"), "w").write(local_manifest)
        rr = "/tmp/rh-closure-c84-accept-fx3"
        tm = FakeTm(dirs_by_remote={
            f"{rr}/build": {"build-inputs.sha256": local_manifest,
                            "binaries.json": '{"binaries":{}}'},
        })
        cc.pull_build_artifacts(tm, cc.Progress(out), out, rr)
        self.assertEqual(open(os.path.join(bld, "binaries.json")).read(),
                         '{"binaries":{}}')


class TestReuseBuildAndAffinity(unittest.TestCase):
    """★c84 诊断:--reuse-build 只绑定已核验 accept 构建;affinity 只作用于
    自己记录的 server PID。"""

    ASSET = {"id": "fx", "name": "fx"}

    def _prev(self, td, mode="accept", tree_hash_value="deadbeef" * 8):
        prev = os.path.join(td, "prev")
        os.makedirs(prev, exist_ok=True)
        idx = {"schema": cc.SCHEMA, "version": 1, "mode": mode,
               "tree_hash": tree_hash_value,
               "build": {"binaries": {"rh-server": "a" * 64, "rh-client": "b" * 64}},
               "remote_root": "/tmp/rh-closure-c84-accept-fx"}
        with open(os.path.join(prev, "run-index.json"), "w") as fh:
            json.dump(idx, fh)
        return prev

    def test_reuse_rejects_missing_index(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(SystemExit):
                cc._reuse_build(FakeTm(), cc.Progress(td), td, td + "/nope")

    def test_reuse_rejects_non_accept(self):
        with tempfile.TemporaryDirectory() as td:
            prev = self._prev(td, mode="targeted")
            with self.assertRaises(SystemExit):
                cc._reuse_build(FakeTm(), cc.Progress(td), td, prev)

    def test_reuse_rejects_stale_hash_vs_workspace(self):
        # prev 声明的 tree_hash ≠ 当前工作区实际构建输入 → 实现已变,必须重编
        with tempfile.TemporaryDirectory() as td:
            prev = self._prev(td)
            os.makedirs(os.path.join(prev, "source-snapshot"), exist_ok=True)
            with self.assertRaises(SystemExit):
                cc._reuse_build(FakeTm(), cc.Progress(td), td, prev)

    def test_affinity_applies_to_recorded_server_pid_only(self):
        tm = FakeTm(out_by_prefix={"affinity": "pid 1's current affinity list: 0-3\n"})
        prog = cc.Progress(tempfile.mkdtemp())
        cfg = cc.RoundCfg("tcp", 1, "fx", "fx", affinity="0-3")
        r = cc.RoundRunner(tm, prog, tempfile.mkdtemp(), "/tmp/rh-fx", cfg)
        r.roles = [(self.ASSET, "server/server.pid", 4242, "server.pid"),
                   (self.ASSET, "server/cpu.jsonl.pid", 4243, "cpu.jsonl.pid")]
        r.apply_affinity()
        name, cmd = next((n, c) for n, c in tm.cmds if n.startswith("affinity-"))
        self.assertIn("taskset -pc 0-3 4242", cmd)     # 只针对 server.pid
        self.assertNotIn("4243", cmd)

    def test_spec_rejects_bad_affinity(self):
        import json as _json
        with tempfile.TemporaryDirectory() as td:
            spec = os.path.join(td, "spec.json")
            open(spec, "w").write(_json.dumps(
                {"name": "x", "proto": "tcp", "mux": 1, "knobs": {},
                 "server_affinity": "rm -rf"}))
            out = os.path.join(td, "out")
            import types
            args = types.SimpleNamespace(out_dir=out, spec=spec,
                                         reuse_build=None, harness_patch=None)
            with self.assertRaises(SystemExit):
                cc.cmd_targeted(args)


class TestSelfTestCLI(unittest.TestCase):
    def test_self_test_exit_zero(self):
        """--self-test 全绿(脱敏/枚举 + 负例 + 正例,详见 self-test 输出)。"""
        p = subprocess.run([sys.executable, os.path.join(fx.BENCH, "closure_c84.py"),
                            "--self-test"], capture_output=True, text=True, timeout=600)
        self.assertEqual(p.returncode, 0, p.stdout[-2000:] + p.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
