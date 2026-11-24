#!/usr/bin/env python3
"""closure_c84 —— c84f631 性能收口轮三机真实验收编排器。

一台本地编排机 + 三台远端机(termark SSH 通道),复用既有 peer.sh 三角色
harness 与 py/verify_goal.py 验收器,不发明新的宽松口径:

    test1(172.16.1.6)   构建 + rh-server
    test2(172.16.1.17)  rh-client + 目标服务(iperf/tcp/udp/http)
    test3(172.16.1.18)  驱动(iperf3/loadgen,不与被测抢 CPU)

子命令(全部由工作流脚本统一执行;本工具不自跑全矩阵):
  baseline --out-dir DIR   git archive c84f631 精确源树 → 同步 test1 独立目录
                           → Linux cargo test --workspace + release 构建
                           → targeted TCP nomux / TLS mux / KCP mux / QUIC mux
                             单流双向 10s×3 配对 + 双端同步 CPU(16 流随 drive
                             默认 spec 一并留作诊断)。只声明 targeted,不冒充
                             全矩阵。
  accept   --out-dir DIR   当前工作区静止校验 → 确定性源快照 → 构建输入哈希与
                           远端逐项匹配 → Linux workspace 测试 + release 构建
                           → 四套 E2E(e2e/resilience/transports/relay-modes)
                           逐套全跑,真实 FAIL 非零即停 → 六协议 mux×{1,4,16}流
                           ×双向 ≥3 配对 + tcp nomux 对照(G1/G5)、逐协议
                           1200B UDP ≥3 排空去重、4096 应用层往返+保持存活。
                           全程同一份源/二进制。
  targeted --out-dir DIR   仅上一份候选诊断的有限 A/B(用户态可逆环境变量),
                           不动 sysctl/NIC/SO_*BUF;新源码静止构建;完整 raw +
                           时间/哈希;不以 targeted 冒充 full。
  verify   --evidence DIR  read-only:Linux test/build 真实 exit 与计数、四 E2E
                           逐套 exit/PASS/FAIL、build-inputs(含 migrations 等
                           编译输入)逐项重算、bin 与所有端点一致、样本/raw/CPU/
                           config/工具/nofile 链;最后调用 py/verify_goal.py
                           (RH_VERIFY_REPO=快照目录)按固定门槛判定,不手填 PASS。
  --self-test              本地对抗自检(不碰远端):busy 样本/缺协议/不足 3 对/
                           错方向/仅握手/旧哈希/失败 E2E/缺 migration 负例。

约定(与本轮红线一致):
  * 每条本地/远端命令都有外层 deadline;超时清理自己创建的进程组
    (start_new_session + SIGKILL 进程组),不做全局扫杀。
  * 远端只终止 runner 自己记录并经 /proc/PID/exe + starttime 校验的 PID;
    测试前预检端口,冲突报错,绝不杀未知进程。
  * 密钥(client secret/token/管理密码)不打印到终端/结果:落 0600 本地
    .secrets/ 与远端 0600 文件,经文件传输;回收日志时原日志保留本地,
    汇总/报告一律走脱敏副本。
  * 远端独立目录 /tmp/rh-closure-c84-<mode>-<stamp>,旧目录一律保留。
  * 源不可变:同步后远端重算 build-inputs 清单,与本地逐字节一致才允许构建;
    baseline 与 accept 固定一致的前端嵌入策略(快照不含 console/dist,
    embedded_ui=false,不偷用 test1 旧 dist)。
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
PY_DIR = os.path.join(BENCH_DIR, "py")
REPO_ROOT = os.path.dirname(BENCH_DIR)
sys.path.insert(0, PY_DIR)
import source_inventory  # noqa: E402  同目录共享模块(枚举规则单一事实来源)

# ---------------------------------------------------------------- 常量

BASELINE_COMMIT = "c84f63174cd2ebafd994c4fafacd5beba4a0240b"
SCHEMA = "closure-c84-run"
SCHEMA_VERSION = 1

PROTOS_ALL = ("tcp", "tls", "ws", "wss", "quic", "kcp")
# baseline targeted 覆盖(TCP nomux + TLS/KCP/QUIC mux 各单流双向)
BASELINE_TARGETED = (("tcp", 0), ("tls", 1), ("kcp", 1), ("quic", 1))
# accept 全矩阵:tcp nomux 对照 + 六协议 mux
ACCEPT_MATRIX = (("tcp", 0),) + tuple((p, 1) for p in PROTOS_ALL)

# 三机资产(termark exec 用 asset id,不假设名称可用)
ASSETS = {
    "test1": {"id": "cizwDkKnGqY47eBL", "ip": "172.16.1.6"},   # 构建 + server
    "test2": {"id": "Du9x3tHGGDhcZJNx", "ip": "172.16.1.17"},  # client + 目标
    "test3": {"id": "tN76t9aSo3zaCwJJ", "ip": "172.16.1.18"},  # 驱动
}

# 与 peer.sh 端口规划一致(环境变量可在 peer 侧覆盖,这里做预检)
PORT_PLAN = {
    "ctl": 17100, "api": 18080, "proxy_base": 20001,
    "t_iperf": 19101, "t_tcp": 19102, "t_udp": 19103, "t_http": 19104,
}
SERVER_LISTEN_PORTS = [17100, 18080, 20001, 20002, 20003, 20004, 19080]
CLIENT_LISTEN_PORTS = [19101, 19102, 19103, 19104]

# 每条命令的外层 deadline(秒)—— async timeout 不是死锁兜底,到点即清理
DEADLINES = {
    "exec_small": 120, "upload_tar": 600, "download": 600,
    "role_start": 600, "drive_poll": 90,
    "cargo_test": 5400, "cargo_build": 5400, "e2e_suite": 3600,
    "drive_round": 5400, "verify_goal": 1800,
}

# 四套 E2E(accept 必跑,真实 FAIL 非零即停)
E2E_SUITES = (
    ("e2e", "tests/e2e.sh"),
    ("resilience", "tests/e2e-resilience.sh"),
    ("transports", "tests/e2e-transports.sh"),
    ("relay-modes", "tests/e2e-relay-modes.sh"),
)

# 确定性工作区快照的收录规则(cargo 构建输入 + 测试/harness 脚本;
# console/ 与 console/dist 一律不收 → embedded_ui=false 固定策略)
WORKSPACE_TOP_FILES = ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rust-toolchain")
WORKSPACE_DIRS = ("crates", "migrations", "tests", "vendor")
WORKSPACE_BENCH_FILES = ("peer.sh", os.path.join("lib", "common.sh"), "closure_c84.py")
WORKSPACE_BENCH_DIRS = ("py",)
WORKSPACE_EXCLUDE_NAMES = {"target", "node_modules", "dist", "__pycache__", ".bin", "runs", ".git"}

MIN_MIGRATIONS = ("migrations/0001_init.sql", "migrations/0002_client_quotas.sql",
                  "migrations/0003_certs.sql")

TARGETED_DEFAULT_SPEC = os.path.join(REPO_ROOT, "benchmark", "runs",
                                     "performance-closure-c84", "lab", "targeted-spec.json")

HEX64 = re.compile(r"^[0-9a-f]{64}$")


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def die(msg: str) -> "None":
    print(f"[closure][fail] {msg}", file=sys.stderr)
    raise SystemExit(1)


# ---------------------------------------------------------------- 进度事件

class Progress:
    """中文阶段短语打印 + 本地事件文件追加(防长时间只看 executing)。"""

    def __init__(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        self.path = os.path.join(out_dir, "events.jsonl")

    def event(self, phase: str, detail: str = "", **kw) -> None:
        rec = {"ts": now_iso(), "phase": phase}
        if detail:
            rec["detail"] = detail
        rec.update(kw)
        print(f"[closure] {phase}" + (f":{detail}" if detail else ""), flush=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- 本地命令(deadline + 进程组清理)

class CommandTimeout(Exception):
    pass


def run_local(cmd, deadline_s: float, cwd=None, env=None):
    """带外层 deadline 的本地命令;超时 SIGKILL 自己创建的进程组后抛异常。"""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            cwd=cwd, env=env, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=deadline_s)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        out, err = proc.communicate()
        raise CommandTimeout(
            f"本地命令超时({deadline_s}s)已清理进程组: {shlex.join(map(str, cmd))[:200]}")
    return proc.returncode, out, err


# ---------------------------------------------------------------- 脱敏

_REDACT_PAIRS = (
    (re.compile(r'"secret"\s*:\s*"[^"]*"'), '"secret":"<redacted>"'),
    (re.compile(r'(?<![A-Za-z0-9_])secret\s*=\s*"[^"]*"'), 'secret = "<redacted>"'),
    (re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{8,}"), "Bearer <redacted>"),
    (re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"),
     "<jwt-redacted>"),
    (re.compile(r"peer-Pass-\S+"), "<bench-admin-pass>"),
)


def redact(text: str) -> str:
    for pat, rep in _REDACT_PAIRS:
        text = pat.sub(rep, text)
    return text


def write_log(path: str, text: str, secret: bool = False) -> None:
    """原日志保留本地;secret 日志 0600,并额外产出脱敏副本(.redacted)。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = 0o600 if secret else 0o644
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
    if secret:
        with open(path + ".redacted", "w", encoding="utf-8") as fh:
            fh.write(redact(text))


# ---------------------------------------------------------------- termark 通道

class TmError(Exception):
    pass


class Tm:
    """termark exec/upload/download 封装:每条命令带 deadline + 进程组清理,
    全部输出落本地日志(secret 项 0600 + 脱敏副本)。"""

    def __init__(self, progress: Progress, logs_dir: str):
        self.progress = progress
        self.logs_dir = logs_dir
        os.makedirs(logs_dir, exist_ok=True)
        self._seq = 0

    def _name(self, name):
        self._seq += 1
        return f"{self._seq:04d}-{name or 'call'}"

    def _call(self, args, deadline, name):
        log = os.path.join(self.logs_dir, self._name(name) + ".log")
        try:
            rc, out, err = run_local(args, deadline)
        except CommandTimeout as exc:
            write_log(log, f"$ {shlex.join(map(str, args))}\n[TIMEOUT] {exc}\n", secret=False)
            raise
        text = out.decode("utf-8", "replace") + (("\n[stderr]\n" + err.decode("utf-8", "replace")) if err else "")
        write_log(log, f"$ {shlex.join(map(str, args))}\n{text}", secret=False)
        return rc, text

    @staticmethod
    def _parse(text: str) -> dict:
        """termark exec 输出 JSON(实测 pretty-printed 多行;远端命令失败时
        termark 自身退出码=1 但 JSON 仍在 stdout)。"""
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except ValueError:
            pass
        i, j = text.find("{"), text.rfind("}")
        if i >= 0 and j > i:
            try:
                obj = json.loads(text[i:j + 1])
                if isinstance(obj, dict):
                    return obj
            except ValueError:
                pass
        return {}

    def exec(self, asset_id: str, cmd: str, deadline: int = None,
             name: str = None, check: bool = True):
        """远端命令;返回 (exitCode, output)。check=True 时非零抛异常;
        check=False 时远端失败也要把状态带回来(poll/诊断依赖)。"""
        deadline = deadline or DEADLINES["exec_small"]
        rc, text = self._call(["termark", "exec", asset_id, cmd], deadline, name or "exec")
        obj = self._parse(text)
        if "exitCode" not in obj:
            # termark 自身失败(通道/认证):不可继续
            raise TmError(f"termark exec 未返回 JSON(exit={rc}): "
                          f"{shlex.join(['termark', 'exec', asset_id])[:120]}\n{text[-500:]}")
        rcode = int(obj.get("exitCode", -1))
        out = str(obj.get("output", ""))
        if check and rcode != 0:
            raise TmError(f"远端命令非零({rcode}): {cmd[:200]}\n{out[-800:]}")
        return rcode, out

    def exec_sh(self, asset_id: str, cmd: str, **kw):
        return self.exec(asset_id, cmd, **kw)[1]

    def upload(self, asset_id: str, local: str, remote: str,
               deadline: int = None, name: str = None) -> None:
        deadline = deadline or DEADLINES["upload_tar"]
        rc, text = self._call(["termark", "upload", asset_id, local, remote],
                              deadline, name or "upload")
        if rc != 0:
            raise TmError(f"termark upload 失败(exit={rc}): {local} → {remote}\n{text[-500:]}")

    def download(self, asset_id: str, remote: str, local: str,
                 deadline: int = None, name: str = None) -> None:
        deadline = deadline or DEADLINES["download"]
        rc, text = self._call(["termark", "download", asset_id, remote, local],
                              deadline, name or "download")
        if rc != 0:
            raise TmError(f"termark download 失败(exit={rc}): {remote} → {local}\n{text[-500:]}")


# ---------------------------------------------------------------- 源快照与构建输入清单

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def enumerate_harness_files(bench_dir: str) -> list:
    """编排 harness 工具件枚举(peer.sh/lib/py/closure_c84.py;
    排除 runs/.bin/__pycache__ 等证据与缓存)。"""
    out = []
    for dirpath, dirnames, filenames in os.walk(bench_dir, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames
                             if d not in WORKSPACE_EXCLUDE_NAMES
                             and not os.path.islink(os.path.join(dirpath, d)))
        for name in sorted(filenames):
            rel = os.path.relpath(os.path.join(dirpath, name), bench_dir)
            top = rel.split(os.sep)[0]
            if top not in ("peer.sh", "lib", "py", "closure_c84.py"):
                continue
            if os.path.islink(os.path.join(dirpath, name)) or name.endswith(".pyc"):
                continue
            out.append(rel)
    return sorted(out)


def snapshot_git_archive(repo: str, dest: str, commit: str) -> dict:
    """baseline:git archive <commit> 导出精确独立源树(不受工作区并发修改影响)。
    产品源码严格等于 commit;随后叠加**编排 harness**工具件
    (benchmark/{closure_c84.py,peer.sh,lib/,py/} 来自当前检出,仅工具,
    不参与 build-inputs/tree_hash/构建),否则远端自校验没有工具可跑。"""
    os.makedirs(dest, exist_ok=True)
    rc, out, err = run_local(["git", "-C", repo, "archive", "--format=tar", commit],
                             deadline_s=300)
    if rc != 0:
        die(f"git archive {commit} 失败: {err.decode('utf-8','replace')[-300:]}")
    with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tf:
        tf.write(out)
        tar_path = tf.name
    try:
        with tarfile.open(tar_path) as tf:
            tf.extractall(dest)
    finally:
        os.unlink(tar_path)
    # harness 叠加(工具件;git archive 里的旧版本被当前工具版本覆盖)
    for rel in enumerate_harness_files(BENCH_DIR):
        src = os.path.join(BENCH_DIR, rel)
        dst = os.path.join(dest, "benchmark", rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
    head = run_local(["git", "-C", repo, "rev-parse", commit], 120)[1].decode().strip()
    return {"rule": f"git-archive:{commit}+harness-overlay", "git_head": head,
            "mode": "baseline"}


def _workspace_include(rel: str) -> bool:
    """确定性收录规则:构建输入 + harness;排除产物/缓存/前端 dist。"""
    parts = rel.split(os.sep)
    if any(p in WORKSPACE_EXCLUDE_NAMES for p in parts):
        return False
    top = parts[0]
    if top in WORKSPACE_DIRS:
        return True
    if top in WORKSPACE_TOP_FILES:
        return True
    if top == "benchmark" and len(parts) > 1:
        rest = os.sep.join(parts[1:])
        if rest in WORKSPACE_BENCH_FILES:
            return True
        return parts[1] in WORKSPACE_BENCH_DIRS
    return False


def enumerate_workspace_files(repo: str) -> list:
    out = []
    for dirpath, dirnames, filenames in os.walk(repo, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames
                             if d not in WORKSPACE_EXCLUDE_NAMES and d != ".git"
                             and not os.path.islink(os.path.join(dirpath, d)))
        for name in sorted(filenames):
            rel = os.path.relpath(os.path.join(dirpath, name), repo)
            if rel.startswith(".git") or os.path.islink(os.path.join(dirpath, name)):
                continue
            if _workspace_include(rel):
                out.append(rel)
    return sorted(out)


def snapshot_workspace(repo: str, dest: str) -> dict:
    """accept:当前工作区静止后的确定性源快照(不含 console/dist → embedded_ui=false)。"""
    files = enumerate_workspace_files(repo)
    if not files:
        die("工作区快照枚举为空(收录规则错误?)")
    for rel in files:
        src = os.path.join(repo, rel)
        dst = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
    head = run_local(["git", "-C", repo, "rev-parse", "HEAD"], 120)[1].decode().strip()
    st = run_local(["git", "-C", repo, "status", "--porcelain"], 120)[1].decode()
    return {"rule": "workspace-deterministic-v1", "git_head": head,
            "git_status_sha256": hashlib.sha256(st.encode()).hexdigest(),
            "file_count": len(files), "mode": "accept"}


def enumerate_build_inputs(tree: str) -> list:
    """构建输入确定性枚举 = source_inventory(Rust/Cargo) + migrations/*.sql
    + rust-toolchain*(cargo 实际读取的额外编译输入;console/dist 固定不嵌)。
    保持既有 sources.sha256 schema 不动,补充口径只落在本清单。"""
    files = set(source_inventory.enumerate_source_files(tree))
    for name in WORKSPACE_TOP_FILES:
        p = os.path.join(tree, name)
        if os.path.isfile(p) and not os.path.islink(p):
            files.add(name)
    mig = os.path.join(tree, "migrations")
    if os.path.isdir(mig) and not os.path.islink(mig):
        for name in sorted(os.listdir(mig)):
            if name.endswith(".sql") and not os.path.islink(os.path.join(mig, name)):
                files.add(os.path.join("migrations", name))
    return sorted(files)


def tree_hash(tree: str, files=None) -> str:
    files = files if files is not None else enumerate_build_inputs(tree)
    h = hashlib.sha256()
    for rel in files:
        h.update(rel.encode())
        h.update(b"\0")
        h.update(sha256_file(os.path.join(tree, rel)).encode())
        h.update(b"\0")
    return h.hexdigest()


def write_build_inputs(tree: str, out_dir: str, meta: dict) -> dict:
    """写 build/build-inputs.sha256(sha256sum -c 兼容)+ build/build-inputs.json。"""
    os.makedirs(out_dir, exist_ok=True)
    files = enumerate_build_inputs(tree)
    missing = [m for m in MIN_MIGRATIONS if m not in files]
    if missing:
        die(f"构建输入枚举缺少 migrations(实际编译输入不完整): {missing}")
    lines = [f"{sha256_file(os.path.join(tree, rel))}  {rel}" for rel in files]
    with open(os.path.join(out_dir, "build-inputs.sha256"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    doc = dict(meta)
    doc.update({
        "created_at": now_iso(),
        "file_count": len(files),
        "tree_hash": tree_hash(tree, files),
        "migrations": [m for m in files if m.startswith("migrations/")],
        "embedded_ui": False,
        "embedded_ui_note": "快照不含 console/dist;基线与候选固定一致的非嵌入策略",
    })
    with open(os.path.join(out_dir, "build-inputs.json"), "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2, sort_keys=True)
    return doc


def read_manifest_lines(path: str) -> dict:
    out = {}
    for ln in open(path, encoding="utf-8").read().splitlines():
        ln = ln.strip()
        if not ln:
            continue
        m = re.match(r"^([0-9a-f]{64})  (\S.+)$", ln)
        if not m:
            raise ValueError(f"build-inputs 行格式不符: {ln[:80]}")
        out[m.group(2)] = m.group(1)
    return out


# ---------------------------------------------------------------- 远端基础操作

def remote_root_for(mode: str, stamp: str) -> str:
    return f"/tmp/rh-closure-c84-{mode}-{stamp}"


def src_root(remote_root: str) -> str:
    """源树在远端独立目录下的固定位置(tree/,构建产物/日志/轮次都在
    remote_root 其余子目录,源树保持不可变)。"""
    return f"{remote_root}/tree"


def remote_sh(tm: Tm, asset: dict, cmd: str, deadline=None, name=None, check=True) -> str:
    return tm.exec_sh(asset["id"], cmd, deadline=deadline, name=name, check=check)


def remote_nohup(tm: Tm, asset: dict, inner_cmd: str, rundir: str, tag: str,
                 deadline: int, poll_interval: float = 20.0) -> int:
    """远端长任务:setsid 包一层,后台包装 bash 自己把 $$ 写进 pid 文件
    (会话组长 PID,治理清理即整组),退出码落 <tag>.exit,轮询到点。

    ★实测教训(linux-baseline attempt1):termark exec 有 60s 硬超时,
    通道可能报 timedOut/exitCode=-1 而远端实际已拉起(pid 文件在、任务
    完整跑完)。因此 launch 的通道结果**不可信时不判死**:统一以 pid 文件
    是否出现为准(最多恢复等待 40s),出现即继续正常轮询;未出现才失败。"""
    exit_f = f"{rundir}/{tag}.exit"
    log_f = f"{rundir}/{tag}.log"
    pid_f = f"{rundir}/{tag}.pid"
    launch_err = None
    launch = (
        f"mkdir -p {shlex.quote(rundir)} && rm -f {exit_f} {pid_f} && "
        # 后台包装 bash:先落自己的 $$(setsid 会话组长),再执行命令
        # (内部 exit N 只结束子 bash,外层总是能落退出码)
        f"setsid bash -c {shlex.quote('echo $$ > ' + pid_f + '; bash -c ' + shlex.quote(inner_cmd) + '; echo $? > ' + exit_f)} "
        f">{log_f} 2>&1 < /dev/null & disown; echo LAUNCHED"
    )
    try:
        tm.exec(asset["id"], launch, deadline=45, name=f"launch-{tag}", check=True)
    except (TmError, CommandTimeout) as exc:
        launch_err = f"{type(exc).__name__}: {exc}"
        tm.progress.event("launch 通道失败/超时,转入 pid 文件恢复判定",
                          f"{tag}: {launch_err[:200]}(远端可能已拉起,不判死)")
    # 统一从 pid 文件取 pid(成功与恢复路径同一条代码;rm -f 已清旧值)
    pid = None
    recover_deadline = time.time() + 40
    while time.time() < recover_deadline:
        out = tm.exec(asset["id"], f"cat {pid_f} 2>/dev/null || echo NONE",
                      deadline=DEADLINES["exec_small"],
                      name=f"recover-pid-{tag}", check=False)[1]
        v = out.strip().splitlines()[-1] if out.strip() else "NONE"
        if v.isdigit():
            pid = v
            break
        time.sleep(2)
    if pid is None:
        raise TmError(f"{tag} 启动失败:pid 文件 40s 内未出现"
                      f"(launch 通道错误: {launch_err})")
    if launch_err:
        tm.progress.event("launch 恢复成功(以 pid 文件为准)",
                          f"{tag} pid={pid}")
    # 记录身份(exe/starttime/pgid)供超时清理校验
    ident = remote_sh(
        tm, asset,
        f"p={pid}; echo exe=$(readlink /proc/$p/exe 2>/dev/null); "
        f"awk '{{print $22}}' /proc/$p/stat 2>/dev/null; "
        f"echo pgid=$(awk '{{print $5}}' /proc/$p/stat 2>/dev/null)",
        name=f"ident-{tag}", check=False)
    tm.progress.event("远端任务已启动", f"{asset.get('name', tag)}/{tag} pid={pid}",
                      pid=pid, ident=ident.strip())
    deadline_ts = time.time() + deadline
    last_beat = 0.0
    while time.time() < deadline_ts:
        rc, out = tm.exec(asset["id"],
                          f"if [ -f {exit_f} ]; then cat {exit_f}; "
                          f"elif kill -0 {pid} 2>/dev/null; then echo RUNNING; "
                          f"else echo LOST; fi",
                          deadline=DEADLINES["drive_poll"], name=f"poll-{tag}", check=False)
        state = out.strip().splitlines()[-1] if out.strip() else "LOST"
        if time.time() - last_beat >= 60:
            tm.progress.event("任务进行中", f"{tag}: {state}")
            last_beat = time.time()
        if state == "RUNNING":
            time.sleep(poll_interval)
            continue
        if state == "LOST":
            governed_kill(tm, asset, int(pid), ident, tag)
            raise TmError(f"{tag} 进程丢失且无退出码文件")
        try:
            return int(state)
        except ValueError:
            continue
    governed_kill(tm, asset, int(pid), ident, tag)
    raise CommandTimeout(f"远端任务超时({deadline}s)已治理清理: {tag}")


def remote_launch(tm: Tm, asset: dict, cmd: str, rundir: str, tag: str) -> tuple:
    """启动常驻远端服务(setsid 自写 pid 文件),立即返回 (pid, ident);
    不等待退出(与 remote_nohup 的有限任务互补)。治理清理走 governed_kill。"""
    exit_f = f"{rundir}/{tag}.exit"
    log_f = f"{rundir}/{tag}.log"
    pid_f = f"{rundir}/{tag}.pid"
    launch = (
        f"mkdir -p {shlex.quote(rundir)} && rm -f {exit_f} {pid_f} && "
        f"setsid bash -c {shlex.quote('echo $$ > ' + pid_f + '; bash -c ' + shlex.quote(cmd) + '; echo $? > ' + exit_f)} "
        f">{log_f} 2>&1 < /dev/null & disown; echo LAUNCHED"
    )
    launch_err = None
    try:
        tm.exec(asset["id"], launch, deadline=45, name=f"launch-{tag}", check=True)
    except (TmError, CommandTimeout) as exc:
        launch_err = f"{type(exc).__name__}: {exc}"
        tm.progress.event("launch 通道失败/超时,转入 pid 文件恢复判定",
                          f"{tag}: {launch_err[:200]}(远端可能已拉起,不判死)")
    pid = None
    recover_deadline = time.time() + 40
    while time.time() < recover_deadline:
        out = tm.exec(asset["id"], f"cat {pid_f} 2>/dev/null || echo NONE",
                      deadline=DEADLINES["exec_small"],
                      name=f"recover-pid-{tag}", check=False)[1]
        v = out.strip().splitlines()[-1] if out.strip() else "NONE"
        if v.isdigit():
            pid = v
            break
        time.sleep(2)
    if pid is None:
        raise TmError(f"{tag} 启动失败:pid 文件 40s 内未出现({launch_err})")
    ident = remote_sh(
        tm, asset,
        f"p={pid}; echo exe=$(readlink /proc/$p/exe 2>/dev/null); "
        f"awk '{{print $22}}' /proc/$p/stat 2>/dev/null; "
        f"echo pgid=$(awk '{{print $5}}' /proc/$p/stat 2>/dev/null)",
        name=f"ident-{tag}", check=False)
    tm.progress.event("远端服务已启动", f"{asset.get('name', tag)}/{tag} pid={pid}",
                      pid=pid, ident=ident.strip())
    return int(pid), ident


def governed_kill(tm: Tm, asset: dict, pid: int, ident: str, tag: str,
                  expect_names=None) -> bool:
    """只终止 runner 自己记录的 PID/进程组:先校验 /proc/PID/exe 基名、
    starttime 与记录一致,且 exe 基名在期望集合内(rh-server/rh-client/
    python3*/iperf3/bash);TERM 后 5s 重验同 PID+starttime(防 PID 复用)
    才 KILL。进程组仅当 PGID==PID(我们自己 setsid 创建的组)才整组治理,
    否则只杀该 PID。绝不按名字扫杀。"""
    expect_names = expect_names or ("rh-server", "rh-client", "ipython3", "python3",
                                    "iperf3", "bash", "sh", "loadgen", "cpu_sampler.py")
    recorded = {}
    for ln in ident.strip().splitlines():
        if ln.startswith("exe="):
            recorded["exe"] = ln[4:].strip()
        elif ln.startswith("pgid="):
            recorded["pgid"] = ln[5:].strip()
        elif ln.strip().isdigit():
            recorded["starttime"] = ln.strip()
    rc, out = tm.exec(asset["id"],
                      f"p={pid}; e=$(readlink /proc/$p/exe 2>/dev/null); "
                      f"s=$(awk '{{print $22}}' /proc/$p/stat 2>/dev/null); "
                      f"g=$(awk '{{print $5}}' /proc/$p/stat 2>/dev/null); "
                      f"echo \"$e|$s|$g\"", name=f"govcheck-{tag}", check=False)
    cur = out.strip().splitlines()[-1] if out.strip() else "||"
    exe_now, st_now, pgid_now = (cur.split("|") + ["", "", ""])[:3]
    base = os.path.basename(exe_now) if exe_now else ""
    ok_name = any(base.startswith(n) for n in expect_names) if base else False
    ok_time = bool(recorded.get("starttime")) and recorded["starttime"] == st_now.strip()
    if not (exe_now and ok_name and ok_time):
        tm.progress.event("跳过治理清理(身份不匹配)", f"{tag} pid={pid} exe={base!r} "
                          f"starttime_match={ok_time}(不杀未知进程)",
                          pid=pid, exe=base, starttime_match=ok_time)
        return False
    group_kill = pgid_now.strip() == str(pid)   # 只治理自己 setsid 的组
    sig_target = f"-{pid}" if group_kill else f"{pid}"
    # TERM → 5s → 重验同 PID+starttime(防复用)→ KILL
    tm.exec(asset["id"],
            f"kill -TERM {sig_target} 2>/dev/null; sleep 5; "
            f"if kill -0 {pid} 2>/dev/null && "
            f"[ \"$(awk '{{print $22}}' /proc/{pid}/stat 2>/dev/null)\" = "
            f"\"{recorded['starttime']}\" ]; then "
            f"kill -KILL {sig_target} 2>/dev/null; fi; true",
            name=f"govkill-{tag}", check=False)
    tm.progress.event("已治理清理", f"{tag} pid={pid} exe={base}"
                      + ("(整组)" if group_kill else ""), pid=pid, exe=base,
                      group=group_kill)
    return True


def preflight_ports(tm: Tm, asset: dict, ports, label: str) -> None:
    """端口预检:被占用即报错(绝不杀未知进程)。ss 不可用时 fail closed。
    端口集合用合法 | 交替(空格拼接会变成永不匹配的单字面量→冲突漏检)。"""
    plist = "|".join(str(p) for p in ports)
    out = remote_sh(tm, asset,
                    f"command -v ss >/dev/null || echo NO_SS; "
                    f"ss -lntu 2>/dev/null | awk '{{print $5}}' | grep -Eo '[0-9]+$' "
                    f"| sort -u | grep -E '^({plist})$' || true",
                    name=f"preflight-{label}")
    if "NO_SS" in out:
        die(f"端口预检不可用({asset.get('name', label)}): 无 ss;fail closed 不盲跑")
    busy = [ln for ln in out.split() if ln.strip() and ln.strip() != "NO_SS"]
    if busy:
        die(f"端口预检失败({asset.get('name', label)}): 端口 {busy} 已被占用;"
            f"按约定不杀未知进程,请人工排查后重跑")


def preflight_round(tm: Tm) -> None:
    preflight_ports(tm, ASSETS["test1"], SERVER_LISTEN_PORTS, "server侧")
    preflight_ports(tm, ASSETS["test2"], CLIENT_LISTEN_PORTS, "client侧")


def check_remote_env(tm: Tm, asset: dict) -> dict:
    """环境探针 + 时钟偏差(本地 t0/t1 夹住远端 date,offset=远端-中点,
    不确定度 ±半程 RTT;仅记录,不据此放缩任何验收口径)。"""
    t0 = time.time()
    out = remote_sh(tm, asset,
                    "uname -r; python3 --version 2>&1 | head -1; "
                    "command -v ss >/dev/null && echo ss=ok; "
                    "command -v jq >/dev/null && echo jq=ok; "
                    "command -v curl >/dev/null && echo curl=ok; "
                    "date +%s.%N; df -h /tmp | tail -1; ulimit -Sn",
                    name=f"env-{asset.get('name','?')}")
    t1 = time.time()
    info = {"raw": out, "rtt_s": round(t1 - t0, 3)}
    for ln in out.splitlines():
        if re.fullmatch(r"\d{9,12}\.\d+", ln.strip()):
            remote_t = float(ln.strip())
            mid = (t0 + t1) / 2
            info["clock_offset_s"] = round(remote_t - mid, 3)
            info["clock_uncertainty_s"] = round((t1 - t0) / 2, 3)
        if ln.startswith(("5.", "4.", "6.")):
            info["kernel"] = ln.strip()
    return info


# ---------------------------------------------------------------- 构建与二进制分发

FD_WANT = 65535


def fd_raise_snippet(want: int = FD_WANT) -> str:
    """父进程范围内 raise nofile(<=hard;与 peer.sh raise_nofile 同口径)。
    嵌入 cargo/test/E2E 父 shell,子进程继承。"""
    return (f"ulimit -n {want} 2>/dev/null || ulimit -n \"$(ulimit -Hn)\" 2>/dev/null; ")


def remote_fd_preflight(tm: Tm, asset: dict, want: int = FD_WANT) -> dict:
    """fd 软限预检:证明在该主机进程范围内可提升到 want(不超过 hard),
    返回并记录实际 soft/hard。不足 → **fail-closed 环境错误**(非产品失败):
    EMFILE(code 24)绝不折算成测试/产品失败(★linux-baseline attempt1 实测:
    默认软限 1024 下 tcp_listener_lifecycle filler 连接 EMFILE → exit 101)。"""
    out = remote_sh(tm, asset,
                    "bash -c 'ulimit -n %d 2>/dev/null || ulimit -n \"$(ulimit -Hn)\" "
                    "2>/dev/null; echo nofile_soft=$(ulimit -Sn) "
                    "nofile_hard=$(ulimit -Hn)'" % want,
                    name=f"fd-preflight-{asset.get('name','?')}")
    m = re.search(r"nofile_soft=(\S+) nofile_hard=(\S+)", out)
    if not m:
        die(f"fd 预检输出不可解析({asset.get('name')}): {out.strip()[:160]}")
    soft, hard = m.group(1), m.group(2)
    ok = soft == "unlimited" or (soft.isdigit() and int(soft) >= want)
    if not ok:
        die(f"环境错误(fail-closed,非产品):{asset.get('name')} fd 软限提升后="
            f"{soft} < {want}(hard={hard});EMFILE 不得计入产品/测试失败,"
            f"请排查该账户 limits(非 sysctl,进程范围即可)")
    tm.progress.event("fd 软限预检通过", f"{asset.get('name')} soft={soft} hard={hard}")
    return {"soft": soft, "hard": hard, "want": want}


def remote_toolchain_pinned(repo: str) -> str:
    p = os.path.join(repo, "rust-toolchain.toml")
    if os.path.isfile(p):
        m = re.search(r'channel\s*=\s*"([^"]+)"', open(p, encoding="utf-8").read())
        if m:
            return m.group(1)
    return ""


def remote_build(tm: Tm, out_dir: str, remote_root: str, toolchain: str) -> dict:
    """test1 上:cargo test --workspace + cargo build --release(真实 exit 落盘)。
    源树在 {remote_root}/tree 且不可变;构建产物 target/ 在源树内由 cargo 生成,
    清单/日志/轮次目录在 remote_root 下。"""
    a = ASSETS["test1"]
    src = src_root(remote_root)
    logs_remote = f"{remote_root}/logs"
    remote_sh(tm, a, f"mkdir -p {logs_remote}", name="mkdir-logs")
    # ★fail-closed fd 预检(非产品):cargo/test/E2E 父 shell 统一 raise 65535
    fd_info = remote_fd_preflight(tm, a)
    pre = (f"export PATH=$HOME/.cargo/bin:$PATH; "
           f"export RUSTUP_TOOLCHAIN={shlex.quote(toolchain)}; "
           # ★实测(linux-baseline attempt1):tcp_listener_lifecycle 的 filler
           # 连接在默认软限 1024 下 EMFILE(code 24)→ 测试失败。构建/测试
           # 子进程统一 raise_nofile(与 peer.sh 同口径;硬限 1048576 实测),
           # 实值已由 remote_fd_preflight 校验并记录。
           + fd_raise_snippet() +
           f"echo nofile_soft=$(ulimit -Sn); cd {src}; ")
    # 工具链自检:缺 channel 时用户态 rustup install(失败=环境错误,如实中止)
    rc, out = tm.exec(a["id"],
                      pre + f"rustup toolchain list | grep -q '{toolchain}' && echo HAVE || echo MISS",
                      name="toolchain-check", check=False)
    if "MISS" in out:
        tm.progress.event("安装缺失工具链(用户态 rustup)", toolchain)
        remote_nohup(tm, a, pre + f"rustup toolchain install {shlex.quote(toolchain)}",
                     remote_root, "toolchain-install", deadline=1800)
    tm.progress.event("开始 Linux workspace 测试", "cargo test --workspace --locked")
    rc_test = remote_nohup(tm, a, pre + "cargo test --workspace --locked",
                           remote_root, "test-workspace", deadline=DEADLINES["cargo_test"])
    remote_sh(tm, a, f"cp {remote_root}/test-workspace.log {logs_remote}/ 2>/dev/null; "
                     f"cp {remote_root}/test-workspace.exit {logs_remote}/ 2>/dev/null; true",
              name="collect-test-log")
    if rc_test != 0:
        tail = remote_sh(tm, a, f"tail -40 {remote_root}/test-workspace.log", check=False)
        die(f"cargo test --workspace 真实失败(exit={rc_test}),按约定不豁免:\n{tail}")
    tm.progress.event("开始 Linux release 构建", "cargo build --release --locked")
    rc_build = remote_nohup(tm, a, pre + "cargo build --release --locked",
                            remote_root, "build-release", deadline=DEADLINES["cargo_build"])
    remote_sh(tm, a, f"cp {remote_root}/build-release.log {logs_remote}/ 2>/dev/null; "
                     f"cp {remote_root}/build-release.exit {logs_remote}/ 2>/dev/null; true",
              name="collect-build-log")
    if rc_build != 0:
        tail = remote_sh(tm, a, f"tail -40 {remote_root}/build-release.log", check=False)
        die(f"cargo build --release 真实失败(exit={rc_build}):\n{tail}")
    # 构建清单(既有 schema)+ build-inputs(本工具补充口径,远端生成后回传比对)
    remote_sh(tm, a,
              f"cd {src} && python3 benchmark/py/build_manifest.py . target/release "
              f"{shlex.quote(remote_root + '/build')} >/dev/null", name="build-manifest")
    remote_sh(tm, a,
              f"cd {src} && python3 benchmark/closure_c84.py _internal-build-inputs "
              f"--tree . --out {shlex.quote(remote_root + '/build')}",
              name="remote-build-inputs")
    ver = remote_sh(tm, a, pre + "rustc --version; cargo --version; uname -r",
                    name="remote-versions")
    # 分发二进制 → test2(peer.sh 要求 BENCH_BIN 内 rh-server/rh-client 都在)
    local_bin = os.path.join(out_dir, "bin")
    os.makedirs(local_bin, exist_ok=True)
    for n in ("rh-server", "rh-client"):
        tm.download(a["id"], f"{src}/target/release/{n}",
                    os.path.join(local_bin, n), name=f"dl-{n}")
    b = ASSETS["test2"]
    remote_sh(tm, b, f"mkdir -p {src}/bin", name="mkdir-test2-bin")
    for n in ("rh-server", "rh-client"):
        tm.upload(b["id"], os.path.join(local_bin, n), f"{src}/bin/{n}",
                  name=f"ul-{n}")
    remote_sh(tm, b, f"chmod +x {src}/bin/rh-server {src}/bin/rh-client",
              name="chmod-test2-bin")
    hashes = {}
    for n in ("rh-server", "rh-client"):
        h1 = remote_sh(tm, a, f"sha256sum {src}/target/release/{n} | awk '{{print $1}}'",
                       name=f"sha-{n}-t1").strip()
        h2 = sha256_file(os.path.join(local_bin, n))
        if h1 != h2:
            die(f"{n} 传输哈希不一致: test1={h1} local={h2}")
        h3 = remote_sh(tm, b, f"sha256sum {src}/bin/{n} | awk '{{print $1}}'",
                       name=f"sha-{n}-t2").strip()
        if h3 != h1:
            die(f"{n} 分发哈希不一致: test2={h3} expect={h1}")
        hashes[n] = h1
    tm.progress.event("构建与分发完成", "binaries sha256 已三端核对",
                      **{f"sha_{k}": v for k, v in hashes.items()})
    return {"test_exit": rc_test, "build_exit": rc_build,
            "versions": ver, "binaries": hashes, "nofile": fd_info}


# ---------------------------------------------------------------- 三机一轮(peer.sh)

class RoundCfg:
    def __init__(self, proto: str, mux: int, label: str, run_id: str,
                 repeat: int = 3, duration: int = 10,
                 conc_levels: str = "64", drive_deadline: int = None,
                 env_extra: dict = None, affinity: str = None):
        self.proto, self.mux, self.label, self.run_id = proto, mux, label, run_id
        self.repeat, self.duration, self.conc_levels = repeat, duration, conc_levels
        self.drive_deadline = drive_deadline or DEADLINES["drive_round"]
        self.env_extra = env_extra or {}
        # CPU affinity 临时对照(taskset -pc 只作用于本 runner 记录的 server
        # PID;进程即起即回收,可逆;非 sysctl)
        self.affinity = affinity


def peer_env(cfg: RoundCfg, remote_root: str) -> str:
    ev = {
        "PEER_DIR": None,  # per-role, caller patches
        "PROTO": cfg.proto, "RELAY": "splice", "MUX": str(cfg.mux),
        "PEER_RUN_ID": cfg.run_id, "BENCH_BIN": None,
        "IPERF3_BIN": "/tmp/rh-bin/iperf3",
        "PEER_REPEAT": str(cfg.repeat), "PEER_DURATION": str(cfg.duration),
        "PEER_CONC_LEVELS": cfg.conc_levels,
    }
    ev.update(cfg.env_extra)
    return ev


def _env_prefix(ev: dict) -> str:
    return "".join(f"{k}={shlex.quote(str(v))} " for k, v in ev.items() if v is not None)


class RoundRunner:
    """一轮 = 一个 (proto,mux) 配置:server(test1) + client(test2) + drive(test3)。
    全程 PID 治理(记录/校验/清理),证据按 verify_goal schema 回收组装。"""

    def __init__(self, tm: Tm, progress: Progress, out_dir: str, remote_root: str,
                 cfg: RoundCfg):
        self.tm, self.progress, self.out_dir = tm, progress, out_dir
        self.remote_root = remote_root
        self.src_root = src_root(remote_root)
        self.cfg = cfg
        self.rdir = f"{remote_root}/rounds/{cfg.label}"
        self.roles = []          # (asset, tag, pid) 已启动远端任务
        self.role_idents = {}    # tag -> ident text
        self.stopped = False

    # -- 启动 ------------------------------------------------------------
    def start_server(self) -> None:
        a = ASSETS["test1"]
        self.tm.exec_sh(a["id"], f"mkdir -p {self.rdir}/server", name="mkdir-server")
        ev = peer_env(self.cfg, self.remote_root)
        ev["PEER_DIR"] = f"{self.rdir}/server"
        ev["BENCH_BIN"] = f"{self.src_root}/target/release"
        cmd = (f"cd {self.src_root}/benchmark && {_env_prefix(ev)} "
               f"bash peer.sh server")
        rc = remote_nohup(self.tm, a, cmd, f"{self.rdir}/server", "server-role",
                          deadline=DEADLINES["role_start"])
        if rc != 0:
            self.stop_all()
            die(f"peer.sh server 失败(exit={rc}):\n"
                f"{self.tm.exec_sh(a['id'], f'tail -30 {self.rdir}/server/server-role.log', check=False)}")
        self._record_role(a, "server")

    def start_client(self) -> None:
        a1, a2 = ASSETS["test1"], ASSETS["test2"]
        # 凭据经 0600 文件流转,不进命令行/终端/结果
        sec_dir = os.path.join(self.out_dir, ".secrets")
        os.makedirs(sec_dir, exist_ok=True)
        os.chmod(sec_dir, 0o700)
        cid = self.tm.exec_sh(a1["id"], f"cat {self.rdir}/server/client_id",
                              name="read-cid").strip()
        if not cid:
            self.stop_all()
            die("server 角色未产出 client_id")
        secret_local = os.path.join(sec_dir, f"{self.cfg.label}-client_secret")
        self.tm.download(a1["id"], f"{self.rdir}/server/client_secret",
                         secret_local, name="dl-secret")
        os.chmod(secret_local, 0o600)
        secret_remote = f"{self.rdir}/client_secret"
        self.tm.upload(a2["id"], secret_local, secret_remote, name="ul-secret")
        cid_remote = f"{self.rdir}/client_id"
        cid_local = os.path.join(sec_dir, f"{self.cfg.label}-cid")
        with open(cid_local, "w", encoding="utf-8") as fh:
            fh.write(cid + "\n")
        os.chmod(cid_local, 0o600)
        self.tm.upload(a2["id"], cid_local, cid_remote, name="ul-cid")
        self.tm.exec_sh(a2["id"], f"chmod 600 {secret_remote} {cid_remote}",
                        name="chmod-creds")
        ev = peer_env(self.cfg, self.remote_root)
        ev["PEER_DIR"] = f"{self.rdir}/client"
        ev["BENCH_BIN"] = f"{self.src_root}/bin"
        cmd = (f"cd {self.src_root}/benchmark && {_env_prefix(ev)} "
               f"bash peer.sh client {ASSETS['test1']['ip']}:{PORT_PLAN['ctl']} "
               f"\"$(cat {cid_remote})\" \"$(cat {secret_remote})\"")
        rc = remote_nohup(self.tm, a2, cmd, f"{self.rdir}/client", "client-role",
                          deadline=DEADLINES["role_start"])
        if rc != 0:
            self.stop_all()
            die(f"peer.sh client 失败(exit={rc}):\n"
                f"{self.tm.exec_sh(a2['id'], f'tail -30 {self.rdir}/client/client-role.log', check=False)}")
        self._record_role(a2, "client")

    def apply_affinity(self) -> None:
        """server_affinity 对照:taskset -pc 只作用于自己记录的 server PID。"""
        aff = getattr(self.cfg, "affinity", None)
        if not aff:
            return
        pid = None
        for _asset, tag, p, _pn in self.roles:
            if tag == "server/server.pid":
                pid = p
        if pid is None:
            die("server_affinity:未找到已记录的 server.pid(不作用于未知进程)")
        out = self.tm.exec_sh(ASSETS["test1"]["id"],
                              f"taskset -pc {shlex.quote(aff)} {pid}",
                              name=f"affinity-{self.cfg.label}", check=False)
        self.progress.event("CPU affinity 对照已应用",
                            f"{self.cfg.label}: taskset -pc {aff} {pid} → "
                            f"{out.strip().splitlines()[-1][:80]}")

    def run_drive(self) -> int:
        a1, a3 = ASSETS["test1"], ASSETS["test3"]
        sec_dir = os.path.join(self.out_dir, ".secrets")
        token_local = os.path.join(sec_dir, f"{self.cfg.label}-token")
        self.tm.download(a1["id"], f"{self.rdir}/server/token", token_local,
                         name="dl-token")
        os.chmod(token_local, 0o600)
        d = f"{self.rdir}/drive"
        self.tm.exec_sh(a3["id"], f"mkdir -p {d}", name="mkdir-drive")
        self.tm.upload(a3["id"], token_local, f"{d}/token", name="ul-token")
        cid_remote = f"{d}/cid"
        cid_local = os.path.join(sec_dir, f"{self.cfg.label}-cid")
        self.tm.upload(a3["id"], cid_local, cid_remote, name="ul-cid")
        self.tm.exec_sh(a3["id"], f"chmod 600 {d}/token {cid_remote}",
                        name="chmod-drive-creds")
        ev = peer_env(self.cfg, self.remote_root)
        ev["PEER_DIR"] = d
        cmd = (f"cd {self.src_root}/benchmark && {_env_prefix(ev)} "
               f"bash peer.sh drive {ASSETS['test1']['ip']} {ASSETS['test2']['ip']} "
               f"{PORT_PLAN['api']} \"$(cat {d}/token)\" \"$(cat {cid_remote})\" {self.cfg.label}")
        self.progress.event("开始压测轮", f"{self.cfg.label} "
                           f"(proto={self.cfg.proto} mux={self.cfg.mux} "
                           f"repeat={self.cfg.repeat} conc={self.cfg.conc_levels})")
        return remote_nohup(self.tm, a3, cmd, d, "drive", deadline=self.cfg.drive_deadline)

    # -- 诊断(观察,不归因;只读 /proc 与本机 metrics)--------------------
    def diagnostics(self) -> None:
        rows = []
        for asset, sub in ((ASSETS["test1"], "server"), (ASSETS["test2"], "client")):
            out = self.tm.exec_sh(
                asset["id"],
                f"for f in {self.rdir}/{sub}/*.pid; do [ -f \"$f\" ] || continue; "
                f"p=$(cat \"$f\"); [ -d /proc/$p ] || continue; "
                f"echo name=$(basename $f) pid=$p "
                f"exe=$(readlink /proc/$p/exe 2>/dev/null) "
                f"ctxt=$(awk '/voluntary_ctxt_switches/{{print $2}}' /proc/$p/status 2>/dev/null)"
                f"/$(awk '/nonvoluntary/{{print $2}}' /proc/$p/status 2>/dev/null) "
                f"cpu=$(awk '{{print $39}}' /proc/$p/stat 2>/dev/null); done",
                name=f"diag-{sub}", check=False)
            rows.append({"role": sub, "procs": out.strip().splitlines()})
        # 运行 path/传输/连接计数器(server 本机 Prometheus 只读抓取,全族)
        metrics = self.tm.exec_sh(
            ASSETS["test1"]["id"],
            f"curl -s -m 10 http://127.0.0.1:{PORT_PLAN['api'] + 1000}/metrics "
            f"| grep -E '^rabbithole_' | head -200",
            name="diag-metrics", check=False)
        # OS UDP 统计(收发/错误/丢包;只读 ss -su,两端)
        udp_stats = []
        for asset, tag in ((ASSETS["test1"], "server"), (ASSETS["test2"], "client")):
            udp_stats.append({"role": tag, "ss_su": self.tm.exec_sh(
                asset["id"], "ss -su 2>/dev/null | tail -n +2",
                name=f"diag-udp-{tag}", check=False).strip().splitlines()})
        ev_dir = os.path.join(self.out_dir, "evidence", self.cfg.label)
        os.makedirs(ev_dir, exist_ok=True)
        with open(os.path.join(ev_dir, "diag-procs.json"), "w", encoding="utf-8") as fh:
            json.dump({"ts": now_iso(), "rows": rows, "udp_stats": udp_stats,
                       "metrics": metrics.strip().splitlines()[:200]},
                      fh, ensure_ascii=False, indent=2)

    # -- 证据回收 --------------------------------------------------------
    def collect(self) -> str:
        ev_dir = os.path.join(self.out_dir, "evidence", self.cfg.label)
        os.makedirs(ev_dir, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix="rh-closure-collect-")
        try:
            self.tm.download(ASSETS["test3"]["id"], f"{self.rdir}/drive",
                             os.path.join(tmp, "drive"), name="dl-drive")
            self.tm.download(ASSETS["test1"]["id"], f"{self.rdir}/server",
                             os.path.join(tmp, "server"), name="dl-server")
            self.tm.download(ASSETS["test2"]["id"], f"{self.rdir}/client",
                             os.path.join(tmp, "client"), name="dl-client")
            # drive 侧:results/raw/schema → 证据根(不含 token/cid 等凭据文件)
            for name in os.listdir(os.path.join(tmp, "drive")):
                if (name.startswith("results-") and name.endswith(".jsonl")) \
                        or name in ("schema.json",):
                    shutil.copy2(os.path.join(tmp, "drive", name), os.path.join(ev_dir, name))
            if os.path.isdir(os.path.join(tmp, "drive", "raw")):
                shutil.copytree(os.path.join(tmp, "drive", "raw"),
                                os.path.join(ev_dir, "raw"))
            # 端点四件套
            for role in ("server", "client"):
                dst = os.path.join(ev_dir, "endpoints", role)
                os.makedirs(dst, exist_ok=True)
                for f in ("endpoint.json", "identity.jsonl", "cpu.jsonl", "nofile.json"):
                    src = os.path.join(tmp, role, f)
                    if os.path.isfile(src):
                        shutil.copy2(src, os.path.join(dst, f))
                # ★c84 诊断:两端 rh 进程日志(原件 + 脱敏副本;log_level=warn,
                # 若有会话错误行会在此可见)
                src_log = os.path.join(tmp, role, f"{role}.log")
                if os.path.isfile(src_log):
                    dlog = os.path.join(ev_dir, "endpoint-logs")
                    os.makedirs(dlog, exist_ok=True)
                    shutil.copy2(src_log, os.path.join(dlog, f"{role}.log"))
                    with open(src_log, encoding="utf-8", errors="replace") as fh:
                        txt = fh.read()
                    with open(os.path.join(dlog, f"{role}.log.redacted"), "w",
                              encoding="utf-8") as fh:
                        fh.write(redact(txt))
            # build 三件套(单份构建,所有证据目录同源)
            build_src = os.path.join(self.out_dir, "build")
            if os.path.isdir(build_src):
                dst = os.path.join(ev_dir, "build")
                os.makedirs(dst, exist_ok=True)
                for f in ("sources.sha256", "binaries.json", "build-inputs.sha256",
                          "build-inputs.json"):
                    src = os.path.join(build_src, f)
                    if os.path.isfile(src):
                        shutil.copy2(src, os.path.join(dst, f))
                bdir = os.path.join(dst, "bin")
                os.makedirs(bdir, exist_ok=True)
                for b in ("rh-server", "rh-client"):
                    srcb = os.path.join(build_src, "bin", b)
                    if os.path.isfile(srcb):
                        shutil.copy2(srcb, os.path.join(bdir, b))
        finally:
            # tmp 里含远端整目录下载(带 token/client_secret)——用完即删,
            # 凭据只保留在 out_dir/.secrets(0600)
            shutil.rmtree(tmp, ignore_errors=True)
        # 结果文件脱敏副本(results 里不含 secret,但 raw stderr 理论可能带;
        # 原件保留,报告用 .redacted)
        for name in os.listdir(ev_dir):
            if name.startswith("results-"):
                p = os.path.join(ev_dir, name)
                with open(p, encoding="utf-8") as fh:
                    txt = fh.read()
                with open(p + ".redacted", "w", encoding="utf-8") as fh:
                    fh.write(redact(txt))
        self.progress.event("证据已回收组装", self.cfg.label, evidence=ev_dir)
        return ev_dir

    # -- PID 治理 --------------------------------------------------------
    def _record_role(self, asset: dict, tag: str) -> None:
        sub = tag  # server/client
        pids = self.tm.exec_sh(
            asset["id"],
            f"for f in {self.rdir}/{sub}/*.pid; do [ -f \"$f\" ] || continue; "
            f"echo \"$(basename $f) $(cat $f)\"; done",
            name=f"list-pids-{tag}", check=False)
        for ln in pids.strip().splitlines():
            parts = ln.split()
            if len(parts) != 2 or not parts[1].isdigit():
                continue
            pname, pid = parts
            ident = self.tm.exec_sh(
                asset["id"],
                f"p={pid}; echo exe=$(readlink /proc/$p/exe 2>/dev/null); "
                f"awk '{{print $22}}' /proc/$p/stat 2>/dev/null",
                name=f"ident-{tag}-{pname}", check=False)
            self.role_idents[f"{tag}/{pname}"] = ident
            self.roles.append((asset, f"{tag}/{pname}", int(pid), pname))
        self.progress.event("角色 PID 已记录", f"{tag}: "
                           f"{[r[1] + '=' + str(r[2]) for r in self.roles if r[1].startswith(tag + '/')]}")

    def stop_all(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        for asset, tag, pid, pname in self.roles:
            governed_kill(self.tm, asset, pid, self.role_idents.get(tag, ""), tag)
        # 兜底:peer.sh 自己写的 pid 文件(cpu 采样器等)再按记录治理一轮;
        # 未记录 starttime 的先补记(读 /proc),校验不过就不杀
        for asset, sub in ((ASSETS["test1"], "server"), (ASSETS["test2"], "client")):
            pids = self.tm.exec_sh(
                asset["id"],
                f"for f in {self.rdir}/{sub}/*.pid; do [ -f \"$f\" ] || continue; "
                f"echo \"$(cat $f) $(basename $f)\"; done",
                name=f"relist-pids-{sub}", check=False)
            for ln in pids.strip().splitlines():
                parts = ln.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    continue
                pid, pname = int(parts[1]), parts[0]
                key = f"{sub}/{pname}"
                if any(r[2] == pid for r in self.roles):
                    continue
                ident = self.tm.exec_sh(
                    asset["id"],
                    f"p={pid}; echo exe=$(readlink /proc/$p/exe 2>/dev/null); "
                    f"awk '{{print $22}}' /proc/$p/stat 2>/dev/null",
                    name=f"ident2-{key}", check=False)
                governed_kill(self.tm, asset, pid, ident, key)
        time.sleep(3)   # 端口释放余量(下一轮预检兜底)

    def run(self) -> dict:
        preflight_round(self.tm)
        try:
            self.start_server()
            self.apply_affinity()
            self.start_client()
            rc = self.run_drive()
            self.diagnostics()
            ev = self.collect()
            return {"label": self.cfg.label, "drive_exit": rc, "evidence": ev,
                    "proto": self.cfg.proto, "mux": self.cfg.mux}
        finally:
            self.stop_all()


# ---------------------------------------------------------------- run-index

def index_load(out_dir: str) -> dict:
    p = os.path.join(out_dir, "run-index.json")
    if os.path.isfile(p):
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    return {"schema": SCHEMA, "version": SCHEMA_VERSION, "rounds": [], "e2e": {},
            "created_at": now_iso()}


def index_save(out_dir: str, idx: dict) -> None:
    idx["updated_at"] = now_iso()
    with open(os.path.join(out_dir, "run-index.json"), "w", encoding="utf-8") as fh:
        json.dump(idx, fh, ensure_ascii=False, indent=2, sort_keys=True)


# ---------------------------------------------------------------- 源同步

def sync_source(tm: Tm, progress: Progress, out_dir: str, snap_dir: str,
                mode: str, stamp: str) -> str:
    """打包快照 → 三机独立目录;远端重算 build-inputs 与本地逐项匹配。"""
    remote_root = remote_root_for(mode, stamp)
    tar_path = os.path.join(out_dir, f"source-{mode}-{stamp}.tar")
    with tarfile.open(tar_path, "w") as tf:
        for rel in enumerate_workspace_files(snap_dir):
            tf.add(os.path.join(snap_dir, rel), arcname=rel, recursive=False)
    tar_sha = sha256_file(tar_path)
    local_inputs = os.path.join(out_dir, "build")
    meta = json.load(open(os.path.join(local_inputs, "build-inputs.json"),
                          encoding="utf-8"))
    # 工具本体独立哈希:baseline 的 git archive 不含新 runner,快照里的
    # benchmark/closure_c84.py 来自 harness 叠加;远端实际用的工具必须与
    # 本文件逐字节一致(工具不参与 build-inputs/生产编译输入)。
    tool_sha = sha256_file(os.path.abspath(__file__))
    for name, asset in (("test1", ASSETS["test1"]), ("test2", ASSETS["test2"]),
                        ("test3", ASSETS["test3"])):
        tm.exec_sh(asset["id"], f"mkdir -p {remote_root} && chmod 700 {remote_root}",
                   name=f"mkdir-{name}")
        tm.upload(asset["id"], tar_path, f"{remote_root}/src.tar", name=f"ul-{name}")
        got = tm.exec_sh(asset["id"], f"sha256sum {remote_root}/src.tar | awk '{{print $1}}'",
                         name=f"sha-{name}").strip()
        if got != tar_sha:
            die(f"{name} 源包哈希不一致: {got} != {tar_sha}")
        tm.exec_sh(asset["id"],
                   f"cd {remote_root} && rm -rf tree && mkdir tree && "
                   f"tar xf src.tar -C tree && "
                   f"python3 tree/benchmark/closure_c84.py _internal-build-inputs-check "
                   f"--tree tree --expect {shlex.quote(meta['tree_hash'])}",
                   name=f"verify-inputs-{name}")
        tool_got = tm.exec_sh(
            asset["id"],
            f"sha256sum {remote_root}/tree/benchmark/closure_c84.py | awk '{{print $1}}'",
            name=f"toolsha-{name}").strip()
        if tool_got != tool_sha:
            die(f"{name} 远端工具哈希与本编排器不一致: {tool_got} != {tool_sha}"
                f"(叠加/传输损坏,拒绝构建)")
        progress.event("源已同步并逐项核对",
                       f"{name} build-inputs tree_hash 与工具哈希均一致")
    return remote_root


def pull_build_artifacts(tm: Tm, progress: Progress, out_dir: str,
                         remote_root: str) -> None:
    """回收远端 build 清单。**比较先于拷贝**:本地 build/build-inputs.sha256
    是快照初始清单,先与远端生成的版本逐字节比较(远端源/迁移被改动则非零
    中止),通过后才允许拷入 sources/binaries 等其余产物。"""
    a = ASSETS["test1"]
    dst_remote = os.path.join(out_dir, "build-remote")
    tm.download(a["id"], f"{remote_root}/build", dst_remote, name="dl-build")
    local_manifest = os.path.join(out_dir, "build", "build-inputs.sha256")
    remote_manifest = os.path.join(dst_remote, "build-inputs.sha256")
    if not os.path.isfile(remote_manifest):
        die(f"远端缺 build/build-inputs.sha256(构建清单未生成?)")
    a_txt = open(local_manifest, encoding="utf-8").read()
    b_txt = open(remote_manifest, encoding="utf-8").read()
    if a_txt != b_txt:
        die("远端 build-inputs.sha256 与本地初始清单不一致"
            "(远端源/编译输入在构建前后被改动,拒绝)")
    for f in ("sources.sha256", "binaries.json", "build-inputs.sha256",
              "build-inputs.json"):
        src = os.path.join(dst_remote, f)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(out_dir, "build", f))
    if os.path.isdir(os.path.join(dst_remote, "bin")):
        shutil.copytree(os.path.join(dst_remote, "bin"),
                        os.path.join(out_dir, "build", "bin"), dirs_exist_ok=True)
    progress.event("远端构建清单已回收(先比较后拷贝)", "build-inputs 逐字节一致")


# ---------------------------------------------------------------- E2E

def run_e2e_all(tm: Tm, progress: Progress, out_dir: str, remote_root: str,
                 idx: dict) -> None:
    """四套 E2E 逐套执行(相对 BIN 目录,真实源码根执行),原始 log/exit
    下载为本地独立证明 + 脱敏副本;run-index 只存摘要,verify 重读原件。
    ★E2E 父 shell 同样 raise nofile 65535(fail-closed 预检;e2e 并发/端口
    场景在默认软限 1024 下同样会 EMFILE 污染结果)。"""
    a = ASSETS["test1"]
    fd_info = remote_fd_preflight(tm, a)
    idx["e2e_nofile"] = fd_info
    logs_local = os.path.join(out_dir, "logs")
    os.makedirs(logs_local, exist_ok=True)
    for key, script in E2E_SUITES:
        progress.event("开始 E2E 套件", key)
        rc = remote_nohup(tm, a,
                          fd_raise_snippet()
                          + f"cd {src_root(remote_root)} && bash {script} target/release",
                          remote_root, f"e2e-{key}", deadline=DEADLINES["e2e_suite"])
        for suffix in ("log", "exit"):
            tm.download(a["id"], f"{remote_root}/e2e-{key}.{suffix}",
                        os.path.join(logs_local, f"e2e-{key}.{suffix}"),
                        name=f"dl-e2e-{key}-{suffix}")
        log_path = os.path.join(logs_local, f"e2e-{key}.log")
        log = open(log_path, encoding="utf-8").read() if os.path.isfile(log_path) else ""
        with open(log_path + ".redacted", "w", encoding="utf-8") as fh:
            fh.write(redact(log))
        m = re.findall(r"PASS=(\d+) FAIL=(\d+)", log)
        idx["e2e"][key] = {"exit": rc, "script": script,
                           "pass_fail": [list(map(int, x)) for x in m],
                           "log": f"logs/e2e-{key}.log",
                           "exit_file": f"logs/e2e-{key}.exit"}
        index_save(out_dir, idx)
        if rc != 0 or any(f != 0 for _, f in (idx["e2e"][key]["pass_fail"] or [[0, 1]])):
            die(f"E2E {key} 真实失败(exit={rc}, {m}),按约定不豁免、不继续")
        progress.event("E2E 套件通过", f"{key}: {m}")


# ---------------------------------------------------------------- verify

def _parse_test_summary(log: str) -> dict:
    lines = re.findall(r"test result: (\w+)\. (\d+) passed", log)
    total = sum(int(p) for _, p in lines)
    bad = [s for s, _ in lines if s != "ok"]
    return {"suites": len(lines), "passed_total": total, "non_ok": bad}


def cmd_verify(evidence_dir: str) -> int:
    """read-only 链路核验 + 调用 verify_goal(固定门槛,不手填)。"""
    idx_path = os.path.join(evidence_dir, "run-index.json")
    if not os.path.isfile(idx_path):
        print(f"FAIL: 缺 run-index.json(不是 closure_c84 产物目录): {evidence_dir}")
        return 1
    idx = json.load(open(idx_path, encoding="utf-8"))
    if idx.get("schema") != SCHEMA:
        print(f"FAIL: run-index schema 不符: {idx.get('schema')!r}")
        return 1
    mode = idx.get("mode", "?")
    logs = os.path.join(evidence_dir, "logs")
    snap = os.path.join(evidence_dir, "source-snapshot")
    results = []

    def check(name, ok, detail=""):
        results.append((name, bool(ok), detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))

    # A. Linux test/build 真实 exit 与计数
    tw = open(os.path.join(logs, "test-workspace.exit")).read().strip() \
        if os.path.isfile(os.path.join(logs, "test-workspace.exit")) else None
    tw_log = open(os.path.join(logs, "test-workspace.log"), encoding="utf-8").read() \
        if os.path.isfile(os.path.join(logs, "test-workspace.log")) else ""
    summ = _parse_test_summary(tw_log)
    check("A1 cargo test --workspace exit==0", tw == "0" and summ["suites"] > 0
          and not summ["non_ok"], f"exit={tw} suites={summ['suites']} "
          f"passed={summ['passed_total']} non_ok={summ['non_ok'][:3]}")
    br = open(os.path.join(logs, "build-release.exit")).read().strip() \
        if os.path.isfile(os.path.join(logs, "build-release.exit")) else None
    check("A2 cargo build --release exit==0", br == "0", f"exit={br}")

    # B. 四套 E2E:读**原始 log/exit 文件**(不认 run-index 手填摘要);
    #    index 摘要与原件不一致、accept 缺原件、原件被删 → 一律 FAIL
    for key, _ in E2E_SUITES:
        e = (idx.get("e2e") or {}).get(key)
        exit_path = os.path.join(logs, f"e2e-{key}.exit")
        log_path = os.path.join(logs, f"e2e-{key}.log")
        if e is None and not os.path.isfile(exit_path):
            if mode == "accept":
                check(f"B E2E {key}", False, "accept 模式缺该套(必须逐套全跑)")
            else:
                print(f"[note] B E2E {key}:未执行(mode={mode},非 accept 不强制)")
            continue
        if not (os.path.isfile(exit_path) and os.path.isfile(log_path)):
            check(f"B E2E {key}", False,
                  f"缺原始证据 {exit_path if not os.path.isfile(exit_path) else log_path}"
                  f"(index 记录 {e})——删 log 即 FAIL,summary 写 PASS 也 FAIL")
            continue
        exit_file = open(exit_path, encoding="utf-8").read().strip()
        log_txt = open(log_path, encoding="utf-8").read()
        m = re.findall(r"PASS=(\d+) FAIL=(\d+)", log_txt)
        pairs = [list(map(int, x)) for x in m]
        exit_int = int(exit_file) if exit_file.lstrip("-").isdigit() else None
        file_ok = exit_int == 0 and pairs \
            and all(f == 0 for _, f in pairs) and any(p > 0 for p, _ in pairs)
        idx_ok = e is None or (e.get("exit") == exit_int
                               and e.get("pass_fail") == pairs)
        check(f"B E2E {key}(原件)", file_ok,
              f"exit_file={exit_file} PASS/FAIL={pairs}")
        check(f"B E2E {key}(index与原件一致)", idx_ok, f"index={e}")

    # C. build-inputs 逐项重算(快照在场)+ 跨证据一致 + 嵌入策略
    # C0. 当前源绑定:accept/targeted 的工作区快照必须仍等于**当前工作区**
    #     的构建输入哈希(证据不能靠伪快照绕过当前源绑定;baseline 绑定
    #     c84f631 导出,不与当前工作区比对)
    rule = str(idx.get("snapshot_rule") or "")
    if mode in ("accept", "targeted") and rule.startswith("workspace-deterministic"):
        try:
            live_hash = tree_hash(REPO_ROOT)
        except OSError:
            live_hash = None
        check("C0 当前工作区构建输入与快照一致(源绑定)", live_hash == idx.get("tree_hash"),
              f"live={str(live_hash)[:16]}… vs 记录={str(idx.get('tree_hash'))[:16]}…")
    else:
        print(f"[note] C0 当前源绑定跳过(mode={mode}, rule={rule[:28]}…)")
    if not os.path.isdir(snap):
        check("C build-inputs 快照存在", False, f"缺 {snap}")
        local_manifest = None
    else:
        recomputed = os.path.join(evidence_dir, ".verify-build-inputs")
        shutil.rmtree(recomputed, ignore_errors=True)
        doc = write_build_inputs(snap, recomputed,
                                 {"snapshot_rule": idx.get("snapshot_rule", "?")})
        local_manifest = os.path.join(recomputed, "build-inputs.sha256")
        check("C1 快照与记录 tree_hash 一致", doc["tree_hash"] == idx.get("tree_hash"),
              f"{doc['tree_hash'][:16]}… vs {str(idx.get('tree_hash'))[:16]}…")
        check("C2 嵌入策略 embedded_ui=false 一致", idx.get("embedded_ui") is False,
              f"embedded_ui={idx.get('embedded_ui')}")
    ev_root = os.path.join(evidence_dir, "evidence")
    manifests = set()
    for name in sorted(os.listdir(ev_root)) if os.path.isdir(ev_root) else []:
        p = os.path.join(ev_root, name, "build", "build-inputs.sha256")
        if os.path.isfile(p):
            manifests.add(open(p, encoding="utf-8").read())
    if local_manifest and os.path.isfile(local_manifest):
        manifests.add(open(local_manifest, encoding="utf-8").read())
    check("C3 build-inputs 跨证据目录完全一致(同一份源)", len(manifests) == 1,
          f"{len(manifests)} 份不同清单" if len(manifests) != 1 else "1 份")

    # D. migrations 编译输入完整性(db.rs include_str 的 000{1,2,3} 全在清单)
    if local_manifest and os.path.isfile(local_manifest):
        man = read_manifest_lines(local_manifest)
        missing = [m for m in MIN_MIGRATIONS if m not in man]
        check("D migrations 编译输入齐全", not missing, f"缺 {missing}" if missing else
              f"{len([m for m in man if m.startswith('migrations/')])} 个 sql")
    else:
        check("D migrations 编译输入齐全", False, "无本地清单可比")

    # E. bin 与所有端点一致(binaries.json == build/bin == endpoints identity)
    bin_hashes = {}
    for name in sorted(os.listdir(ev_root)) if os.path.isdir(ev_root) else []:
        bj = os.path.join(ev_root, name, "build", "binaries.json")
        if os.path.isfile(bj):
            doc = json.load(open(bj, encoding="utf-8"))
            for b, meta in (doc.get("binaries") or {}).items():
                bin_hashes.setdefault(b, set()).add(meta.get("sha256"))
    ok_e = all(len(v) == 1 for v in bin_hashes.values()) and len(bin_hashes) == 2
    detail_e = {k: sorted(v) for k, v in bin_hashes.items()}
    check("E binaries.json 跨目录单一构建", ok_e, f"{detail_e}")

    # F. 轮次完整性与作用域诚实声明
    rounds = idx.get("rounds") or []
    check("F1 全部压测轮 drive exit==0", all(r.get("drive_exit") == 0 for r in rounds)
          and rounds, f"{[(r.get('label'), r.get('drive_exit')) for r in rounds]}")
    if mode == "accept":
        want = {(p, m) for p, m in ACCEPT_MATRIX}
        got = {(r.get("proto"), int(r.get("mux", -1))) for r in rounds}
        check("F2 accept 全矩阵齐(6协议mux+tcp nomux 对照)", want <= got,
              f"缺 {sorted(want - got)}")
    else:
        print(f"[note] 模式={mode}:按设计不做 accept 全矩阵声明(verify_goal 门槛"
              f"仍按固定阈值判定,缺协议自然 FAIL)")

    # G. verify_goal(固定门槛;RH_VERIFY_REPO=快照)
    if os.path.isdir(snap):
        env = dict(os.environ)
        env["RH_VERIFY_REPO"] = snap
        try:
            rc, out, err = run_local(
                [sys.executable, os.path.join(PY_DIR, "verify_goal.py"),
                 "--evidence", ev_root], DEADLINES["verify_goal"], env=env)
        except CommandTimeout as exc:
            print(f"FAIL: verify_goal 超时: {exc}")
            rc, out = 1, b""
        report = out.decode("utf-8", "replace")
        with open(os.path.join(evidence_dir, "verify-goal-report.txt"), "w",
                  encoding="utf-8") as fh:
            fh.write(report)
        for ln in report.splitlines():
            if ln.startswith("[PASS]") or ln.startswith("[FAIL]") or ln.startswith("总体"):
                print("  " + ln)
        check("G verify_goal 固定门槛", rc == 0, f"exit={rc}")
    else:
        check("G verify_goal 固定门槛", False, "无源快照,源-构建绑定不可验")

    n_fail = sum(1 for _, ok, _ in results if not ok)
    print(f"\n== closure verify 汇总: {len(results) - n_fail}/{len(results)} 项通过 "
          f"(mode={mode}) ==")
    print(f"(verify_goal 证据根: {ev_root};工作流应使用 "
          f"`--evidence {ev_root}` 调用 py/verify_goal.py)")
    return 0 if n_fail == 0 else 1


# ---------------------------------------------------------------- self-test(本地,无远端)

def _fx_lab(root: str, samples: int = 3) -> str:
    """借用 verify_goal 的合成证据构造器搭一个完整 out-dir 装置。"""
    import verify_goal as vg
    out = os.path.join(root, "out")
    ev_root = os.path.join(out, "evidence")
    os.makedirs(ev_root, exist_ok=True)
    vg.build_good_set(ev_root, samples=samples)
    # 源快照 = fx-repo + migrations + rust-toolchain(让 build-inputs 口径可验)
    snap = os.path.join(out, "source-snapshot")
    repo = vg._fx_repo(root)
    shutil.copytree(repo, snap, dirs_exist_ok=True)
    os.makedirs(os.path.join(snap, "migrations"), exist_ok=True)
    for m in MIN_MIGRATIONS:
        rel = os.path.basename(m)
        with open(os.path.join(snap, "migrations", rel), "w", encoding="utf-8") as fh:
            fh.write(f"-- fx migration {rel}\n")
    with open(os.path.join(snap, "rust-toolchain.toml"), "w", encoding="utf-8") as fh:
        fh.write('[toolchain]\nchannel = "fx"\n')
    doc = write_build_inputs(snap, os.path.join(out, "build"),
                             {"snapshot_rule": "self-test"})
    logs = os.path.join(out, "logs")
    os.makedirs(logs, exist_ok=True)
    with open(os.path.join(logs, "test-workspace.exit"), "w") as fh:
        fh.write("0\n")
    with open(os.path.join(logs, "test-workspace.log"), "w") as fh:
        fh.write("test result: ok. 42 passed; 0 failed\n")
    with open(os.path.join(logs, "build-release.exit"), "w") as fh:
        fh.write("0\n")
    # 四套 E2E 的**原始** log/exit 文件(verify 只认原件,index 是摘要)
    for k, _ in E2E_SUITES:
        with open(os.path.join(logs, f"e2e-{k}.exit"), "w") as fh:
            fh.write("0\n")
        with open(os.path.join(logs, f"e2e-{k}.log"), "w") as fh:
            fh.write("PASS: fx-case-a\nPASS: fx-case-b\n\n== 结果: PASS=10 FAIL=0 ==\n")
    idx = {"schema": SCHEMA, "version": SCHEMA_VERSION, "mode": "accept",
           "snapshot_rule": "self-test", "tree_hash": doc["tree_hash"],
           "embedded_ui": False,
           "rounds": [{"label": f"{p}-mux", "proto": p, "mux": 1, "drive_exit": 0}
                      for p in PROTOS_ALL] +
                    [{"label": "tcp-nomux", "proto": "tcp", "mux": 0, "drive_exit": 0}],
           "e2e": {k: {"exit": 0, "pass_fail": [[10, 0]],
                       "log": f"logs/e2e-{k}.log", "exit_file": f"logs/e2e-{k}.exit"}
                   for k, _ in E2E_SUITES}}
    index_save(out, idx)
    # 把 build-inputs 拷进每个证据目录(模拟真实回收)
    for name in os.listdir(ev_root):
        dst = os.path.join(ev_root, name, "build")
        os.makedirs(dst, exist_ok=True)
        for f in ("build-inputs.sha256", "build-inputs.json"):
            shutil.copy2(os.path.join(out, "build", f), os.path.join(dst, f))
    return out


def cmd_self_test() -> int:
    import verify_goal as vg

    def expect(name, fn):
        """fn() 返回 True = 行为符合预期(正例通过/负例被抓);异常=不符。"""
        try:
            got = fn()
        except Exception as exc:  # noqa: BLE001
            print(f"  [{name}] 异常: {type(exc).__name__}: {exc}")
            got = False
        ok = bool(got)
        print(f"  [{'ok' if ok else 'BAD'}] {name}")
        return ok

    checks = []

    # 单元:脱敏
    def _t_redact():
        s = 'curl -H "Authorization: Bearer aa.bbb.ccc" / "secret":"TOPSECRET" secret = "abc"\npeer-Pass-1'
        r = redact(s)
        return ("TOPSECRET" not in r and "abc" not in r.split("secret =")[0]
                and "peer-Pass" not in r and "Bearer <redacted>" in r)
    checks.append(expect("脱敏:secret/token/密码不出现在输出", _t_redact))

    # 单元:构建输入枚举(migrations + toolchain 收录;dist 排除)
    def _t_enum():
        with tempfile.TemporaryDirectory() as td:
            tree = os.path.join(td, "tree")
            os.makedirs(os.path.join(tree, "crates", "x/src"))
            os.makedirs(os.path.join(tree, "migrations"))
            os.makedirs(os.path.join(tree, "console", "dist"))
            open(os.path.join(tree, "Cargo.toml"), "w").write("[workspace]\n")
            open(os.path.join(tree, "crates", "x", "Cargo.toml"), "w").write("x\n")
            open(os.path.join(tree, "crates", "x", "src", "m.rs"), "w").write("fn m(){}\n")
            for m in MIN_MIGRATIONS:
                open(os.path.join(tree, m), "w").write("--\n")
            open(os.path.join(tree, "rust-toolchain.toml"), "w").write('[toolchain]\nchannel="fx"\n')
            open(os.path.join(tree, "console", "dist", "index.html"), "w").write("<i>\n")
            got = enumerate_build_inputs(tree)
            return (all(m in got for m in MIN_MIGRATIONS)
                    and "rust-toolchain.toml" in got
                    and "crates/x/src/m.rs" in got
                    and not any(x.startswith("console/") for x in got))
    checks.append(expect("枚举:migrations/toolchain 收录、dist 排除", _t_enum))

    # 负例:缺 migration 清单必须被 verify 抓住
    def _t_missing_migration():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            for name in os.listdir(os.path.join(out, "evidence")):
                p = os.path.join(out, "evidence", name, "build", "build-inputs.sha256")
                lines = [ln for ln in open(p, encoding="utf-8").read().splitlines()
                         if "0002_client_quotas" not in ln]
                open(p, "w", encoding="utf-8").write("\n".join(lines) + "\n")
            return cmd_verify(out) != 0
    checks.append(expect("负例:缺 migration(0002)→ verify FAIL", _t_missing_migration))

    # 负例:失败 E2E —— **原始 log/exit 文件**真实失败(非手填 index)
    def _t_failed_e2e():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            lg = os.path.join(out, "logs")
            open(os.path.join(lg, "e2e-transports.exit"), "w").write("1\n")
            open(os.path.join(lg, "e2e-transports.log"), "w").write(
                "PASS: a\nFAIL: b\n\n== 结果: PASS=3 FAIL=2 ==\n")
            return cmd_verify(out) != 0
    checks.append(expect("负例:E2E 原件真实失败 → verify FAIL", _t_failed_e2e))

    # 负例:E2E 原件被删(index 仍写 PASS)→ 删 log 即 FAIL
    def _t_e2e_log_deleted():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            os.remove(os.path.join(out, "logs", "e2e-resilience.log"))
            return cmd_verify(out) != 0
    checks.append(expect("负例:E2E log 被删(index 写 PASS 也 FAIL)", _t_e2e_log_deleted))

    # 负例:index 摘要与原件不一致(手改 index 冒充)
    def _t_e2e_index_forged():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            idx = index_load(out)
            idx["e2e"]["relay-modes"]["pass_fail"] = [[99, 0]]
            index_save(out, idx)
            return cmd_verify(out) != 0
    checks.append(expect("负例:E2E index 与原件不一致 → FAIL", _t_e2e_index_forged))

    # 负例:工作区快照规则却绑定不上当前源(伪快照绕过当前源绑定)
    def _t_live_binding():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            idx = index_load(out)
            idx["snapshot_rule"] = "workspace-deterministic-v1"  # 声明来自工作区
            index_save(out, idx)
            # fixture 快照是合成树,当前真实工作区哈希必然不同 → C0 必 FAIL
            return cmd_verify(out) != 0
    checks.append(expect("负例:伪工作区快照(当前源绑定失败)→ FAIL", _t_live_binding))

    # 负例:旧哈希(快照在清单生成后被篡改)
    def _old_hash_wrapper():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            rs = os.path.join(out, "source-snapshot", "crates")
            for dirpath, _, files in os.walk(rs):
                for f in files:
                    if f.endswith(".rs"):
                        with open(os.path.join(dirpath, f), "a") as fh:
                            fh.write("// tampered\n")
                        return cmd_verify(out) != 0
        return False
    checks.append(expect("负例:源被改(旧哈希)→ verify FAIL", _old_hash_wrapper))

    # 证据层负例(借 verify_goal 判定力,经我的 verify 入口必须传导为非零)
    def _ev_case(mutate):
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td)
            mutate(os.path.join(out, "evidence"))
            return cmd_verify(out) != 0

    def _m_busy(ev_root):
        # busy 样本:同 key 重复时间窗(克隆样本冒充独立测量)
        import glob
        f = sorted(glob.glob(os.path.join(ev_root, "tcp-mux", "results-*.jsonl")))[0]
        lines = open(f, encoding="utf-8").read().splitlines()
        dup = None
        for ln in lines:
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            if r.get("metric") == "throughput":
                dup = ln
                break
        if dup is None:
            raise RuntimeError("fixture 无 throughput 记录")
        open(f, "a", encoding="utf-8").write(dup + "\n")

    def _m_missing_proto(ev_root):
        shutil.rmtree(os.path.join(ev_root, "kcp-mux"))

    def _m_wrong_direction(ev_root):
        import glob
        d = os.path.join(ev_root, "tcp-mux")
        f = sorted(glob.glob(os.path.join(d, "results-*.jsonl")))[0]
        recs = [json.loads(ln) for ln in open(f, encoding="utf-8")]
        victim = next(r for r in recs if r.get("metric") == "throughput"
                      and r.get("dir") == "upload" and r.get("path") == "tunnel")
        rawp = os.path.join(d, victim["raw_json"])   # raw 相对证据目录本身
        raw = json.load(open(rawp, encoding="utf-8"))
        raw["start"]["test_start"]["reverse"] = 1   # 声明 upload 实测 download
        json.dump(raw, open(rawp, "w", encoding="utf-8"))

    def _m_handshake_only(ev_root):
        import glob
        for f in glob.glob(os.path.join(ev_root, "*", "results-*.jsonl")):
            lines = open(f, encoding="utf-8").read().splitlines()
            out = []
            for ln in lines:
                r = json.loads(ln)
                if r.get("metric") == "concurrent":
                    for k in ("hold_alive", "hold_rounds", "alive_by_round",
                              "initial_verified"):
                        r.pop(k, None)      # 只剩握手口径
                out.append(json.dumps(r))
            open(f, "w", encoding="utf-8").write("\n".join(out) + "\n")

    checks.append(expect("负例:busy 样本(重复时间窗)→ FAIL",
                         lambda: _ev_case(_m_busy)))
    checks.append(expect("负例:缺协议(kcp 目录被删)→ FAIL",
                         lambda: _ev_case(_m_missing_proto)))
    checks.append(expect("负例:错方向(raw reverse 与记录不符)→ FAIL",
                         lambda: _ev_case(_m_wrong_direction)))
    checks.append(expect("负例:仅握手并发(缺保持证据)→ FAIL",
                         lambda: _ev_case(_m_handshake_only)))

    def _two_pairs():
        with tempfile.TemporaryDirectory() as td:
            out = _fx_lab(td, samples=2)
            return cmd_verify(out) != 0
    checks.append(expect("负例:不足 3 对(samples=2)→ FAIL", _two_pairs))

    # 正例:完好装置必须通过(防止负例把正路也堵死)
    def _positive():
        with tempfile.TemporaryDirectory() as td:
            return cmd_verify(_fx_lab(td)) == 0
    checks.append(expect("正例:完好装置 verify 通过", _positive))

    ok = all(checks)
    print(f"== closure_c84 self-test: {'PASS' if ok else 'FAIL'} "
          f"({sum(checks)}/{len(checks)}) ==")
    return 0 if ok else 1


# ---------------------------------------------------------------- 子命令:baseline / accept / targeted

def _common_setup(args, mode: str):
    out_dir = os.path.abspath(args.out_dir)
    if os.path.exists(out_dir) and os.listdir(out_dir):
        die(f"--out-dir 非空目录(不覆盖既有证据): {out_dir}")
    os.makedirs(out_dir, exist_ok=True)
    progress = Progress(out_dir)
    tm = Tm(progress, os.path.join(out_dir, "logs"))
    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    idx = index_load(out_dir)
    idx.update({"mode": mode, "started_at": now_iso(),
                "baseline_commit": BASELINE_COMMIT if mode == "baseline" else None,
                "tool": os.path.abspath(__file__),
                "tool_sha256": sha256_file(os.path.abspath(__file__))})
    return out_dir, progress, tm, stamp, idx


def _quiesce_check(repo: str, progress: Progress) -> str:
    """静止校验 = **内容哈希**前后一致(git status 文本相同不能证明文件内容
    未动:已标 M 的文件再改一次 status 仍相同),git status 作辅助信号。"""
    h1 = tree_hash(repo, enumerate_workspace_files(repo))
    st1 = run_local(["git", "-C", repo, "status", "--porcelain"], 120)[1]
    time.sleep(5)
    h2 = tree_hash(repo, enumerate_workspace_files(repo))
    st2 = run_local(["git", "-C", repo, "status", "--porcelain"], 120)[1]
    if h1 != h2:
        die(f"工作区未静止(快照收录文件内容哈希 5s 内变化: {h1[:12]}→{h2[:12]}):"
            f"先让并行任务完成再验收")
    if st1 != st2:
        die("工作区未静止(git status 5s 内发生变化)")
    head = run_local(["git", "-C", repo, "rev-parse", "HEAD"], 120)[1].decode().strip()
    progress.event("工作区静止校验通过(内容哈希)", f"HEAD={head} hash={h1[:16]}…")
    return head


def _prepare_source(args, out_dir, progress, mode: str):
    snap = os.path.join(out_dir, "source-snapshot")
    if mode == "baseline":
        meta = snapshot_git_archive(REPO_ROOT, snap, BASELINE_COMMIT)
        progress.event("基线源快照完成", f"git archive {BASELINE_COMMIT}")
    else:
        head = _quiesce_check(REPO_ROOT, progress)
        meta = snapshot_workspace(REPO_ROOT, snap)
        progress.event("候选源快照完成", f"{meta['file_count']} 文件(确定性规则)")
    toolchain = remote_toolchain_pinned(snap) or "stable"
    doc = write_build_inputs(snap, os.path.join(out_dir, "build"),
                             {"snapshot_rule": meta["rule"], "git_head": meta.get("git_head"),
                              "toolchain_channel": toolchain})
    progress.event("构建输入清单完成",
                   f"{doc['file_count']} 项 tree_hash={doc['tree_hash'][:16]}… "
                   f"(含 migrations {len(doc['migrations'])} 个)")
    return snap, meta, doc, toolchain


def _run_rounds(args, out_dir, progress, tm, remote_root, idx, matrix, mode):
    for proto, mux in matrix:
        label = f"{mode}-{proto}-mux{mux}"
        run_id = f"c84-{mode}-{proto}-mux{mux}-{int(time.time())}"
        cfg = RoundCfg(proto, mux, label, run_id,
                       repeat=getattr(args, "repeat", 3),
                       duration=getattr(args, "duration", 10),
                       conc_levels=getattr(args, "conc_levels", None) or
                       ("64" if mode == "baseline" else "64 256 1024 4096"))
        runner = RoundRunner(tm, progress, out_dir, remote_root, cfg)
        rec = runner.run()
        idx["rounds"].append(rec)
        index_save(out_dir, idx)
        if rec["drive_exit"] != 0:
            die(f"压测轮真实失败(label={label} exit={rec['drive_exit']}),按约定停止")
        progress.event("压测轮完成", label)
    idx["matrix_complete"] = True
    index_save(out_dir, idx)


def cmd_baseline(args) -> int:
    out_dir, progress, tm, stamp, idx = _common_setup(args, "baseline")
    for name, asset in ASSETS.items():
        info = check_remote_env(tm, asset)
        idx.setdefault("env", {})[name] = info
    index_save(out_dir, idx)
    snap, meta, doc, toolchain = _prepare_source(args, out_dir, progress, "baseline")
    idx.update({"snapshot_rule": meta["rule"], "git_head": meta.get("git_head"),
                "tree_hash": doc["tree_hash"], "embedded_ui": doc["embedded_ui"],
                "toolchain_channel": toolchain})
    index_save(out_dir, idx)
    remote_root = sync_source(tm, progress, out_dir, snap, "baseline", stamp)
    idx["remote_root"] = remote_root
    index_save(out_dir, idx)
    build = remote_build(tm, out_dir, remote_root, toolchain)
    idx.update({"build": build})
    index_save(out_dir, idx)
    # 远端 build/ 清单回传:比较先于拷贝(远端源/编译输入被改动即非零中止)
    pull_build_artifacts(tm, progress, out_dir, remote_root)
    progress.event("targeted 基线矩阵开始",
                   "TCP nomux / TLS mux / KCP mux / QUIC mux(单流双向×3,16流随 drive 留诊断)")
    _run_rounds(args, out_dir, progress, tm, remote_root, idx,
                BASELINE_TARGETED, "baseline")
    idx["scope"] = ("targeted-baseline:仅 4 配置单流双向;不声明全矩阵/门槛通过;"
                    "16 流数据为诊断附带")
    index_save(out_dir, idx)
    progress.event("baseline 完成", out_dir)
    return 0


def cmd_accept(args) -> int:
    out_dir, progress, tm, stamp, idx = _common_setup(args, "accept")
    for name, asset in ASSETS.items():
        info = check_remote_env(tm, asset)
        idx.setdefault("env", {})[name] = info
    index_save(out_dir, idx)
    snap, meta, doc, toolchain = _prepare_source(args, out_dir, progress, "accept")
    idx.update({"snapshot_rule": meta["rule"], "git_head": meta.get("git_head"),
                "tree_hash": doc["tree_hash"], "embedded_ui": doc["embedded_ui"],
                "toolchain_channel": toolchain,
                "git_status_sha256": meta.get("git_status_sha256")})
    index_save(out_dir, idx)
    remote_root = sync_source(tm, progress, out_dir, snap, "accept", stamp)
    idx["remote_root"] = remote_root
    index_save(out_dir, idx)
    build = remote_build(tm, out_dir, remote_root, toolchain)
    idx.update({"build": build})
    index_save(out_dir, idx)
    pull_build_artifacts(tm, progress, out_dir, remote_root)
    # logs(test/build)逐文件回收本地(verify 按 exit/计数核对;真实 exit 不豁免)
    lg = os.path.join(out_dir, "logs")
    os.makedirs(lg, exist_ok=True)
    for f in ("test-workspace.log", "test-workspace.exit",
              "build-release.log", "build-release.exit"):
        tm.download(ASSETS["test1"]["id"], f"{remote_root}/logs/{f}",
                    os.path.join(lg, f), name=f"dl-log-{f}")
    run_e2e_all(tm, progress, out_dir, remote_root, idx)
    progress.event("accept 全矩阵开始", "6协议 mux×{1,4,16}×双向×≥3 + tcp nomux 对照;"
                   "1200B UDP×≥3;4096 verified+保持")
    _run_rounds(args, out_dir, progress, tm, remote_root, idx, ACCEPT_MATRIX, "accept")
    idx["scope"] = "full-accept:六协议+tcp nomux 对照,四 E2E 全过,同一份源/二进制"
    index_save(out_dir, idx)
    progress.event("accept 完成", f"{out_dir}(verify 由编排脚本另行调用)")
    return 0


FORBIDDEN_KNOB_PAT = re.compile(r"RCVBUF|SNDBUF|sysctl|net\.", re.I)


def production_input_files(tree: str) -> list:
    """生产构建输入 = build inputs 排除集成测试源(release 二进制不编译
    crates/**/tests/*.rs;test manifest 属于新一轮验收,不得由 reuse 冒充)。"""
    return [f for f in enumerate_build_inputs(tree)
            if not (f.startswith("crates/") and "/tests/" in f)]


def _reuse_build(tm: Tm, progress: Progress, out_dir: str, prev_dir: str,
                 harness_patch: str = None) -> str:
    """targeted --reuse-build:绑定上一轮 accept 的源/二进制/远端目录做有界诊断。
    校验链(任何一环不符即拒,绝不拿旧 hash 冒充新构建):
      * prev run-index 必须是 accept 且有 tree_hash + build.binaries;
      * prev source-snapshot 重算 build-inputs tree_hash == 记录值;
      * 当前工作区构建输入 hash == 记录值(实现变了就必须重新编译,不接受复用);
      * 三机远端 tree 仍在且 tree_hash 一致;远端二进制 sha256 == 记录值。
    --harness-patch FILE 仅覆盖远端 benchmark/py/ 下的测试工具件(不参与
    build-inputs;观测性改动,SHA 记录在 run-index),产品二进制不动。
    本目录**不含**任何新 test/E2E manifest(scope=targeted-reuse)。"""
    prev = os.path.abspath(prev_dir)
    pidx_path = os.path.join(prev, "run-index.json")
    if not os.path.isfile(pidx_path):
        die(f"--reuse-build:缺 {pidx_path}(不是可复用的产物目录)")
    pidx = json.load(open(pidx_path, encoding="utf-8"))
    if pidx.get("mode") != "accept" or not pidx.get("tree_hash"):
        die(f"--reuse-build:仅接受 mode=accept 的产物(得到 mode={pidx.get('mode')!r})")
    want_tree = pidx["tree_hash"]
    want_bins = ((pidx.get("build") or {}).get("binaries")) or {}
    if set(want_bins) != {"rh-server", "rh-client"}:
        die(f"--reuse-build:prev build.binaries 不完整: {want_bins}")
    snap = os.path.join(prev, "source-snapshot")
    if not os.path.isdir(snap) or tree_hash(snap) != want_tree:
        die("--reuse-build:prev source-snapshot 缺失或 tree_hash 不符(证据不完整,拒绝)")
    # 生产源绑定:排除 crates/**/tests/** 后逐文件比对(release 二进制不含
    # 集成测试源);test-only 差异如实记录,任何生产源差异都必须重新编译
    prev_prod = {f: sha256_file(os.path.join(snap, f))
                 for f in production_input_files(snap)}
    live_prod = {f: sha256_file(os.path.join(REPO_ROOT, f))
                 for f in production_input_files(REPO_ROOT)}
    prod_changed = sorted(k for k in prev_prod
                          if prev_prod.get(k) != live_prod.get(k))
    test_only_delta = []
    if prod_changed:
        die(f"--reuse-build:生产源已变化({len(prod_changed)} 个文件,如 "
            f"{prod_changed[:3]})——实现变化必须重新编译,不得复用旧构建")
    # 记录 test-only 差异(允许,但必须可见;不得冒充 test manifest)
    all_prev = read_manifest_lines(os.path.join(prev, "build",
                                                "build-inputs.sha256")) \
        if os.path.isfile(os.path.join(prev, "build", "build-inputs.sha256")) \
        else {f: sha256_file(os.path.join(snap, f))
              for f in enumerate_build_inputs(snap)}
    for f, h in all_prev.items():
        fp = os.path.join(REPO_ROOT, f)
        if os.path.isfile(fp) and sha256_file(fp) != h:
            test_only_delta.append(f)
    bad_delta = [f for f in test_only_delta
                 if not (f.startswith("crates/") and "/tests/" in f)]
    if bad_delta:
        die(f"--reuse-build:工作区与 prev 存在非 test-only 差异: {bad_delta[:3]}")
    remote_root = pidx.get("remote_root") or ""
    if not remote_root.startswith("/tmp/rh-closure-c84-"):
        die(f"--reuse-build:prev remote_root 异常: {remote_root!r}")
    for name, asset in ASSETS.items():
        tm.exec_sh(asset["id"],
                   f"cd {remote_root} && python3 tree/benchmark/closure_c84.py "
                   f"_internal-build-inputs-check --tree tree "
                   f"--expect {shlex.quote(want_tree)}",
                   name=f"reuse-inputs-{name}")
    for n, want in want_bins.items():
        got = tm.exec_sh(ASSETS["test1"]["id"],
                         f"sha256sum {src_root(remote_root)}/target/release/{n} "
                         f"| awk '{{print $1}}'", name=f"reuse-sha-{n}").strip()
        if got != want:
            die(f"--reuse-build:远端 {n} sha256 {got[:12]}… ≠ 记录 {want[:12]}…")
        got2 = tm.exec_sh(ASSETS["test2"]["id"],
                          f"sha256sum {src_root(remote_root)}/bin/{n} "
                          f"| awk '{{print $1}}'", name=f"reuse-sha2-{n}").strip()
        if got2 != want:
            die(f"--reuse-build:test2 {n} sha256 不符")
    progress.event("reuse-build 绑定通过",
                   f"tree_hash={want_tree[:16]}… binaries 已核对(未重新编译)")
    # 诊断目录自带 prev 的 build 证据副本(本地拷贝;raw 与 bin 均在)
    prev_build = os.path.join(prev, "build")
    if os.path.isdir(prev_build):
        shutil.copytree(prev_build, os.path.join(out_dir, "build"),
                        dirs_exist_ok=True)
    patch_sha = None
    if harness_patch:
        base = os.path.basename(harness_patch)
        if not base.endswith(".py") or "/" in base:
            die(f"--harness-patch 只接受 benchmark/py/ 下的单个 .py 文件名: {base}")
        patch_sha = sha256_file(harness_patch)
        for name, asset in ASSETS.items():
            tm.upload(asset["id"], harness_patch,
                      f"{src_root(remote_root)}/benchmark/py/{base}",
                      name=f"harness-patch-{name}")
        progress.event("harness 观测性补丁已覆盖(仅测试工具件)",
                       f"{base} sha256={patch_sha[:16]}…(build-inputs 不受影响)")
    idx = index_load(out_dir)
    idx.update({
        "reuse_build": {"source": prev, "tree_hash": want_tree,
                        "binaries": want_bins, "remote_root": remote_root,
                        "test_only_delta": test_only_delta,
                        "harness_patch": {"file": base if harness_patch else None,
                                          "sha256": patch_sha}},
        "tree_hash": want_tree, "embedded_ui": pidx.get("embedded_ui"),
        "snapshot_rule": f"reuse-of:{os.path.basename(prev)}",
        "remote_root": remote_root,
    })
    index_save(out_dir, idx)
    return remote_root


def cmd_targeted(args) -> int:
    spec_path = args.spec or TARGETED_DEFAULT_SPEC
    if not os.path.isfile(spec_path):
        die(f"targeted 需要 --spec FILE(或默认 {TARGETED_DEFAULT_SPEC});"
            f"没有上一份候选诊断描述时不猜")
    spec = json.load(open(spec_path, encoding="utf-8"))
    for key in ("name", "proto", "mux", "knobs"):
        if key not in spec:
            die(f"targeted spec 缺字段 {key}: {spec_path}")
    for k in spec["knobs"]:
        if FORBIDDEN_KNOB_PAT.search(k):
            die(f"targeted 拒绝非用户态旋钮 {k}(红线:不动 sysctl/NIC/SO_*BUF)")
    if spec.get("server_affinity") is not None             and not re.fullmatch(r"[0-9][0-9,-]*", str(spec["server_affinity"])):
        die(f"targeted server_affinity 只允许 CPU 列表(如 0-3): "
            f"{spec.get('server_affinity')!r}")
    out_dir, progress, tm, stamp, idx = _common_setup(args, "targeted")
    idx.update({"targeted_spec": spec, "spec_path": os.path.abspath(spec_path)})
    index_save(out_dir, idx)
    reuse = bool(getattr(args, "reuse_build", None))
    if reuse:
        remote_root = _reuse_build(tm, progress, out_dir, args.reuse_build,
                                   getattr(args, "harness_patch", None))
        idx["scope"] = ("targeted-reuse:复用已核验 accept 构建做有界诊断;"
                        "不含新 test/E2E manifest,不冒充 full")
        index_save(out_dir, idx)
    else:
        snap, meta, doc, toolchain = _prepare_source(args, out_dir, progress, "accept")
        idx.update({"snapshot_rule": meta["rule"], "git_head": meta.get("git_head"),
                    "tree_hash": doc["tree_hash"], "embedded_ui": doc["embedded_ui"]})
        index_save(out_dir, idx)
        remote_root = sync_source(tm, progress, out_dir, snap, "targeted", stamp)
        idx["remote_root"] = remote_root
        index_save(out_dir, idx)
        build = remote_build(tm, out_dir, remote_root, toolchain)
        idx.update({"build": build})
        index_save(out_dir, idx)
        pull_build_artifacts(tm, progress, out_dir, remote_root)
    # 旋钮取值集合的笛卡尔积 → 有限 A/B 臂
    keys = sorted(spec["knobs"])
    arms = [{}]
    for k in keys:
        arms = [dict(a, **{k: v}) for a in arms for v in spec["knobs"][k]]
    proto, mux = spec["proto"], int(spec["mux"])
    # ★实测(h1 attempt2):peer.sh label 只允许 [a-zA-Z0-9._-],"k=v" 里的
    # '=' 会被 drive 拒绝 → 臂标签/文件名统一消毒("="→"-")
    def _sanitize(s: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]", "-", str(s))
    for arm in arms:
        tag = _sanitize("-".join(f"{k}={arm[k]}" for k in keys)) or "default"
        label = f"targeted-{_sanitize(spec['name'])}-{tag}"
        cfg = RoundCfg(proto, mux, label,
                       f"c84-targeted-{_sanitize(spec['name'])}-{tag}-{int(time.time())}",
                       repeat=int(spec.get("repeat", 3)),
                       duration=int(spec.get("duration", 10)),
                       conc_levels=spec.get("conc_levels", "64"),
                       env_extra=arm,
                       affinity=spec.get("server_affinity"))
        runner = RoundRunner(tm, progress, out_dir, remote_root, cfg)
        rec = runner.run()
        rec["arm"] = arm
        idx["rounds"].append(rec)
        index_save(out_dir, idx)
        if rec["drive_exit"] != 0:
            die(f"targeted 臂失败(label={label})")
    idx["scope"] = "targeted:有限 A/B 诊断,不冒充 full;门槛判定仍走 verify_goal"
    index_save(out_dir, idx)
    progress.event("targeted 完成", out_dir)
    return 0


# ---------------------------------------------------------------- 内部子命令(远端执行)

def cmd_internal_build_inputs(args) -> int:
    tree = os.path.abspath(args.tree)
    out = os.path.abspath(args.out)
    doc = write_build_inputs(tree, out, {"snapshot_rule": "remote-generated"})
    # 附加远端工具链实况(rustc/cargo 真实版本,构建可追溯)
    try:
        import subprocess as sp
        rustc = sp.run(["rustc", "--version"], capture_output=True, text=True,
                       timeout=30).stdout.strip()
        cargo = sp.run(["cargo", "--version"], capture_output=True, text=True,
                       timeout=30).stdout.strip()
    except OSError:
        rustc = cargo = ""
    p = os.path.join(out, "build-inputs.json")
    d = json.load(open(p, encoding="utf-8"))
    d["remote_rustc"] = rustc
    d["remote_cargo"] = cargo
    json.dump(d, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=2,
              sort_keys=True)
    print(json.dumps({"tree_hash": doc["tree_hash"], "files": doc["file_count"]}))
    return 0


def cmd_internal_build_inputs_check(args) -> int:
    tree = os.path.abspath(args.tree)
    got = tree_hash(tree)
    if got != args.expect:
        print(f"FAIL: build-inputs tree_hash 不一致: got={got} expect={args.expect}")
        return 1
    print(f"OK: build-inputs tree_hash 一致({got[:16]}…)")
    return 0


# ---------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser(
        description="c84f631 性能收口轮三机验收编排器(baseline/accept/targeted/verify)",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("baseline", help="git archive c84f631 → 构建 → targeted 4 配置")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--duration", type=int, default=10)
    p.add_argument("--conc-levels", default="64")

    p = sub.add_parser("accept", help="工作区静止快照 → 构建+四E2E → 全矩阵")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--duration", type=int, default=10)
    p.add_argument("--conc-levels", default="64 256 1024 4096")

    p = sub.add_parser("targeted", help="上一份候选诊断的有限 A/B")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--spec", help="A/B spec JSON(默认 lab/targeted-spec.json)")
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--duration", type=int, default=10)
    p.add_argument("--reuse-build", dest="reuse_build",
                   help="复用上一轮 accept 产物目录的源/二进制/远端树(有界诊断,不重新编译)")
    p.add_argument("--harness-patch",
                   help="覆盖远端 benchmark/py/<file>.py(仅测试工具件,SHA 记录)")

    p = sub.add_parser("verify", help="read-only 证据链核验 + verify_goal 固定门槛")
    p.add_argument("--evidence", required=True)

    p = sub.add_parser("_internal-build-inputs", help=argparse.SUPPRESS)
    p.add_argument("--tree", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("_internal-build-inputs-check", help=argparse.SUPPRESS)
    p.add_argument("--tree", required=True)
    p.add_argument("--expect", required=True)

    ap.add_argument("--self-test", action="store_true",
                    help="本地对抗自检(不碰远端)")

    args = ap.parse_args()
    if args.self_test:
        return cmd_self_test()
    if not args.cmd:
        ap.print_help()
        return 2
    if args.cmd == "baseline":
        return cmd_baseline(args)
    if args.cmd == "accept":
        return cmd_accept(args)
    if args.cmd == "targeted":
        return cmd_targeted(args)
    if args.cmd == "verify":
        return cmd_verify(args.evidence)
    if args.cmd == "_internal-build-inputs":
        return cmd_internal_build_inputs(args)
    if args.cmd == "_internal-build-inputs-check":
        return cmd_internal_build_inputs_check(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
