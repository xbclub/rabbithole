# vhost 专项基准:rabbithole vs frp(TCP / TLS × HTTP / HTTPS)

> 被测: rabbithole `0.4.1`(commit `d713b3b`)vs frp `v0.71.0`
> 拓扑:`benchmark/vhost-peer.sh`(与 `peer.sh` 同三机、同端口规划)
> 工具:`benchmark/vhostbench`(Go,新增)
> 轮次:`rh-tcp` / `rh-tls` / `frp-tcp` / `frp-tls`,每轮无效样本 **0**

## 1. 测什么,为什么必须新开工具

既有 `peer.sh` 的 vhost 覆盖只有一档:`16 连接 × 8s` 的明文 HTTP QPS,而且
源站 `target.py` 固定返回 **4 KiB**、没有 TLS 监听。它测不出:

- 大响应体的吞吐(4 KiB 响应在 8 连接下受 QPS 约束,不是带宽)
- HTTPS vhost(`https_port` 从未被打开过)
- 「控制面 tcp vs tls」与「vhost http vs https」两个维度的正交归因

`benchmark/vhostbench` 补的正是这些:五个子命令 `serve` / `qps` / `tput` /
`lat` / `conc`,源站是可选的 TLS、响应体大小可调,压测端走 `net/http` 但
禁用 h2 与 gzip(与被测两端的 HTTP/1.1 语义一致)。

**测试矩阵(2×2)**:

| | HTTP vhost(明文) | HTTPS vhost(SNI 透传) |
|---|---|---|
| 控制面 tcp | `rh-tcp` / `frp-tcp` | 同左 |
| 控制面 tls | `rh-tls` / `frp-tls` | 同左 |

HTTPS vhost 的语义**两侧一致**:不上传证书时 rh 与 frp 都是按 SNI 把 TLS
原样透传到客户端侧,由源站终结。所以量的是「隧道搬运加密流」,不是
「服务端终结 TLS」。

## 2. 结论先讲

### 2.1 QPS:rh 与 frp 持平,但都只剩 1/4(HTTP)~ 2/5(HTTPS)

| 控制面 | vhost | rh 直连 | rh 隧道 | 保留 | frp 直连 | frp 隧道 | 保留 | rh vs frp |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| tcp | HTTP | 24859 | **6384** | 25.7% | 22805 | 5456 | 23.9% | +17.0% |
| tcp | HTTPS | 21490 | **9660** | 45.0% | 23472 | 8254 | 35.2% | +17.0% |
| tls | HTTP | 23277 | **5736** | 24.6% | 25322 | 7362 | 29.1% | −22.1% |
| tls | HTTPS | 22649 | **8454** | 37.3% | 16140 | 8621 | 53.4% | −1.9% |

合并两个控制面后(⚠ 见下方噪声说明):

| | HTTP vhost 保留率 | HTTPS vhost 保留率 |
|---|---:|---:|
| rh | **25.2%** | **41.1%** |
| frp | **26.5%** | **44.3%** |

⚠ **`frp-tls` 的 HTTPS QPS 直连只有 16140**,明显低于其余三组的
21000~25000,是该轮基线自身偏低(p50=0.873ms,其余三组 0.5~0.57ms;
`errors=0`、`bad_status=0`,数据有效但受瞬时干扰)。它把 frp 的
HTTPS 保留率从 ~35% 抬高到 53.4%,因此**这一格不能直接比较**。
按另外三组估算 frp 的 HTTPS 保留率应在 ~35%,即与 rh 的 41.1%
差 5 个百分点以内。

**吞吐层面的差距在这里**:QPS 场景的响应体只有 4 KiB,每请求的固定开销
(隧道建流、握手、调度)占了大头,直连 24000 QPS 到隧道只剩 6000。
rh 与 frp 的保留率差 1.3 / 3.2 个百分点,**在单轮波动范围内
(rh 自己 tcp/tls 之间就差了 4.5 个百分点),不构成可判定的差异**。

### 2.2 延迟:rh 在 HTTPS 上明显更好,HTTP 上略差

| 控制面 | vhost | rh 隧道 p50 | frp 隧道 p50 | rh 相对 frp |
|---|---|---:|---:|---:|
| tcp | HTTP | 1.735 ms | 1.786 ms | −2.9% |
| tcp | HTTPS | **1.381 ms** | 1.583 ms | **−12.8%** |
| tls | HTTP | 1.887 ms | 1.657 ms | +13.9% |
| tls | HTTPS | **1.250 ms** | 1.555 ms | **−19.6%** |

合并后:HTTP rh 1.811ms vs frp 1.722ms(+5.2%,rh 略差);
HTTPS rh **1.316ms** vs frp 1.569ms(**rh 快 16%**)。

**HTTPS 侧 rh 稳定领先**,两个控制面方向一致(tcp −12.8% / tls −19.6%),
且 HTTPS 的直连基线两侧几乎相同(0.51~0.55ms),可以排除基线偏差。
HTTP 侧则是 frp 略好一件(tcp −2.9% 在噪声内,tls +13.9% 超出噪声)——
合并后 rh +5.2%,方向上 rh 略差。

### 2.3 建连:HTTPS 侧 rh 明显更快

| 控制面 | 场景 | rh | frp | rh 相对 frp |
|---|---|---:|---:|---:|
| tcp | HTTP 建连 p50 | 2.340 ms | 2.569 ms | −8.9% |
| tcp | HTTPS 建连 p50 | **6.007 ms** | 7.371 ms | **−18.5%** |
| tls | HTTP 建连 p50 | 2.567 ms | 2.288 ms | +12.2% |
| tls | HTTPS 建连 p50 | **5.826 ms** | 7.315 ms | **−20.4%** |

HTTPS 建连 p50 两侧都为 ~3.7~3.9ms 直连,隧道后 rh 只加 2.0ms,frp 加 3.5ms。

### 2.4 吞吐:两侧都接近线路,单轮噪声大,不能据此判优劣

| 控制面 | vhost | rh 直连 | rh 隧道 | 保留 | frp 直连 | frp 隧道 | 保留 |
|---|---|---:|---:|---:|---:|---:|---:|
| tcp | HTTP | 3.042 | 2.250 | 74.0% | 2.478 | 2.753 | **111.1%** |
| tcp | HTTPS | 2.685 | 2.771 | 103.2% | 2.926 | 2.929 | 100.1% |
| tls | HTTP | 2.635 | 2.254 | 85.5% | 2.892 | 2.027 | 70.1% |
| tls | HTTPS | 2.859 | 2.950 | 103.2% | 3.482 | 2.185 | 62.8% |

**`frp-tcp` 的 HTTP 档出现 111.1%(隧道比直连还快)—— 这在物理上不可能**,
说明该档的单轮噪声大于两实现的真实差异。四组里保留率从 62.8% 到 111.1%
横跨近乎一倍,不足以支撑任何"谁更快"的判断。

要判定吞吐必须 `PEER_REPEAT >= 5` 取中位数,本轮每档只有 1 次,
**这一列不作结论**。

### 2.5 并发:两侧都撑得住 4096,无差异

4 档 × 3 轮 × 4 组,全部 `alive = N/N`:

| 并发 | rh-tcp | rh-tls | frp-tcp | frp-tls |
|---|---|---|---|---|
| 64 | 64/64 | 64/64 | 64/64 | 64/64 |
| 256 | 256/256 | 256/256 | 256/256 | 256/256 |
| 1024 | 1024/1024 | 1024/1024 | 1024/1024 | 1024/1024 |
| 4096 | 4096/4096 | 4096/4096 | 4096/4096 | 4096/4096 |

存活复验是**应用层再发一个请求拿到 200**(不是只看 fd 没关),所以这是
"4096 条连接同时可用",不是"4096 个 socket 建好了"。

## 3. 差距从哪来:架构差异

rh 的 HTTP vhost 每个请求都会走一遍
`get_channel` → `START_FORWARD` → `hyper::client::conn::handshake`
(`crates/rh-server/src/vhost.rs:207-232`);

frp 的 vhost 是连接级的 muxer + 客户端侧 `httputil.ReverseProxy`,
同一个上游连接被复用。

这解释了为什么 **QPS 场景(4 KiB 小响应、每请求开销占主导)两侧都被压到
直连的 1/4**,以及为什么 **吞吐场景(4 MiB 大响应、每字节开销占主导)两侧
都能接近线路**。QPS 差距是设计选择的结果,不是同一实现的效率高低 ——
rh 用每请求新建上游通道换取了规则热更新与配额精确归因。

## 4. 测量过程中修掉的缺陷(都会造成假结论)

这部分必须留在文档里,否则数字不可信。

| # | 缺陷 | 后果 | 怎么发现的 |
|---|---|---|---|
| 1 | `conc` 存活复验只读到响应头,响应体留在 `bufio` | `hold_alive` 恒为 0 | 本机负向验证:注入"跳过响应体读取"后 256/256 → 0/64 |
| 2 | `conc` 复验总超时 5s | 4096 档直连只有 **4~322** 存活 | 三机复现;负向验证确认根因是超时不是并发度(5s→322,15s→4096) |
| 3 | TLS listener 未关 Nagle(类型断言对 `*tls.Conn` 失效) | 明文比 TLS **慢 10 倍**(p50 8.3ms vs 0.76ms) | 三机首轮 HTTP 1740 QPS vs HTTPS 18261 QPS |
| 4 | 源站端口被上一轮 `python3 target.py` 占住,新源站 bind 失败静默退出 | HTTP 明文 QPS 1769(实际测的是 Python 服务) | 日志里 `bind: address already in use` |
| 5 | `run_one` 少传 `tag` 参数,后续参数整体错位 | 所有场景打错端口 | raw 文件名与端口对不上 |
| 6 | `tput-tls` 打 4 KiB 源站 + 错域名 | 隧道吞吐 0.13 Gbps(实际 2.9) | `requests=40436 × 4KiB` vs 直连 `1002 × 4MiB` |
| 7 | `FAILS` 在 `$(...)` 子 shell 里递增 | 整轮全红仍报"无效样本: 0" | 负向验证 |
| 8 | 编排器多实例并发抢机器 | raw/results 交错 | 结果文件 label 串轮 |

现在这些都有闸门:源站进程存活 + `ss` 端口归属校验、按场景校验关键字段
(`lat` 样本数 ≥ 99%、`conc` 建成与存活 ≥ 99%、`tput` 平均响应体 ≥ 50% `--body`)、
编排器单实例锁。

## 5. 复跑

```bash
# 三机(需 git 可得 commit d713b3b)
cd /tmp/rh-bench && git fetch -q origin bench/tls-alpn && git reset -q --hard origin/bench/tls-alpn

# test1
BENCH_BIN=/tmp/rh-bin VHOST_BIN=/tmp/rh-bin/vhostbench PEER_DIR=/tmp/vh-peer \
  VHOST_IMPL=rh PROTO=tcp RELAY=splice MUX=1 VHOST_RUN_ID=rh-tcp \
  setsid bash benchmark/vhost-peer.sh server > /tmp/vh-srv.out 2>&1 &

# test2(凭据取 test1 输出)
... VHOST_IMPL=rh PROTO=tcp ... setsid bash benchmark/vhost-peer.sh client <A_IP>:17100 <cid> <secret> &

# test3
... VHOST_IMPL=rh PROTO=tcp ... setsid bash benchmark/vhost-peer.sh drive <A_IP> <B_IP> 18080 <token> <cid> rh-tcp &
```

frp 侧 `VHOST_IMPL=frp`,client 角色只需 `<A_IP>`。

`vhostbench` 是三台都要的静态二进制(三台无 Go 工具链):

```bash
cd benchmark/vhostbench && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/vhostbench-linux .
```

## 6. 已知限制

1. **吞吐列只有 1 次/档**,不足以判定优劣(见 §2.4)。要判定需
   `PEER_REPEAT>=5`。
2. **HTTPS vhost 只覆盖 SNI 透传**,不覆盖「rh 上传证书后服务端终结 TLS」
   那条路径(`vhost.rs` 的 `certs` 命中分支) —— 那是 rh 独有能力,
   没有 frp 对照,单列会更合适。
3. **源站是自签 ECDSA**,两端都 `InsecureSkipVerify`。量的是隧道,不是
   证书校验;但也意味着不覆盖证书链验证的 CPU 开销。
4. 三机是同一 10GbE 网段,RTT < 1ms,不覆盖高 RTT 下的表现。
