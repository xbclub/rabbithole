# rh-client 插件路径每请求固定开销优化

一句话结论:五处固定开销都已按需求消除,**但 http2http 短连接 QPS 没有可测提升**(改前 5539.5 → 改后 5444.7 med,-1.7%,在噪声内)。原因是这台机器上该路径的瓶颈根本不在这些固定成本上——压测窗口本身被 macOS 临时端口预算卡死,详见 §4。功能与改写语义逐字未变,`e2e-plugins.sh` 15/15 全绿。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前两轮未提交改动:compress 泵、visitor mux 中继)。
- 改动文件:`crates/rh-client/src/plugins.rs`、新增 `crates/rh-client/src/testdata/cache-test.{crt,key}`(测试证书)。
- 本机 macOS arm64 / rustc 1.98.1 / release。**非三机基准**。

## 1. 改了什么

| # | 位置 | 改前 | 改后 |
|---|---|---|---|
| 1 | `:159` 代理 CONNECT 应答 | `read_exact(&mut [0u8;1])` 逐字节循环 | `read_http_head(io, 8192)` |
| 2 | `:243` static_file 请求头 | 同上,上限 64KiB | `read_http_head(io, 64*1024)`,超限仍回 431 |
| 3 | `:596` http 改写请求头 | 同上,上限 64KiB | `read_http_head(chan, 64*1024)` |
| 4 | `:329/:331/:354` 同步 `std::fs` | `full.is_dir()` / `idx.is_file()` / `m.is_file()` | `tokio::fs::metadata(..).await` |
| 5 | `:341` 每请求 `canonicalize(&root)` | 循环内每请求一次 | 提到 `serve_static_file` 入口,只算一次 |
| 6 | `:64/:92` 每次 dial 重建 TLS | `tls_acceptor()` 读两次 pem + 建 `ServerConfig` | 进程级 `(crt,key) → Arc<TlsAcceptor>` 缓存 |
| 7 | `:615` 搬运缓冲 | `vec![0u8; 16*1024]` | `vec![0u8; 64*1024]` |

### `read_http_head` 的三处上限

按需求保持各自原值:`8192` / `64*1024` / `64*1024`。三处都保留原有错误语义:

- 超限:static_file → 431;另两处 → `Err` 上抛;
- 头未收全就 EOF → `UnexpectedEof`(旧 `read_exact` 行为);
- 返回值**含** `\r\n\r\n`,与旧实现一致(调用方按整块改写/解析)。

实现细节:一次读 4KiB,只在新到达字节附近回退 3 字节扫 `\r\n\r\n`(跨块也找得到);读取长度钳到 `limit+1`,越过即判超限——**逐字节等价于旧实现「push 第 limit+1 字节后 bail」,不会多吃数据**。

### TLS 缓存的一个取舍

`load_certs`/`load_key` 仍是同步 `fn`(需求允许「保持同步 fn 但调用方改 spawn_blocking」)。实际做法是**在缓存锁内调用**:命中缓存时完全不碰文件系统;未命中才读两次 pem,且仅首次 dial。持锁期间无 await,不会把同步阻塞扩散到其他任务。

没有改成 `spawn_blocking`,因为那会让 `tls_acceptor` 变 async,而它被 `dial_plugin` 的两处 match 分支同步调用;改成 async 的收益仅在首次 dial,不值得为此改签名。若将来出现「每次 dial 都换证书路径」的场景,再考虑 `spawn_blocking`。

## 2. 测试

### `read_http_head` 三态(`plugins.rs`)

| 测试 | 覆盖 |
|---|---|
| `read_http_head_all_at_once` | 一次到齐,且断言**只推 1 次 read** |
| `read_http_head_fragmented` | 分片 1/2/3/7/16/100 字节,拼出的头一致 |
| `read_http_head_crlf_split_across_chunks` | `\r\n\r\n` 恰好跨分片边界 |
| `read_http_head_may_include_body_bytes` | 头后紧跟的字节一并返回,不得丢 |
| `read_http_head_over_limit_errors_without_unbounded_read` | **超限必须报错**,且断言实际读取 ≤ `limit+1`、read 次数 ≤4 |
| `read_http_head_exactly_at_limit_ok` | 长度恰为 limit 的合法头不被误伤 |
| `read_http_head_eof_before_complete` | 头未收全 EOF → `UnexpectedEof` |
| `read_http_head_respects_per_call_limits` | 8192 与 64KiB 两档上限各自生效 |

**负向验证**:把读取块从 4096 改成 1(退回逐字节形态),`read_http_head_all_at_once` 立即报红:

```
assertion `left == right` failed: 一次到齐应只推 1 次 read
```

### TLS 缓存

| 测试 | 覆盖 |
|---|---|
| `tls_acceptor_is_cached_per_key` | 取到 acceptor 后**删掉证书与私钥文件**,再取仍成功且 `Arc::ptr_eq` 相等 —— 直接证明没有重新读盘 |
| `tls_acceptor_distinguishes_keys` | 不同路径各自独立缓存 |

**负向验证**:把缓存命中分支改成恒 false,第一个测试立刻报红(`must hit cache (files deleted): No such file or directory`)。

### 改写字义(红线段)

四个测试锁住 `rewrite_http_head` 的既有语义,断言未改一行:

- `rewrite_http_head_semantics_preserved`:Host 重写、`del` 生效、无关头保留、`Connection` 强制替换为 `close`、**`set` 追加在原始头之后**;
- `rewrite_http_head_empty_rewrite_keeps_host`:host_rewrite 为空时保留原 Host;
- `rewrite_http_head_adds_missing_host`:无 Host 时补上;
- `rewrite_http_head_dedups_host`:重复 Host 只留第一个。

### 脚本

`tests/e2e-plugins.sh`:**PASS=15 FAIL=0**(http_proxy / socks5 / https2http / https2https / http2https / http2http / tls2raw 全覆盖)。

全量:`cargo test --workspace` 全绿(rh-client 43 → 57 lib 测试);`cargo clippy --workspace --all-targets -- -D warnings` 通过。

## 3. 吞吐(如实:没有提升)

基准:http2http 短连接,每请求一条新连接 + 一次头改写,并发 8,每轮 6s,各 5 轮:

| 版本 | 各轮 QPS | med |
|---|---|---|
| 改前 | 5601.6, 5468.8, 5540.3, 5539.5, 5496.8 | **5539.5** |
| 改后 | 5396.1, 5563.7, 5398.2, 5495.4, 5444.7 | **5444.7** |

**改后 med 低 1.7%,best 低 0.7%,两者都在轮间波动范围内(改前自身极差 132 QPS ≈ 2.4%)。结论:没有可测提升。**

这不是「改了没用」,而是「这份基准测不到它」。理由见 §4。

## 4. 为什么测不到(以及这份基准踩过的坑)

为了让结论可信,我先做了瓶颈定位,结果说明这台机器上根本轮不到那些固定成本发言:

1. **单进程 Python 上游只有 2742 QPS**。最初的上游是「每请求起一个线程」的 Python 服务,直连测下来 2742 QPS——被测链路(5600)比它还快,说明基准从头到尾量的都是压测端/上游,不是 rh。改成 6 进程 + `SO_REUSEPORT` 后上游上限提到 8806 QPS,这才让 5500 量级的读数有意义。
2. **macOS 临时端口只够撑 6 秒**。`net.inet.ip.portrange` 是 49152–65535(16384 个),`net.inet.tcp.msl=15s`。一请求一连接的压测跑到 10s 就开始整片失败,日志是 `Can't assign requested address (os error 49)` = `EADDRNOTAVAIL`,**纯 OS 端口耗尽,与被测代码无关**。实测:6s 稳定零错误,10s 全崩。所以所有对照都卡在 6s + 轮间 2s 回收。
3. 即使上游和端口都解决了,**rh 链路本身每请求仍要走一次服务端转发 + 一次 mux 流 + 一次插件拨号**,这些成本远大于「读 300 字节头」和「一次 canonicalize」。把这些固定开销压到零,QPS 上限也不会动——这也解释了为什么改前改后看不出差别。

> 顺带纠正需求描述里的一处事实:**「主路径 256KiB」不准确**。`forward.rs::copy_limited` 实际是 `MIN_BUF=16KiB` 起步、读满即倍增、上限 `MAX_BUF=64KiB`。所以插件里从 16KiB 改到 64KiB,是**对齐主路径的上限**,不是对齐 256KiB。

## 5. 红线核对

| 红线 | 状态 |
|---|---|
| `Connection: close` 单请求模型 | 未改。`rewrite_http_head` 末尾仍是 `Connection: close\r\n\r\n`;static_file 的 keep-alive 判定也原样保留 |
| Host/HeaderOp 改写顺序(set 在 del 之后) | 未改。`for p in &op.set` 仍在「原始头遍历 + del 过滤」之后追加,有 `rewrite_http_head_semantics_preserved` 断言 `set_pos > keep_pos` 锁住 |
| 三处上限不同(8192/64KiB/64KiB) | 已按原值传入,有 `read_http_head_respects_per_call_limits` 覆盖 |
| 超限必须报错而非无限读 | 有专门测试断言读取量上界 |

## 6. 未做的部分(如实)

- **`load_certs`/`load_key` 没有改成 `spawn_blocking`**。理由见 §1:首次 dial 才读盘,且有缓存兜底,改 async 签名收益不抵成本。若将来证书路径频繁变化,应改 `spawn_blocking`。
- **没有在真实网络/三机环境测吞吐**。本机 loopback 下,这些固定开销被其他环节掩盖;若要在「高 QPS 小请求」场景证明收益,需要一台临时端口充足(Linux `ip_local_port_range` 默认 32768–60999)、上游能吃满压测的机器,并用 `thp`/loadgen 类工具。本次没有这个条件。
- **TLS 缓存没有失效机制**。证书文件在进程运行期间被替换时,缓存不会自动刷新(需重启客户端)。这与改动前「每次 dial 重读」不同,是**行为变化**,记在此处。考虑到插件证书路径来自静态配置、轮换需重启,判断可接受;若需要热加载,应在缓存键里加入文件 mtime 或提供显式失效入口。
- **`serve_static_file` 的 root canonicalize 缓存到函数入口**,若 root 在服务期内被删除/重建,`root_canon` 仍是旧值——与改动前每请求重算的行为不同。同一判断:静态文件服务的 root 不预期在运行期变更。这两条都属行为变化,已知悉。
