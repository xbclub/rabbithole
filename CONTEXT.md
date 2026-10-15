# rabbithole

内网穿透与端口转发系统:公网一侧的 rh-server 接受 rh-client 接入,把公网入口的流量送进内网。
本文件是本仓库的**统一语言(ubiquitous language)**,只有术语,不含实现细节。

## Language

### 转发面

**转发规则**（代码/API 中的 service、`ServiceRow`、`/api/services`）:
一条把公网入口的流量送进内网的配置,由 `kind` 决定其形态(tcp/udp/http/https/tcpmux/stcp/sudp/xtcp)。
_避免_:服务、隧道、代理、规则条目

**访问器**（visitor、`VisitorRow`、`/api/visitors`）:
位于内网另一侧、主动连出去把流量接回本地的配置,与一条 stcp/sudp/xtcp 转发规则配对使用。
_避免_:客户端、访问端

**客户端**（client、`ClientRow`、`/api/clients`）:
一台接入 rh-server 的 rh-client 实例,是转发规则与访问器的归属主体。
_避免_:节点、agent、设备

**客户端密钥**（client secret）:
客户端接入 rh-server 的凭据,也是 SSH 网关的密码认证凭据;仅创建与轮换时一次性展示。
_避免_:密钥、token、密码

**访问密钥**（`secret_key`）:
stcp/sudp/xtcp 转发规则与访问器之间的共享密钥;完全由服务端生成与轮换,客户端与访问器都不持有副本。
_避免_:密钥、密钥规则、密码

### 管理面

**控制台**（console、`console/`）:
面向运营者的管理界面,由 rh-server 托管,用于管理客户端、转发规则、访问器、证书与服务器设置。
_避免_:后台、admin、dashboard

**总览**（overview）:
控制台的首页,呈现客户端/转发规则的计数、流量速率与最近事件。
_避免_:仪表盘、首页
