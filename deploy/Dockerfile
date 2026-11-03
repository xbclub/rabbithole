# rabbithole 服务端镜像(基于 CI 预编译的 linux gnu 二进制)。
# Dockerfile 内没有任何 RUN 步骤:多架构(amd64/arm64)在 CI 里逐架构单平台构建
# 再以 manifest 合并,全程不需要 QEMU 模拟。
#
# 注意:GitHub Actions 的 upload/download-artifact 会剥掉可执行位,
# 二进制统一以 COPY --chmod=755 落位,不依赖源文件权限。
#
# 客户端另有独立镜像 deploy/Dockerfile.client(ghcr.io/.../rabbithole-client)。
#
# CI 用法(自动):release 工作流下载静态二进制到 bin/<arch>/ 后按架构构建。
# 本地用法(先准备 gnu 二进制):
#   cross build --release --target x86_64-unknown-linux-gnu -p rh-server
#   mkdir -p bin/amd64 && cp target/x86_64-unknown-linux-gnu/release/rh-server bin/amd64/
#   docker build -f deploy/Dockerfile --build-arg BIN_DIR=bin/amd64 -t rabbithole-server:local .
# 基础镜像 debian-slim:二进制为 gnu 动态链接(glibc);入口脚本是纯 POSIX sh(dash 兼容)。
# (2026-11-03 弃 alpine+musl:三机实测 musl 产物单流比 gnu 慢 ~7-9%,性能优先)
FROM debian:13-slim
ARG BIN_DIR=bin/amd64
COPY --chmod=755 ${BIN_DIR}/rh-server /usr/local/bin/
COPY deploy/docker-server.toml /etc/rabbithole/server.toml
COPY --chmod=755 deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# 控制面 TLS 7100 / Web 控制台 8080 / vhost HTTP 8081 / TCPMux 8082 / HTTPS 8443;
# 转发端口段(默认 20000-21000)按需映射
EXPOSE 7100 8080 8081 8082 8443
VOLUME /var/lib/rabbithole

ENTRYPOINT ["docker-entrypoint.sh"]
