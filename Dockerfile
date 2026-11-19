# pg-relay + pg-headless 一体镜像（Cloady 免费工作区部署包）
# 构建上下文 = 本 cloady/ 目录（先跑 ./prepare.sh 同步 app/）。
# 对外只暴露中继端口 18901；无头执行器只监听容器内回环，由中继内部调用。
FROM node:24-bookworm-slim

# 无头执行器版本钉死 v0.3.12；校验值与本机生产二进制逐字节核对一致：
#   tar 包 sha256 = c44580d8...（33972508 字节，与 GitHub Release 资产大小一致）
#   解压后二进制 sha256 = 3f52551b...（与现网 pg-headless/bin/aistudio2api 相同）
ARG AISTUDIO2API_VERSION=v0.3.12
ARG AISTUDIO2API_TAR_SHA256=c44580d86043f42e32e62679d5870002bed8f6d5d68e64cb0f652851b33d9f86
ARG AISTUDIO2API_BIN_SHA256=3f52551b0aca52610603279bc290a62abfdbc5c576e2e3e466d1d96172da15e8

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates gzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 无头执行器：构建期从 GitHub Release 下载（不进部署包——解压后 81MB 超过
# Cloady 上传文件夹 50MB 上限；下载路线包体只剩约 0.6MB），双重 sha256 校验。
RUN mkdir -p /app/headless \
 && curl -fL -o /tmp/aistudio2api.tar.gz \
      "https://github.com/Mag1cFall/AIStudio2API/releases/download/${AISTUDIO2API_VERSION}/aistudio2api-${AISTUDIO2API_VERSION}-linux-amd64.tar.gz" \
 && echo "${AISTUDIO2API_TAR_SHA256}  /tmp/aistudio2api.tar.gz" | sha256sum -c - \
 && tar -xzf /tmp/aistudio2api.tar.gz -C /tmp \
 && install -m 0755 "/tmp/aistudio2api-${AISTUDIO2API_VERSION}-linux-amd64/aistudio2api" /app/headless/aistudio2api \
 && echo "${AISTUDIO2API_BIN_SHA256}  /app/headless/aistudio2api" | sha256sum -c - \
 && rm -rf /tmp/aistudio2api.tar.gz "/tmp/aistudio2api-${AISTUDIO2API_VERSION}-linux-amd64"

# mihomo（Clash.Meta）：把 Clash 订阅节点转成本地 socks 端口供中继流量池
# 使用。版本钉死 v1.19.34，compatible 变体（基线 CPU 兼容，云主机稳妥）；
# 校验值由官方 GitHub Release 下载件在本机实测得出，gz 与二进制双重校验。
ARG MIHOMO_VERSION=v1.19.34
ARG MIHOMO_GZ_SHA256=2763f5b414527f7baf6dc57e3f82f3ab561eedf34bfd7c2f2cde1f7e592efe06
ARG MIHOMO_BIN_SHA256=5b6644c487e5dffae86b0e0e9bb5cec445579a3a9d03977c09576fee2aeefc01

RUN mkdir -p /app/mihomo \
 && curl -fL -o /tmp/mihomo.gz \
      "https://github.com/MetaCubeX/mihomo/releases/download/${MIHOMO_VERSION}/mihomo-linux-amd64-compatible-${MIHOMO_VERSION}.gz" \
 && echo "${MIHOMO_GZ_SHA256}  /tmp/mihomo.gz" | sha256sum -c - \
 && gunzip -c /tmp/mihomo.gz > /app/mihomo/mihomo \
 && chmod 0755 /app/mihomo/mihomo \
 && echo "${MIHOMO_BIN_SHA256}  /app/mihomo/mihomo" | sha256sum -c - \
 && rm -f /tmp/mihomo.gz

# 中继源码
COPY app/ /app/relay/

# 中继在本机部署时只绑 127.0.0.1（前面有隧道）；容器里要让平台入口打进来，
# 构建期把监听地址改成可用环境变量覆盖、默认 0.0.0.0。锚点对不上就让构建失败，
# 防止源码改版后静默打出一个连不上的镜像。
RUN grep -q "^const HOST = '127.0.0.1';$" /app/relay/src/server.js \
 && sed -i "s/^const HOST = '127.0.0.1';$/const HOST = process.env.PG_RELAY_HOST || '0.0.0.0';/" /app/relay/src/server.js \
 && grep -q "PG_RELAY_HOST" /app/relay/src/server.js

RUN cd /app/relay && npm ci --omit=dev --no-audit --no-fund

COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh && mkdir -p /app/relay/data /app/headless/runtime

ENV PG_PORT=18901 HL_PORT=2048 HL_PREVIEW_PORT=2050
EXPOSE 18901
CMD ["/app/start.sh"]
