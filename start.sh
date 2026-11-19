#!/usr/bin/env bash
# pg-relay + pg-headless 容器内启动脚本（Cloady 一体部署）
#
# 布局（APP_HOME = 本脚本所在目录，镜像内为 /app）：
#   relay/     Node 中继源码；config.json / headless.json / current-url.txt
#              由本脚本按环境变量在每次启动时生成（密钥不落镜像）
#   headless/  AIStudio2API 二进制；.env 由本脚本生成；runtime/ 是其数据目录
#              （账户不在 runtime 根下：AISTUDIO_AUTH_STATES=runtime/auth 把
#              账户目录指进已挂持久卷的 runtime/，否则账户落在易失层、
#              每次 redeploy 必丢——2026-11-19 独立排查定案）
#
# 守护方式：进程同生共死——任一退出，本脚本杀掉其余并以非零码退出，
# 由平台重启整个容器。各进程日志都直通 stdout。
# 可选组件：设了 CLASH_SUB_URL 时，启动前先跑订阅转换器并拉起 mihomo，
# 把订阅节点变成回环 socks 端口并入中继流量池（见 src/clash-convert.js）。
set -u

# 容器内不需要任何代理：中继只打回环、执行器直连谷歌。若运行环境注入了
# 代理变量（某些平台/调试环境会），Node 的 fetch 会试图拿代理打回环地址而
# 失败——启动前一律清掉，保证行为与设计一致。
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy NODE_USE_ENV_PROXY
export NO_PROXY="127.0.0.1,localhost" no_proxy="127.0.0.1,localhost"

APP_HOME="$(cd "$(dirname "$0")" && pwd)"
RELAY_DIR="$APP_HOME/relay"
HL_DIR="$APP_HOME/headless"

fail() { echo "[start] ERROR: $*" >&2; exit 1; }

# ---- 必填环境变量（在 Cloady 面板里填，清单见 ENV.md）----
[ -n "${PG_API_KEY:-}" ]        || fail "缺少 PG_API_KEY（对外服务 Key）"
[ -n "${HL_PROXY_API_KEY:-}" ]  || fail "缺少 HL_PROXY_API_KEY（中继->无头 内部生成 Key）"
[ -n "${HL_ADMIN_USERNAME:-}" ] || fail "缺少 HL_ADMIN_USERNAME（无头管理账号）"
[ -n "${HL_ADMIN_PASSWORD:-}" ] || fail "缺少 HL_ADMIN_PASSWORD（无头管理口令）"

PG_PORT="${PORT:-${PG_PORT:-18901}}"          # Cloady 若注入 PORT 则从之
HL_PORT="${HL_PORT:-2048}"
# 2049 在 Fetch 规范的禁止端口名单上（NFS 端口），Node 的 fetch 会直接报
# "bad port" 拒绝连接，中继将永远连不上无头执行器。默认 2048 安全，别改成 2049。
[ "$HL_PORT" != "2049" ] || fail "HL_PORT 不能用 2049（Fetch 禁止端口），请用默认 2048"
HL_PREVIEW_PORT="${HL_PREVIEW_PORT:-2050}"

mkdir -p "$RELAY_DIR/data" "$HL_DIR/runtime" "$HL_DIR/runtime/auth"

# ---- 生成中继配置 ----
node -e '
  const fs = require("fs");
  fs.writeFileSync(process.argv[1], JSON.stringify({
    apiKey: process.env.PG_API_KEY,
    port: Number(process.argv[2]),
  }, null, 2));
  fs.writeFileSync(process.argv[3], JSON.stringify({
    baseUrl: "http://127.0.0.1:" + process.argv[4],
    apiKey: process.env.HL_PROXY_API_KEY,
    adminUsername: process.env.HL_ADMIN_USERNAME,
    adminPassword: process.env.HL_ADMIN_PASSWORD,
  }, null, 2));
' "$RELAY_DIR/config.json" "$PG_PORT" "$RELAY_DIR/headless.json" "$HL_PORT" \
  || fail "生成 relay 配置失败"

# 公开地址：填了 PUBLIC_URL 就写 current-url.txt（控制台与脚本下载链接用）；
# 不填则删掉该文件，中继会回退到平台转发头（x-forwarded-host）拼地址。
if [ -n "${PUBLIC_URL:-}" ]; then
  printf '%s' "$PUBLIC_URL" > "$RELAY_DIR/current-url.txt"
else
  rm -f "$RELAY_DIR/current-url.txt"
fi

# ---- 生成无头 .env ----
# 注意：PROXY 固定留空——Cloady 容器出站是机房直连，不再需要本机的 egress 代理，
# 无头执行器直连谷歌。切勿把本机 proxy.env 的值带过来。
{
  printf 'LISTEN_ADDR=127.0.0.1:%s\n' "$HL_PORT"
  printf 'PREVIEW_LISTEN_ADDR=127.0.0.1:%s\n' "$HL_PREVIEW_PORT"
  printf 'PROXY=\n'
  printf 'PROXY_API_KEY=%s\n' "$HL_PROXY_API_KEY"
  printf 'ADMIN_AUTH_ENABLED=true\n'
  printf 'ADMIN_USERNAME=%s\n' "$HL_ADMIN_USERNAME"
  printf 'ADMIN_PASSWORD=%s\n' "$HL_ADMIN_PASSWORD"
  printf 'WAA_BACKEND=%s\n' "${WAA_BACKEND:-spidermonkey}"
  printf 'WARM_WORKER_LIMIT=%s\n' "${WARM_WORKER_LIMIT:-2}"
  printf 'AUTO_START=true\n'
  printf 'UPSTREAM_CHANNELS=playground\n'
  printf 'AISTUDIO_AUTH_STATES=runtime/auth\n'
} > "$HL_DIR/.env" || fail "生成 headless .env 失败"
chmod 600 "$HL_DIR/.env" "$RELAY_DIR/config.json" "$RELAY_DIR/headless.json"

echo "[start] relay port=$PG_PORT headless=127.0.0.1:$HL_PORT preview=127.0.0.1:$HL_PREVIEW_PORT"

# ---- Clash 订阅节点（可选）：转成本地 socks 端口并入中继流量池 ----
# 未设 CLASH_SUB_URL 时整段跳过，行为与之前完全一致。转换器失败不许
# 拖死启动（它自己保证退出码 0，这里再兜一层）；mihomo 只在生成了
# clash.yaml 时拉起，日志直通容器 stdout。
MIHOMO_PID=""
if [ -n "${CLASH_SUB_URL:-}" ] && [ -x "$APP_HOME/mihomo/mihomo" ]; then
  echo "[start] 拉取 Clash 订阅并生成节点池…"
  CLASH_OUT_DIR="$RELAY_DIR/data" node "$RELAY_DIR/src/clash-convert.js" || \
    echo "[start] WARN: 订阅转换异常，池里只剩手工条目" >&2
  if [ -s "$RELAY_DIR/data/clash.yaml" ]; then
    (cd "$RELAY_DIR/data" && exec "$APP_HOME/mihomo/mihomo" -d "$RELAY_DIR/data" -f "$RELAY_DIR/data/clash.yaml") &
    MIHOMO_PID=$!
    # 等第一个池端口可连（最多 15s）；超时不阻塞启动——中继先起来，
    # 池端口在 mihomo 就绪后自然可用（分配只在推送导入时发生）。
    FIRST_PORT="$(node -e 'try{const n=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));if(n[0])process.stdout.write(String(n[0].port))}catch{}' "$RELAY_DIR/data/clash-nodes.json")"
    if [ -n "$FIRST_PORT" ]; then
      for _ in $(seq 1 30); do
        if timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$FIRST_PORT" 2>/dev/null; then
          echo "[start] mihomo 池端口 $FIRST_PORT 已就绪"
          break
        fi
        kill -0 "$MIHOMO_PID" 2>/dev/null || { echo "[start] WARN: mihomo 已退出，池节点暂不可用" >&2; break; }
        sleep 0.5
      done
    fi
  fi
fi

# ---- 拉起进程（无头 + 中继，mihomo 已在上面按需拉起）----
(cd "$HL_DIR" && exec ./aistudio2api) &
HL_PID=$!
(cd "$RELAY_DIR" && exec node src/server.js) &
RELAY_PID=$!

PIDS="$HL_PID $RELAY_PID"
[ -n "$MIHOMO_PID" ] && PIDS="$PIDS $MIHOMO_PID"

trap 'kill $PIDS 2>/dev/null; wait $PIDS 2>/dev/null; exit 0' TERM INT

wait -n $PIDS
echo "[start] 有进程退出，整体关闭以便平台重启" >&2
kill $PIDS 2>/dev/null
wait $PIDS 2>/dev/null
exit 1
