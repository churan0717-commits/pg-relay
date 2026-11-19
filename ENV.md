# 环境变量清单（部署时在 Cloady 面板里填）

所有密钥都只经面板环境变量注入，由 `start.sh` 在每次启动时生成
`relay/config.json`、`relay/headless.json`、`headless/.env`（均 600 权限），
不进部署包、不进镜像。值不要写进任何仓库或聊天记录。

## 必填

| 变量 | 说明 | 取值来源 |
|---|---|---|
| `PG_API_KEY` | 对外服务 Key：脚本、酒馆、手机 App 都用它，也是管理台登录 Key | **沿用现网那把**（在本机 `~/workspace/pg-relay/config.json` 的 `apiKey`，掩码 muse-…27f1）。沿用它，脚本和酒馆端就不用换 Key，只换地址 |
| `HL_PROXY_API_KEY` | 中继 → 无头执行器的内部生成 Key（`X-API-Key`） | 沿用现网 `~/workspace/pg-relay/headless.json` 的 `apiKey`，或新生成一串 32 位以上随机串；只在容器内回环用，不对外 |
| `HL_ADMIN_USERNAME` | 无头执行器管理账号名 | 沿用现网 `headless.json` 的 `adminUsername`，或新设（如 `admin`） |
| `HL_ADMIN_PASSWORD` | 无头执行器管理口令（中继用它登录管理 API 做 Cookie 导入） | 沿用现网 `headless.json` 的 `adminPassword`，或新设强口令。**只用字母/数字/`._-`**（要写进 .env，特殊符号可能被解析坏） |

## 选填

| 变量 | 默认 | 说明 |
|---|---|---|
| `PUBLIC_URL` | 空 | 应用的 Cloady 公开地址（如 `https://应用名-短码-工作区名.cloady.io`）。首次部署拿到地址后填上并重新部署，控制台里的 API Base 与脚本下载链接才会拼对。不填也能用：中继回退到平台转发头拼地址 |
| `PORT` / `PG_PORT` | 18901 | 中继对外端口。若走 Cloady API 的 deploy 端点，`port` 参数传 18901；面板部署保持默认即可，别乱改 |
| `HL_PORT` | 2048 | 无头执行器主端口（仅容器内回环）。**绝不能改成 2049**：该端口在 Fetch 规范禁止名单上（NFS 端口），中继的 fetch 会直接报 `bad port` 连不上执行器；`start.sh` 对 2049 有硬拦截 |
| `HL_PREVIEW_PORT` | 2050 | 无头执行器预览端口（仅容器内回环） |
| `WARM_WORKER_LIMIT` | 2 | 无头预热账号数。免费档内存/CPU 紧，**保持 2，别调大** |
| `WAA_BACKEND` | spidermonkey | 与现网一致，别改 |
| `CLASH_SUB_URL` | 空 | Clash 订阅地址。填了之后，每次容器启动先拉订阅、把节点经 mihomo 转成回环 socks 端口（42001 起）自动并入中继流量池，推送导入账户时与手工条目一起轮换分配；不填则只有手工池。**订阅地址本身是凭证，只填面板、不要写进任何文件或聊天**；拉取失败或无可用节点不影响服务启动（池里只剩手工条目）。节点增删在机场/订阅侧做，服务重启后刷新 |
| `PG_EXTRA_VLESS_LINKS` | 空 | 额外 vless 节点分享链接（一个或多个，换行或逗号分隔，设为 secret）。链接多到超过单变量 4096 字符上限时，可继续拆到 `PG_EXTRA_VLESS_LINKS_2`、`PG_EXTRA_VLESS_LINKS_3`，转换时按序拼接。启动转换时逐条解析成 mihomo vless 条目追加进节点池，本地 socks 端口从 42077 起按链接顺序分配；订阅整体失败时只要额外节点可用，配置照样生成。**链接是凭证，只填面板、不要写进任何文件或聊天**。配合 `PG_POOL_ALLOW_PORTS` 白名单控制池里实际启用哪些端口 |

## 明确不要填

- **`PROXY` / `HTTP_PROXY` / `HTTPS_PROXY` 一律不填。**
  本机部署时无头执行器要借这台 VM 的 egress 代理出站；Cloady 容器是机房
  直连出站，不需要、也不应该带任何代理——`start.sh` 生成的 `.env` 里
  `PROXY=` 固定留空，无头执行器直连谷歌。这正是搬上 Cloady 的目的之一。
  尤其不要把本机 `pg-relay/proxy.env` 的值抄过来：那是这台 VM 的运行时
  代理凭据，在 Cloady 上既连不通、也毫无意义。
