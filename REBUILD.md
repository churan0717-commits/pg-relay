# pg-relay 一键重建手册

服务构成（全部在 Cloady 应用 `pg-relay`，工作区 `workspace-214ed1`）：

- 代码：GitHub 仓库 `churan0717-commits/pg-relay`（main 分支），含 Dockerfile + docker-compose.yml（命名卷 relay-data / headless-data）+ cloady.json。
- 配置：全部走 Cloady 环境变量，值备份在本机 `~/workspace/pg-relay/rebuild-env.json`（600，不入仓）：
  PG_API_KEY、HL_PROXY_API_KEY、HL_ADMIN_USERNAME、HL_ADMIN_PASSWORD、CLASH_SUB_URL（secret）。
  注意 PROXY/HTTP(S)_PROXY 在 Cloady 上必须为空。
- 规模：scale cpu=1、memory=1（即免费档 0.5 核 / 1GB），端口 18901。

## 场景一：应用还在，只是挂了/要更新（地址不变）

```bash
export CLOADY_TOKEN=<见保险箱 custom.cloady>
cloady redeploy workspace-214ed1 pg-relay
# 等 deploys[0].status=live 后验：/health、/api/console/status（账户数应保留）、/api/console/pool（应为 76）
```

## 场景二：应用被删/工作区异常（全量重建，地址后缀会变——平台规则）

1. 用 API deployApp：workspace=workspace-214ed1，name=pg-relay，
   source={type:"github", repo:"churan0717-commits/pg-relay", branch:"main"}，
   port=18901，price 守卫 0（被计价就停、先查工作区配额）。
2. 设置全部环境变量（值从 rebuild-env.json 读，CLASH_SUB_URL 设为 secret）。
3. PATCH scale：cpu=1、memory=1。
4. 等 live 后验 health/status/pool；新地址形如 pg-relay-<新后缀>-workspace-214ed1.cloady.io，
   需同步更新：管理台地址、脚本（脚本从服务端按当前域名动态生成，装新版即可）、酒馆/App 里的 base URL。
5. 请用户做两件只有他能做的事：重推一次 Cookie、重设站长口令。其余全自动。

## 数据面说明

- 命名卷（relay-data、headless-data）扛 redeploy 与容器重启；应用被删则卷随应用消失（场景二处理）。
- 本机后备栈：`~/workspace/pg-relay/install.sh` 可在本机重装 systemd 三件套（pg-relay/pg-tunnel/pg-headless），
  隧道地址轮换，仅作应急。
