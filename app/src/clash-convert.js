'use strict';

/**
 * clash-convert.js — Clash 订阅 → mihomo 池配置转换器
 *
 * 由 start.sh 在容器启动时（拉起中继前）调用，也可独立运行：
 *   CLASH_SUB_URL=<订阅地址> CLASH_OUT_DIR=/app/relay/data node src/clash-convert.js
 *
 * 流程：拉订阅（Clash 客户端 UA）→ 解析 proxies → 过滤信息类假节点 →
 * 在 CLASH_OUT_DIR（默认 /app/relay/data）生成两份产物：
 *   clash.yaml       mihomo 配置：proxies 原样写入；每个节点一条回环
 *                    socks listener（127.0.0.1:42001 起），中继把这些
 *                    端口当作 socks5 代理并入流量池。
 *   clash-nodes.json [{ port, label }]，中继启动/读状态时据此重建池视图。
 *
 * 订阅地址是凭证：只从环境变量读，不落盘、不进日志。
 * 任何失败（拉不到、解析失败、零可用节点）都只写空 nodes、删掉旧
 * clash.yaml 并以退出码 0 结束——绝不拖死整个服务启动。
 *
 * 额外节点（可选）：环境变量 PG_EXTRA_VLESS_LINKS 可放一个或多个
 * vless:// 分享链接（换行或逗号分隔）；链接多到单变量放不下时可拆到
 * PG_EXTRA_VLESS_LINKS_2、PG_EXTRA_VLESS_LINKS_3，按序拼接。逐个解析成 mihomo vless 条目
 * 追加进同一份配置，本地 socks 端口从 42077 起（订阅节点占
 * 42001..42076，互不冲突）。订阅整体失败时只要额外节点可用，配置
 * 照样生成；链接本身是凭证，同样只从环境变量读、绝不进日志。
 *
 * UA 注意（实测）：面板按 UA 分发格式——clash-verge / ClashMeta UA
 * 返回完整 Clash YAML（81 节点）；ClashforWindows UA 只返回 2 个推广
 * 假节点的残缺配置；Shadowrocket / sing-box UA 返回非 YAML 格式。
 * 故固定用 clash-verge UA。
 */

const fs = require('node:fs');
const path = require('node:path');
const yaml = require('../vendor/js-yaml.min.js');

const SUB_URL = process.env.CLASH_SUB_URL || '';
const EXTRA_LINKS_RAW = [
  process.env.PG_EXTRA_VLESS_LINKS || '',
  process.env.PG_EXTRA_VLESS_LINKS_2 || '',
  process.env.PG_EXTRA_VLESS_LINKS_3 || '',
]
  .filter((s) => s.trim())
  .join('\n');
const OUT_DIR = process.env.CLASH_OUT_DIR || '/app/relay/data';
const BASE_PORT = 42001;
const EXTRA_BASE_PORT = 42077;
const SUB_UA = 'clash-verge/v2.0.0';
// 信息类假节点关键词：面板塞的流量/到期提示与推广入口，不是真节点
const INFO_KEYWORDS = ['剩余', '到期', '流量', '官网', '订阅', '重置', '电报', '防失联'];

function writeAtomic(file, content) {
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, content, { mode: 0o600 });
  fs.renameSync(tmp, file);
}

async function fetchSubscription(url) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const r = await fetch(url, {
        headers: { 'User-Agent': SUB_UA },
        redirect: 'follow',
        signal: AbortSignal.timeout(20_000),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return await r.text();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error('订阅拉取失败');
}

/**
 * 解析一条 vless:// 分享链接为 mihomo vless proxy 条目。
 * 支持 tcp / ws 传输、reality / tls / 无加密安全层；解析不了返回 null。
 * 只取字段、不打印链接本身（凭证）。
 */
function parseVlessLink(link) {
  let u;
  try {
    u = new URL(link);
  } catch {
    return null;
  }
  if (u.protocol !== 'vless:') return null;
  const uuid = decodeURIComponent(u.username || '');
  const server = u.hostname || '';
  const port = Number(u.port) || 443;
  if (!uuid || !server) return null;
  let name = '';
  try {
    name = decodeURIComponent((u.hash || '').replace(/^#/, '')).trim();
  } catch {
    name = '';
  }
  if (!name) name = `vless-${server}:${port}`;
  const q = u.searchParams;
  const network = (q.get('type') || 'tcp').toLowerCase();
  const security = (q.get('security') || 'none').toLowerCase();
  const proxy = {
    name,
    type: 'vless',
    server,
    port,
    uuid,
    udp: true,
  };
  if (network !== 'tcp') proxy.network = network;
  if (security === 'tls' || security === 'reality') proxy.tls = true;
  const sni = q.get('sni') || q.get('servername') || '';
  if (sni) proxy.servername = sni;
  const fp = q.get('fp') || q.get('client-fingerprint') || '';
  if (fp) proxy['client-fingerprint'] = fp;
  const flow = q.get('flow') || '';
  if (flow) proxy.flow = flow;
  if (q.get('insecure') === '1' || q.get('allowInsecure') === '1') {
    proxy['skip-cert-verify'] = true;
  }
  if (security === 'reality') {
    const pbk = q.get('pbk') || q.get('public-key') || '';
    if (!pbk) return null; // reality 没有公钥必然连不上，别生成废条目
    const opts = { 'public-key': pbk };
    const sid = q.get('sid') || q.get('short-id') || '';
    if (sid) opts['short-id'] = sid;
    proxy['reality-opts'] = opts;
  }
  if (network === 'ws') {
    const opts = {};
    const p = q.get('path') || '';
    if (p) opts.path = p;
    const host = q.get('host') || '';
    if (host) opts.headers = { Host: host };
    if (Object.keys(opts).length) proxy['ws-opts'] = opts;
  }
  if (network === 'grpc') {
    const svc = q.get('serviceName') || '';
    if (svc) proxy['grpc-opts'] = { 'grpc-service-name': svc };
  }
  return proxy;
}

/** PG_EXTRA_VLESS_LINKS：换行或逗号分隔，逐条解析，坏链接跳过。 */
function parseExtraVlessLinks(raw) {
  if (!raw || !raw.trim()) return [];
  const out = [];
  for (const part of raw.split(/[\n,]+/)) {
    const link = part.trim();
    if (!link) continue;
    const p = parseVlessLink(link);
    if (p) out.push(p);
    else console.error('[clash-convert] 一条额外 vless 链接解析失败，已跳过');
  }
  return out;
}

async function main() {
  const nodesPath = path.join(OUT_DIR, 'clash-nodes.json');
  const yamlPath = path.join(OUT_DIR, 'clash.yaml');
  const writeEmpty = (why) => {
    console.error(`[clash-convert] ${why}；写空节点表（池里只剩手工条目）`);
    try { writeAtomic(nodesPath, '[]\n'); } catch (e) {
      console.error('[clash-convert] 写空节点表失败', e.message);
    }
    try { fs.unlinkSync(yamlPath); } catch {}
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });

  // ---- 订阅节点（端口 BASE_PORT 起）----
  const seenNames = new Set();
  const usable = [];
  let subTotal = 0;
  let subError = '';
  if (!SUB_URL) {
    subError = 'CLASH_SUB_URL 未设置';
  } else {
    let text = null;
    try {
      text = await fetchSubscription(SUB_URL);
    } catch (e) {
      subError = `订阅拉取失败: ${e.message}`;
    }
    if (text !== null) {
      let doc = null;
      try {
        doc = yaml.load(text);
      } catch (e) {
        subError = `订阅 YAML 解析失败: ${e.message}`;
      }
      if (doc) {
        const all = Array.isArray(doc.proxies) ? doc.proxies : [];
        subTotal = all.length;
        for (const p of all) {
          if (!p || typeof p.name !== 'string' || !p.name || !p.server || !p.port) continue;
          if (INFO_KEYWORDS.some((k) => p.name.includes(k))) continue;
          if (seenNames.has(p.name)) continue; // 同名节点 mihomo 会拒绝，留先到的
          seenNames.add(p.name);
          usable.push(p);
        }
      }
    }
  }

  // ---- 额外 vless 节点（端口 EXTRA_BASE_PORT 起，与订阅段互不冲突）----
  const extras = [];
  for (const p of parseExtraVlessLinks(EXTRA_LINKS_RAW)) {
    if (seenNames.has(p.name)) {
      console.error(`[clash-convert] 额外节点与已有节点同名，已跳过: ${p.name}`);
      continue;
    }
    seenNames.add(p.name);
    extras.push(p);
  }

  if (!usable.length && !extras.length) {
    writeEmpty(subError || `订阅无可用节点（共 ${subTotal} 条，过滤后 0）且无额外节点`);
    return;
  }
  if (subError) {
    console.error(`[clash-convert] ${subError}；仅用额外节点 ${extras.length} 条生成配置`);
  }

  const proxies = [...usable, ...extras];
  const nodes = [
    ...usable.map((p, i) => ({ port: BASE_PORT + i, label: p.name })),
    ...extras.map((p, i) => ({ port: EXTRA_BASE_PORT + i, label: p.name })),
  ];
  const listeners = nodes.map((n, i) => ({
    name: `pool-${i}`,
    type: 'socks',
    listen: '127.0.0.1',
    port: n.port,
    proxy: n.label,
    udp: false,
  }));
  const cfg = { 'log-level': 'warning', proxies, listeners };
  writeAtomic(yamlPath, yaml.dump(cfg, { lineWidth: -1, noRefs: true }));
  writeAtomic(nodesPath, JSON.stringify(nodes, null, 2) + '\n');
  console.log(
    `[clash-convert] 订阅节点 ${usable.length}/${subTotal}（端口 ${BASE_PORT}..${BASE_PORT + usable.length - 1}）` +
      (extras.length
        ? `，额外节点 ${extras.length}（端口 ${EXTRA_BASE_PORT}..${EXTRA_BASE_PORT + extras.length - 1}）`
        : ''),
  );
}

main().catch((e) => {
  // 兜底：任何未预期错误也不许让启动失败
  console.error('[clash-convert] 未预期错误（忽略，服务照常启动）', e && e.message);
  process.exit(0);
});
