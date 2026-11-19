'use strict';

/**
 * pg-relay — Catiecli 脚本协议兼容后端（一期 WS 挂机 + 二期无头）
 *
 * - HTTP 网关：OpenAI /v1/chat/completions、/v1/models；
 *   Gemini 原生 /v1beta/models/{model}:generateContent 与 :streamGenerateContent
 * - WS 执行器端点：/api/pg/ws（协议与 Catiecli Playground Executor v2.21.0 对齐）
 * - 二期无头：POST /api/pggo/push 接收脚本 Cookie 推送 → 转 AIStudio2API
 *   （pg-headless，只绑本机）导入；pggo- 前缀模型转发给它执行，pg- 仍走 WS。
 * - 脚本托管：/api/pg/script.user.js（域名按当前隧道地址动态替换）
 * - 管理网页：/（填 Key 查看执行器状态 / 复制 API Base / 下载脚本 / pggo 状态与代理）
 *
 * JSPB(wire) 编解码与格式转换借自 pg-bridge 开源仓库 relay-server.js
 * （vendored 于 ../vendor/），不使用其签名服务：proof 由用户浏览器里
 * 的脚本页面现铸，本服务只负责调度与格式转换。
 */

const http = require('node:http');
const net = require('node:net');
const tls = require('node:tls');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer } = require('ws');

const bridge = require('../vendor/relay-server.js');
const {
  buildWireContentsFromPayload,
  buildGenerateWireBody,
  parseMakerSuiteResponse,
  parseSingleMakerSuiteFrame,
  IncrementalJspbFrameParser,
  toOpenAIChatCompletion,
  toGeminiResponse,
} = bridge;

const ROOT = path.resolve(__dirname, '..');
const CONFIG_PATH = path.join(ROOT, 'config.json');
const CURRENT_URL_PATH = path.join(ROOT, 'current-url.txt');
const ORIG_SCRIPT_PATH = path.join(
  ROOT,
  'vendor-script',
  'Catiecli_Playground_Executor-2.21.0.user.js',
);
const CONSOLE_HTML_PATH = path.join(__dirname, 'console.html');
const LOGIN_HTML_PATH = path.join(__dirname, 'login.html');
const PEACH_PATH = path.join(__dirname, 'assets', 'peach.webp');
let peachBuf = null; // 站点门面图，首次请求时读入内存缓存
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
let API_KEY = String(config.apiKey || ''); // 有效值可被 data/apikey-override.json 覆盖（见下方站长分层一节）
const PORT = Number(config.port || 18901);
const HOST = '127.0.0.1';
if (!API_KEY) {
  console.error('[pg-relay] config.json 缺少 apiKey');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// 二期（无头模式）配置：headless.json 存在才启用。里面是本机回环的
// AIStudio2API 地址与其内部 API Key / 管理账号口令，只存本机、永不外发。
// ---------------------------------------------------------------------------
const HEADLESS_PATH = path.join(ROOT, 'headless.json');
let HEADLESS = null;
try {
  HEADLESS = JSON.parse(fs.readFileSync(HEADLESS_PATH, 'utf8'));
} catch {}
const HL_BASE = HEADLESS ? String(HEADLESS.baseUrl || 'http://127.0.0.1:2048').replace(/\/$/, '') : null;

// pggo 持久状态：邮箱→自填代理、流量池（代理池+轮询游标+邮箱分配）、
// 上次推送结果（供管理台展示）。旧 state 文件缺池字段时默认空，向后兼容。
const DATA_DIR = path.join(ROOT, 'data');
const PGGO_STATE_PATH = path.join(DATA_DIR, 'pggo-state.json');
let pggoState = {
  proxies: { byEmail: {}, default: '' },
  proxyPool: [],
  poolCursor: 0,
  poolAssignments: {},
  lastPush: null,
};
try {
  const saved = JSON.parse(fs.readFileSync(PGGO_STATE_PATH, 'utf8'));
  if (saved && typeof saved === 'object') {
    pggoState = {
      proxies: {
        byEmail: (saved.proxies && saved.proxies.byEmail) || {},
        default: (saved.proxies && saved.proxies.default) || '',
      },
      proxyPool: Array.isArray(saved.proxyPool)
        ? saved.proxyPool.filter((p) => p && typeof p.url === 'string')
        : [],
      poolCursor: Number.isFinite(saved.poolCursor) ? saved.poolCursor : 0,
      poolAssignments:
        saved.poolAssignments && typeof saved.poolAssignments === 'object'
          ? saved.poolAssignments
          : {},
      lastPush: saved.lastPush || null,
    };
  }
} catch {}
function savePggoState() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(PGGO_STATE_PATH, JSON.stringify(pggoState, null, 2) + '\n', { mode: 0o600 });
  } catch (e) {
    console.error('[pg-relay] pggo 状态落盘失败', e.message);
  }
}

// ---------------------------------------------------------------------------
// 用量记录：生成类请求（/v1/chat/completions、/v1beta generateContent）逐条
// 落 JSONL（data/usage.jsonl，与 pggo-state.json 同目录、同为持久卷）。
// 每条：ts / model（客户端请求的原始模型名）/ channel（pg|pggo，按前缀）/
// ok / status（最终 HTTP 状态码）/ error（失败原因，≤200 字）/ stream /
// durMs（请求总耗时毫秒）/ ttfbMs（流式首个内容块到达毫秒；非流式与 durMs
// 相同；流式在首块前就失败则不写此字段）。老记录没有 durMs/ttfbMs，前端
// 显示「—」。
// 只在请求的最终出口记一笔（流式在流结束/出错/客户端断开时记）；写入失败
// 一律吞掉，绝不影响主流程。启动时 + 每 6 小时清理一次，只留最近 14 天。
// 客户端断开且尚未发出任何响应时 status 记 499（借用 nginx 的约定）。
// ---------------------------------------------------------------------------
const USAGE_PATH = path.join(DATA_DIR, 'usage.jsonl');
const USAGE_KEEP_MS = 14 * 24 * 3600 * 1000;

function usageDayKey(ts) {
  // 按 Asia/Shanghai（UTC+8，无夏令时）切日
  return new Date(ts + 8 * 3600 * 1000).toISOString().slice(0, 10);
}

function recordUsage(rec) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFileSync(USAGE_PATH, JSON.stringify({
      ts: Date.now(),
      model: String(rec.model || ''),
      channel: rec.channel === 'pggo' ? 'pggo' : 'pg',
      ok: !!rec.ok,
      status: Number(rec.status) || 0,
      error: rec.ok ? '' : String(rec.error || '').slice(0, 200),
      stream: !!rec.stream,
      ...(Number.isFinite(rec.durMs) ? { durMs: Math.max(0, Math.round(rec.durMs)) } : {}),
      ...(Number.isFinite(rec.ttfbMs) ? { ttfbMs: Math.max(0, Math.round(rec.ttfbMs)) } : {}),
    }) + '\n');
  } catch {}
}

// 一次性记录器：同一请求无论有多少出口，只记第一笔（防重复/漏记）。
// 创建时刻即请求进入时刻；流式路径在写出首个内容块时调 rec.markTtfb()
// 打首字点，最终出口调 rec(...) 时按进入→现在算总耗时 durMs。
function makeUsageRecorder(modelRaw, stream) {
  let recorded = false;
  let ttfbMs = null;
  const t0 = Date.now();
  const channel = isPggoModel(modelRaw) ? 'pggo' : 'pg';
  const model = String(modelRaw || '');
  const rec = (ok, status, error) => {
    if (recorded) return;
    recorded = true;
    const durMs = Date.now() - t0;
    recordUsage({
      model,
      channel,
      ok,
      status,
      error,
      stream,
      durMs,
      // 非流式：首字即全部，ttfbMs 与 durMs 相同；流式未打出首字就结束
      // （失败/断开）则不写 ttfbMs，前端回落到总耗时显示。
      ttfbMs: stream ? (ttfbMs != null ? ttfbMs : undefined) : durMs,
    });
  };
  rec.markTtfb = () => {
    if (ttfbMs == null) ttfbMs = Date.now() - t0;
  };
  return rec;
}

function readUsageRecords() {
  try {
    return fs.readFileSync(USAGE_PATH, 'utf8')
      .split('\n')
      .filter((ln) => ln.trim())
      .map((ln) => { try { return JSON.parse(ln); } catch { return null; } })
      .filter((r) => r && Number.isFinite(Number(r.ts)));
  } catch {
    return [];
  }
}

function pruneUsageLog() {
  try {
    const cutoff = Date.now() - USAGE_KEEP_MS;
    const kept = readUsageRecords().filter((r) => Number(r.ts) >= cutoff);
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(USAGE_PATH, kept.map((r) => JSON.stringify(r)).join('\n') + (kept.length ? '\n' : ''));
  } catch {}
}
pruneUsageLog();
setInterval(pruneUsageLog, 6 * 3600 * 1000).unref();

// 某日汇总：day 缺省/格式不对 → 今天（Asia/Shanghai）；days 为有数据的
// 日期倒序；byModel 按条数倒序；errors 为当天失败（最新在前，最多 50 条）；
// entries 为当天全部流水（最新在前，最多 300 条），供管理台逐条看时间明细。
function usageSummary(dayParam) {
  const records = readUsageRecords();
  const days = [...new Set(records.map((r) => usageDayKey(Number(r.ts))))].sort().reverse();
  const day = /^\d{4}-\d{2}-\d{2}$/.test(dayParam || '') ? dayParam : usageDayKey(Date.now());
  const todays = records.filter((r) => usageDayKey(Number(r.ts)) === day);
  const byModelMap = new Map();
  let ok = 0;
  for (const r of todays) {
    if (r.ok) ok += 1;
    const key = String(r.model || '');
    if (!byModelMap.has(key)) byModelMap.set(key, { model: key, total: 0, ok: 0, fail: 0 });
    const m = byModelMap.get(key);
    m.total += 1;
    if (r.ok) m.ok += 1; else m.fail += 1;
  }
  const byModel = [...byModelMap.values()]
    .sort((a, b) => b.total - a.total || (a.model < b.model ? -1 : 1));
  const errors = todays
    .filter((r) => !r.ok)
    .sort((a, b) => Number(b.ts) - Number(a.ts))
    .slice(0, 50)
    .map((r) => ({
      ts: Number(r.ts),
      model: String(r.model || ''),
      status: Number(r.status) || 0,
      error: String(r.error || ''),
    }));
  const numOrNull = (v) => (v == null || !Number.isFinite(Number(v)) ? null : Number(v));
  const entries = todays
    .slice()
    .sort((a, b) => Number(b.ts) - Number(a.ts))
    .slice(0, 300)
    .map((r) => ({
      ts: Number(r.ts),
      model: String(r.model || ''),
      channel: r.channel === 'pggo' ? 'pggo' : 'pg',
      ok: !!r.ok,
      status: Number(r.status) || 0,
      error: String(r.error || ''),
      stream: !!r.stream,
      durMs: numOrNull(r.durMs),
      ttfbMs: numOrNull(r.ttfbMs),
    }));
  return { day, days, total: todays.length, ok, fail: todays.length - ok, byModel, errors, entries };
}

// ---------------------------------------------------------------------------
// 流量池：代理分享链接解析 + 推送时的代理选择
// 分享链接形如 socks://<base64令牌>@host:port#备注 —— userinfo 是单个
// base64 令牌，解码后才是 user:pass。解析统一规范化成标准代理 URL：
// scheme 统一（socks→socks5）、user/pass 百分号编码、# 后内容作 label。
// ---------------------------------------------------------------------------
function encUserinfo(s) {
  return encodeURIComponent(s).replace(/[!'()*~]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());
}
function decUserinfo(s) {
  try { return decodeURIComponent(s); } catch { return s; }
}

// 解析一行代理 → { url, label }；失败抛 Error（message 即跳过原因）
// 支持三种写法：① 带协议头的分享链接 socks5:// / http(s)://（userinfo 可为
// base64 令牌）；② 四段式裸格式 host:port:用户名:密码；③ 裸 @ 式
// 用户名:密码@host:port。②③ 是住宅代理的常见发放格式，统一按 http 存入池。
// 全角冒号「：」先统一成半角再解析（从厂商后台复制出来常带全角）。
function parseProxyLine(raw) {
  let s = String(raw || '').trim().replace(/：/g, ':');
  if (!s) throw new Error('空行');
  let label = '';
  const hashIdx = s.indexOf('#');
  if (hashIdx !== -1) {
    const frag = s.slice(hashIdx + 1);
    try { label = decodeURIComponent(frag); } catch { label = frag; }
    label = label.trim();
    s = s.slice(0, hashIdx).trim();
  }
  const m = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.+)$/.exec(s);
  if (!m) return parseBareProxy(s, label);
  let scheme = m[1].toLowerCase();
  if (scheme === 'socks') scheme = 'socks5';
  if (!['socks5', 'http', 'https'].includes(scheme)) throw new Error(`不支持的协议 ${m[1]}`);
  let rest = m[2].trim();
  let userinfo = null;
  const atIdx = rest.lastIndexOf('@');
  if (atIdx !== -1) {
    userinfo = rest.slice(0, atIdx);
    rest = rest.slice(atIdx + 1);
  }
  const hm = /^(.+):(\d{1,5})$/.exec(rest);
  if (!hm) throw new Error('缺少主机或端口（应形如 host:port）');
  const host = hm[1];
  const port = Number(hm[2]);
  if (!host || /\s/.test(host)) throw new Error('主机不合法');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('端口不合法');
  let user = '';
  let pass = '';
  if (userinfo !== null && userinfo !== '') {
    const colonIdx = userinfo.indexOf(':');
    if (colonIdx !== -1) {
      user = decUserinfo(userinfo.slice(0, colonIdx));
      pass = decUserinfo(userinfo.slice(colonIdx + 1));
    } else if (/^[A-Za-z0-9+/=_-]+$/.test(userinfo) && userinfo.length % 4 !== 1) {
      // 无冒号且像 base64：先按令牌解码，解出 user:pass 才算数
      let decoded = '';
      try {
        decoded = Buffer.from(userinfo.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
      } catch {}
      const di = decoded.indexOf(':');
      if (di !== -1 && !/[\x00-\x1f\x7f]/.test(decoded)) {
        user = decoded.slice(0, di);
        pass = decoded.slice(di + 1);
      } else {
        user = decUserinfo(userinfo);
      }
    } else {
      user = decUserinfo(userinfo);
    }
  }
  let url = `${scheme}://`;
  if (user) {
    url += encUserinfo(user);
    if (pass) url += ':' + encUserinfo(pass);
    url += '@';
  }
  url += `${host}:${port}`;
  return { url, label };
}

// 无协议头的裸格式 → 统一规范化为 http://user:pass@host:port（住宅代理默认
// HTTP；user/pass 按池内既有约定百分号编码存储，使用时上游自行解码）。
// @ 式：密码可含 @ 与 :（userinfo 取最后一个 @ 之前、密码取第一个 : 之后）；
// 四段式：密码可含 :（第 4 段起整体作密码）。
function parseBareProxy(s, label) {
  const checkHostPort = (host, portStr) => {
    if (!host || /\s/.test(host)) throw new Error('主机不合法');
    if (!/^\d{1,5}$/.test(portStr)) throw new Error('端口不合法');
    const port = Number(portStr);
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('端口不合法');
    return port;
  };
  const build = (user, pass, host, port) => {
    if (!user) throw new Error('缺少用户名');
    let url = 'http://' + encUserinfo(user);
    if (pass) url += ':' + encUserinfo(pass);
    url += `@${host}:${port}`;
    return { url, label };
  };
  // 四段式优先判：host:port: 开头（主机含点或为 localhost）。密码里可能带
  // @，不能先按 @ 切；而 @ 式的用户名段不会长成「主机:端口:」的样子。
  const seg = /^([^:@\s]+):(\d{1,5}):/.exec(s);
  if (seg && (seg[1].includes('.') || seg[1] === 'localhost')) {
    const port = checkHostPort(seg[1], seg[2]);
    const rest = s.slice(seg[0].length); // 用户名[:密码]，密码可含 : 与 @
    const ci = rest.indexOf(':');
    const user = ci === -1 ? rest : rest.slice(0, ci);
    const pass = ci === -1 ? '' : rest.slice(ci + 1);
    return build(user, pass, seg[1], port);
  }
  const atIdx = s.lastIndexOf('@');
  if (atIdx !== -1) {
    const userinfo = s.slice(0, atIdx);
    const hm = /^(.+):(\d{1,5})$/.exec(s.slice(atIdx + 1));
    if (!hm) throw new Error('缺少主机或端口（应形如 用户名:密码@host:port）');
    const port = checkHostPort(hm[1], hm[2]);
    const ci = userinfo.indexOf(':');
    if (ci !== -1) return build(userinfo.slice(0, ci), userinfo.slice(ci + 1), hm[1], port);
    // 无冒号：与分享链接同规矩，像 base64 令牌且能解出 user:pass 才按令牌算
    if (/^[A-Za-z0-9+/=_-]+$/.test(userinfo) && userinfo.length % 4 !== 1) {
      let decoded = '';
      try {
        decoded = Buffer.from(userinfo.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
      } catch {}
      const di = decoded.indexOf(':');
      if (di !== -1 && !/[\x00-\x1f\x7f]/.test(decoded)) {
        return build(decoded.slice(0, di), decoded.slice(di + 1), hm[1], port);
      }
    }
    return build(userinfo, '', hm[1], port);
  }
  const parts = s.split(':');
  if (parts.length >= 4) {
    const port = checkHostPort(parts[0], parts[1]);
    return build(parts[2], parts.slice(3).join(':'), parts[0], port);
  }
  throw new Error('无法识别格式（支持 socks5:// / http:// 分享链接、四段式 host:port:用户名:密码、用户名:密码@host:port）');
}

// 逐行解析批量文本 → { entries: [{url,label}], skipped: [{line, reason}] }
// line 为原文行号（1 起）；空行直接忽略、不计入 skipped。同 URL 去重。
function parseProxyPoolText(text) {
  const entries = [];
  const skipped = [];
  const seen = new Set();
  String(text || '').split(/\r?\n/).forEach((raw, i) => {
    if (!raw.trim()) return;
    try {
      const e = parseProxyLine(raw);
      if (seen.has(e.url)) {
        skipped.push({ line: i + 1, reason: '与前面一行重复' });
        return;
      }
      seen.add(e.url);
      entries.push({ url: e.url, label: e.label, line: i + 1 });
    } catch (err) {
      skipped.push({ line: i + 1, reason: err.message });
    }
  });
  return { entries, skipped };
}

// ---------------------------------------------------------------------------
// 流量池（订阅部分）：src/clash-convert.js 在服务启动时把 Clash 订阅节点
// 转成回环 socks 端口（mihomo listeners），节点表写在 clash-nodes.json。
// 池视图 = 手工条目（存 state）+ clash 条目（每次按 nodes 文件重建，
// 不写进 state 的 proxyPool，避免重复与陈旧）；文件 mtime 变了才重读。
// 分配记录存 URL 字符串，订阅不变时 clash 端口稳定，天然兼容。
// ---------------------------------------------------------------------------
const CLASH_NODES_PATH = process.env.CLASH_NODES_PATH || path.join(DATA_DIR, 'clash-nodes.json');
let clashNodesCache = { mtimeMs: -1, entries: [] };
function clashPoolEntries() {
  let mtimeMs;
  try {
    mtimeMs = fs.statSync(CLASH_NODES_PATH).mtimeMs;
  } catch {
    return [];
  }
  if (mtimeMs !== clashNodesCache.mtimeMs) {
    let entries = [];
    try {
      const list = JSON.parse(fs.readFileSync(CLASH_NODES_PATH, 'utf8'));
      if (Array.isArray(list)) {
        entries = list
          .filter((n) => n && Number.isInteger(n.port) && n.port >= 1 && n.port <= 65535)
          .map((n) => ({
            url: `socks5://127.0.0.1:${n.port}`,
            label: String(n.label || ''),
            source: 'clash',
          }));
      }
    } catch {}
    clashNodesCache = { mtimeMs, entries };
  }
  return clashNodesCache.entries;
}

// 合并池视图：手工条目在前（source=manual），clash 条目在后，同 URL 去重
function mergedPool() {
  const seen = new Set();
  const out = [];
  for (const p of pggoState.proxyPool) {
    if (seen.has(p.url)) continue;
    seen.add(p.url);
    out.push({ url: p.url, label: p.label || '', source: 'manual' });
  }
  // 可选白名单：PG_POOL_ALLOW_PORTS=42001,42008 时订阅节点只启用这些端口
  const allowPorts = String(process.env.PG_POOL_ALLOW_PORTS || '')
    .split(',').map((s) => s.trim()).filter(Boolean);
  for (const p of clashPoolEntries()) {
    if (allowPorts.length && !allowPorts.includes(String(p.url).split(':').pop())) continue;
    if (seen.has(p.url)) continue;
    seen.add(p.url);
    out.push(p);
  }
  return out;
}
function isClashPoolUrl(u) {
  return clashPoolEntries().some((p) => p.url === u);
}

// ---------------------------------------------------------------------------
// 流量池一键测速：逐个经条目自己的代理探测「到谷歌通不通、快不快」。
// 无第三方依赖：SOCKS5 手写最小客户端（net 直连：问候 / 无认证或账密认证 /
// CONNECT），HTTP 代理走 CONNECT；隧道上再套 TLS 发一条 HTTP/1.1 请求，
// 只读状态行。单项超时 6s、并发上限 6，全部测完一次返回。
// 错误文案一律短句且不带代理账密；响应里的 proxy 字段打码。
// ---------------------------------------------------------------------------
const POOL_TEST_TIMEOUT_MS = 6000;
const POOL_TEST_CONCURRENCY = 6;

function poolTestParseProxy(rawUrl) {
  let u;
  try {
    u = new URL(rawUrl);
  } catch {
    return null;
  }
  const proto = u.protocol.replace(':', '').toLowerCase();
  if (!['socks5', 'socks', 'http', 'https'].includes(proto)) return null;
  return {
    proto: proto === 'socks' ? 'socks5' : proto,
    host: u.hostname,
    port: Number(u.port) || (proto === 'https' ? 443 : proto === 'http' ? 80 : 1080),
    username: u.username ? decodeURIComponent(u.username) : '',
    password: u.password ? decodeURIComponent(u.password) : '',
  };
}

function poolTestMaskUrl(rawUrl) {
  return String(rawUrl || '').replace(/\/\/[^/@:]+(:[^/@]*)?@/, '//***@');
}

function poolTestFail(msg) {
  const e = new Error(msg);
  e.poolTestMsg = msg;
  return e;
}

function poolTestErrText(e) {
  if (!e) return '连接失败';
  if (e.poolTestMsg) return e.poolTestMsg;
  const c = e.code || '';
  if (c === 'ECONNREFUSED') return '连接拒绝';
  if (c === 'ETIMEDOUT' || c === 'ECONNABORTED') return '连接超时';
  if (c === 'ENOTFOUND' || c === 'EAI_AGAIN') return '域名解析失败';
  if (c === 'ECONNRESET' || c === 'EPIPE') return '连接被重置';
  if (/CERT|TLS|SSL/.test(c) || /certificate|handshake/i.test(e.message || '')) return 'TLS 失败';
  return '连接失败';
}

// SOCKS5 隧道：连上代理 → 问候/认证 → CONNECT 目标，成功后 resolve 裸 socket
function poolTestSocks5Tunnel(px, dstHost, dstPort, deadline) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(px.port, px.host);
    let settled = false;
    const timer = setTimeout(() => fail(poolTestFail('连接超时')), Math.max(1, deadline - Date.now()));
    function fail(e) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { sock.destroy(); } catch {}
      reject(e);
    }
    sock.once('error', fail);
    sock.once('connect', () => {
      const methods = px.username ? [0x00, 0x02] : [0x00];
      sock.write(Buffer.from([0x05, methods.length, ...methods]));
    });
    let buf = Buffer.alloc(0);
    let stage = 'greeting';
    function sendConnect() {
      stage = 'connect';
      const hostBuf = Buffer.from(dstHost, 'utf8');
      sock.write(Buffer.concat([
        Buffer.from([0x05, 0x01, 0x00, 0x03, hostBuf.length]),
        hostBuf,
        Buffer.from([(dstPort >> 8) & 0xff, dstPort & 0xff]),
      ]));
    }
    sock.on('data', (chunk) => {
      if (settled) return;
      buf = Buffer.concat([buf, chunk]);
      try {
        if (stage === 'greeting') {
          if (buf.length < 2) return;
          if (buf[0] !== 0x05) throw poolTestFail('代理协议错误');
          const method = buf[1];
          buf = buf.subarray(2);
          if (method === 0x02) {
            if (!px.username) throw poolTestFail('代理认证失败');
            const un = Buffer.from(px.username, 'utf8');
            const pw = Buffer.from(px.password, 'utf8');
            sock.write(Buffer.concat([Buffer.from([0x01, un.length]), un, Buffer.from([pw.length]), pw]));
            stage = 'auth';
          } else if (method === 0x00) {
            sendConnect();
          } else {
            throw poolTestFail('代理认证失败');
          }
          return;
        }
        if (stage === 'auth') {
          if (buf.length < 2) return;
          if (buf[1] !== 0x00) throw poolTestFail('代理认证失败');
          buf = buf.subarray(2);
          sendConnect();
          return;
        }
        if (stage === 'connect') {
          if (buf.length < 4) return;
          if (buf[1] !== 0x00) throw poolTestFail('代理连接失败');
          const atyp = buf[3];
          let need = 4;
          if (atyp === 0x01) need += 4;
          else if (atyp === 0x03) {
            if (buf.length < 5) return;
            need += 1 + buf[4];
          } else if (atyp === 0x04) need += 16;
          else throw poolTestFail('代理协议错误');
          need += 2;
          if (buf.length < need) return;
          settled = true;
          clearTimeout(timer);
          sock.removeAllListeners('data');
          sock.removeAllListeners('error');
          resolve(sock);
        }
      } catch (e) {
        fail(e);
      }
    });
  });
}

// HTTP 代理隧道：CONNECT 目标，2xx 后 resolve 裸 socket
function poolTestHttpTunnel(px, dstHost, dstPort, deadline) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(px.port, px.host);
    let settled = false;
    const timer = setTimeout(() => fail(poolTestFail('连接超时')), Math.max(1, deadline - Date.now()));
    function fail(e) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { sock.destroy(); } catch {}
      reject(e);
    }
    sock.once('error', fail);
    sock.once('connect', () => {
      let req = `CONNECT ${dstHost}:${dstPort} HTTP/1.1\r\nHost: ${dstHost}:${dstPort}\r\n`;
      if (px.username) {
        req += `Proxy-Authorization: Basic ${Buffer.from(`${px.username}:${px.password}`).toString('base64')}\r\n`;
      }
      req += '\r\n';
      sock.write(req);
    });
    let buf = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      if (settled) return;
      buf = Buffer.concat([buf, chunk]);
      const idx = buf.indexOf('\r\n\r\n');
      if (idx === -1) {
        if (buf.length > 16384) fail(poolTestFail('代理协议错误'));
        return;
      }
      const head = buf.subarray(0, idx).toString('latin1');
      const m = head.match(/^HTTP\/1\.\d (\d{3})/);
      if (!m) return fail(poolTestFail('代理协议错误'));
      const status = Number(m[1]);
      if (status === 407) return fail(poolTestFail('代理认证失败'));
      if (status < 200 || status >= 300) return fail(poolTestFail('代理连接失败'));
      settled = true;
      clearTimeout(timer);
      sock.removeAllListeners('data');
      sock.removeAllListeners('error');
      resolve(sock);
    });
  });
}

// 经代理对 host 发一次 HTTPS GET，只读状态行；返回 {connectMs, status, totalMs}
async function poolTestFetch(px, host, pathName) {
  const t0 = Date.now();
  const deadline = t0 + POOL_TEST_TIMEOUT_MS;
  const tunnel = px.proto === 'socks5'
    ? await poolTestSocks5Tunnel(px, host, 443, deadline)
    : await poolTestHttpTunnel(px, host, 443, deadline);
  const connectMs = Date.now() - t0;
  try {
    const status = await new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => done(poolTestFail('响应超时')), Math.max(1, deadline - Date.now()));
      const tlsSock = tls.connect({ socket: tunnel, servername: host }, () => {
        tlsSock.write(
          `GET ${pathName} HTTP/1.1\r\nHost: ${host}\r\nUser-Agent: Mozilla/5.0 pg-relay-pool-test\r\nAccept: */*\r\nConnection: close\r\n\r\n`,
        );
      });
      function done(e, v) {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try { tlsSock.destroy(); } catch {}
        if (e) reject(e);
        else resolve(v);
      }
      let buf = Buffer.alloc(0);
      tlsSock.on('data', (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        const idx = buf.indexOf('\r\n');
        if (idx === -1) {
          if (buf.length > 16384) done(poolTestFail('响应异常'));
          return;
        }
        const m = buf.subarray(0, idx).toString('latin1').match(/^HTTP\/1\.\d (\d{3})/);
        if (!m) return done(poolTestFail('响应异常'));
        done(null, Number(m[1]));
      });
      tlsSock.once('error', (e) => done(e));
      tlsSock.once('close', () => done(poolTestFail('连接被重置')));
    });
    return { connectMs, status, totalMs: Date.now() - t0 };
  } catch (e) {
    try { tunnel.destroy(); } catch {}
    throw e;
  }
}

async function poolTestEntry(entry) {
  const base = {
    key: entry.url,
    label: entry.label || '',
    proxy: poolTestMaskUrl(entry.url),
    ok: false,
    connectMs: null,
    googleMs: null,
    googleStatus: null,
    aistudioMs: null,
    aistudioStatus: null,
    error: '',
  };
  const px = poolTestParseProxy(entry.url);
  if (!px) {
    base.error = '代理地址无法解析';
    return base;
  }
  if (px.proto === 'https') {
    base.error = '暂不支持 HTTPS 代理测速';
    return base;
  }
  const [g, a] = await Promise.allSettled([
    poolTestFetch(px, 'www.google.com', '/generate_204'),
    poolTestFetch(px, 'aistudio.google.com', '/'),
  ]);
  if (g.status === 'fulfilled') {
    base.connectMs = g.value.connectMs;
    base.googleMs = g.value.totalMs;
    base.googleStatus = g.value.status;
    base.ok = g.value.status === 204;
    if (!base.ok) base.error = `谷歌返回 ${g.value.status}`;
  } else {
    base.error = poolTestErrText(g.reason);
  }
  if (a.status === 'fulfilled') {
    base.aistudioMs = a.value.totalMs;
    base.aistudioStatus = a.value.status;
    if (base.connectMs == null) base.connectMs = a.value.connectMs;
  }
  return base;
}

async function poolTestAll(entries) {
  const results = new Array(entries.length);
  let next = 0;
  async function worker() {
    while (next < entries.length) {
      const i = next;
      next += 1;
      results[i] = await poolTestEntry(entries[i]);
    }
  }
  const n = Math.min(POOL_TEST_CONCURRENCY, entries.length);
  await Promise.all(Array.from({ length: n }, () => worker()));
  return results;
}

// 推送导入时选代理：按邮箱绑定 > 默认代理 > 流量池（手工+订阅合并池）。
// 走池时同一邮箱复用既有分配（账户出口不乱跳）；分配记录指向的 URL
// 已被移出池时重新分配。返回 { proxy, source }。
function pickProxyForEmail(email) {
  if (email && pggoState.proxies.byEmail[email]) {
    return { proxy: pggoState.proxies.byEmail[email], source: 'byEmail' };
  }
  if (pggoState.proxies.default) {
    return { proxy: pggoState.proxies.default, source: 'default' };
  }
  const pool = mergedPool();
  if (email && pool.length) {
    const assigned = pggoState.poolAssignments[email];
    if (assigned && pool.some((p) => p.url === assigned)) {
      return { proxy: assigned, source: 'pool' };
    }
    const entry = pool[pggoState.poolCursor % pool.length];
    pggoState.poolCursor += 1;
    pggoState.poolAssignments[email] = entry.url;
    savePggoState();
    return { proxy: entry.url, source: 'pool' };
  }
  return { proxy: '', source: 'none' };
}

// ---------------------------------------------------------------------------
// 站长分层（owner）：服务 Key 只管 API 调用；管理台另设站长口令。
// - owner.json（data/，600）：scrypt 哈希+盐，口令由站长本人在管理台自设。
// - apikey-override.json（data/，600）：站长在管理台改出的新服务 Key，
//   存在时优先于 config.apiKey，改完立即生效、重启不丢。
// - 站长口令一旦设置，/api/console/* 只认站长会话 Cookie，服务 Key
//   不再能进管理台；未设置时管理台沿用服务 Key 鉴权（升级不锁死）。
// ---------------------------------------------------------------------------
const OWNER_PATH = path.join(DATA_DIR, 'owner.json');
const APIKEY_OVERRIDE_PATH = path.join(DATA_DIR, 'apikey-override.json');

try {
  const ov = JSON.parse(fs.readFileSync(APIKEY_OVERRIDE_PATH, 'utf8'));
  if (ov && typeof ov.apiKey === 'string' && ov.apiKey) API_KEY = ov.apiKey;
} catch {}

let ownerRecord = null; // { salt, hash, createdAt }
try {
  const o = JSON.parse(fs.readFileSync(OWNER_PATH, 'utf8'));
  if (o && typeof o.salt === 'string' && typeof o.hash === 'string') ownerRecord = o;
} catch {}

function writeDataFile600(file, obj) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2) + '\n', { mode: 0o600 });
  try { fs.chmodSync(file, 0o600); } catch {}
}

function setApiKey(newKey) {
  API_KEY = newKey;
  try {
    writeDataFile600(APIKEY_OVERRIDE_PATH, {
      apiKey: newKey,
      updatedAt: Math.floor(Date.now() / 1000),
    });
  } catch (e) {
    console.error('[pg-relay] 服务 Key 覆盖落盘失败（内存中已生效）', e.message);
  }
}

function hashOwnerPassword(password, saltHex) {
  return crypto.scryptSync(String(password), Buffer.from(saltHex, 'hex'), 64).toString('hex');
}

function saveOwnerRecord(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  ownerRecord = {
    salt,
    hash: hashOwnerPassword(password, salt),
    createdAt: Math.floor(Date.now() / 1000),
  };
  writeDataFile600(OWNER_PATH, ownerRecord);
}

function verifyOwnerPassword(password) {
  if (!ownerRecord) return false;
  const got = Buffer.from(hashOwnerPassword(password, ownerRecord.salt), 'hex');
  const want = Buffer.from(ownerRecord.hash, 'hex');
  return got.length === want.length && crypto.timingSafeEqual(got, want);
}

// 站长会话：随机 token 只以 sha256 哈希存内存，Cookie 发原文（httpOnly）
const OWNER_COOKIE = 'pg_owner';
const OWNER_SESSION_MS = 7 * 24 * 3600 * 1000;
const ownerSessions = new Map(); // sha256(token) -> 过期时间(ms)

function ownerTokenHash(token) {
  return crypto.createHash('sha256').update(token).digest('hex');
}

function readOwnerToken(req) {
  const cookie = req.headers.cookie || '';
  const m = cookie.match(new RegExp(`(?:^|;\\s*)${OWNER_COOKIE}=([0-9a-f]{64})`));
  return m ? m[1] : '';
}

function issueOwnerSession(res) {
  const token = crypto.randomBytes(32).toString('hex');
  ownerSessions.set(ownerTokenHash(token), Date.now() + OWNER_SESSION_MS);
  res.setHeader(
    'Set-Cookie',
    `${OWNER_COOKIE}=${token}; HttpOnly; Path=/; Max-Age=${Math.floor(OWNER_SESSION_MS / 1000)}; SameSite=Lax`,
  );
}

function ownerSessionValid(req) {
  const token = readOwnerToken(req);
  if (!token) return false;
  const h = ownerTokenHash(token);
  const exp = ownerSessions.get(h);
  if (!exp) return false;
  if (Date.now() > exp) {
    ownerSessions.delete(h);
    return false;
  }
  return true;
}

function destroyOwnerSession(req, res) {
  const token = readOwnerToken(req);
  if (token) ownerSessions.delete(ownerTokenHash(token));
  res.setHeader('Set-Cookie', `${OWNER_COOKIE}=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax`);
}

// 站长登录失败锁定：连错 5 次锁 5 分钟（内存计数，简单实现）
const ownerLoginGuard = { fails: 0, lockedUntil: 0 };

// ---------------------------------------------------------------------------
// 模型表（借 pg-bridge 静态表；对外带 pg- 前缀，与 Catiecli 生态一致）
// ---------------------------------------------------------------------------
const BASE_MODELS = [
  { id: 'gemini-4-argon', displayName: 'Gemini 4 Argon' },
  { id: 'gemini-4-argon-high', displayName: 'Gemini 4 Argon (High Thinking)' },
  { id: 'gemini-4-argon-low', displayName: 'Gemini 4 Argon (Low Thinking)' },
  { id: 'gemini-4-carbon', displayName: 'Gemini 4 Carbon' },
  { id: 'gemini-4-carbon-high', displayName: 'Gemini 4 Carbon (High Thinking)' },
  { id: 'gemini-4-carbon-low', displayName: 'Gemini 4 Carbon (Low Thinking)' },
  { id: 'gemini-4-hydrogen', displayName: 'Gemini 4 Hydrogen' },
  { id: 'gemini-4-hydrogen-high', displayName: 'Gemini 4 Hydrogen (High Thinking)' },
  { id: 'gemini-4-hydrogen-low', displayName: 'Gemini 4 Hydrogen (Low Thinking)' },
  { id: 'gemini-3.8-flash', displayName: 'Gemini 3.8 Flash' },
  { id: 'gemini-3.8-flash-high', displayName: 'Gemini 3.8 Flash (High Thinking)' },
  { id: 'gemini-3.8-flash-low', displayName: 'Gemini 3.8 Flash (Low Thinking)' },
  { id: 'gemini-3.6-flash', displayName: 'Gemini 3.6 Flash' },
  { id: 'gemini-3.5-flash-lite', displayName: 'Gemini 3.5 Flash Lite' },
  { id: 'gemini-3.5-flash-lite-high', displayName: 'Gemini 3.5 Flash Lite (High Thinking)' },
  { id: 'gemini-3.5-flash-lite-low', displayName: 'Gemini 3.5 Flash Lite (Low Thinking)' },
  { id: 'gemini-3.1-pro-preview', displayName: 'Gemini 3.1 Pro Preview' },
  { id: 'gemini-3.1-pro-preview-high', displayName: 'Gemini 3.1 Pro Preview (High Thinking)' },
  { id: 'gemini-3.1-pro-preview-low', displayName: 'Gemini 3.1 Pro Preview (Low Thinking)' },
  { id: 'gemini-nano-banana-2.1', displayName: 'Nano Banana 2.1' },
  { id: 'gemini-3.1-flash-image', displayName: 'Nano Banana 2 (Gemini 3.1 Flash Image)' },
  { id: 'gemini-3.1-flash-lite-image', displayName: 'Nano Banana 2 Lite (Gemini 3.1 Flash Lite Image)' },
  { id: 'gemini-3-pro-image', displayName: 'Nano Banana Pro (Gemini 3 Pro Image)' },
];
const MODELS = BASE_MODELS.map((m) => ({ ...m, id: `pg-${m.id}` }));

function stripModelPrefix(model) {
  let m = String(model || '').trim();
  m = m.replace(/^models\//, '');
  if (m.startsWith('pggo-')) m = m.slice(5);
  else if (m.startsWith('pg-')) m = m.slice(3);
  return m || 'gemini-3.8-flash';
}

const FINISH_REASON_MAP = {
  0: 'FINISH_REASON_UNSPECIFIED',
  1: 'STOP',
  2: 'MAX_TOKENS',
  3: 'SAFETY',
  4: 'RECITATION',
  5: 'OTHER',
};

function geminiUsage(u) {
  if (!u) return { promptTokenCount: 0, candidatesTokenCount: 0, totalTokenCount: 0 };
  const out = {
    promptTokenCount: Number(u.promptTokenCount ?? 0),
    candidatesTokenCount: Number(u.candidatesTokenCount ?? 0),
    totalTokenCount: Number(u.totalTokenCount ?? 0),
  };
  for (const k of [
    'cachedContentTokenCount',
    'toolUsePromptTokenCount',
    'thoughtsTokenCount',
    'promptTokensDetails',
    'cacheTokensDetails',
    'candidatesTokensDetails',
    'toolUsePromptTokensDetails',
  ]) {
    if (u[k] != null) out[k] = u[k];
  }
  return out;
}

// ---------------------------------------------------------------------------
// 执行器注册表与在途请求
// ---------------------------------------------------------------------------
const executors = new Map(); // apiKey -> { ws, connectedAt, clientVersion, executorType }
const pending = new Map(); // requestId -> entry
const EXECUTOR_WAIT_MS = 120_000; // 无执行器时等待其上线
const REQUEST_TIMEOUT_MS = 10 * 60_000; // 总超时（脚本硬超时 15 分钟，留余量）

function executorOnline() {
  const e = executors.get(API_KEY);
  return !!(e && e.ws.readyState === 1);
}

function sendFrame(ws, obj) {
  try {
    if (ws.readyState === 1) ws.send(JSON.stringify(obj));
  } catch {}
}

function settleCleanup(entry) {
  entry.done = true;
  pending.delete(entry.requestId);
  clearTimeout(entry.timer);
  if (entry.waitIv) clearInterval(entry.waitIv);
}

function failPending(entry, status, code, message) {
  if (entry.done) return;
  settleCleanup(entry);
  entry.handlers.onError(status, { code, message }, entry);
}

function completePending(entry) {
  if (entry.done) return;
  settleCleanup(entry);
  entry.handlers.onComplete(entry);
}

/**
 * 把一个逻辑请求交给执行器执行。
 * handlers: { onChunk(piece, entry), onError(status, err, entry), onComplete(entry) }
 * 执行器断线时在途请求不立即失败：保留到各自超时，迟到帧按 requestId 收养
 * （脚本重连后会 outbox 补发）。
 */
function dispatchRequest({ model, stream, wireBodyObj, handlers }) {
  const requestId = crypto.randomUUID();
  const entry = {
    requestId,
    model,
    stream: !!stream,
    rawText: '',
    done: false,
    dispatched: false,
    createdAt: Date.now(),
    handlers,
    frame: {
      type: 'request',
      requestId,
      model,
      stream: !!stream,
      requestBody: JSON.stringify(wireBodyObj),
    },
    timer: null,
    waitIv: null,
  };
  entry.timer = setTimeout(() => {
    failPending(entry, 504, 'request_timeout', '执行器超时未返回（10 分钟）');
  }, REQUEST_TIMEOUT_MS);
  pending.set(requestId, entry);

  entry.tryDispatch = () => {
    if (entry.done || entry.dispatched) return;
    const ex = executors.get(API_KEY);
    if (ex && ex.ws.readyState === 1) {
      entry.dispatched = true;
      if (entry.waitIv) {
        clearInterval(entry.waitIv);
        entry.waitIv = null;
      }
      sendFrame(ex.ws, entry.frame);
    }
  };
  entry.tryDispatch();

  if (!entry.dispatched) {
    const waitDeadline = Date.now() + EXECUTOR_WAIT_MS;
    entry.waitIv = setInterval(() => {
      if (entry.done || entry.dispatched) return;
      entry.tryDispatch();
      if (!entry.dispatched && Date.now() > waitDeadline) {
        failPending(entry, 503, 'executor_offline', '执行器不在线：请先在浏览器打开 AI Studio 页面并确认脚本面板显示已连接');
      }
    }, 500);
  }
  return entry;
}

function cancelRequest(entry) {
  if (entry.done) return;
  settleCleanup(entry);
  const ex = executors.get(API_KEY);
  if (ex && ex.ws.readyState === 1) {
    sendFrame(ex.ws, { type: 'cancel', requestId: entry.requestId });
  }
}

// 执行器结果帧路由（按 requestId 收养，与帧来自哪条连接无关）
function routeResultFrame(msg) {
  const entry = pending.get(msg.requestId);
  if (!entry || entry.done) return; // 迟到帧无主：丢弃
  if (msg.type === 'response') {
    entry.rawText = String(msg.rawResponse || '');
    if (Number(msg.status) && Number(msg.status) !== 200) {
      failPending(entry, Number(msg.status), 'upstream_error', entry.rawText.slice(0, 500) || '上游错误');
      return;
    }
    completePending(entry);
  } else if (msg.type === 'stream-chunk') {
    const piece = String(msg.chunkBody || '');
    entry.rawText += piece;
    entry.handlers.onChunk(piece, entry);
  } else if (msg.type === 'stream-end') {
    if (Number(msg.status) && Number(msg.status) !== 200) {
      failPending(entry, Number(msg.status), 'upstream_error', '上游流式错误');
      return;
    }
    completePending(entry);
  } else if (msg.type === 'error') {
    const status = Number(msg.status) || 502;
    const err = msg.error || {};
    failPending(entry, status, err.code || 'executor_error', err.message || '执行器错误');
  }
}

// ---------------------------------------------------------------------------
// wire 构造：Gemini/OpenAI 输入 -> { wire, binding }
// binding 为 wire（proof 槽位置 null）的 JSON 串，脚本侧 sha256 后由页面
// WAA snapshot 现铸 proof 注入 wire[4]。
// ---------------------------------------------------------------------------
function buildWireBodyObj({ model, payload, generationConfig, tools }) {
  const wireContents = buildWireContentsFromPayload(payload);
  if (!Array.isArray(wireContents) || wireContents.length === 0) {
    const err = new Error('请求缺少有效内容（messages / contents 为空）');
    err.httpStatus = 400;
    throw err;
  }
  const wireJson = buildGenerateWireBody({
    model,
    wireContents,
    tools: tools || null,
    proof: '',
    generationConfig: generationConfig || {},
  });
  const wire = JSON.parse(wireJson);
  wire[4] = null; // proof 槽位：脚本注入
  const binding = JSON.stringify(wire);
  return { wire, binding };
}

// ---------------------------------------------------------------------------
// HTTP 工具
// ---------------------------------------------------------------------------
function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-API-Key, X-Goog-Api-Key',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  });
  res.end(body);
}

function readJsonBody(req, maxBytes = 16 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > maxBytes) {
        reject(Object.assign(new Error('请求体过大'), { httpStatus: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8');
      if (!text) return resolve({});
      try {
        resolve(JSON.parse(text));
      } catch {
        reject(Object.assign(new Error('请求体不是有效 JSON'), { httpStatus: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function extractKey(req, url) {
  const auth = req.headers['authorization'] || '';
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  if (req.headers['x-api-key']) return String(req.headers['x-api-key']).trim();
  if (req.headers['x-goog-api-key']) return String(req.headers['x-goog-api-key']).trim();
  if (url && url.searchParams.get('key')) return url.searchParams.get('key');
  return '';
}

function requireAuth(req, res, url) {
  const key = extractKey(req, url);
  if (key && key === API_KEY) return true;
  sendJson(res, 401, {
    error: { message: 'API Key 无效或缺失', type: 'invalid_request_error', code: 'invalid_api_key' },
  });
  return false;
}

// 管理台鉴权：站长口令已设置 → 只认站长会话；未设置 → 沿用服务 Key。
function requireConsoleAuth(req, res, url) {
  if (ownerRecord) {
    if (ownerSessionValid(req)) return true;
    sendJson(res, 401, {
      error: {
        message: '管理台需要站长登录（服务 Key 只能调 API，不能进管理台）',
        type: 'invalid_request_error',
        code: 'owner_required',
      },
    });
    return false;
  }
  return requireAuth(req, res, url);
}

function openAiError(res, status, message, code) {
  if (res.headersSent) {
    try { res.end(); } catch {}
    return;
  }
  sendJson(res, status, { error: { message, type: 'relay_error', code: code || 'relay_error' } });
}

function publicBase(req) {
  try {
    const u = fs.readFileSync(CURRENT_URL_PATH, 'utf8').trim();
    if (u) return u.replace(/\/$/, '');
  } catch {}
  if (req) {
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    const proto = req.headers['x-forwarded-proto'] || 'http';
    if (host) return `${proto}://${host}`;
  }
  return `http://127.0.0.1:${PORT}`;
}

// ---------------------------------------------------------------------------
// 二期：AIStudio2API（pg-headless）客户端
// - 生成 API 走内部 PROXY_API_KEY（X-API-Key 头）
// - 管理 API 走管理员会话 Cookie（登录一次缓存，401 时重登一次）
// ---------------------------------------------------------------------------
let hlCookie = null;
let hlLoginInflight = null;

async function hlLogin() {
  if (hlLoginInflight) return hlLoginInflight;
  hlLoginInflight = (async () => {
    const r = await fetch(`${HL_BASE}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: HEADLESS.adminUsername,
        password: HEADLESS.adminPassword,
        remember: true,
      }),
      signal: AbortSignal.timeout(10_000),
    });
    if (!r.ok) throw new Error(`pg-headless 管理登录失败: HTTP ${r.status}`);
    const setCookies = typeof r.headers.getSetCookie === 'function' ? r.headers.getSetCookie() : [];
    const pair = setCookies.map((c) => c.split(';')[0]).find((c) => c.startsWith('aistudio_admin='));
    if (!pair) throw new Error('pg-headless 管理登录未返回会话 Cookie');
    hlCookie = pair;
    return hlCookie;
  })();
  try {
    return await hlLoginInflight;
  } finally {
    hlLoginInflight = null;
  }
}

async function hlAdmin(pathname, opts = {}) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      if (!hlCookie) await hlLogin();
      const r = await fetch(`${HL_BASE}${pathname}`, {
        ...opts,
        headers: { 'Content-Type': 'application/json', ...(opts.headers || {}), Cookie: hlCookie },
        signal: opts.signal || AbortSignal.timeout(20_000),
      });
      if (r.status === 401 && attempt === 0) {
        hlCookie = null;
        continue;
      }
      return r;
    } catch (e) {
      lastErr = e;
      if (attempt === 0) {
        hlCookie = null;
        continue;
      }
    }
  }
  throw lastErr || new Error('pg-headless 管理接口不可达');
}

let hlAccountsCache = { at: 0, accounts: null, up: false };
async function hlAccounts(force = false) {
  if (!HEADLESS) return { up: false, accounts: [] };
  if (!force && hlAccountsCache.accounts && Date.now() - hlAccountsCache.at < 15_000) {
    return hlAccountsCache;
  }
  try {
    const r = await hlAdmin('/api/accounts');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    hlAccountsCache = { at: Date.now(), accounts: Array.isArray(d.accounts) ? d.accounts : [], up: true };
  } catch (e) {
    hlAccountsCache = { at: Date.now(), accounts: hlAccountsCache.accounts, up: false };
  }
  return hlAccountsCache;
}

async function hlServiceStatus() {
  const r = await hlAdmin('/api/status');
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

// 无头写操作失败时提取上游错误面（{error:{code,message}} 或纯文本）
async function hlAdminError(r, what) {
  const text = await r.text().catch(() => '');
  let msg = '';
  try {
    const d = JSON.parse(text);
    if (d && d.error) msg = typeof d.error === 'string' ? d.error : String(d.error.message || d.error.code || '');
    if (!msg && d && d.message) msg = String(d.message);
  } catch {}
  const e = new Error(`${what}失败: ${msg || text.slice(0, 200) || `HTTP ${r.status}`}`);
  e.httpStatus = r.status === 404 ? 404 : 502;
  return e;
}

function hlFindAccount(accounts, id) {
  const needle = String(id || '').toLowerCase();
  return (accounts || []).find(
    (a) => a && (a.id === id || String(a.label || '').toLowerCase() === needle),
  ) || null;
}

// 启用/禁用：无头的 PUT /api/accounts/{id} 是整表保存（label 必须等于 id=邮箱，
// 且 locale/timezone 不能为空），所以先读回账户现值、只翻 enabled 再整表提交，
// 避免把代理/语言/时区洗掉。已是目标状态时幂等返回、不发写请求。
async function hlSetAccountEnabled(id, enabled) {
  const acc = await hlAccounts(true);
  if (!acc.up) {
    const e = new Error('无头执行服务未就绪，无法操作账户');
    e.httpStatus = 502;
    throw e;
  }
  const target = hlFindAccount(acc.accounts, id);
  if (!target) {
    const e = new Error(`账户不存在：${id}`);
    e.httpStatus = 404;
    throw e;
  }
  if (!!target.enabled === !!enabled) return target;
  const key = target.id || target.label;
  const r = await hlAdmin(`/api/accounts/${encodeURIComponent(key)}`, {
    method: 'PUT',
    body: JSON.stringify({
      label: target.label || key,
      enabled: !!enabled,
      proxy: target.proxy || '',
      locale: target.locale || 'en-US',
      timezone: target.timezone || 'UTC',
    }),
    signal: AbortSignal.timeout(30_000),
  });
  if (!r.ok) throw await hlAdminError(r, enabled ? '启用账户' : '禁用账户');
  const d = await r.json().catch(() => null);
  hlAccountsCache = { at: 0, accounts: null, up: false };
  return (d && d.account) || target;
}

// 改代理立即生效：无头 PUT /api/accounts/{id} 整表保存时，只要 proxy 与现值
// 不同，上游 saveAccountConfig（internal/app/admin.go）会走
// workers.prepareUpdate 关掉旧 Worker、按新配置发布 Worker 参数
// （workerConfigFor 优先取 config.Proxy，internal/app/worker_manager.go），
// 下一次请求即以新代理冷启动——所以照启用/禁用的读-改-写整表回写即可让新
// 代理立即生效，不需要禁用→启用一轮，也不需要重新推送 Cookie。
// 只换 proxy，enabled/locale/timezone 全部读现值回填，避免洗掉其他配置。
// onlyIfCurrent 给定时，仅当账户现用代理正是它才写（清除绑定时用，避免误洗
// 流量池分配给账户的代理）。任何失败都如实返回 { applied:false, reason }。
async function hlSetAccountProxy(id, proxy, onlyIfCurrent) {
  const acc = await hlAccounts(true);
  if (!acc.up) return { applied: false, reason: '无头执行服务未就绪' };
  const target = hlFindAccount(acc.accounts, id);
  if (!target) return { applied: false, reason: 'account_not_found' };
  if (onlyIfCurrent !== undefined && (target.proxy || '') !== onlyIfCurrent) {
    return { applied: false, reason: '账户当前代理不是这条绑定（可能来自流量池分配），未改动账户配置' };
  }
  if ((target.proxy || '') === proxy) return { applied: true, unchanged: true, account: target };
  const key = target.id || target.label;
  try {
    const r = await hlAdmin(`/api/accounts/${encodeURIComponent(key)}`, {
      method: 'PUT',
      body: JSON.stringify({
        label: target.label || key,
        enabled: !!target.enabled,
        proxy,
        locale: target.locale || 'en-US',
        timezone: target.timezone || 'UTC',
      }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!r.ok) {
      const e = await hlAdminError(r, '写入账户代理');
      return { applied: false, reason: e.message };
    }
    const d = await r.json().catch(() => null);
    hlAccountsCache = { at: 0, accounts: null, up: false };
    return { applied: true, account: (d && d.account) || target };
  } catch (e) {
    return { applied: false, reason: e.message || '写入账户代理失败' };
  }
}

// 删除：无头 DELETE /api/accounts/{id} 连账户目录一起删，不可恢复
// （恢复只能重新推送 Cookie）。
async function hlDeleteAccount(id) {
  const acc = await hlAccounts(true);
  if (!acc.up) {
    const e = new Error('无头执行服务未就绪，无法操作账户');
    e.httpStatus = 502;
    throw e;
  }
  const target = hlFindAccount(acc.accounts, id);
  if (!target) {
    const e = new Error(`账户不存在：${id}`);
    e.httpStatus = 404;
    throw e;
  }
  const key = target.id || target.label;
  const r = await hlAdmin(`/api/accounts/${encodeURIComponent(key)}`, {
    method: 'DELETE',
    signal: AbortSignal.timeout(30_000),
  });
  if (r.status !== 204 && !r.ok) throw await hlAdminError(r, '删除账户');
  hlAccountsCache = { at: 0, accounts: null, up: false };
  return target;
}

// 确保生成服务在跑（导入成功后调用；零账户时上游会报错，吞掉即可）
async function hlEnsureStarted() {
  try {
    const st = await hlServiceStatus();
    if (st && st.running) return true;
  } catch {}
  try {
    const r = await hlAdmin('/api/control/start', { method: 'POST' });
    if (r.ok) {
      const st = await r.json().catch(() => null);
      return !!(st && st.running);
    }
  } catch {}
  return false;
}

async function hlImportCookies({ email, cookiesText, authUser, proxy, locale, timezone }) {
  const r = await hlAdmin('/api/accounts/import/cookies', {
    method: 'POST',
    body: JSON.stringify({
      items: [{ email: email || '', cookies: cookiesText, auth_user: authUser || 0 }],
      proxy: proxy || '',
      locale,
      timezone,
    }),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await r.text();
  let d = null;
  try { d = JSON.parse(text); } catch {}
  if (r.status !== 202 || !d || !d.task || !d.task.id) {
    const msg = (d && d.error && (d.error.message || d.error.code)) || text.slice(0, 300) || `HTTP ${r.status}`;
    throw new Error(`导入提交失败: ${msg}`);
  }
  return d.task.id;
}

async function hlGetTask(taskId) {
  const r = await hlAdmin('/api/accounts/tasks');
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const d = await r.json();
  return (d.tasks || []).find((t) => t.id === taskId) || null;
}

async function hlWaitTask(taskId, budgetMs) {
  const deadline = Date.now() + budgetMs;
  for (;;) {
    let task = null;
    try {
      task = await hlGetTask(taskId);
    } catch {}
    if (task && (task.state === 'done' || task.state === 'failed')) return task;
    if (Date.now() >= deadline) return null;
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
}

function validLocale(v) {
  const s = String(v || '').trim();
  return /^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$/.test(s) ? s : null;
}
function validTimezone(v) {
  const s = String(v || '').trim();
  if (!s) return null;
  try {
    Intl.DateTimeFormat('en-US', { timeZone: s });
    return s;
  } catch {
    return null;
  }
}

// 导入超过脚本 30s 预算时先回执、后台继续等结果（见 handlePggoPush 的说明）
function finishPushInBackground(taskId, pushId) {
  (async () => {
    const task = await hlWaitTask(taskId, 5 * 60_000).catch(() => null);
    if (!pggoState.lastPush || pggoState.lastPush.id !== pushId) return;
    if (task && task.state === 'done') {
      const item = (task.items || [])[0] || {};
      const label = item.label || pggoState.lastPush.email || 'account';
      const usedProxy = pggoState.lastPush.proxy || '';
      pggoState.lastPush.status = 'ok';
      pggoState.lastPush.email = label;
      pggoState.lastPush.message = '导入成功（后台完成）';
      if (usedProxy && pggoState.lastPush.proxySource !== 'pool') pggoState.proxies.byEmail[label] = usedProxy;
      savePggoState();
      hlAccountsCache.at = 0;
      hlEnsureStarted().catch(() => {});
    } else if (task && task.state === 'failed') {
      const item = (task.items || [])[0] || {};
      pggoState.lastPush.status = 'failed';
      pggoState.lastPush.message = item.error || '导入失败';
      savePggoState();
    } else {
      pggoState.lastPush.status = 'failed';
      pggoState.lastPush.message = '导入任务超时未完成，请在管理台确认账户状态或重新推送';
      savePggoState();
    }
  })().catch(() => {});
}

// ---------------------------------------------------------------------------
// 二期：脚本 Cookie 推送适配（POST /api/pggo/push）
// 脚本侧协议（v2.21.0 源码核对）：X-API-Key 头、30s 超时；
// 成功要 200 + {label, cookies, created_at}；失败读 error.message / detail。
// 时间对策：同步等导入结果最多 ~24s（常见失败——未登录/缺件/被拒——都在
// 识别环节快速返回，能如实透传给脚本）；真号导入含模型目录同步可能更慢，
// 超预算则先按成功形态回执（导入已在上游登记、不会丢），后台继续等，
// 最终成败写进 lastPush 供管理台展示——脚本与管理台不一致时以管理台为准。
// ---------------------------------------------------------------------------
async function handlePggoPush(req, res, url) {
  if (!requireAuth(req, res, url)) return;
  if (!HEADLESS) {
    return sendJson(res, 501, {
      error: { message: '无头模式尚未部署（本机缺少 headless.json 配置）', code: 'headless_not_configured' },
    });
  }
  let body;
  try {
    body = await readJsonBody(req);
  } catch (e) {
    return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
  }
  const cookies = Array.isArray(body.cookies) ? body.cookies.filter((c) => c && c.name && c.value) : [];
  if (!cookies.length) {
    return sendJson(res, 400, { error: { message: '未收到任何 Cookie', code: 'no_cookies' } });
  }
  const names = new Set(cookies.map((c) => c.name));
  const required = ['__Secure-1PSID', 'SAPISID', '__Secure-1PAPISID', '__Secure-3PAPISID'];
  const missing = required.filter((n) => !names.has(n));
  if (missing.length) {
    return sendJson(res, 400, {
      error: { message: `缺少必需 Cookie: ${missing.join(', ')}（请确认已登录 AI Studio 且脚本 Cookie 权限为 all）`, code: 'missing_cookies' },
    });
  }
  const email = typeof body.email === 'string' ? body.email.trim() : '';
  const au = parseInt(body.authuser, 10);
  const authUser = Number.isFinite(au) && au > 0 ? au : 0;
  const fp = body.fingerprint && typeof body.fingerprint === 'object' ? body.fingerprint : {};
  // locale + timezone 必须成对给出：AIStudio2API 在两者齐备时跳过已失效的
  // GeoIP 库（尖刺实证）；指纹缺失时用默认值兜底，保证不触发查库路径。
  const locale = validLocale(fp.language) || 'en-US';
  const timezone = validTimezone(fp.timezone) || 'UTC';
  const picked = pickProxyForEmail(email);
  const proxy = picked.proxy;

  const createdAt = Math.floor(Date.now() / 1000);
  const pushId = `${createdAt}-${crypto.randomBytes(4).toString('hex')}`;
  pggoState.lastPush = {
    id: pushId,
    at: createdAt,
    email: email || null,
    cookies: cookies.length,
    proxy,
    proxySource: picked.source,
    status: 'importing',
    message: '正在导入',
  };
  savePggoState();

  let taskId;
  try {
    taskId = await hlImportCookies({
      email,
      cookiesText: JSON.stringify(cookies),
      authUser,
      proxy,
      locale,
      timezone,
    });
  } catch (e) {
    pggoState.lastPush.status = 'failed';
    pggoState.lastPush.message = String((e && e.message) || e);
    savePggoState();
    return sendJson(res, 502, { error: { message: pggoState.lastPush.message, code: 'import_submit_failed' } });
  }

  const task = await hlWaitTask(taskId, 24_000).catch(() => null);
  if (task && task.state === 'done') {
    const item = (task.items || [])[0] || {};
    const label = item.label || email || 'account';
    pggoState.lastPush.status = 'ok';
    pggoState.lastPush.email = label;
    pggoState.lastPush.message = '导入成功';
    // 池分配的代理已由 poolAssignments 固定，不再回写 byEmail（删池条目时能干净解绑）
    if (proxy && picked.source !== 'pool') pggoState.proxies.byEmail[label] = proxy;
    savePggoState();
    hlAccountsCache.at = 0;
    hlEnsureStarted().catch(() => {});
    return sendJson(res, 200, { label, cookies: cookies.length, created_at: createdAt });
  }
  if (task && task.state === 'failed') {
    const item = (task.items || [])[0] || {};
    const msg = item.error || '导入失败';
    pggoState.lastPush.status = 'failed';
    pggoState.lastPush.message = msg;
    savePggoState();
    return sendJson(res, 400, { error: { message: msg, code: item.code || 'import_failed' } });
  }
  // 24s 内未定：导入任务已在上游运行，先按脚本期望形态回执，后台跟进。
  finishPushInBackground(taskId, pushId);
  return sendJson(res, 200, {
    label: email || 'account',
    cookies: cookies.length,
    created_at: createdAt,
    pending: true,
  });
}

// ---------------------------------------------------------------------------
// 手动导入 Cookie（管理台站长专用）：iOS 等无法跑脚本推送的设备，在
// 管理台把 Cookie 文本粘进来直达导入。规范化成与脚本推送 body.cookies
// 相同的 [{name, value, ...}] 形态后，走与 handlePggoPush 完全相同的
// 导入路径（pickProxyForEmail → hlImportCookies → hlWaitTask），上游
// 结果/拒绝原文如实带回。Cookie 只在内存中过手：不写日志、不落状态
// 文件（lastPush 只记条数，与推送一致）。
// ---------------------------------------------------------------------------
function parseManualCookies(raw) {
  const text = String(raw || '').trim();
  if (!text) return { error: 'Cookie 内容为空：先把 Cookie 粘贴进来再点导入' };
  let arr = null;
  let upstreamText = null;
  if (text.startsWith('[') || text.startsWith('{')) {
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      return { error: 'JSON 解析失败：请粘贴完整的 Cookie JSON 数组，或 a=b; c=d 形式的 Cookie 文本' };
    }
    if (Array.isArray(parsed)) arr = parsed;
    else if (parsed && Array.isArray(parsed.cookies)) arr = parsed.cookies;
    else return { error: '这段 JSON 里没有 Cookie 数组：应为 [{name, value, ...}] 形式' };
  } else if (text.includes('\t')) {
    // Netscape cookies.txt：上游原生能解析，原文直达；这里只抽 name/value
    // 做本层的缺件检查与计数。
    arr = [];
    for (const line of text.split(/\r?\n/)) {
      const f = line.replace(/^#HttpOnly_/, '').split('\t');
      if (f.length === 7 && f[5] && f[6]) arr.push({ name: f[5], value: f[6] });
    }
    if (!arr.length) {
      return { error: '无法识别 Cookie 格式：应为 a=b; c=d; ... 形式，或 Cookie JSON 数组' };
    }
    upstreamText = text;
  } else {
    // Cookie 头字符串：a=b; c=d; ...（value 里可以有 =，按第一个 = 切）
    arr = [];
    for (const part of text.split(';')) {
      const i = part.indexOf('=');
      if (i <= 0) continue;
      const name = part.slice(0, i).trim();
      const value = part.slice(i + 1).trim();
      if (name && value) arr.push({ name, value });
    }
    if (!arr.length) {
      return { error: '无法识别 Cookie 格式：应为 a=b; c=d; ... 形式，或 Cookie JSON 数组' };
    }
  }
  const cookies = arr
    .filter((c) => c && typeof c === 'object' && c.name != null && String(c.name).trim() && c.value != null && String(c.value) !== '')
    .map((c) => ({ ...c, name: String(c.name).trim(), value: String(c.value) }));
  if (!cookies.length) return { error: '没有解析出有效的 Cookie 条目（每条需要 name 和 value）' };
  if (upstreamText == null) {
    if (text.startsWith('[') || text.startsWith('{')) {
      // 上游 JSON 解析会丢弃没有 domain 的条目：缺 domain 的补 .google.com
      // （与上游对头字符串的默认归属一致），完整导出对象原样保留。
      upstreamText = JSON.stringify(cookies.map((c) => (
        typeof c.domain === 'string' && c.domain.trim() ? c : { ...c, domain: '.google.com' }
      )));
    } else {
      // 头字符串重组成一行干净文本交给上游原生解析（它给每条补
      // .google.com 归属）；直接 JSON 化会因缺 domain 被上游整批丢弃。
      upstreamText = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
    }
  }
  return { cookies, upstreamText };
}

async function handleConsolePggoImport(req, res, url) {
  if (!requireConsoleAuth(req, res, url)) return;
  if (!HEADLESS) {
    return sendJson(res, 501, {
      error: { message: '无头模式尚未部署（本机缺少 headless.json 配置）', code: 'headless_not_configured' },
    });
  }
  let body;
  try {
    body = await readJsonBody(req);
  } catch (e) {
    return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
  }
  const parsed = parseManualCookies(typeof body.cookie === 'string' ? body.cookie : '');
  if (parsed.error) {
    return sendJson(res, 400, { error: { message: parsed.error, code: 'invalid_cookie_format' } });
  }
  const cookies = parsed.cookies;
  const names = new Set(cookies.map((c) => c.name));
  const required = ['__Secure-1PSID', 'SAPISID', '__Secure-1PAPISID', '__Secure-3PAPISID'];
  const missing = required.filter((n) => !names.has(n));
  if (missing.length) {
    return sendJson(res, 400, {
      error: { message: `缺少必需 Cookie: ${missing.join(', ')}（请确认已登录 AI Studio 且复制的是完整 Cookie）`, code: 'missing_cookies' },
    });
  }
  // 邮箱要等上游导入完成才知道（label）；先按空邮箱选代理（与推送时邮箱
  // 为空同口径：默认代理优先），导入成功后再按 label 走池分配补齐。
  const picked = pickProxyForEmail('');
  let proxy = picked.proxy;

  const createdAt = Math.floor(Date.now() / 1000);
  const pushId = `${createdAt}-${crypto.randomBytes(4).toString('hex')}`;
  pggoState.lastPush = {
    id: pushId,
    at: createdAt,
    email: null,
    cookies: cookies.length,
    proxy,
    proxySource: picked.source,
    status: 'importing',
    message: '正在导入（手动导入）',
  };
  savePggoState();

  let taskId;
  try {
    taskId = await hlImportCookies({
      email: '',
      cookiesText: parsed.upstreamText,
      authUser: 0,
      proxy,
      locale: 'en-US',
      timezone: 'UTC',
    });
  } catch (e) {
    pggoState.lastPush.status = 'failed';
    pggoState.lastPush.message = String((e && e.message) || e);
    savePggoState();
    return sendJson(res, 502, { error: { message: pggoState.lastPush.message, code: 'import_submit_failed' } });
  }

  const task = await hlWaitTask(taskId, 24_000).catch(() => null);
  if (task && task.state === 'done') {
    const item = (task.items || [])[0] || {};
    const label = item.label || 'account';
    pggoState.lastPush.status = 'ok';
    pggoState.lastPush.email = label;
    pggoState.lastPush.message = '导入成功（手动导入）';
    if (proxy && picked.source !== 'pool') pggoState.proxies.byEmail[label] = proxy;
    savePggoState();
    hlAccountsCache.at = 0;
    hlEnsureStarted().catch(() => {});
    // 导入前不知道邮箱、没走成池分配的，导入后按 label 补一次池分配并
    // 立即写入账户（与推送「池轮换、一账户固定一个」口径一致）。
    let proxyNote = '';
    if (picked.source === 'none') {
      const picked2 = pickProxyForEmail(label);
      if (picked2.proxy) {
        const r = await hlSetAccountProxy(label, picked2.proxy);
        if (r.applied) {
          proxy = picked2.proxy;
          proxyNote = '，已分配池代理并写入账户';
          pggoState.lastPush.proxy = proxy;
          pggoState.lastPush.proxySource = picked2.source;
          savePggoState();
        }
      }
    }
    return sendJson(res, 200, {
      ok: true,
      label,
      cookies: cookies.length,
      proxy,
      message: `导入成功：${label}（${cookies.length} 条 Cookie）${proxyNote}`,
    });
  }
  if (task && task.state === 'failed') {
    const item = (task.items || [])[0] || {};
    const msg = item.error || '导入失败';
    pggoState.lastPush.status = 'failed';
    pggoState.lastPush.message = msg;
    savePggoState();
    return sendJson(res, 400, { error: { message: msg, code: item.code || 'import_failed' } });
  }
  // 24s 内未定：与推送同规矩，先回执、后台跟进，最终结果写进 lastPush。
  finishPushInBackground(taskId, pushId);
  return sendJson(res, 200, {
    ok: true,
    pending: true,
    label: 'account',
    cookies: cookies.length,
    message: '导入已提交，正在后台完成；稍后刷新总览查看账户与最终结果。',
  });
}

// ---------------------------------------------------------------------------
// 二期：pggo- 模型转发到本机 AIStudio2API（流式/非流式统一原样透传）
// ---------------------------------------------------------------------------
function isPggoModel(raw) {
  return String(raw || '').trim().replace(/^models\//, '').startsWith('pggo-');
}

// 转发前检查：未部署/服务不可达/零账户分别给出明确错误面。
// 通过返回 true；失败已自行回错并返回 { status, message }（供用量记录）。
async function pggoPreflight(res, geminiStyle) {
  const fail = (status, message, code) => {
    if (geminiStyle) {
      sendJson(res, status, { error: { code: status, message, status: 'INTERNAL' } });
    } else {
      openAiError(res, status, message, code);
    }
    return { status, message };
  };
  if (!HEADLESS) {
    return fail(501, '无头模式尚未部署（本机缺少 headless.json 配置）', 'headless_not_configured');
  }
  const acc = await hlAccounts();
  if (!acc.up) {
    return fail(502, '无头执行服务（pg-headless）未就绪，请稍后重试或联系管理员查看服务状态', 'headless_unavailable');
  }
  if (!acc.accounts || acc.accounts.length === 0) {
    return fail(
      503,
      '尚未导入任何 AI Studio 账户（pggo 无头模式）：请先在浏览器打开 AI Studio 页面，用脚本菜单「推送 Cookie 到云端 (pggo)」推送一次（建议用小号），再重试',
      'account_required',
    );
  }
  if (!acc.accounts.some((a) => a && a.enabled !== false)) {
    return fail(
      503,
      '已导入的 AI Studio 账户全部处于禁用状态：请在管理台「云端无头模式（pggo）」里启用至少一个账户后再试',
      'account_disabled',
    );
  }
  return true;
}

async function forwardToHeadless(req, res, upstreamPath, payload, rec) {
  const controller = new AbortController();
  let done = false;
  res.on('close', () => {
    if (!done) controller.abort();
  });
  let upstream;
  try {
    upstream = await fetch(`${HL_BASE}${upstreamPath}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': HEADLESS.apiKey },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
  } catch (e) {
    if (controller.signal.aborted) {
      if (rec) rec(false, 499, 'client_disconnected');
      return;
    }
    if (rec) rec(false, 502, `无头执行服务不可达: ${e.message}`);
    return openAiError(res, 502, `无头执行服务不可达: ${e.message}`, 'headless_unavailable');
  }
  const headers = { 'Access-Control-Allow-Origin': '*' };
  const ct = upstream.headers.get('content-type');
  if (ct) headers['Content-Type'] = ct;
  const isSse = !!(ct && ct.includes('text/event-stream'));
  if (isSse) {
    headers['Cache-Control'] = 'no-cache, no-transform';
    headers['Connection'] = 'keep-alive';
    headers['X-Accel-Buffering'] = 'no';
  }
  const upOk = upstream.status >= 200 && upstream.status < 300;
  // 非流式报错：先把上游小体积错误体读下来，提取报错原因记入用量，再原样回给客户端
  if (!upOk && !isSse && upstream.body) {
    let text = '';
    try { text = await upstream.text(); } catch {}
    done = true;
    let msg = '';
    try {
      const d = JSON.parse(text);
      if (d && d.error) msg = typeof d.error === 'string' ? d.error : String(d.error.message || d.error.code || '');
      if (!msg && d && d.message) msg = String(d.message);
    } catch {}
    if (rec) rec(false, upstream.status, msg || `无头服务返回 HTTP ${upstream.status}`);
    try {
      res.writeHead(upstream.status, headers);
      res.end(text);
    } catch {}
    return;
  }
  res.writeHead(upstream.status, headers);
  if (!upstream.body) {
    done = true;
    res.end();
    if (rec) rec(upOk, upstream.status, upOk ? '' : `无头服务返回 HTTP ${upstream.status}`);
    return;
  }
  let readErr = false;
  try {
    const reader = upstream.body.getReader();
    for (;;) {
      const { done: rdDone, value } = await reader.read();
      if (rdDone) break;
      if (value && value.length) {
        if (rec) rec.markTtfb();
        res.write(Buffer.from(value));
      }
    }
  } catch {
    // 客户端断开或上游中断：直接收尾
    readErr = true;
  } finally {
    done = true;
    try { res.end(); } catch {}
    if (rec) {
      if (controller.signal.aborted) rec(false, upstream.status, 'client_disconnected');
      else if (readErr) rec(false, upstream.status, '上游流中断');
      else rec(upOk, upstream.status, upOk ? '' : `无头服务返回 HTTP ${upstream.status}`);
    }
  }
}

async function handlePggoChat(req, res, body) {
  const rec = makeUsageRecorder(body && body.model, !!(body && body.stream));
  const pf = await pggoPreflight(res, false);
  if (pf !== true) {
    rec(false, pf.status, pf.message);
    return;
  }
  // 生成服务没在跑时先拉起一次（导入后已自动处理，这里兜底）
  try {
    const st = await hlServiceStatus().catch(() => null);
    if (st && !st.running) await hlEnsureStarted();
  } catch {}
  const payload = { ...body, model: stripModelPrefix(body.model) };
  return forwardToHeadless(req, res, '/v1/chat/completions', payload, rec);
}

async function handlePggoGemini(req, res, url, modelRaw, action, body) {
  const rec = makeUsageRecorder(modelRaw, action === 'streamGenerateContent');
  const pf = await pggoPreflight(res, true);
  if (pf !== true) {
    rec(false, pf.status, pf.message);
    return;
  }
  try {
    const st = await hlServiceStatus().catch(() => null);
    if (st && !st.running) await hlEnsureStarted();
  } catch {}
  const model = stripModelPrefix(modelRaw);
  const params = new URLSearchParams(url.search);
  params.delete('key');
  if (action === 'streamGenerateContent' && !params.has('alt')) params.set('alt', 'sse');
  const qs = params.toString();
  return forwardToHeadless(req, res, `/v1beta/models/${encodeURIComponent(model)}:${action}${qs ? `?${qs}` : ''}`, body, rec);
}

async function hlModelIds() {
  if (!HEADLESS) return [];
  try {
    const r = await fetch(`${HL_BASE}/v1/models`, {
      headers: { 'X-API-Key': HEADLESS.apiKey },
      signal: AbortSignal.timeout(3000),
    });
    if (r.ok) {
      const d = await r.json();
      const ids = (d.data || []).map((m) => m && m.id).filter(Boolean);
      if (ids.length) return ids;
    }
  } catch {}
  // 生成服务未启动/零账户时上游列不出模型：用与 pg- 相同的静态目录兜底
  return BASE_MODELS.map((m) => m.id);
}

async function hlStatusSummary() {
  if (!HEADLESS) return { enabled: false };
  const acc = await hlAccounts();
  let service = null;
  try {
    const st = await hlServiceStatus();
    service = { running: !!st.running, state: st.state || null, accounts: st.accounts || null };
  } catch {}
  return {
    enabled: true,
    up: acc.up,
    service,
    accounts: (acc.accounts || []).map((a) => ({
      id: a.id || a.label,
      label: a.label,
      state: a.state,
      enabled: a.enabled,
      proxy: a.proxy || '',
      egress: a.egress || '',
      locale: a.locale || '',
      timezone: a.timezone || '',
      modelCount: Array.isArray(a.models) ? a.models.length : 0,
      message: a.message || '',
    })),
    lastPush: pggoState.lastPush
      ? {
          at: pggoState.lastPush.at,
          email: pggoState.lastPush.email,
          cookies: pggoState.lastPush.cookies,
          status: pggoState.lastPush.status,
          message: pggoState.lastPush.message,
        }
      : null,
    proxies: { default: pggoState.proxies.default, byEmail: pggoState.proxies.byEmail },
    pool: mergedPool(),
    poolAssignments: pggoState.poolAssignments,
  };
}

// ---------------------------------------------------------------------------
// OpenAI /v1/chat/completions
// ---------------------------------------------------------------------------
async function handleChatCompletions(req, res, url) {
  if (!requireAuth(req, res, url)) return;
  let body;
  try {
    body = await readJsonBody(req);
  } catch (e) {
    return openAiError(res, e.httpStatus || 400, e.message, 'invalid_body');
  }
  if (isPggoModel(body.model)) {
    return await handlePggoChat(req, res, body);
  }
  const rec = makeUsageRecorder(body.model, !!body.stream);
  const model = stripModelPrefix(body.model);
  const stream = !!body.stream;
  const generationConfig = {
    temperature: body.temperature,
    topP: body.top_p ?? body.topP,
    topK: body.top_k ?? body.topK,
    maxOutputTokens: body.max_tokens ?? body.max_completion_tokens ?? body.maxOutputTokens,
    reasoningEffort: body.reasoning_effort ?? body.reasoningEffort ?? body.reasoning?.effort,
    thinkingConfig: body.thinking_config || body.thinkingConfig,
    responseMimeType: body.response_format?.type === 'json_object' ? 'application/json' : undefined,
  };
  let wireBodyObj;
  try {
    wireBodyObj = buildWireBodyObj({
      model,
      payload: body,
      generationConfig,
      tools: body.tools || body.functions || null,
    });
  } catch (e) {
    rec(false, e.httpStatus || 400, e.message);
    return openAiError(res, e.httpStatus || 400, e.message, 'invalid_request');
  }

  const completionId = `chatcmpl-${crypto.randomBytes(12).toString('hex')}`;
  const created = Math.floor(Date.now() / 1000);

  if (!stream) {
    await new Promise((resolve) => {
      dispatchRequest({
        model,
        stream: false,
        wireBodyObj,
        handlers: {
          onChunk: () => {},
          onError: (status, err) => {
            rec(false, status, err.message);
            openAiError(res, status, err.message, err.code);
            resolve();
          },
          onComplete: (entry) => {
            try {
              const parsed = parseMakerSuiteResponse(entry.rawText);
              rec(true, 200, '');
              sendJson(res, 200, { ...toOpenAIChatCompletion(parsed, model), id: completionId, created });
            } catch (err) {
              rec(false, 502, `上游响应解析失败: ${err.message}`);
              openAiError(res, 502, `上游响应解析失败: ${err.message}`, 'parse_error');
            }
            resolve();
          },
        },
      });
    });
    return;
  }

  // 流式
  let headersSent = false;
  let finished = false;
  const ensureHeaders = () => {
    if (headersSent) return;
    headersSent = true;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      'Access-Control-Allow-Origin': '*',
    });
  };
  const writeChunk = (delta, finishReason) => {
    ensureHeaders();
    const chunk = {
      id: completionId,
      object: 'chat.completion.chunk',
      created,
      model,
      choices: [{ index: 0, delta, finish_reason: finishReason ?? null }],
    };
    res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  };
  let finishCode = null;
  const parser = new IncrementalJspbFrameParser((frame) => {
    const d = parseSingleMakerSuiteFrame(frame);
    const delta = {};
    if (d.deltaThought) delta.reasoning_content = d.deltaThought;
    if (d.deltaText) delta.content = d.deltaText;
    if (d.functionCalls && d.functionCalls.length > 0) {
      delta.tool_calls = d.functionCalls.map((item, idx) => {
        const fc = item.functionCall || item;
        return {
          index: idx,
          id: fc.id || `call_${crypto.randomBytes(6).toString('hex')}`,
          type: 'function',
          function: { name: String(fc.name || ''), arguments: JSON.stringify(fc.args || {}) },
        };
      });
    }
    if (delta.content || delta.reasoning_content || delta.tool_calls) {
      rec.markTtfb();
      writeChunk(delta, null);
    }
    if (d.finishReasonCode != null) finishCode = d.finishReasonCode;
  });

  const entry = dispatchRequest({
    model,
    stream: true,
    wireBodyObj,
    handlers: {
      onChunk: (piece) => {
        try { parser.feed(piece); } catch {}
      },
      onError: (status, err) => {
        if (finished) return;
        finished = true;
        rec(false, headersSent ? 200 : status, err.message);
        if (!headersSent) {
          openAiError(res, status, err.message, err.code);
        } else {
          res.write(`data: ${JSON.stringify({ error: { message: err.message, type: 'relay_error', code: err.code } })}\n\n`);
          res.end();
        }
      },
      onComplete: () => {
        if (finished) return;
        finished = true;
        rec(true, 200, '');
        writeChunk({}, finishCode === 2 ? 'length' : 'stop');
        res.write('data: [DONE]\n\n');
        res.end();
      },
    },
  });
  res.on('close', () => {
    if (!finished) {
      finished = true;
      rec(false, headersSent ? 200 : 499, 'client_disconnected');
      cancelRequest(entry);
    }
  });
}

// ---------------------------------------------------------------------------
// Gemini 原生 /v1beta/models/{model}:generateContent | :streamGenerateContent
// ---------------------------------------------------------------------------
async function handleGemini(req, res, url, modelRaw, action) {
  if (!requireAuth(req, res, url)) return;
  let body;
  try {
    body = await readJsonBody(req);
  } catch (e) {
    return sendJson(res, e.httpStatus || 400, {
      error: { code: e.httpStatus || 400, message: e.message, status: 'INVALID_ARGUMENT' },
    });
  }
  if (isPggoModel(modelRaw)) {
    return await handlePggoGemini(req, res, url, modelRaw, action, body);
  }
  const rec = makeUsageRecorder(modelRaw, action === 'streamGenerateContent');
  const model = stripModelPrefix(modelRaw);
  const generationConfig = {
    ...(body.generationConfig || body.generation_config || {}),
    ...(body.thinkingConfig ? { thinkingConfig: body.thinkingConfig } : {}),
  };
  let wireBodyObj;
  try {
    wireBodyObj = buildWireBodyObj({ model, payload: body, generationConfig, tools: body.tools || null });
  } catch (e) {
    rec(false, e.httpStatus || 400, e.message);
    return sendJson(res, e.httpStatus || 400, {
      error: { code: e.httpStatus || 400, message: e.message, status: 'INVALID_ARGUMENT' },
    });
  }
  const stream = action === 'streamGenerateContent';

  if (!stream) {
    await new Promise((resolve) => {
      dispatchRequest({
        model,
        stream: false,
        wireBodyObj,
        handlers: {
          onChunk: () => {},
          onError: (status, err) => {
            rec(false, status, err.message);
            sendJson(res, status, { error: { code: status, message: err.message, status: 'INTERNAL' } });
            resolve();
          },
          onComplete: (entry) => {
            try {
              const parsed = parseMakerSuiteResponse(entry.rawText);
              rec(true, 200, '');
              sendJson(res, 200, toGeminiResponse(parsed, model));
            } catch (err) {
              rec(false, 502, `上游响应解析失败: ${err.message}`);
              sendJson(res, 502, {
                error: { code: 502, message: `上游响应解析失败: ${err.message}`, status: 'INTERNAL' },
              });
            }
            resolve();
          },
        },
      });
    });
    return;
  }

  // 流式 SSE（Gemini 形态）
  let headersSent = false;
  let finished = false;
  let sawFinish = false;
  let lastUsage = null;
  const ensureHeaders = () => {
    if (headersSent) return;
    headersSent = true;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      'Access-Control-Allow-Origin': '*',
    });
  };
  const parser = new IncrementalJspbFrameParser((frame) => {
    const d = parseSingleMakerSuiteFrame(frame);
    if (d.usage) lastUsage = d.usage;
    const hasParts = Array.isArray(d.geminiParts) && d.geminiParts.length > 0;
    const hasFinish = d.finishReasonCode != null;
    if (!hasParts && !hasFinish) return;
    if (hasParts) rec.markTtfb();
    ensureHeaders();
    const candidate = {
      content: { role: 'model', parts: hasParts ? d.geminiParts : [{ text: '' }] },
      index: 0,
    };
    if (hasFinish) {
      sawFinish = true;
      candidate.finishReason = FINISH_REASON_MAP[d.finishReasonCode] || 'STOP';
    }
    const chunk = {
      candidates: [candidate],
      ...(hasFinish || d.usage ? { usageMetadata: geminiUsage(d.usage || lastUsage) } : {}),
      modelVersion: model,
    };
    res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  });
  const entry = dispatchRequest({
    model,
    stream: true,
    wireBodyObj,
    handlers: {
      onChunk: (piece) => {
        try { parser.feed(piece); } catch {}
      },
      onError: (status, err) => {
        if (finished) return;
        finished = true;
        rec(false, headersSent ? 200 : status, err.message);
        if (!headersSent) {
          sendJson(res, status, { error: { code: status, message: err.message, status: 'INTERNAL' } });
        } else {
          res.write(`data: ${JSON.stringify({ error: { code: status, message: err.message, status: 'INTERNAL' } })}\n\n`);
          res.end();
        }
      },
      onComplete: () => {
        if (finished) return;
        finished = true;
        rec(true, 200, '');
        ensureHeaders();
        if (!sawFinish) {
          const finalChunk = {
            candidates: [
              { content: { role: 'model', parts: [{ text: '' }] }, finishReason: 'STOP', index: 0 },
            ],
            usageMetadata: geminiUsage(lastUsage),
            modelVersion: model,
          };
          res.write(`data: ${JSON.stringify(finalChunk)}\n\n`);
        }
        res.end();
      },
    },
  });
  res.on('close', () => {
    if (!finished) {
      finished = true;
      rec(false, headersSent ? 200 : 499, 'client_disconnected');
      cancelRequest(entry);
    }
  });
}

// ---------------------------------------------------------------------------
// 脚本托管（域名动态替换）
// ---------------------------------------------------------------------------
let origScriptCache = null;
function serveScript(req, res) {
  if (!origScriptCache) {
    origScriptCache = fs.readFileSync(ORIG_SCRIPT_PATH, 'utf8');
  }
  const base = publicBase(req);
  const domain = base.replace(/^https?:\/\//, '');
  const out = origScriptCache.split('catiecli.sukaka.top').join(domain);
  res.writeHead(200, {
    'Content-Type': 'text/javascript; charset=utf-8',
    'Cache-Control': 'no-cache',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(out);
}

// ---------------------------------------------------------------------------
// 主 HTTP 服务
// ---------------------------------------------------------------------------
// 管理台 pggo 账户操作（禁用/启用/删除）：鉴权与其它 /api/console/* 一致；
// 删除连带清掉中继侧该邮箱的代理绑定与池分配记录（与池删除同规矩）。
async function handlePggoAccountAction(req, res, url, action) {
  if (!requireConsoleAuth(req, res, url)) return;
  let body;
  try {
    body = await readJsonBody(req);
  } catch (e) {
    return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
  }
  const id = (typeof body.email === 'string' && body.email.trim())
    || (typeof body.id === 'string' && body.id.trim())
    || '';
  if (!id) {
    return sendJson(res, 400, { error: { message: '缺少账户标识（email）', code: 'account_required' } });
  }
  if (!HEADLESS) {
    return sendJson(res, 501, { error: { message: '无头模式尚未部署', code: 'headless_not_configured' } });
  }
  try {
    if (action === 'delete') {
      await hlDeleteAccount(id);
      let changed = false;
      if (pggoState.proxies.byEmail[id]) { delete pggoState.proxies.byEmail[id]; changed = true; }
      if (pggoState.poolAssignments[id]) { delete pggoState.poolAssignments[id]; changed = true; }
      if (changed) savePggoState();
    } else {
      await hlSetAccountEnabled(id, action === 'enable');
    }
    return sendJson(res, 200, { ok: true, headless: await hlStatusSummary() });
  } catch (e) {
    return sendJson(res, e.httpStatus || 502, {
      error: { message: e.message, code: 'pggo_account_action_failed' },
    });
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-API-Key, X-Goog-Api-Key',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      });
      return res.end();
    }

    if (req.method === 'GET' && (pathname === '/' || pathname === '/console')) {
      // 站长口令已设置且无有效站长会话 → 只给极简登录页：不含任何管理台
      // 卡片/数据/脚本链接，先输对口令才看得到内容。未设口令（过渡状态）
      // 与已登录时照常返回完整管理台，本机后备栈不受影响。
      if (ownerRecord && !ownerSessionValid(req)) {
        const loginHtml = fs.readFileSync(LOGIN_HTML_PATH, 'utf8');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
        return res.end(loginHtml);
      }
      const html = fs.readFileSync(CONSOLE_HTML_PATH, 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return res.end(html);
    }

    if (req.method === 'GET' && pathname === '/peach.webp') {
      // 门面图：登录页未登录就要显示，必须公开。文件随源码部署，读一次缓存。
      try {
        if (!peachBuf) peachBuf = fs.readFileSync(PEACH_PATH);
        res.writeHead(200, { 'Content-Type': 'image/webp', 'Cache-Control': 'public, max-age=86400' });
        return res.end(peachBuf);
      } catch {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('not found');
      }
    }

    if (req.method === 'GET' && pathname === '/health') {
      const ex = executors.get(API_KEY);
      return sendJson(res, 200, {
        ok: true,
        service: '蜜桃可可',
        phase: 2,
        executorOnline: executorOnline(),
        executorConnectedAt: ex ? ex.connectedAt : null,
        executorClientVersion: ex ? ex.clientVersion : null,
        pendingRequests: pending.size,
        headless: HEADLESS
          ? {
              enabled: true,
              up: hlAccountsCache.up,
              accounts: hlAccountsCache.accounts ? hlAccountsCache.accounts.length : null,
            }
          : { enabled: false },
        publicUrl: publicBase(req),
        uptimeSec: Math.round(process.uptime()),
      });
    }

    if (req.method === 'GET' && pathname === '/api/console/owner') {
      return sendJson(res, 200, {
        ok: true,
        ownerSet: !!ownerRecord,
        ownerLoggedIn: ownerSessionValid(req),
      });
    }

    if (req.method === 'POST' && pathname === '/api/console/owner/setup') {
      if (ownerRecord) {
        return sendJson(res, 409, {
          error: { message: '站长口令已经设置过了，不能重复设置', code: 'owner_already_set' },
        });
      }
      if (!requireAuth(req, res, url)) return;
      let sbody;
      try {
        sbody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      const pw = typeof sbody.password === 'string' ? sbody.password : '';
      if (!pw) {
        return sendJson(res, 400, {
          error: { message: '站长口令不能为空', code: 'password_empty' },
        });
      }
      try {
        saveOwnerRecord(pw);
      } catch (e) {
        return sendJson(res, 500, { error: { message: `站长口令保存失败: ${e.message}`, code: 'owner_save_failed' } });
      }
      issueOwnerSession(res);
      return sendJson(res, 200, { ok: true, ownerSet: true, ownerLoggedIn: true });
    }

    if (req.method === 'POST' && pathname === '/api/console/owner/login') {
      if (!ownerRecord) {
        return sendJson(res, 400, {
          error: { message: '尚未设置站长口令', code: 'owner_not_set' },
        });
      }
      if (Date.now() < ownerLoginGuard.lockedUntil) {
        return sendJson(res, 429, {
          error: { message: '站长登录失败次数过多，已锁定几分钟，请稍后再试', code: 'owner_locked' },
        });
      }
      let lbody;
      try {
        lbody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      const pw = typeof lbody.password === 'string' ? lbody.password : '';
      if (!verifyOwnerPassword(pw)) {
        ownerLoginGuard.fails += 1;
        if (ownerLoginGuard.fails >= 5) {
          ownerLoginGuard.fails = 0;
          ownerLoginGuard.lockedUntil = Date.now() + 5 * 60 * 1000;
        }
        return sendJson(res, 401, {
          error: { message: '站长口令不对', code: 'owner_bad_password' },
        });
      }
      ownerLoginGuard.fails = 0;
      issueOwnerSession(res);
      return sendJson(res, 200, { ok: true, ownerLoggedIn: true });
    }

    if (req.method === 'POST' && pathname === '/api/console/owner/logout') {
      destroyOwnerSession(req, res);
      return sendJson(res, 200, { ok: true, ownerLoggedIn: false });
    }

    if (req.method === 'GET' && pathname === '/api/console/key') {
      // 站长常看当前服务 Key：鉴权与改 Key 完全同层（requireConsoleAuth）——
      // 已设站长口令时只认站长会话，光带服务 Key 拿不到；未设口令的过渡期
      // 带有效服务 Key 可读。Key 本来就存在本服务（config/override），
      // 这里只是把「当前有效值」如实回给有权看的人，不产生新副本。
      if (!requireConsoleAuth(req, res, url)) return;
      return sendJson(res, 200, { ok: true, apiKey: API_KEY });
    }

    if (req.method === 'POST' && pathname === '/api/console/key') {
      if (!requireConsoleAuth(req, res, url)) return;
      let kbody;
      try {
        kbody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      let newKey;
      if (kbody.generate === true) {
        newKey = `muse-${crypto.randomBytes(24).toString('hex')}`;
      } else {
        newKey = typeof kbody.newKey === 'string' ? kbody.newKey.trim() : '';
        if (newKey.length < 16) {
          return sendJson(res, 400, {
            error: { message: '新 Key 至少 16 位', code: 'key_too_short' },
          });
        }
      }
      setApiKey(newKey);
      return sendJson(res, 200, {
        ok: true,
        apiKey: newKey,
        note: '新 Key 已立即生效，旧 Key 作废。脚本面板、酒馆、手机 App 里的 Key 都要换成这把新 Key。',
      });
    }

    if (req.method === 'GET' && pathname === '/api/console/status') {
      if (!requireConsoleAuth(req, res, url)) return;
      const ex = executors.get(API_KEY);
      return sendJson(res, 200, {
        ok: true,
        executorOnline: executorOnline(),
        executorConnectedAt: ex ? ex.connectedAt : null,
        executorConnectedForSec: ex ? Math.round((Date.now() - ex.connectedAt) / 1000) : null,
        executorClientVersion: ex ? ex.clientVersion : null,
        pendingRequests: pending.size,
        apiBase: publicBase(req),
        headless: await hlStatusSummary(),
      });
    }

    if (req.method === 'GET' && pathname === '/api/console/usage') {
      if (!requireConsoleAuth(req, res, url)) return;
      return sendJson(res, 200, { ok: true, ...usageSummary(url.searchParams.get('day') || '') });
    }

    if (req.method === 'GET' && pathname === '/api/console/pggo') {
      if (!requireConsoleAuth(req, res, url)) return;
      return sendJson(res, 200, { ok: true, headless: await hlStatusSummary() });
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/proxy') {
      if (!requireConsoleAuth(req, res, url)) return;
      let pbody;
      try {
        pbody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      const pEmail = typeof pbody.email === 'string' ? pbody.email.trim() : '';
      const pProxy = typeof pbody.proxy === 'string' ? pbody.proxy.trim() : '';
      if (pProxy && !/^(https?|socks5):\/\/\S+$/i.test(pProxy) && !/^[\w.-]+:\d{2,5}$/.test(pProxy)) {
        return sendJson(res, 400, {
          error: { message: '代理地址格式不对，应形如 http://user:pass@host:port 或 socks5://host:port', code: 'invalid_proxy' },
        });
      }
      if (!pEmail) {
        pggoState.proxies.default = pProxy;
        savePggoState();
        return sendJson(res, 200, {
          ok: true,
          proxies: { default: pggoState.proxies.default, byEmail: pggoState.proxies.byEmail },
          appliedToAccount: false,
          note: '默认代理已保存，在未单独绑定的账户下次推送/导入时生效。',
        });
      }
      const prevBinding = pggoState.proxies.byEmail[pEmail] || '';
      if (pProxy) pggoState.proxies.byEmail[pEmail] = pProxy;
      else delete pggoState.proxies.byEmail[pEmail];
      savePggoState();
      // 绑定存好后，若该邮箱的账户已导入无头服务，立即把代理同步写进账户
      // 配置生效（hlSetAccountProxy），不再等下次推送。清除绑定时仅当账户现
      // 用代理正是被删的这条绑定才同步清掉，避免误洗流量池分配的代理。
      let appliedToAccount = false;
      let note;
      if (pProxy) {
        const r = await hlSetAccountProxy(pEmail, pProxy);
        appliedToAccount = !!r.applied;
        note = r.applied
          ? r.unchanged
            ? '绑定已保存；该账户当前已在用这个代理，无需变更。'
            : '绑定已保存，并已立即写入账户配置生效（账户将以新代理重启，下一次请求会短暂预热）。'
          : r.reason === 'account_not_found'
            ? '绑定已保存；该邮箱还没有导入账户，将在首次推送时生效。'
            : `绑定已保存，但暂未写入账户（${r.reason}），将在下次推送时生效。`;
      } else if (prevBinding) {
        const r = await hlSetAccountProxy(pEmail, '', prevBinding);
        appliedToAccount = !!r.applied;
        note = r.applied
          ? '绑定已清除，并已同步清掉账户配置里的代理，立即生效（账户将以新代理重启，下一次请求会短暂预热）。'
          : r.reason === 'account_not_found'
            ? '绑定已清除；该邮箱还没有导入账户。'
            : `绑定已清除；账户配置未改动（${r.reason}）。`;
      } else {
        note = '该邮箱没有已保存的绑定，未做改动。';
      }
      return sendJson(res, 200, {
        ok: true,
        proxies: { default: pggoState.proxies.default, byEmail: pggoState.proxies.byEmail },
        appliedToAccount,
        note,
      });
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/import') {
      return await handleConsolePggoImport(req, res, url);
    }

    if (req.method === 'GET' && pathname === '/api/console/pool') {
      if (!requireConsoleAuth(req, res, url)) return;
      return sendJson(res, 200, {
        ok: true,
        pool: mergedPool(),
        assignments: pggoState.poolAssignments,
      });
    }

    if (req.method === 'POST' && pathname === '/api/console/pool') {
      if (!requireConsoleAuth(req, res, url)) return;
      let poolBody;
      try {
        poolBody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      const text = typeof poolBody.text === 'string' ? poolBody.text : '';
      const { entries, skipped } = parseProxyPoolText(text);
      let added = 0;
      for (const e of entries) {
        if (pggoState.proxyPool.some((p) => p.url === e.url)) {
          skipped.push({ line: e.line, reason: '已在池中' });
          continue;
        }
        if (isClashPoolUrl(e.url)) {
          skipped.push({ line: e.line, reason: '已在池中（订阅节点）' });
          continue;
        }
        pggoState.proxyPool.push({ url: e.url, label: e.label });
        added += 1;
      }
      if (added) savePggoState();
      return sendJson(res, 200, {
        ok: true,
        added,
        skipped,
        pool: mergedPool(),
        assignments: pggoState.poolAssignments,
      });
    }

    if (req.method === 'POST' && pathname === '/api/console/pool/remove') {
      if (!requireConsoleAuth(req, res, url)) return;
      let rmBody;
      try {
        rmBody = await readJsonBody(req);
      } catch (e) {
        return sendJson(res, e.httpStatus || 400, { error: { message: e.message, code: 'invalid_body' } });
      }
      const rmUrl = typeof rmBody.url === 'string' ? rmBody.url : '';
      const inManual = pggoState.proxyPool.some((p) => p.url === rmUrl);
      if (!inManual && isClashPoolUrl(rmUrl)) {
        return sendJson(res, 400, {
          error: {
            message: '这是订阅生成的节点，不能在池里删除；要增删节点请到机场/订阅侧调整，服务重启后自动刷新',
            code: 'clash_entry_readonly',
          },
        });
      }
      const before = pggoState.proxyPool.length;
      pggoState.proxyPool = pggoState.proxyPool.filter((p) => p.url !== rmUrl);
      const removed = pggoState.proxyPool.length !== before;
      // 分配记录一并清掉；已导入账户的配置里已写入的代理不受影响（下次推送才变）
      for (const [em, u] of Object.entries(pggoState.poolAssignments)) {
        if (u === rmUrl) delete pggoState.poolAssignments[em];
      }
      if (removed) savePggoState();
      return sendJson(res, 200, {
        ok: true,
        removed,
        pool: mergedPool(),
        assignments: pggoState.poolAssignments,
      });
    }

    if (req.method === 'POST' && pathname === '/api/console/pool/test') {
      if (!requireConsoleAuth(req, res, url)) return;
      const results = await poolTestAll(mergedPool());
      return sendJson(res, 200, { ok: true, results });
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/start') {
      if (!requireConsoleAuth(req, res, url)) return;
      const started = await hlEnsureStarted();
      return sendJson(res, 200, { ok: true, started, headless: await hlStatusSummary() });
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/account/disable') {
      return await handlePggoAccountAction(req, res, url, 'disable');
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/account/enable') {
      return await handlePggoAccountAction(req, res, url, 'enable');
    }

    if (req.method === 'POST' && pathname === '/api/console/pggo/account/delete') {
      return await handlePggoAccountAction(req, res, url, 'delete');
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/api/pg/script.user.js') {
      if (req.method === 'HEAD') {
        res.writeHead(200, {
          'Content-Type': 'text/javascript; charset=utf-8',
          'Cache-Control': 'no-cache',
          'Access-Control-Allow-Origin': '*',
        });
        return res.end();
      }
      return serveScript(req, res);
    }

    if (req.method === 'POST' && pathname === '/api/pggo/push') {
      return await handlePggoPush(req, res, url);
    }
    if (req.method === 'POST' && pathname === '/api/robin/bind-by-key') {
      if (!requireAuth(req, res, url)) return;
      return sendJson(res, 501, {
        ok: false,
        error: 'phase2_not_available',
        message: 'Robin 凭证线不在一期范围内。',
      });
    }

    if (req.method === 'GET' && pathname === '/v1/models') {
      if (!requireAuth(req, res, url)) return;
      const data = MODELS.map((m) => ({
        id: m.id,
        object: 'model',
        created: 0,
        owned_by: 'google-aistudio-playground',
      }));
      if (HEADLESS) {
        const seen = new Set(data.map((m) => m.id));
        for (const id of await hlModelIds()) {
          const pid = `pggo-${id}`;
          if (!seen.has(pid)) {
            seen.add(pid);
            data.push({ id: pid, object: 'model', created: 0, owned_by: 'google-aistudio-headless' });
          }
        }
      }
      return sendJson(res, 200, { object: 'list', data });
    }

    if (req.method === 'GET' && (pathname === '/v1beta/models' || pathname === '/models')) {
      if (!requireAuth(req, res, url)) return;
      const models = MODELS.map((m) => ({
        name: `models/${m.id}`,
        version: '001',
        displayName: m.displayName,
        description: `${m.displayName} via AI Studio Playground (蜜桃可可)`,
        inputTokenLimit: 1048576,
        outputTokenLimit: 65536,
        supportedGenerationMethods: ['generateContent', 'streamGenerateContent'],
      }));
      if (HEADLESS) {
        const baseById = new Map(BASE_MODELS.map((m) => [m.id, m.displayName]));
        for (const id of await hlModelIds()) {
          models.push({
            name: `models/pggo-${id}`,
            version: '001',
            displayName: `${baseById.get(id) || id} (pggo 无头)`,
            description: `${id} via AI Studio headless (蜜桃可可 pggo)`,
            inputTokenLimit: 1048576,
            outputTokenLimit: 65536,
            supportedGenerationMethods: ['generateContent', 'streamGenerateContent'],
          });
        }
      }
      return sendJson(res, 200, { models });
    }

    if (req.method === 'POST' && pathname === '/v1/chat/completions') {
      return await handleChatCompletions(req, res, url);
    }

    const geminiMatch = pathname.match(/^\/v1beta\/models\/(.+?):(generateContent|streamGenerateContent)$/);
    if (req.method === 'POST' && geminiMatch) {
      return await handleGemini(req, res, url, decodeURIComponent(geminiMatch[1]), geminiMatch[2]);
    }

    return sendJson(res, 404, {
      error: { message: `Not found: ${req.method} ${pathname}`, type: 'invalid_request_error', code: 'not_found' },
    });
  } catch (e) {
    console.error('[pg-relay] handler error', e);
    if (!res.headersSent) {
      sendJson(res, 500, { error: { message: `内部错误: ${e.message}`, type: 'relay_error', code: 'internal' } });
    } else {
      try { res.end(); } catch {}
    }
  }
});

// ---------------------------------------------------------------------------
// WS 执行器端点 /api/pg/ws
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 * 1024 });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/api/pg/ws') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  let registeredKey = null;
  const registerTimer = setTimeout(() => {
    try { ws.close(4008, 'register timeout'); } catch {}
  }, 15_000);

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString('utf8'));
    } catch {
      return;
    }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'register') {
      clearTimeout(registerTimer);
      const key = String(msg.apiKey || '');
      if (key !== API_KEY || (msg.executorType && msg.executorType !== 'playground')) {
        try { ws.close(4008, 'invalid api key'); } catch {}
        return;
      }
      // 同 Key 单活：新连顶旧连（旧连关 4001，脚本侧不会重连）
      const old = executors.get(key);
      if (old && old.ws !== ws) {
        try { old.ws.close(4001, 'replaced by new connection'); } catch {}
      }
      registeredKey = key;
      executors.set(key, {
        ws,
        connectedAt: Date.now(),
        clientVersion: String(msg.clientVersion || ''),
        executorType: 'playground',
      });
      console.log(`[pg-relay] 执行器上线 clientVersion=${msg.clientVersion || '?'}`);
      sendFrame(ws, { type: 'registered', serverTime: Date.now() });
      // 有等待中的请求时尝试补发
      for (const entry of pending.values()) {
        if (!entry.dispatched && entry.tryDispatch) entry.tryDispatch();
      }
      return;
    }

    if (msg.type === 'ping') {
      sendFrame(ws, { type: 'pong', ts: Date.now() });
      return;
    }

    if (
      msg.type === 'response' ||
      msg.type === 'stream-chunk' ||
      msg.type === 'stream-end' ||
      msg.type === 'error'
    ) {
      if (msg.requestId) routeResultFrame(msg);
      return;
    }
  });

  ws.on('close', () => {
    clearTimeout(registerTimer);
    if (registeredKey) {
      const cur = executors.get(registeredKey);
      if (cur && cur.ws === ws) {
        executors.delete(registeredKey);
        console.log('[pg-relay] 执行器下线（在途请求保留，等待迟到帧收养或超时）');
      }
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, HOST, () => {
  console.log(`[pg-relay] listening on http://${HOST}:${PORT}`);
});
