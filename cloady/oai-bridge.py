#!/usr/bin/env python3
"""OpenAI chat/completions -> Gemini generateContent. Python 3 stdlib only."""
import json, os, re, threading, time, uuid, urllib.request, urllib.error
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

UPSTREAM = os.environ.get("OAI_BRIDGE_UPSTREAM", "http://127.0.0.1:8080").rstrip("/")
HOST = os.environ.get("OAI_BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("OAI_BRIDGE_PORT", "8765"))
SAFETY = [{"category": c, "threshold": "BLOCK_NONE"} for c in (
    "HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
    "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT")]
FINISH = {"STOP": "stop", "MAX_TOKENS": "length", "SAFETY": "content_filter",
          "PROHIBITED_CONTENT": "content_filter", "BLOCKLIST": "content_filter",
          "RECITATION": "content_filter", "SPII": "content_filter"}


def to_parts(content):
    if content is None:
        return []
    if isinstance(content, str):
        return [{"text": content}] if content else []
    parts = []
    for p in content:
        if p.get("type") == "text" and p.get("text"):
            parts.append({"text": p["text"]})
        elif p.get("type") == "image_url":
            url = (p.get("image_url") or {}).get("url", "")
            if url.startswith("data:") and ";base64," in url:
                mime, data = url[5:].split(";base64,", 1)
                parts.append({"inlineData": {"mimeType": mime, "data": data}})
    return parts


def to_gemini(body):
    system, contents = [], []
    for m in body.get("messages", []):
        role, parts = m.get("role"), to_parts(m.get("content"))
        if not parts:
            continue
        if role in ("system", "developer"):
            system.extend(parts)
            continue
        g_role = "model" if role == "assistant" else "user"
        if contents and contents[-1]["role"] == g_role:
            contents[-1]["parts"].extend(parts)
        else:
            contents.append({"role": g_role, "parts": parts})
    if not contents:
        contents, system = [{"role": "user", "parts": system or [{"text": "."}]}], []
    req = {"contents": contents, "safetySettings": SAFETY}
    if system:
        req["systemInstruction"] = {"parts": system}
    cfg = {}
    for o, g in (("temperature", "temperature"), ("top_p", "topP"), ("top_k", "topK")):
        if body.get(o) is not None:
            cfg[g] = body[o]
    mt = body.get("max_completion_tokens") or body.get("max_tokens")
    if mt:
        cfg["maxOutputTokens"] = mt
    stop = body.get("stop")
    if stop:
        cfg["stopSequences"] = [stop] if isinstance(stop, str) else stop
    # gemini-4 系列默认思考会把输出额度烧光、正文一个字不出（酒馆表现为�空回）。
    # 实测 thinkingBudget 不生效、thinkingLevel 生效：默认压到 low 档。
    # 客户端带 reasoning_effort 时按它映射；环境变量 OAI_BRIDGE_THINKING_LEVEL 可改默认值，设 off 则不注入。
    lvl = None
    effort = body.get("reasoning_effort")
    if isinstance(effort, str) and effort:
        lvl = {"minimal": "low", "low": "low", "medium": "medium", "high": "high"}.get(effort.lower())
    if lvl is None:
        lvl = os.environ.get("OAI_BRIDGE_THINKING_LEVEL", "low")
    if lvl and lvl != "off":
        cfg["thinkingConfig"] = {"thinkingLevel": lvl}
    if cfg:
        req["generationConfig"] = cfg
    return req


def from_gemini(g):
    cands = g.get("candidates") or []
    if not cands:
        reason = (g.get("promptFeedback") or {}).get("blockReason", "")
        return "", "content_filter" if reason else "stop"
    c = cands[0]
    text = "".join(p.get("text", "") for p in (c.get("content") or {}).get("parts", [])
                   if not p.get("thought"))
    return text, FINISH.get(c.get("finishReason", "STOP"), "stop")


def usage(g):
    u = g.get("usageMetadata") or {}
    p, c = u.get("promptTokenCount", 0), u.get("candidatesTokenCount", 0)
    return {"prompt_tokens": p, "completion_tokens": c, "total_tokens": u.get("totalTokenCount", p + c)}


# ---------------- 用量记账（内存 + 本地 jsonl，巡检定期收走） ----------------
USAGE_FILE = os.environ.get("OAI_BRIDGE_USAGE_FILE", "/app/data/usage.jsonl")
CST = timezone(timedelta(hours=8))
_records = deque(maxlen=20000)
_lock = threading.Lock()
_appends = [0]
_acct_cache = {"ts": 0.0, "email": ""}


def _load_usage():
    try:
        with open(USAGE_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        _records.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass


def _account_email():
    now = time.time()
    if now - _acct_cache["ts"] < 60:
        return _acct_cache["email"]
    email = ""
    try:
        r = urllib.request.Request(UPSTREAM + "/api/accounts")
        with urllib.request.urlopen(r, timeout=5) as resp:
            accts = json.loads(resp.read() or b"[]")
        if isinstance(accts, list) and len(accts) == 1:
            email = accts[0].get("email", "") or ""
    except Exception:
        pass
    _acct_cache.update(ts=now, email=email)
    return email


def record_usage(model, path_kind, usage_meta, ttfb_ms, dur_ms, ok, finish, error=""):
    u = usage_meta or {}
    rec = {
        "id": uuid.uuid4().hex[:16],
        "ts": int(time.time() * 1000),
        "model": model,
        "account": _account_email(),
        "path": path_kind,
        "prompt": u.get("promptTokenCount", 0) or 0,
        "completion": u.get("candidatesTokenCount", 0) or 0,
        "thoughts": u.get("thoughtsTokenCount", 0) or 0,
        "ttfb_ms": ttfb_ms,
        "dur_ms": dur_ms,
        "ok": bool(ok),
        "finish": finish or "",
        "error": (error or "")[:200],
    }
    with _lock:
        _records.append(rec)
        try:
            with open(USAGE_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            _appends[0] += 1
            if _appends[0] % 500 == 0 and os.path.getsize(USAGE_FILE) > 8 * 1024 * 1024:
                with open(USAGE_FILE, "w", encoding="utf-8") as f:
                    for r in _records:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
        except OSError:
            pass
    return rec


def _mask(email):
    if "@" not in email:
        return email
    name, dom = email.split("@", 1)
    return (name[:3] + "•••@" + dom) if len(name) > 3 else email


def _is_fail(r):
    return (not r["ok"]) or r["completion"] == 0


def _window(day):
    now = datetime.now(CST)
    if day == "us":
        start = now.replace(hour=15, minute=0, second=0, microsecond=0)
        if now < start:
            start -= timedelta(days=1)
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def usage_report(day):
    start, end = _window(day)
    s_ms, e_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    with _lock:
        rows = [r for r in _records if s_ms <= r["ts"] < e_ms]
    total = len(rows)
    fail = sum(1 for r in rows if _is_fail(r))
    summary = {"total": total, "ok": total - fail, "fail": fail,
               "prompt": sum(r["prompt"] for r in rows),
               "completion": sum(r["completion"] for r in rows),
               "thoughts": sum(r["thoughts"] for r in rows)}
    by_model = {}
    for r in rows:
        m = by_model.setdefault(r["model"], {"model": r["model"], "total": 0, "prompt": 0, "completion": 0, "thoughts": 0})
        m["total"] += 1
        m["prompt"] += r["prompt"]
        m["completion"] += r["completion"]
        m["thoughts"] += r["thoughts"]
    by_acct = {}
    for r in rows:
        key = r["account"] or "—"
        a = by_acct.setdefault(key, {"account": _mask(key), "total": 0, "ok": 0, "fail": 0,
                                     "prompt": 0, "completion": 0, "thoughts": 0})
        a["total"] += 1
        if _is_fail(r):
            a["fail"] += 1
        else:
            a["ok"] += 1
        a["prompt"] += r["prompt"]
        a["completion"] += r["completion"]
        a["thoughts"] += r["thoughts"]
    for a in by_acct.values():
        a["share"] = round(a["total"] / total * 100) if total else 0
    recs = sorted(rows, key=lambda r: -r["ts"])[:50]
    return {"day": day,
            "start": start.strftime("%m月%d日 %H:%M"),
            "end": end.strftime("%m月%d日 %H:%M"),
            "summary": summary,
            "byModel": sorted(by_model.values(), key=lambda m: -m["total"]),
            "byAccount": sorted(by_acct.values(), key=lambda a: -a["total"]),
            "records": recs}


USAGE_PAGE = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>用量日志 · 奶盖绿茶</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; background: #f9fafb; color: #111827; padding: 14px 12px 28px; }
  .wrap { max-width: 430px; margin: 0 auto; }
  .head { display: flex; align-items: center; justify-content: space-between; padding: 0 2px 10px; }
  .head b { font-size: 17px; } .head span { font-size: 12px; color: #9ca3af; font-weight: 400; }
  .pill { font-size: 12px; color: #047857; border: 1px solid #d1d5db; border-radius: 999px; padding: 4px 12px; background: #fff; cursor: pointer; }
  .dayrow { display: flex; gap: 8px; }
  .daybtn { flex: 1; padding: 9px 0; border-radius: 10px; border: 1.5px solid #d1d5db; background: #fff; color: #111827; font-size: 13px; font-weight: 600; cursor: pointer; }
  .daybtn.on { border-color: #10a37f; background: #10a37f; color: #fff; }
  .win { font-size: 12px; color: #9ca3af; padding: 8px 2px 2px; }
  .tiles { display: flex; gap: 8px; margin-top: 8px; }
  .tile { flex: 1; text-align: center; background: #ecfdf5; border: 1px solid #d1fae5; border-radius: 14px; padding: 10px 4px; }
  .tile b { font-size: 22px; font-weight: 800; display: block; }
  .tile span { font-size: 11px; color: #6b7280; }
  .tok { flex: 1; background: #fff; border: 1px solid #e5e7eb; border-radius: 14px; padding: 9px 12px; }
  .tok span { font-size: 11px; color: #9ca3af; display: block; }
  .tok b { font-size: 17px; }
  h3 { margin-top: 14px; font-size: 13px; padding: 0 2px; }
  .card { background: #fff; border: 1.5px solid #e5e7eb; border-radius: 14px; margin-top: 6px; overflow: hidden; }
  .row { display: flex; justify-content: space-between; padding: 10px 12px; font-size: 13px; }
  .row + .row { border-top: 1px solid #f3f4f6; }
  .sub { font-size: 12px; color: #9ca3af; }
  .acct { background: #fff; border: 1.5px solid #10a37f; border-radius: 14px; padding: 10px 12px; margin-top: 6px; box-shadow: 0 3px 10px rgba(16,163,127,.15); }
  .bar { height: 6px; border-radius: 3px; background: #d1fae5; margin-top: 8px; overflow: hidden; }
  .bar i { display: block; height: 100%; background: linear-gradient(90deg, #34d399, #10a37f); }
  .rec { padding: 9px 12px; font-size: 12px; }
  .rec + .rec { border-top: 1px solid #f3f4f6; }
  .ok { color: #059669; font-weight: 600; } .bad { color: #ef4444; font-weight: 600; }
  .empty { padding: 14px 12px; font-size: 12px; color: #9ca3af; text-align: center; }
</style></head><body><div class="wrap">
  <div class="head"><b>用量日志 <span>奶盖绿茶</span></b><span class="pill" onclick="load()">刷新</span></div>
  <div class="dayrow">
    <button class="daybtn on" id="bCn" onclick="setDay('cn')">0点 · 中国日</button>
    <button class="daybtn" id="bUs" onclick="setDay('us')">15点 · 美国日</button>
  </div>
  <div class="win" id="win">统计区间：—</div>
  <div class="tiles">
    <div class="tile"><b id="sTotal">–</b><span>总请求</span></div>
    <div class="tile"><b id="sOk" style="color:#059669">–</b><span>成功</span></div>
    <div class="tile"><b id="sFail" style="color:#ef4444">–</b><span>失败</span></div>
  </div>
  <div class="tiles">
    <div class="tok"><span>输入 token</span><b id="sIn">–</b></div>
    <div class="tok"><span>输出 token</span><b id="sOut">–</b></div>
    <div class="tok"><span>思考 token</span><b id="sThink" style="color:#047857">–</b></div>
  </div>
  <h3>按模型</h3><div class="card" id="byModel"><div class="empty">暂无记录</div></div>
  <h3>按账号</h3><div id="byAccount"><div class="card"><div class="empty">暂无记录</div></div></div>
  <h3>流水</h3><div class="card" id="records"><div class="empty">暂无记录</div></div>
</div>
<script>
  var day = 'cn';
  function fmtTok(n){ if(n>=10000){ var v=n/10000; var s = v>=100 ? String(Math.round(v)) : String(Math.round(v*10)/10); return s+'万'; } return n.toLocaleString('en-US'); }
  function fmtNum(n){ return n.toLocaleString('en-US'); }
  function fmtMs(ms){ return (ms==null) ? '—' : (ms/1000).toFixed(1)+'s'; }
  function fmtTime(ts){ var d=new Date(ts); function p(x){ return (x<10?'0':'')+x; } return p(d.getHours())+':'+p(d.getMinutes()); }
  function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;'); }
  function setDay(d){ day=d; document.getElementById('bCn').className='daybtn'+(d==='cn'?' on':''); document.getElementById('bUs').className='daybtn'+(d==='us'?' on':''); load(); }
  function load(){
    fetch('/api/usage?day='+day).then(function(r){ return r.json(); }).then(function(d){
      document.getElementById('win').textContent = '统计区间：'+d.start+' → '+d.end;
      var s=d.summary;
      document.getElementById('sTotal').textContent=s.total;
      document.getElementById('sOk').textContent=s.ok;
      document.getElementById('sFail').textContent=s.fail;
      document.getElementById('sIn').textContent=fmtTok(s.prompt);
      document.getElementById('sOut').textContent=fmtTok(s.completion);
      document.getElementById('sThink').textContent=fmtTok(s.thoughts);
      var bm=document.getElementById('byModel');
      bm.innerHTML = d.byModel.length ? d.byModel.map(function(m){
        return '<div class="row"><span>'+esc(m.model)+'</span><span class="sub">'+m.total+' 发 · 入 '+fmtTok(m.prompt)+' · 出 '+fmtTok(m.completion)+'</span></div>';
      }).join('') : '<div class="empty">暂无记录</div>';
      var ba=document.getElementById('byAccount');
      ba.innerHTML = d.byAccount.length ? d.byAccount.map(function(a){
        return '<div class="acct"><div style="display:flex;align-items:flex-start;gap:8px;">'
          +'<div style="font-weight:700;font-size:13px;padding-top:4px;">'+esc(a.account)+'</div>'
          +'<div style="margin-left:auto;text-align:right;"><b style="font-size:22px;line-height:1.1;">'+a.total+'</b>'
          +'<span style="display:block;font-size:11px;color:#9ca3af;">总请求</span></div></div>'
          +'<div class="bar"><i style="width:'+a.share+'%"></i></div>'
          +'<div style="font-size:12px;color:#6b7280;margin-top:7px;">成功 '+a.ok+' · 失败 '+a.fail+' · 占比 '+a.share+'% · 输入 '+fmtTok(a.prompt)+' · 输出 '+fmtTok(a.completion)+' · 思考 '+fmtTok(a.thoughts)+'</div></div>';
      }).join('') : '<div class="card"><div class="empty">暂无记录</div></div>';
      var rc=document.getElementById('records');
      rc.innerHTML = d.records.length ? d.records.map(function(r){
        var st = !r.ok ? '<span class="bad">失败</span>' : (r.completion===0 ? '<span class="bad">失败 · 空回</span>' : '<span class="ok">成功</span>');
        return '<div class="rec"><div style="display:flex;justify-content:space-between;"><span>'+fmtTime(r.ts)+' · '+esc(r.model)+'</span>'+st+'</div>'
          +'<div style="color:#9ca3af;margin-top:3px;">输入 '+fmtNum(r.prompt)+' → 输出 '+fmtNum(r.completion)+'（思考 '+fmtNum(r.thoughts)+'）· 首字 '+fmtMs(r.ttfb_ms)+' · 总 '+fmtMs(r.dur_ms)+'</div></div>';
      }).join('') : '<div class="empty">暂无记录</div>';
    }).catch(function(){});
  }
  load();
</script></body></html>
"""

PANEL_PILL = ('<!--taotao-usage-pill--><a href="/usage" target="_blank" '
              'style="position:fixed;right:14px;bottom:14px;z-index:9999;background:#10a37f;color:#fff;'
              'padding:9px 14px;border-radius:999px;font:600 13px -apple-system,\'PingFang SC\',sans-serif;'
              'text-decoration:none;box-shadow:0 4px 14px rgba(16,163,127,.35)">用量日志</a>')


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def key(self):
        auth = self.headers.get("Authorization", "")
        return auth[7:].strip() if auth.lower().startswith("bearer ") else self.headers.get("x-goog-api-key", "")

    def call(self, method, path, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        r = urllib.request.Request(UPSTREAM + path, data=data, method=method,
                                   headers={"Content-Type": "application/json", "x-goog-api-key": self.key()})
        try:
            with urllib.request.urlopen(r, timeout=600) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"error": {"message": raw.decode("utf-8", "replace")}}
        except (urllib.error.URLError, OSError) as e:
            return 502, {"error": {"message": f"upstream {UPSTREAM} unreachable: {e}"}}

    def send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(b)

    def send_html(self, html):
        b = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(b)

    def err(self, code, g):
        e = g.get("error") if isinstance(g.get("error"), dict) else {"message": json.dumps(g, ensure_ascii=False)}
        self.send(code, {"error": {"message": e.get("message", ""), "type": "upstream_error", "code": code}})

    def do_OPTIONS(self):
        self.send_response(204)
        # "*" in Allow-Headers does not cover Authorization, so echo what the browser asked for
        asked = self.headers.get("Access-Control-Request-Headers") or "Authorization, Content-Type"
        for k, v in (("Access-Control-Allow-Origin", "*"), ("Access-Control-Allow-Headers", asked),
                     ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
                     ("Access-Control-Allow-Private-Network", "true"), ("Content-Length", "0")):
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path in ("/v1/models", "/models"):
            code, g = self.call("GET", "/v1beta/models")
            if code != 200:
                return self.err(code, g)
            ids = [m.get("name", "").removeprefix("models/") for m in g.get("models", [])]
            return self.send(200, {"object": "list", "data": [
                {"id": i, "object": "model", "created": 0, "owned_by": "google"} for i in ids if i]})
        if path == "/health":
            return self.send(200, {"ok": True, "upstream": UPSTREAM})
        if path == "/usage":
            return self.send_html(USAGE_PAGE)
        if path == "/api/usage":
            q = parse_qs(urlparse(self.path).query)
            return self.send(200, usage_report((q.get("day") or ["cn"])[0]))
        if path == "/api/usage/export":
            q = parse_qs(urlparse(self.path).query)
            since = (q.get("since") or [""])[0]
            with _lock:
                rows = list(_records)
            if since:
                idx = next((i for i, r in enumerate(rows) if r["id"] == since), -1)
                rows = rows[idx + 1:] if idx >= 0 else rows
            return self.send(200, {"records": rows})
        return self.passthrough()

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/")
        if path not in ("/v1/chat/completions", "/chat/completions"):
            return self.passthrough()
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except ValueError:
            return self.send(400, {"error": {"message": "bad json"}})
        model = str(body.get("model") or "gemini-2.5-flash").removeprefix("models/")
        if body.get("stream"):
            return self.stream(model, to_gemini(body))
        t0 = time.time()
        code, g = self.call("POST", f"/v1beta/models/{model}:generateContent", to_gemini(body))
        dur_ms = int((time.time() - t0) * 1000)
        if code != 200:
            record_usage(model, "oai", {}, None, dur_ms, False, "",
                         (g.get("error") or {}).get("message", "") if isinstance(g.get("error"), dict) else "")
            return self.err(code, g)
        text, finish = from_gemini(g)
        record_usage(model, "oai", g.get("usageMetadata"), None, dur_ms, True, finish)
        self.send(200, {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion",
                        "created": int(time.time()), "model": model,
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                                     "finish_reason": finish}], "usage": usage(g)})

    def stream(self, model, payload):
        t0 = time.time()
        r = urllib.request.Request(f"{UPSTREAM}/v1beta/models/{model}:streamGenerateContent?alt=sse",
                                   data=json.dumps(payload).encode(), method="POST",
                                   headers={"Content-Type": "application/json", "x-goog-api-key": self.key()})
        try:
            resp = urllib.request.urlopen(r, timeout=600)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                gerr = json.loads(raw)
            except ValueError:
                gerr = {"error": {"message": raw.decode("utf-8", "replace")}}
            record_usage(model, "oai-stream", {}, None, int((time.time() - t0) * 1000), False, "",
                         (gerr.get("error") or {}).get("message", "") if isinstance(gerr.get("error"), dict) else "")
            return self.err(e.code, gerr)
        except (urllib.error.URLError, OSError) as e:
            record_usage(model, "oai-stream", {}, None, int((time.time() - t0) * 1000), False, "",
                         f"upstream unreachable: {e}")
            return self.err(502, {"error": {"message": f"upstream {UPSTREAM} unreachable: {e}"}})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.close_connection = True
        base = {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion.chunk",
                "created": int(time.time()), "model": model}

        def emit(delta, fin=None):
            chunk = dict(base, choices=[{"index": 0, "delta": delta, "finish_reason": fin}])
            self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
            self.wfile.flush()

        t_first = None
        last_usage = {}
        stream_err = ""
        disconnected = False
        finish = None
        try:
            emit({"role": "assistant", "content": ""})
            with resp:
                for line in resp:
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    g = json.loads(line[5:].strip() or b"{}")
                    if g.get("usageMetadata"):
                        last_usage = g["usageMetadata"]
                    if isinstance(g.get("error"), dict):
                        stream_err = g["error"].get("message", "")
                        emit({"content": f"[error] {stream_err}"})
                        finish = "stop"
                        break
                    text, fin = from_gemini(g)
                    if text:
                        if t_first is None:
                            t_first = time.time()
                        emit({"content": text})
                    else:
                        # thinking chunks carry no text; a comment keeps idle clients from timing out
                        self.wfile.write(b": thinking\n\n")
                        self.wfile.flush()
                    if (g.get("candidates") or [{}])[0].get("finishReason"):
                        finish = fin
            emit({}, finish or "stop")
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        record_usage(model, "oai-stream", last_usage,
                     int((t_first - t0) * 1000) if t_first else None,
                     int((time.time() - t0) * 1000),
                     not stream_err and not disconnected, finish or "",
                     stream_err or ("client disconnected" if disconnected else ""))

    def passthrough(self):
        # Front-door fallback: anything that is not an OpenAI route is proxied
        # verbatim to the upstream aistudio-api (panel, /api/*, /v1beta/*).
        t_start = time.time()
        length = int(self.headers.get("Content-Length", 0) or 0)
        data = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in ("host", "content-length", "connection")}
        r = urllib.request.Request(UPSTREAM + self.path, data=data, method=self.command,
                                   headers=headers)
        try:
            resp = urllib.request.urlopen(r, timeout=600)
        except urllib.error.HTTPError as e:
            resp = e
        except (urllib.error.URLError, OSError) as e:
            return self.send(502, {"error": {"message": f"upstream unreachable: {e}"}})
        self.send_response(resp.status)
        ctype = resp.headers.get("Content-Type")
        if ctype:
            self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        gm = re.search(r"/models/([^/:]+):(generateContent|streamGenerateContent)", self.path) \
            if self.command == "POST" else None
        try:
            with resp:
                if gm and gm.group(2) == "streamGenerateContent":
                    t0 = t_start
                    t_first = None
                    last_usage = {}
                    fin = ""
                    gerr = ""
                    for line in resp:
                        self.wfile.write(line)
                        self.wfile.flush()
                        s = line.strip()
                        if not s.startswith(b"data:"):
                            continue
                        try:
                            g = json.loads(s[5:].strip() or b"{}")
                        except ValueError:
                            continue
                        if g.get("usageMetadata"):
                            last_usage = g["usageMetadata"]
                        if isinstance(g.get("error"), dict):
                            gerr = g["error"].get("message", "")
                        cand = (g.get("candidates") or [{}])[0]
                        if cand.get("finishReason"):
                            fin = cand["finishReason"]
                        if t_first is None and any(
                                p.get("text") and not p.get("thought")
                                for p in (cand.get("content") or {}).get("parts", [])):
                            t_first = time.time()
                    record_usage(gm.group(1), "native-stream", last_usage,
                                 int((t_first - t0) * 1000) if t_first else None,
                                 int((time.time() - t0) * 1000),
                                 resp.status == 200 and not gerr, fin, gerr)
                elif gm:
                    t0 = t_start
                    raw = resp.read()
                    self.wfile.write(raw)
                    self.wfile.flush()
                    usage_meta, fin, gerr = {}, "", ""
                    try:
                        g = json.loads(raw or b"{}")
                        usage_meta = g.get("usageMetadata") or {}
                        fin = (g.get("candidates") or [{}])[0].get("finishReason", "") \
                            if g.get("candidates") else ""
                        if isinstance(g.get("error"), dict):
                            gerr = g["error"].get("message", "")
                    except ValueError:
                        pass
                    record_usage(gm.group(1), "native", usage_meta, None,
                                 int((time.time() - t0) * 1000),
                                 resp.status == 200 and not gerr, fin, gerr)
                elif ctype and "text/html" in ctype:
                    raw = resp.read()
                    if b"</body>" in raw and b"taotao-usage-pill" not in raw:
                        raw = raw.replace(b"</body>", PANEL_PILL.encode() + b"</body>", 1)
                    self.wfile.write(raw)
                    self.wfile.flush()
                else:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_PUT(self):
        return self.passthrough()

    def do_DELETE(self):
        return self.passthrough()

    def do_PATCH(self):
        return self.passthrough()

    def log_message(self, fmt, *args):
        print(time.strftime("%H:%M:%S"), self.command, self.path, *args[1:2], flush=True)


if __name__ == "__main__":
    _load_usage()
    print(f"oai-bridge on http://{HOST}:{PORT}/v1 -> {UPSTREAM} (usage records: {len(_records)})", flush=True)
    ThreadingHTTPServer((HOST, PORT), H).serve_forever()
