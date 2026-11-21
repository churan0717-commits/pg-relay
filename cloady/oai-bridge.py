#!/usr/bin/env python3
"""OpenAI chat/completions -> Gemini generateContent. Python 3 stdlib only."""
import copy, hashlib, hmac, json, os, random, re, secrets, threading, time, uuid, urllib.request, urllib.error
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

# ---- 抗截断 B：合成工具调用传输（仿 su-kaka 现行 gcli2api / agy2api） ----
# 给请求塞一个随机命名的假工具并强制调用（toolConfig ANY），要求模型把完整回复
# 写进工具的 content 参数，桥再把参数还原成正文发给客户端：正文不走正文通道，
# 躲开流式审查中途掐断。实测本站（AI Studio 线）：工具参数整包到达、不分片，
# 所以流式下正文是「等全文到齐→切片下发」，首字延迟约等于整篇生成时间。
# 若 finish=length（参数本身撞上输出上限）→ 回喂已收内容自动续写，最多
# ANTI_TRUNC_MAX 次（环境变量 OAI_BRIDGE_ANTI_TRUNC_MAX，默认 3）。
# 本线实测：工具调用被上限截断时整包被上游吞掉、无部分可捞（不像 Antigravity 线
# 有 partialArgs 前缀）——所以若多轮仍一字未得，最后兜底走一次普通文本请求，
# 至少交出半截正文，好过空回。环境变量 OAI_BRIDGE_TOOL_TRANSPORT=off 整体
# 关闭 B。客户端自己带 tools 时：工具照常转发给谷歌、调用按 OpenAI 格式交回；
# 若同时选了防截断，回复工具与客户端工具同场（toolConfig AUTO，正文走回复
# 工具、插件工具被调用时原样交回客户端，不自动续写）。
TOOL_TRANSPORT = os.environ.get("OAI_BRIDGE_TOOL_TRANSPORT", "on") != "off"
ANTI_TRUNC_MAX = int(os.environ.get("OAI_BRIDGE_ANTI_TRUNC_MAX", "3"))
# 启用方式与 su-kaka 对齐：模型名前缀标注（agy2api 用「抗截断-」、gcli2api 用
# 「抗截断/」），本站用「防截断-」为主、另两种写法也认。带前缀才走 B 并剥掉
# 前缀再打上游；不带前缀的模型名行为与原来完全一致。/v1/models 会多列一份
# 带前缀的模型，酒馆下拉里能直接选。
ANTI_PREFIXES = ("防截断-", "抗截断-", "抗截断/")


def split_anti_prefix(model):
    """(剥掉防截断前缀的模型名, 是否带前缀)"""
    for p in ANTI_PREFIXES:
        if model.startswith(p):
            return model[len(p):], True
    return model, False
_TOOL_WORDS = ("emit", "relay", "convey", "deliver", "transmit", "present")
CONTINUATION_PROMPT = ("【系统提示】你上一段回复在发送途中被中断了。请从中断处紧接着继续："
                       "不要重复已经写过的内容，不要道歉、不要解释、不要重新开头。")


def new_tool_name():
    return random.choice(_TOOL_WORDS) + "_" + uuid.uuid4().hex[:12]


def _append_system(req, text):
    part = {"text": text}
    si = req.get("systemInstruction")
    req = dict(req)
    if si and si.get("parts"):
        si = dict(si)
        si["parts"] = list(si["parts"]) + [part]
        req["systemInstruction"] = si
    else:
        req["systemInstruction"] = {"parts": [part]}
    return req


def inject_tool(req, name, coexist=False):
    req = copy.deepcopy(req)
    decl = {"name": name,
            "description": "Send your final reply to the user through this tool.",
            "parameters": {"type": "OBJECT",
                           "properties": {
                               "thought": {"type": "STRING",
                                           "description": "A brief summary of your reasoning."},
                               "content": {"type": "STRING",
                                           "description": "Your complete reply text, exactly as the user should read it."}},
                           "required": ["content"]}}
    if coexist:
        # 与客户端工具同场：声明并在一起，不强压 ANY（插件工具要能被正常选中），
        # 靠系统指令引导正文走回复工具；客户端已给了 toolConfig 时尊重它的。
        decls = []
        for t in req.get("tools") or []:
            decls.extend(t.get("functionDeclarations") or [])
        decls.append(decl)
        req["tools"] = [{"functionDeclarations": decls}]
        if "toolConfig" not in req:
            req["toolConfig"] = {"functionCallingConfig": {"mode": "AUTO"}}
    else:
        req["tools"] = [{"functionDeclarations": [decl]}]
        req["toolConfig"] = {"functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": [name]}}
    return _append_system(req, (
        "【系统要求】你必须通过名为 " + name + " 的工具发送你的最终回复："
        "把完整回复正文原样写进该工具的 content 参数，thought 参数只写一句简短的思考摘要；"
        "不要在工具之外输出任何正文，也不要向用户提及这个工具或这条要求。"))


def client_tools(body):
    """客户端带来的工具（OpenAI tools / 旧版 functions）→ Gemini 函数声明列表。"""
    decls = []
    for t in body.get("tools") or []:
        if isinstance(t, dict) and t.get("type") in (None, "function"):
            f = t.get("function") or {}
            if f.get("name"):
                d = {"name": f["name"], "description": f.get("description", "")}
                if f.get("parameters"):
                    d["parameters"] = f["parameters"]
                decls.append(d)
    for f in body.get("functions") or []:
        if isinstance(f, dict) and f.get("name"):
            d = {"name": f["name"], "description": f.get("description", "")}
            if f.get("parameters"):
                d["parameters"] = f["parameters"]
            decls.append(d)
    return decls


def extract_calls(g, skip=None):
    """从一条 Gemini 响应抽 functionCall（跳过 skip 名）→ OpenAI tool_calls 列表。"""
    calls = []
    cands = g.get("candidates") or []
    if not cands:
        return calls
    for p in (cands[0].get("content") or {}).get("parts", []):
        fc = p.get("functionCall")
        if fc and fc.get("name") and fc.get("name") != skip:
            args = fc.get("args")
            if not isinstance(args, str):
                args = json.dumps(args or {}, ensure_ascii=False)
            calls.append({"id": "call_" + uuid.uuid4().hex[:24], "type": "function",
                          "function": {"name": fc["name"], "arguments": args}})
    return calls


def with_tool_continuation(req, name, accumulated, sig=None):
    # Gemini 4 硬性要求：回喂的 functionCall 必须带上模型当初发出的 thoughtSignature，
    # 缺了直接 400。签名取自上一轮 functionCall 所在的 part。
    part = {"functionCall": {"name": name, "args": {"content": accumulated}}}
    if sig:
        part["thoughtSignature"] = sig
    r = copy.deepcopy(req)
    r["contents"] = list(r["contents"]) + [
        {"role": "model", "parts": [part]},
        {"role": "user", "parts": [
            {"functionResponse": {"name": name, "response": {"result": "received"}}},
            {"text": CONTINUATION_PROMPT}]},
    ]
    return r


def _content_from_args_str(s):
    """args 是字符串（可能被截断的 JSON）时，抢救 content 字段已解码的前缀。"""
    i = s.find('"content"')
    if i < 0:
        return None
    colon = s.find(':', i)
    j = s.find('"', colon + 1) if colon >= 0 else -1
    if j < 0:
        return None
    out, k = [], j + 1
    while k < len(s):
        ch = s[k]
        if ch == "\\" and k + 1 < len(s):
            esc = s[k + 1]
            if esc == "u" and len(s) >= k + 6:
                try:
                    out.append(chr(int(s[k + 2:k + 6], 16)))
                    k += 6
                    continue
                except ValueError:
                    break
            out.append({"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f"}.get(esc, esc))
            k += 2
            continue
        if ch == '"':
            break
        out.append(ch)
        k += 1
    return "".join(out)


def extract_parts(g, name=None):
    """从一条 Gemini 响应/流块抽 (工具正文 or None, 纯文本, finish or None, 工具签名 or None)。"""
    cands = g.get("candidates") or []
    if not cands:
        reason = (g.get("promptFeedback") or {}).get("blockReason", "")
        return None, "", "content_filter" if reason else "stop", None
    c = cands[0]
    call, plain, sig = None, "", None
    for p in (c.get("content") or {}).get("parts", []):
        fc = p.get("functionCall")
        if fc and (name is None or fc.get("name") == name):
            if p.get("thoughtSignature"):
                sig = p["thoughtSignature"]
            args = fc.get("args")
            content = None
            if isinstance(args, dict):
                content = args.get("content")
            elif isinstance(args, str):
                content = _content_from_args_str(args)
            if content:
                call = (call or "") + str(content)
        elif p.get("text") and not p.get("thought"):
            plain += p["text"]
    fin = FINISH.get(c["finishReason"], "stop") if c.get("finishReason") else None
    return call, plain, fin, sig


def _sum_usage(total, meta):
    for k in ("promptTokenCount", "candidatesTokenCount", "thoughtsTokenCount", "totalTokenCount"):
        v = (meta or {}).get(k)
        if v:
            total[k] = total.get(k, 0) + v
    return total


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
    id2name = {}
    for m in body.get("messages", []):
        role = m.get("role")
        if role in ("system", "developer"):
            system.extend(to_parts(m.get("content")))
            continue
        parts = []
        if role == "assistant":
            parts.extend(to_parts(m.get("content")))
            for tc_ in m.get("tool_calls") or []:
                fn = tc_.get("function") or {}
                nm = fn.get("name") or tc_.get("name")
                if not nm:
                    continue
                if tc_.get("id"):
                    id2name[tc_["id"]] = nm
                args = fn.get("arguments")
                try:
                    args = json.loads(args) if isinstance(args, str) else (args or {})
                except ValueError:
                    args = {"_raw": str(args)}
                parts.append({"functionCall": {"name": nm, "args": args}})
            fc_ = m.get("function_call")
            if isinstance(fc_, dict) and fc_.get("name"):
                args = fc_.get("arguments")
                try:
                    args = json.loads(args) if isinstance(args, str) else (args or {})
                except ValueError:
                    args = {"_raw": str(args)}
                parts.append({"functionCall": {"name": fc_["name"], "args": args}})
            g_role = "model"
        elif role in ("tool", "function"):
            nm = m.get("name") or id2name.get(m.get("tool_call_id") or "", "tool")
            raw = m.get("content")
            if isinstance(raw, list):
                raw = "".join(p.get("text", "") for p in raw if isinstance(p, dict))
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except ValueError:
                parsed = None
            resp = parsed if isinstance(parsed, dict) else {"result": "" if raw is None else str(raw)}
            parts.append({"functionResponse": {"name": nm, "response": resp}})
            g_role = "user"
        else:
            parts.extend(to_parts(m.get("content")))
            g_role = "user"
        if not parts:
            continue
        if contents and contents[-1]["role"] == g_role:
            contents[-1]["parts"].extend(parts)
        else:
            contents.append({"role": g_role, "parts": parts})
    if not contents:
        contents, system = [{"role": "user", "parts": system or [{"text": "."}]}], []
    req = {"contents": contents, "safetySettings": SAFETY}
    if system:
        req["systemInstruction"] = {"parts": system}
    decls = client_tools(body)
    if decls:
        req["tools"] = [{"functionDeclarations": decls}]
        tc = body.get("tool_choice")
        if tc is None and isinstance(body.get("function_call"), dict):
            fcn = body["function_call"].get("name")
            tc = {"type": "function", "function": {"name": fcn}} if fcn else body["function_call"]
        mode, allowed = None, None
        if tc == "none":
            mode = "NONE"
        elif tc == "required":
            mode = "ANY"
        elif isinstance(tc, dict):
            nm = ((tc.get("function") or {}).get("name")) or tc.get("name")
            if nm:
                mode, allowed = "ANY", [nm]
        if mode:
            fcc = {"mode": mode}
            if allowed:
                fcc["allowedFunctionNames"] = allowed
            req["toolConfig"] = {"functionCallingConfig": fcc}
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


# ---------------- 面板口令（站长门） ----------------
# 面板与管理接口（/、/api/*、/usage 等）必须登录后才能用；/v1/* API 与
# /health 不拦（酒馆调用不受影响）。口令哈希、会话签名密钥、巡检内部令牌
# 都存在数据卷上，重启/换容器不丢。口令由用户本人在登录页设置，桥只存哈希。
DATA_DIR = os.path.dirname(USAGE_FILE) or "."
PANEL_AUTH_FILE = os.path.join(DATA_DIR, "panel-auth.json")
PANEL_SECRET_FILE = os.path.join(DATA_DIR, "panel-secret")
INTERNAL_TOKEN_FILE = os.path.join(DATA_DIR, "internal-token")
SESSION_TTL = 7 * 24 * 3600


def _data_file(path, create=None):
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        if create is None:
            return ""
        val = create()
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(val)
            os.chmod(path, 0o600)
        except OSError:
            pass
        return val


def _panel_secret():
    return _data_file(PANEL_SECRET_FILE, lambda: secrets.token_hex(32))


def _internal_token():
    return _data_file(INTERNAL_TOKEN_FILE, lambda: secrets.token_hex(24))


def _panel_hash():
    try:
        with open(PANEL_AUTH_FILE, encoding="utf-8") as f:
            return json.load(f).get("hash", "")
    except (OSError, ValueError):
        return ""


def _hash_pw(pw, salt=None):
    salt = salt or secrets.token_bytes(16)
    dk = hashlib.scrypt(pw.encode("utf-8"), salt=salt, n=16384, r=8, p=1, dklen=32)
    return salt.hex() + "$" + dk.hex()


def _verify_pw(pw, stored):
    try:
        salt_hex, dk_hex = stored.split("$", 1)
        dk = hashlib.scrypt(pw.encode("utf-8"), salt=bytes.fromhex(salt_hex),
                            n=16384, r=8, p=1, dklen=32)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except (ValueError, TypeError):
        return False


def _session_value():
    exp = str(int(time.time()) + SESSION_TTL)
    sig = hmac.new(_panel_secret().encode(), exp.encode(), hashlib.sha256).hexdigest()
    return exp + "." + sig


def _session_ok(value):
    try:
        exp, sig = value.split(".", 1)
        if int(exp) < int(time.time()):
            return False
        want = hmac.new(_panel_secret().encode(), exp.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, want)
    except (ValueError, TypeError):
        return False


def _login_page(setup, msg=""):
    title = "设置站长口令" if setup else "奶盖绿茶 · 登录"
    btn = "设置并进入" if setup else "进入"
    action = "/__panel/setup" if setup else "/__panel/login"
    confirm = ('<input name="pw2" type="text" placeholder="再输一遍确认" autocomplete="off">'
               if setup else "")
    hint = ("第一次打开，先给面板设个口令，设完就只有你能进。"
            if setup else "输入站长口令进入面板。")
    msg_html = ('<div style="color:#dc2626;font-size:13px;margin-top:10px;">'
                + msg + "</div>") if msg else ""
    return """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>""" + title + """</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; background: #f9fafb; color: #111827; padding: 40px 16px; }
  .card { max-width: 340px; margin: 0 auto; background: #fff; border: 1.5px solid #e5e7eb; border-radius: 16px; padding: 22px 18px; box-shadow: 0 6px 20px rgba(16,163,127,.08); }
  h1 { font-size: 17px; margin-bottom: 6px; }
  p { font-size: 12px; color: #9ca3af; margin-bottom: 14px; }
  input { width: 100%; padding: 11px 12px; border: 1.5px solid #d1d5db; border-radius: 10px; font-size: 15px; margin-bottom: 10px; -webkit-text-security: disc; }
  input:focus { outline: none; border-color: #10a37f; }
  button { width: 100%; padding: 11px 0; border: none; border-radius: 10px; background: #10a37f; color: #fff; font-size: 15px; font-weight: 600; cursor: pointer; }
</style></head><body><div class="card">
  <h1>""" + title + """</h1><p>""" + hint + """</p>
  <form method="POST" action=\"""" + action + """\">
    <input name="pw" type="text" placeholder="站长口令" autocomplete="off">
    """ + confirm + """
    <button type="submit">""" + btn + """</button>
  </form>""" + msg_html + """
</div></body></html>
"""


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


def record_usage(model, path_kind, usage_meta, ttfb_ms, dur_ms, ok, finish, error="", cont=0):
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
        "cont": cont,
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
          +'<div style="color:#9ca3af;margin-top:3px;">输入 '+fmtNum(r.prompt)+' → 输出 '+fmtNum(r.completion)+'（思考 '+fmtNum(r.thoughts)+'）· 首字 '+fmtMs(r.ttfb_ms)+' · 总 '+fmtMs(r.dur_ms)+(r.cont ? ' · 续写 '+r.cont+' 次' : '')+'</div></div>';
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

    def _authed(self):
        tok = self.headers.get("X-Internal-Token", "")
        if tok and hmac.compare_digest(tok, _internal_token()):
            return True
        for part in self.headers.get("Cookie", "").split(";"):
            part = part.strip()
            if part.startswith("ng_session="):
                return _session_ok(part[len("ng_session="):])
        return False

    def _need_auth(self, page=False):
        if page:
            self.send_response(302)
            self.send_header("Location", "/__panel/login")
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            self.send(401, {"error": {"message": "panel login required"}})

    def _panel_post(self, setup):
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        form = parse_qs(raw)
        pw = (form.get("pw") or [""])[0]
        if setup:
            if _panel_hash():
                return self.send_html(_login_page(False, "口令已经设过了，请直接登录"))
            pw2 = (form.get("pw2") or [""])[0]
            if len(pw) < 4:
                return self.send_html(_login_page(True, "口令至少 4 个字"))
            if pw != pw2:
                return self.send_html(_login_page(True, "两次输入不一致，再试一次"))
            try:
                with open(PANEL_AUTH_FILE, "w", encoding="utf-8") as f:
                    json.dump({"hash": _hash_pw(pw)}, f)
                os.chmod(PANEL_AUTH_FILE, 0o600)
            except OSError:
                return self.send_html(_login_page(True, "存口令失败，请再试一次"))
        else:
            if not _panel_hash():
                return self.send_html(_login_page(True))
            if not _verify_pw(pw, _panel_hash()):
                time.sleep(0.6)
                return self.send_html(_login_page(False, "口令不对，再试一次"))
        self.send_response(302)
        self.send_header("Location", "/")
        self.send_header("Set-Cookie", "ng_session=" + _session_value()
                         + "; Path=/; Max-Age=" + str(SESSION_TTL) + "; HttpOnly; SameSite=Lax")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path == "/__panel/login":
            return self.send_html(_login_page(setup=not _panel_hash()))
        if path == "/__panel/logout":
            self.send_response(302)
            self.send_header("Location", "/__panel/login")
            self.send_header("Set-Cookie", "ng_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
            self.send_header("Content-Length", "0")
            return self.end_headers()
        if path == "/__panel/internal-token":
            # 只在口令还没设时开放，供巡检取走内部令牌；用户设完口令即永久关闭
            if _panel_hash():
                return self.send(404, {"error": {"message": "not found"}})
            return self.send(200, {"token": _internal_token()})
        if path in ("/v1/models", "/models"):
            code, g = self.call("GET", "/v1beta/models")
            if code != 200:
                return self.err(code, g)
            ids = [m.get("name", "").removeprefix("models/") for m in g.get("models", [])]
            data = []
            for i in ids:
                if not i:
                    continue
                data.append({"id": i, "object": "model", "created": 0, "owned_by": "google"})
                # 每个文本模型多列一个「防截断-」版（走 B 通道），酒馆下拉里直接可选
                if i.startswith("gemini-") and not any(
                        s in i for s in ("image", "tts", "embedding", "embed")):
                    data.append({"id": "防截断-" + i, "object": "model",
                                 "created": 0, "owned_by": "google"})
            return self.send(200, {"object": "list", "data": data})
        if path == "/health":
            return self.send(200, {"ok": True, "upstream": UPSTREAM})
        if path == "/usage":
            if not self._authed():
                return self._need_auth(page=True)
            return self.send_html(USAGE_PAGE)
        if path == "/api/usage":
            if not self._authed():
                return self._need_auth()
            q = parse_qs(urlparse(self.path).query)
            return self.send(200, usage_report((q.get("day") or ["cn"])[0]))
        if path == "/api/usage/export":
            if not self._authed():
                return self._need_auth()
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
        if path == "/__panel/login":
            return self._panel_post(setup=False)
        if path == "/__panel/setup":
            return self._panel_post(setup=True)
        if path not in ("/v1/chat/completions", "/chat/completions"):
            return self.passthrough()
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except ValueError:
            return self.send(400, {"error": {"message": "bad json"}})
        label = str(body.get("model") or "gemini-2.5-flash").removeprefix("models/")
        model, anti = split_anti_prefix(label)
        has_client_tools = bool(client_tools(body))
        use_tool = anti and TOOL_TRANSPORT
        if body.get("stream"):
            return self.stream(model, to_gemini(body), use_tool, label, has_client_tools)
        greq = to_gemini(body)
        name = new_tool_name() if use_tool else None
        greq_plain = greq
        if use_tool:
            greq = inject_tool(greq, name, coexist=has_client_tools)
        t0 = time.time()
        total_usage = {}
        acc_text = ""
        finish = ""
        cont = 0
        sig = None
        calls = []
        while True:
            if cont == 0 or not acc_text:
                nxt = greq  # 还没攒下正文（如死在思考阶段）→ 原样重发，不拼续写
            else:
                nxt = with_tool_continuation(greq, name, acc_text, sig)
            code, g = self.call("POST", f"/v1beta/models/{model}:generateContent", nxt)
            if code != 200:
                if cont > 0 and acc_text:
                    finish = "length"  # 续写请求失败：把已收到的部分交出去，好过报错
                    break
                record_usage(label, "oai", total_usage, None, int((time.time() - t0) * 1000), False, "",
                             (g.get("error") or {}).get("message", "") if isinstance(g.get("error"), dict) else "", cont)
                return self.err(code, g)
            _sum_usage(total_usage, g.get("usageMetadata"))
            calls = extract_calls(g, skip=name)
            if use_tool:
                call, plain, fin, csig = extract_parts(g, name)
                if csig:
                    sig = csig
                text = call if call is not None else plain
                finish = fin or "stop"
            else:
                text, finish = from_gemini(g)
            acc_text += text
            if calls:
                break  # 客户端工具被调用：这一轮交给客户端处理，不自动续写
            if finish != "length" or not use_tool or cont >= ANTI_TRUNC_MAX:
                break
            cont += 1
        if use_tool and not acc_text and not calls and finish != "content_filter":
            # 工具通道多轮一字未得（调用被上限整包吞掉）：兜底走一次普通文本，
            # 交半截正文也好过空回。兜底失败不覆盖已有 finish/账目。
            code, g = self.call("POST", f"/v1beta/models/{model}:generateContent", greq_plain)
            if code == 200:
                _sum_usage(total_usage, g.get("usageMetadata"))
                acc_text, finish = from_gemini(g)
        dur_ms = int((time.time() - t0) * 1000)
        if calls:
            finish = "tool_calls"
        record_usage(label, "oai", total_usage, None, dur_ms, True, finish, "", cont)
        msg = {"role": "assistant", "content": acc_text if acc_text or not calls else None}
        if calls:
            msg["tool_calls"] = calls
        self.send(200, {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion",
                        "created": int(time.time()), "model": label,
                        "choices": [{"index": 0, "message": msg,
                                     "finish_reason": finish}],
                        "usage": {"prompt_tokens": total_usage.get("promptTokenCount", 0),
                                  "completion_tokens": total_usage.get("candidatesTokenCount", 0),
                                  "total_tokens": total_usage.get("totalTokenCount", 0)}})

    def stream(self, model, payload, use_tool=False, label=None, has_client_tools=False):
        label = label or model
        t0 = time.time()
        name = new_tool_name() if use_tool else None
        plain_payload = payload
        if use_tool:
            payload = inject_tool(payload, name, coexist=has_client_tools)

        def open_upstream(cur):
            r = urllib.request.Request(f"{UPSTREAM}/v1beta/models/{model}:streamGenerateContent?alt=sse",
                                       data=json.dumps(cur).encode(), method="POST",
                                       headers={"Content-Type": "application/json", "x-goog-api-key": self.key()})
            try:
                return urllib.request.urlopen(r, timeout=600), 0, None
            except urllib.error.HTTPError as e:
                raw = e.read()
                try:
                    gerr = json.loads(raw)
                except ValueError:
                    gerr = {"error": {"message": raw.decode("utf-8", "replace")}}
                return None, e.code, gerr
            except (urllib.error.URLError, OSError) as e:
                return None, 502, {"error": {"message": f"upstream {UPSTREAM} unreachable: {e}"}}

        # 首轮在发响应头之前打开：上游报错还能按 HTTP 错误返回（与旧行为一致）
        resp, ecode, gerr = open_upstream(payload)
        if resp is None:
            record_usage(label, "oai-stream", {}, None, int((time.time() - t0) * 1000), False, "",
                         (gerr.get("error") or {}).get("message", "") if isinstance(gerr.get("error"), dict) else "")
            return self.err(ecode, gerr)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.close_connection = True
        base = {"id": "chatcmpl-" + uuid.uuid4().hex[:24], "object": "chat.completion.chunk",
                "created": int(time.time()), "model": label}

        # 空块保活：等上游出声期间，每隔几秒发一个空内容块，让酒馆端计时走字、
        # 连接不被中间环节当闲置掐掉（学 uncut-bridge）。间隔秒数由环境变量
        # OAI_BRIDGE_KEEPALIVE_SEC 控制（默认 5），0 关闭。写套接字统一走锁，
        # 保活线程与主线程不会把 SSE 行写花。
        wlock = threading.Lock()
        last_data = [time.time()]
        ka_done = [False]

        def write_bytes(b):
            with wlock:
                self.wfile.write(b)
                self.wfile.flush()

        def emit(delta, fin=None):
            chunk = dict(base, choices=[{"index": 0, "delta": delta, "finish_reason": fin}])
            write_bytes(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
            last_data[0] = time.time()

        def keepalive():
            try:
                sec = float(os.environ.get("OAI_BRIDGE_KEEPALIVE_SEC", "5"))
            except ValueError:
                sec = 5.0
            if sec <= 0:
                return
            while not ka_done[0]:
                time.sleep(1)
                if ka_done[0]:
                    break
                if time.time() - last_data[0] >= sec:
                    try:
                        emit({"content": ""})
                    except (BrokenPipeError, ConnectionResetError):
                        break

        threading.Thread(target=keepalive, daemon=True).start()

        acc_text = ""
        total_usage = {}
        t_first = None
        stream_err = ""
        disconnected = False
        finish = None
        cont = 0
        sig = None
        client_called = False

        def emit_piece(piece):
            nonlocal t_first
            if t_first is None:
                t_first = time.time()
            if use_tool:
                # 工具参数整包到达：切成小片下发，客户端渲染更顺，也避免单个巨块
                for i in range(0, len(piece), 120):
                    emit({"content": piece[i:i + 120]})
            else:
                emit({"content": piece})

        try:
            emit({"role": "assistant", "content": ""})
            while True:
                if cont > 0:
                    # 还没攒下正文（如死在思考阶段）→ 原样重发，不拼续写
                    nxt = with_tool_continuation(payload, name, acc_text, sig) if acc_text else payload
                    resp, ecode, gerr = open_upstream(nxt)
                    if resp is None:
                        if acc_text:
                            break  # 续写打不开：已发的部分就是最终结果，好过报错
                        stream_err = (gerr.get("error") or {}).get("message", "") \
                            if isinstance(gerr.get("error"), dict) else "upstream error"
                        break
                attempt_finish = None
                attempt_err = ""
                attempt_usage = {}
                with resp:
                    for line in resp:
                        line = line.strip()
                        if not line.startswith(b"data:"):
                            continue
                        g = json.loads(line[5:].strip() or b"{}")
                        if g.get("usageMetadata"):
                            # 流内 usageMetadata 是累计值：一轮内取最后一份，跨轮再相加
                            attempt_usage = g["usageMetadata"]
                        if isinstance(g.get("error"), dict):
                            attempt_err = g["error"].get("message", "")
                            break
                        raw_fin = (g.get("candidates") or [{}])[0].get("finishReason")
                        chunk_calls = extract_calls(g, skip=name if use_tool else None)
                        if chunk_calls:
                            for i, c in enumerate(chunk_calls):
                                emit({"tool_calls": [dict(c, index=i)]})
                            client_called = True
                            attempt_finish = "tool_calls"
                            break
                        if use_tool:
                            call, plain, fin, csig = extract_parts(g, name)
                            if csig:
                                sig = csig
                            piece = call if call is not None else plain
                        else:
                            piece, fin = from_gemini(g)
                        if piece:
                            acc_text += piece
                            emit_piece(piece)
                        else:
                            # thinking chunks carry no text; a comment keeps idle clients from timing out
                            write_bytes(b": thinking\n\n")
                        if raw_fin:
                            attempt_finish = fin
                if attempt_err:
                    stream_err = attempt_err
                    break
                _sum_usage(total_usage, attempt_usage)
                finish = attempt_finish
                if client_called:
                    break  # 客户端工具被调用：交给客户端，不续写、不兜底
                retryable = finish == "length" or (finish is None and not acc_text)
                if not retryable or not use_tool or cont >= ANTI_TRUNC_MAX:
                    break
                cont += 1
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        if use_tool and not acc_text and not stream_err and not disconnected \
                and not client_called and finish != "content_filter":
            # 工具通道多轮一字未得（调用被上限整包吞掉/流无声断掉）：兜底走一次
            # 普通文本，交半截正文也好过空回。
            try:
                resp, ecode, gerr = open_upstream(plain_payload)
                if resp is not None:
                    fb_usage = {}
                    with resp:
                        for line in resp:
                            line = line.strip()
                            if not line.startswith(b"data:"):
                                continue
                            g = json.loads(line[5:].strip() or b"{}")
                            if g.get("usageMetadata"):
                                fb_usage = g["usageMetadata"]
                            if isinstance(g.get("error"), dict):
                                break
                            piece, fin = from_gemini(g)
                            if piece:
                                acc_text += piece
                                if t_first is None:
                                    t_first = time.time()
                                emit({"content": piece})
                            if (g.get("candidates") or [{}])[0].get("finishReason"):
                                finish = fin
                    _sum_usage(total_usage, fb_usage)
            except (BrokenPipeError, ConnectionResetError):
                disconnected = True
        if stream_err:
            try:
                emit({"content": f"[error] {stream_err}"})
            except (BrokenPipeError, ConnectionResetError):
                disconnected = True
        try:
            emit({}, finish or "stop")
            write_bytes(b"data: [DONE]\n\n")
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        ka_done[0] = True
        record_usage(label, "oai-stream", total_usage,
                     int((t_first - t0) * 1000) if t_first else None,
                     int((time.time() - t0) * 1000),
                     not stream_err and not disconnected, finish or "",
                     stream_err or ("client disconnected" if disconnected else ""), cont)

    def passthrough(self):
        # Front-door fallback: anything that is not an OpenAI route is proxied
        # verbatim to the upstream aistudio-api (panel, /api/*, /v1beta/*).
        # 面板与管理接口先过站长门：没登录的浏览器跳登录页，接口调用回 401。
        if not self._authed():
            return self._need_auth(page="text/html" in self.headers.get("Accept", ""))
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
