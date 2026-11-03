#!/usr/bin/env python3
"""dashboard.py: proxy.py plus a live browser view of the traffic (see SPEC.md).

    python3 dashboard.py                  # proxy on :8041, dashboard on http://127.0.0.1:8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

Accepts proxy.py's --port and --upstream, plus --dashboard-port. proxy.py is not
modified: its Tap class is swapped for a subclass that copies the bytes it
reads and writes. Standard library only; nothing is written to disk.
"""

from __future__ import annotations

import argparse
import itertools
import json
import queue
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import proxy

PAGE = Path(__file__).with_name("dashboard.html")
KEEP = {"x-claude-code-session-id", "user-agent", "anthropic-beta"}
MASK = {"authorization", "x-api-key"}
COMPACTED = "continued from a previous conversation"  # opening of Claude Code's compaction summary

captured: queue.Queue = queue.Queue()
_req_ids = itertools.count(1)


def put(*item) -> None:
    """Hand a capture event to the consumer thread. Never raises into the proxy."""
    try:
        captured.put_nowait(item)
    except Exception:  # noqa: BLE001
        traceback.print_exc()


class Tee:
    """Stands in for rfile/wfile: every call passes through, bytes read or written are copied."""

    def __init__(self, inner, kind: str, rid: int) -> None:
        self._inner, self._kind, self._rid = inner, kind, rid

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def read(self, *args):
        data = self._inner.read(*args)
        self._copy(data)
        return data

    def write(self, data):
        n = self._inner.write(data)
        self._copy(data)
        return n

    def _copy(self, data) -> None:
        try:
            put(self._kind, self._rid, bytes(data))
        except Exception:  # noqa: BLE001
            traceback.print_exc()


class TappedTap(proxy.Tap):
    def relay(self) -> None:
        rid = next(_req_ids)
        try:
            headers = {k.lower(): v[:16] + "..." if k.lower() in MASK else v
                       for k, v in self.headers.items() if k.lower() in KEEP | MASK}
            put("request", rid, time.time(), self.command, self.path, headers,
                int(self.headers.get("content-length") or 0))
        except Exception:  # noqa: BLE001
            traceback.print_exc()
        rfile, wfile = self.rfile, self.wfile
        self.rfile, self.wfile = Tee(rfile, "body", rid), Tee(wfile, "out", rid)
        try:
            super().relay()
        finally:
            self.rfile, self.wfile = rfile, wfile
            put("done", rid, time.time())


# ---- message helpers --------------------------------------------------------

def blocks(msg: dict) -> list[dict]:
    c = msg.get("content")
    return [{"type": "text", "text": c}] if isinstance(c, str) else [b for b in c or [] if isinstance(b, dict)]


def texts(msg: dict) -> list[str]:
    return [b.get("text", "") for b in blocks(msg) if b.get("type") == "text"]


def fingerprint(msg: dict) -> str:
    """Role plus each block's type and text/id. Thinking blocks and cache_control
    are left out: Claude Code drops or moves them between requests."""
    keys = {"text": "text", "tool_use": "id", "tool_result": "tool_use_id"}
    return json.dumps([msg.get("role")] + [[b.get("type"), b.get(keys.get(b.get("type"), ""))]
                                           for b in blocks(msg) if b.get("type") not in ("thinking", "redacted_thinking")])


def system_texts(body: dict) -> list[str]:
    s = body.get("system") or []
    return [s] if isinstance(s, str) else [b.get("text", "") for b in s if isinstance(b, dict)]


def purpose(body: dict) -> str:
    """One-line label for a side call: its own instruction (last system block), else its prompt."""
    msgs = body.get("messages") or []
    for text in system_texts(body)[-1:] + (texts(msgs[0]) if msgs else []):
        for line in text.splitlines():
            if line.strip():
                return line.strip()[:60]
    return "side call"


def synth(msg: dict):
    """A non-streamed JSON reply, replayed as the SSE events a stream would have sent."""
    yield {"type": "message_start", "message": {**msg, "content": []}}
    for i, b in enumerate(msg.get("content") or []):
        yield {"type": "content_block_start", "index": i, "content_block": b}
        yield {"type": "content_block_stop", "index": i}
    yield {"type": "message_delta", "delta": {"stop_reason": msg.get("stop_reason")}, "usage": msg.get("usage") or {}}
    yield {"type": "message_stop"}


# ---- store -------------------------------------------------------------------

class Store:
    """Parses captured traffic, groups it into chats and keeps the event log the browser replays."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[str] = []          # every event sent to browsers, as JSON
        self.subs: list[queue.Queue] = []    # one queue per open /events stream
        self.sessions: dict[str, dict] = {}  # session id -> {"n", "top": [chat ids], "reserved"}
        self.chats: dict[int, dict] = {}     # chat id -> {"id", "sid", "root", "fps", "tools", "retry"}
        self.reqs: dict[int, dict] = {}
        self.pending: dict[str, dict] = {}   # tool_use id -> tool call still waiting for its result
        self.chat_ids = itertools.count(1)

    def emit(self, **ev) -> None:
        line = json.dumps(ev, ensure_ascii=False)
        with self.lock:
            self.events.append(line)
            for q in self.subs:
                q.put((len(self.events) - 1, line))

    def handle(self, kind: str, rid: int, *args) -> None:
        if kind == "request":
            ts, method, path, headers, length = args
            self.reqs[rid] = {"id": rid, "ts": ts, "method": method, "path": path, "headers": headers,
                              "length": length, "body": b"", "out": [], "buf": b"", "status": None,
                              "sse": False, "tool_uses": {}, "classified": False}
            if not length:
                self.classify(self.reqs[rid])
            return
        st = self.reqs.get(rid)
        if st is None:
            return
        if kind == "body":
            st["body"] += args[0]
            if len(st["body"]) >= st["length"]:
                self.classify(st)
        elif kind == "out":
            self.classify(st)
            self.out(st, args[0])
        elif kind == "done":
            self.classify(st)
            self.done(st, args[0])

    # -- grouping (SPEC: "Grouping requests into chats")

    def record(self, sid: str, tools: bool, root: int | None = None) -> dict:
        cid = next(self.chat_ids)
        self.chats[cid] = {"id": cid, "sid": sid, "root": root or cid, "fps": [], "tools": tools, "retry": 0}
        return self.chats[cid]

    def home(self, sess: dict, sid: str) -> int:
        """The session's current tab. Before it has one, an id is reserved for its first chat."""
        if sess["top"]:
            return sess["top"][-1]
        if sess["reserved"] is None:
            sess["reserved"] = self.record(sid, True)["id"]
        return sess["reserved"]

    def nested(self, sess: dict, sid: str, tools: bool, parent: dict, kind: str, label: str, ts: float) -> dict:
        chat = self.record(sid, tools, root=self.chats[parent["chat"]]["root"])
        self.emit(t="chat", id=chat["id"], n=sess["n"], m=None, parent=parent, root=chat["root"],
                  kind=kind, label=label, ts=ts)
        return chat

    def classify(self, st: dict) -> None:
        if st["classified"]:
            return
        st["classified"] = True
        sid = st["sid"] = st["headers"].get("x-claude-code-session-id", "")
        if sid not in self.sessions:
            self.sessions[sid] = {"n": len(self.sessions) + 1, "top": [], "reserved": None}
        sess = self.sessions[sid]
        try:
            body = json.loads(st["body"])
        except ValueError:
            body = None
        if st["method"] != "POST" or st["path"].split("?")[0] != "/v1/messages" or not isinstance(body, dict):
            st["other"] = True
            st["chat"] = self.home(sess, sid)
            self.emit(t="req", other=True, id=st["id"], chat=st["chat"], root=self.chats[st["chat"]]["root"],
                      ts=st["ts"], method=st["method"], path=st["path"])
            return

        msgs = body.get("messages") or []
        has_tools = bool(body.get("tools"))
        fps = [fingerprint(m) for m in msgs]
        for m in msgs:
            for b in blocks(m):
                if b.get("type") == "tool_result":
                    self.pending.pop(b.get("tool_use_id"), None)
        first = {t.strip() for t in texts(msgs[0])} if msgs else set()
        retry, show = 0, msgs

        chat = self.continued(sid, fps, has_tools)
        agent = has_tools and next((w for w in reversed(self.pending.values()) if w["sid"] == sid
                                    and isinstance(w["input"].get("prompt"), str)
                                    and w["input"]["prompt"].strip() in first), None)
        if chat:
            n = len(chat["fps"])
            retry = chat["retry"] + 1 if n == len(fps) else 0
            show = [m for m in msgs[n:] if m.get("role") != "assistant"]  # the reply was already streamed
        elif sess["top"] and any(COMPACTED in t for t in first):
            chat = self.chats[sess["top"][-1]]
            self.emit(t="divider", chat=chat["id"], ts=st["ts"], text="compacted")
        elif agent:
            chat = self.nested(sess, sid, True, {"chat": agent["chat"], "tool": agent["id"]}, "sub",
                               agent["input"].get("description") or "subagent", st["ts"])
        elif not has_tools:
            waiting = next((w for w in reversed(self.pending.values()) if w["sid"] == sid), None)
            parent = ({"chat": waiting["chat"], "tool": waiting["id"]} if waiting
                      else {"chat": self.home(sess, sid), "tool": None})
            chat = self.nested(sess, sid, False, parent, "side", purpose(body), st["ts"])
        else:
            # shortcut: a one-off request that has tools opens a tab, not a side call; demoting it later needs hindsight.
            chat = self.chats[self.home(sess, sid)] if not sess["top"] else self.record(sid, True)
            sess["reserved"] = None
            sess["top"].append(chat["id"])
            self.emit(t="chat", id=chat["id"], n=sess["n"], m=len(sess["top"]), parent=None, root=chat["id"],
                      kind="chat", label="", ts=st["ts"])
        chat["fps"], chat["retry"], chat["tools"] = fps, retry, has_tools
        st["chat"] = chat["id"]
        tools = [{"name": t.get("name", ""), "description": (t.get("description") or "").split("\n")[0]}
                 for t in body.get("tools") or [] if isinstance(t, dict)]
        self.emit(t="req", id=st["id"], chat=chat["id"], root=chat["root"], ts=st["ts"], model=body.get("model", ""),
                  system=system_texts(body), tools=tools, msgs=show, retry=retry)

    def continued(self, sid: str, fps: list[str], tools: bool) -> dict | None:
        """The chat whose history this request extends (longest match, newest on ties)."""
        best = None
        for c in reversed(self.chats.values()):
            n = len(c["fps"])
            if (c["sid"] == sid and c["tools"] == tools and n and fps[:n] == c["fps"]
                    and (best is None or n > len(best["fps"]))):
                best = c
        return best

    # -- response parsing

    def out(self, st: dict, data: bytes) -> None:
        st["out"].append(data)
        if st.get("other"):
            return
        st["buf"] += data
        if st["status"] is None:
            if b"\r\n\r\n" not in st["buf"]:
                return
            head, _, st["buf"] = st["buf"].partition(b"\r\n\r\n")
            head = head.decode("latin-1")
            parts = head.split("\r\n")[0].split(" ", 2)
            st["status"], st["reason"] = int(parts[1]), parts[2] if len(parts) > 2 else ""
            st["sse"] = "text/event-stream" in head.lower()
        if not st["sse"] or st["status"] >= 300:
            return
        st["buf"] = st["buf"].replace(b"\r\n", b"\n")
        while b"\n\n" in st["buf"]:  # chunks don't align with events: wait for the blank line
            event, _, st["buf"] = st["buf"].partition(b"\n\n")
            data = "\n".join(line[5:] for line in event.decode("utf-8", "replace").split("\n") if line.startswith("data:"))
            try:
                self.sse(st, json.loads(data))
            except ValueError:
                pass

    def sse(self, st: dict, d: dict) -> None:
        t, i = d.get("type"), d.get("index")
        if t == "ping":
            return
        if t == "content_block_start" and d["content_block"].get("type") == "tool_use":
            cb = d["content_block"]
            st["tool_uses"][i] = {"id": cb.get("id"), "input": cb.get("input") or {}, "json": ""}
        elif t == "content_block_delta" and i in st["tool_uses"]:
            st["tool_uses"][i]["json"] += d["delta"].get("partial_json", "")
        elif t == "content_block_stop" and i in st["tool_uses"]:
            tu = st["tool_uses"].pop(i)
            try:
                inp = json.loads(tu["json"]) if tu["json"] else tu["input"]
            except ValueError:
                inp = {}
            self.pending[tu["id"]] = {"id": tu["id"], "sid": st["sid"], "chat": st["chat"],
                                      "input": inp if isinstance(inp, dict) else {}}
        self.emit(t="sse", req=st["id"], d=d)

    def done(self, st: dict, ts: float) -> None:
        if st.get("other"):
            return
        status, error = st["status"], None
        if status is None:
            error = "no response from upstream (see the proxy's terminal)"
        elif status >= 300:
            error = st["buf"].decode("utf-8", "replace")
        elif not st["sse"]:
            try:
                for d in synth(json.loads(st["buf"])):
                    self.sse(st, d)
            except (ValueError, AttributeError):
                error = st["buf"].decode("utf-8", "replace")
        reason = st.get("reason") or ""
        if error and not reason:
            try:
                reason = json.loads(st["buf"])["error"]["type"].removesuffix("_error").replace("_", " ").capitalize()
            except Exception:  # noqa: BLE001
                pass
        self.emit(t="done", req=st["id"], ts=ts, status=status, reason=reason, error=error)

    def raw(self, rid: int) -> dict | None:
        st = self.reqs.get(rid)
        if st is None:
            return None
        try:
            body = json.loads(st["body"])
        except ValueError:
            body = st["body"].decode("utf-8", "replace")
        return {"request": {"method": st["method"], "path": st["path"], "headers": st["headers"], "body": body},
                "response": b"".join(st["out"]).decode("utf-8", "replace")}


store = Store()


def consume(s: Store) -> None:
    while True:
        item = captured.get()
        try:
            s.handle(*item)
        except Exception:  # noqa: BLE001  (dashboard bugs must never stop capture)
            traceback.print_exc()


# ---- browser side ------------------------------------------------------------

class Page(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def send(self, code: int, ctype: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/":
            return self.send(200, "text/html; charset=utf-8", PAGE.read_bytes())
        if self.path == "/events":
            return self.stream()
        if self.path.startswith("/raw/") and self.path[5:].isdigit():
            raw = store.raw(int(self.path[5:]))
            if raw:
                return self.send(200, "application/json", json.dumps(raw, ensure_ascii=False).encode())
        self.send(404, "text/plain", b"not found")

    def stream(self) -> None:
        try:  # a reconnecting EventSource says where it stopped
            start = int(self.headers.get("Last-Event-ID", "-1")) + 1
        except ValueError:
            start = 0
        q: queue.Queue = queue.Queue()
        with store.lock:
            backlog = [(i, store.events[i]) for i in range(start, len(store.events))]
            total = len(store.events)
            store.subs.append(q)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            if start > total:  # the dashboard restarted under this page
                self.wfile.write(b"event: reset\ndata: {}\n\n")
            self.wfile.write("".join(f"id: {i}\ndata: {e}\n\n" for i, e in backlog).encode())
            while True:
                try:
                    i, e = q.get(timeout=15)
                    self.wfile.write(f"id: {i}\ndata: {e}\n\n".encode())
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
        except OSError:
            pass
        finally:
            with store.lock:
                store.subs.remove(q)


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--dashboard-port", type=int, default=8042)
    args, rest = parser.parse_known_args()
    sys.argv = [sys.argv[0], *rest]  # proxy.main() parses --port / --upstream itself
    threading.Thread(target=consume, args=(store,), daemon=True).start()
    page = ThreadingHTTPServer(("127.0.0.1", args.dashboard_port), Page)
    threading.Thread(target=page.serve_forever, daemon=True).start()
    print(f"dashboard on http://127.0.0.1:{args.dashboard_port}")
    proxy.Tap = TappedTap  # main() looks Tap up at call time
    proxy.main()


if __name__ == "__main__":
    main()
