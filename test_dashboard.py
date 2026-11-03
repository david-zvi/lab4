"""Tests for dashboard.py: chat grouping, SSE parsing, and capture through the real proxy.

    python3 -m unittest test_dashboard
"""

import http.client
import json
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import dashboard
import proxy

TOOLS = [{"name": "Read", "description": "Reads a file\nmore"}]


def sse(*events) -> bytes:
    return "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events).encode()


def reply(*blocks) -> bytes:
    evs = [{"type": "message_start", "message": {"model": "m", "usage": {"input_tokens": 3}}}]
    for i, b in enumerate(blocks):
        if b["type"] == "text":
            evs += [{"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}},
                    {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": b["text"]}}]
        else:
            evs += [{"type": "content_block_start", "index": i, "content_block": {**b, "input": {}}},
                    {"type": "content_block_delta", "index": i,
                     "delta": {"type": "input_json_delta", "partial_json": json.dumps(b["input"])}}]
        evs.append({"type": "content_block_stop", "index": i})
    return sse(*evs, {"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 5}},
               {"type": "message_stop"})


def tool_use(tid, name="Read", **inp):
    return {"type": "tool_use", "id": tid, "name": name, "input": inp}


def user(*content):
    return {"role": "user", "content": list(content)}


def text(t):
    return {"type": "text", "text": t}


class Grouping(unittest.TestCase):
    def setUp(self):
        self.s = dashboard.Store()
        self.rid = 0

    def feed(self, msgs, response=b"", tools=TOOLS, sid="s1", status=200, ctype="text/event-stream", system=None):
        self.rid += 1
        body = json.dumps({"model": "claude-x", "messages": msgs, "tools": tools, "system": system or [], "stream": True}).encode()
        self.s.handle("request", self.rid, time.time(), "POST", "/v1/messages?beta=true",
                      {"x-claude-code-session-id": sid}, len(body))
        self.s.handle("body", self.rid, body)
        self.s.handle("out", self.rid, f"HTTP/1.1 {status} \r\nContent-Type: {ctype}\r\n\r\n".encode())
        for i in range(0, len(response), 7):  # chunk boundaries that cut lines and events
            self.s.handle("out", self.rid, response[i:i + 7])
        self.s.handle("done", self.rid, time.time())
        return self.rid

    def events(self, t):
        return [e for e in map(json.loads, self.s.events) if e["t"] == t]

    def req(self, rid):
        return next(e for e in self.events("req") if e["id"] == rid)

    def test_continuation_pairs_tool_result(self):
        first = [user(text("<system-reminder>x</system-reminder>"), text("read a"))]
        self.feed(first, reply(text("ok"), tool_use("t1", file_path="/a")))
        self.assertIn("t1", self.s.pending)
        self.assertEqual(self.s.pending["t1"]["input"], {"file_path": "/a"})
        second = first + [{"role": "assistant", "content": [text("ok"), tool_use("t1", file_path="/a")]},
                          user({"type": "tool_result", "tool_use_id": "t1", "content": "hello"})]
        rid = self.feed(second, reply(text("done")))
        self.assertEqual(len(self.events("chat")), 1)
        self.assertEqual(self.req(rid)["chat"], self.req(1)["chat"])
        self.assertEqual([m["role"] for m in self.req(rid)["msgs"]], ["user"])  # assistant turn was streamed already
        self.assertEqual(self.req(1)["tools"], [{"name": "Read", "description": "Reads a file"}])
        self.assertNotIn("t1", self.s.pending)
        deltas = [e["d"]["delta"]["text"] for e in self.events("sse") if e["d"].get("delta", {}).get("type") == "text_delta"]
        self.assertEqual(deltas, ["ok", "done"])

    def test_side_call_nests_under_waiting_tool(self):
        self.feed([user(text("fetch"))], reply(tool_use("t1", "WebFetch", url="http://x")))
        self.feed([user(text("summarize this page"))], reply(text("summary")), tools=[], system=[text("Summarize pages")])
        side = self.events("chat")[-1]
        self.assertEqual((side["kind"], side["label"]), ("side", "Summarize pages"))
        self.assertEqual(side["parent"], {"chat": self.events("chat")[0]["id"], "tool": "t1"})

    def test_subagent_and_its_continuation(self):
        self.feed([user(text("go"))], reply(tool_use("a1", "Agent", prompt="find x", description="search")))
        sub1 = [user(text("<system-reminder>r</system-reminder>"), text("find x"))]
        r1 = self.feed(sub1, reply(tool_use("g1", "Grep", pattern="x")))
        r2 = self.feed(sub1 + [{"role": "assistant", "content": [tool_use("g1", "Grep", pattern="x")]},
                               user({"type": "tool_result", "tool_use_id": "g1", "content": "x.py"})], reply(text("found")))
        sub = self.events("chat")[-1]
        self.assertEqual((sub["kind"], sub["label"], sub["parent"]["tool"]), ("sub", "search", "a1"))
        self.assertEqual(self.req(r1)["chat"], sub["id"])
        self.assertEqual(self.req(r2)["chat"], sub["id"])
        self.assertEqual(sub["root"], self.events("chat")[0]["id"])

    def test_new_chats_and_compaction(self):
        self.feed([user(text("one"))], reply(text("a")))
        self.feed([user(text("two"))], reply(text("b")), sid="s2")
        self.feed([user(text("three"))], reply(text("c")))  # /clear: same session, fresh history
        rid = self.feed([user(text("This session is being continued from a previous conversation that ran out of context."))],
                        reply(text("d")))
        chats = self.events("chat")
        self.assertEqual([(c["n"], c["m"]) for c in chats], [(1, 1), (2, 1), (1, 2)])
        self.assertEqual(self.req(rid)["chat"], chats[2]["id"])
        self.assertEqual(self.events("divider")[0]["chat"], chats[2]["id"])

    def test_error_and_retry(self):
        msgs = [user(text("hi"))]
        self.feed(msgs, reply(text("a")))
        rid = self.feed(msgs, b'{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}',
                        status=529, ctype="application/json")
        self.assertEqual(self.req(rid)["retry"], 1)
        done = self.events("done")[-1]
        self.assertEqual((done["status"], done["reason"]), (529, "Overloaded"))
        self.assertIn("overloaded_error", done["error"])

    def test_side_call_before_first_chat_lands_in_it(self):
        self.feed([user(text("make a title"))], reply(text("Title")), tools=[])
        self.feed([user(text("real prompt"))], reply(text("ok")))
        side, main = self.events("chat")
        self.assertEqual(side["parent"], {"chat": main["id"], "tool": None})

    def test_non_streamed_reply_is_replayed_as_events(self):
        msg = {"model": "m", "content": [text("plain")], "stop_reason": "end_turn", "usage": {"output_tokens": 1}}
        self.feed([user(text("q"))], json.dumps(msg).encode(), ctype="application/json")
        types = [e["d"]["type"] for e in self.events("sse")]
        self.assertEqual(types, ["message_start", "content_block_start", "content_block_stop", "message_delta", "message_stop"])


class Upstream(BaseHTTPRequestHandler):
    body = reply(text("hello"))

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers["content-length"]))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(self.body)


class Capture(unittest.TestCase):
    def setUp(self):
        self.store = dashboard.Store()
        threading.Thread(target=dashboard.consume, args=(self.store,), daemon=True).start()
        up = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        threading.Thread(target=up.serve_forever, daemon=True).start()
        proxy.UPSTREAM = f"http://127.0.0.1:{up.server_port}"
        self.px = ThreadingHTTPServer(("127.0.0.1", 0), dashboard.TappedTap)
        threading.Thread(target=self.px.serve_forever, daemon=True).start()

    def post(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.px.server_port)
        conn.request("POST", "/v1/messages", json.dumps({"messages": [user(text("hi"))], "tools": TOOLS}),
                     {"Authorization": "Bearer sk-ant-0123456789abcdef-secret", "X-Claude-Code-Session-Id": "abc"})
        resp = conn.getresponse()
        return resp.status, resp.read()

    def wait_done(self, n):
        for _ in range(100):
            if sum(json.loads(e)["t"] == "done" for e in self.store.events) >= n:
                return [json.loads(e) for e in self.store.events]
            time.sleep(0.02)
        self.fail("capture never finished")

    def test_forwarded_unchanged_and_captured(self):
        status, body = self.post()
        self.assertEqual((status, body), (200, Upstream.body))
        events = self.wait_done(1)
        st = next(iter(self.store.reqs.values()))
        self.assertEqual(st["headers"]["authorization"], "Bearer sk-ant-01...")
        self.assertNotIn("secret", json.dumps(self.store.raw(st["id"])))
        self.assertEqual(events[-1]["error"], None)

        dead = socket.socket()
        dead.bind(("127.0.0.1", 0))
        proxy.UPSTREAM = f"http://127.0.0.1:{dead.getsockname()[1]}"
        dead.close()
        status, _ = self.post()
        self.assertEqual(status, 502)
        self.assertEqual(self.wait_done(2)[-1]["status"], 502)


if __name__ == "__main__":
    unittest.main()
