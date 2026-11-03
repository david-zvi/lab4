# SPEC: Live dashboard for agent ↔ model traffic

## Goal

While the proxy runs, a browser page shows, live, every message between the agent (Claude Code) and the model, formatted so that at a glance you can tell **your prompt**, **the model's reply**, **a tool call** and **its result** apart, and follow the conversation as it grows. Readability is the point of the product, not polish on top.

## Non-goals

- No changes to `proxy.py`.
- No persistence: nothing is written to disk.
- No editing, replaying or injecting traffic. The dashboard is read-only.
- No third-party dependencies (Python or JS), no build step, no network access from the page.

## Running it

```
python3 dashboard.py                 # starts the proxy on :8041 and the dashboard on :8042
ANTHROPIC_BASE_URL=http://localhost:8041 claude
open http://127.0.0.1:8042
```

`dashboard.py` accepts the same `--port` and `--upstream` flags as `proxy.py` and passes them through, plus `--dashboard-port` (default 8042).

## Architecture

```
claude ──► :8041  proxy.py's Tap (original code, original terminal printing) ──► Anthropic
                     │ wrapped rfile / wfile copy the bytes
                     ▼
               dashboard.py: parse → group into chats → in-memory store
                     │ Server-Sent Events
                     ▼
               browser  :8042  (one static HTML page, vanilla JS)
```

### Capture (proxy.py untouched)

- `dashboard.py` does `import proxy`, defines `TappedTap(proxy.Tap)` and sets `proxy.Tap = TappedTap` before calling `proxy.main()`. `main()` looks `Tap` up at call time, so the original server runs the subclass.
- `TappedTap.relay()` wraps `self.rfile` and `self.wfile` in tee objects, then calls `super().relay()`:
  - the `rfile` tee copies the request body bytes;
  - the `wfile` tee copies the response head (status line and headers) and every body chunk as it is written to the agent.
- Each handler instance gets a monotonically increasing `req_id`. Events (`request`, `response_head`, `chunk`, `done`) are pushed to a `queue.Queue` and processed by one consumer thread. **Capture must never block or break forwarding**: tee writes only enqueue, and any exception in dashboard code is caught and logged, never raised into `relay()`.
- Request headers kept: `X-Claude-Code-Session-Id`, `User-Agent`, `anthropic-beta`. `Authorization` and `x-api-key` are masked exactly as `proxy.py` masks them (first 16 chars + `...`) before storing.
- Only `POST /v1/messages` requests are parsed into chats. Anything else (e.g. `count_tokens`, `GET`s) is shown only in the raw view of the session.

### Parsing

- Request body: JSON with `model`, `system`, `tools`, `messages`, `stream`.
- Response: SSE (`event:` / `data:` lines). Build the reply incrementally from `message_start`, `content_block_start`, `content_block_delta` (`text_delta`, `thinking_delta`, `signature_delta`, `input_json_delta`), `content_block_stop`, `message_delta` (stop reason, usage) and `message_stop`. Each step is forwarded to the browser so the reply streams live.
- Non-2xx responses and proxy `502`s become API-error entries (status plus body).
- SSE chunk boundaries do not align with lines: buffer until a blank line ends an event.

### Grouping requests into chats

Every request resends the full history, so grouping uses history continuity, not headers alone. A request is classified in this order:

1. **Continuation:** its `messages` extend (prefix-match, comparing roles and content blocks) the history of an existing chat from the same session id. Append the new messages and the reply to that chat.
2. **Compaction:** same session id, and the history starts with Claude Code's compaction summary instead of the old history. It stays in the same chat, with a divider `── compacted · HH:MM:SS ──`.
3. **Subagent:** it has `tools`, and its first user message text equals the `prompt` input of an Agent `tool_use` in a chat of the same session whose result has not arrived yet. Nest it under that tool card.
4. **Side call:** it has no `tools`, or it is a single request that is never continued. If a tool call in the same session is waiting for its result, nest the side call under that tool card (e.g. WebFetch's page summarization). Otherwise place it in the timeline at its timestamp (e.g. title generation).
5. **New chat:** anything else, e.g. a new `claude` instance or `/clear`. Opens a new tab.

## UI

### Tabs and panes

- One **tab per chat**. Label: `claude #N` (N = order in which session ids first appeared), plus `· chat M` when one instance has several chats (after `/clear`), plus the model name. A dot marks a chat that is streaming right now.
- Any tab can be opened **side by side** with others (split into 2 or more columns, each pane independent).
- **Open beside**: any expandable entry longer than **10 lines** (subagents, side calls, injected blocks, system prompt, tools, thinking, long tool results) has an "open beside" button that shows it in a new pane, formatted like a full chat.

### Message types (bubbles + paired tool cards)

```
                    ┌─ YOU ─────────────────────────────┐
                    │ Read small_file.txt              │
                    │ ▸ system-reminder: CLAUDE.md     │
                    │ ▸ system-reminder: user context  │
                    │ ▸ system-reminder: attribution   │
                    └──────────────────────────────────┘
MODEL                                         16:39:25 · req #2
  ▸ system prompt (3 blocks)
  ▸ tools (23)
  ▸ thinking
  claude-sonnet-5-5 · in 2 · cache r 22771 / w 12813 · out 153 · 3.1s
┌ TOOL  Read  small_file.txt ──────────────────┐
│ call:   file_path="/home/.../small_file.txt"
│ result: 1  This is a small file
└──────────────────────────────────────────────┘
MODEL                                         16:39:28 · req #3
The file contains "This is a small file".
```

- **Your prompt**: right-aligned bubble with its own color. Shows only the text you typed (the last plain-text block not wrapped in `<system-reminder>` or similar harness tags). Harness-injected blocks in the same message appear as **collapsed rows inside the bubble**, one per block, labeled by type.
- **Model reply**: left-aligned, rendered markdown, no bubble. Header: timestamp and `req #`. Every reply carries collapsed rows for the **system prompt** and **tools** of the request that produced it, and a collapsed **thinking** row that expands to the raw block exactly as sent (`thinking` text, even if empty, plus `signature`).
- **Tool call + result**: **one card**: the call (tool name, short summary of the main argument in the header, full arguments in the body) with its result directly below, matched by `tool_use_id`. Until the result arrives, the card shows `waiting for result…`. Tool results longer than 10 lines are collapsed to the first 10 lines with "show all" and "open beside".
- **Subagent / side call**: entry inside the card or timeline position that triggered it, labeled `SUBAGENT · <description>` or `SIDE CALL · <purpose>` with a one-line summary of the result. Full color, **not dimmed**. Expands in place to the full exchange; over 10 lines, "open beside".
- **API error**: red header line `API ERROR · 529 Overloaded · HH:MM:SS · req #N (retry k)`, collapsed; click to show the error body.
- **Failed tool result** (`is_error: true`): the tool card's header turns red and says `FAILED`; the card is collapsed to the header, and a click shows the call and error text.
- **Dividers**: `/compact` points, as a full-width line.

Each type has a distinct color **and** a text label (`YOU`, `MODEL`, `TOOL`, `SUBAGENT`, `SIDE CALL`, `API ERROR`), so the types stay distinguishable without color too.

### Live behavior

- Replies stream **live**: text grows as it arrives, and tool-call arguments fill in from `input_json_delta`, with a `streaming…` marker until `message_stop`.
- **Scrolling** (per pane): follows the bottom while you are at the bottom. Once you scroll up it stops following and shows `↓ N new`, which jumps back and resumes following.
- Opening or refreshing the page loads the full in-memory history since `dashboard.py` started, then continues live.

### Markdown

Small built-in renderer (~60 lines of JS): fenced code blocks, inline code, headings, ordered and unordered lists, bold and italic, links, tables. All content is HTML-escaped **before** markdown is applied; traffic is untrusted input and must never be inserted as raw HTML.

### Settings (toggles, saved in `localStorage`, all on by default)

- **Usage per reply**: model, input / output tokens, cache read / write, duration (from `message_start` and `message_delta`).
- **Raw view per request**: a `raw` toggle on each model reply showing the exact request JSON and SSE stream (auth masked).
- **Search / filter**: search box per pane, plus type filters (your prompts, replies, tools, subagents, side calls, errors).

## Security

- Dashboard binds to `127.0.0.1` only.
- Auth headers are masked before storage. Nothing is written to disk.
- All rendered content is escaped (see Markdown).
