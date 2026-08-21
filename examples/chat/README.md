# Chat Example

A WhatsApp-style, local-first chat app built on `effect-local`. It is the
comprehensive end-to-end example for the stack: a React client with a durable
per-user replica in OPFS SQLite, a Node sync server with authentication and
authorization, ephemeral typing and presence, and durable delivery/read
receipts — with no loading spinners anywhere, because the UI always renders
from the local replica.

## Quickstart

```sh
pnpm install
pnpm -C examples/chat dev
```

Open the printed Vite URL (default `http://localhost:5173`) in two browser
windows and log in as two different users:

| User  | Password |
| ----- | -------- |
| alice | alice123 |
| bob   | bob123   |
| carol | carol123 |
| dave  | dave123  |

Send messages both ways and watch the ticks advance: one gray check (accepted
by the server), two gray checks (delivered to the peer), two blue checks (read
by the peer). Typing indicators and online presence update live. Open a second
tab for the same user and both tabs stay in sync through the multi-tab leader
election — only one tab owns the real replica.

`pnpm -C examples/chat dev` runs `scripts/dev.mjs`, which spawns the sync
server (`CHAT_PORT`, default 4100, SQLite file `CHAT_DB`, default `chat.db`)
and Vite together. Vite proxies `/login` and `/sync` (WebSocket) to the server.

## Layout

```
shared/   @effect-local/example-chat-shared
          domain.ts   — branded ids (UserId, ConversationId, MessageId),
                        models, mutations, queries, ephemeral definitions,
                        the hard-coded user roster
          handlers.ts — deterministic mutation/query handlers shared by
                        client AND server (a replica requirement)
          auth.ts     — login wire contracts and the authenticated Principal
server/   @effect-local/example-chat-server
          server.ts   — makeServerLayer({ port, databaseFile }): /login route,
                        authenticator, per-mutation authorization, ServerStore,
                        EphemeralHub, SyncServer over WebSocket
          main.ts     — thin entrypoint reading CHAT_PORT / CHAT_DB
client/   Vite + React app
          replica.ts  — per-user graph: MultiTab + BrowserReplica over an OPFS
                        SQLite worker, session persistence, login/logout atoms
          chat.tsx    — conversation view: message window pagination, ticks,
                        typing publisher, failed-message overlay
          sqlite.worker.ts — wa-sqlite OPFS worker bootstrap
test/     domain.test.ts — tick-state derivation matrix, branded id invariants
          smoke.test.ts  — in-process end-to-end: real server composition plus
                           real SyncClient + SqlReplica stacks over loopback
                           WebSockets
```

## What it demonstrates

- **Durable local-first state.** Every message write is an optimistic local
  mutation that renders immediately; the sync engine reconciles with the
  server in the background. There are no loading states — queries read from
  the local SQLite replica.
- **Authentication and authorization.** `/login` exchanges a username and
  password for a bearer token. The server authenticates the WebSocket
  handshake and authorizes every mutation: you can only send as yourself,
  start conversations you belong to, and advance your own read state. A
  spoofed `senderId` is rejected and the optimistic write rolls back.
- **Delivery and read receipts.** A shared `ConversationReadState` entity per
  (conversation, user) holds two monotonic cursors, `deliveredUpTo` and
  `readUpTo`. Recipients advance them automatically (delivery on arrival, read
  when the conversation is visible); senders derive per-message tick states
  from the peer's cursors. Reads are membership-gated so senders can observe
  the peer's rows.
- **Ephemeral typing and presence.** Typing is keyed ephemeral state with a
  TTL, published while the draft is non-empty and cleared on send; presence is
  an ephemeral member profile. Ephemeral identity is minted by the app and is
  deliberately decoupled from the replica's multi-tab client id.
- **Multi-tab out of the box.** `MultiTab.layer` elects one leader tab per
  user; followers proxy the replica over `BroadcastChannel`. Reload or open
  another tab and everything keeps working.
- **WhatsApp-style failure UX.** A message whose mutation fails terminally
  (rejected by the server) rolls back out of the durable window and reappears
  from a client-only failed overlay with a red warning icon; retry re-issues
  `SendMessage` with the same message id, discard drops it. A message that
  cannot reach the server yet shows a clock (pending).
- **Growing-window pagination.** The message list reads through a
  `MessagesWindow` query whose `LIMIT` grows as you scroll up, so old history
  pages in reactively from the local replica.

## Testing

```sh
pnpm -C examples/chat test    # domain unit tests + in-process e2e smoke tests
pnpm -C examples/chat check   # project typecheck
```

The smoke tests boot the real `makeServerLayer` on an ephemeral port with
in-memory SQLite and connect real client stacks per user — no fakes. They
cover login (200/401), send → accepted → delivered → read tick progression,
typing publish/clear, `NeedsAuthentication` for bad tokens, and sender
spoofing rejection with optimistic rollback.

## Notes

- The user roster and passwords are hard-coded in `shared/src/domain.ts` — the
  point is to showcase the auth flow, not to model credential storage.
- The failed-message overlay is in-memory per tab; a reload drops failed
  bubbles (the durable log only holds accepted history). "Delete" removes the
  overlay entry only: if the underlying mutation had already been queued
  durably (a narrow window when the owning tab dies mid-commit), it can still
  be delivered — the library has no pending-mutation withdrawal API.
- Receipt cursors are message `createdAt` wall-clock millis. Cross-device
  clock skew can make unread counts and ticks misbehave; a production version
  should cursor on a server-assigned per-conversation sequence instead.
- **Ephemeral identity is client-asserted.** Durable mutations are bound to
  the authenticated principal server-side, but the ephemeral hub's authorize
  hook receives `{ spaceId, member, principal }` — not the published value —
  so a valid token holder can publish presence/typing claiming another user's
  id. Binding that needs a library-level change (the hub authorization input
  would have to carry the value).
- Hardening deliberately left out of this demo (worth doing before copying it
  anywhere real): the WebSocket upgrade performs no `Origin` check, and the
  `/login` body is read unbounded.
- The database file and port are configurable via `CHAT_DB` / `CHAT_PORT`; the
  client dev proxy targets `CHAT_SERVER_URL` when set.
