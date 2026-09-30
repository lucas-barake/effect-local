# Chat Example

A WhatsApp-style, local-first chat app built on `effect-local`. It is the
comprehensive end-to-end example for the stack: a React client with a durable
per-user replica in OPFS SQLite, an Expo client with the same replica in
on-device SQLite, a Node sync server with authentication and
authorization, ephemeral typing and presence, and durable delivery/read
receipts — with no loading spinners anywhere, because the UI always renders
from the local replica.

The client and server are composed almost entirely from library Layers:
`SyncClient.layerWebSocket` for the socket, serialization, protocol session,
credential middleware, sync engine, and ephemeral client in one call;
`Authentication.layerCredentialProviderStatic` for the bearer token;
`BrowserSqlite.layerWorker` for the OPFS worker lifecycle; and
`PrincipalAssertion.layerJson` on the server. The application code that remains
is the domain, the authorization policy, and the UI.

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

### Expo client

The Expo app in `mobile/` talks to the same server. Start the server on its own and point the app at it:

```sh
pnpm -C examples/chat dev:server
pnpm -C examples/chat/mobile start
```

Open it in Expo Go or a development build. `EXPO_PUBLIC_CHAT_SERVER_URL` sets the server; it defaults to
`http://10.0.2.2:4100` on Android, which is the host machine as seen from the emulator, and to
`http://localhost:4100` elsewhere. On a physical device use the host's LAN address. Sign in as bob on the phone and
as alice in the browser, and the conversation, ticks, typing, and presence flow between them.

## Layout

```
shared/   @effect-local/example-chat-shared
          domain.ts   — branded ids (UserId, ConversationId, MessageId),
                        models, mutations, queries, ephemeral definitions,
                        the hard-coded user roster
          handlers.ts — deterministic mutation/query handlers shared by
                        client AND server (a replica requirement)
          auth.ts     — login wire contracts and the authenticated Principal
          client.ts   — the per-session atom graph both clients render:
                        queries, sends and retries, presence, typing, and the
                        delivery and settlement daemons
          session.ts  — the login request and the stored session schema
server/   @effect-local/example-chat-server
          server.ts   — makeServerLayer({ port, databaseFile }): /login route,
                        authenticator, per-mutation authorization, ServerStore,
                        EphemeralHub, PrincipalAssertion.layerJson, SyncServer
                        over WebSocket
          main.ts     — thin entrypoint reading CHAT_PORT / CHAT_DB
client/   Vite + React app
          replica.ts  — per-session graph: MultiTab + BrowserReplica over
                        BrowserSqlite.layerWorker and SyncClient.layerWebSocket;
                        the stored session is an Atom.kvs over localStorage and
                        login goes through HttpClient
          chat.tsx    — conversation view: message window pagination, ticks,
                        typing publisher, failed-message overlay
          sqlite.worker.ts — OpfsWorker.run over the worker's own port
mobile/   Expo SDK 57 app
          runtime.ts  — per-session graph: ExpoReplica over expo-sqlite,
                        expo-crypto, and React Native's WebSocket; the session
                        is an Atom.kvs over expo-secure-store
          Chat.tsx    — conversation view with ticks, typing, and read
                        receipts while the app is in the foreground
e2e-mobile/ Playwright spec plus Maestro flows: a web user and an Android
          user chat both ways through one server
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
  password for a bearer token, which the client hands to the sync stack as a
  static `CredentialProvider`. The server authenticates the WebSocket
  handshake and authorizes every mutation: you can only send as yourself,
  start conversations you belong to, and advance your own read state. A
  spoofed `senderId` is rejected and the optimistic write rolls back. A
  rejected token parks the space at `NeedsAuthentication`; the banner signs
  out and reloads so the next login builds a fresh client.
- **Delivery and read receipts.** A shared `ConversationReadState` entity per
  (conversation, user) holds two monotonic cursors, `deliveredUpTo` and
  `readUpTo`. Recipients advance them automatically (delivery on arrival, read
  when the conversation is visible); senders derive per-message tick states
  from the peer's cursors. Reads are membership-gated so senders can observe
  the peer's rows.
- **Ephemeral typing and presence.** Typing is keyed ephemeral state with a
  TTL, published while the draft is non-empty through `graph.publishEphemeral`
  and cleared on send through `graph.removeEphemeral`; presence is an
  ephemeral member profile. Ephemeral identity is minted once per page load
  with `Identity.makeClientId` and is deliberately decoupled from the
  replica's multi-tab client id.
- **Multi-tab out of the box.** `MultiTab.layer` elects one leader tab per
  user; followers proxy the replica over `BroadcastChannel`. Reload or open
  another tab and everything keeps working.
- **Deploys with tabs still open.** When a tab from a newer deploy opens, it
  takes the replica over and every tab of the older build shows "This app was
  updated in another tab" with a Reload button, driven by the typed
  `BuildSuperseded` failure of the space status. The Playwright suite builds
  the client twice, the second build under `/next/` with a bumped tab wire
  protocol (`client/vite.e2e-next.config.ts`), and drives both on one origin.
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
pnpm -C examples/chat e2e     # browser e2e
pnpm -C examples/chat e2e:mobile  # browser plus Android e2e, needs a running emulator with the app installed
```

`e2e:mobile` expects a release build of the app, built with
`EXPO_PUBLIC_CHAT_SERVER_URL=http://10.0.2.2:4199`, installed on a running Android emulator, and the Maestro CLI on
the path. The `e2e-android` job in `.github/workflows/check.yml` shows the exact steps.

The smoke tests boot the real `makeServerLayer` on an ephemeral port with
in-memory SQLite and connect real client stacks per user — no fakes. They
cover login (200/401), send → accepted → delivered → read tick progression,
typing publish/clear, `NeedsAuthentication` for bad tokens, and sender
spoofing rejection with optimistic rollback. The client stack in the tests is
the same `SyncClient.layerWebSocket` composition as the browser, over Node's
WebSocket constructor.

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
