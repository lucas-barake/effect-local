# Effect Local Chat

A local-first 1:1 chat, in the shape of a very small WhatsApp, built entirely on Effect Local. Every
tab renders from a SQLite replica in the browser; the network is only ever a synchronization
concern.

- Text-only 1:1 conversations between four hardcoded users (every pair is a conversation)
- WhatsApp-style delivery ticks per message: clock, one gray tick, two gray ticks, two blue ticks
- Presence and typing indicators over the transient channel, unread counts, conversation previews
- Multi-tab: tabs of one user share a single replica through a `SharedWorker` and OPFS
- A Node relay server persisting custody in Postgres, exporting OTEL traces to Jaeger
- React + [Base UI](https://base-ui.com) + `@effect/atom-react`, synced over the Effect Local RPC
  relay (WebSocket)

## Running it

```sh
# From examples/chat. Postgres (:5433) + Jaeger (:16686 UI, :4318 OTLP).
docker compose up -d

# The relay server, on :8787.
pnpm dev:server

# The app, on :5174.
pnpm dev
```

Open http://127.0.0.1:5174, pick a user. **A browser profile is one device**: to chat between two
users, open the second user in an incognito window, another browser, or another profile. Tabs of
the same profile are the multi-tab story instead — open several, they share one replica. The app
detects a different user's live replica in the same profile and says so instead of hanging.

Traces land in Jaeger at http://localhost:16686 under the `chat-relay` service: relay `Open`,
`Push`, `Settle` spans and the inbox store's SQL spans.

The Playwright suite drives two full browser devices against a real relay (needs the compose stack;
uses the `chat_test` database so a dev relay can keep running):

```sh
pnpm test:browser
```

## How it works

### One replica per user, shared across tabs

Each tab attaches to a per-user `SharedWorker` (`OwnershipCoordinator.layerTab`). One tab provides
the OPFS database worker; the coordinator elects and migrates ownership as tabs come and go. The
engine inside the worker is a full `SqlReplica` (`layerRelayWithBindings`) plus the relay client
runtime, so queries, mutations, sync, and delivery tracking all live behind one message port. The
OPFS database name embeds the user id — two users in one profile would otherwise contend for the
same wa-sqlite handle pool, which is also why a profile is "one device".

### Conversations are provisioned, not created

Peer sync can only converge documents both replicas already hold, and two replicas independently
creating the same document id fork the Automerge genesis — the merge silently shadows one side's
writes. So exactly one replica ever creates the conversation documents: an ephemeral seeder inside
the relay server creates every roster pair once, exports a backup archive, and persists it in
Postgres. Every browser replica bootstraps by `restoreBackup({ mode: "clone" })` of that same
archive, giving all replicas one shared genesis per document. After that first fetch the app is
fully offline-capable.

### The document is a flat record

```
{ "<message uuid>": { author, body, sentAtMillis, deliveredAtMillis, readAtMillis } }
```

Every key has exactly one creator — the sender creates the entry, and the recipient only assigns
scalar fields inside an entry the sender created. Concurrent edits are register-level and
single-writer, so no merge can orphan a container. The `chat_messages_v1` SQL projection turns the
record into the rows the queries read.

Only state that must survive a disconnect is in here. Awareness is not, which is what keeps the
document small enough to stay cheap to sync.

### Ticks

| Tick     | Source                                                                                                                                                                                    |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| clock    | The mutation committed to the local replica; the relay has not accepted it                                                                                                                |
| one gray | `CommandDelivery.isRelayCustodyAccepted` — the relay durably owns the message. The message id _is_ the send command's uuid, so any tab can resubscribe the delivery stream after a reload |
| two gray | `deliveredAtMillis` — written into the document by the recipient's device as soon as its replica holds the message, whether or not the conversation is on screen                          |
| two blue | `readAtMillis` — written when the recipient has the conversation open                                                                                                                     |

The sync layer only ever proves relay custody back to the sender; delivery and read receipts are
ordinary document data flowing back through the same sync, which is exactly how they stay correct
offline.

### Presence and typing are transient

`Activity` is a `Transient.make` contract registered on the replica definition, carrying a union of
two fieldless events:

```ts
export class Present extends Schema.TaggedClass<Present>()("Present", {}) {}
export class Typing extends Schema.TaggedClass<Typing>()("Typing", {}) {}

export const Activity = Transient.make("Activity", { document: Conversation, payload: Schema.Union([Present, Typing]) })
```

A transient message rides the same authenticated relay session as a document push, but it has no
outbox, no custody, no receipt, no settlement, and no replay. It never enters the document, its
Automerge history, or the relay's Postgres inboxes. An open tab beats `Present` at every counterpart
every 5 seconds, and the composer beats `Typing` while a draft exists, throttled so that a fast
typist cannot outrun the relay's per-session token bucket.

Two consequences follow from having no durability, and both are deliberate:

- **The sender is the session, not the payload.** Both events are fieldless. A receiver reads the
  sender from `message.peerId`, which the relay authenticated, so a payload cannot claim to be
  someone else.
- **Expiry belongs to the receiver.** Nothing tells you a peer went away; beats simply stop. Each
  replica believes an entry for 12 seconds (two missed beats), and a typing entry for 5. That is why
  "last seen at 14:32" is gone from the UI: nothing durable is recorded, so nothing can be claimed
  about an absent peer.

The typing beacon reads the draft as a stream rather than through the atom graph, so a keystroke
does not rebuild the pipeline and the throttle bucket survives the burst. `strategy: "enforce"`
drops what does not fit instead of queueing it, which is also what ends the indicator: the last
keystroke before a pause is usually the dropped one, and the counterpart's window expires.

### One relay channel per conversation

The relay allows one live session per peer endpoint (its inbox key is tenant + subject + peer, and
a new session replaces the previous one), and a session pushes to exactly one remote. Each user
therefore holds one pair-scoped endpoint identity per conversation — one WebSocket, client, and
session per counterpart — and the server's authorization policy grants each endpoint exactly its
pair's document.

### The demo's shortcuts

- Every token is hardcoded and public (`identities.ts`), so any browser can impersonate any user. A
  real deployment issues credentials from its own identity system.
- The roster is fixed at provisioning time. Adding a user later needs a document-adoption path the
  library does not have yet.
- The relay runs a single-runner in-memory cluster; inbox custody itself is durable in Postgres. A
  server restart only drops in-flight entity calls — and even losing the whole database is
  survivable, since every client still holds the full history and re-pushes within its retry
  horizon.
