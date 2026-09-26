# @lucas-barake/effect-local-browser

Browser replicas that every open tab shares, and the Effect Atom graph over them.

`BrowserReplica.layer` turns the tabs of one origin into an Effect Cluster. Every tab is a runner that talks to the
others over `BroadcastChannel`, and Web Locks decide which tab is alive and which one leads. The leader tab opens the
SQLite database, runs the sync engine, and hosts one replica entity. Every tab, the leader included, reaches that
entity through `Entity.client`, so reads, writes, live queries, settlements, and ephemera behave the same in each tab.
When the leader closes, another tab takes the lock, reopens the database, and the cluster resends in-flight calls.
Mutations carry caller-minted ids, so a resent mutation is recorded once.

```ts
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Layer from "effect/Layer"
import * as Socket from "effect/unstable/socket/Socket"

const layerReplica = BrowserReplica.layer({
  name: "chat",
  definition,
  layerDatabase: BrowserSqlite.layerWorker(() => new Worker(new URL("./sqlite.worker.ts", import.meta.url))),
  layerSync: SyncClient.layerWebSocket({ url: "wss://example.com/sync" }).pipe(
    Layer.provide(Socket.layerWebSocketConstructorGlobal),
    Layer.provide(Authentication.layerCredentialProviderStatic(bearer))
  ),
  spaces: [spaceId],
  ephemerals,
  profiles: { presence: Presence }
}).pipe(Layer.provide(layerHandlers))

const graph = ReplicaAtom.make(layerReplica)
```

`name` scopes the cluster, its locks, and the durable client id, so two replicas on one origin need different names.
Everything else is optional. `replica` forwards `SqlReplica` options, `sharding` overrides the tab cluster's
`ShardingConfig`, and `retryDelay` (1 second) paces leader election retries. `layerPlatform` replaces the Web Locks,
`BroadcastChannel`, and `localStorage` adapters, which is how the tests run several tabs in one process.
`BrowserSqlite.layerWorker` spawns and owns a dedicated SQLite WASM worker that is terminated when the Layer's scope
closes, and `BrowserSqlite.layerMessagePort` adapts an application-owned worker port instead.

Start the worker with `BrowserSqliteWorker.run`. It holds a Web Lock for the database inside the worker for as long as
the worker lives, so when leadership moves to another tab the new leader waits for the previous worker to close the
OPFS file instead of opening it concurrently:

```ts
import * as BrowserSqliteWorker from "@lucas-barake/effect-local-browser/BrowserSqliteWorker"
import * as Effect from "effect/Effect"

declare const self: DedicatedWorkerGlobalScope

void Effect.runPromise(BrowserSqliteWorker.run({ port: self, dbName: "chat" }))
```

Leadership follows visibility. Chrome deprioritizes the process of a tab whose pages are hidden, so a hidden leader
hands the replica to a visible tab, which takes about 150 ms, and a hidden tab does not take leadership while another
tab is visible.

`ReplicaAtom.make` builds one Atom runtime from that Layer with space-addressed entities, queries, mutations, receipts,
settlements, lifecycle operations, and ephemera.

```ts
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"

const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"))

const Typing = Ephemeral.make("Typing", {
  kind: "event",
  payload: { conversationId: ConversationId, active: Schema.Boolean }
})

const ReadPosition = Ephemeral.make("ReadPosition", {
  kind: "state",
  key: ConversationId,
  payload: { messageId: Schema.String }
})

const Presence = Ephemeral.member({ status: Schema.String })

const member = Protocol.EphemeralMember.make({
  clientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

export const sessionAtom = graph.ephemeral(Presence, {
  spaceId,
  member,
  value: { status: "online" },
  ttl: "30 seconds"
})

export const typingAtom = graph.ephemeralEvents(sessionAtom, Typing)
export const positionsAtom = graph.ephemeralState(sessionAtom, ReadPosition)
export const rosterAtom = graph.ephemeralMembers(sessionAtom)
export const publishTypingAtom = graph.publishEphemeral(Typing, { spaceId, member })
export const publishPositionAtom = graph.publishEphemeral(ReadPosition, { spaceId, member })
```

A definition is declared once and drives everything: the accepted payload type, the JSON encoding on the wire, the
channel filtering, and the automatic decoding on receive. Application code never supplies channel strings, protocol
tags, or `Schema.decode` calls.

`graph.ephemeral` returns the session atom for one `(space, member)` pair. Mounting any projection derived from it
opens exactly one joined server stream, shared by every typed projection. `ephemeralEvents` resolves to the latest
decoded `{ member, payload }` envelope and only observes events published while it is mounted. `ephemeralState`
resolves to the full decoded entry list for its definition, replayed immediately to late subscribers and updated
last-writer-wins per `(member, key)`. `ephemeralMembers` resolves to the decoded roster using the member definition
supplied at session creation.

Publish command atoms wrap the same typed client operation and expose their progress as an Atom `AsyncResult`, which
is why they are built with `runtime.fn`:

```ts
registry.set(publishTypingAtom, {
  payload: { conversationId: ConversationId.make("conversation-42"), active: true },
  ttl: "5 seconds"
})

registry.set(publishPositionAtom, {
  key: ConversationId.make("conversation-42"),
  payload: { messageId: "message-108" },
  ttl: "1 minute"
})
```

The result atom reports waiting, `Success`, or `Failure` (including a typed `EphemeralEncodeError` when a payload or
key does not satisfy its schema). Commands run concurrently; a later write never interrupts an in-flight publish.

A malformed remote value fails only the projection atom for that definition with a typed `EphemeralDecodeError`. The
shared session and every other projection stay live, and refreshing the failed state projection re-reads the current
session view. Peers running an incompatible schema for one channel therefore surface as an isolated decode failure on
that projection, never as a runtime defect. TTL inputs everywhere accept `Duration.Input`; only the serialized RPC
protocol uses integer `ttlMillis` fields.

The scoped client keeps the private server session capability out of atom state, schedules heartbeats from the
accepted lease, and rejoins with a replacement snapshot when its subscriber misses a revision. Disposing the registry
or evicting the idle session atom closes the joined stream and lets the server emit member departure.

Ephemera never enters browser SQLite or the durable mutation log. Persist a read or delivery position with an ordinary
domain mutation when it must survive server restart or the configured state TTL.

`publishEphemeral` and `removeEphemeral` return memoized atom functions for one (definition, space, member) target so a
component can publish or clear state without reaching for the `EphemeralClient` service.

The remaining graph families expose `entity`, `query`, `mutation`, `pending`, `receipt`, `settlements`, `scope`,
`setScope`, `activation`, `activate`, `deactivate`, `status`, `spaces`, `join`, `leave`, and the constant-size
`aggregateStatus`. Effect `Reactivity` refreshes only mounted reads whose exact space, entity, or index range changed.
Leaving a space invalidates retained atoms for that address.

Profiles passed to `ephemeral` sessions must be registered in `BrowserReplica.layer`'s `profiles` option, and
ephemeral definitions in `ephemerals`, because the leader decodes them by name when it relays for another tab.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
