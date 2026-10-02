# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization and bounded ephemera for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, snapshot bootstrap pages, wake streams, and
ephemeral join, publish, and heartbeat operations over one WebSocket. `SyncServer.layer` builds the whole server:
the authenticated gateway, one Effect Cluster entity per space, the durable `ServerStore`, the ephemeral hub, signed
principal assertions, and the maintenance singleton. `SyncClient.layer` implements `SyncEngine`, while
`EphemeralClient.layer` exposes the joined ephemeral channel. `ReplicaAtom.make` turns any Layer that provides
`Replica`, `QueryReactivity`, `EphemeralClient`, and `Crypto` into an Effect Atom graph, so the browser, Expo, and Node replicas
share one reactive binding. The graph mints its ephemeral member from that `Crypto`, and any further services the
Layer provides stay available to effects run through `graph.runtime`.

## Server

```ts
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as SyncServer from "@lucas-barake/effect-local-rpc/SyncServer"
import * as SingleRunner from "effect/cluster/SingleRunner"
import * as HttpRouter from "effect/http/HttpRouter"
import * as Layer from "effect/Layer"

const layerSync = SyncServer.layer({
  definition,
  authorizeAccess,
  authorizeMutation,
  authorizeRead,
  authorizeEphemeral
}).pipe(Layer.provideMerge(SyncServer.layerProtocolWebSocket({ path: "/sync" })))

export const layerServer = HttpRouter.serve(layerSync).pipe(
  Layer.provide(Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))),
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
  Layer.provide(layerMutationHandlers),
  Layer.provide(layerDatabase),
  Layer.provide([layerHttpServer, SyncRpc.layerJson()])
)
```

`HttpRouter.serve` serves only the routes registered by the layer passed to it, so the WebSocket protocol belongs
inside that layer. Add other routes, such as a login endpoint, to the same layer.

The four authorization callbacks are required. Every limit has a default: `store` takes any `ServerStore` option (see
`ServerStore.defaults`), `ephemeral` any `EphemeralHub` option, `spaces` any `SpaceEntity.HandlerOptions` (see
`SpaceEntity.defaults`), and `maintenance.interval` defaults to one hour. The layer exposes `ServerStore`, so an
application can call `invalidateReadAuthorization` after a permission change or `maintain` from an admin task.

Each space is served by one `EffectLocal/Space` entity, so an active space costs one resident entity. The entity
serializes SubmitBatch and Discard behind one admission permit and serves Pull, Bootstrap, Watch, and ephemeral operations
concurrently. Bootstrap authorizations, bootstrap pages per space, and ephemeral join verifications have fail-fast
bounds, and ephemeral publish and heartbeat requests queue behind their own per-space bound. Saturation reports typed `CapacityExceeded` with resource
`bootstrap authorizations`, `bootstrap pages`, or `ephemeral join verifications`.

For one process, provide `SingleRunner.layer`. For several processes, provide Effect Cluster's runner transport and
SQL runner and message storage instead. `SyncServer.layer` itself is the same on every runner. The gateway on any
runner forwards a request to the runner that owns the space, and the maintenance sweep runs on one runner at a time.

Run the runners' socket transport with NDJSON serialization, for example
`NodeClusterSocket.layer({ serialization: "ndjson" })`. In Effect `4.0.0` the default SchemaBinary runner
serialization breaks volatile streaming entity calls between runners after their first element, which would stop
cross-runner watches and presence. `packages/local-rpc/test/MultiRunner.test.ts` runs two real runners over sockets
with shared SQL storage and covers batch submit, watch, presence, mismatched assertion secrets, and the maintenance
singleton.

Requests reach the space entity with a principal assertion signed by HMAC-SHA256. Pass the same `assertionSecret`
(`Redacted`, at least 32 bytes) to every runner. Without it each process generates its own secret, which is correct
for a single process and makes cross-runner requests fail with `AuthorizationDenied`.

## Ephemeral semantics

The client API is schema first. A channel is declared once with `Ephemeral.make` and requires an explicit
`kind: "event"` or `kind: "state"`; the roster value schema comes from `Ephemeral.member`. The definition's name is the
wire channel, its payload schema encodes on publish and decodes on receive, and a state definition adds a typed key
codec whose encoded form must satisfy the bounded wire key. `Ephemeral.group` rejects duplicate names when an
application collects its definitions.

```ts
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Effect from "effect/Effect"
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

const program = Effect.gen(function*() {
  const ephemeral = yield* EphemeralClient.EphemeralClient
  const session = yield* ephemeral.session(Presence, {
    spaceId,
    member,
    value: { status: "online" },
    ttl: "30 seconds"
  })
  yield* ephemeral.publish(Typing, {
    spaceId,
    member,
    payload: { conversationId: ConversationId.make("conversation-42"), active: true },
    ttl: "5 seconds"
  })
  yield* ephemeral.publish(ReadPosition, {
    spaceId,
    member,
    key: ConversationId.make("conversation-42"),
    payload: { messageId: "message-108" },
    ttl: "1 minute"
  })
  const typing = session.events(Typing)
  const positions = session.state(ReadPosition)
  const roster = session.members
  yield* session.updateMember({ status: "away" })
})
```

`session` opens exactly one joined server stream per `(spaceId, clientId, membershipIncarnation)` and every typed
projection derives from it. Identical concurrent session requests share the same runtime; a request with a different
member value or ttl for a live member fails with `InvalidConfiguration` instead of silently evicting the previous
server session. `session.events` yields decoded live envelopes for its definition only. `session.state` yields the
full decoded entry list for its definition, immediately on subscription (late subscribers included) and on every
change. `session.members` yields the decoded roster. `clear` and `remove` are the typed counterparts of event
clearing and state removal.

Payload and key encoding failures fail the publish with a typed `EphemeralEncodeError`. A malformed remote value —
for example a peer running an incompatible schema for one channel — fails only the projection stream that decodes it,
with a typed `EphemeralDecodeError`; the shared session and every other projection continue, and resubscribing a state
projection replays the current view. There is no schema version negotiation for ephemeral definitions; they never
participate in durable `Definition` schema identity or replication negotiation.

`session` and TTL-bearing publishes accept `ttl: Duration.Input`. Only the serialized RPC protocol uses integer
`ttlMillis` fields.

The underlying transport identifies a member by `(clientId, membershipIncarnation)` and delivers one ordered stream:

- The first message is a snapshot containing the complete current roster and retained state for that space.
- An event is live only, for typing or similar signals. It is never included in a later snapshot. The server clears it
  when its TTL expires, and callers may clear it sooner.
- State is last-writer-wins per `(member, definition, key)`. It replays to later joiners and supports explicit
  removal. Read positions and delivery positions fit this shape.
- Member liveness is server leased. `EphemeralClient` heartbeats while its session is scoped. Session teardown or
  lease expiry removes the roster entry and emits a departure.
- A new join for the same member identity replaces the old session. The old stream terminates without reconnecting,
  and its live events are cleared before the replacement becomes current.
- Retained state survives member departure until its own server-enforced TTL. This lets a later member observe the
  latest position after a brief disconnect.

Every public message carries its space and an ephemeral revision. Spaces never share roster, state, events, or
revisions. `maximumSpaces` is one hub-wide active-space bound. Other limits apply independently within each space.
Snapshot construction and subscription acquisition use the same per-space critical section as mutation, so there is
no snapshot-to-live gap.

The shared fan-out is bounded and sliding. Each subscriber verifies consecutive revisions. Only a subscriber that
misses a revision closes and rejoins for a replacement snapshot. Other subscribers continue without a snapshot herd.
Live events remain best effort and may be lost. Roster and retained state recover without leaving a successful
subscriber silently stale.

The join RPC privately gives `EphemeralClient` a server-generated session capability and the accepted lease. The
client does not expose the capability in roster or atom state. Publish and heartbeat require it, so a public
`(clientId, membershipIncarnation)` pair is not authority. Expiry, replacement, teardown, or failed periodic
authorization closes the stream and invalidates the capability.

Ephemera never calls `ServerStore`, writes SQL, enters the authoritative mutation log, or requests durable Cluster
mailbox persistence. Server restart may discard it. Applications that need a read or delivery position to survive
restart or the state TTL must persist the compact latest position through a normal application mutation. That durable
policy belongs to the application rather than this generic best-effort transport.

## Bounds and expiry

`EphemeralHub` validates every option when its Layer is built. Every option is optional: `capacity`,
`maximumSpaces`, `maximumWatchersPerSpace`, and `maximumMembersPerSpace` default to 1024, `maximumEventKeysPerMember`
to 64, `maximumEventKeysPerSpace` to 4096, `maximumStateKeysPerMember` to 256, `maximumStateKeysPerSpace` to 16384,
`maximumBytesPerMember` to 1 MiB, `maximumBytesPerSpace` to 16 MiB, and `maximumSnapshotBytes` to the 4 MiB frame
limit. TTL bounds default to the wire maxima. `spaceIdleTtl` must be at least `maximumStateTtl`, so idle eviction
cannot shorten promised state replay.

The wire contract also caps each encoded join or publish payload at 16 KiB, channel and key strings at 256 characters,
member and event TTLs at 60 seconds, and state TTLs at seven days. The server takes the smaller of the requested TTL
and its configured maximum. Member values and retained state count toward per-member and per-space byte limits. Event,
state, member, watcher, and active-space counts have independent limits. Capacity rejection is typed and authorization
runs before capacity disclosure.

`maximumSnapshotBytes` bounds the complete roster and retained-state snapshot below the shared RPC frame limit.
`capacity` bounds the shared per-space delta history, not the number of subscribers. Excess joins fail with
`CapacityExceeded { resource: "ephemeral watchers", limit }` and release their allowance on every stream exit. Active
watchers are exported as `effect_local_server_ephemeral_watcher_count`.

## Client and protocol session

Share one `ProtocolSession` between synchronization and ephemera so both services use one selected protocol version
and renegotiation gate:

```ts
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as ProtocolSession from "@lucas-barake/effect-local-rpc/ProtocolSession"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Layer from "effect/Layer"

const layerSession = ProtocolSession.layerWithOptions({
  supportedProtocolVersions: [1, 2],
  sessionAcquisitionTimeout: "10 seconds"
})

export const layerClientRpc = Layer.merge(
  SyncClient.layerFromSession({ rpcTimeout: "10 seconds" }),
  EphemeralClient.layerFromSession({
    rpcTimeout: "10 seconds",
    heartbeatInterval: "20 seconds"
  })
).pipe(
  Layer.provide(layerSession),
  Layer.provide(layerRpcProtocol),
  Layer.provide(layerAuthentication)
)
```

The actual heartbeat interval is no longer than half the server-accepted member lease. Negotiation selects the highest shared
version. A peer rejection causes one renegotiation and retry. No common version returns terminal `UpgradeRequired`.

`ProtocolSession` and `SyncServer` default to `Protocol.supportedProtocolVersions`, which is `[1]`. `SyncEngine.submitBatch`
sends one `SubmitBatch`, which carries up to `Protocol.maximumSubmitBatchEntries` envelopes of one space and returns their
receipts in order. The server admits a batch
one SQL transaction per envelope and returns a shorter prefix when the response would exceed `Protocol.maximumBatchBytes`
or the batch has run for the store's `maximumSubmitBatchDuration`, default 1 second. The client resubmits the rest.

`sessionAcquisitionTimeout` and `rpcTimeout` accept `Duration.Input` and default to 10 seconds. They bound negotiation,
unary RPCs, and stream acquisition. Established join and watch streams may remain idle. Expiry returns typed
`OperationTimeout`. Socket ping and reconnect detect dead connections without converting healthy idle streams into
retry traffic.

For the common case of one WebSocket per client, `SyncClient.layerWebSocket` composes the socket, JSON
serialization, protocol session, credential middleware, sync engine, and ephemeral client. It requires only a
`CredentialProvider` and a `WebSocketConstructor`, and it builds the credential middleware fresh so two clients with
different providers under one memo map never share it:

```ts
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Socket from "effect/socket/Socket"

export const layerSync = SyncClient.layerWebSocket({ url: "wss://example.com/sync" }).pipe(
  Layer.provide(Socket.layerWebSocketConstructorGlobal),
  Layer.provide(Authentication.layerCredentialProviderStatic(Redacted.make(token)))
)
```

`url` also accepts an `Effect`, which runs again on every reconnect, so a discovered or signed address is never pinned to
the first connection. `Authentication.layerCredentialProvider`
takes a `SubscriptionRef` of credentials when the application rotates tokens; `awaitChange` resolves with the first
credential whose generation differs from the rejected one.

`SyncClient.layerProtocolSocket` exposes Effect's socket retry options:

```ts
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Schedule from "effect/Schedule"

const layerRpcProtocol = SyncClient.layerProtocolSocket({
  retryTransientErrors: true,
  retryPolicy: Schedule.exponential("250 millis").pipe(
    Schedule.jittered,
    Schedule.upTo({ times: 8 })
  )
})
```

## Authentication

The client calls `CredentialProvider.acquire` for every RPC and sends a redacted bearer credential with its generation.
The server resolves a JSON principal through `Authenticator`, and `Authentication.layerServer` turns that into the RPC
middleware `SyncServer.layer` requires. Provide your own `Authentication.Authentication` middleware instead when you
need to wrap it, for example to add rate limiting. The gateway signs the principal into an assertion and the space
entity verifies it before any authorization callback runs, so browser payloads never carry or choose principal
authority.

`CredentialRejected` pauses the rejected credential generation until `awaitChange` returns a new one.
`AuthenticatorUnavailable` is retryable. `AuthorizationDenied` is terminal.

Use `SyncRpc.layerJson` on both sides. It bounds and sanitizes complete JSON frames. Production ingress must enforce
the same native frame limit with a reverse proxy or lower-level WebSocket upgrade handler.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
