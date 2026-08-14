# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization and bounded ephemera for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, snapshot bootstrap pages, wake streams, and
ephemeral join, publish, and heartbeat operations over one WebSocket. `SyncServer.layer` authenticates the public
facade and routes space operations through `SpaceEntity.Client`. `SyncClient.layer` implements `SyncEngine`, while
`EphemeralClient.layer` exposes the joined ephemeral channel.

## Space ownership

`SpaceEntity` uses five volatile Cluster entity types. `SpaceAdmissionEntity` serializes Submit and Discard.
`SpaceReadEntity` forks Pull and immutable Bootstrap pages. `SpaceWatchEntity` owns long lived sync watches.
`SpaceEphemeralJoinEntity` owns joined streams. `SpaceEphemeralCommandEntity` owns publish and heartbeat. The join
entity admits authorization without a preauthorization occupancy cap, then `EphemeralHub.maximumWatchersPerSpace`
applies the required bound. A full join lane cannot starve commands or mutation admission.

```ts
import * as EphemeralHub from "@lucas-barake/effect-local-rpc/EphemeralHub"
import * as SpaceEntity from "@lucas-barake/effect-local-rpc/SpaceEntity"
import * as Layer from "effect/Layer"

const layerEphemeral = EphemeralHub.layer({
  capacity: 1_024,
  maximumSpaces: 1_024,
  maximumWatchersPerSpace: 1_024,
  maximumMembersPerSpace: 1_024,
  maximumEventKeysPerMember: 64,
  maximumEventKeysPerSpace: 4_096,
  maximumStateKeysPerMember: 256,
  maximumStateKeysPerSpace: 16_384,
  maximumBytesPerMember: 1024 * 1024,
  maximumBytesPerSpace: 16 * 1024 * 1024,
  maximumSnapshotBytes: 4 * 1024 * 1024,
  memberTtl: "1 minute",
  authorizationRefreshInterval: "30 seconds",
  maximumEventTtl: "1 minute",
  maximumStateTtl: "7 days",
  spaceIdleTtl: "7 days",
  authorize: authorizeEphemeral
})

const layerSpace = SpaceEntity.layer({
  admissionMailboxCapacity: 64,
  readMailboxCapacity: 64,
  watchMailboxCapacity: 2_048,
  ephemeralJoinMailboxCapacity: 1_280,
  ephemeralCommandMailboxCapacity: 256,
  maximumConcurrentBootstrapAuthorizations: 64,
  maximumConcurrentBootstrapPagesPerSpace: 8,
  maximumConcurrentEphemeralJoinVerificationsPerSpace: 64,
  maximumConcurrentEphemeralRequestsPerSpace: 64
}).pipe(
  Layer.provide(layerStore),
  Layer.provide(layerEphemeral),
  Layer.provide(layerRunner)
)
```

All nine numeric `SpaceEntity` options are required positive safe integers. Size `ephemeralJoinMailboxCapacity` for
active watchers plus bounded pending verification. Join verification and command work have separate concurrency
limits. A full entity mailbox maps to `ServerUnavailable`. Bootstrap concurrency is fail fast. Saturation reports
typed `CapacityExceeded` with resource `bootstrap authorizations`, `bootstrap pages`, or
`ephemeral join verifications`.

For one process, provide `SingleRunner.layer` with the selected runner storage. A multi-runner deployment provides
Effect Cluster runner transport, storage, health, serialization, and sharding Layers. This package does not choose
those deployment policies.

## Ephemeral semantics

A join identifies a member by `(clientId, membershipIncarnation)` and returns one ordered stream:

- The first message is a snapshot containing the complete current roster and retained state for that space.
- `Event` is a live-only stream value for typing or similar signals. It is never included in a later snapshot. The
  server emits `EventCleared` when its TTL expires, and callers may clear it sooner.
- `SetState` is last-writer-wins state per `(member, channel, key)`. It replays to later joiners and supports explicit
  removal. Read positions and delivery positions fit this shape.
- Member liveness is server leased. `EphemeralClient` heartbeats while its joined stream is scoped. Stream teardown or
  lease expiry removes the roster entry and emits `MemberLeft`.
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

`EphemeralHub` validates every option when its Layer is built. `maximumWatchersPerSpace` is required. Defaults for the
other limits are shown in the example above. `spaceIdleTtl` must be at least `maximumStateTtl`, so idle eviction cannot
shorten promised state replay.

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

The actual heartbeat interval is no longer than half the requested member TTL. Negotiation selects the highest shared
version. A peer rejection causes one renegotiation and retry. No common version returns terminal `UpgradeRequired`.

`sessionAcquisitionTimeout` and `rpcTimeout` accept `Duration.Input` and default to 10 seconds. They bound negotiation,
unary RPCs, and stream acquisition. Established join and watch streams may remain idle. Expiry returns typed
`OperationTimeout`. Socket ping and reconnect detect dead connections without converting healthy idle streams into
retry traffic.

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
The server provides a JSON principal through `Authenticator`. `SyncServer.layerHandlers` requires a
`PrincipalAssertion.Issuer`, and `SpaceEntity.layerHandlers` requires the matching verifier. The opaque assertion is
verified before an ephemeral authorization callback runs. Browser payloads never carry or choose principal authority.

`ServerStore.layer` requires access, mutation, and read authorization callbacks. `EphemeralHub.layer` requires a tagged
join, publish, or heartbeat callback containing the space, member, and verified principal. Use `layerTrusted` only when
allow all is intentional.

`CredentialRejected` pauses the rejected credential generation until `awaitChange` returns a new one.
`AuthenticatorUnavailable` is retryable. `AuthorizationDenied` is terminal. Applications own assertion authenticity,
expiry, and key rotation.

Use `SyncRpc.layerJson` on both sides. It bounds and sanitizes complete JSON frames. Production ingress must enforce
the same native frame limit with a reverse proxy or lower-level WebSocket upgrade handler.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
