# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, snapshot bootstrap pages, wake streams, presence
publication, and presence watch streams over one WebSocket. `SyncServer.layer` keeps that public authenticated facade
and routes each operation through `SpaceEntity.Client`. `SyncClient.layer` adapts the generated typed client to
`SyncEngine`.

`SpaceEntity` routes one space through four volatile Cluster entity types. `SpaceAdmissionEntity` serializes Submit and
Discard. `SpaceReadEntity` forks Pull and immutable Bootstrap page reads. `SpaceWatchEntity` forks long lived sync and
presence streams. `SpacePresencePublishEntity` bounds concurrent presence publications. Separate finite mailboxes keep
paused Bootstrap pages and long lived streams out of mutation admission. `SpaceEntity.layerClient` aggregates the four
generated clients for the public facade. `SpaceEntity.layer(options)` composes handlers and that client.

```ts
import * as PresenceHub from "@lucas-barake/effect-local-rpc/PresenceHub"
import * as SpaceEntity from "@lucas-barake/effect-local-rpc/SpaceEntity"
import * as Layer from "effect/Layer"

const PresenceLive = PresenceHub.layer({
  capacity: 1_024,
  maximumWatchersPerSpace: 1_024,
  authorize: authorizePresence
})

const SpaceLive = SpaceEntity.layer({
  admissionMailboxCapacity: 64,
  readMailboxCapacity: 64,
  watchMailboxCapacity: 2_048,
  presencePublicationMailboxCapacity: 256,
  maximumConcurrentBootstrapPagesPerSpace: 8,
  maximumConcurrentPresencePublicationsPerSpace: 64
}).pipe(
  Layer.provide(StoreLive),
  Layer.provide(PresenceLive),
  Layer.provide(RunnerLive)
)
```

All six numeric `SpaceEntity` options are required positive safe integers. A full mailbox maps to `ServerUnavailable`.
Bootstrap concurrency is fail fast and reports typed `CapacityExceeded { resource: "bootstrap pages", limit }`.

For one process, provide `SingleRunner.layer` with the selected runner storage. A Node multi runner deployment provides
`SocketRunner.layer` with `NodeClusterSocket.layerSocketServer` and `NodeClusterSocket.layerClientProtocol`, then supplies
its chosen `RunnerStorage`, `MessageStorage`, `RunnerHealth`, `RpcSerialization`, and `ShardingConfig` Layers. The package
does not select those deployment policies.

The client pending row retains custody until Submit returns. `ServerStore` then retains the authoritative application
event and receipt. A runner failure before admission makes the volatile entity call fail, so reconciliation resubmits the
same identity. A lost reply after SQL commit is recovered from the stored receipt. Cluster mailbox persistence is not
used because Effect `4.0.0-beta.103` retains completed payloads and replies without per-request retention control. A custom
`ServerStore` service can route by `spaceId` to database shards. The provided SQL layer uses the supplied `SqlClient` as
one partition. Pulls return either a bounded dense suffix or `BootstrapRequired`. Bootstrap pages repeat an immutable
manifest and stay under the configured frame bound. Wake and presence streams are live hints routed through the
current owner of the space.

Authentication follows Effect RPC middleware conventions. The client reads redacted `Credentials` and writes a
bearer header. The server uses `Authenticator` to provide a JSON `Principal`. `ServerStore.layer` requires explicit
access, mutation admission, and read callbacks. `PresenceHub.layer` requires a tagged publish or watch callback and
includes the claimed client ID for publish policy. Use the named `layerTrusted` constructors only where allow all is
intentional. The principal is inserted into the internal entity payload by `SyncServer`. Browser payloads never carry
or choose it.

`PresenceHub.maximumWatchersPerSpace` caps active presence streams. It is independent from `capacity`, the sliding
per space update queue depth. Excess presence streams fail with `CapacityExceeded { resource: "presence watchers",
limit }` and release their allowance on interruption or authorization failure. The live population is exported as
`effect_local_server_presence_watcher_count`.

`ServerStore.maximumWatchersPerSpace` separately caps sync streams. One accepted mutation publishes one shared wake
after SQL commit. Delivering it performs no SQLite transaction or space row write per watcher. The benchmark at
`bench/Fanout.bench.ts` exercises the production composed path with 64, 256, and 1,024 watchers.

Use `SyncRpc.layerJson` for the WebSocket serialization Layer. It bounds each complete UTF-8 JSON frame and makes
remote defects opaque. The high level Effect Node server does not expose the underlying `ws` `maxPayload` option, so
production ingress must also enforce `SyncRpc.maximumFrameBytes` with a reverse proxy or a lower level upgrade handler.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
