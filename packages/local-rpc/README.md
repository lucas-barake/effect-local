# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, wake streams, presence publication, and presence
watch streams over one WebSocket. `SyncServer.layer` keeps that public authenticated facade and routes each operation
through `SpaceEntity.Client`. `SyncClient.layer` adapts the generated typed client to `SyncEngine`.

`SpaceEntity` gives every space one Cluster routing key. All operations are explicitly volatile. The entity command lane
is sequential. Read and stream handlers use `Rpc.fork`, so long lived watches do not block mutation commands. Options
expose mailbox, idle time, defect, and span controls without allowing callers to break the single writer invariant.
`SpaceEntity.layerClient` provides the domain client used by the public facade. `SpaceEntity.layer()` composes both.

For one process, provide `SingleRunner.layer` with the selected runner storage. A Node multi runner deployment provides
`SocketRunner.layer` with `NodeClusterSocket.layerSocketServer` and `NodeClusterSocket.layerClientProtocol`, then supplies
its chosen `RunnerStorage`, `MessageStorage`, `RunnerHealth`, `RpcSerialization`, and `ShardingConfig` Layers. The package
does not select those deployment policies.

The client pending row retains custody until Submit returns. `ServerStore` then retains the authoritative application
event and receipt. A runner failure before admission makes the volatile entity call fail, so reconciliation resubmits the
same identity. A lost reply after SQL commit is recovered from the stored receipt. Cluster mailbox persistence is not
used because Effect beta 101 retains completed payloads and replies without per-request retention control. A custom
`ServerStore` service can route by `spaceId` to database shards. The provided SQL layer uses the supplied `SqlClient` as
one partition. Pulls are repeatable reads. Wake and presence streams are live hints routed through the current owner of
the space.

Authentication follows Effect RPC middleware conventions. The client reads redacted `Credentials` and writes a
bearer header. The server uses `Authenticator` to provide a JSON `Principal`. `ServerStore.layer` requires explicit
access, mutation admission, and read callbacks. `PresenceHub.layer` requires a tagged publish or watch callback and
includes the claimed client ID for publish policy. Use the named `layerTrusted` constructors only where allow all is
intentional. The principal is inserted into the internal entity payload by `SyncServer`. Browser payloads never carry
or choose it.

Use `SyncRpc.layerJson` for the WebSocket serialization Layer. It bounds each complete UTF-8 JSON frame and makes
remote defects opaque. The high level Effect Node server does not expose the underlying `ws` `maxPayload` option, so
production ingress must also enforce `SyncRpc.maximumFrameBytes` with a reverse proxy or a lower level upgrade handler.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
