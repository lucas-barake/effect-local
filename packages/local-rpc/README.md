# @lucas-barake/effect-local-rpc

Authenticated Effect RPC synchronization for Effect Local.

One `SyncRpc.Rpcs` group carries mutation submission, ordered pulls, wake streams, presence publication, and presence
watch streams over one WebSocket. `SyncServer.layer` binds the group to `ServerStore` and `PresenceHub`.
`SyncClient.layer` adapts the generated typed client to `SyncEngine`.

Authentication follows Effect RPC middleware conventions. The client reads redacted `Credentials` and writes a
bearer header. The server uses `Authenticator` to provide a JSON `Principal`. Mutation and read authorization remain
application callbacks on `ServerStore`; presence authorization remains a callback on `PresenceHub`.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
