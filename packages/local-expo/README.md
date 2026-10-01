# @lucas-barake/effect-local-expo

Effect Local replicas on React Native, backed by Expo modules.

`ExpoReplica.layer` builds the same `SqlReplica` the browser and Node use, over an on-device SQLite database opened
with `expo-sqlite`, Effect's `Crypto` served by `expo-crypto`, and React Native's WebSocket. It runs in Expo Go and in
development or release builds, because both native modules ship with the Expo SDK.

```sh
npx expo install expo-sqlite expo-crypto
pnpm add @lucas-barake/effect-local @lucas-barake/effect-local-sql @lucas-barake/effect-local-rpc \
  @lucas-barake/effect-local-expo effect
```

```ts
import * as ExpoReplica from "@lucas-barake/effect-local-expo/ExpoReplica"
import * as ReactNativeSocket from "@lucas-barake/effect-local-expo/ReactNativeSocket"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Layer from "effect/Layer"

export const graph = ReplicaAtom.make(
  ExpoReplica.layer({
    definition,
    database: { filename: `chat-${userId}.db` },
    initialSpaces: [spaceId]
  }).pipe(
    Layer.provideMerge(SyncClient.layerWebSocket({ url: "wss://example.com/sync" })),
    Layer.provide(ReactNativeSocket.layerWebSocketConstructor),
    Layer.provide(Authentication.layerCredentialProviderStatic(bearer)),
    Layer.provide(layerDomain)
  )
)
```

The replica mints its client identity the first time it opens a database file and keeps it in that file, so the
identity lives exactly as long as the local data. Use one file per signed in account. The sync engine is a requirement
of the layer, like any other service. `Layer.provideMerge` keeps its `EphemeralClient` in the graph's context, and
`ReactNativeSocket.layerWebSocketConstructor` gives it React Native's WebSocket. `ReplicaAtom.make` mints the graph's
ephemeral member from the `Crypto` the layer exposes, so an app needs no global polyfills.

## Modules

- `ExpoSqliteClient` is an Effect `SqlClient` over `expo-sqlite`, shaped like the official `@effect/sql-sqlite-*`
  clients: `make`, `layer`, and `layerConfig`, with `filename`, `directory`, `disableWAL`, `spanAttributes`,
  `transformResultNames`, and `transformQueryNames`.
- `ExpoCrypto.layer` provides `Crypto` from `expo-crypto`: native secure random bytes of any size and SHA digests.
- `ReactNativeSocket.layerWebSocketConstructor` provides `Socket.WebSocketConstructor` from React Native's WebSocket.
  It forwards handshake headers and delays a close requested while the socket is still connecting until the socket
  opens, because Android ignores that close and would keep the late connection open.
- `ExpoReplica.layer` composes the three with `SqlReplica.layer`.

## expo-sqlite behavior

- `expo-sqlite` runs native calls on a concurrent queue with no ordering per connection, so the client holds its single
  connection for the whole native execution of every statement, and for the whole of every transaction. Native calls
  cannot be cancelled, so an interrupted statement keeps the connection until its native call settles, and closing
  waits for in-flight work.
- The client opens its own native connection instead of sharing `expo-sqlite`'s cached handle for the same file, so
  application code using `expo-sqlite` directly never shares its transaction state.
- WAL journaling is on unless `disableWAL` is set.
- `expo-sqlite` returns INTEGER columns as JavaScript numbers. Integers beyond `Number.MAX_SAFE_INTEGER` lose
  precision, so `SqlClient.SafeIntegers` fails the statement with a `SqlError` instead of rounding, and a `bigint`
  parameter outside the safe range fails before it reaches native code. iOS binds JavaScript numbers as SQLite REAL,
  as `node:sqlite` does; Android binds whole numbers as INTEGER.
- `expo-sqlite` prepares only the first statement of a SQL string and silently ignores the rest, so run one
  statement per call.
- Prepared statements are reused through a least recently used cache of 200, finalized when evicted and before the
  database closes.
- Native errors are classified into `SqlError` reasons from their SQLite result code. `expo-sqlite` reports primary
  codes only, so a UNIQUE violation is a `ConstraintError`.
