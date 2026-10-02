import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Stream from "effect/Stream"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000d01")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000d01")

const layerDatabase = () =>
  Layer.mergeAll(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const layerServer = ServerStore.layerTrusted({
  definition: Domain.definition,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(Layer.provide(layerRuntime), Layer.provide(layerDatabase()))

const layerSync = Effect.gen(function*() {
  const store = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => store.admitBatch(request, null),
    discard: (request) => store.discard(request, null),
    pull: store.pull,
    bootstrap: store.bootstrap,
    watch: store.watch
  })
}).pipe(Layer.effect(SyncEngine.SyncEngine), Layer.provide(layerServer))

const layerReplica = SqlReplica.layer({
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceId],
  retryDelay: "10 millis"
}).pipe(
  Layer.provide(layerSync),
  Layer.provide(Domain.layerHandlers),
  Layer.provide(layerDatabase()),
  Layer.provideMerge(Reactivity.layer)
)

const provideReplica = Effect.provide(layerReplica)

const awaitStatus = (space: Replica.Space, predicate: (status: ReplicaStatus.SpaceStatus) => boolean) =>
  Reactivity.Reactivity.use((reactivity) =>
    reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
      Stream.filter(predicate),
      Stream.runHead,
      Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
    )
  )

const settledOrFailed = (status: ReplicaStatus.SpaceStatus) =>
  status._tag === "Failed" || (status._tag === "Online" && status.synced && status.pending === 0)

describe("rejoining a space", () => {
  it.effect(
    "syncs again after the previous membership changed its replication scope",
    Effect.fnUntraced(
      function*() {
        const replica = yield* Replica.Replica
        const first = yield* replica.space(spaceId)
        yield* first.mutate(Domain.PutTodo, Domain.todo("todo-1"))
        yield* first.setScope(Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }))
        const scoped = yield* awaitStatus(first, settledOrFailed)
        assert.strictEqual(scoped._tag, "Online")

        yield* replica.leave(spaceId)
        const rejoined = yield* replica.join(spaceId)
        yield* rejoined.mutate(Domain.PutTodo, Domain.todo("todo-2"))
        const status = yield* awaitStatus(rejoined, settledOrFailed)
        assert.strictEqual(status._tag, "Online")
        const synced = yield* rejoined.get(Domain.Todo, "todo-1")
        assert.isTrue(Option.isSome(synced))
        const pending = yield* rejoined.pending
        assert.deepStrictEqual(pending, [])
      },
      provideReplica
    ),
    30_000
  )
})
