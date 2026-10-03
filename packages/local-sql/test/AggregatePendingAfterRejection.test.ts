import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Stream from "effect/Stream"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const deniedSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000a1")
const allowedSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000a2")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000a1")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layer({
  definition: Domain.definition,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration,
  authorizeAccess: () => Effect.void,
  authorizeRead: () => Effect.void,
  authorizeMutation: ({ mutation }) => {
    if (mutation.spaceId === deniedSpaceId) return Effect.fail({ _tag: "Forbidden" })
    return Effect.void
  }
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerServerDatabase)
)

const layerRemote = Effect.gen(function*() {
  const server = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => server.admitBatch(request, null),
    discard: (request) => server.discard(request, null),
    pull: server.pull,
    bootstrap: server.bootstrap,
    watch: server.watch
  })
}).pipe(Layer.effect(SyncEngine.SyncEngine), Layer.provide(layerServer))

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const layerReplica = SqlReplica.layer({
  definition: Domain.definition,
  clientId,
  initialSpaces: [deniedSpaceId, allowedSpaceId],
  defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  migration,
  retryDelay: "1 minute",
  maximumRetryDelay: "1 minute"
}).pipe(
  Layer.provide(Domain.layerHandlers),
  Layer.provide(layerRemote)
)

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

describe("aggregate status after a server rejection", () => {
  it.effect(
    "drops the pending count of a mutation the server rejected without advancing the cursor",
    Effect.fnUntraced(function*() {
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const replica = Context.get(
        yield* Layer.build(layerReplica.pipe(Layer.provide(Layer.succeedContext(database)))),
        Replica.Replica
      )
      const denied = yield* replica.space(deniedSpaceId)
      const allowed = yield* replica.space(allowedSpaceId)
      yield* denied.activate
      yield* allowed.activate
      yield* awaitStatus(reactivity, denied, (status) => status._tag === "Online")
      yield* awaitStatus(reactivity, allowed, (status) => status._tag === "Online")

      const rejected = yield* denied.mutate(Domain.PutTodo, Domain.todo("denied"))
      yield* allowed.mutate(Domain.PutTodo, Domain.todo("allowed"))
      yield* awaitStatus(reactivity, allowed, (status) => status._tag === "Online" && status.pending === 0)

      const receipt = yield* denied.receipt(Domain.PutTodo, rejected.envelope.mutationId)
      assert.isTrue(Option.isSome(receipt) && receipt.value._tag === "Rejected")
      const space = yield* denied.status
      assert.strictEqual(space._tag, "Online")
      assert.strictEqual(space.pending, 0)
      assert.strictEqual((yield* replica.status).totalPending, space.pending)
    })
  )
})
