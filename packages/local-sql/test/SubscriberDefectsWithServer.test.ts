import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
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

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000b1")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

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
  authorizeMutation: () => Effect.void
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerServerDatabase)
)

const remoteOf = (server: ServerStore.ServerStore["Service"]) =>
  SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    credentialGeneration: Effect.succeed(0),
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => server.admitBatch(request, null),
    discard: (request) => server.discard(request, null),
    pull: server.pull,
    bootstrap: server.bootstrap,
    watch: server.watch
  })

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const openClient = Effect.fnUntraced(function*(remote: SyncEngine.SyncEngine["Service"], clientId: Identity.ClientId) {
  const database = yield* Layer.build(Layer.fresh(layerDatabase))
  const layerReplica = SqlReplica.layer({
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceId],
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    migration,
    retryDelay: "1 minute",
    maximumRetryDelay: "1 minute"
  }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
    Layer.provide(Layer.succeedContext(database))
  )
  const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
  return { reactivity: Context.get(database, Reactivity.Reactivity), space: yield* replica.space(spaceId) }
})

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.stream([ReactivityKey.status(space.spaceId)], space.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const isOnlineDrained = (status: ReplicaStatus.SpaceStatus) => status._tag === "Online" && status.pending === 0

describe("a subscriber of an entity that throws on every notification", () => {
  it.effect(
    "does not stop a replica from receiving that entity over several pages or from changing it",
    Effect.fnUntraced(function*() {
      const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
      const remote = remoteOf(server)
      const writer = yield* openClient(remote, Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000b1"))
      let split = 0
      const paged = SyncEngine.SyncEngine.of({
        ...remote,
        pull: (request) =>
          Effect.map(remote.pull(request), (page) => {
            if (!("changes" in page) || page.changes.length === 0 || page.hasMore) return page
            split += 1
            return { ...page, hasMore: true }
          })
      })
      const reader = yield* openClient(paged, Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000b2"))
      let throws = 0
      reader.reactivity.registerUnsafe([ReactivityKey.entity(spaceId, Domain.Todo.name, "shared")], () => {
        throws += 1
        decodeURIComponent("%")
      })
      yield* writer.space.activate
      yield* reader.space.activate
      yield* awaitStatus(writer.reactivity, writer.space, isOnlineDrained)
      yield* awaitStatus(reader.reactivity, reader.space, isOnlineDrained)

      yield* writer.space.mutate(Domain.PutTodo, Domain.todo("shared", "from the writer"))
      yield* awaitStatus(writer.reactivity, writer.space, isOnlineDrained)
      yield* reader.space.deactivate
      yield* reader.space.activate
      yield* awaitStatus(reader.reactivity, reader.space, isOnlineDrained)
      const received = yield* reader.space.get(Domain.Todo, "shared")
      const throwsWhenReceived = throws
      yield* reader.space.mutate(Domain.PutTodo, Domain.todo("shared", "from the reader"))
      yield* awaitStatus(reader.reactivity, reader.space, isOnlineDrained)
      const changed = yield* reader.space.get(Domain.Todo, "shared")

      assert.strictEqual(Option.map(received, (todo) => todo.title).pipe(Option.getOrNull), "from the writer")
      assert.isAbove(split, 0, "the entity arrived in a page that announced more pages")
      assert.isAbove(throwsWhenReceived, 0, "the subscriber threw when the entity arrived")
      assert.strictEqual(Option.map(changed, (todo) => todo.title).pipe(Option.getOrNull), "from the reader")
      assert.isAbove(throws, throwsWhenReceived, "the subscriber threw when the entity changed")
    }, Effect.scoped)
  )
})

describe("a replica activated inside a batch of the caller", () => {
  it.effect(
    "announces the first sync of its space after the batch ended",
    Effect.fnUntraced(function*() {
      const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
      const client = yield* openClient(
        remoteOf(server),
        Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000b3")
      )

      yield* client.reactivity.withBatch(client.space.activate)
      const synced = yield* awaitStatus(client.reactivity, client.space, (status) => status.synced)

      assert.isTrue(synced.synced)
    }, Effect.scoped)
  )
})
