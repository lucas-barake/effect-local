import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as BackupStore from "../src/BackupStore.js"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as DurableRuntime from "../src/DurableRuntime.js"
import * as EntityReplica from "../src/EntityReplica.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("EntityReplica in-flight command limit", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 16,
    maxQueuedRpc: 1
  }

  const buildLive = (executor: Layer.Layer<CommandExecutor.CommandExecutor>) => {
    const database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const infrastructure = Layer.mergeAll(bootstrap, ReplicaLimits.layer(limits))
    const gate = ReplicaGate.layer.pipe(Layer.provideMerge(infrastructure))
    const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
    const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
    const compaction = Compaction.layer.pipe(Layer.provideMerge(recovery))
    const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))
    const commands = Layer.merge(executor, projections)
    const queries = QueryExecutor.layer(definition).pipe(
      Layer.provideMerge(Layer.merge(commands, Reactivity.layer))
    )
    const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
    const backups = BackupStore.layer(definition).pipe(Layer.provideMerge(publisher))
    const durable = DurableRuntime.layer(definition).pipe(
      Layer.provideMerge(Layer.merge(backups, compaction))
    )
    return EntityReplica.layer(definition).pipe(Layer.provideMerge(durable))
  }

  it.effect("rejects a concurrent distinct command beyond the in-flight limit", () =>
    Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const executor = Layer.succeed(
        CommandExecutor.CommandExecutor,
        CommandExecutor.CommandExecutor.of({
          create: (_document, options) =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(CommandOutcome.durablyCommitted(options.commandId, options.documentId))
            ),
          mutate: (_mutation, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          delete: (_document, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
        })
      )
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const firstId = yield* Identity.makeCommandId
        const secondId = yield* Identity.makeCommandId
        const first = yield* Effect.forkChild(replica.create(Task, { commandId: firstId, value: { title: "first" } }))
        yield* Deferred.await(started)
        const rejected = yield* Effect.flip(replica.create(Task, { commandId: secondId, value: { title: "second" } }))
        assert.strictEqual(rejected.reason._tag, "QuotaExceeded")
        yield* Deferred.succeed(release, undefined)
        const committed = yield* Fiber.join(first)
        assert.strictEqual(committed._tag, "DurablyCommittedLocal")
      }).pipe(Effect.provide(buildLive(executor)))
    }).pipe(TestClock.withLive))
})
