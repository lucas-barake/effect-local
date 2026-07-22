import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type { ConfigError } from "effect/Config"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as Migrator from "effect/unstable/sql/Migrator"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as BackupStore from "./BackupStore.js"
import * as CommandExecutor from "./CommandExecutor.js"
import * as CommitPublisher from "./CommitPublisher.js"
import * as Compaction from "./Compaction.js"
import * as DocumentStore from "./DocumentStore.js"
import * as DurableRuntime from "./DurableRuntime.js"
import * as EntityReplica from "./EntityReplica.js"
import * as InternalAutomerge from "./internal/automerge.js"
import type * as PeerSync from "./PeerSync.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaGate from "./ReplicaGate.js"
import type * as ReplicaWorkflow from "./ReplicaWorkflow.js"
import type * as SqlProjection from "./SqlProjection.js"

export const layerFromServices = (definition: ReplicaDefinition.Any): Layer.Layer<
  Replica.Replica,
  never,
  | BackupStore.BackupStore
  | CommandExecutor.CommandExecutor
  | CommitPublisher.CommitPublisher
  | DocumentStore.DocumentStore
  | QueryExecutor.QueryExecutor
  | ReplicaGate.ReplicaGate
  | Crypto.Crypto
> =>
  Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const commands = yield* CommandExecutor.CommandExecutor
      const publisher = yield* CommitPublisher.CommitPublisher
      const documents = yield* DocumentStore.DocumentStore
      const queries = yield* QueryExecutor.QueryExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const crypto = yield* Crypto.Crypto

      const withPermit = <A, E, R,>(f: (permit: ReplicaGate.Permit) => Effect.Effect<A, E, R>) =>
        gate.shared.pipe(Effect.flatMap(f), Effect.scoped)

      const service: Replica.Replica["Service"] = {
        create: (document, options) =>
          withPermit((permit) =>
            Effect.gen(function*() {
              const documentId = Identity.documentIdFromCommandId(options.commandId)
              const encoded = yield* Document.encode(document, documentId, options.value)
              const requestHash = yield* CommandExecutor.createRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                document,
                documentId,
                encoded
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              const outcome = yield* commands.create(document, { ...options, documentId, permit, requestHash })
              yield* publisher.publishPending
              return outcome
            })
          ),
        get: (document, documentId) =>
          withPermit(() =>
            Effect.acquireUseRelease(
              documents.load(document, documentId),
              (stored) => Effect.succeed(stored.snapshot),
              (stored) => Effect.sync(() => InternalAutomerge.free(stored.automerge))
            )
          ),
        mutate: <M extends Mutation.Any,>(mutation: M, options: {
          readonly commandId: Identity.CommandId
          readonly documentId: Identity.DocumentId
          readonly payload?: M["payloadSchema"]["Type"]
        }) =>
          withPermit((permit) =>
            Effect.gen(function*() {
              const payload = options.payload as M["payloadSchema"]["Type"]
              const encoded = yield* Schema.encodeEffect(mutation.payloadSchema)(payload).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.DocumentDecodeError({
                      documentId: options.documentId,
                      cause: new ReplicaError.SchemaCause({
                        message: String(cause),
                        path: []
                      })
                    })
                  })
                )
              )
              const requestHash = yield* CommandExecutor.mutationRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                documentId: options.documentId,
                mutation,
                payload: encoded
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              const outcome = yield* commands.mutate(mutation, { ...options, payload, permit, requestHash })
              yield* publisher.publishPending
              return outcome
            })
          ),
        delete: (document, options) =>
          withPermit((permit) =>
            Effect.gen(function*() {
              const requestHash = yield* CommandExecutor.deleteRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                document,
                documentId: options.documentId
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              const outcome = yield* commands.delete(document, { ...options, permit, requestHash })
              yield* publisher.publishPending
              return outcome
            })
          ),
        query: <Q extends Query.Any,>(
          query: Q,
          ...payload: [Q["payloadSchema"]["Type"]] extends [void] ? readonly []
            : readonly [payload: Q["payloadSchema"]["Type"]]
        ) => withPermit(() => queries.execute(query, payload[0] as Q["payloadSchema"]["Type"])),
        lookupMutation: (mutation, commandId) =>
          withPermit((permit) => commands.lookupMutation(mutation, commandId, permit)),
        lookupCreate: (_document, commandId) => withPermit((permit) => commands.lookupCreate(commandId, permit)),
        lookupDelete: (_document, commandId) => withPermit((permit) => commands.lookupDelete(commandId, permit)),
        flush: withPermit(() => publisher.publishPending).pipe(Effect.asVoid),
        status: Stream.succeed({ _tag: "Ready", pendingCommands: 0 }),
        exportBackup: backups.export,
        restoreBackup: (options) =>
          backups.restore(options).pipe(
            Effect.andThen(publisher.invalidate(ReplicaDefinition.invalidationKeys(definition)))
          ),
        exportDocument: (document, documentId) =>
          withPermit(() =>
            Effect.acquireUseRelease(
              documents.load(document, documentId),
              (stored) =>
                Effect.succeed({
                  documentName: document.name,
                  schemaVersion: document.version,
                  value: stored.encoded
                }),
              (stored) => Effect.sync(() => InternalAutomerge.free(stored.automerge))
            )
          ),
        importDocument: (document, options) =>
          Effect.gen(function*() {
            if (options.value.documentName !== document.name || options.value.schemaVersion !== document.version) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new ReplicaError.SchemaCause({
                    message: "Portable document definition mismatch",
                    path: []
                  })
                })
              })
            }
            const documentId = Identity.documentIdFromCommandId(options.commandId)
            const value = yield* Document.decode(document, documentId, options.value.value)
            return yield* service.create(document, { commandId: options.commandId, value })
          })
      }
      return service
    })
  )

export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: { readonly projections: Bindings }
): Layer.Layer<
  | CommitPublisher.CommitPublisher
  | PeerSync.PeerSync
  | Replica.Replica
  | ReplicaGate.ReplicaGate
  | ReplicaWorkflow.CompactionWorkflow
  | Sharding.Sharding,
  ConfigError | Migrator.MigrationError | ReplicaError.ReplicaError | SqlError.SqlError,
  | CommandExecutor.MutationHandlers<D>
  | ProjectionStore.BindingServices<Bindings>
  | QueryExecutor.QueryHandlers<D>
  | ReplicaLimits.ReplicaLimits
  | Crypto.Crypto
  | SqlClient.SqlClient
> => {
  const expected = new Set(definition.projections)
  const actual = new Set(options.projections.map((binding) => binding.projection))
  if (expected.size !== actual.size || [...expected].some((projection) => !actual.has(projection))) {
    throw new TypeError("SqlReplica requires exactly one SQL binding for every projection")
  }
  const bootstrap = ReplicaBootstrap.layer(definition)
  const gate = ReplicaGate.layer.pipe(Layer.provideMerge(bootstrap))
  const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
  const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
  const compaction = Compaction.layer.pipe(Layer.provideMerge(recovery))
  const projections = ProjectionStore.layer(options.projections).pipe(Layer.provideMerge(store))
  const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(projections))
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
