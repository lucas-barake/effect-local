import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type { ConfigError } from "effect/Config"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as BackupStore from "./BackupStore.js"
import * as CheckpointAuthority from "./CheckpointAuthority.js"
import * as CommandDeliveryPublisher from "./CommandDeliveryPublisher.js"
import * as CommandDeliveryStore from "./CommandDeliveryStore.js"
import * as CommandExecutor from "./CommandExecutor.js"
import * as CommitPublisher from "./CommitPublisher.js"
import * as Compaction from "./Compaction.js"
import * as DocumentStore from "./DocumentStore.js"
import * as DurableRuntime from "./DurableRuntime.js"
import * as EntityReplica from "./EntityReplica.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as InternalConflicts from "./internal/conflicts.js"
import * as NativeError from "./internal/nativeError.js"
import * as PeerConnectionStatus from "./PeerConnectionStatus.js"
import type * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
import type * as PeerSync from "./PeerSync.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as Recovery from "./Recovery.js"
import * as RelayConnectionStatus from "./RelayConnectionStatus.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaEvolution from "./ReplicaEvolution.js"
import * as ReplicaGate from "./ReplicaGate.js"
import * as ReplicaHealth from "./ReplicaHealth.js"
import * as ReplicaOperationScheduler from "./ReplicaOperationScheduler.js"
import type * as ReplicaWorkflow from "./ReplicaWorkflow.js"
import type * as SqlProjection from "./SqlProjection.js"

export interface Options<Bindings extends ReadonlyArray<SqlProjection.Any>,> {
  readonly checkpointAuthority?: CheckpointAuthority.Implementation
  readonly deliveryPublisher?: CommandDeliveryPublisher.Options
  readonly health?: ReplicaHealth.Options
  readonly projections: Bindings
}

export const layerFromServices = (definition: ReplicaDefinition.Any): Layer.Layer<
  Replica.Replica,
  never,
  | BackupStore.BackupStore
  | CommandExecutor.CommandExecutor
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | DocumentStore.DocumentStore
  | QueryExecutor.QueryExecutor
  | ReplicaGate.ReplicaGate
  | ReplicaOperationScheduler.ReplicaOperationScheduler
  | ReplicaHealth.ReplicaHealth
  | ReplicaLimits.ReplicaLimits
  | Crypto.Crypto
> =>
  Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const backups = yield* BackupStore.BackupStore
      const commands = yield* CommandExecutor.CommandExecutor
      const deliveryPublisher = yield* CommandDeliveryPublisher.CommandDeliveryPublisher
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const publisher = yield* CommitPublisher.CommitPublisher
      const documents = yield* DocumentStore.DocumentStore
      const queries = yield* QueryExecutor.QueryExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const health = yield* ReplicaHealth.ReplicaHealth
      const limits = yield* ReplicaLimits.ReplicaLimits
      const crypto = yield* Crypto.Crypto

      const withPermit = <A, E, R,>(f: (permit: ReplicaGate.Permit) => Effect.Effect<A, E, R>) =>
        Effect.scoped(Effect.gen(function*() {
          yield* scheduler.interactive
          const permit = yield* gate.admit
          return yield* f(permit)
        }))

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
              yield* deliveryPublisher.publishPending.pipe(Effect.catchTag("ReplicaError", () => Effect.void))
              return yield* CommandOutcome.committedOrFail(outcome)
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
        inspectConflicts: (document, documentId) =>
          withPermit(() =>
            Effect.acquireUseRelease(
              documents.loadConflictSource(document, documentId),
              (source) =>
                InternalConflicts.inspect(source.stored.automerge, limits).pipe(
                  Effect.map((conflicts) => ({ snapshot: source.stored.snapshot, conflicts }))
                ),
              (source) => Effect.sync(() => InternalAutomerge.free(source.stored.automerge))
            )
          ).pipe(
            Effect.withSpan("SqlReplica.inspectConflicts", {
              attributes: { "document.type": document.name }
            })
          ),
        resolveConflict: (document, options) =>
          withPermit((permit) =>
            Effect.gen(function*() {
              const resolution = yield* InternalConflicts.encodeResolutionCanonical(options.resolution)
              const requestHash = yield* CommandExecutor.resolutionRequestHash({
                incarnation: permit.incarnation,
                commandId: options.commandId,
                document,
                documentId: options.documentId,
                resolution
              }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              const outcome = yield* commands.resolve(document, {
                ...options,
                permit,
                requestHash
              })
              yield* CommandOutcome.committedOrFail(outcome)
              yield* publisher.publishPending.pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CommandOutcomeUnknown({
                      commandId: options.commandId,
                      cause
                    })
                  })
                )
              )
              yield* deliveryPublisher.publishPending.pipe(Effect.catchTag("ReplicaError", () => Effect.void))
              return undefined
            })
          ).pipe(
            Effect.withSpan("SqlReplica.resolveConflict", {
              attributes: {
                "document.type": document.name,
                "conflict.choice": options.resolution.choice._tag,
                "conflict.path_depth": options.resolution.path.parents.length + 1
              }
            })
          ),
        mutate: <M extends Mutation.Any,>(mutation: M, options: {
          readonly commandId: Identity.CommandId
          readonly documentId: Identity.DocumentId
          readonly payload?: M["payloadSchema"]["Type"]
        }) =>
          withPermit((permit) =>
            Effect.gen(function*() {
              const payload = options.payload
              const encoded = yield* Schema.encodeEffect(mutation.payloadSchema)(payload).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.DocumentDecodeError({
                      documentId: options.documentId,
                      cause
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
              yield* deliveryPublisher.publishPending.pipe(Effect.catchTag("ReplicaError", () => Effect.void))
              return yield* CommandOutcome.committedOrFail(outcome)
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
              yield* deliveryPublisher.publishPending.pipe(Effect.catchTag("ReplicaError", () => Effect.void))
              return yield* CommandOutcome.committedOrFail(outcome)
            })
          ),
        query: (query, ...payload) => withPermit(() => queries.execute(query, payload[0])),
        lookupMutation: (mutation, commandId) =>
          withPermit((permit) => commands.lookupMutation(mutation, commandId, permit)),
        lookupCreate: (_document, commandId) => withPermit((permit) => commands.lookupCreate(commandId, permit)),
        lookupDelete: (_document, commandId) => withPermit((permit) => commands.lookupDelete(commandId, permit)),
        lookupConflictResolution: (document, options) =>
          withPermit((permit) => commands.lookupResolution(document, { ...options, permit })).pipe(
            Effect.withSpan("SqlReplica.lookupConflictResolution", {
              attributes: {
                "document.type": document.name,
                "conflict.choice": options.resolution.choice._tag,
                "conflict.path_depth": options.resolution.path.parents.length + 1
              }
            })
          ),
        lookupCommandDelivery: (commandId) => withPermit(() => deliveries.lookup(commandId)),
        commandDeliveryChanges: deliveryPublisher.changes,
        flush: withPermit(() =>
          Effect.all([publisher.drainPending, deliveryPublisher.publishPending], { discard: true })
        ),
        status: health.status,
        // The export does all its database work on the first pull and then serves in-memory chunks,
        // so admission is held for that pull only. Holding it for the stream's lifetime starves the
        // background lane behind the consumer's pace; requiring it again for a pull that does no
        // work lets a full queue discard a completely produced archive.
        exportBackup: (options) =>
          Stream.transformPull(
            backups.export(options),
            (pull) =>
              Effect.sync(() => {
                let produced = false
                return Effect.suspend(() =>
                  (() => {
                    if (produced) return pull
                    return (Effect.scoped(scheduler.interactive.pipe(Effect.andThen(pull))).pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          produced = true
                        })
                      )
                    ))
                  })()
                )
              })
          ),
        restoreBackup: (options) =>
          Effect.scoped(scheduler.interactive.pipe(Effect.andThen(
            Effect.uninterruptibleMask((interruptible) =>
              Effect.gen(function*() {
                const restored = yield* Effect.exit(interruptible(backups.restore(options)))
                const refreshed = yield* Effect.exit(Effect.all([
                  publisher.invalidate(ReplicaDefinition.invalidationKeys(definition)),
                  deliveryPublisher.refresh
                ], { discard: true }))
                return yield* Exit.asVoidAll([restored, refreshed])
              })
            )
          ))),
        installBackupDocument: (document, options) =>
          Effect.scoped(scheduler.interactive.pipe(Effect.andThen(
            Effect.gen(function*() {
              yield* backups.installDocument(document, options)
              yield* publisher.publishPending
            })
          ))),
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
                  cause: NativeError.nativeError("Portable document definition mismatch")
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

export const servicesLayer = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: Options<Bindings>
) => {
  const expected = new Set(definition.projections)
  const actual = new Set(options.projections.map((binding) => binding.projection))
  if (
    options.projections.length !== expected.size ||
    [...expected].some((projection) => !actual.has(projection))
  ) {
    return NativeError.throwTypeError("SqlReplica requires exactly one SQL binding for every projection")
  }
  const bootstrap = ReplicaBootstrap.layer(definition)
  const checkpointAuthority = (() => {
    if (options.checkpointAuthority === undefined) return (CheckpointAuthority.layerRejectAll)
    return (CheckpointAuthority.layer(options.checkpointAuthority))
  })()
  const gate = ReplicaGate.layer.pipe(Layer.provideMerge(bootstrap))
  const scheduler = ReplicaOperationScheduler.layer
  const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
  const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
  const compaction = Compaction.layer.pipe(
    Layer.provideMerge(Layer.merge(recovery, checkpointAuthority))
  )
  const projections = ProjectionStore.layer(options.projections).pipe(Layer.provideMerge(store))
  const evolution = ReplicaEvolution.layer(definition).pipe(Layer.provideMerge(projections))
  const health = ReplicaHealth.layer(definition, options.health ?? ReplicaHealth.defaultOptions).pipe(
    Layer.provideMerge(evolution)
  )
  const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(health))
  const queries = QueryExecutor.layer(definition).pipe(
    Layer.provideMerge(Layer.merge(commands, Reactivity.layer))
  )
  const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
  const deliveryStore = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
  const deliveryPublisher = CommandDeliveryPublisher.layer(
    options.deliveryPublisher ?? CommandDeliveryPublisher.defaultOptions
  ).pipe(
    Layer.provideMerge(Layer.merge(deliveryStore, scheduler))
  )
  const backups = BackupStore.layer(definition).pipe(
    Layer.provideMerge(Layer.mergeAll(publisher, deliveryPublisher, checkpointAuthority))
  )
  // Retain the client so service-level consumers observe the same database instance as the graph.
  const sql = Layer.effect(SqlClient.SqlClient, SqlClient.SqlClient)
  const infrastructure = Layer.mergeAll(
    backups,
    checkpointAuthority,
    compaction,
    deliveryStore,
    deliveryPublisher,
    scheduler,
    sql
  )
  return infrastructure
}

export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: Options<Bindings>
): Layer.Layer<
  | CommitPublisher.CommitPublisher
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | CommandDeliveryStore.CommandDeliveryStore
  | PeerConnectionStatus.PeerConnectionStatus
  | PeerConnectionStatus.Reporter
  | PeerSync.PeerSync
  | RelayConnectionStatus.RelayConnectionStatus
  | Replica.Replica
  | ReplicaEvolution.ReplicaEvolution
  | ReplicaGate.ReplicaGate
  | ReplicaOperationScheduler.ReplicaOperationScheduler
  | ReplicaWorkflow.CompactionWorkflow
  | ReplicaWorkflow.HistoryRewriteWorkflow
  | Sharding.Sharding,
  ConfigError | Migrator.MigrationError | ReplicaError.ReplicaError | SqlError.SqlError,
  | CommandExecutor.MutationHandlers<D>
  | ProjectionStore.BindingServices<Bindings>
  | QueryExecutor.QueryHandlers<D>
  | ReplicaLimits.ReplicaLimits
  | Crypto.Crypto
  | SqlClient.SqlClient
> => {
  const durable = DurableRuntime.layer(definition).pipe(
    Layer.provideMerge(servicesLayer(definition, options))
  )
  return Layer.mergeAll(
    EntityReplica.layer(definition).pipe(Layer.provideMerge(durable)),
    PeerConnectionStatus.layer,
    // A direct replica has no relay, and that is a fact rather than a missing dependency, so it is
    // answered here instead of being pushed onto every consumer. `layerRelay` deliberately does not
    // provide it: a relay replica has to compose `RelayConnectionStatus.layerProtocolSocket` with
    // its socket, and leaving the requirement open is what makes forgetting that a compile error.
    RelayConnectionStatus.layerNotConfigured
  )
}

export const layerRelay = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: Options<Bindings>
): Layer.Layer<
  | CommitPublisher.CommitPublisher
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | CommandDeliveryStore.CommandDeliveryStore
  | PeerConnectionStatus.PeerConnectionStatus
  | PeerConnectionStatus.Reporter
  | PeerSync.PeerSync
  | Replica.Replica
  | ReplicaEvolution.ReplicaEvolution
  | ReplicaGate.ReplicaGate
  | ReplicaOperationScheduler.ReplicaOperationScheduler
  | ReplicaWorkflow.CompactionWorkflow
  | ReplicaWorkflow.HistoryRewriteWorkflow
  | Sharding.Sharding,
  ConfigError | Migrator.MigrationError | ReplicaError.ReplicaError | SqlError.SqlError,
  | CommandExecutor.MutationHandlers<D>
  | ProjectionStore.BindingServices<Bindings>
  | QueryExecutor.QueryHandlers<D>
  | ReplicaLimits.ReplicaLimits
  | PeerRelayReceiptLimits.PeerRelayReceiptLimits
  | Crypto.Crypto
  | SqlClient.SqlClient
> => {
  const durable = DurableRuntime.layerRelay(definition).pipe(
    Layer.provideMerge(servicesLayer(definition, options))
  )
  return Layer.merge(
    EntityReplica.layer(definition).pipe(Layer.provideMerge(durable)),
    PeerConnectionStatus.layer
  )
}

/**
 * Every binding's own Layer, as one Layer.
 *
 * `Layer.empty` is a seed, not decoration: `Layer.mergeAll` takes a non-empty tuple, and a spread of
 * `options.projections` is an array that may be empty.
 */
const bindingLayers = (projections: ReadonlyArray<SqlProjection.Any>) =>
  Layer.mergeAll(Layer.empty, ...projections.map((binding) => binding.layer))

export const servicesLayerWithBindings = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: Options<Bindings>
) => servicesLayer(definition, options).pipe(Layer.provide(bindingLayers(options.projections)))

export const layerWithBindings = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: Options<Bindings>
) => layer(definition, options).pipe(Layer.provide(bindingLayers(options.projections)))

/**
 * Without this a consumer with projections cannot reach the relay stack through the documented
 * constructors at all: `layerWithBindings` wraps `layer`, so every binding-shaped deployment is
 * silently a direct-topology one, and `PeerRelayClientRuntime` then refuses to build on it.
 */
export const layerRelayWithBindings = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: Options<Bindings>
) => layerRelay(definition, options).pipe(Layer.provide(bindingLayers(options.projections)))
