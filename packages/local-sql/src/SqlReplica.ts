import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type { ConfigError } from "effect/Config"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as Migrator from "effect/unstable/sql/Migrator"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as BackupStore from "./BackupStore.js"
import * as CommandDeliveryPublisher from "./CommandDeliveryPublisher.js"
import * as CommandDeliveryStore from "./CommandDeliveryStore.js"
import * as CommandExecutor from "./CommandExecutor.js"
import * as CommitPublisher from "./CommitPublisher.js"
import * as Compaction from "./Compaction.js"
import * as DocumentStore from "./DocumentStore.js"
import * as DurableRuntime from "./DurableRuntime.js"
import * as EntityReplica from "./EntityReplica.js"
import * as InternalAutomerge from "./internal/automerge.js"
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
import type * as ReplicaWorkflow from "./ReplicaWorkflow.js"
import type * as SqlProjection from "./SqlProjection.js"

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
  | ReplicaHealth.ReplicaHealth
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
      const health = yield* ReplicaHealth.ReplicaHealth
      const crypto = yield* Crypto.Crypto

      const withPermit = <A, E, R,>(f: (permit: ReplicaGate.Permit) => Effect.Effect<A, E, R>) =>
        gate.admit.pipe(Effect.flatMap(f), Effect.scoped)

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
        lookupCommandDelivery: (commandId) => deliveries.lookup(commandId),
        commandDeliveryChanges: (commandId) => deliveryPublisher.changes(commandId),
        flush: withPermit(() =>
          Effect.all([publisher.publishPending, deliveryPublisher.publishPending], { discard: true })
        ),
        status: health.status,
        exportBackup: backups.export,
        restoreBackup: (options) =>
          Effect.uninterruptibleMask((interruptible) =>
            Effect.gen(function*() {
              const restored = yield* Effect.exit(interruptible(backups.restore(options)))
              const refreshed = yield* Effect.exit(Effect.all([
                publisher.invalidate(ReplicaDefinition.invalidationKeys(definition)),
                deliveryPublisher.refresh
              ], { discard: true }))
              return yield* Exit.asVoidAll([restored, refreshed])
            })
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
                  cause: new Error("Portable document definition mismatch")
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

const makeBase = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
) => {
  const expected = new Set(definition.projections)
  const actual = new Set(options.projections.map((binding) => binding.projection))
  if (
    options.projections.length !== expected.size ||
    [...expected].some((projection) => !actual.has(projection))
  ) {
    throw new TypeError("SqlReplica requires exactly one SQL binding for every projection")
  }
  const bootstrap = ReplicaBootstrap.layer(definition)
  const gate = ReplicaGate.layer.pipe(Layer.provideMerge(bootstrap))
  const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
  const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
  const compaction = Compaction.layer.pipe(Layer.provideMerge(recovery))
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
  const deliveryPublisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
    Layer.provideMerge(deliveryStore)
  )
  const backups = BackupStore.layer(definition).pipe(
    Layer.provideMerge(Layer.merge(publisher, deliveryPublisher))
  )
  const infrastructure = Layer.mergeAll(
    backups,
    compaction,
    deliveryStore,
    deliveryPublisher
  )
  return { infrastructure, connections: PeerConnectionStatus.layer }
}

export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
): Layer.Layer<
  | CommitPublisher.CommitPublisher
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | PeerConnectionStatus.PeerConnectionStatus
  | PeerConnectionStatus.Reporter
  | PeerSync.PeerSync
  | RelayConnectionStatus.RelayConnectionStatus
  | Replica.Replica
  | ReplicaEvolution.ReplicaEvolution
  | ReplicaGate.ReplicaGate
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
  const { connections, infrastructure } = makeBase(definition, options)
  const durable = DurableRuntime.layer(definition).pipe(
    Layer.provideMerge(infrastructure)
  )
  return Layer.mergeAll(
    EntityReplica.layer(definition).pipe(Layer.provideMerge(durable)),
    connections,
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
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
): Layer.Layer<
  | CommitPublisher.CommitPublisher
  | CommandDeliveryPublisher.CommandDeliveryPublisher
  | PeerConnectionStatus.PeerConnectionStatus
  | PeerConnectionStatus.Reporter
  | PeerSync.PeerSync
  | Replica.Replica
  | ReplicaEvolution.ReplicaEvolution
  | ReplicaGate.ReplicaGate
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
  const { connections, infrastructure } = makeBase(definition, options)
  const durable = DurableRuntime.layerRelay(definition).pipe(
    Layer.provideMerge(infrastructure)
  )
  return Layer.merge(
    EntityReplica.layer(definition).pipe(Layer.provideMerge(durable)),
    connections
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

export const layerWithBindings = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
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
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
) => layerRelay(definition, options).pipe(Layer.provide(bindingLayers(options.projections)))
