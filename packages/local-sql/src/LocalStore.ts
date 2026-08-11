import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ClientLineage from "./internal/clientLineage.js"
import * as Codec from "./internal/codec.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as TerminalRejection from "./internal/TerminalRejection.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"
import * as SchemaEvolution from "./SchemaEvolution.js"

export interface Options {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly scope: Protocol.ReplicationScope
  readonly maximumPendingMutations?: number
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly schemaEvolutionBatchBytes?: number
  readonly retainedReceipts: number
  readonly maximumReceipts: number
  readonly retainedHistoryEntries: number
  readonly maximumBootstrapEntities: number
  readonly maximumBootstrapBytes: number
  readonly maximumBootstrapPageBytes: number
  readonly migration: Migrations.Options
}

export interface ReconciliationGenerations {
  readonly requested: number
  readonly completed: number
}

export interface ReplicationState {
  readonly clientId: Identity.ClientId
  readonly scope: Protocol.ReplicationScope
  readonly scopeGeneration: Identity.ReplicationScopeGeneration
  readonly cursor: Protocol.ReplicationCursor | null
}

export interface Service {
  readonly mutate: <M extends Mutation.Any,>(
    mutation: M,
    payload: Mutation.Payload<M>
  ) => Effect.Effect<Protocol.PendingMutation, ReplicaError.ReplicaError | Mutation.Rejection<M>>
  readonly get: <M extends Model.Any,>(
    model: M,
    key: Model.Key<M>
  ) => Effect.Effect<Option.Option<Model.Value<M>>, ReplicaError.ReplicaError>
  readonly pending: Effect.Effect<ReadonlyArray<Protocol.PendingMutation>, ReplicaError.ReplicaError>
  readonly receipt: (
    mutationId: Identity.MutationId
  ) => Effect.Effect<Option.Option<Protocol.Receipt>, ReplicaError.ReplicaError>
  readonly cursor: Effect.Effect<Identity.ServerSequence, ReplicaError.ReplicaError>
  readonly replicationState: Effect.Effect<ReplicationState, ReplicaError.ReplicaError>
  readonly setScope: (scope: Protocol.ReplicationScope) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly applyViewPage: (page: Protocol.PullPage) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly revokeReplication: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly pendingCount: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly reconciliationGenerations: Effect.Effect<ReconciliationGenerations, ReplicaError.ReplicaError>
  readonly requestReconciliation: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly completeReconciliation: (generation: number) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly applyEntries: (
    entries: ReadonlyArray<Protocol.AcceptedMutation>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly applyReceipts: (
    receipts: ReadonlyArray<Protocol.Receipt>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly applyReceipt: (receipt: Protocol.Receipt) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly persistReceipt: (receipt: Protocol.Receipt) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly settleReceipts: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly prepareBootstrap: (
    manifest: Protocol.SnapshotManifest
  ) => Effect.Effect<number, ReplicaError.ReplicaError>
  readonly stageBootstrapPage: (
    page: Protocol.BootstrapPage
  ) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  readonly installBootstrap: (
    manifest: Protocol.SnapshotManifest
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly invalidateStatus: Effect.Effect<void>
}

export class Store extends Context.Service<Store, Service>()("@lucas-barake/effect-local-sql/LocalStore") {}

export const layer = (
  options: Options
): Layer.Layer<
  Store,
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto | MutationRuntime.MutationRuntime | Reactivity.Reactivity
> =>
  Layer.effect(
    Store,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const crypto = yield* Crypto.Crypto
      const runtime = yield* MutationRuntime.MutationRuntime
      const reactivity = yield* Reactivity.Reactivity
      const maximumPending = options.maximumPendingMutations ?? 10_000
      if (!Number.isSafeInteger(maximumPending) || maximumPending <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumPendingMutations",
          message: "maximumPendingMutations must be a positive safe integer"
        })
      }
      if (!Number.isSafeInteger(options.retainedReceipts) || options.retainedReceipts < 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "retainedReceipts",
          message: "retainedReceipts must be a nonnegative safe integer"
        })
      }
      if (!Number.isSafeInteger(options.maximumReceipts) || options.maximumReceipts <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumReceipts",
          message: "maximumReceipts must be a positive safe integer"
        })
      }
      if (options.retainedReceipts >= options.maximumReceipts) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "retainedReceipts",
          message: "retainedReceipts must be less than maximumReceipts"
        })
      }
      if (options.maximumReceipts < maximumPending) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumReceipts",
          message: "maximumReceipts must be at least maximumPendingMutations"
        })
      }
      if (!Number.isSafeInteger(options.retainedHistoryEntries) || options.retainedHistoryEntries < 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "retainedHistoryEntries",
          message: "retainedHistoryEntries must be a nonnegative safe integer"
        })
      }
      const bootstrapLimits: ReadonlyArray<readonly [string, number]> = [
        ["maximumBootstrapEntities", options.maximumBootstrapEntities],
        ["maximumBootstrapBytes", options.maximumBootstrapBytes],
        ["maximumBootstrapPageBytes", options.maximumBootstrapPageBytes]
      ]
      for (const [option, value] of bootstrapLimits) {
        if (!Number.isSafeInteger(value) || value <= 0) {
          return yield* new ReplicaError.InvalidConfiguration({
            option,
            message: `${option} must be a positive safe integer`
          })
        }
      }
      if (options.maximumBootstrapPageBytes > Protocol.maximumBatchBytes) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumBootstrapPageBytes",
          message: `maximumBootstrapPageBytes must not exceed ${Protocol.maximumBatchBytes}`
        })
      }
      yield* Migrations.client(options)
      const evolution = options.evolution ?? Evolution.make({ current: options.definition })
      let evolutionOptions: SchemaEvolution.ClientOptions = {
        definition: options.definition,
        evolution,
        spaceId: options.spaceId,
        clientId: options.clientId
      }
      if (options.schemaEvolutionBatchSize !== undefined) {
        evolutionOptions = { ...evolutionOptions, batchSize: options.schemaEvolutionBatchSize }
      }
      if (options.schemaEvolutionBatchBytes !== undefined) {
        evolutionOptions = { ...evolutionOptions, batchBytes: options.schemaEvolutionBatchBytes }
      }
      yield* SchemaEvolution.client(evolutionOptions)
      const registerLineage = ClientLineage.make(sql)

      const migrateEntryChanges = (entry: Protocol.AcceptedMutation) =>
        Effect.forEach(entry.changes, (change) => {
          const migration = {
            evolution,
            source: entry.sourceSchema,
            model: change.entity.model,
            modelVersion: change.entity.modelVersion,
            key: change.entity.key
          }
          const finish = (migrated: Evolution.MigratedModel) =>
            Effect.gen(function*() {
              yield* registerLineage(change.entity.model, migrated)
              const entity = {
                model: change.entity.model,
                modelVersion: migrated.modelVersion,
                key: migrated.key
              }
              if (change._tag === "Delete") return Protocol.Delete.make({ entity })
              if (migrated.value === undefined) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Migrated upsert for ${change.entity.model} has no value`
                })
              }
              return Protocol.Upsert.make({ entity, value: migrated.value })
            })
          if (change._tag === "Upsert") {
            return Evolution.migrateModel({ ...migration, value: change.value }).pipe(Effect.flatMap(finish))
          }
          return Evolution.migrateModel(migration).pipe(Effect.flatMap(finish))
        })

      const findMeta = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.ClientMetaRow,
        execute: () =>
          sql`SELECT space_id, client_id, definition_hash, schema_version, schema_hash, schema_generation,
          active_schema_generation,
          target_schema_version, target_schema_hash, migration_hash,
          next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, installed_snapshot_id, installed_snapshot_sequence,
          installed_snapshot_terminal_sequence, replication_view_id, replication_view_revision,
          desired_scope_json, desired_scope_digest, scope_generation
        FROM effect_local_client_meta WHERE singleton = 1`
      })
      const findPending = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.PendingRow,
        execute: () =>
          sql`SELECT mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
        source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json FROM effect_local_pending ORDER BY local_sequence`
      })
      const findPendingByMutation = SqlSchema.findOneOption({
        Request: Identity.MutationId,
        Result: Rows.PendingRow,
        execute: (mutationId) =>
          sql`SELECT mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
        source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json FROM effect_local_pending WHERE mutation_id = ${mutationId}`
      })
      const findReplayPending = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.PendingRow,
        execute: () =>
          sql`SELECT p.mutation_id, p.local_sequence, p.basis, p.name, p.payload_json, p.digest, p.digest_version,
        p.source_schema_version, p.source_schema_hash, p.mutation_version,
        p.optimistic_result_json, p.changes_json FROM effect_local_pending AS p
        WHERE NOT EXISTS (
          SELECT 1 FROM effect_local_server_log AS l WHERE l.mutation_id = p.mutation_id
        )
        ORDER BY p.local_sequence`
      })
      const findReceipt = SqlSchema.findOneOption({
        Request: Identity.MutationId,
        Result: Rows.ReceiptRow,
        execute: (mutationId) => sql`SELECT receipt_json FROM effect_local_receipts WHERE mutation_id = ${mutationId}`
      })
      const findPendingReceipt = SqlSchema.findOneOption({
        Request: Schema.Struct({ after: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
        Result: Rows.PendingReceiptRow,
        execute: ({ after }) =>
          sql`SELECT p.mutation_id, p.local_sequence, p.basis, p.name, p.payload_json, p.digest, p.digest_version,
          p.source_schema_version, p.source_schema_hash, p.mutation_version,
          p.optimistic_result_json, p.changes_json, r.receipt_json,
          l.server_sequence, l.mutation_id AS entry_mutation_id, l.entry_json
        FROM effect_local_pending AS p
        INNER JOIN effect_local_receipts AS r ON r.mutation_id = p.mutation_id
        LEFT JOIN effect_local_server_log AS l ON l.mutation_id = p.mutation_id
        WHERE p.local_sequence > ${after}
        ORDER BY p.local_sequence LIMIT 1`
      })
      const findLogEntry = SqlSchema.findOneOption({
        Request: Identity.ServerSequence,
        Result: Rows.ClientLogRow,
        execute: (sequence) =>
          sql`SELECT server_sequence, mutation_id, entry_json FROM effect_local_server_log
        WHERE server_sequence = ${sequence}`
      })
      const countPending = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_pending`
      })
      const countReceipts = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_receipts`
      })
      const findPrunableReceipts = SqlSchema.findAll({
        Request: Schema.Int,
        Result: Rows.MutationIdRow,
        execute: (limit) =>
          sql`SELECT r.mutation_id FROM effect_local_receipts AS r
          WHERE NOT EXISTS (
            SELECT 1 FROM effect_local_pending AS p WHERE p.mutation_id = r.mutation_id
          )
          ORDER BY r.local_sequence LIMIT ${limit}`
      })
      const findScopedBootstrap = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: Rows.ClientScopedBootstrapRow,
        execute: () =>
          sql`SELECT snapshot_id, space_id, client_id, definition_hash, schema_version, schema_hash,
            scope_digest, scope_generation, view_id, view_revision, server_sequence, terminal_sequence,
            entry_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest
          FROM effect_local_client_scoped_bootstrap WHERE singleton = 1`
      })
      const findScopedBootstrapEntries = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.ScopedSnapshotEntryRow,
        execute: () =>
          sql`SELECT ordinal, change_json, entry_bytes
          FROM effect_local_client_scoped_bootstrap_entries ORDER BY ordinal`
      })
      const findEntityIdentities = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.EntityIdentityRow,
        execute: () =>
          sql`SELECT model, model_version, entity_key FROM effect_local_canonical_entities
          UNION SELECT model, model_version, entity_key FROM effect_local_visible_entities`
      })

      const meta = findMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      const normalizedScope = yield* Protocol.validateReplicationScope(options.definition, options.scope)
      const desiredScopeJson = yield* Codec.stringify(normalizedScope)
      const desiredScopeDigest = Protocol.MutationDigest.make(
        yield* Canonical.digest({ format: 1, scope: normalizedScope }).pipe(
          Effect.provideService(Crypto.Crypto, crypto)
        )
      )
      let initializedMeta = yield* meta
      if (initializedMeta.definition_hash !== options.definition.hash) {
        return yield* new ReplicaError.DefinitionMismatch({
          expected: options.definition.hash,
          actual: initializedMeta.definition_hash
        })
      }
      if (initializedMeta.space_id !== options.spaceId || initializedMeta.client_id !== options.clientId) {
        return yield* new ReplicaError.ReplicaIdentityMismatch({
          expectedSpaceId: options.spaceId,
          actualSpaceId: initializedMeta.space_id,
          expectedClientId: options.clientId,
          actualClientId: initializedMeta.client_id
        })
      }
      if (
        initializedMeta.desired_scope_json !== desiredScopeJson ||
        initializedMeta.desired_scope_digest !== desiredScopeDigest || initializedMeta.scope_generation === 0
      ) {
        yield* sql`UPDATE effect_local_client_meta SET desired_scope_json = ${desiredScopeJson},
          desired_scope_digest = ${desiredScopeDigest}, scope_generation = scope_generation + 1
          WHERE singleton = 1`.pipe(Effect.mapError(StorageUnavailable.make))
        initializedMeta = yield* meta
      }
      const schemaGeneration = initializedMeta.schema_generation
      const activeGeneration = initializedMeta.active_schema_generation
      const validateFence = (current: typeof Rows.ClientMetaRow.Type) => {
        if (current.schema_generation !== schemaGeneration) {
          return Effect.fail(
            new ReplicaError.SchemaGenerationConflict({
              expected: schemaGeneration,
              actual: current.schema_generation
            })
          )
        }
        if (
          current.schema_version !== options.definition.schemaIdentity.version ||
          current.schema_hash !== options.definition.schemaIdentity.hash ||
          current.target_schema_version !== null || current.target_schema_hash !== null ||
          current.migration_hash !== null
        ) {
          return Effect.fail(
            new ReplicaError.StaleSchema({
              expectedVersion: options.definition.schemaIdentity.version,
              expectedHash: options.definition.schemaIdentity.hash,
              actualVersion: current.schema_version,
              actualHash: current.schema_hash
            })
          )
        }
        if (current.definition_hash !== options.definition.hash) {
          return Effect.fail(
            new ReplicaError.DefinitionMismatch({
              expected: options.definition.hash,
              actual: current.definition_hash
            })
          )
        }
        return Effect.void
      }
      if (
        (initializedMeta.installed_snapshot_id === null &&
          (initializedMeta.installed_snapshot_sequence !== 0 ||
            initializedMeta.installed_snapshot_terminal_sequence !== 0)) ||
        initializedMeta.installed_snapshot_sequence > initializedMeta.server_cursor
      ) {
        return yield* new ReplicaError.StorageCorrupt({
          message: "Installed snapshot metadata conflicts with the durable server cursor"
        })
      }

      const decodePendingRow = (row: typeof Rows.PendingRow.Type) =>
        Effect.gen(function*() {
          const payload = yield* Codec.parse(row.payload_json).pipe(
            Effect.flatMap((value) => Codec.decode(Schema.Json, value))
          )
          const optimisticResult = yield* Codec.parse(row.optimistic_result_json)
          const changes = yield* Codec.parse(row.changes_json)
          const identity = {
            spaceId: options.spaceId,
            clientId: options.clientId,
            mutationId: row.mutation_id,
            localSequence: row.local_sequence,
            basis: row.basis,
            name: row.name,
            payload,
            digestVersion: row.digest_version,
            sourceSchema: Identity.SchemaIdentity.make({
              version: row.source_schema_version,
              hash: row.source_schema_hash
            }),
            mutationVersion: row.mutation_version
          }
          const expectedDigest = yield* Protocol.mutationDigest(identity).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
          if (row.digest !== expectedDigest) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Pending mutation ${row.mutation_id} digest does not match its durable identity`
            })
          }
          return yield* Codec.decode(Protocol.PendingMutation, {
            envelope: { ...identity, digest: row.digest },
            optimisticResult,
            changes
          })
        })

      const pending = sql.withTransaction(Effect.gen(function*() {
        yield* validateFence(yield* meta)
        return yield* findPending(undefined).pipe(
          Effect.mapError(StorageUnavailable.make),
          Effect.flatMap(Effect.forEach(decodePendingRow))
        )
      })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const restoreAndReplay = (initialDirty: ReadonlyArray<Protocol.EntityKey>) =>
        Effect.gen(function*() {
          const dirty = new Map(initialDirty.map((entity) => [SqlTransaction.entityKey(entity), entity]))
          const currentPending = yield* pending
          const replayPending = yield* findReplayPending(undefined).pipe(
            Effect.mapError(StorageUnavailable.make),
            Effect.flatMap(Effect.forEach(decodePendingRow))
          )
          for (const item of currentPending) {
            for (const change of item.changes) dirty.set(SqlTransaction.entityKey(change.entity), change.entity)
          }
          for (const entity of dirty.values()) {
            const keyJson = yield* Codec.stringify(entity.key)
            yield* sql`DELETE FROM effect_local_visible_entities WHERE model = ${entity.model} AND entity_key = ${keyJson}`
            yield* sql`INSERT INTO effect_local_client_visible_entities_data
              (generation, model, entity_key, value_json, model_version)
            SELECT ${activeGeneration}, model, entity_key, value_json, model_version
            FROM effect_local_client_canonical_entities_data
            WHERE generation = ${activeGeneration} AND model = ${entity.model} AND entity_key = ${keyJson}
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_client_retractions AS r
                WHERE r.generation = ${activeGeneration} AND r.model = ${entity.model}
                  AND r.entity_key = ${keyJson}
              )`
          }
          for (const item of replayPending) {
            const changes: Array<Protocol.EntityChange> = []
            const result = yield* sql.withTransaction(
              runtime.executeEnvelope(
                item.envelope,
                SqlTransaction.local({
                  sql,
                  definition: options.definition,
                  table: "visible",
                  generation: activeGeneration,
                  changes
                }),
                changes
              ).pipe(
                Effect.flatMap((executionResult) => {
                  if (Result.isFailure(executionResult)) {
                    return Effect.fail(
                      new TerminalRejection.TerminalRejection({
                        origin: "Mutation",
                        rejection: executionResult.failure
                      })
                    )
                  }
                  return Effect.succeed(executionResult.success)
                })
              )
            ).pipe(
              Effect.map(Result.succeed),
              Effect.catchTag("TerminalRejection", ({ rejection }) => Effect.succeed(Result.fail(rejection)))
            )
            if (Result.isSuccess(result)) {
              for (const change of changes) {
                const id = SqlTransaction.entityKey(change.entity)
                if (!dirty.has(id)) dirty.set(id, change.entity)
              }
              yield* sql`UPDATE effect_local_pending
            SET optimistic_result_json = ${yield* Codec.stringify(result.success.result)},
                changes_json = ${yield* Codec.stringify(changes)}
            WHERE mutation_id = ${item.envelope.mutationId}`
            } else {
              yield* sql`UPDATE effect_local_pending SET changes_json = '[]'
            WHERE mutation_id = ${item.envelope.mutationId}`
            }
          }
          yield* sql`DELETE FROM effect_local_visible_entities WHERE EXISTS (
            SELECT 1 FROM effect_local_client_retractions AS r
            WHERE r.generation = ${activeGeneration}
              AND r.model = effect_local_visible_entities.model
              AND r.entity_key = effect_local_visible_entities.entity_key
          )`
        })

      const invalidate = (
        entities: ReadonlyArray<Protocol.EntityKey>,
        receiptIds: ReadonlyArray<Identity.MutationId> = []
      ) => {
        const uniqueEntities = Array.from(
          new Map(entities.map((entity) => [SqlTransaction.entityKey(entity), entity])).values()
        )
        const uniqueReceiptIds = Array.from(new Set(receiptIds))
        let invalidateReceipts = Effect.void
        if (uniqueReceiptIds.length > 0) {
          invalidateReceipts = reactivity.invalidate(
            uniqueReceiptIds.map((mutationId) => `effect-local:receipt:${mutationId}`)
          )
        }
        return reactivity.invalidate({
          "effect-local:entities": uniqueEntities.map((entity) => [entity.model, entity.key]),
          "effect-local:status": []
        }).pipe(Effect.andThen(invalidateReceipts))
      }

      const pruneReceipts = (target: number) =>
        Effect.gen(function*() {
          const count = yield* countReceipts(undefined).pipe(Effect.mapError(StorageUnavailable.make))
          const excess = Math.max(0, count.count - target)
          if (excess === 0) return []
          const rows = yield* findPrunableReceipts(excess).pipe(Effect.mapError(StorageUnavailable.make))
          for (let offset = 0; offset < rows.length; offset += 500) {
            const mutationIds = rows.slice(offset, offset + 500).map((row) => row.mutation_id)
            yield* sql`DELETE FROM effect_local_receipts WHERE mutation_id IN ${sql.in(mutationIds)}`
          }
          return rows.map((row) => row.mutation_id)
        })

      const bootstrapMatches = (
        row: typeof Rows.ClientScopedBootstrapRow.Type,
        manifest: Protocol.SnapshotManifest
      ) =>
        row.snapshot_id === manifest.snapshotId &&
        row.space_id === manifest.spaceId &&
        row.client_id === manifest.clientId &&
        row.definition_hash === manifest.definitionHash &&
        row.schema_version === manifest.schema.version &&
        row.schema_hash === manifest.schema.hash &&
        row.scope_digest === manifest.scopeDigest &&
        row.scope_generation === manifest.scopeGeneration &&
        row.view_id === manifest.cursor.viewId &&
        row.view_revision === manifest.cursor.revision &&
        row.server_sequence === manifest.sequence &&
        row.terminal_sequence === manifest.terminalSequenceThrough &&
        row.entry_count === manifest.entityCount &&
        row.content_bytes === manifest.contentBytes &&
        row.digest === manifest.digest

      const persistReceipt = (receipt: Protocol.Receipt) =>
        Effect.gen(function*() {
          if ((yield* Protocol.encodedBytesEffect(receipt)) > Protocol.maximumReceiptBytes) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Receipt ${receipt.mutationId} exceeds the protocol byte limit`
            })
          }
          let inserted = false
          let pruned: ReadonlyArray<Identity.MutationId> = []
          yield* sql.withTransaction(Effect.gen(function*() {
            yield* validateFence(yield* meta)
            if (receipt.spaceId !== options.spaceId || receipt.clientId !== options.clientId) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: "Receipt identity does not match this replica"
              })
            }
            const storedReceipt = yield* findReceipt(receipt.mutationId).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            if (Option.isSome(storedReceipt)) {
              const decoded = yield* Codec.parse(storedReceipt.value.receipt_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
              )
              if ((yield* Canonical.stringifyEffect(decoded)) !== (yield* Canonical.stringifyEffect(receipt))) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Conflicting duplicate receipt ${receipt.mutationId}`
                })
              }
              return yield* Effect.void
            }
            const storedPending = yield* findPendingByMutation(receipt.mutationId).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            if (Option.isNone(storedPending)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Receipt does not match pending mutation ${receipt.mutationId}`
              })
            }
            const pendingMutation = yield* decodePendingRow(storedPending.value)
            if (pendingMutation.envelope.localSequence !== receipt.localSequence) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Receipt does not match pending mutation ${receipt.mutationId}`
              })
            }
            pruned = yield* pruneReceipts(options.retainedReceipts)
            const receiptCount = yield* countReceipts(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (receiptCount.count >= options.maximumReceipts) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "client receipts",
                limit: options.maximumReceipts
              })
            }
            let receiptSchema = receipt.sourceSchema
            if (receipt._tag === "Expired") receiptSchema = pendingMutation.envelope.sourceSchema
            let receiptMutationVersion: Identity.SchemaVersion | null = null
            if (receipt._tag === "Accepted" || receipt._tag === "Rejected") {
              receiptMutationVersion = receipt.mutationVersion
            } else if (receipt._tag === "Expired") {
              receiptMutationVersion = pendingMutation.envelope.mutationVersion
            }
            let receiptName: string | null = null
            if (receipt._tag === "Accepted" || receipt._tag === "Rejected") {
              receiptName = receipt.name
            } else if (receipt._tag === "Expired") {
              receiptName = pendingMutation.envelope.name
            }
            let rejectionOrigin: string | null = null
            if (receipt._tag === "Rejected") rejectionOrigin = receipt.origin
            else if (receipt._tag === "Legacy") rejectionOrigin = "Legacy"
            yield* sql`INSERT INTO effect_local_client_receipts_data
              (generation, mutation_id, local_sequence, receipt_json, source_schema_version, source_schema_hash,
                mutation_version, mutation_name, rejection_origin)
              VALUES (${activeGeneration}, ${receipt.mutationId}, ${receipt.localSequence},
                ${yield* Codec.stringify(receipt)},
            ${receiptSchema.version}, ${receiptSchema.hash}, ${receiptMutationVersion}, ${receiptName},
            ${rejectionOrigin})`
            inserted = true
            return yield* Effect.void
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          if (inserted || pruned.length > 0) {
            let receiptIds = pruned
            if (inserted) receiptIds = [receipt.mutationId, ...pruned]
            yield* invalidate([], receiptIds)
          }
          return yield* Effect.void
        }).pipe(Effect.withSpan("LocalStore.persistReceipt", {
          attributes: { "mutation.id": receipt.mutationId }
        }))

      const settleCoveredReceipts = (
        installed: typeof Rows.ClientMetaRow.Type,
        touched: Map<string, Protocol.EntityKey>
      ) =>
        Effect.gen(function*() {
          let after = 0
          while (true) {
            const found = yield* findPendingReceipt({ after }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(found)) break
            const row = found.value
            after = row.local_sequence
            const pendingMutation = yield* decodePendingRow(row)
            const receipt = yield* Codec.parse(row.receipt_json).pipe(
              Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
            )
            if (
              receipt.spaceId !== pendingMutation.envelope.spaceId ||
              receipt.clientId !== pendingMutation.envelope.clientId ||
              receipt.mutationId !== pendingMutation.envelope.mutationId ||
              receipt.localSequence !== pendingMutation.envelope.localSequence
            ) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Receipt does not match pending mutation ${receipt.mutationId}`
              })
            }
            if (receipt._tag === "Accepted") {
              if (row.entry_json === null) {
                if (receipt.serverSequence > installed.server_cursor) continue
              } else {
                if (row.server_sequence === null || row.entry_mutation_id === null) {
                  return yield* new ReplicaError.StorageCorrupt({
                    message: `Authoritative entry metadata is incomplete for ${receipt.mutationId}`
                  })
                }
                const entry = yield* Codec.parse(row.entry_json).pipe(
                  Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
                )
                if (
                  row.server_sequence !== entry.sequence ||
                  row.entry_mutation_id !== entry.mutationId ||
                  entry.spaceId !== receipt.spaceId ||
                  entry.clientId !== receipt.clientId ||
                  entry.mutationId !== receipt.mutationId ||
                  entry.localSequence !== receipt.localSequence ||
                  entry.sequence !== receipt.serverSequence ||
                  entry.digest !== pendingMutation.envelope.digest
                ) {
                  return yield* new ReplicaError.ProtocolInvalid({
                    message: `Accepted receipt conflicts with authoritative entry ${receipt.mutationId}`
                  })
                }
              }
            } else if (receipt._tag === "Expired") {
              const coveredByInstalledSnapshot = installed.installed_snapshot_id !== null &&
                receipt.snapshotSequence <= installed.installed_snapshot_sequence &&
                receipt.terminalSequenceThrough <= installed.installed_snapshot_terminal_sequence
              if (!coveredByInstalledSnapshot) continue
            }
            for (const change of pendingMutation.changes) {
              touched.set(SqlTransaction.entityKey(change.entity), change.entity)
            }
            yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${receipt.mutationId}`
          }
          return yield* Effect.void
        })

      const settleReceipts = Effect.gen(function*() {
        const touched = new Map<string, Protocol.EntityKey>()
        let prunedReceiptIds: ReadonlyArray<Identity.MutationId> = []
        yield* sql.withTransaction(Effect.gen(function*() {
          const installed = yield* meta
          yield* validateFence(installed)
          yield* settleCoveredReceipts(installed, touched)
          if (touched.size > 0) {
            yield* restoreAndReplay(Array.from(touched.values()))
            yield* sql`UPDATE effect_local_client_meta
              SET visible_revision = visible_revision + 1 WHERE singleton = 1`
          }
          const current = yield* meta
          const logFloor = Math.max(0, current.server_cursor - options.retainedHistoryEntries)
          yield* sql`DELETE FROM effect_local_server_log
            WHERE server_sequence <= ${logFloor} AND NOT EXISTS (
              SELECT 1 FROM effect_local_pending AS p
              WHERE p.mutation_id = effect_local_server_log.mutation_id
            )`
          prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
          return yield* Effect.void
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (touched.size > 0) yield* invalidate(Array.from(touched.values()))
        if (prunedReceiptIds.length > 0) yield* invalidate([], prunedReceiptIds)
      }).pipe(Effect.withSpan("LocalStore.settleReceipts"))

      const applyReceipts = (receipts: ReadonlyArray<Protocol.Receipt>) =>
        Effect.gen(function*() {
          for (const receipt of receipts) yield* persistReceipt(receipt)
          yield* settleReceipts
        }).pipe(Effect.withSpan("LocalStore.applyReceipts", {
          attributes: { "receipt.count": receipts.length }
        }))

      const replicationState = sql.withTransaction(Effect.gen(function*() {
        const current = yield* meta
        yield* validateFence(current)
        const scope = yield* Codec.parse(current.desired_scope_json).pipe(
          Effect.flatMap((value) => Codec.decode(Protocol.ReplicationScope, value)),
          Effect.flatMap((value) => Protocol.validateReplicationScope(options.definition, value))
        )
        const scopeJson = yield* Codec.stringify(scope)
        const scopeDigest = Protocol.MutationDigest.make(
          yield* Canonical.digest({ format: 1, scope }).pipe(Effect.provideService(Crypto.Crypto, crypto))
        )
        if (scopeJson !== current.desired_scope_json || scopeDigest !== current.desired_scope_digest) {
          return yield* new ReplicaError.StorageCorrupt({
            message: "Durable replication scope does not match its digest"
          })
        }
        let cursor: Protocol.ReplicationCursor | null = null
        if (current.replication_view_id !== null) {
          cursor = Protocol.ReplicationCursor.make({
            viewId: current.replication_view_id,
            revision: current.replication_view_revision
          })
        }
        return {
          clientId: options.clientId,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(current.scope_generation),
          cursor
        }
      })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const setScope = (scope: Protocol.ReplicationScope) =>
        Effect.gen(function*() {
          const normalized = yield* Protocol.validateReplicationScope(options.definition, scope)
          const scopeJson = yield* Codec.stringify(normalized)
          const scopeDigest = Protocol.MutationDigest.make(
            yield* Canonical.digest({ format: 1, scope: normalized }).pipe(
              Effect.provideService(Crypto.Crypto, crypto)
            )
          )
          yield* sql.withTransaction(Effect.gen(function*() {
            const current = yield* meta
            yield* validateFence(current)
            if (
              current.desired_scope_json === scopeJson &&
              current.desired_scope_digest === scopeDigest
            ) return yield* Effect.void
            yield* sql`UPDATE effect_local_client_meta SET
              desired_scope_json = ${scopeJson}, desired_scope_digest = ${scopeDigest},
              scope_generation = scope_generation + 1,
              requested_generation = requested_generation + 1
              WHERE singleton = 1`
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap_entries`
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap WHERE singleton = 1`
            return yield* Effect.void
          }))
        }).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.setScope")
        )

      const validateManifest = (manifest: Protocol.SnapshotManifest) =>
        Effect.gen(function*() {
          if (manifest.spaceId !== options.spaceId || manifest.clientId !== options.clientId) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: "Snapshot replica identity does not match this replica"
            })
          }
          if (manifest.definitionHash !== options.definition.hash) {
            return yield* new ReplicaError.DefinitionMismatch({
              expected: options.definition.hash,
              actual: manifest.definitionHash
            })
          }
          if (
            manifest.schema.version !== options.definition.schemaIdentity.version ||
            manifest.schema.hash !== options.definition.schemaIdentity.hash
          ) {
            return yield* new ReplicaError.StaleSchema({
              expectedVersion: options.definition.schemaIdentity.version,
              expectedHash: options.definition.schemaIdentity.hash,
              actualVersion: manifest.schema.version,
              actualHash: manifest.schema.hash
            })
          }
          const current = yield* meta
          yield* validateFence(current)
          if (
            manifest.scopeGeneration !== current.scope_generation ||
            manifest.scopeDigest !== current.desired_scope_digest
          ) {
            return yield* new ReplicaError.StaleReplicationScope({
              expected: current.scope_generation,
              actual: manifest.scopeGeneration
            })
          }
          if (manifest.entityCount > options.maximumBootstrapEntities) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "bootstrap entries",
              limit: options.maximumBootstrapEntities
            })
          }
          if (manifest.contentBytes > options.maximumBootstrapBytes) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "bootstrap bytes",
              limit: options.maximumBootstrapBytes
            })
          }
          return yield* Effect.void
        })

      const validateViewChange = (change: Protocol.ViewChange, source: string) =>
        Effect.gen(function*() {
          const model = options.definition.modelByName.get(change.entity.model)
          if (model === undefined) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `${source} contains unknown model ${change.entity.model}`
            })
          }
          if (change.entity.modelVersion !== model.version) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `${source} contains the wrong model version for ${change.entity.model}`
            })
          }
          const key = yield* Codec.decode(model.key, change.entity.key).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ProtocolInvalid({ message: `${source} contains an invalid entity key`, cause })
            )
          )
          let valueJson: string | null = null
          if (change._tag === "Upsert") {
            const value = yield* Codec.decode(model.schema, change.value).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ProtocolInvalid({ message: `${source} contains an invalid entity value`, cause })
              )
            )
            valueJson = yield* Codec.stringify(value)
          }
          return { keyJson: yield* Codec.stringify(key), valueJson }
        })

      const applyViewChange = (change: Protocol.ViewChange, keyJson: string, valueJson: string | null) =>
        Effect.gen(function*() {
          if (change._tag === "Upsert") {
            if (valueJson === null) {
              return yield* new ReplicaError.StorageCorrupt({ message: "Validated upsert lost its value" })
            }
            yield* sql`DELETE FROM effect_local_client_retractions
              WHERE generation = ${activeGeneration} AND model = ${change.entity.model}
                AND entity_key = ${keyJson}`
            yield* sql`INSERT INTO effect_local_client_canonical_entities_data
              (generation, model, entity_key, value_json, model_version)
              VALUES (${activeGeneration}, ${change.entity.model}, ${keyJson}, ${valueJson},
                ${change.entity.modelVersion})
              ON CONFLICT (generation, model, entity_key) DO UPDATE SET
                value_json = excluded.value_json, model_version = excluded.model_version`
            return yield* Effect.void
          }
          yield* sql`DELETE FROM effect_local_client_canonical_entities_data
            WHERE generation = ${activeGeneration} AND model = ${change.entity.model}
              AND entity_key = ${keyJson}`
          if (change._tag === "Delete") {
            yield* sql`DELETE FROM effect_local_client_retractions
              WHERE generation = ${activeGeneration} AND model = ${change.entity.model}
                AND entity_key = ${keyJson}`
            return yield* Effect.void
          }
          yield* sql`INSERT INTO effect_local_client_retractions
            (generation, model, model_version, entity_key)
            VALUES (${activeGeneration}, ${change.entity.model}, ${change.entity.modelVersion}, ${keyJson})
            ON CONFLICT (generation, model, entity_key) DO UPDATE SET
              model_version = excluded.model_version`
          return yield* Effect.void
        })

      const prepareBootstrap = (manifest: Protocol.SnapshotManifest) =>
        Effect.gen(function*() {
          yield* validateManifest(manifest)
          return yield* sql.withTransaction(Effect.gen(function*() {
            const current = yield* findScopedBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(current) && bootstrapMatches(current.value, manifest)) {
              return current.value.next_ordinal - 1
            }
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap_entries`
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap WHERE singleton = 1`
            yield* sql`INSERT INTO effect_local_client_scoped_bootstrap
              (singleton, snapshot_id, space_id, client_id, definition_hash, schema_version, schema_hash,
                scope_digest, scope_generation, view_id, view_revision, server_sequence, terminal_sequence,
                entry_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest)
              VALUES (1, ${manifest.snapshotId}, ${manifest.spaceId}, ${manifest.clientId},
                ${manifest.definitionHash}, ${manifest.schema.version}, ${manifest.schema.hash},
                ${manifest.scopeDigest}, ${manifest.scopeGeneration}, ${manifest.cursor.viewId},
                ${manifest.cursor.revision}, ${manifest.sequence}, ${manifest.terminalSequenceThrough},
                ${manifest.entityCount}, ${manifest.contentBytes}, ${manifest.digest}, 0, 0,
                ${Protocol.initialSnapshotDigest})`
            return -1
          }))
        }).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.prepareBootstrap", { attributes: { "snapshot.id": manifest.snapshotId } })
        )

      const stageBootstrapPage = (page: Protocol.BootstrapPage) =>
        Effect.gen(function*() {
          yield* validateManifest(page.manifest)
          if ((yield* Protocol.encodedBytesEffect(page)) > options.maximumBootstrapPageBytes) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "bootstrap page bytes",
              limit: options.maximumBootstrapPageBytes
            })
          }
          return yield* sql.withTransaction(Effect.gen(function*() {
            const found = yield* findScopedBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(found) || !bootstrapMatches(found.value, page.manifest)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Bootstrap page does not match durable snapshot ${page.manifest.snapshotId}`
              })
            }
            const stage = found.value
            let nextOrdinal = stage.next_ordinal
            let receivedBytes = stage.received_bytes
            let rollingDigest = stage.rolling_digest
            const staged = yield* findScopedBootstrapEntries(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            const identities = new Set<string>()
            for (const row of staged) {
              const entry = yield* Codec.parse(row.change_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntry, value))
              )
              const { keyJson } = yield* validateViewChange(entry.change, `Snapshot ${page.manifest.snapshotId}`)
              identities.add(`${entry.change.entity.model}\\u0000${keyJson}`)
            }
            for (const entry of page.entries) {
              if (entry.ordinal !== nextOrdinal) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message:
                    `Snapshot ${page.manifest.snapshotId} expected ordinal ${nextOrdinal} but found ${entry.ordinal}`
                })
              }
              const validated = yield* validateViewChange(entry.change, `Snapshot ${page.manifest.snapshotId}`)
              const entryBytes = yield* Protocol.encodedBytesEffect(entry.change)
              if (entry.entryBytes !== entryBytes) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} entry ${entry.ordinal} has invalid byte metadata`
                })
              }
              const identity = `${entry.change.entity.model}\\u0000${validated.keyJson}`
              if (identities.has(identity)) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} contains a duplicate entity`
                })
              }
              identities.add(identity)
              receivedBytes += entryBytes
              if (receivedBytes > page.manifest.contentBytes) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} exceeds its declared bytes`
                })
              }
              rollingDigest = Protocol.SnapshotDigest.make(
                yield* Canonical.digest({ previous: rollingDigest, entry }).pipe(
                  Effect.provideService(Crypto.Crypto, crypto)
                )
              )
              yield* sql`INSERT INTO effect_local_client_scoped_bootstrap_entries
                (snapshot_id, ordinal, change_json, entry_bytes)
                VALUES (${page.manifest.snapshotId}, ${entry.ordinal},
                  ${yield* Codec.stringify(entry)}, ${entry.entryBytes})`
              nextOrdinal += 1
            }
            if (nextOrdinal > page.manifest.entityCount) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} exceeds its declared entry count`
              })
            }
            const complete = nextOrdinal === page.manifest.entityCount
            if (!complete && page.entries.length === 0) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} page made no progress`
              })
            }
            if (page.hasMore === complete) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} continuation disagrees with its entry count`
              })
            }
            if (
              complete &&
              (receivedBytes !== page.manifest.contentBytes || rollingDigest !== page.manifest.digest)
            ) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} does not match its manifest`
              })
            }
            yield* sql`UPDATE effect_local_client_scoped_bootstrap SET
              next_ordinal = ${nextOrdinal}, received_bytes = ${receivedBytes},
              rolling_digest = ${rollingDigest} WHERE singleton = 1`
            return complete
          }))
        }).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.stageBootstrapPage", {
            attributes: { "snapshot.id": page.manifest.snapshotId, "entry.count": page.entries.length }
          })
        )

      const installBootstrap = (manifest: Protocol.SnapshotManifest) =>
        Effect.gen(function*() {
          yield* validateManifest(manifest)
          const dirty = new Map<string, Protocol.EntityKey>()
          let prunedReceiptIds: ReadonlyArray<Identity.MutationId> = []
          yield* sql.withTransaction(Effect.gen(function*() {
            const found = yield* findScopedBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(found) || !bootstrapMatches(found.value, manifest)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${manifest.snapshotId} is not staged for installation`
              })
            }
            const stage = found.value
            if (
              stage.next_ordinal !== manifest.entityCount ||
              stage.received_bytes !== manifest.contentBytes ||
              stage.rolling_digest !== manifest.digest
            ) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${manifest.snapshotId} is incomplete`
              })
            }
            const rows = yield* findScopedBootstrapEntries(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (rows.length !== manifest.entityCount) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} durable entry count is incomplete`
              })
            }
            let receivedBytes = 0
            let rollingDigest = Protocol.initialSnapshotDigest
            const entries: Array<{
              readonly entry: Protocol.SnapshotEntry
              readonly keyJson: string
              readonly valueJson: string | null
            }> = []
            const stagedIdentities = new Set<string>()
            for (let index = 0; index < rows.length; index++) {
              const row = rows[index]
              const entry = yield* Codec.parse(row.change_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntry, value))
              )
              if (row.ordinal !== index || entry.ordinal !== index || row.entry_bytes !== entry.entryBytes) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} durable entry metadata is inconsistent`
                })
              }
              const validated = yield* validateViewChange(entry.change, `Snapshot ${manifest.snapshotId}`)
              const entryBytes = yield* Protocol.encodedBytesEffect(entry.change)
              if (entry.entryBytes !== entryBytes) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} durable entry ${index} has invalid byte metadata`
                })
              }
              const identity = `${entry.change.entity.model}\\u0000${validated.keyJson}`
              if (stagedIdentities.has(identity)) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} contains a duplicate durable entity`
                })
              }
              stagedIdentities.add(identity)
              receivedBytes += entryBytes
              rollingDigest = Protocol.SnapshotDigest.make(
                yield* Canonical.digest({ previous: rollingDigest, entry }).pipe(
                  Effect.provideService(Crypto.Crypto, crypto)
                )
              )
              entries.push({ entry, ...validated })
            }
            if (receivedBytes !== manifest.contentBytes || rollingDigest !== manifest.digest) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} durable entries do not match its manifest`
              })
            }
            const current = yield* meta
            if (manifest.sequence < current.server_cursor) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${manifest.snapshotId} is older than the local server watermark`
              })
            }
            const prior = yield* findEntityIdentities(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            for (const row of prior) {
              const entity = yield* Codec.decode(Protocol.EntityKey, {
                model: row.model,
                modelVersion: row.model_version,
                key: yield* Codec.parse(row.entity_key)
              })
              dirty.set(SqlTransaction.entityKey(entity), entity)
              const identity = `${row.model}\\u0000${row.entity_key}`
              if (!stagedIdentities.has(identity)) {
                yield* sql`INSERT INTO effect_local_client_retractions
                  (generation, model, model_version, entity_key)
                  VALUES (${activeGeneration}, ${row.model}, ${row.model_version}, ${row.entity_key})
                  ON CONFLICT (generation, model, entity_key) DO UPDATE SET
                    model_version = excluded.model_version`
              }
            }

            let after = 0
            while (true) {
              const pendingReceipt = yield* findPendingReceipt({ after }).pipe(Effect.mapError(StorageUnavailable.make))
              if (Option.isNone(pendingReceipt)) break
              const row = pendingReceipt.value
              after = row.local_sequence
              const receipt = yield* Codec.parse(row.receipt_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
              )
              const pendingMutation = yield* decodePendingRow(row)
              if (
                receipt.spaceId !== pendingMutation.envelope.spaceId ||
                receipt.clientId !== pendingMutation.envelope.clientId ||
                receipt.mutationId !== pendingMutation.envelope.mutationId ||
                receipt.localSequence !== pendingMutation.envelope.localSequence
              ) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Receipt does not match durable pending mutation ${row.mutation_id}`
                })
              }
              let covered = true
              if (receipt._tag === "Accepted") {
                covered = receipt.serverSequence <= manifest.sequence &&
                  (receipt.terminalSequence === undefined ||
                    receipt.terminalSequence <= manifest.terminalSequenceThrough)
              } else if (receipt._tag === "Rejected") {
                covered = receipt.terminalSequence !== undefined &&
                  receipt.terminalSequence <= manifest.terminalSequenceThrough
              } else if (receipt._tag === "Expired") {
                covered = receipt.snapshotSequence <= manifest.sequence &&
                  receipt.terminalSequenceThrough <= manifest.terminalSequenceThrough
              }
              if (covered) yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${row.mutation_id}`
            }

            yield* sql`DELETE FROM effect_local_canonical_entities`
            for (const item of entries) {
              dirty.set(SqlTransaction.entityKey(item.entry.change.entity), item.entry.change.entity)
              yield* applyViewChange(item.entry.change, item.keyJson, item.valueJson)
            }
            yield* sql`DELETE FROM effect_local_server_log WHERE server_sequence <= ${manifest.sequence}`
            yield* sql`DELETE FROM effect_local_visible_entities`
            yield* sql`INSERT INTO effect_local_client_visible_entities_data
              (generation, model, entity_key, value_json, model_version)
              SELECT ${activeGeneration}, model, entity_key, value_json, model_version
              FROM effect_local_client_canonical_entities_data
              WHERE generation = ${activeGeneration} AND NOT EXISTS (
                SELECT 1 FROM effect_local_client_retractions AS r
                WHERE r.generation = ${activeGeneration}
                  AND r.model = effect_local_client_canonical_entities_data.model
                  AND r.entity_key = effect_local_client_canonical_entities_data.entity_key
              )`
            yield* restoreAndReplay([])
            yield* sql`UPDATE effect_local_client_meta SET
              server_cursor = ${manifest.sequence},
              replication_view_id = ${manifest.cursor.viewId},
              replication_view_revision = ${manifest.cursor.revision},
              installed_snapshot_id = ${manifest.snapshotId},
              installed_snapshot_sequence = ${manifest.sequence},
              installed_snapshot_terminal_sequence = ${manifest.terminalSequenceThrough},
              visible_revision = visible_revision + 1
              WHERE singleton = 1`
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap_entries`
            yield* sql`DELETE FROM effect_local_client_scoped_bootstrap WHERE singleton = 1`
            prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
            return yield* Effect.void
          }))
          yield* invalidate(Array.from(dirty.values()), prunedReceiptIds)
        }).pipe(
          Effect.uninterruptible,
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.installBootstrap", {
            attributes: { "snapshot.id": manifest.snapshotId, "server.sequence": manifest.sequence }
          })
        )

      const applyViewPage = (page: Protocol.PullPage) =>
        Effect.gen(function*() {
          const touched = new Map<string, Protocol.EntityKey>()
          yield* sql.withTransaction(Effect.gen(function*() {
            const current = yield* meta
            yield* validateFence(current)
            if (page.scopeGeneration !== current.scope_generation) {
              return yield* new ReplicaError.StaleReplicationScope({
                expected: current.scope_generation,
                actual: page.scopeGeneration
              })
            }
            if (
              current.replication_view_id === null ||
              current.replication_view_id !== page.cursor.viewId ||
              page.cursor.revision !== current.replication_view_revision + 1
            ) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: "Replication page cursor does not follow the durable view cursor"
              })
            }
            const contentBytes = yield* Protocol.encodedBytesEffect(page.changes)
            const pageDigest = Protocol.MutationDigest.make(
              yield* Canonical.digest({ format: 1, changes: page.changes }).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              )
            )
            if (contentBytes !== page.contentBytes || pageDigest !== page.digest) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: "Replication page does not match its byte metadata or digest"
              })
            }
            for (const change of page.changes) {
              const validated = yield* validateViewChange(change, "Replication page")
              yield* applyViewChange(change, validated.keyJson, validated.valueJson)
              touched.set(SqlTransaction.entityKey(change.entity), change.entity)
            }
            if (page.hasMore) {
              yield* sql`UPDATE effect_local_client_meta SET
                replication_view_revision = ${page.cursor.revision},
                visible_revision = visible_revision + 1 WHERE singleton = 1`
            } else {
              yield* sql`UPDATE effect_local_client_meta SET
                replication_view_revision = ${page.cursor.revision},
                server_cursor = ${page.serverSequence}, visible_revision = visible_revision + 1
                WHERE singleton = 1`
            }
            if (!page.hasMore) yield* settleCoveredReceipts(yield* meta, touched)
            if (touched.size > 0) yield* restoreAndReplay(Array.from(touched.values()))
            return yield* Effect.void
          }))
          yield* invalidate(Array.from(touched.values()))
        }).pipe(
          Effect.uninterruptible,
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.applyViewPage", {
            attributes: { "change.count": page.changes.length, "server.sequence": page.serverSequence }
          })
        )

      const revokeReplication = Effect.gen(function*() {
        const dirty = new Map<string, Protocol.EntityKey>()
        yield* sql.withTransaction(Effect.gen(function*() {
          const current = yield* meta
          yield* validateFence(current)
          const identities = yield* findEntityIdentities(undefined).pipe(Effect.mapError(StorageUnavailable.make))
          for (const row of identities) {
            const entity = yield* Codec.decode(Protocol.EntityKey, {
              model: row.model,
              modelVersion: row.model_version,
              key: yield* Codec.parse(row.entity_key)
            })
            dirty.set(SqlTransaction.entityKey(entity), entity)
            yield* sql`INSERT INTO effect_local_client_retractions
              (generation, model, model_version, entity_key)
              VALUES (${activeGeneration}, ${row.model}, ${row.model_version}, ${row.entity_key})
              ON CONFLICT (generation, model, entity_key) DO UPDATE SET
                model_version = excluded.model_version`
          }
          yield* sql`DELETE FROM effect_local_canonical_entities`
          yield* sql`DELETE FROM effect_local_visible_entities`
          yield* sql`DELETE FROM effect_local_client_scoped_bootstrap_entries`
          yield* sql`DELETE FROM effect_local_client_scoped_bootstrap WHERE singleton = 1`
          yield* sql`UPDATE effect_local_client_meta SET
            replication_view_id = NULL, replication_view_revision = 0,
            visible_revision = visible_revision + 1 WHERE singleton = 1`
        }))
        yield* invalidate(Array.from(dirty.values()))
      }).pipe(
        Effect.uninterruptible,
        Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
        Effect.withSpan("LocalStore.revokeReplication")
      )

      const reconciliationGenerations = sql.withTransaction(Effect.gen(function*() {
        const row = yield* meta
        yield* validateFence(row)
        return { requested: row.requested_generation, completed: row.completed_generation }
      })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const nextReconciliationGeneration = (requested: number) =>
        Effect.gen(function*() {
          if (requested >= Number.MAX_SAFE_INTEGER) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "reconciliation generations",
              limit: Number.MAX_SAFE_INTEGER
            })
          }
          return requested + 1
        })

      const requestReconciliation = sql.withTransaction(
        Effect.gen(function*() {
          const current = yield* meta
          yield* validateFence(current)
          const requested = yield* nextReconciliationGeneration(current.requested_generation)
          yield* sql`UPDATE effect_local_client_meta
          SET requested_generation = ${requested}
          WHERE singleton = 1`
          return requested
        })
      ).pipe(
        Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
        Effect.withSpan("LocalStore.requestReconciliation")
      )

      const completeReconciliation = (generation: number) =>
        Effect.gen(function*() {
          if (!Number.isSafeInteger(generation) || generation < 0) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: "Reconciliation generation must be a nonnegative safe integer"
            })
          }
          yield* sql.withTransaction(Effect.gen(function*() {
            const current = yield* meta
            yield* validateFence(current)
            if (generation > current.requested_generation) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Reconciliation generation ${generation} was not requested`
              })
            }
            yield* sql`UPDATE effect_local_client_meta
            SET completed_generation = MAX(completed_generation, ${generation})
            WHERE singleton = 1`
            return yield* Effect.void
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          return yield* Effect.void
        }).pipe(Effect.withSpan("LocalStore.completeReconciliation", {
          attributes: { "reconciliation.generation": generation }
        }))

      const service: Service = {
        mutate: (mutation, payloadValue) =>
          Effect.gen(function*() {
            const result = yield* sql.withTransaction(Effect.gen(function*() {
              const storedMeta = yield* meta
              yield* validateFence(storedMeta)
              yield* nextReconciliationGeneration(storedMeta.requested_generation)
              if (storedMeta.next_local_sequence >= Number.MAX_SAFE_INTEGER) {
                return yield* new ReplicaError.CapacityExceeded({
                  resource: "local sequence",
                  limit: Number.MAX_SAFE_INTEGER - 1
                })
              }
              const count = yield* countPending(undefined).pipe(Effect.mapError(StorageUnavailable.make))
              if (count.count >= maximumPending) {
                return yield* new ReplicaError.CapacityExceeded({
                  resource: "pending mutations",
                  limit: maximumPending
                })
              }
              const mutationId = yield* Identity.makeMutationId.pipe(Effect.provideService(Crypto.Crypto, crypto))
              const encodedPayload = yield* Codec.encode(mutation.payloadSchema, payloadValue)
              const payloadJsonValue = yield* Schema.decodeUnknownEffect(Schema.Json)(encodedPayload).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.StorageCorrupt({ message: "Mutation payload is not JSON", cause })
                )
              )
              const identity = {
                spaceId: options.spaceId,
                clientId: options.clientId,
                mutationId,
                localSequence: storedMeta.next_local_sequence,
                basis: storedMeta.server_cursor,
                name: mutation.name,
                payload: payloadJsonValue,
                digestVersion: 2 as const,
                sourceSchema: options.definition.schemaIdentity,
                mutationVersion: mutation.version
              }
              const digest = yield* Protocol.mutationDigest(identity).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              )
              const envelope = yield* Codec.decode(Protocol.MutationEnvelope, { ...identity, digest })
              if ((yield* Protocol.encodedBytesEffect(envelope)) > Protocol.maximumMutationBytes) {
                return yield* new ReplicaError.CapacityExceeded({
                  resource: "mutation bytes",
                  limit: Protocol.maximumMutationBytes
                })
              }
              const changes: Array<Protocol.EntityChange> = []
              const executed = yield* runtime.execute(
                mutation.name,
                payloadJsonValue,
                SqlTransaction.local({
                  sql,
                  definition: options.definition,
                  table: "visible",
                  generation: activeGeneration,
                  changes
                }),
                changes
              )
              if (Result.isFailure(executed)) {
                const rejection = yield* Codec.decode(mutation.rejectionSchema, executed.failure)
                return yield* Effect.fail(rejection)
              }
              const pendingMutation: Protocol.PendingMutation = {
                envelope,
                optimisticResult: executed.success.result,
                changes
              }
              yield* sql`INSERT INTO effect_local_client_pending_data
            (generation, mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
              source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json)
            VALUES (${activeGeneration}, ${mutationId}, ${envelope.localSequence}, ${envelope.basis}, ${envelope.name},
              ${yield* Codec.stringify(envelope.payload)}, ${digest}, ${envelope.digestVersion},
              ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash}, ${envelope.mutationVersion}, ${yield* Codec
                .stringify(
                  executed.success.result
                )},
              ${yield* Codec.stringify(changes)})`
              yield* sql`UPDATE effect_local_client_meta
            SET next_local_sequence = next_local_sequence + 1,
                visible_revision = visible_revision + 1,
                requested_generation = requested_generation + 1
            WHERE singleton = 1`
              return pendingMutation
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            yield* invalidate(result.changes.map((change) => change.entity))
            return result
          }).pipe(Effect.withSpan("LocalStore.mutate", {
            attributes: {
              "mutation.name": mutation.name,
              "space.id": options.spaceId,
              "client.id": options.clientId
            }
          })),
        get: (model, key) =>
          sql.withTransaction(Effect.gen(function*() {
            yield* validateFence(yield* meta)
            return yield* SqlTransaction.local({
              sql,
              definition: options.definition,
              table: "visible",
              generation: activeGeneration
            }).get(
              model,
              key
            )
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
        pending,
        receipt: (mutationId) =>
          sql.withTransaction(Effect.gen(function*() {
            yield* validateFence(yield* meta)
            const row = yield* findReceipt(mutationId).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(row)) return Option.none()
            return Option.some(
              yield* Codec.parse(row.value.receipt_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
              )
            )
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
        cursor: sql.withTransaction(Effect.gen(function*() {
          const row = yield* meta
          yield* validateFence(row)
          return Identity.ServerSequence.make(row.server_cursor)
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
        replicationState,
        setScope,
        applyViewPage,
        revokeReplication,
        pendingCount: sql.withTransaction(Effect.gen(function*() {
          yield* validateFence(yield* meta)
          return (yield* countPending(undefined).pipe(Effect.mapError(StorageUnavailable.make))).count
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
        reconciliationGenerations,
        requestReconciliation,
        completeReconciliation,
        applyEntries: (entries) =>
          Effect.gen(function*() {
            if (entries.length === 0) return yield* Effect.void
            const touched: Array<Protocol.EntityKey> = []
            const settledReceiptIds: Array<Identity.MutationId> = []
            let prunedReceiptIds: ReadonlyArray<Identity.MutationId> = []
            yield* sql.withTransaction(Effect.gen(function*() {
              const currentMeta = yield* meta
              yield* validateFence(currentMeta)
              let cursor = currentMeta.server_cursor
              for (const entry of entries) {
                if (entry.spaceId !== options.spaceId) {
                  return yield* new ReplicaError.ProtocolInvalid({
                    message: `Entry space ${entry.spaceId} does not match replica space ${options.spaceId}`
                  })
                }
                if (entry.sequence <= cursor) {
                  const stored = yield* findLogEntry(entry.sequence).pipe(Effect.mapError(StorageUnavailable.make))
                  if (Option.isNone(stored)) {
                    return yield* new ReplicaError.StorageCorrupt({
                      message: `Server cursor covers missing log entry ${entry.sequence}`
                    })
                  }
                  const decoded = yield* Codec.parse(stored.value.entry_json).pipe(
                    Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
                  )
                  if (
                    stored.value.server_sequence !== decoded.sequence ||
                    stored.value.mutation_id !== decoded.mutationId ||
                    (yield* Canonical.stringifyEffect(decoded)) !== (yield* Canonical.stringifyEffect(entry))
                  ) {
                    return yield* new ReplicaError.ProtocolInvalid({
                      message: `Conflicting duplicate server entry ${entry.sequence}`
                    })
                  }
                  continue
                }
                const expected = cursor + 1
                if (entry.sequence !== expected) {
                  return yield* new ReplicaError.CursorGap({ expected, actual: entry.sequence })
                }
                yield* sql`INSERT INTO effect_local_server_log
              (server_sequence, mutation_id, entry_json, source_schema_version, source_schema_hash)
              VALUES (${entry.sequence}, ${entry.mutationId}, ${yield* Codec.stringify(entry)},
                ${entry.sourceSchema.version}, ${entry.sourceSchema.hash})`
                const currentChanges = yield* migrateEntryChanges(entry)
                for (const change of currentChanges) {
                  touched.push(change.entity)
                  yield* SqlTransaction.applyLocalChange(sql, "canonical", activeGeneration, change)
                }
                if (entry.clientId === options.clientId) {
                  const storedPending = yield* findPendingByMutation(entry.mutationId).pipe(
                    Effect.mapError(StorageUnavailable.make)
                  )
                  if (Option.isSome(storedPending)) {
                    const decodedPending = yield* decodePendingRow(storedPending.value)
                    if (
                      decodedPending.envelope.spaceId !== entry.spaceId ||
                      decodedPending.envelope.clientId !== entry.clientId ||
                      decodedPending.envelope.mutationId !== entry.mutationId ||
                      decodedPending.envelope.localSequence !== entry.localSequence ||
                      decodedPending.envelope.digest !== entry.digest
                    ) {
                      return yield* new ReplicaError.ProtocolInvalid({
                        message: `Accepted entry conflicts with pending mutation ${entry.mutationId}`
                      })
                    }
                  }
                  const storedReceipt = yield* findReceipt(entry.mutationId).pipe(
                    Effect.mapError(StorageUnavailable.make)
                  )
                  if (Option.isSome(storedReceipt)) {
                    const receipt = yield* Codec.parse(storedReceipt.value.receipt_json).pipe(
                      Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
                    )
                    if (
                      receipt._tag !== "Accepted" ||
                      receipt.spaceId !== entry.spaceId ||
                      receipt.clientId !== entry.clientId ||
                      receipt.mutationId !== entry.mutationId ||
                      receipt.localSequence !== entry.localSequence ||
                      receipt.serverSequence !== entry.sequence
                    ) {
                      return yield* new ReplicaError.ProtocolInvalid({
                        message: `Accepted entry conflicts with receipt ${entry.mutationId}`
                      })
                    }
                    if (Option.isSome(storedPending)) {
                      touched.push(
                        ...(yield* decodePendingRow(storedPending.value)).changes.map((change) => change.entity)
                      )
                      yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${entry.mutationId}`
                    }
                    settledReceiptIds.push(entry.mutationId)
                  }
                }
                cursor = entry.sequence
              }
              yield* restoreAndReplay(touched)
              yield* sql`UPDATE effect_local_client_meta
            SET server_cursor = ${cursor}, visible_revision = visible_revision + 1 WHERE singleton = 1`
              const logFloor = Math.max(0, cursor - options.retainedHistoryEntries)
              yield* sql`DELETE FROM effect_local_server_log
                WHERE server_sequence <= ${logFloor} AND NOT EXISTS (
                  SELECT 1 FROM effect_local_pending AS p
                  WHERE p.mutation_id = effect_local_server_log.mutation_id
                )`
              prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
              return yield* Effect.void
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            yield* invalidate(
              touched,
              [...settledReceiptIds, ...prunedReceiptIds]
            )
            return undefined
          }).pipe(Effect.withSpan("LocalStore.applyEntries", {
            attributes: { "entry.count": entries.length, "space.id": options.spaceId }
          })),
        applyReceipts,
        applyReceipt: (receipt) => applyReceipts([receipt]),
        persistReceipt,
        settleReceipts,
        prepareBootstrap,
        stageBootstrapPage,
        installBootstrap,
        invalidateStatus: reactivity.invalidate(["effect-local:status"])
      }
      return Store.of(service)
    })
  )
