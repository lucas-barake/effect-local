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
import * as Semaphore from "effect/Semaphore"
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
  readonly maximumPendingMutations?: number
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly schemaEvolutionBatchBytes?: number
  readonly projectionReplayBatchSize?: number
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

export interface Service {
  readonly membershipIncarnation: Identity.MembershipIncarnation
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
      const projectionReplayBatchSize = options.projectionReplayBatchSize ?? 256
      if (!Number.isSafeInteger(projectionReplayBatchSize) || projectionReplayBatchSize <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "projectionReplayBatchSize",
          message: "projectionReplayBatchSize must be a positive safe integer"
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
      const projectionGate = yield* Semaphore.make(1)
      const registerLineage = ClientLineage.make(sql, options.spaceId)

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
          sql`SELECT space_id, membership_incarnation, definition_hash, schema_version, schema_hash,
          schema_generation, active_schema_generation, active_projection_generation,
          projection_schema_generation,
          target_schema_version, target_schema_hash, migration_hash,
          next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, installed_snapshot_id, installed_snapshot_sequence,
          installed_snapshot_terminal_sequence, projection_replay_generation, projection_replay_cursor
        FROM effect_local_client_spaces WHERE space_id = ${options.spaceId}`
      })
      const findPending = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.PendingRow,
        execute: () =>
          sql`SELECT membership_incarnation, mutation_id, local_sequence, basis, name, payload_json, digest,
        digest_version,
        source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json FROM effect_local_client_pending_data
        WHERE space_id = ${options.spaceId} AND schema_generation = (
          SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
        ORDER BY local_sequence`
      })
      const findPendingByMutation = SqlSchema.findOneOption({
        Request: Identity.MutationId,
        Result: Rows.PendingRow,
        execute: (mutationId) =>
          sql`SELECT membership_incarnation, mutation_id, local_sequence, basis, name, payload_json, digest,
        digest_version,
        source_schema_version, source_schema_hash, mutation_version,
        optimistic_result_json, changes_json FROM effect_local_client_pending_data
        WHERE space_id = ${options.spaceId} AND schema_generation = (
          SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          AND mutation_id = ${mutationId}`
      })
      const findReplayPendingBatch = SqlSchema.findAll({
        Request: Schema.Struct({ after: Schema.Int, limit: Schema.Int }),
        Result: Rows.PendingRow,
        execute: ({ after, limit }) =>
          sql`SELECT p.membership_incarnation, p.mutation_id, p.local_sequence, p.basis, p.name, p.payload_json,
          p.digest, p.digest_version, p.source_schema_version, p.source_schema_hash, p.mutation_version,
          p.optimistic_result_json, p.changes_json FROM effect_local_client_pending_data AS p
          WHERE p.space_id = ${options.spaceId} AND p.schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          AND p.local_sequence > ${after} AND NOT EXISTS (
            SELECT 1 FROM effect_local_server_log AS l
            WHERE l.space_id = ${options.spaceId} AND l.mutation_id = p.mutation_id
          )
          ORDER BY p.local_sequence LIMIT ${limit}`
      })
      const findProjectionRows = SqlSchema.findAll({
        Request: Schema.Struct({ schemaGeneration: Schema.Int, projectionGeneration: Schema.Int, limit: Schema.Int }),
        Result: Rows.ProjectionEntityRow,
        execute: ({ schemaGeneration, projectionGeneration, limit }) =>
          sql`SELECT c.model, c.model_version, c.entity_key, c.value_json
          FROM effect_local_client_canonical_entities_data AS c
          WHERE c.space_id = ${options.spaceId} AND c.schema_generation = ${schemaGeneration}
            AND NOT EXISTS (
              SELECT 1 FROM effect_local_client_visible_entities_data AS v
              WHERE v.space_id = c.space_id AND v.schema_generation = c.schema_generation
                AND v.projection_generation = ${projectionGeneration}
                AND v.model = c.model AND v.entity_key = c.entity_key
            )
          ORDER BY c.model, c.entity_key LIMIT ${limit}`
      })
      const findProjectionRowIds = SqlSchema.findAll({
        Request: Schema.Struct({
          schemaGeneration: Schema.Int,
          projectionGeneration: Schema.Int,
          keep: Schema.Boolean,
          limit: Schema.Int
        }),
        Result: Rows.RowIdRow,
        execute: ({ schemaGeneration: requestedSchemaGeneration, projectionGeneration, keep, limit }) => {
          if (keep) {
            return sql`SELECT rowid AS row_id FROM effect_local_client_visible_entities_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${requestedSchemaGeneration}
                AND projection_generation <> ${projectionGeneration} ORDER BY rowid LIMIT ${limit}`
          }
          return sql`SELECT rowid AS row_id FROM effect_local_client_visible_entities_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${requestedSchemaGeneration}
                AND projection_generation = ${projectionGeneration} ORDER BY rowid LIMIT ${limit}`
        }
      })
      const findReceipt = SqlSchema.findOneOption({
        Request: Identity.MutationId,
        Result: Rows.ReceiptRow,
        execute: (mutationId) =>
          sql`SELECT receipt_json FROM effect_local_client_receipts_data
          WHERE space_id = ${options.spaceId} AND schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
            AND mutation_id = ${mutationId}`
      })
      const findPendingReceipt = SqlSchema.findOneOption({
        Request: Schema.Struct({ after: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
        Result: Rows.PendingReceiptRow,
        execute: ({ after }) =>
          sql`SELECT p.membership_incarnation, p.mutation_id, p.local_sequence, p.basis, p.name, p.payload_json,
          p.digest, p.digest_version,
          p.source_schema_version, p.source_schema_hash, p.mutation_version,
          p.optimistic_result_json, p.changes_json, r.receipt_json,
          l.server_sequence, l.mutation_id AS entry_mutation_id, l.entry_json
        FROM effect_local_client_pending_data AS p
        INNER JOIN effect_local_client_receipts_data AS r
          ON r.space_id = p.space_id AND r.schema_generation = p.schema_generation
          AND r.mutation_id = p.mutation_id
        LEFT JOIN effect_local_server_log AS l ON l.space_id = p.space_id AND l.mutation_id = p.mutation_id
        WHERE p.space_id = ${options.spaceId} AND p.schema_generation = (
          SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          AND p.local_sequence > ${after}
        ORDER BY p.local_sequence LIMIT 1`
      })
      const findPendingLog = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.PendingLogRow,
        execute: () =>
          sql`SELECT p.membership_incarnation, p.mutation_id, p.local_sequence, p.basis, p.name, p.payload_json,
            p.digest, p.digest_version, p.source_schema_version, p.source_schema_hash, p.mutation_version,
            p.optimistic_result_json, p.changes_json, l.server_sequence,
            l.mutation_id AS entry_mutation_id, l.entry_json
          FROM effect_local_client_pending_data AS p
          INNER JOIN effect_local_server_log AS l ON l.space_id = p.space_id AND l.mutation_id = p.mutation_id
          WHERE p.space_id = ${options.spaceId} AND p.schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          ORDER BY p.local_sequence`
      })
      const findLogEntry = SqlSchema.findOneOption({
        Request: Identity.ServerSequence,
        Result: Rows.ClientLogRow,
        execute: (sequence) =>
          sql`SELECT membership_incarnation, server_sequence, mutation_id, entry_json
          FROM effect_local_server_log
        WHERE space_id = ${options.spaceId} AND server_sequence = ${sequence}`
      })
      const countPending = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.CountRow,
        execute: () =>
          sql`SELECT COUNT(*) AS count FROM effect_local_client_pending_data
          WHERE space_id = ${options.spaceId} AND schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})`
      })
      const countReceipts = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.CountRow,
        execute: () =>
          sql`SELECT COUNT(*) AS count FROM effect_local_client_receipts_data
          WHERE space_id = ${options.spaceId} AND schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})`
      })
      const findPrunableReceipts = SqlSchema.findAll({
        Request: Schema.Int,
        Result: Rows.MutationIdRow,
        execute: (limit) =>
          sql`SELECT r.mutation_id FROM effect_local_client_receipts_data AS r
          WHERE r.space_id = ${options.spaceId} AND r.schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          AND NOT EXISTS (
            SELECT 1 FROM effect_local_client_pending_data AS p WHERE p.space_id = r.space_id
              AND p.schema_generation = r.schema_generation AND p.mutation_id = r.mutation_id
          )
          ORDER BY r.local_sequence LIMIT ${limit}`
      })
      const findBootstrap = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: Rows.BootstrapRow,
        execute: () =>
          sql`SELECT snapshot_id, space_id, definition_hash, schema_version, schema_hash,
            server_sequence, terminal_sequence,
            entity_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest
          FROM effect_local_bootstrap WHERE space_id = ${options.spaceId}`
      })
      const findStagedEntities = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.SnapshotEntityRow,
        execute: () =>
          sql`SELECT ordinal, model, model_version, entity_key, value_json, entity_bytes
          FROM effect_local_bootstrap_entities WHERE space_id = ${options.spaceId} ORDER BY ordinal`
      })
      const findStagedIdentities = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.EntityIdentityRow,
        execute: () =>
          sql`SELECT model, model_version, entity_key FROM effect_local_bootstrap_entities
          WHERE space_id = ${options.spaceId}`
      })
      const findEntityIdentities = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.EntityIdentityRow,
        execute: () =>
          sql`SELECT model, model_version, entity_key FROM effect_local_client_visible_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = (
            SELECT active_schema_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
            AND projection_generation = (
              SELECT active_projection_generation FROM effect_local_client_spaces WHERE space_id = ${options.spaceId})
          UNION SELECT model, model_version, entity_key FROM effect_local_bootstrap_entities
            WHERE space_id = ${options.spaceId}`
      })

      const meta = findMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      const initializedMeta = yield* meta
      if (initializedMeta.definition_hash !== options.definition.hash) {
        return yield* new ReplicaError.DefinitionMismatch({
          expected: options.definition.hash,
          actual: initializedMeta.definition_hash
        })
      }
      const schemaGeneration = initializedMeta.schema_generation
      const activeGeneration = initializedMeta.active_schema_generation
      const validateFence = (current: typeof Rows.ClientMetaRow.Type) => {
        if (current.membership_incarnation !== initializedMeta.membership_incarnation) {
          return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: options.spaceId }))
        }
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
            membershipIncarnation: row.membership_incarnation,
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

      const invalidate = (
        entities: ReadonlyArray<Protocol.EntityKey>,
        receiptIds: ReadonlyArray<Identity.MutationId> = []
      ) => {
        const uniqueEntities = Array.from(
          new Map(entities.map((entity) => [SqlTransaction.entityKey(entity), entity])).values()
        )
        const uniqueReceiptIds = Array.from(new Set(receiptIds))
        let invalidateReceipts = Effect.void
        const spaceKey = `effect-local:space:${options.spaceId}`
        if (uniqueReceiptIds.length > 0) {
          invalidateReceipts = reactivity.invalidate(
            uniqueReceiptIds.map((mutationId) => `${spaceKey}:receipt:${mutationId}`)
          )
        }
        return reactivity.invalidate({
          [spaceKey]: [],
          [`${spaceKey}:entities`]: uniqueEntities.map((entity) => [entity.model, entity.key]),
          [`${spaceKey}:status`]: [],
          "effect-local:status": []
        }).pipe(Effect.andThen(invalidateReceipts))
      }

      const nextProjectionGeneration = (current: number) => {
        if (current >= Number.MAX_SAFE_INTEGER) {
          return Effect.fail(
            new ReplicaError.CapacityExceeded({
              resource: "projection generation",
              limit: Number.MAX_SAFE_INTEGER
            })
          )
        }
        return Effect.succeed(current + 1)
      }

      const requestProjectionReplay = (current: typeof Rows.ClientMetaRow.Type) =>
        Effect.gen(function*() {
          if (current.projection_replay_generation !== null) {
            return yield* new ReplicaError.StorageCorrupt({
              message: "Projection replay was not resumed before another replay was requested"
            })
          }
          const target = yield* nextProjectionGeneration(current.active_projection_generation)
          yield* sql`UPDATE effect_local_client_spaces
            SET projection_replay_generation = ${target}, projection_replay_cursor = NULL
            WHERE space_id = ${options.spaceId}`
          return target
        })

      const replayPendingBatch = (
        rows: ReadonlyArray<typeof Rows.PendingRow.Type>,
        replaySchemaGeneration: number,
        projectionGeneration: number
      ) =>
        Effect.gen(function*() {
          for (const row of rows) {
            const item = yield* decodePendingRow(row)
            const changes: Array<Protocol.EntityChange> = []
            const result = yield* sql.withTransaction(
              runtime.executeEnvelope(
                item.envelope,
                SqlTransaction.local({
                  sql,
                  definition: options.definition,
                  table: "visible",
                  spaceId: options.spaceId,
                  schemaGeneration: replaySchemaGeneration,
                  projectionGeneration,
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
              yield* sql`UPDATE effect_local_client_pending_data
                SET optimistic_result_json = ${yield* Codec.stringify(result.success.result)},
                    changes_json = ${yield* Codec.stringify(changes)}
                WHERE space_id = ${options.spaceId} AND schema_generation = ${replaySchemaGeneration}
                  AND mutation_id = ${item.envelope.mutationId}`
            } else {
              yield* sql`UPDATE effect_local_client_pending_data SET changes_json = '[]'
                WHERE space_id = ${options.spaceId} AND schema_generation = ${replaySchemaGeneration}
                  AND mutation_id = ${item.envelope.mutationId}`
            }
          }
        })

      const rebuildProjection = Effect.gen(function*() {
        let current = yield* sql.withTransaction(meta).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
        if (current.projection_replay_generation === null) return yield* Effect.void
        yield* validateFence(current)
        let target = current.projection_replay_generation
        if (target === current.active_projection_generation) {
          target = yield* nextProjectionGeneration(current.active_projection_generation)
          yield* sql.withTransaction(sql`UPDATE effect_local_client_spaces
            SET projection_replay_generation = ${target}, projection_replay_cursor = NULL
            WHERE space_id = ${options.spaceId}`).pipe(
            Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
          current = { ...current, projection_replay_generation: target, projection_replay_cursor: null }
        }
        const replaySchemaGeneration = current.active_schema_generation
        let cursor = current.projection_replay_cursor

        if (cursor === null) {
          while (true) {
            const deleted = yield* sql.withTransaction(Effect.gen(function*() {
              const row = yield* meta
              yield* validateFence(row)
              if (row.projection_replay_generation !== target || row.projection_replay_cursor !== null) {
                return yield* new ReplicaError.StorageCorrupt({ message: "Projection cleanup fence changed" })
              }
              const ids = yield* findProjectionRowIds({
                schemaGeneration: replaySchemaGeneration,
                projectionGeneration: target,
                keep: false,
                limit: projectionReplayBatchSize
              }).pipe(Effect.mapError(StorageUnavailable.make))
              if (ids.length === 0) {
                yield* sql`UPDATE effect_local_client_spaces SET projection_replay_cursor = 'canonical'
                  WHERE space_id = ${options.spaceId}`
                return false
              }
              yield* sql`DELETE FROM effect_local_client_visible_entities_data
                WHERE rowid IN ${sql.in(ids.map((id) => id.row_id))}`
              return true
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            if (!deleted) break
            yield* Effect.yieldNow
          }
          cursor = "canonical"
        }

        if (cursor === "canonical") {
          while (true) {
            const copied = yield* sql.withTransaction(Effect.gen(function*() {
              const row = yield* meta
              yield* validateFence(row)
              if (row.projection_replay_generation !== target || row.projection_replay_cursor !== "canonical") {
                return yield* new ReplicaError.StorageCorrupt({ message: "Projection copy fence changed" })
              }
              const rows = yield* findProjectionRows({
                schemaGeneration: replaySchemaGeneration,
                projectionGeneration: target,
                limit: projectionReplayBatchSize
              }).pipe(Effect.mapError(StorageUnavailable.make))
              if (rows.length === 0) {
                yield* sql`UPDATE effect_local_client_spaces SET projection_replay_cursor = 'pending:0'
                  WHERE space_id = ${options.spaceId}`
                return false
              }
              for (const entity of rows) {
                yield* sql`INSERT INTO effect_local_client_visible_entities_data
                  (space_id, schema_generation, projection_generation, model, model_version, entity_key, value_json)
                  VALUES (${options.spaceId}, ${replaySchemaGeneration}, ${target}, ${entity.model},
                    ${entity.model_version}, ${entity.entity_key}, ${entity.value_json})`
              }
              return true
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            if (!copied) break
            yield* Effect.yieldNow
          }
          cursor = "pending:0"
        }

        if (!cursor.startsWith("pending:")) {
          return yield* new ReplicaError.StorageCorrupt({ message: "Projection replay cursor is invalid" })
        }
        let after = Number(cursor.slice("pending:".length))
        if (!Number.isSafeInteger(after) || after < 0) {
          return yield* new ReplicaError.StorageCorrupt({ message: "Projection replay cursor is invalid" })
        }
        while (true) {
          const replayed = yield* sql.withTransaction(Effect.gen(function*() {
            const row = yield* meta
            yield* validateFence(row)
            if (
              row.projection_replay_generation !== target ||
              row.projection_replay_cursor !== `pending:${after}`
            ) {
              return yield* new ReplicaError.StorageCorrupt({ message: "Projection replay fence changed" })
            }
            const rows = yield* findReplayPendingBatch({ after, limit: projectionReplayBatchSize }).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            if (rows.length === 0) return null
            yield* replayPendingBatch(rows, replaySchemaGeneration, target)
            const next = rows[rows.length - 1].local_sequence
            yield* sql`UPDATE effect_local_client_spaces SET projection_replay_cursor = ${`pending:${next}`}
              WHERE space_id = ${options.spaceId}`
            return next
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          if (replayed === null) break
          after = replayed
          yield* Effect.yieldNow
        }

        yield* sql.withTransaction(Effect.gen(function*() {
          const row = yield* meta
          yield* validateFence(row)
          if (
            row.projection_replay_generation !== target ||
            row.projection_replay_cursor !== `pending:${after}`
          ) {
            return yield* new ReplicaError.StorageCorrupt({ message: "Projection promotion fence changed" })
          }
          yield* sql`UPDATE effect_local_client_spaces SET
            active_projection_generation = ${target},
            projection_schema_generation = ${replaySchemaGeneration},
            projection_replay_generation = NULL,
            projection_replay_cursor = NULL,
            visible_revision = visible_revision + 1
            WHERE space_id = ${options.spaceId}`
          return yield* Effect.void
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

        while (true) {
          const deleted = yield* sql.withTransaction(Effect.gen(function*() {
            const ids = yield* findProjectionRowIds({
              schemaGeneration: replaySchemaGeneration,
              projectionGeneration: target,
              keep: true,
              limit: projectionReplayBatchSize
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (ids.length === 0) return false
            yield* sql`DELETE FROM effect_local_client_visible_entities_data
              WHERE rowid IN ${sql.in(ids.map((id) => id.row_id))}`
            return true
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          if (!deleted) break
          yield* Effect.yieldNow
        }
        yield* invalidate([])
        return yield* Effect.void
      })

      const withProjectionGate = <A, E, R,>(effect: Effect.Effect<A, E, R>) =>
        projectionGate.withPermit(rebuildProjection.pipe(Effect.andThen(effect)))

      const pruneReceipts = (target: number) =>
        Effect.gen(function*() {
          const count = yield* countReceipts(undefined).pipe(Effect.mapError(StorageUnavailable.make))
          const excess = Math.max(0, count.count - target)
          if (excess === 0) return []
          const rows = yield* findPrunableReceipts(excess).pipe(Effect.mapError(StorageUnavailable.make))
          for (let offset = 0; offset < rows.length; offset += 500) {
            const mutationIds = rows.slice(offset, offset + 500).map((row) => row.mutation_id)
            yield* sql`DELETE FROM effect_local_client_receipts_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}
                AND mutation_id IN ${sql.in(mutationIds)}`
          }
          return rows.map((row) => row.mutation_id)
        })

      const bootstrapMatches = (
        row: typeof Rows.BootstrapRow.Type,
        manifest: Protocol.SnapshotManifest
      ) =>
        row.snapshot_id === manifest.snapshotId &&
        row.space_id === manifest.spaceId &&
        row.definition_hash === manifest.definitionHash &&
        row.schema_version === manifest.schema.version &&
        row.schema_hash === manifest.schema.hash &&
        row.server_sequence === manifest.sequence &&
        row.terminal_sequence === manifest.terminalSequenceThrough &&
        row.entity_count === manifest.entityCount &&
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
            if (receipt.membershipIncarnation !== initializedMeta.membership_incarnation) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: "Receipt incarnation does not match this membership"
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
              (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, receipt_json,
                source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin)
              VALUES (${options.spaceId}, ${activeGeneration}, ${receipt.membershipIncarnation},
                ${receipt.mutationId}, ${receipt.localSequence},
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

      const settleReceipts = withProjectionGate(Effect.gen(function*() {
        const touched = new Map<string, Protocol.EntityKey>()
        let prunedReceiptIds: ReadonlyArray<Identity.MutationId> = []
        yield* sql.withTransaction(Effect.gen(function*() {
          const installed = yield* meta
          yield* validateFence(installed)
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
              receipt.membershipIncarnation !== pendingMutation.envelope.membershipIncarnation ||
              receipt.mutationId !== pendingMutation.envelope.mutationId ||
              receipt.localSequence !== pendingMutation.envelope.localSequence
            ) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Receipt does not match pending mutation ${receipt.mutationId}`
              })
            }
            if (receipt._tag === "Accepted") {
              if (row.entry_json === null) {
                if (
                  installed.installed_snapshot_id === null ||
                  receipt.serverSequence > installed.installed_snapshot_sequence ||
                  (receipt.terminalSequence !== undefined &&
                    receipt.terminalSequence > installed.installed_snapshot_terminal_sequence)
                ) continue
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
                  entry.membershipIncarnation !== receipt.membershipIncarnation ||
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
            yield* sql`DELETE FROM effect_local_client_pending_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}
                AND mutation_id = ${receipt.mutationId}`
          }
          if (touched.size > 0) {
            yield* requestProjectionReplay(yield* meta)
          }
          const current = yield* meta
          const logFloor = Math.max(0, current.server_cursor - options.retainedHistoryEntries)
          yield* sql`DELETE FROM effect_local_server_log
            WHERE space_id = ${options.spaceId} AND server_sequence <= ${logFloor} AND NOT EXISTS (
              SELECT 1 FROM effect_local_client_pending_data AS p
              WHERE p.space_id = ${options.spaceId} AND p.schema_generation = ${activeGeneration}
                AND p.mutation_id = effect_local_server_log.mutation_id
            )`
          prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
          return yield* Effect.void
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (touched.size > 0) yield* rebuildProjection
        if (touched.size > 0) yield* invalidate(Array.from(touched.values()))
        if (prunedReceiptIds.length > 0) yield* invalidate([], prunedReceiptIds)
      })).pipe(Effect.withSpan("LocalStore.settleReceipts"))

      const applyReceipts = (receipts: ReadonlyArray<Protocol.Receipt>) =>
        Effect.gen(function*() {
          for (const receipt of receipts) yield* persistReceipt(receipt)
          yield* settleReceipts
        }).pipe(Effect.withSpan("LocalStore.applyReceipts", {
          attributes: { "receipt.count": receipts.length }
        }))

      const validateManifest = (manifest: Protocol.SnapshotManifest) =>
        Effect.gen(function*() {
          if (manifest.spaceId !== options.spaceId) {
            return yield* new ReplicaError.ProtocolInvalid({ message: "Snapshot space does not match this replica" })
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
          if (manifest.entityCount > options.maximumBootstrapEntities) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "bootstrap entities",
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

      const prepareBootstrap = (manifest: Protocol.SnapshotManifest) =>
        Effect.gen(function*() {
          yield* validateManifest(manifest)
          return yield* sql.withTransaction(Effect.gen(function*() {
            yield* validateFence(yield* meta)
            const current = yield* findBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(current) && bootstrapMatches(current.value, manifest)) {
              return current.value.next_ordinal - 1
            }
            yield* sql`DELETE FROM effect_local_bootstrap_entities WHERE space_id = ${options.spaceId}`
            yield* sql`DELETE FROM effect_local_bootstrap WHERE space_id = ${options.spaceId}`
            yield* sql`INSERT INTO effect_local_bootstrap
              (snapshot_id, space_id, definition_hash, schema_version, schema_hash,
                server_sequence, terminal_sequence,
                entity_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest)
              VALUES (${manifest.snapshotId}, ${manifest.spaceId}, ${manifest.definitionHash},
                ${manifest.schema.version}, ${manifest.schema.hash},
                ${manifest.sequence}, ${manifest.terminalSequenceThrough}, ${manifest.entityCount},
                ${manifest.contentBytes}, ${manifest.digest}, 0, 0, ${Protocol.initialSnapshotDigest})`
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
            yield* validateFence(yield* meta)
            const found = yield* findBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(found) || !bootstrapMatches(found.value, page.manifest)) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Bootstrap page does not match the durable snapshot stage ${page.manifest.snapshotId}`
              })
            }
            const stage = found.value
            let nextOrdinal = stage.next_ordinal
            let receivedBytes = stage.received_bytes
            let rollingDigest = stage.rolling_digest
            const stagedIdentities = yield* findStagedIdentities(undefined).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            const stagedKeys = new Set(
              stagedIdentities.map((row) => `${row.model}\u0000${row.entity_key}`)
            )
            const stagedRows: Array<{
              readonly ordinal: number
              readonly model: string
              readonly model_version: number
              readonly entity_key: string
              readonly value_json: string
              readonly entity_bytes: number
            }> = []
            for (const entity of page.entities) {
              if (entity.ordinal !== nextOrdinal) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message:
                    `Snapshot ${page.manifest.snapshotId} expected ordinal ${nextOrdinal} but found ${entity.ordinal}`
                })
              }
              const model = options.definition.modelByName.get(entity.model)
              if (model === undefined) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} contains unknown model ${entity.model}`
                })
              }
              if (entity.modelVersion !== model.version) {
                return yield* new ReplicaError.StaleSchema({
                  expectedVersion: options.definition.schemaIdentity.version,
                  expectedHash: options.definition.schemaIdentity.hash,
                  actualVersion: page.manifest.schema.version,
                  actualHash: page.manifest.schema.hash
                })
              }
              yield* Codec.decode(model.key, entity.key).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ProtocolInvalid({
                    message: `Snapshot ${page.manifest.snapshotId} contains an invalid entity key`,
                    cause
                  })
                )
              )
              yield* Codec.decode(model.schema, entity.value).pipe(
                Effect.mapError((cause) =>
                  new ReplicaError.ProtocolInvalid({
                    message: `Snapshot ${page.manifest.snapshotId} contains an invalid entity value`,
                    cause
                  })
                )
              )
              const entityBytes = yield* Protocol.encodedBytesEffect({
                model: entity.model,
                modelVersion: entity.modelVersion,
                key: entity.key,
                value: entity.value
              })
              if (entity.entityBytes !== entityBytes) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} entity ${entity.ordinal} has invalid byte metadata`
                })
              }
              const keyJson = yield* Codec.stringify(entity.key)
              const stagedKey = `${entity.model}\u0000${keyJson}`
              if (stagedKeys.has(stagedKey)) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} contains duplicate entity ${entity.model}`
                })
              }
              stagedKeys.add(stagedKey)
              receivedBytes += entityBytes
              if (receivedBytes > page.manifest.contentBytes) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Snapshot ${page.manifest.snapshotId} exceeds its declared bytes`
                })
              }
              rollingDigest = Protocol.SnapshotDigest.make(
                yield* Canonical.digest({
                  previous: rollingDigest,
                  entity
                }).pipe(Effect.provideService(Crypto.Crypto, crypto))
              )
              stagedRows.push({
                ordinal: entity.ordinal,
                model: entity.model,
                model_version: entity.modelVersion,
                entity_key: keyJson,
                value_json: yield* Codec.stringify(entity.value),
                entity_bytes: entity.entityBytes
              })
              nextOrdinal += 1
            }
            for (let offset = 0; offset < stagedRows.length; offset += 100) {
              const batch = stagedRows.slice(offset, offset + 100).map((row) => ({
                space_id: options.spaceId,
                ordinal: row.ordinal,
                model: row.model,
                model_version: row.model_version,
                entity_key: row.entity_key,
                value_json: row.value_json,
                entity_bytes: row.entity_bytes
              }))
              yield* sql`INSERT INTO effect_local_bootstrap_entities ${sql.insert(batch)}`
            }
            if (nextOrdinal > page.manifest.entityCount) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} exceeds its declared entity count`
              })
            }
            const complete = nextOrdinal === page.manifest.entityCount
            if (!complete && page.entities.length === 0) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} page made no progress`
              })
            }
            if (page.hasMore === complete) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${page.manifest.snapshotId} continuation disagrees with its entity count`
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
            yield* sql`UPDATE effect_local_bootstrap SET
              next_ordinal = ${nextOrdinal}, received_bytes = ${receivedBytes}, rolling_digest = ${rollingDigest}
              WHERE space_id = ${options.spaceId}`
            return complete
          }))
        }).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("LocalStore.stageBootstrapPage", {
            attributes: { "snapshot.id": page.manifest.snapshotId, "entity.count": page.entities.length }
          })
        )

      const installBootstrap = (manifest: Protocol.SnapshotManifest) =>
        withProjectionGate(Effect.gen(function*() {
          yield* validateManifest(manifest)
          const dirty = new Map<string, Protocol.EntityKey>()
          let prunedReceiptIds: ReadonlyArray<Identity.MutationId> = []
          yield* sql.withTransaction(Effect.gen(function*() {
            yield* validateFence(yield* meta)
            const found = yield* findBootstrap(undefined).pipe(Effect.mapError(StorageUnavailable.make))
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
            const stagedEntities = yield* findStagedEntities(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (stagedEntities.length !== manifest.entityCount) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} durable entity count is incomplete`
              })
            }
            let receivedBytes = 0
            let rollingDigest = Protocol.initialSnapshotDigest
            const stagedKeys = new Set<string>()
            for (let index = 0; index < stagedEntities.length; index++) {
              const row = stagedEntities[index]
              if (row.ordinal !== index) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} durable ordinals are not contiguous`
                })
              }
              const model = options.definition.modelByName.get(row.model)
              if (model === undefined) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} contains unknown durable model ${row.model}`
                })
              }
              if (row.model_version !== model.version) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} durable model version does not match ${row.model}`
                })
              }
              const key = yield* Codec.parse(row.entity_key).pipe(
                Effect.flatMap((value) => Codec.decode(model.key, value))
              )
              const value = yield* Codec.parse(row.value_json).pipe(
                Effect.flatMap((encodedValue) => Codec.decode(model.schema, encodedValue))
              )
              const keyJson = yield* Codec.stringify(key)
              const valueJson = yield* Codec.stringify(value)
              if (keyJson !== row.entity_key || valueJson !== row.value_json) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} contains noncanonical durable entity JSON`
                })
              }
              const stagedKey = `${row.model}\u0000${keyJson}`
              if (stagedKeys.has(stagedKey)) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} contains a duplicate durable entity`
                })
              }
              stagedKeys.add(stagedKey)
              const entityBytes = yield* Protocol.encodedBytesEffect({
                model: row.model,
                modelVersion: row.model_version,
                key,
                value
              })
              if (row.entity_bytes !== entityBytes) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Snapshot ${manifest.snapshotId} durable entity ${row.ordinal} has invalid byte metadata`
                })
              }
              const entity = Protocol.SnapshotEntity.make({
                ordinal: row.ordinal,
                model: row.model,
                modelVersion: row.model_version,
                key,
                value,
                entityBytes
              })
              receivedBytes += entityBytes
              rollingDigest = Protocol.SnapshotDigest.make(
                yield* Canonical.digest({ previous: rollingDigest, entity }).pipe(
                  Effect.provideService(Crypto.Crypto, crypto)
                )
              )
            }
            if (receivedBytes !== manifest.contentBytes || rollingDigest !== manifest.digest) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} durable entities do not match its manifest`
              })
            }
            const current = yield* meta
            if (manifest.sequence < current.server_cursor) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Snapshot ${manifest.snapshotId} is older than the local cursor`
              })
            }
            const identities = yield* findEntityIdentities(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            for (const row of identities) {
              const entity = yield* Codec.decode(Protocol.EntityKey, {
                model: row.model,
                modelVersion: row.model_version,
                key: yield* Codec.parse(row.entity_key)
              })
              dirty.set(SqlTransaction.entityKey(entity), entity)
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
                receipt.membershipIncarnation !== pendingMutation.envelope.membershipIncarnation ||
                receipt.mutationId !== pendingMutation.envelope.mutationId ||
                receipt.localSequence !== pendingMutation.envelope.localSequence
              ) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Receipt does not match durable pending mutation ${row.mutation_id}`
                })
              }
              let covered = false
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
              } else {
                covered = true
              }
              if (covered) {
                yield* sql`DELETE FROM effect_local_client_pending_data
                  WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}
                    AND mutation_id = ${row.mutation_id}`
              }
            }

            const pendingLogs = yield* findPendingLog(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            for (const row of pendingLogs) {
              const pendingMutation = yield* decodePendingRow(row)
              const entry = yield* Codec.parse(row.entry_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
              )
              if (
                row.server_sequence !== entry.sequence ||
                row.entry_mutation_id !== entry.mutationId ||
                entry.mutationId !== pendingMutation.envelope.mutationId ||
                entry.clientId !== options.clientId ||
                entry.membershipIncarnation !== initializedMeta.membership_incarnation ||
                entry.localSequence !== pendingMutation.envelope.localSequence ||
                entry.digest !== pendingMutation.envelope.digest
              ) {
                return yield* new ReplicaError.StorageCorrupt({
                  message: `Pending mutation ${row.mutation_id} has conflicting accepted log evidence`
                })
              }
              if (entry.sequence <= manifest.sequence) {
                yield* sql`DELETE FROM effect_local_client_pending_data
                  WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}
                    AND mutation_id = ${row.mutation_id}`
              }
            }

            yield* sql`DELETE FROM effect_local_client_canonical_entities_data
              WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}`
            yield* sql`INSERT INTO effect_local_client_canonical_entities_data
              (space_id, schema_generation, model, model_version, entity_key, value_json)
              SELECT ${options.spaceId}, ${activeGeneration}, model, model_version, entity_key, value_json
              FROM effect_local_bootstrap_entities WHERE space_id = ${options.spaceId} ORDER BY ordinal`
            yield* sql`DELETE FROM effect_local_server_log
              WHERE space_id = ${options.spaceId} AND server_sequence <= ${manifest.sequence}`
            yield* sql`UPDATE effect_local_client_spaces SET
              server_cursor = ${manifest.sequence},
              installed_snapshot_id = ${manifest.snapshotId},
              installed_snapshot_sequence = ${manifest.sequence},
              installed_snapshot_terminal_sequence = ${manifest.terminalSequenceThrough}
              WHERE space_id = ${options.spaceId}`
            yield* requestProjectionReplay(yield* meta)
            yield* sql`DELETE FROM effect_local_bootstrap_entities WHERE space_id = ${options.spaceId}`
            yield* sql`DELETE FROM effect_local_bootstrap WHERE space_id = ${options.spaceId}`
            prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
            return yield* Effect.void
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          yield* rebuildProjection
          yield* invalidate(Array.from(dirty.values()), prunedReceiptIds)
        })).pipe(
          Effect.uninterruptible,
          Effect.withSpan("LocalStore.installBootstrap", {
            attributes: { "snapshot.id": manifest.snapshotId, "server.sequence": manifest.sequence }
          })
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
          yield* sql`UPDATE effect_local_client_spaces
          SET requested_generation = ${requested}
          WHERE space_id = ${options.spaceId}`
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
            yield* sql`UPDATE effect_local_client_spaces
            SET completed_generation = MAX(completed_generation, ${generation})
            WHERE space_id = ${options.spaceId}`
            return yield* Effect.void
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          return yield* Effect.void
        }).pipe(Effect.withSpan("LocalStore.completeReconciliation", {
          attributes: { "reconciliation.generation": generation }
        }))

      yield* projectionGate.withPermit(rebuildProjection)

      const service: Service = {
        membershipIncarnation: initializedMeta.membership_incarnation,
        mutate: (mutation, payloadValue) =>
          withProjectionGate(Effect.gen(function*() {
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
                digestVersion: 3 as const,
                membershipIncarnation: storedMeta.membership_incarnation,
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
                  spaceId: options.spaceId,
                  schemaGeneration: storedMeta.active_schema_generation,
                  projectionGeneration: storedMeta.active_projection_generation,
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
            (space_id, schema_generation, membership_incarnation, mutation_id, local_sequence, basis, name,
              payload_json, digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
              optimistic_result_json, changes_json)
            VALUES (${options.spaceId}, ${storedMeta.active_schema_generation}, ${envelope.membershipIncarnation}, ${mutationId},
              ${envelope.localSequence}, ${envelope.basis}, ${envelope.name},
              ${yield* Codec.stringify(envelope.payload)}, ${digest}, ${envelope.digestVersion},
              ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash}, ${envelope.mutationVersion}, ${yield* Codec
                .stringify(
                  executed.success.result
                )},
              ${yield* Codec.stringify(changes)})`
              yield* sql`UPDATE effect_local_client_spaces
            SET next_local_sequence = next_local_sequence + 1,
                visible_revision = visible_revision + 1,
                requested_generation = requested_generation + 1
            WHERE space_id = ${options.spaceId}`
              return pendingMutation
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            yield* invalidate(result.changes.map((change) => change.entity))
            return result
          })).pipe(Effect.withSpan("LocalStore.mutate", {
            attributes: {
              "mutation.name": mutation.name,
              "space.id": options.spaceId,
              "client.id": options.clientId
            }
          })),
        get: (model, key) =>
          sql.withTransaction(Effect.gen(function*() {
            const current = yield* meta
            yield* validateFence(current)
            return yield* SqlTransaction.local({
              sql,
              definition: options.definition,
              table: "visible",
              spaceId: options.spaceId,
              schemaGeneration: current.active_schema_generation,
              projectionGeneration: current.active_projection_generation
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
        pendingCount: sql.withTransaction(Effect.gen(function*() {
          yield* validateFence(yield* meta)
          return (yield* countPending(undefined).pipe(Effect.mapError(StorageUnavailable.make))).count
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
        reconciliationGenerations,
        requestReconciliation,
        completeReconciliation,
        applyEntries: (entries) =>
          withProjectionGate(Effect.gen(function*() {
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
              (space_id, membership_incarnation, server_sequence, mutation_id, entry_json,
                source_schema_version, source_schema_hash)
              VALUES (${options.spaceId}, ${entry.membershipIncarnation}, ${entry.sequence}, ${entry.mutationId},
                ${yield* Codec.stringify(entry)},
                ${entry.sourceSchema.version}, ${entry.sourceSchema.hash})`
                const currentChanges = yield* migrateEntryChanges(entry)
                for (const change of currentChanges) {
                  touched.push(change.entity)
                  yield* SqlTransaction.applyLocalChange(
                    sql,
                    "canonical",
                    options.spaceId,
                    currentMeta.active_schema_generation,
                    currentMeta.active_projection_generation,
                    change
                  )
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
                      decodedPending.envelope.membershipIncarnation !== entry.membershipIncarnation ||
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
                      receipt.membershipIncarnation !== entry.membershipIncarnation ||
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
                      yield* sql`DELETE FROM effect_local_client_pending_data
                        WHERE space_id = ${options.spaceId} AND schema_generation = ${activeGeneration}
                          AND mutation_id = ${entry.mutationId}`
                    }
                    settledReceiptIds.push(entry.mutationId)
                  }
                }
                cursor = entry.sequence
              }
              yield* sql`UPDATE effect_local_client_spaces
            SET server_cursor = ${cursor}
            WHERE space_id = ${options.spaceId}`
              yield* requestProjectionReplay(yield* meta)
              const logFloor = Math.max(0, cursor - options.retainedHistoryEntries)
              yield* sql`DELETE FROM effect_local_server_log
                WHERE space_id = ${options.spaceId} AND server_sequence <= ${logFloor} AND NOT EXISTS (
                  SELECT 1 FROM effect_local_client_pending_data AS p
                  WHERE p.space_id = ${options.spaceId} AND p.schema_generation = ${activeGeneration}
                    AND p.mutation_id = effect_local_server_log.mutation_id
                )`
              prunedReceiptIds = yield* pruneReceipts(options.retainedReceipts)
              return yield* Effect.void
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            yield* rebuildProjection
            yield* invalidate(
              touched,
              [...settledReceiptIds, ...prunedReceiptIds]
            )
            return undefined
          })).pipe(Effect.withSpan("LocalStore.applyEntries", {
            attributes: { "entry.count": entries.length, "space.id": options.spaceId }
          })),
        applyReceipts,
        applyReceipt: (receipt) => applyReceipts([receipt]),
        persistReceipt,
        settleReceipts,
        prepareBootstrap,
        stageBootstrapPage,
        installBootstrap,
        invalidateStatus: reactivity.invalidate([
          `effect-local:space:${options.spaceId}:status`,
          "effect-local:status"
        ])
      }
      return Store.of(service)
    })
  )
