import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
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
import * as Codec from "./internal/codec.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as TerminalRejection from "./internal/TerminalRejection.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"

export interface Options {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly maximumPendingMutations?: number
}

export interface ReconciliationGenerations {
  readonly requested: number
  readonly completed: number
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
      yield* Migrations.client(options)

      const findMeta = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.ClientMetaRow,
        execute: () =>
          sql`SELECT space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation
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

      const meta = findMeta(undefined).pipe(Effect.mapError(StorageUnavailable.make))
      const initializedMeta = yield* meta
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

      const pending = findPending(undefined).pipe(
        Effect.mapError(StorageUnavailable.make),
        Effect.flatMap(Effect.forEach(decodePendingRow))
      )

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
            yield* sql`INSERT INTO effect_local_visible_entities (model, entity_key, value_json)
          SELECT model, entity_key, value_json FROM effect_local_canonical_entities
          WHERE model = ${entity.model} AND entity_key = ${keyJson}`
          }
          for (const item of replayPending) {
            const changes: Array<Protocol.EntityChange> = []
            const result = yield* sql.withTransaction(
              runtime.execute(
                item.envelope.name,
                item.envelope.payload,
                SqlTransaction.local({ sql, definition: options.definition, table: "visible", changes }),
                changes
              ).pipe(
                Effect.flatMap((result) =>
                  Result.isFailure(result)
                    ? Effect.fail(new TerminalRejection.TerminalRejection({
                      origin: "Mutation",
                      rejection: result.failure
                    }))
                    : Effect.succeed(result.success)
                )
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
        })

      const invalidate = (
        entities: ReadonlyArray<Protocol.EntityKey>,
        receiptIds: ReadonlyArray<Identity.MutationId> = []
      ) => {
        const uniqueEntities = Array.from(
          new Map(entities.map((entity) => [SqlTransaction.entityKey(entity), entity])).values()
        )
        const uniqueReceiptIds = Array.from(new Set(receiptIds))
        return (
          reactivity.invalidate({
            "effect-local:entities": uniqueEntities.map((entity) => [entity.model, entity.key]),
            "effect-local:status": []
          }).pipe(Effect.andThen(
            uniqueReceiptIds.length === 0 ?
              Effect.void :
              reactivity.invalidate(uniqueReceiptIds.map((mutationId) => `effect-local:receipt:${mutationId}`))
          ))
        )
      }

      const persistReceipt = (receipt: Protocol.Receipt) =>
        Effect.gen(function*() {
          if ((yield* Protocol.encodedBytesEffect(receipt)) > Protocol.maximumReceiptBytes) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Receipt ${receipt.mutationId} exceeds the protocol byte limit`
            })
          }
          let inserted = false
          yield* sql.withTransaction(Effect.gen(function*() {
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
              return
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
            yield* sql`INSERT INTO effect_local_receipts (mutation_id, local_sequence, receipt_json)
          VALUES (${receipt.mutationId}, ${receipt.localSequence}, ${yield* Codec.stringify(receipt)})`
            inserted = true
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
          if (inserted) yield* invalidate([], [receipt.mutationId])
        }).pipe(Effect.withSpan("LocalStore.persistReceipt", {
          attributes: { "mutation.id": receipt.mutationId }
        }))

      const settleReceipts = Effect.gen(function*() {
        const touched = new Map<string, Protocol.EntityKey>()
        yield* sql.withTransaction(Effect.gen(function*() {
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
              if (row.entry_json === null) continue
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
            for (const change of pendingMutation.changes) {
              touched.set(SqlTransaction.entityKey(change.entity), change.entity)
            }
            yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${receipt.mutationId}`
          }
          if (touched.size > 0) {
            yield* restoreAndReplay(Array.from(touched.values()))
            yield* sql`UPDATE effect_local_client_meta
          SET visible_revision = visible_revision + 1 WHERE singleton = 1`
          }
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (touched.size > 0) yield* invalidate(Array.from(touched.values()))
      }).pipe(Effect.withSpan("LocalStore.settleReceipts"))

      const applyReceipts = (receipts: ReadonlyArray<Protocol.Receipt>) =>
        Effect.gen(function*() {
          for (const receipt of receipts) yield* persistReceipt(receipt)
          yield* settleReceipts
        }).pipe(Effect.withSpan("LocalStore.applyReceipts", {
          attributes: { "receipt.count": receipts.length }
        }))

      const reconciliationGenerations = meta.pipe(
        Effect.map((row) => ({
          requested: row.requested_generation,
          completed: row.completed_generation
        }))
      )

      const nextReconciliationGeneration = (requested: number) =>
        requested >= Number.MAX_SAFE_INTEGER
          ? Effect.fail(
            new ReplicaError.CapacityExceeded({
              resource: "reconciliation generations",
              limit: Number.MAX_SAFE_INTEGER
            })
          )
          : Effect.succeed(requested + 1)

      const requestReconciliation = sql.withTransaction(
        Effect.gen(function*() {
          const current = yield* meta
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
            if (generation > current.requested_generation) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Reconciliation generation ${generation} was not requested`
              })
            }
            yield* sql`UPDATE effect_local_client_meta
            SET completed_generation = MAX(completed_generation, ${generation})
            WHERE singleton = 1`
          })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
        }).pipe(Effect.withSpan("LocalStore.completeReconciliation", {
          attributes: { "reconciliation.generation": generation }
        }))

      const service: Service = {
        mutate: (mutation, payloadValue) =>
          Effect.gen(function*() {
            const result = yield* sql.withTransaction(Effect.gen(function*() {
              const storedMeta = yield* meta
              if (storedMeta.definition_hash !== options.definition.hash) {
                return yield* new ReplicaError.DefinitionMismatch({
                  expected: options.definition.hash,
                  actual: storedMeta.definition_hash
                })
              }
              yield* nextReconciliationGeneration(storedMeta.requested_generation)
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
                SqlTransaction.local({ sql, definition: options.definition, table: "visible", changes }),
                changes
              )
              if (Result.isFailure(executed)) {
                const rejection = yield* Codec.decode(mutation.rejectionSchema, executed.failure)
                return yield* Effect.fail(rejection as Mutation.Rejection<typeof mutation>)
              }
              const pendingMutation: Protocol.PendingMutation = {
                envelope,
                optimisticResult: executed.success.result,
                changes
              }
              yield* sql`INSERT INTO effect_local_pending
            (mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
              source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json)
            VALUES (${mutationId}, ${envelope.localSequence}, ${envelope.basis}, ${envelope.name},
              ${yield* Codec.stringify(envelope.payload)}, ${digest}, ${envelope.digestVersion},
              ${envelope.sourceSchema.version}, ${envelope.sourceSchema.hash}, ${envelope.mutationVersion}, ${yield* Codec.stringify(
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
          SqlTransaction.local({ sql, definition: options.definition, table: "visible" }).get(model, key),
        pending,
        receipt: (mutationId) =>
          findReceipt(mutationId).pipe(
            Effect.mapError(StorageUnavailable.make),
            Effect.flatMap((row) =>
              Option.isNone(row) ?
                Effect.succeed(Option.none()) :
                Codec.parse(row.value.receipt_json).pipe(
                  Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value)),
                  Effect.map(Option.some)
                )
            )
          ),
        cursor: meta.pipe(Effect.map((row) => Identity.ServerSequence.make(row.server_cursor))),
        pendingCount: countPending(undefined).pipe(
          Effect.mapError(StorageUnavailable.make),
          Effect.map((row) => row.count)
        ),
        reconciliationGenerations,
        requestReconciliation,
        completeReconciliation,
        applyEntries: (entries) =>
          Effect.gen(function*() {
            if (entries.length === 0) return
            const touched = entries.flatMap((entry) => entry.changes.map((change) => change.entity))
            const settledReceiptIds: Array<Identity.MutationId> = []
            yield* sql.withTransaction(Effect.gen(function*() {
              let cursor = (yield* meta).server_cursor
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
                yield* sql`INSERT INTO effect_local_server_log (server_sequence, mutation_id, entry_json)
              VALUES (${entry.sequence}, ${entry.mutationId}, ${yield* Codec.stringify(entry)})`
                for (const change of entry.changes) {
                  yield* SqlTransaction.applyLocalChange(sql, "canonical", change)
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
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
            yield* invalidate(
              touched,
              settledReceiptIds
            )
          }).pipe(Effect.withSpan("LocalStore.applyEntries", {
            attributes: { "entry.count": entries.length, "space.id": options.spaceId }
          })),
        applyReceipts,
        applyReceipt: (receipt) => applyReceipts([receipt]),
        persistReceipt,
        settleReceipts,
        invalidateStatus: reactivity.invalidate(["effect-local:status"])
      }
      return Store.of(service)
    })
  )
