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
import * as SqlTransaction from "./internal/transaction.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"

const storageError = (cause: unknown) => new ReplicaError.StorageUnavailable({ cause })

export interface Options {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly maximumPendingMutations?: number
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
  readonly applyEntries: (
    entries: ReadonlyArray<Protocol.AcceptedMutation>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly applyReceipt: (receipt: Protocol.Receipt) => Effect.Effect<void, ReplicaError.ReplicaError>
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
          sql`SELECT space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision
        FROM effect_local_client_meta WHERE singleton = 1`
      })
      const findPending = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.PendingRow,
        execute: () =>
          sql`SELECT mutation_id, local_sequence, basis, name, payload_json, digest,
        optimistic_result_json, changes_json FROM effect_local_pending ORDER BY local_sequence`
      })
      const findReceipt = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Rows.ReceiptRow,
        execute: (mutationId) => sql`SELECT receipt_json FROM effect_local_receipts WHERE mutation_id = ${mutationId}`
      })
      const findLogEntry = SqlSchema.findOneOption({
        Request: Schema.Number,
        Result: Rows.ServerLogRow,
        execute: (sequence) => sql`SELECT entry_json FROM effect_local_server_log WHERE server_sequence = ${sequence}`
      })
      const countPending = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_pending`
      })

      const meta = findMeta(undefined).pipe(Effect.mapError(storageError))
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
          const payload = yield* Codec.parse(row.payload_json)
          const optimisticResult = yield* Codec.parse(row.optimistic_result_json)
          const changes = yield* Codec.parse(row.changes_json)
          return yield* Codec.decode(Protocol.PendingMutation, {
            envelope: {
              spaceId: options.spaceId,
              clientId: options.clientId,
              mutationId: row.mutation_id,
              localSequence: row.local_sequence,
              basis: row.basis,
              name: row.name,
              payload,
              digest: row.digest
            },
            optimisticResult,
            changes
          })
        })

      const pending = findPending(undefined).pipe(
        Effect.mapError(storageError),
        Effect.flatMap(Effect.forEach(decodePendingRow))
      )

      const restoreAndReplay = (initialDirty: ReadonlyArray<Protocol.EntityKey>) =>
        Effect.gen(function*() {
          const dirty = new Map(initialDirty.map((entity) => [SqlTransaction.entityKey(entity), entity]))
          const currentPending = yield* pending
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
          for (const item of currentPending) {
            const changes: Array<Protocol.EntityChange> = []
            const result = yield* runtime.execute(
              item.envelope.name,
              item.envelope.payload,
              SqlTransaction.local({ sql, definition: options.definition, table: "visible", changes }),
              changes
            )
            for (const change of changes) {
              const id = SqlTransaction.entityKey(change.entity)
              if (!dirty.has(id)) {
                dirty.set(id, change.entity)
              }
            }
            if (Result.isSuccess(result)) {
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
      ) =>
        reactivity.invalidate({
          "effect-local:entities": entities.map((entity) => [entity.model, entity.key]),
          "effect-local:status": []
        }).pipe(Effect.andThen(
          receiptIds.length === 0 ?
            Effect.void :
            reactivity.invalidate(receiptIds.map((mutationId) => `effect-local:receipt:${mutationId}`))
        ))

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
              const count = yield* countPending(undefined).pipe(Effect.mapError(storageError))
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
                payload: payloadJsonValue
              }
              const digest = yield* Canonical.digest(identity).pipe(Effect.provideService(Crypto.Crypto, crypto))
              const envelope = yield* Codec.decode(Protocol.MutationEnvelope, { ...identity, digest })
              if (Protocol.encodedBytes(envelope) > Protocol.maximumMutationBytes) {
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
            (mutation_id, local_sequence, basis, name, payload_json, digest, optimistic_result_json, changes_json)
            VALUES (${mutationId}, ${envelope.localSequence}, ${envelope.basis}, ${envelope.name},
              ${yield* Codec.stringify(envelope.payload)}, ${digest}, ${yield* Codec.stringify(
                executed.success.result
              )},
              ${yield* Codec.stringify(changes)})`
              yield* sql`UPDATE effect_local_client_meta
            SET next_local_sequence = next_local_sequence + 1, visible_revision = visible_revision + 1
            WHERE singleton = 1`
              return pendingMutation
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause))))
            yield* invalidate(result.changes.map((change) => change.entity))
            return result
          }),
        get: (model, key) =>
          SqlTransaction.local({ sql, definition: options.definition, table: "visible" }).get(model, key),
        pending,
        receipt: (mutationId) =>
          findReceipt(mutationId).pipe(
            Effect.mapError(storageError),
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
        pendingCount: countPending(undefined).pipe(Effect.mapError(storageError), Effect.map((row) => row.count)),
        applyEntries: (entries) =>
          Effect.gen(function*() {
            if (entries.length === 0) return
            const touched = entries.flatMap((entry) => entry.changes.map((change) => change.entity))
            yield* sql.withTransaction(Effect.gen(function*() {
              let cursor = (yield* meta).server_cursor
              for (const entry of entries) {
                if (entry.envelope.spaceId !== options.spaceId) {
                  return yield* new ReplicaError.ProtocolInvalid({
                    message: `Entry space ${entry.envelope.spaceId} does not match replica space ${options.spaceId}`
                  })
                }
                if (entry.sequence <= cursor) {
                  const stored = yield* findLogEntry(entry.sequence).pipe(Effect.mapError(storageError))
                  if (Option.isNone(stored)) {
                    return yield* new ReplicaError.StorageCorrupt({
                      message: `Server cursor covers missing log entry ${entry.sequence}`
                    })
                  }
                  const decoded = yield* Codec.parse(stored.value.entry_json).pipe(
                    Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
                  )
                  if (Canonical.stringify(decoded) !== Canonical.stringify(entry)) {
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
              VALUES (${entry.sequence}, ${entry.envelope.mutationId}, ${yield* Codec.stringify(entry)})`
                for (const change of entry.changes) {
                  yield* SqlTransaction.applyLocalChange(sql, "canonical", change)
                }
                if (entry.envelope.clientId === options.clientId) {
                  const receipt: Protocol.AcceptedReceipt = {
                    _tag: "Accepted",
                    spaceId: entry.envelope.spaceId,
                    clientId: entry.envelope.clientId,
                    mutationId: entry.envelope.mutationId,
                    localSequence: entry.envelope.localSequence,
                    serverSequence: entry.sequence,
                    result: entry.result
                  }
                  const storedReceipt = yield* findReceipt(receipt.mutationId).pipe(Effect.mapError(storageError))
                  if (Option.isSome(storedReceipt)) {
                    const decoded = yield* Codec.parse(storedReceipt.value.receipt_json).pipe(
                      Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
                    )
                    if (Canonical.stringify(decoded) !== Canonical.stringify(receipt)) {
                      return yield* new ReplicaError.ProtocolInvalid({
                        message: `Accepted entry conflicts with receipt ${receipt.mutationId}`
                      })
                    }
                  }
                  yield* sql`INSERT INTO effect_local_receipts (mutation_id, local_sequence, receipt_json)
                VALUES (${receipt.mutationId}, ${receipt.localSequence}, ${yield* Codec.stringify(receipt)})
                ON CONFLICT (mutation_id) DO NOTHING`
                  yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${entry.envelope.mutationId}`
                }
                cursor = entry.sequence
              }
              yield* restoreAndReplay(touched)
              yield* sql`UPDATE effect_local_client_meta
            SET server_cursor = ${cursor}, visible_revision = visible_revision + 1 WHERE singleton = 1`
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause))))
            yield* invalidate(
              touched,
              entries.filter((entry) => entry.envelope.clientId === options.clientId).map((entry) =>
                entry.envelope.mutationId
              )
            )
          }),
        applyReceipt: (receipt) =>
          Effect.gen(function*() {
            const touched: Array<Protocol.EntityKey> = []
            yield* sql.withTransaction(Effect.gen(function*() {
              if (receipt.spaceId !== options.spaceId || receipt.clientId !== options.clientId) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: "Receipt identity does not match this replica"
                })
              }
              const storedReceipt = yield* findReceipt(receipt.mutationId).pipe(Effect.mapError(storageError))
              if (Option.isSome(storedReceipt)) {
                const decoded = yield* Codec.parse(storedReceipt.value.receipt_json).pipe(
                  Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value))
                )
                if (Canonical.stringify(decoded) !== Canonical.stringify(receipt)) {
                  return yield* new ReplicaError.ProtocolInvalid({
                    message: `Conflicting duplicate receipt ${receipt.mutationId}`
                  })
                }
                return
              }
              const existing = (yield* pending).find((item) => item.envelope.mutationId === receipt.mutationId)
              if (existing === undefined || existing.envelope.localSequence !== receipt.localSequence) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Receipt does not match pending mutation ${receipt.mutationId}`
                })
              }
              yield* sql`INSERT INTO effect_local_receipts (mutation_id, local_sequence, receipt_json)
            VALUES (${receipt.mutationId}, ${receipt.localSequence}, ${yield* Codec.stringify(receipt)})
            ON CONFLICT (mutation_id) DO NOTHING`
              if (receipt._tag === "Rejected") {
                touched.push(...existing.changes.map((change) => change.entity))
                yield* sql`DELETE FROM effect_local_pending WHERE mutation_id = ${receipt.mutationId}`
                yield* restoreAndReplay(touched)
                yield* sql`UPDATE effect_local_client_meta SET visible_revision = visible_revision + 1 WHERE singleton = 1`
              }
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause))))
            yield* invalidate(touched, [receipt.mutationId])
          }),
        invalidateStatus: reactivity.invalidate(["effect-local:status"])
      }
      return Store.of(service)
    })
  )
