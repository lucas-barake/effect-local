import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./internal/codec.js"
import * as Rows from "./internal/rows.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"

export interface Service {
  readonly submit: (envelope: Protocol.MutationEnvelope) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly admit: (
    envelope: Protocol.MutationEnvelope,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly pull: (request: Protocol.PullRequest) => Effect.Effect<Protocol.PullPage, ReplicaError.ReplicaError>
  readonly pullAuthorized: (
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.PullPage, ReplicaError.ReplicaError>
  readonly watch: (spaceId: Identity.SpaceId) => Stream.Stream<Protocol.Wake, never>
  readonly watchAuthorized: (
    spaceId: Identity.SpaceId,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Stream.Stream<Protocol.Wake, never>, ReplicaError.ReplicaError>
}

export class ServerStore extends Context.Service<ServerStore, Service>()(
  "@lucas-barake/effect-local-sql/ServerStore"
) {}

export const layer = <R = never,>(options: {
  readonly definition: Definition.Any
  readonly authorize?: (input: {
    readonly envelope: Protocol.MutationEnvelope
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
  readonly authorizeRead?: (input: {
    readonly spaceId: Identity.SpaceId
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
  readonly wakeCapacity?: number
}): Layer.Layer<
  ServerStore,
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto | MutationRuntime.MutationRuntime | R
> =>
  Layer.effect(
    ServerStore,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const crypto = yield* Crypto.Crypto
      const runtime = yield* MutationRuntime.MutationRuntime
      const context = yield* Effect.context<R>()
      const wakes = yield* PubSub.sliding<Protocol.Wake>(options.wakeCapacity ?? 1_024)
      yield* Effect.addFinalizer(() => PubSub.shutdown(wakes))
      yield* Migrations.server

      const findReceiptByMutation = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Schema.String, mutationId: Schema.String }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, mutationId }) =>
          sql`SELECT digest, mutation_id, receipt_json
        FROM effect_local_server_receipts WHERE space_id = ${spaceId} AND mutation_id = ${mutationId}`
      })
      const findReceiptBySequence = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Schema.String, clientId: Schema.String, localSequence: Schema.Number }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, clientId, localSequence }) =>
          sql`SELECT digest, mutation_id, receipt_json
        FROM effect_local_server_receipts
        WHERE space_id = ${spaceId} AND client_id = ${clientId} AND local_sequence = ${localSequence}`
      })
      const lockClient = SqlSchema.findOne({
        Request: Schema.Struct({ spaceId: Schema.String, clientId: Schema.String }),
        Result: Rows.ServerClientRow,
        execute: ({ spaceId, clientId }) =>
          sql`UPDATE effect_local_server_clients
        SET last_local_sequence = last_local_sequence
        WHERE space_id = ${spaceId} AND client_id = ${clientId}
        RETURNING last_local_sequence`
      })
      const lockSpace = SqlSchema.findOne({
        Request: Schema.String,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`UPDATE effect_local_server_spaces
        SET next_server_sequence = next_server_sequence
        WHERE space_id = ${spaceId}
        RETURNING definition_hash, next_server_sequence`
      })
      const findLog = SqlSchema.findAll({
        Request: Schema.Struct({ spaceId: Schema.String, after: Schema.Number, limit: Schema.Number }),
        Result: Rows.ServerLogRow,
        execute: ({ spaceId, after, limit }) =>
          sql`SELECT entry_json FROM effect_local_authoritative_log
        WHERE space_id = ${spaceId} AND server_sequence > ${after}
        ORDER BY server_sequence LIMIT ${limit}`
      })

      const decodeReceipt = (json: string) =>
        Codec.parse(json).pipe(Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value)))
      const authorizeRead = (spaceId: Identity.SpaceId, principal: typeof Schema.Json.Type) =>
        options.authorizeRead === undefined ?
          Effect.void :
          options.authorizeRead({ spaceId, principal }).pipe(
            Effect.provide(context),
            Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
          )

      const admit = (envelope: Protocol.MutationEnvelope, principal: typeof Schema.Json.Type) =>
        Effect.gen(function*() {
          if (Protocol.encodedBytes(envelope) > Protocol.maximumMutationBytes) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "mutation bytes",
              limit: Protocol.maximumMutationBytes
            })
          }
          const { digest, ...identity } = envelope
          const expectedDigest = yield* Canonical.digest(identity).pipe(Effect.provideService(Crypto.Crypto, crypto))
          if (digest !== expectedDigest) {
            return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
          }
          return yield* sql.withTransaction(Effect.gen(function*() {
            const existingByMutation = yield* findReceiptByMutation({
              spaceId: envelope.spaceId,
              mutationId: envelope.mutationId
            }).pipe(Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause })))
            if (Option.isSome(existingByMutation)) {
              if (existingByMutation.value.digest !== envelope.digest) {
                return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
              }
              return yield* decodeReceipt(existingByMutation.value.receipt_json)
            }
            const existingBySequence = yield* findReceiptBySequence({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              localSequence: envelope.localSequence
            }).pipe(Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause })))
            if (Option.isSome(existingBySequence)) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
            }

            yield* sql`INSERT INTO effect_local_server_spaces (space_id, definition_hash, next_server_sequence)
          VALUES (${envelope.spaceId}, ${options.definition.hash}, 1) ON CONFLICT (space_id) DO NOTHING`
            yield* sql`INSERT INTO effect_local_server_clients (space_id, client_id, last_local_sequence)
          VALUES (${envelope.spaceId}, ${envelope.clientId}, 0)
          ON CONFLICT (space_id, client_id) DO NOTHING`
            const storedSpace = yield* lockSpace(envelope.spaceId).pipe(
              Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
            )
            const client = yield* lockClient({ spaceId: envelope.spaceId, clientId: envelope.clientId }).pipe(
              Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
            )
            const committedByMutation = yield* findReceiptByMutation({
              spaceId: envelope.spaceId,
              mutationId: envelope.mutationId
            }).pipe(Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause })))
            if (Option.isSome(committedByMutation)) {
              if (committedByMutation.value.digest !== envelope.digest) {
                return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
              }
              return yield* decodeReceipt(committedByMutation.value.receipt_json)
            }
            const committedBySequence = yield* findReceiptBySequence({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              localSequence: envelope.localSequence
            }).pipe(Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause })))
            if (Option.isSome(committedBySequence)) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
            }
            if (storedSpace.definition_hash !== options.definition.hash) {
              return yield* new ReplicaError.DefinitionMismatch({
                expected: options.definition.hash,
                actual: storedSpace.definition_hash
              })
            }
            const expected = client.last_local_sequence + 1
            if (envelope.localSequence !== expected) {
              return yield* new ReplicaError.OutOfOrderMutation({ expected, actual: envelope.localSequence })
            }

            const authorization = options.authorize === undefined ?
              Result.succeed(undefined) :
              yield* options.authorize({ envelope, principal }).pipe(Effect.provide(context), Effect.result)
            let receipt: Protocol.Receipt
            if (Result.isFailure(authorization)) {
              receipt = {
                _tag: "Rejected",
                spaceId: envelope.spaceId,
                clientId: envelope.clientId,
                mutationId: envelope.mutationId,
                localSequence: envelope.localSequence,
                rejection: authorization.failure
              }
            } else {
              const changes: Array<Protocol.EntityChange> = []
              const executed = yield* runtime.execute(
                envelope.name,
                envelope.payload,
                SqlTransaction.server({ sql, definition: options.definition, spaceId: envelope.spaceId, changes }),
                changes
              )
              if (Result.isFailure(executed)) {
                receipt = {
                  _tag: "Rejected",
                  spaceId: envelope.spaceId,
                  clientId: envelope.clientId,
                  mutationId: envelope.mutationId,
                  localSequence: envelope.localSequence,
                  rejection: executed.failure
                }
              } else {
                const sequence = Identity.ServerSequence.make(storedSpace.next_server_sequence)
                yield* sql`UPDATE effect_local_server_spaces
              SET next_server_sequence = next_server_sequence + 1 WHERE space_id = ${envelope.spaceId}`
                const entry: Protocol.AcceptedMutation = {
                  sequence,
                  envelope,
                  result: executed.success.result,
                  changes: executed.success.changes
                }
                yield* sql`INSERT INTO effect_local_authoritative_log (space_id, server_sequence, mutation_id, entry_json)
              VALUES (${envelope.spaceId}, ${sequence}, ${envelope.mutationId}, ${yield* Codec.stringify(entry)})`
                receipt = {
                  _tag: "Accepted",
                  spaceId: envelope.spaceId,
                  clientId: envelope.clientId,
                  mutationId: envelope.mutationId,
                  localSequence: envelope.localSequence,
                  serverSequence: sequence,
                  result: executed.success.result
                }
              }
            }
            yield* sql`INSERT INTO effect_local_server_receipts
          (space_id, client_id, local_sequence, mutation_id, digest, receipt_json)
          VALUES (${envelope.spaceId}, ${envelope.clientId}, ${envelope.localSequence}, ${envelope.mutationId},
            ${envelope.digest}, ${yield* Codec.stringify(receipt)})`
            yield* sql`UPDATE effect_local_server_clients SET last_local_sequence = ${envelope.localSequence}
          WHERE space_id = ${envelope.spaceId} AND client_id = ${envelope.clientId}`
            return receipt
          }))
        }).pipe(
          Effect.catchIf(
            SqlError.isSqlError,
            (cause) => Effect.fail(new ReplicaError.UnknownCommitOutcome({ mutationId: envelope.mutationId, cause }))
          ),
          Effect.tap((receipt) =>
            receipt._tag === "Accepted"
              ? PubSub.publish(wakes, { spaceId: envelope.spaceId, sequence: receipt.serverSequence }).pipe(
                Effect.asVoid
              )
              : Effect.void
          ),
          Effect.withSpan("ServerStore.submit", {
            attributes: {
              "mutation.name": envelope.name,
              "mutation.id": envelope.mutationId,
              "space.id": envelope.spaceId
            }
          })
        )

      const pull = (request: Protocol.PullRequest) =>
        findLog({ ...request, limit: request.limit + 1 }).pipe(
          Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause })),
          Effect.flatMap((rows) =>
            Effect.forEach(rows, (row) =>
              Codec.parse(row.entry_json).pipe(
                Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
              ))
          ),
          Effect.flatMap((entries) => {
            const selected: Array<Protocol.AcceptedMutation> = []
            let bytes = Protocol.encodedBytes({ entries: [], hasMore: true })
            for (const entry of entries) {
              if (selected.length >= request.limit) break
              const entryBytes = Protocol.encodedBytes(entry) + (selected.length === 0 ? 0 : 1)
              if (bytes + entryBytes > Protocol.maximumBatchBytes) {
                if (selected.length === 0) {
                  return Effect.fail(
                    new ReplicaError.CapacityExceeded({
                      resource: "accepted mutation bytes",
                      limit: Protocol.maximumBatchBytes
                    })
                  )
                }
                break
              }
              selected.push(entry)
              bytes += entryBytes
            }
            return Effect.succeed({
              entries: selected,
              hasMore: entries.length > selected.length
            })
          })
        )
      const watch = (spaceId: Identity.SpaceId) =>
        Stream.unwrap(
          PubSub.subscribe(wakes).pipe(
            Effect.map((subscription) =>
              Stream.fromSubscription(subscription).pipe(
                Stream.filter((wake) => wake.spaceId === spaceId)
              )
            )
          )
        )

      return ServerStore.of({
        submit: (envelope) => admit(envelope, null),
        admit,
        pull,
        pullAuthorized: (request, principal) =>
          authorizeRead(request.spaceId, principal).pipe(Effect.andThen(pull(request))),
        watch,
        watchAuthorized: (spaceId, principal) => authorizeRead(spaceId, principal).pipe(Effect.as(watch(spaceId)))
      })
    })
  )
