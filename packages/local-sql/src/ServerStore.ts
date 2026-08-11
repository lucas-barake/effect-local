import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
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
import * as SchemaEvolution from "./SchemaEvolution.js"

export interface Service {
  readonly submit: (
    request: Protocol.SubmitRequest | Protocol.MutationEnvelope
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly admit: (
    request: Protocol.SubmitRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly pull: (
    request: Protocol.PullRequest | Omit<Protocol.PullRequest, "schema">
  ) => Effect.Effect<Protocol.PullPage, ReplicaError.ReplicaError>
  readonly pullAuthorized: (
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.PullPage, ReplicaError.ReplicaError>
  readonly watch: (
    request: Protocol.WatchRequest | Identity.SpaceId
  ) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
  readonly watchAuthorized: (
    request: Protocol.WatchRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>, ReplicaError.ReplicaError>
}

export class ServerStore extends Context.Service<ServerStore, Service>()(
  "@lucas-barake/effect-local-sql/ServerStore"
) {}

export interface Options<R = never,> {
  readonly definition: Definition.Any
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly authorizeAccess: (input: {
    readonly spaceId: Identity.SpaceId
    readonly clientId: Identity.ClientId
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
  readonly authorizeMutation: (input: {
    readonly mutation: MutationRuntime.CurrentMutationView
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
  readonly authorizeRead: (input: {
    readonly spaceId: Identity.SpaceId
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
  readonly wakeCapacity?: number
}

export const layer = <R = never,>(options: Options<R>): Layer.Layer<
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
      const evolution = options.evolution ?? Evolution.make({ current: options.definition })
      const context = yield* Effect.context<R>()
      const wakeCapacity = options.wakeCapacity ?? 1_024
      if (
        runtime.migrationHash !== evolution.migrationHash ||
        runtime.schemaIdentity.version !== evolution.current.schemaIdentity.version ||
        runtime.schemaIdentity.hash !== evolution.current.schemaIdentity.hash
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "mutationRuntime",
          message: "MutationRuntime and ServerStore must use the same evolution catalog"
        })
      }
      if (!Number.isSafeInteger(wakeCapacity) || wakeCapacity <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "wakeCapacity",
          message: "wakeCapacity must be a positive safe integer"
        })
      }
      const wakes = yield* RcMap.make({
        lookup: (_spaceId: Identity.SpaceId) =>
          Effect.acquireRelease(
            PubSub.sliding<Protocol.Wake>(wakeCapacity),
            PubSub.shutdown
          )
      })
      yield* Migrations.server

      const findReceiptByMutation = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Schema.String, mutationId: Schema.String }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, mutationId }) =>
          sql`SELECT r.space_id, r.client_id, r.local_sequence, r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, l.server_sequence
        FROM effect_local_server_receipts AS r
        LEFT JOIN effect_local_authoritative_log AS l
          ON l.space_id = r.space_id AND l.mutation_id = r.mutation_id
        WHERE r.space_id = ${spaceId} AND r.mutation_id = ${mutationId}`
      })
      const findReceiptBySequence = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Schema.String, clientId: Schema.String, localSequence: Schema.Number }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, clientId, localSequence }) =>
          sql`SELECT r.space_id, r.client_id, r.local_sequence, r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, l.server_sequence
        FROM effect_local_server_receipts AS r
        LEFT JOIN effect_local_authoritative_log AS l
          ON l.space_id = r.space_id AND l.mutation_id = r.mutation_id
        WHERE r.space_id = ${spaceId} AND r.client_id = ${clientId} AND r.local_sequence = ${localSequence}`
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
        RETURNING definition_hash, schema_version, schema_hash, schema_generation,
          target_schema_version, target_schema_hash, migration_hash, next_server_sequence`
      })
      const findLogMetadata = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          after: Identity.ServerSequence,
          limit: Schema.Int.check(Schema.isGreaterThan(0))
        }),
        Result: Rows.ServerLogMetadataRow,
        execute: ({ spaceId, after, limit }) =>
          sql`SELECT server_sequence, entry_bytes FROM effect_local_authoritative_log
        WHERE space_id = ${spaceId} AND server_sequence > ${after}
        ORDER BY server_sequence LIMIT ${limit}`
      })
      const findLogEntries = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          after: Identity.ServerSequence,
          through: Identity.ServerSequence
        }),
        Result: Rows.ServerLogRow,
        execute: ({ spaceId, after, through }) =>
          sql`SELECT l.space_id, l.server_sequence, l.mutation_id, l.entry_bytes, l.entry_json,
          r.client_id AS receipt_client_id, r.local_sequence AS receipt_local_sequence, r.digest,
          l.source_schema_version, l.source_schema_hash
        FROM effect_local_authoritative_log AS l
        INNER JOIN effect_local_server_receipts AS r
          ON r.space_id = l.space_id AND r.mutation_id = l.mutation_id
        WHERE l.space_id = ${spaceId} AND l.server_sequence > ${after} AND l.server_sequence <= ${through}
        ORDER BY l.server_sequence`
      })
      const findSpace = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`SELECT definition_hash, schema_version, schema_hash, schema_generation,
            target_schema_version, target_schema_hash, migration_hash, next_server_sequence
            FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
      })

      const decodeReceipt = (json: string) =>
        Codec.parse(json).pipe(Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value)))
      const decodeStoredReceipt = (
        row: typeof Rows.ServerReceiptRow.Type,
        envelope: Protocol.MutationEnvelope
      ) =>
        Effect.gen(function*() {
          const receipt = yield* decodeReceipt(row.receipt_json)
          if (
            row.space_id !== envelope.spaceId ||
            row.client_id !== envelope.clientId ||
            row.local_sequence !== envelope.localSequence ||
            row.mutation_id !== envelope.mutationId ||
            receipt.spaceId !== row.space_id ||
            receipt.clientId !== row.client_id ||
            receipt.localSequence !== row.local_sequence ||
            receipt.mutationId !== row.mutation_id ||
            receipt.sourceSchema.version !== row.source_schema_version ||
            receipt.sourceSchema.hash !== row.source_schema_hash ||
            (receipt._tag !== "Legacy" && (
              receipt.mutationVersion !== row.mutation_version || receipt.name !== row.mutation_name
            )) ||
            (receipt._tag === "Accepted"
              ? row.server_sequence === null || receipt.serverSequence !== row.server_sequence
              : receipt._tag === "Rejected"
              ? row.server_sequence !== null
              : receipt.serverSequence !== row.server_sequence)
          ) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Durable receipt ${envelope.mutationId} conflicts with its SQL identity`
            })
          }
          return receipt
        })
      const receiptCapacityRejection = {
        _tag: "CapacityExceeded" as const,
        resource: "receipt bytes",
        limit: Protocol.maximumReceiptBytes
      }
      const rejectedReceipt = (
        envelope: Protocol.MutationEnvelope,
        mutation: MutationRuntime.CurrentMutationView,
        rejection: Schema.Json,
        origin: Protocol.RejectionOrigin
      ) =>
        Effect.gen(function*() {
          const receipt = Protocol.RejectedReceipt.make({
            spaceId: envelope.spaceId,
            clientId: envelope.clientId,
            mutationId: envelope.mutationId,
            localSequence: envelope.localSequence,
            name: mutation.name,
            sourceSchema: options.definition.schemaIdentity,
            mutationVersion: mutation.mutationVersion,
            origin,
            rejection
          })
          return (yield* Protocol.encodedBytesEffect(receipt)) <= Protocol.maximumReceiptBytes
            ? receipt
            : Protocol.RejectedReceipt.make({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              mutationId: envelope.mutationId,
              localSequence: envelope.localSequence,
              name: mutation.name,
              sourceSchema: options.definition.schemaIdentity,
              mutationVersion: mutation.mutationVersion,
              origin: "Capacity",
              rejection: receiptCapacityRejection
            })
        })
      const authorizeAccess = (envelope: Protocol.MutationEnvelope, principal: typeof Schema.Json.Type) =>
        options.authorizeAccess({
          spaceId: envelope.spaceId,
          clientId: envelope.clientId,
          principal
        }).pipe(
          Effect.provide(context),
          Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
        )
      const authorizeRead = (spaceId: Identity.SpaceId, principal: typeof Schema.Json.Type) =>
        options.authorizeRead({ spaceId, principal }).pipe(
          Effect.provide(context),
          Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
        )

      const validateCallerSchema = (schema: Identity.SchemaIdentity) =>
        schema.version === options.definition.schemaIdentity.version &&
          schema.hash === options.definition.schemaIdentity.hash
          ? Effect.void
          : Effect.fail(
            new ReplicaError.StaleSchema({
              expectedVersion: options.definition.schemaIdentity.version,
              expectedHash: options.definition.schemaIdentity.hash,
              actualVersion: schema.version,
              actualHash: schema.hash
            })
          )

      const validateStoredSpace = (space: typeof Rows.ServerMetaRow.Type) => {
        if (
          space.schema_version !== options.definition.schemaIdentity.version ||
          space.schema_hash !== options.definition.schemaIdentity.hash ||
          space.target_schema_version !== null || space.target_schema_hash !== null ||
          space.migration_hash !== null
        ) {
          return Effect.fail(
            new ReplicaError.StaleSchema({
              expectedVersion: options.definition.schemaIdentity.version,
              expectedHash: options.definition.schemaIdentity.hash,
              actualVersion: space.schema_version,
              actualHash: space.schema_hash
            })
          )
        }
        return space.definition_hash === options.definition.hash
          ? Effect.void
          : Effect.fail(
            new ReplicaError.DefinitionMismatch({
              expected: options.definition.hash,
              actual: space.definition_hash
            })
          )
      }

      const prepareSpace = (spaceId: Identity.SpaceId, schema: Identity.SchemaIdentity) =>
        Effect.gen(function*() {
          yield* validateCallerSchema(schema)
          yield* sql`INSERT INTO effect_local_server_spaces
            (space_id, definition_hash, next_server_sequence, schema_version, schema_hash, schema_generation)
            VALUES (${spaceId}, ${options.definition.hash}, 1, ${options.definition.schemaIdentity.version},
              ${options.definition.schemaIdentity.hash}, 0) ON CONFLICT (space_id) DO NOTHING`
          yield* SchemaEvolution.server({
            definition: options.definition,
            evolution,
            spaceId,
            ...(options.schemaEvolutionBatchSize === undefined
              ? {}
              : { batchSize: options.schemaEvolutionBatchSize })
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const admit = (request: Protocol.SubmitRequest, principal: typeof Schema.Json.Type) => {
        const { envelope } = request
        return Effect.gen(function*() {
          yield* authorizeAccess(envelope, principal)
          yield* prepareSpace(envelope.spaceId, request.schema)
          if ((yield* Protocol.encodedBytesEffect(envelope)) > Protocol.maximumMutationBytes) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "mutation bytes",
              limit: Protocol.maximumMutationBytes
            })
          }
          const { digest, ...identity } = envelope
          const expectedDigest = yield* Protocol.mutationDigest(identity).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
          if (digest !== expectedDigest) {
            return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
          }
          const mutation = yield* runtime.prepare(envelope)
          return yield* sql.withTransaction(Effect.gen(function*() {
            const existingByMutation = yield* findReceiptByMutation({
              spaceId: envelope.spaceId,
              mutationId: envelope.mutationId
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(existingByMutation)) {
              if (existingByMutation.value.digest !== envelope.digest) {
                return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
              }
              return yield* decodeStoredReceipt(existingByMutation.value, envelope).pipe(
                Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution))
              )
            }
            const existingBySequence = yield* findReceiptBySequence({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              localSequence: envelope.localSequence
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(existingBySequence)) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
            }

            yield* sql`INSERT INTO effect_local_server_clients (space_id, client_id, last_local_sequence)
          VALUES (${envelope.spaceId}, ${envelope.clientId}, 0)
          ON CONFLICT (space_id, client_id) DO NOTHING`
            const storedSpace = yield* lockSpace(envelope.spaceId).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            const client = yield* lockClient({ spaceId: envelope.spaceId, clientId: envelope.clientId }).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            const committedByMutation = yield* findReceiptByMutation({
              spaceId: envelope.spaceId,
              mutationId: envelope.mutationId
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(committedByMutation)) {
              if (committedByMutation.value.digest !== envelope.digest) {
                return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
              }
              return yield* decodeStoredReceipt(committedByMutation.value, envelope).pipe(
                Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution))
              )
            }
            const committedBySequence = yield* findReceiptBySequence({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              localSequence: envelope.localSequence
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(committedBySequence)) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
            }
            yield* validateStoredSpace(storedSpace)
            const expected = client.last_local_sequence + 1
            if (envelope.localSequence !== expected) {
              return yield* new ReplicaError.OutOfOrderMutation({ expected, actual: envelope.localSequence })
            }

            const authorization = yield* options.authorizeMutation({ mutation, principal }).pipe(
              Effect.provide(context),
              Effect.result
            )
            let receipt: Protocol.Receipt
            if (Result.isFailure(authorization)) {
              receipt = yield* rejectedReceipt(envelope, mutation, authorization.failure, "Authorization")
            } else {
              const changes: Array<Protocol.EntityChange> = []
              const executed = yield* sql.withTransaction(
                runtime.execute(
                  mutation.name,
                  mutation.payload,
                  SqlTransaction.server({ sql, definition: options.definition, spaceId: envelope.spaceId, changes }),
                  changes
                ).pipe(
                  Effect.flatMap((result) =>
                    Effect.gen(function*() {
                      if (Result.isFailure(result)) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Mutation",
                          rejection: result.failure
                        })
                      }
                      const sequence = Identity.ServerSequence.make(storedSpace.next_server_sequence)
                      const entry = Protocol.AcceptedMutation.make({
                        sequence,
                        spaceId: envelope.spaceId,
                        clientId: envelope.clientId,
                        mutationId: envelope.mutationId,
                        localSequence: envelope.localSequence,
                        sourceSchema: options.definition.schemaIdentity,
                        digest: envelope.digest,
                        changes: result.success.changes
                      })
                      const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                      const pageBytes = yield* Protocol.encodedBytesEffect({ entries: [entry], hasMore: false })
                      if (pageBytes > Protocol.maximumBatchBytes) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: {
                            _tag: "CapacityExceeded",
                            resource: "accepted mutation bytes",
                            limit: Protocol.maximumBatchBytes
                          }
                        })
                      }
                      const receipt = Protocol.AcceptedReceipt.make({
                        spaceId: envelope.spaceId,
                        clientId: envelope.clientId,
                        mutationId: envelope.mutationId,
                        localSequence: envelope.localSequence,
                        name: mutation.name,
                        sourceSchema: options.definition.schemaIdentity,
                        mutationVersion: mutation.mutationVersion,
                        serverSequence: sequence,
                        result: result.success.result
                      })
                      if ((yield* Protocol.encodedBytesEffect(receipt)) > Protocol.maximumReceiptBytes) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: receiptCapacityRejection
                        })
                      }
                      return {
                        entry,
                        entryBytes,
                        entryJson: yield* Codec.stringify(entry),
                        receipt
                      }
                    })
                  )
                )
              ).pipe(
                Effect.map(Result.succeed),
                Effect.catchTag("TerminalRejection", (terminal) => Effect.succeed(Result.fail(terminal)))
              )
              if (Result.isFailure(executed)) {
                receipt = yield* rejectedReceipt(
                  envelope,
                  mutation,
                  executed.failure.rejection,
                  executed.failure.origin
                )
              } else {
                const { entry, entryBytes, entryJson } = executed.success
                const sequence = entry.sequence
                yield* sql`UPDATE effect_local_server_spaces
              SET next_server_sequence = next_server_sequence + 1 WHERE space_id = ${envelope.spaceId}`
                yield* sql`INSERT INTO effect_local_authoritative_log
              (space_id, server_sequence, mutation_id, entry_bytes, entry_json,
                source_schema_version, source_schema_hash, mutation_version)
              VALUES (${envelope.spaceId}, ${sequence}, ${envelope.mutationId}, ${entryBytes}, ${entryJson},
                ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
                ${mutation.mutationVersion})`
                receipt = executed.success.receipt
              }
            }
            yield* sql`INSERT INTO effect_local_server_receipts
          (space_id, client_id, local_sequence, mutation_id, digest, receipt_json, digest_version,
            source_schema_version, source_schema_hash, mutation_version, mutation_name, rejection_origin)
          VALUES (${envelope.spaceId}, ${envelope.clientId}, ${envelope.localSequence}, ${envelope.mutationId},
            ${envelope.digest}, ${yield* Codec.stringify(receipt)}, ${envelope.digestVersion},
            ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash}, ${receipt.mutationVersion},
            ${receipt.name}, ${receipt._tag === "Rejected" ? receipt.origin : null})`
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
              ? RcMap.has(wakes, envelope.spaceId).pipe(
                Effect.flatMap((hasWatchers) =>
                  hasWatchers
                    ? Effect.scoped(
                      RcMap.get(wakes, envelope.spaceId).pipe(
                        Effect.flatMap((channel) =>
                          PubSub.publish(channel, {
                            spaceId: envelope.spaceId,
                            sequence: receipt.serverSequence
                          })
                        )
                      )
                    )
                    : Effect.void
                ),
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
      }

      const pull = (request: Protocol.PullRequest) =>
        sql.withTransaction(Effect.gen(function*() {
          const storedSpace = yield* lockSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          yield* validateStoredSpace(storedSpace)
          const metadata = yield* findLogMetadata({ ...request, limit: request.limit + 1 }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          for (let index = 0; index < metadata.length; index++) {
            const expected = request.after + index + 1
            if (metadata[index]!.server_sequence !== expected) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Authoritative log expected sequence ${expected} but found ${metadata[index]!.server_sequence}`
              })
            }
          }
          let pageBytes = yield* Protocol.encodedBytesEffect({ entries: [], hasMore: false })
          const selectedMetadata: Array<typeof Rows.ServerLogMetadataRow.Type> = []
          for (const row of metadata) {
            if (selectedMetadata.length >= request.limit) break
            const separatorBytes = selectedMetadata.length === 0 ? 0 : 1
            if (pageBytes + separatorBytes + row.entry_bytes > Protocol.maximumBatchBytes) break
            selectedMetadata.push(row)
            pageBytes += separatorBytes + row.entry_bytes
          }
          if (metadata.length > 0 && selectedMetadata.length === 0) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Authoritative entry ${metadata[0]!.server_sequence} exceeds the pull byte limit`
            })
          }
          const through = selectedMetadata.at(-1)?.server_sequence ?? request.after
          const rows = yield* findLogEntries({ spaceId: request.spaceId, after: request.after, through }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          if (rows.length !== selectedMetadata.length) {
            return yield* new ReplicaError.StorageCorrupt({
              message: "Authoritative log entries do not match their durable receipt metadata"
            })
          }
          const entries: Array<Protocol.AcceptedMutation> = []
          for (let index = 0; index < rows.length; index++) {
            const row = rows[index]!
            const metadataRow = selectedMetadata[index]!
            const entry = yield* Codec.parse(row.entry_json).pipe(
              Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
            )
            const encodedBytes = yield* Protocol.encodedBytesEffect(entry)
            if (
              row.space_id !== request.spaceId ||
              row.server_sequence !== metadataRow.server_sequence ||
              row.server_sequence !== entry.sequence ||
              row.mutation_id !== entry.mutationId ||
              row.entry_bytes !== metadataRow.entry_bytes ||
              row.entry_bytes !== encodedBytes ||
              row.receipt_client_id !== entry.clientId ||
              row.receipt_local_sequence !== entry.localSequence ||
              row.digest !== entry.digest ||
              row.source_schema_version !== entry.sourceSchema.version ||
              row.source_schema_hash !== entry.sourceSchema.hash ||
              entry.spaceId !== request.spaceId
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Authoritative entry ${row.server_sequence} conflicts with durable metadata`
              })
            }
            entries.push(entry)
          }
          const page = Protocol.PullPage.make({
            entries,
            hasMore: metadata.length > entries.length
          })
          if ((yield* Protocol.encodedBytesEffect(page)) > Protocol.maximumBatchBytes) {
            return yield* new ReplicaError.StorageCorrupt({
              message: "Authoritative pull page exceeds its encoded byte limit"
            })
          }
          return page
        })).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("ServerStore.pull", {
            attributes: {
              "space.id": request.spaceId,
              "server.after": request.after,
              "page.limit": request.limit
            }
          })
        )
      const watch = (request: Protocol.WatchRequest, principal: typeof Schema.Json.Type) =>
        Stream.unwrap(
          RcMap.get(wakes, request.spaceId).pipe(
            Effect.flatMap((channel) => PubSub.subscribe(channel)),
            Effect.flatMap((subscription) =>
              findSpace(request.spaceId).pipe(
                Effect.mapError(StorageUnavailable.make),
                Effect.flatMap((stored) =>
                  Effect.gen(function*() {
                    if (Option.isSome(stored)) yield* validateStoredSpace(stored.value)
                    const sequence = Identity.ServerSequence.make(
                      Option.isSome(stored) ? stored.value.next_server_sequence - 1 : 0
                    )
                    return Stream.succeed({ spaceId: request.spaceId, sequence } satisfies Protocol.Wake).pipe(
                      Stream.concat(Stream.fromSubscription(subscription)),
                      Stream.mapEffect((wake) =>
                        authorizeRead(request.spaceId, principal).pipe(
                          Effect.andThen(
                            sql.withTransaction(Effect.gen(function*() {
                              const current = yield* lockSpace(request.spaceId).pipe(
                                Effect.mapError(StorageUnavailable.make)
                              )
                              yield* validateStoredSpace(current)
                              return wake
                            })).pipe(
                              Effect.catchIf(
                                SqlError.isSqlError,
                                (cause) => Effect.fail(StorageUnavailable.make(cause))
                              )
                            )
                          )
                        )
                      )
                    )
                  })
                )
              )
            )
          )
        )

      const trustedSubmitRequest = (
        request: Protocol.SubmitRequest | Protocol.MutationEnvelope
      ): Protocol.SubmitRequest =>
        Schema.is(Protocol.MutationEnvelope)(request)
          ? { envelope: request, schema: options.definition.schemaIdentity }
          : request
      const trustedPullRequest = (
        request: Protocol.PullRequest | Omit<Protocol.PullRequest, "schema">
      ): Protocol.PullRequest =>
        Schema.is(Protocol.PullRequest)(request)
          ? request
          : { ...request, schema: options.definition.schemaIdentity }
      const trustedWatchRequest = (
        request: Protocol.WatchRequest | Identity.SpaceId
      ): Protocol.WatchRequest =>
        typeof request === "string"
          ? { spaceId: request, schema: options.definition.schemaIdentity }
          : request

      return ServerStore.of({
        submit: (request) => admit(trustedSubmitRequest(request), null),
        admit,
        pull: (input) => {
          const request = trustedPullRequest(input)
          return authorizeRead(request.spaceId, null).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.andThen(pull(request))
          )
        },
        pullAuthorized: (request, principal) =>
          authorizeRead(request.spaceId, principal).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.andThen(pull(request))
          ),
        watch: (input) => {
          const request = trustedWatchRequest(input)
          return Stream.unwrap(
            authorizeRead(request.spaceId, null).pipe(
              Effect.andThen(prepareSpace(request.spaceId, request.schema)),
              Effect.as(watch(request, null))
            )
          )
        },
        watchAuthorized: (request, principal) =>
          authorizeRead(request.spaceId, principal).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.as(watch(request, principal))
          )
      })
    })
  )

export const layerTrusted = (options: {
  readonly definition: Definition.Any
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly wakeCapacity?: number
}): Layer.Layer<
  ServerStore,
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto | MutationRuntime.MutationRuntime
> =>
  layer({
    ...options,
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () => Effect.void
  })
