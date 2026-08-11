import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import type * as Duration from "effect/Duration"
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
import * as Configuration from "./internal/configuration.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as TerminalRejection from "./internal/TerminalRejection.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"
import * as SchemaEvolution from "./SchemaEvolution.js"

export interface HistoryOptions {
  readonly migration: Migrations.Options
  readonly retainedHistoryEntries: number
  readonly maximumHistoryEntries: number
  readonly retainedReceipts: number
  readonly maximumReceipts: number
  readonly maximumSnapshotEntities: number
  readonly maximumSnapshotBytes: number
  readonly maximumBootstrapPageBytes: number
  readonly pruneBatchSize: number
  readonly retainedSnapshots: number
  readonly maintenanceConcurrency: number
  readonly maintenanceSpaceBatchSize: number
}

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
  ) => Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError>
  readonly pullAuthorized: (
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError>
  readonly watch: (
    request: Protocol.WatchRequest | Identity.SpaceId
  ) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
  readonly bootstrap: (
    request: Protocol.BootstrapRequest | Omit<Protocol.BootstrapRequest, "schema">
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly bootstrapAuthorized: (
    request: Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly maintain: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly maintainAll: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watchAuthorized: (
    request: Protocol.WatchRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>, ReplicaError.ReplicaError>
}

export class ServerStore extends Context.Service<ServerStore, Service>()(
  "@lucas-barake/effect-local-sql/ServerStore"
) {}

export interface Options<R = never,> extends HistoryOptions {
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

type NumericHistoryOption = Exclude<keyof HistoryOptions, "migration">

const nonNegativeOptions: ReadonlyArray<NumericHistoryOption> = ["retainedHistoryEntries", "retainedReceipts"]
const positiveOptions: ReadonlyArray<NumericHistoryOption> = [
  "maximumHistoryEntries",
  "maximumReceipts",
  "maximumSnapshotEntities",
  "maximumSnapshotBytes",
  "maximumBootstrapPageBytes",
  "pruneBatchSize",
  "retainedSnapshots",
  "maintenanceConcurrency",
  "maintenanceSpaceBatchSize"
]

const validateOptions = (options: HistoryOptions) =>
  Effect.gen(function*() {
    for (const option of nonNegativeOptions) {
      if (!Number.isSafeInteger(options[option]) || options[option] < 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a nonnegative safe integer`
        })
      }
    }
    for (const option of positiveOptions) {
      if (!Number.isSafeInteger(options[option]) || options[option] <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a positive safe integer`
        })
      }
    }
    if (options.retainedHistoryEntries >= options.maximumHistoryEntries) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "retainedHistoryEntries",
        message: "retainedHistoryEntries must be less than maximumHistoryEntries"
      })
    }
    if (options.retainedReceipts >= options.maximumReceipts) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "retainedReceipts",
        message: "retainedReceipts must be less than maximumReceipts"
      })
    }
    if (options.maximumBootstrapPageBytes > Protocol.maximumBatchBytes) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "maximumBootstrapPageBytes",
        message: `maximumBootstrapPageBytes must not exceed ${Protocol.maximumBatchBytes}`
      })
    }
    return yield* Effect.void
  })

const LegacySpaceRow = Schema.Struct({
  definition_hash: Schema.String,
  legacy_schema_version: Schema.NullOr(Identity.SchemaVersion),
  legacy_schema_hash: Schema.NullOr(Identity.SchemaHash)
})

export const layer = <R = never,>(options: Options<R>): Layer.Layer<
  ServerStore,
  ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto | MutationRuntime.MutationRuntime | R
> =>
  Layer.effect(
    ServerStore,
    Effect.gen(function*() {
      yield* validateOptions(options)
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
      yield* Migrations.server(options.migration)

      const wakes = yield* RcMap.make({
        lookup: (_spaceId: Identity.SpaceId) =>
          Effect.acquireRelease(PubSub.sliding<Protocol.Wake>(wakeCapacity), PubSub.shutdown)
      })
      const findReceiptByMutation = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, mutationId: Identity.MutationId }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, mutationId }) =>
          sql`SELECT r.space_id, r.client_id, r.local_sequence, r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, r.terminal_sequence, r.server_sequence
        FROM effect_local_server_receipts AS r
        WHERE r.space_id = ${spaceId} AND r.mutation_id = ${mutationId}`
      })
      const findReceiptBySequence = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          clientId: Identity.ClientId,
          localSequence: Identity.LocalSequence
        }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, clientId, localSequence }) =>
          sql`SELECT r.space_id, r.client_id, r.local_sequence, r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, r.terminal_sequence, r.server_sequence
        FROM effect_local_server_receipts AS r
        WHERE r.space_id = ${spaceId} AND r.client_id = ${clientId} AND r.local_sequence = ${localSequence}`
      })
      const lockClient = SqlSchema.findOne({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, clientId: Identity.ClientId }),
        Result: Rows.ServerClientRow,
        execute: ({ spaceId, clientId }) =>
          sql`UPDATE effect_local_server_clients
        SET last_local_sequence = last_local_sequence
        WHERE space_id = ${spaceId} AND client_id = ${clientId}
        RETURNING last_local_sequence, expired_local_sequence`
      })
      const lockSpace = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`UPDATE effect_local_server_spaces
        SET next_server_sequence = next_server_sequence
        WHERE space_id = ${spaceId}
        RETURNING definition_hash, schema_version, schema_hash, schema_generation,
          target_schema_version, target_schema_hash, migration_hash, next_server_sequence,
          next_terminal_sequence, history_floor,
            receipt_floor, retained_history_count, retained_receipt_count, entity_count, entity_bytes,
            snapshot_id, snapshot_sequence, snapshot_terminal_sequence, metadata_verified`
      })
      const findSpace = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`SELECT definition_hash, schema_version, schema_hash, schema_generation,
            target_schema_version, target_schema_hash, migration_hash,
            next_server_sequence, next_terminal_sequence, history_floor,
            receipt_floor, retained_history_count, retained_receipt_count, entity_count, entity_bytes,
            snapshot_id, snapshot_sequence, snapshot_terminal_sequence, metadata_verified
          FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
      })
      const findSpaces = SqlSchema.findAll({
        Request: Schema.Struct({ after: Schema.String, limit: Schema.Int }),
        Result: Rows.SpaceIdRow,
        execute: ({ after, limit }) =>
          sql`SELECT space_id FROM effect_local_server_spaces
          WHERE space_id > ${after} ORDER BY space_id LIMIT ${limit}`
      })
      const countHistory = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.CountRow,
        execute: (spaceId) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_authoritative_log WHERE space_id = ${spaceId}`
      })
      const countReceipts = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.CountRow,
        execute: (spaceId) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_server_receipts WHERE space_id = ${spaceId}`
      })
      const findSpaceCounts = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.ServerCountRow,
        execute: (spaceId) =>
          sql`SELECT history_count, receipt_count FROM effect_local_server_space_counts
          WHERE space_id = ${spaceId}`
      })
      const findEntities = SqlSchema.findAll({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, limit: Schema.Int }),
        Result: Rows.ServerEntityRow,
        execute: ({ spaceId, limit }) =>
          sql`SELECT model, model_version, entity_key, value_json, entity_bytes
          FROM effect_local_server_entities WHERE space_id = ${spaceId}
          ORDER BY model, entity_key LIMIT ${limit}`
      })
      const findLargestEntity = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: Rows.ServerEntityRow,
        execute: (spaceId) =>
          sql`SELECT model, model_version, entity_key, value_json, entity_bytes
          FROM effect_local_server_entities WHERE space_id = ${spaceId}
          ORDER BY entity_bytes DESC, model, entity_key LIMIT 1`
      })
      const findLogMetadata = SqlSchema.findAll({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, after: Identity.ServerSequence, limit: Schema.Int }),
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
          sql`SELECT space_id, server_sequence, client_id, local_sequence, mutation_id, digest,
            entry_bytes, entry_json, source_schema_version, source_schema_hash
          FROM effect_local_authoritative_log
          WHERE space_id = ${spaceId} AND server_sequence > ${after} AND server_sequence <= ${through}
          ORDER BY server_sequence`
      })
      const findSnapshot = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, snapshotId: Identity.SnapshotId }),
        Result: Rows.SnapshotManifestRow,
        execute: ({ spaceId, snapshotId }) =>
          sql`SELECT space_id, snapshot_id, definition_hash, schema_version, schema_hash,
            server_sequence, terminal_sequence,
            entity_count, content_bytes, digest
          FROM effect_local_server_snapshots
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}`
      })
      const findSnapshotEntityMetadata = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          after: Schema.Int,
          limit: Schema.Int
        }),
        Result: Rows.SnapshotEntityMetadataRow,
        execute: ({ spaceId, snapshotId, after, limit }) =>
          sql`SELECT ordinal, wire_bytes
          FROM effect_local_server_snapshot_entities
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId} AND ordinal > ${after}
          ORDER BY ordinal LIMIT ${limit}`
      })
      const findSnapshotEntityWire = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          after: Schema.Int,
          through: Schema.Int
        }),
        Result: Rows.SnapshotEntityWireRow,
        execute: ({ spaceId, snapshotId, after, through }) =>
          sql`SELECT ordinal, wire_json, wire_bytes
          FROM effect_local_server_snapshot_entities
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}
            AND ordinal > ${after} AND ordinal <= ${through}
          ORDER BY ordinal`
      })
      const findHistoryPrune = SqlSchema.findAll({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, through: Identity.ServerSequence, limit: Schema.Int }),
        Result: Rows.SequenceRow,
        execute: ({ spaceId, through, limit }) =>
          sql`SELECT server_sequence FROM effect_local_authoritative_log
          WHERE space_id = ${spaceId} AND server_sequence <= ${through}
          ORDER BY server_sequence LIMIT ${limit}`
      })
      const findReceiptPrune = SqlSchema.findAll({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, through: Identity.TerminalSequence, limit: Schema.Int }),
        Result: Rows.TerminalReceiptIdentityRow,
        execute: ({ spaceId, through, limit }) =>
          sql`SELECT terminal_sequence, client_id, local_sequence FROM effect_local_server_receipts
          WHERE space_id = ${spaceId} AND terminal_sequence <= ${through}
          ORDER BY terminal_sequence LIMIT ${limit}`
      })
      const findLegacySpace = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: LegacySpaceRow,
        execute: (spaceId) =>
          sql`SELECT definition_hash, legacy_schema_version, legacy_schema_hash
          FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
      })
      const resolveLegacyEnvelope = (envelope: Protocol.MutationEnvelope) =>
        Effect.gen(function*() {
          if (envelope.digestVersion !== 1) return envelope
          const stored = yield* findLegacySpace(envelope.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          let sourceDefinition: Definition.Any | undefined
          if (Option.isSome(stored)) {
            const incompleteBaseline = (stored.value.legacy_schema_version === null) !==
              (stored.value.legacy_schema_hash === null)
            if (incompleteBaseline) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${envelope.spaceId} has incomplete legacy schema metadata`
              })
            }
            if (stored.value.legacy_schema_version !== null && stored.value.legacy_schema_hash !== null) {
              sourceDefinition = evolution.definitionByIdentity.get(
                `${stored.value.legacy_schema_version}:${stored.value.legacy_schema_hash}`
              )
              if (sourceDefinition === undefined) {
                return yield* new ReplicaError.ProtocolInvalid({
                  message:
                    `Persisted legacy schema ${stored.value.legacy_schema_version}:${stored.value.legacy_schema_hash} is not configured`
                })
              }
            } else {
              sourceDefinition = evolution.legacyBaselineByHash.get(stored.value.definition_hash)?.definition
            }
          }
          if (sourceDefinition === undefined) {
            const identities = new Map<string, Definition.Any>()
            for (const baseline of evolution.legacyBaselines) {
              identities.set(
                `${baseline.definition.schemaIdentity.version}:${baseline.definition.schemaIdentity.hash}`,
                baseline.definition
              )
            }
            if (identities.size !== 1) {
              return yield* new ReplicaError.ProtocolInvalid({
                message: "Legacy mutation digest has no unambiguous configured schema baseline"
              })
            }
            sourceDefinition = identities.values().next().value
          }
          const mutation = sourceDefinition?.mutationByName.get(envelope.name)
          if (sourceDefinition === undefined || mutation === undefined) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Legacy mutation ${envelope.name} has no configured schema baseline`
            })
          }
          return Protocol.MutationEnvelope.make({
            ...envelope,
            sourceSchema: sourceDefinition.schemaIdentity,
            mutationVersion: mutation.version
          })
        })

      const manifestFromRow = (row: typeof Rows.SnapshotManifestRow.Type) =>
        Protocol.SnapshotManifest.make({
          spaceId: row.space_id,
          definitionHash: row.definition_hash,
          schema: Identity.SchemaIdentity.make({ version: row.schema_version, hash: row.schema_hash }),
          snapshotId: row.snapshot_id,
          sequence: row.server_sequence,
          terminalSequenceThrough: row.terminal_sequence,
          entityCount: row.entity_count,
          contentBytes: row.content_bytes,
          digest: row.digest
        })
      const currentManifest = (spaceId: Identity.SpaceId, meta: typeof Rows.ServerMetaRow.Type) =>
        Effect.gen(function*() {
          if (meta.snapshot_id === null) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Space ${spaceId} has compacted history without a published snapshot`
            })
          }
          const stored = yield* findSnapshot({ spaceId, snapshotId: meta.snapshot_id }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          if (Option.isNone(stored)) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Space ${spaceId} points to missing snapshot ${meta.snapshot_id}`
            })
          }
          const manifest = manifestFromRow(stored.value)
          if (
            manifest.definitionHash !== meta.definition_hash ||
            manifest.schema.version !== meta.schema_version ||
            manifest.schema.hash !== meta.schema_hash ||
            manifest.sequence !== meta.snapshot_sequence ||
            manifest.terminalSequenceThrough !== meta.snapshot_terminal_sequence
          ) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Space ${spaceId} snapshot pointer conflicts with its manifest`
            })
          }
          return manifest
        })
      const decodeReceipt = (json: string) =>
        Codec.parse(json).pipe(Effect.flatMap((value) => Codec.decode(Protocol.Receipt, value)))
      const decodeStoredReceipt = (row: typeof Rows.ServerReceiptRow.Type, envelope: Protocol.MutationEnvelope) =>
        Effect.gen(function*() {
          const receipt = yield* decodeReceipt(row.receipt_json)
          if (receipt._tag === "Expired") {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Durable receipt ${envelope.mutationId} cannot be expired`
            })
          }
          let receiptSequenceMismatch = row.server_sequence !== null
          if (receipt._tag === "Accepted") {
            receiptSequenceMismatch = row.server_sequence === null || receipt.serverSequence !== row.server_sequence
          }
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
              : receipt.serverSequence !== row.server_sequence) ||
            ((receipt._tag === "Accepted" || receipt._tag === "Rejected") &&
              receipt.terminalSequence !== undefined && receipt.terminalSequence !== row.terminal_sequence) ||
            receiptSequenceMismatch
          ) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Durable receipt ${envelope.mutationId} conflicts with its SQL identity`
            })
          }
          return { ...receipt, terminalSequence: row.terminal_sequence }
        })
      const receiptCapacityRejection: {
        readonly _tag: "CapacityExceeded"
        readonly resource: string
        readonly limit: number
      } = {
        _tag: "CapacityExceeded",
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
          if ((yield* Protocol.encodedBytesEffect(receipt)) <= Protocol.maximumReceiptBytes) return receipt
          return Protocol.RejectedReceipt.make({
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
            (space_id, definition_hash, next_server_sequence, schema_version, schema_hash, schema_generation,
              next_terminal_sequence, history_floor, receipt_floor, retained_history_count,
              retained_receipt_count, entity_count, entity_bytes, snapshot_sequence,
              snapshot_terminal_sequence, metadata_verified)
            VALUES (${spaceId}, ${options.definition.hash}, 1, ${options.definition.schemaIdentity.version},
              ${options.definition.schemaIdentity.hash}, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1)
            ON CONFLICT (space_id) DO NOTHING`
          yield* sql`INSERT INTO effect_local_server_space_counts (space_id, history_count, receipt_count)
            VALUES (${spaceId}, 0, 0) ON CONFLICT (space_id) DO NOTHING`
          yield* SchemaEvolution.server({
            definition: options.definition,
            evolution,
            spaceId,
            ...(options.schemaEvolutionBatchSize === undefined
              ? {}
              : { batchSize: options.schemaEvolutionBatchSize })
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const bootstrapEntityFits = (spaceId: Identity.SpaceId, entity: Protocol.SnapshotEntity) =>
        Protocol.encodedBytesEffect(Protocol.BootstrapPage.make({
          manifest: Protocol.SnapshotManifest.make({
            spaceId,
            definitionHash: options.definition.hash,
            schema: options.definition.schemaIdentity,
            snapshotId: Identity.SnapshotId.make("snp_ffffffff-ffff-4fff-bfff-ffffffffffff"),
            sequence: Identity.ServerSequence.make(Number.MAX_SAFE_INTEGER),
            terminalSequenceThrough: Identity.TerminalSequence.make(Number.MAX_SAFE_INTEGER),
            entityCount: options.maximumSnapshotEntities,
            contentBytes: options.maximumSnapshotBytes,
            digest: Protocol.SnapshotDigest.make("f".repeat(64))
          }),
          entities: [entity],
          hasMore: true
        })).pipe(Effect.map((bytes) => bytes <= options.maximumBootstrapPageBytes))

      const decodeEntityRows = (spaceId: Identity.SpaceId, rows: ReadonlyArray<typeof Rows.ServerEntityRow.Type>) =>
        Effect.gen(function*() {
          if (rows.length > options.maximumSnapshotEntities) {
            return yield* new ReplicaError.CapacityExceeded({
              resource: "snapshot entities",
              limit: options.maximumSnapshotEntities
            })
          }
          const entities: Array<Protocol.SnapshotEntity> = []
          let contentBytes = 0
          let digest = Protocol.initialSnapshotDigest
          for (let ordinal = 0; ordinal < rows.length; ordinal++) {
            const row = rows[ordinal]
            const model = options.definition.modelByName.get(row.model)
            if (model === undefined) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${spaceId} contains unknown model ${row.model}`
              })
            }
            const key = yield* Codec.parse(row.entity_key)
            const value = yield* Codec.parse(row.value_json)
            yield* Codec.decode(model.key, key)
            yield* Codec.decode(model.schema, value)
            if (
              (yield* Codec.stringify(key)) !== row.entity_key || (yield* Codec.stringify(value)) !== row.value_json
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${spaceId} contains a noncanonical entity row`
              })
            }
            if (row.model_version !== model.version) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${spaceId} contains stale model version for ${row.model}`
              })
            }
            const entityBytes = yield* Protocol.encodedBytesEffect({
              model: row.model,
              modelVersion: row.model_version,
              key,
              value
            })
            if (row.entity_bytes !== 0 && row.entity_bytes !== entityBytes) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${spaceId} entity byte metadata is inconsistent`
              })
            }
            const entity = Protocol.SnapshotEntity.make({
              ordinal,
              model: row.model,
              modelVersion: row.model_version,
              key,
              value,
              entityBytes
            })
            if (!(yield* bootstrapEntityFits(spaceId, entity))) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "bootstrap entity bytes",
                limit: options.maximumBootstrapPageBytes
              })
            }
            contentBytes += entityBytes
            if (contentBytes > options.maximumSnapshotBytes) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "snapshot bytes",
                limit: options.maximumSnapshotBytes
              })
            }
            digest = Protocol.SnapshotDigest.make(
              yield* Canonical.digest({ previous: digest, entity }).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              )
            )
            entities.push(entity)
          }
          return { entities, contentBytes, digest }
        })

      const repairLockedSpace = (spaceId: Identity.SpaceId, meta: typeof Rows.ServerMetaRow.Type) =>
        Effect.gen(function*() {
          if (meta.definition_hash !== options.definition.hash) {
            return yield* new ReplicaError.DefinitionMismatch({
              expected: options.definition.hash,
              actual: meta.definition_hash
            })
          }
          const rows = yield* findEntities({
            spaceId,
            limit: options.maximumSnapshotEntities + 1
          }).pipe(Effect.mapError(StorageUnavailable.make))
          const decoded = yield* decodeEntityRows(spaceId, rows)
          const history = yield* countHistory(spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          const receipts = yield* countReceipts(spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          for (let index = 0; index < rows.length; index++) {
            const row = rows[index]
            const entity = decoded.entities[index]
            if (row.entity_bytes !== entity.entityBytes) {
              yield* sql`UPDATE effect_local_server_entities SET entity_bytes = ${entity.entityBytes}
                WHERE space_id = ${spaceId} AND model = ${row.model} AND entity_key = ${row.entity_key}`
            }
          }
          yield* sql`UPDATE effect_local_server_spaces SET
            retained_history_count = ${history.count},
            retained_receipt_count = ${receipts.count},
            entity_count = ${decoded.entities.length},
            entity_bytes = ${decoded.contentBytes},
            metadata_verified = 1
            WHERE space_id = ${spaceId}`
          yield* sql`UPDATE effect_local_server_space_counts SET
            history_count = ${history.count}, receipt_count = ${receipts.count}
            WHERE space_id = ${spaceId}`
          const repaired: typeof Rows.ServerMetaRow.Type = {
            ...meta,
            retained_history_count: history.count,
            retained_receipt_count: receipts.count,
            entity_count: decoded.entities.length,
            entity_bytes: decoded.contentBytes,
            metadata_verified: 1
          }
          return repaired
        })

      const admit = (request: Protocol.SubmitRequest, principal: typeof Schema.Json.Type) => {
        const submittedEnvelope = request.envelope
        return Effect.gen(function*() {
          yield* authorizeAccess(submittedEnvelope, principal)
          yield* validateCallerSchema(request.schema)
          const exactReceipt = yield* findReceiptByMutation({
            spaceId: submittedEnvelope.spaceId,
            mutationId: submittedEnvelope.mutationId
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(exactReceipt)) {
            if (exactReceipt.value.digest !== submittedEnvelope.digest) {
              return yield* new ReplicaError.MutationIdentityConflict({
                mutationId: submittedEnvelope.mutationId
              })
            }
            return yield* decodeStoredReceipt(exactReceipt.value, submittedEnvelope).pipe(
              Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution))
            )
          }
          const envelope = yield* resolveLegacyEnvelope(submittedEnvelope)
          yield* prepareSpace(envelope.spaceId, request.schema)
          if (envelope.digestVersion === 1) {
            yield* sql`UPDATE effect_local_server_spaces SET
              legacy_schema_version = COALESCE(legacy_schema_version, ${envelope.sourceSchema.version}),
              legacy_schema_hash = COALESCE(legacy_schema_hash, ${envelope.sourceSchema.hash})
              WHERE space_id = ${envelope.spaceId}`
          }
          const sizeInput = envelope.digestVersion === 1
            ? {
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              mutationId: envelope.mutationId,
              localSequence: envelope.localSequence,
              basis: envelope.basis,
              name: envelope.name,
              payload: envelope.payload,
              digest: envelope.digest
            }
            : envelope
          if ((yield* Protocol.encodedBytesEffect(sizeInput)) > Protocol.maximumMutationBytes) {
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
            yield* sql`INSERT INTO effect_local_server_clients
              (space_id, client_id, last_local_sequence, expired_local_sequence)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, 0, 0)
              ON CONFLICT (space_id, client_id) DO NOTHING`
            let storedSpace: typeof Rows.ServerMetaRow.Type = yield* lockSpace(envelope.spaceId).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            if (storedSpace.metadata_verified === 0) {
              storedSpace = yield* repairLockedSpace(envelope.spaceId, storedSpace)
            }
            const verifiedCounts = yield* findSpaceCounts(envelope.spaceId).pipe(
              Effect.mapError(StorageUnavailable.make)
            )
            if (
              storedSpace.retained_history_count !== verifiedCounts.history_count ||
              storedSpace.retained_receipt_count !== verifiedCounts.receipt_count
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Space ${envelope.spaceId} retained row counters are inconsistent`
              })
            }
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
            if (envelope.localSequence <= client.expired_local_sequence) {
              const manifest = yield* currentManifest(envelope.spaceId, storedSpace)
              return Protocol.ExpiredReceipt.make({
                spaceId: envelope.spaceId,
                clientId: envelope.clientId,
                mutationId: envelope.mutationId,
                localSequence: envelope.localSequence,
                name: mutation.name,
                sourceSchema: options.definition.schemaIdentity,
                mutationVersion: mutation.mutationVersion,
                snapshotId: manifest.snapshotId,
                snapshotSequence: manifest.sequence,
                terminalSequenceThrough: manifest.terminalSequenceThrough
              })
            }
            if (envelope.localSequence <= client.last_local_sequence) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Client ${envelope.clientId} is missing retained receipt ${envelope.localSequence}`
              })
            }
            const expected = client.last_local_sequence + 1
            if (envelope.localSequence !== expected) {
              return yield* new ReplicaError.OutOfOrderMutation({ expected, actual: envelope.localSequence })
            }
            if (storedSpace.retained_receipt_count >= options.maximumReceipts) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "server receipts",
                limit: options.maximumReceipts
              })
            }
            if (storedSpace.retained_history_count >= options.maximumHistoryEntries) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "server history",
                limit: options.maximumHistoryEntries
              })
            }
            if (storedSpace.next_terminal_sequence >= Number.MAX_SAFE_INTEGER) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "terminal sequence",
                limit: Number.MAX_SAFE_INTEGER - 1
              })
            }
            if (storedSpace.next_server_sequence >= Number.MAX_SAFE_INTEGER) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "server sequence",
                limit: Number.MAX_SAFE_INTEGER - 1
              })
            }

            const authorization = yield* options.authorizeMutation({ mutation, principal }).pipe(
              Effect.provide(context),
              Effect.result
            )
            let receipt: Protocol.AcceptedReceipt | Protocol.RejectedReceipt
            if (Result.isFailure(authorization)) {
              receipt = {
                ...(yield* rejectedReceipt(envelope, mutation, authorization.failure, "Authorization")),
                terminalSequence: Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
              }
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
                      const state = yield* lockSpace(envelope.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
                      if (state.entity_count > options.maximumSnapshotEntities) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: {
                            _tag: "CapacityExceeded",
                            resource: "snapshot entities",
                            limit: options.maximumSnapshotEntities
                          }
                        })
                      }
                      if (state.entity_bytes > options.maximumSnapshotBytes) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: {
                            _tag: "CapacityExceeded",
                            resource: "snapshot bytes",
                            limit: options.maximumSnapshotBytes
                          }
                        })
                      }
                      const largest = yield* findLargestEntity(envelope.spaceId).pipe(
                        Effect.mapError(StorageUnavailable.make)
                      )
                      if (Option.isSome(largest)) {
                        const row = largest.value
                        const entity = Protocol.SnapshotEntity.make({
                          ordinal: Math.max(0, options.maximumSnapshotEntities - 1),
                          model: row.model,
                          modelVersion: row.model_version,
                          key: yield* Codec.parse(row.entity_key),
                          value: yield* Codec.parse(row.value_json),
                          entityBytes: row.entity_bytes
                        })
                        if (!(yield* bootstrapEntityFits(envelope.spaceId, entity))) {
                          return yield* new TerminalRejection.TerminalRejection({
                            origin: "Capacity",
                            rejection: {
                              _tag: "CapacityExceeded",
                              resource: "bootstrap entity bytes",
                              limit: options.maximumBootstrapPageBytes
                            }
                          })
                        }
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
                      if (
                        (yield* Protocol.encodedBytesEffect({ entries: [entry], hasMore: false })) >
                          Protocol.maximumBatchBytes
                      ) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: {
                            _tag: "CapacityExceeded",
                            resource: "accepted mutation bytes",
                            limit: Protocol.maximumBatchBytes
                          }
                        })
                      }
                      const accepted = Protocol.AcceptedReceipt.make({
                        spaceId: envelope.spaceId,
                        clientId: envelope.clientId,
                        mutationId: envelope.mutationId,
                        localSequence: envelope.localSequence,
                        name: mutation.name,
                        sourceSchema: options.definition.schemaIdentity,
                        mutationVersion: mutation.mutationVersion,
                        serverSequence: sequence,
                        terminalSequence: Identity.TerminalSequence.make(storedSpace.next_terminal_sequence),
                        result: result.success.result
                      })
                      if ((yield* Protocol.encodedBytesEffect(accepted)) > Protocol.maximumReceiptBytes) {
                        return yield* new TerminalRejection.TerminalRejection({
                          origin: "Capacity",
                          rejection: receiptCapacityRejection
                        })
                      }
                      return {
                        entry,
                        entryBytes,
                        entryJson: yield* Codec.stringify(entry),
                        receipt: accepted
                      }
                    })
                  )
                )
              ).pipe(
                Effect.map(Result.succeed),
                Effect.catchTag("TerminalRejection", (terminal) => Effect.succeed(Result.fail(terminal)))
              )
              if (Result.isFailure(executed)) {
                receipt = {
                  ...(yield* rejectedReceipt(
                    envelope,
                    mutation,
                    executed.failure.rejection,
                    executed.failure.origin
                  )),
                  terminalSequence: Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
                }
              } else {
                const { entry, entryBytes, entryJson } = executed.success
                yield* sql`UPDATE effect_local_server_spaces SET
                  next_server_sequence = next_server_sequence + 1
                  WHERE space_id = ${envelope.spaceId}`
                yield* sql`INSERT INTO effect_local_authoritative_log
                  (space_id, server_sequence, client_id, local_sequence, mutation_id, digest, entry_bytes, entry_json,
                    source_schema_version, source_schema_hash, mutation_version)
                  VALUES (${envelope.spaceId}, ${entry.sequence}, ${envelope.clientId}, ${envelope.localSequence},
                    ${envelope.mutationId}, ${envelope.digest}, ${entryBytes}, ${entryJson},
                    ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
                    ${mutation.mutationVersion})`
                receipt = executed.success.receipt
              }
            }
            const terminalSequence = Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
            let receiptServerSequence: Identity.ServerSequence | null = null
            if (receipt._tag === "Accepted") receiptServerSequence = receipt.serverSequence
            yield* sql`INSERT INTO effect_local_server_receipts
              (space_id, client_id, local_sequence, mutation_id, digest, terminal_sequence, server_sequence,
                receipt_json, digest_version, source_schema_version, source_schema_hash, mutation_version,
                mutation_name, rejection_origin)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, ${envelope.localSequence}, ${envelope.mutationId},
                ${envelope.digest}, ${terminalSequence}, ${receiptServerSequence},
                ${yield* Codec.stringify(receipt)}, ${envelope.digestVersion},
                ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash}, ${receipt.mutationVersion},
                ${receipt.name}, ${receipt._tag === "Rejected" ? receipt.origin : null})`
            yield* sql`UPDATE effect_local_server_spaces SET
              next_terminal_sequence = next_terminal_sequence + 1
              WHERE space_id = ${envelope.spaceId}`
            yield* sql`UPDATE effect_local_server_clients SET last_local_sequence = ${envelope.localSequence}
              WHERE space_id = ${envelope.spaceId} AND client_id = ${envelope.clientId}`
            return receipt
          }))
        }).pipe(
          Effect.catchIf(
            SqlError.isSqlError,
            (cause) =>
              Effect.fail(new ReplicaError.UnknownCommitOutcome({ mutationId: submittedEnvelope.mutationId, cause }))
          ),
          Effect.tap((receipt) =>
            receipt._tag === "Accepted"
              ? RcMap.has(wakes, submittedEnvelope.spaceId).pipe(
                Effect.flatMap((hasWatchers) =>
                  hasWatchers
                    ? Effect.scoped(
                      RcMap.get(wakes, submittedEnvelope.spaceId).pipe(
                        Effect.flatMap((channel) =>
                          PubSub.publish(channel, {
                            spaceId: submittedEnvelope.spaceId,
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
              "mutation.name": submittedEnvelope.name,
              "mutation.id": submittedEnvelope.mutationId,
              "space.id": submittedEnvelope.spaceId
            }
          })
        )
      }

      const pull = (request: Protocol.PullRequest) =>
        sql.withTransaction(Effect.gen(function*() {
          const stored = yield* findSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isNone(stored)) return Protocol.PullPage.make({ entries: [], hasMore: false })
          const meta = stored.value
          yield* validateStoredSpace(meta)
          if (request.after < meta.history_floor) {
            return Protocol.BootstrapRequired.make({ manifest: yield* currentManifest(request.spaceId, meta) })
          }
          const head = meta.next_server_sequence - 1
          if (request.after > head) {
            return yield* new ReplicaError.CursorGap({ expected: head, actual: request.after })
          }
          const metadata = yield* findLogMetadata({ ...request, limit: request.limit + 1 }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          for (let index = 0; index < metadata.length; index++) {
            const expected = request.after + index + 1
            if (metadata[index].server_sequence !== expected) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Authoritative log expected sequence ${expected} but found ${metadata[index].server_sequence}`
              })
            }
          }
          let pageBytes = yield* Protocol.encodedBytesEffect({ entries: [], hasMore: false })
          const selected: Array<typeof Rows.ServerLogMetadataRow.Type> = []
          for (const row of metadata) {
            if (selected.length >= request.limit) break
            let separatorBytes = 1
            if (selected.length === 0) separatorBytes = 0
            if (pageBytes + separatorBytes + row.entry_bytes > Protocol.maximumBatchBytes) break
            selected.push(row)
            pageBytes += separatorBytes + row.entry_bytes
          }
          if (metadata.length > 0 && selected.length === 0) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Authoritative entry ${metadata[0].server_sequence} exceeds the pull byte limit`
            })
          }
          const through = selected.at(-1)?.server_sequence ?? request.after
          const rows = yield* findLogEntries({ spaceId: request.spaceId, after: request.after, through }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          if (rows.length !== selected.length) {
            return yield* new ReplicaError.StorageCorrupt({ message: "Authoritative log metadata is incomplete" })
          }
          const entries: Array<Protocol.AcceptedMutation> = []
          for (let index = 0; index < rows.length; index++) {
            const row = rows[index]
            const metadataRow = selected[index]
            const entry = yield* Codec.parse(row.entry_json).pipe(
              Effect.flatMap((value) => Codec.decode(Protocol.AcceptedMutation, value))
            )
            if (
              row.space_id !== request.spaceId ||
              row.server_sequence !== metadataRow.server_sequence ||
              row.server_sequence !== entry.sequence ||
              row.client_id !== entry.clientId ||
              row.local_sequence !== entry.localSequence ||
              row.mutation_id !== entry.mutationId ||
              row.digest !== entry.digest ||
              row.source_schema_version !== entry.sourceSchema.version ||
              row.source_schema_hash !== entry.sourceSchema.hash ||
              row.entry_bytes !== metadataRow.entry_bytes ||
              row.entry_bytes !== (yield* Protocol.encodedBytesEffect(entry)) ||
              entry.spaceId !== request.spaceId
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Authoritative entry ${row.server_sequence} conflicts with durable metadata`
              })
            }
            entries.push(entry)
          }
          const page = Protocol.PullPage.make({ entries, hasMore: metadata.length > entries.length })
          if ((yield* Protocol.encodedBytesEffect(page)) > Protocol.maximumBatchBytes) {
            return yield* new ReplicaError.StorageCorrupt({
              message: "Authoritative pull page exceeds its encoded byte limit"
            })
          }
          return page
        })).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("ServerStore.pull", {
            attributes: { "space.id": request.spaceId, "server.after": request.after, "page.limit": request.limit }
          })
        )
      const bootstrap = (request: Protocol.BootstrapRequest) =>
        sql.withTransaction(Effect.gen(function*() {
          const stored = yield* findSnapshot({ spaceId: request.spaceId, snapshotId: request.snapshotId }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          let manifest: Protocol.SnapshotManifest
          let afterOrdinal = request.afterOrdinal
          if (Option.isNone(stored)) {
            const current = yield* findSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isNone(current) || current.value.snapshot_id === null) {
              return yield* new ReplicaError.SnapshotUnavailable({ snapshotId: request.snapshotId })
            }
            yield* validateStoredSpace(current.value)
            manifest = yield* currentManifest(request.spaceId, current.value)
            afterOrdinal = -1
          } else {
            manifest = manifestFromRow(stored.value)
          }
          if (manifest.definitionHash !== options.definition.hash) {
            return yield* new ReplicaError.DefinitionMismatch({
              expected: options.definition.hash,
              actual: manifest.definitionHash
            })
          }
          if (
            manifest.schema.version !== request.schema.version ||
            manifest.schema.hash !== request.schema.hash
          ) {
            return yield* new ReplicaError.StaleSchema({
              expectedVersion: request.schema.version,
              expectedHash: request.schema.hash,
              actualVersion: manifest.schema.version,
              actualHash: manifest.schema.hash
            })
          }
          if (afterOrdinal >= manifest.entityCount) {
            return yield* new ReplicaError.CursorGap({
              expected: Math.max(-1, manifest.entityCount - 1),
              actual: afterOrdinal
            })
          }
          const metadata = yield* findSnapshotEntityMetadata({
            spaceId: request.spaceId,
            snapshotId: manifest.snapshotId,
            after: afterOrdinal,
            limit: request.limit + 1
          }).pipe(Effect.mapError(StorageUnavailable.make))
          const emptyMoreBytes = yield* Protocol.encodedBytesEffect(
            Protocol.BootstrapPage.make({ manifest, entities: [], hasMore: true })
          )
          const emptyFinalBytes = yield* Protocol.encodedBytesEffect(
            Protocol.BootstrapPage.make({ manifest, entities: [], hasMore: false })
          )
          let selectedCount = 0
          let selectedWireBytes = 0
          for (const row of metadata) {
            if (selectedCount >= request.limit) break
            const expectedOrdinal = afterOrdinal + selectedCount + 1
            if (row.ordinal !== expectedOrdinal) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} expected ordinal ${expectedOrdinal} but found ${row.ordinal}`
              })
            }
            const hasMore = row.ordinal + 1 < manifest.entityCount
            let candidateBaseBytes = emptyFinalBytes
            if (hasMore) candidateBaseBytes = emptyMoreBytes
            const candidateBytes = candidateBaseBytes +
              selectedWireBytes + row.wire_bytes + selectedCount
            if (candidateBytes > options.maximumBootstrapPageBytes) break
            selectedCount += 1
            selectedWireBytes += row.wire_bytes
          }
          if (metadata.length > 0 && selectedCount === 0) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Snapshot entity ${metadata[0].ordinal} exceeds the bootstrap page limit`
            })
          }
          const through = afterOrdinal + selectedCount
          let rows: ReadonlyArray<typeof Rows.SnapshotEntityWireRow.Type> = []
          if (selectedCount > 0) {
            rows = yield* findSnapshotEntityWire({
              spaceId: request.spaceId,
              snapshotId: manifest.snapshotId,
              after: afterOrdinal,
              through
            }).pipe(Effect.mapError(StorageUnavailable.make))
          }
          if (rows.length !== selectedCount) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Snapshot ${manifest.snapshotId} durable page rows are incomplete`
            })
          }
          const entities: Array<Protocol.SnapshotEntity> = []
          for (let index = 0; index < rows.length; index++) {
            const row = rows[index]
            const entity = yield* Codec.parse(row.wire_json).pipe(
              Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntity, value))
            )
            if (
              entity.ordinal !== afterOrdinal + index + 1 ||
              row.wire_bytes !== (yield* Protocol.encodedBytesEffect(entity)) ||
              row.wire_json !== (yield* Codec.stringify(entity))
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} durable entity ${row.ordinal} is inconsistent`
              })
            }
            entities.push(entity)
          }
          const hasMore = through + 1 < manifest.entityCount
          const page = Protocol.BootstrapPage.make({ manifest, entities, hasMore })
          if ((yield* Protocol.encodedBytesEffect(page)) > options.maximumBootstrapPageBytes) {
            return yield* new ReplicaError.StorageCorrupt({ message: "Bootstrap page exceeds its byte limit" })
          }
          return page
        })).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("ServerStore.bootstrap", {
            attributes: { "space.id": request.spaceId, "snapshot.id": request.snapshotId }
          })
        )

      const prepareSnapshot = (spaceId: Identity.SpaceId) =>
        sql.withTransaction(Effect.gen(function*() {
          const stored = yield* findSpace(spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isNone(stored)) return Option.none()
          let meta: typeof Rows.ServerMetaRow.Type = stored.value
          if (meta.metadata_verified === 0) {
            const locked = yield* lockSpace(spaceId).pipe(Effect.mapError(StorageUnavailable.make))
            meta = yield* repairLockedSpace(spaceId, locked)
          }
          yield* validateStoredSpace(meta)
          const rows = yield* findEntities({ spaceId, limit: options.maximumSnapshotEntities + 1 }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          const decoded = yield* decodeEntityRows(spaceId, rows)
          if (decoded.entities.length !== meta.entity_count || decoded.contentBytes !== meta.entity_bytes) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Space ${spaceId} state counters do not match authoritative entities`
            })
          }
          const snapshotId = yield* Identity.makeSnapshotId.pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError((cause) => new ReplicaError.StorageUnavailable({ cause }))
          )
          return Option.some({
            observedNextServer: meta.next_server_sequence,
            observedNextTerminal: meta.next_terminal_sequence,
            observedSchemaGeneration: meta.schema_generation,
            manifest: Protocol.SnapshotManifest.make({
              spaceId,
              definitionHash: meta.definition_hash,
              schema: Identity.SchemaIdentity.make({ version: meta.schema_version, hash: meta.schema_hash }),
              snapshotId,
              sequence: Identity.ServerSequence.make(meta.next_server_sequence - 1),
              terminalSequenceThrough: Identity.TerminalSequence.make(meta.next_terminal_sequence - 1),
              entityCount: decoded.entities.length,
              contentBytes: decoded.contentBytes,
              digest: decoded.digest
            }),
            entities: decoded.entities
          })
        })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const publishAndPrune = (
        prepared: Option.Option<
          Effect.Success<ReturnType<typeof prepareSnapshot>> extends Option.Option<infer A> ? A : never
        >
      ) =>
        Option.match(prepared, {
          onNone: () => Effect.void,
          onSome: (candidate) =>
            sql.withTransaction(Effect.gen(function*() {
              const meta = yield* lockSpace(candidate.manifest.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
              yield* validateStoredSpace(meta)
              if (
                meta.next_server_sequence !== candidate.observedNextServer ||
                meta.next_terminal_sequence !== candidate.observedNextTerminal ||
                meta.schema_generation !== candidate.observedSchemaGeneration
              ) return
              let snapshotId = meta.snapshot_id
              if (
                snapshotId === null ||
                meta.snapshot_sequence !== candidate.manifest.sequence ||
                meta.snapshot_terminal_sequence !== candidate.manifest.terminalSequenceThrough
              ) {
                snapshotId = candidate.manifest.snapshotId
                yield* sql`INSERT INTO effect_local_server_snapshots
                  (space_id, snapshot_id, definition_hash, schema_version, schema_hash,
                    server_sequence, terminal_sequence,
                    entity_count, content_bytes, digest)
                  VALUES (${candidate.manifest.spaceId}, ${snapshotId}, ${candidate.manifest.definitionHash},
                    ${candidate.manifest.schema.version}, ${candidate.manifest.schema.hash},
                    ${candidate.manifest.sequence}, ${candidate.manifest.terminalSequenceThrough},
                    ${candidate.manifest.entityCount}, ${candidate.manifest.contentBytes}, ${candidate.manifest.digest})`
                const snapshotRows = yield* Effect.forEach(candidate.entities, (entity) =>
                  Effect.gen(function*() {
                    const wireJson = yield* Codec.stringify(entity)
                    return {
                      space_id: candidate.manifest.spaceId,
                      snapshot_id: snapshotId,
                      ordinal: entity.ordinal,
                      model: entity.model,
                      model_version: entity.modelVersion,
                      entity_key: yield* Codec.stringify(entity.key),
                      value_json: yield* Codec.stringify(entity.value),
                      entity_bytes: entity.entityBytes,
                      wire_json: wireJson,
                      wire_bytes: yield* Protocol.encodedBytesEffect(entity)
                    }
                  }))
                for (let offset = 0; offset < snapshotRows.length; offset += 100) {
                  yield* sql`INSERT INTO effect_local_server_snapshot_entities
                    ${sql.insert(snapshotRows.slice(offset, offset + 100))}`
                }
              }
              const historyFloor = Identity.ServerSequence.make(Math.max(
                meta.history_floor,
                candidate.manifest.sequence - options.retainedHistoryEntries
              ))
              const receiptFloor = Identity.TerminalSequence.make(Math.max(
                meta.receipt_floor,
                candidate.manifest.terminalSequenceThrough - options.retainedReceipts
              ))
              yield* sql`UPDATE effect_local_server_spaces SET
                snapshot_id = ${snapshotId},
                snapshot_sequence = ${candidate.manifest.sequence},
                snapshot_terminal_sequence = ${candidate.manifest.terminalSequenceThrough},
                history_floor = ${historyFloor},
                receipt_floor = ${receiptFloor}
                WHERE space_id = ${candidate.manifest.spaceId}`

              const history = yield* findHistoryPrune({
                spaceId: candidate.manifest.spaceId,
                through: historyFloor,
                limit: options.pruneBatchSize
              }).pipe(Effect.mapError(StorageUnavailable.make))
              if (history.length > 0) {
                const through = history.at(-1)!.server_sequence
                yield* sql`DELETE FROM effect_local_authoritative_log
                  WHERE space_id = ${candidate.manifest.spaceId} AND server_sequence <= ${through}`
              }
              const receipts = yield* findReceiptPrune({
                spaceId: candidate.manifest.spaceId,
                through: receiptFloor,
                limit: options.pruneBatchSize
              }).pipe(Effect.mapError(StorageUnavailable.make))
              if (receipts.length > 0) {
                const through = receipts.at(-1)!.terminal_sequence
                yield* sql`UPDATE effect_local_server_clients AS c SET
                  expired_local_sequence = MAX(expired_local_sequence, COALESCE((
                    SELECT MAX(r.local_sequence) FROM effect_local_server_receipts AS r
                    WHERE r.space_id = c.space_id AND r.client_id = c.client_id
                      AND r.terminal_sequence <= ${through}
                  ), expired_local_sequence))
                  WHERE c.space_id = ${candidate.manifest.spaceId} AND EXISTS (
                    SELECT 1 FROM effect_local_server_receipts AS r
                    WHERE r.space_id = c.space_id AND r.client_id = c.client_id
                      AND r.terminal_sequence <= ${through}
                  )`
                yield* sql`DELETE FROM effect_local_server_receipts
                  WHERE space_id = ${candidate.manifest.spaceId} AND terminal_sequence <= ${through}`
              }
              yield* sql`DELETE FROM effect_local_server_snapshot_entities
                WHERE space_id = ${candidate.manifest.spaceId} AND snapshot_id IN (
                  SELECT snapshot_id FROM effect_local_server_snapshots
                  WHERE space_id = ${candidate.manifest.spaceId}
                  ORDER BY server_sequence DESC, terminal_sequence DESC
                  LIMIT -1 OFFSET ${options.retainedSnapshots}
                )`
              yield* sql`DELETE FROM effect_local_server_snapshots
                WHERE space_id = ${candidate.manifest.spaceId} AND snapshot_id IN (
                  SELECT snapshot_id FROM effect_local_server_snapshots
                  WHERE space_id = ${candidate.manifest.spaceId}
                  ORDER BY server_sequence DESC, terminal_sequence DESC
                  LIMIT -1 OFFSET ${options.retainedSnapshots}
                )`
            })).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
        })

      const maintain = (spaceId: Identity.SpaceId) =>
        prepareSnapshot(spaceId).pipe(
          Effect.flatMap(publishAndPrune),
          Effect.withSpan("ServerStore.maintain", { attributes: { "space.id": spaceId } })
        )
      const maintainAll = Effect.gen(function*() {
        let after = ""
        while (true) {
          const spaces = yield* findSpaces({ after, limit: options.maintenanceSpaceBatchSize }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          if (spaces.length === 0) return
          yield* Effect.forEach(spaces, ({ space_id }) => maintain(space_id), {
            concurrency: options.maintenanceConcurrency,
            discard: true
          })
          after = spaces.at(-1)!.space_id
        }
      })
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
      const trustedBootstrapRequest = (
        request: Protocol.BootstrapRequest | Omit<Protocol.BootstrapRequest, "schema">
      ): Protocol.BootstrapRequest =>
        Schema.is(Protocol.BootstrapRequest)(request)
          ? request
          : { ...request, schema: options.definition.schemaIdentity }

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
        bootstrap: (input) => {
          const request = trustedBootstrapRequest(input)
          return authorizeRead(request.spaceId, null).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.andThen(bootstrap(request))
          )
        },
        bootstrapAuthorized: (request, principal) =>
          authorizeRead(request.spaceId, principal).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.andThen(bootstrap(request))
          ),
        maintain,
        maintainAll,
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

export const layerTrusted = (
  options: {
    readonly definition: Definition.Any
    readonly evolution?: Evolution.Evolution
    readonly schemaEvolutionBatchSize?: number
    readonly wakeCapacity?: number
  } & HistoryOptions
): Layer.Layer<
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

export interface MaintenanceOptions {
  readonly interval: Duration.Input
  readonly runOnStart?: boolean
}

export interface MaintenanceService {
  readonly run: Effect.Effect<void, ReplicaError.ReplicaError>
}

export class HistoryMaintenance extends Context.Service<HistoryMaintenance, MaintenanceService>()(
  "@lucas-barake/effect-local-sql/ServerStore/HistoryMaintenance"
) {}

export const layerMaintenance = (options: MaintenanceOptions): Layer.Layer<
  HistoryMaintenance,
  ReplicaError.ReplicaError,
  ServerStore
> =>
  Layer.effect(
    HistoryMaintenance,
    Effect.gen(function*() {
      const store = yield* ServerStore
      const intervalMillis = yield* Configuration.positiveFiniteDurationMillis(
        "historyMaintenance.interval",
        options.interval
      )
      if (options.runOnStart === true) yield* store.maintainAll
      yield* Effect.forkScoped(Effect.forever(
        Effect.sleep(intervalMillis).pipe(
          Effect.andThen(store.maintainAll),
          Effect.catch((error) => Effect.logError("History maintenance failed", error))
        )
      ))
      return HistoryMaintenance.of({ run: store.maintainAll })
    })
  )
