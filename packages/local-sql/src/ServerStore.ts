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
import * as Semaphore from "effect/Semaphore"
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
  readonly discard: (
    request: Protocol.DiscardRequest,
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
  readonly acceptedSchemaVersions?: number
  readonly schemaEvolutionBatchSize?: number
  readonly schemaEvolutionBatchBytes?: number
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
      const acceptedSchemaVersions = options.acceptedSchemaVersions ?? 0
      if (!Number.isSafeInteger(acceptedSchemaVersions) || acceptedSchemaVersions < 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "acceptedSchemaVersions",
          message: "acceptedSchemaVersions must be a nonnegative safe integer"
        })
      }
      if (acceptedSchemaVersions > evolution.steps.length) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "acceptedSchemaVersions",
          message: "acceptedSchemaVersions exceeds the configured evolution history"
        })
      }
      const minimumAcceptedSchemaVersion = options.definition.version - acceptedSchemaVersions
      if (acceptedSchemaVersions > 0) {
        for (const entry of evolution.steps.slice(-acceptedSchemaVersions)) {
          yield* Evolution.validateDowngradeTarget(evolution, entry.from.schemaIdentity).pipe(
            Effect.mapError(() =>
              new ReplicaError.InvalidConfiguration({
                option: "acceptedSchemaVersions",
                message:
                  `Schema ${entry.from.schemaIdentity.version}:${entry.from.schemaIdentity.hash} cannot be projected from the current definition`
              })
            )
          )
        }
      }
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
      const projectionGates = yield* RcMap.make({
        lookup: (_key: string) => Semaphore.make(1)
      })

      const wakes = yield* RcMap.make({
        lookup: (_spaceId: Identity.SpaceId) =>
          Effect.acquireRelease(PubSub.sliding<Protocol.Wake>(wakeCapacity), PubSub.shutdown)
      })
      const findReceiptByMutation = SqlSchema.findOneOption({
        Request: Schema.Struct({ spaceId: Identity.SpaceId, mutationId: Identity.MutationId }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, mutationId }) =>
          sql`SELECT r.space_id, r.client_id, r.membership_incarnation, r.local_sequence,
          r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, r.terminal_sequence, r.server_sequence
        FROM effect_local_server_receipts AS r
        WHERE r.space_id = ${spaceId} AND r.mutation_id = ${mutationId}`
      })
      const findReceiptBySequence = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          clientId: Identity.ClientId,
          membershipIncarnation: Identity.MembershipIncarnation,
          localSequence: Identity.LocalSequence
        }),
        Result: Rows.ServerReceiptRow,
        execute: ({ spaceId, clientId, membershipIncarnation, localSequence }) =>
          sql`SELECT r.space_id, r.client_id, r.membership_incarnation, r.local_sequence,
          r.digest, r.mutation_id, r.receipt_json,
          r.digest_version, r.source_schema_version, r.source_schema_hash, r.mutation_version,
          r.mutation_name, r.rejection_origin, r.terminal_sequence, r.server_sequence
        FROM effect_local_server_receipts AS r
        WHERE r.space_id = ${spaceId} AND r.client_id = ${clientId}
          AND r.membership_incarnation = ${membershipIncarnation} AND r.local_sequence = ${localSequence}`
      })
      const lockClient = SqlSchema.findOne({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          clientId: Identity.ClientId,
          membershipIncarnation: Identity.MembershipIncarnation
        }),
        Result: Rows.ServerClientRow,
        execute: ({ spaceId, clientId, membershipIncarnation }) =>
          sql`UPDATE effect_local_server_clients
        SET last_local_sequence = last_local_sequence
        WHERE space_id = ${spaceId} AND client_id = ${clientId}
          AND membership_incarnation = ${membershipIncarnation}
        RETURNING last_local_sequence, expired_local_sequence`
      })
      const lockSpace = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`UPDATE effect_local_server_spaces
        SET next_server_sequence = next_server_sequence
        WHERE space_id = ${spaceId}
        RETURNING definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
          target_schema_version, target_schema_hash, migration_hash, next_server_sequence,
          next_terminal_sequence, history_floor,
            receipt_floor, retained_history_count, retained_receipt_count, entity_count, entity_bytes,
            snapshot_id, snapshot_sequence, snapshot_terminal_sequence, metadata_verified`
      })
      const findSpace = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: Rows.ServerMetaRow,
        execute: (spaceId) =>
          sql`SELECT definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
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
          sql`SELECT space_id, server_sequence, client_id, membership_incarnation,
            local_sequence, mutation_id, digest,
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
      const findSnapshotProjection = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          target: Identity.SchemaIdentity
        }),
        Result: Rows.SnapshotProjectionRow,
        execute: ({ spaceId, snapshotId, target }) =>
          sql`SELECT space_id, snapshot_id, target_schema_version, target_schema_hash, definition_hash,
            entity_count, content_bytes, digest
          FROM effect_local_server_snapshot_projections
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}
            AND target_schema_version = ${target.version} AND target_schema_hash = ${target.hash}`
      })
      const findSnapshotProjectionEntityMetadata = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          target: Identity.SchemaIdentity,
          after: Schema.Int,
          limit: Schema.Int
        }),
        Result: Rows.SnapshotEntityMetadataRow,
        execute: ({ spaceId, snapshotId, target, after, limit }) =>
          sql`SELECT ordinal, wire_bytes
          FROM effect_local_server_snapshot_projection_entities
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}
            AND target_schema_version = ${target.version} AND target_schema_hash = ${target.hash}
            AND ordinal > ${after}
          ORDER BY ordinal LIMIT ${limit}`
      })
      const findSnapshotProjectionEntityWire = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          target: Identity.SchemaIdentity,
          after: Schema.Int,
          through: Schema.Int
        }),
        Result: Rows.SnapshotEntityWireRow,
        execute: ({ spaceId, snapshotId, target, after, through }) =>
          sql`SELECT ordinal, wire_json, wire_bytes
          FROM effect_local_server_snapshot_projection_entities
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}
            AND target_schema_version = ${target.version} AND target_schema_hash = ${target.hash}
            AND ordinal > ${after} AND ordinal <= ${through}
          ORDER BY ordinal`
      })
      const findSnapshotProjectionEntityByKey = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          snapshotId: Identity.SnapshotId,
          target: Identity.SchemaIdentity,
          model: Schema.String,
          entityKey: Schema.String
        }),
        Result: Schema.Struct({ ordinal: Schema.Int }),
        execute: ({ spaceId, snapshotId, target, model, entityKey }) =>
          sql`SELECT ordinal FROM effect_local_server_snapshot_projection_entities
          WHERE space_id = ${spaceId} AND snapshot_id = ${snapshotId}
            AND target_schema_version = ${target.version} AND target_schema_hash = ${target.hash}
            AND model = ${model} AND entity_key = ${entityKey}`
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
          } else if (receipt._tag === "Legacy") {
            receiptSequenceMismatch = receipt.serverSequence !== row.server_sequence
          }
          if (
            row.space_id !== envelope.spaceId ||
            row.client_id !== envelope.clientId ||
            row.membership_incarnation !== envelope.membershipIncarnation ||
            row.local_sequence !== envelope.localSequence ||
            row.mutation_id !== envelope.mutationId ||
            receipt.spaceId !== row.space_id ||
            receipt.clientId !== row.client_id ||
            receipt.membershipIncarnation !== row.membership_incarnation ||
            receipt.localSequence !== row.local_sequence ||
            receipt.mutationId !== row.mutation_id ||
            receipt.sourceSchema.version !== row.source_schema_version ||
            receipt.sourceSchema.hash !== row.source_schema_hash ||
            (receipt._tag !== "Legacy" && (
              receipt.mutationVersion !== row.mutation_version || receipt.name !== row.mutation_name
            )) ||
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
            membershipIncarnation: envelope.membershipIncarnation,
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
            membershipIncarnation: envelope.membershipIncarnation,
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

      const validateCallerSchema = (schema: Identity.SchemaIdentity) => {
        const definition = evolution.definitionByIdentity.get(`${schema.version}:${schema.hash}`)
        if (definition !== undefined && definition.version >= minimumAcceptedSchemaVersion) {
          return Effect.succeed(definition)
        }
        return Effect.fail(
          new ReplicaError.StaleSchema({
            expectedVersion: options.definition.schemaIdentity.version,
            expectedHash: options.definition.schemaIdentity.hash,
            actualVersion: schema.version,
            actualHash: schema.hash
          })
        )
      }

      const projectReceipt = (receipt: Protocol.Receipt, target: Definition.Any) =>
        Effect.gen(function*() {
          if (
            receipt._tag === "Legacy" ||
            (receipt.sourceSchema.version === target.schemaIdentity.version &&
              receipt.sourceSchema.hash === target.schemaIdentity.hash)
          ) return receipt
          const mutation = target.mutationByName.get(receipt.name)
          if (mutation === undefined) {
            return yield* new ReplicaError.SchemaEvolutionUnsupported({
              sourceVersion: receipt.sourceSchema.version,
              sourceHash: receipt.sourceSchema.hash,
              targetVersion: target.schemaIdentity.version,
              targetHash: target.schemaIdentity.hash
            })
          }
          if (receipt._tag === "Expired") {
            return Protocol.ExpiredReceipt.make({
              ...receipt,
              sourceSchema: target.schemaIdentity,
              mutationVersion: mutation.version
            })
          }
          if (receipt._tag === "Rejected" && receipt.origin !== "Mutation") {
            return Protocol.RejectedReceipt.make({
              ...receipt,
              sourceSchema: target.schemaIdentity,
              mutationVersion: mutation.version
            })
          }
          if (receipt._tag === "Accepted") {
            const result = yield* Evolution.migrateMutationSuccessTo({
              evolution,
              source: receipt.sourceSchema,
              target: target.schemaIdentity,
              mutation: receipt.name,
              mutationVersion: receipt.mutationVersion,
              value: receipt.result
            })
            return Protocol.AcceptedReceipt.make({
              ...receipt,
              sourceSchema: result.schemaIdentity,
              mutationVersion: result.mutationVersion,
              result: result.value
            })
          }
          const rejection = yield* Evolution.migrateMutationRejectionTo({
            evolution,
            source: receipt.sourceSchema,
            target: target.schemaIdentity,
            mutation: receipt.name,
            mutationVersion: receipt.mutationVersion,
            value: receipt.rejection
          })
          return Protocol.RejectedReceipt.make({
            ...receipt,
            sourceSchema: rejection.schemaIdentity,
            mutationVersion: rejection.mutationVersion,
            rejection: rejection.value
          })
        })

      const projectEntry = (entry: Protocol.AcceptedMutation, target: Definition.Any) =>
        Effect.gen(function*() {
          if (
            target.schemaIdentity.version === options.definition.schemaIdentity.version &&
            target.schemaIdentity.hash === options.definition.schemaIdentity.hash
          ) return entry
          const changes: Array<Protocol.EntityChange> = []
          for (const change of entry.changes) {
            if (!target.modelByName.has(change.entity.model)) continue
            let migrated: Evolution.MigratedModel
            if (change._tag === "Upsert") {
              migrated = yield* Evolution.migrateModelTo({
                evolution,
                source: entry.sourceSchema,
                target: target.schemaIdentity,
                model: change.entity.model,
                modelVersion: change.entity.modelVersion,
                key: change.entity.key,
                value: change.value
              })
            } else {
              migrated = yield* Evolution.migrateModelTo({
                evolution,
                source: entry.sourceSchema,
                target: target.schemaIdentity,
                model: change.entity.model,
                modelVersion: change.entity.modelVersion,
                key: change.entity.key
              })
            }
            const entity = Protocol.EntityKey.make({
              model: change.entity.model,
              modelVersion: migrated.modelVersion,
              key: migrated.key
            })
            if (change._tag === "Delete") changes.push(Protocol.Delete.make({ entity }))
            else changes.push(Protocol.Upsert.make({ entity, value: migrated.value }))
          }
          return Protocol.AcceptedMutation.make({ ...entry, sourceSchema: target.schemaIdentity, changes })
        })

      const ensureSnapshotProjection = (manifest: Protocol.SnapshotManifest, target: Definition.Any) =>
        Effect.gen(function*() {
          const existing = yield* findSnapshotProjection({
            spaceId: manifest.spaceId,
            snapshotId: manifest.snapshotId,
            target: target.schemaIdentity
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(existing)) {
            const row = existing.value
            if (row.definition_hash !== target.hash) {
              return yield* new ReplicaError.StorageCorrupt({
                message: `Snapshot ${manifest.snapshotId} projection definition is inconsistent`
              })
            }
            return Protocol.SnapshotManifest.make({
              ...manifest,
              definitionHash: row.definition_hash,
              schema: target.schemaIdentity,
              entityCount: row.entity_count,
              contentBytes: row.content_bytes,
              digest: row.digest
            })
          }
          const projectionKey = Canonical.stringify({
            spaceId: manifest.spaceId,
            snapshotId: manifest.snapshotId,
            target: target.schemaIdentity
          })
          return yield* Effect.scoped(
            RcMap.get(projectionGates, projectionKey).pipe(
              Effect.flatMap((gate) =>
                gate.withPermit(sql.withTransaction(Effect.gen(function*() {
                  const stored = yield* findSnapshotProjection({
                    spaceId: manifest.spaceId,
                    snapshotId: manifest.snapshotId,
                    target: target.schemaIdentity
                  }).pipe(Effect.mapError(StorageUnavailable.make))
                  if (Option.isSome(stored)) {
                    const row = stored.value
                    if (row.definition_hash !== target.hash) {
                      return yield* new ReplicaError.StorageCorrupt({
                        message: `Snapshot ${manifest.snapshotId} projection definition is inconsistent`
                      })
                    }
                    return Protocol.SnapshotManifest.make({
                      ...manifest,
                      definitionHash: row.definition_hash,
                      schema: target.schemaIdentity,
                      entityCount: row.entity_count,
                      contentBytes: row.content_bytes,
                      digest: row.digest
                    })
                  }

                  let sourceOrdinal = -1
                  let projectedOrdinal = 0
                  let contentBytes = 0
                  let digest = Protocol.initialSnapshotDigest
                  let projectedRows: Array<{
                    readonly space_id: Identity.SpaceId
                    readonly snapshot_id: Identity.SnapshotId
                    readonly target_schema_version: Identity.SchemaVersion
                    readonly target_schema_hash: Identity.SchemaHash
                    readonly ordinal: number
                    readonly model: string
                    readonly model_version: Identity.SchemaVersion
                    readonly entity_key: string
                    readonly wire_json: string
                    readonly wire_bytes: number
                  }> = []
                  const flushProjectedRows = Effect.suspend(() => {
                    if (projectedRows.length === 0) return Effect.void
                    const batch = projectedRows
                    projectedRows = []
                    return sql`INSERT INTO effect_local_server_snapshot_projection_entities ${sql.insert(batch)}`.pipe(
                      Effect.asVoid,
                      Effect.catchReason("SqlError", "UniqueViolation", (_reason, error) =>
                        Effect.gen(function*() {
                          const batchKeys = new Set<string>()
                          for (const row of batch) {
                            const key = Canonical.stringify([row.model, row.entity_key])
                            if (batchKeys.has(key)) {
                              return yield* new ReplicaError.SchemaKeyCollision({
                                model: row.model,
                                key: row.entity_key
                              })
                            }
                            batchKeys.add(key)
                            const conflict = yield* findSnapshotProjectionEntityByKey({
                              spaceId: row.space_id,
                              snapshotId: row.snapshot_id,
                              target: target.schemaIdentity,
                              model: row.model,
                              entityKey: row.entity_key
                            }).pipe(Effect.mapError(StorageUnavailable.make))
                            if (Option.isSome(conflict)) {
                              return yield* new ReplicaError.SchemaKeyCollision({
                                model: row.model,
                                key: row.entity_key
                              })
                            }
                          }
                          return yield* error
                        }))
                    )
                  })
                  while (sourceOrdinal + 1 < manifest.entityCount) {
                    const through = Math.min(sourceOrdinal + 256, manifest.entityCount - 1)
                    const rows = yield* findSnapshotEntityWire({
                      spaceId: manifest.spaceId,
                      snapshotId: manifest.snapshotId,
                      after: sourceOrdinal,
                      through
                    }).pipe(Effect.mapError(StorageUnavailable.make))
                    if (rows.length === 0) {
                      return yield* new ReplicaError.StorageCorrupt({
                        message: `Snapshot ${manifest.snapshotId} durable rows are incomplete`
                      })
                    }
                    for (const row of rows) {
                      const entity = yield* Codec.parse(row.wire_json).pipe(
                        Effect.flatMap((value) => Codec.decode(Protocol.SnapshotEntity, value))
                      )
                      if (
                        entity.ordinal !== sourceOrdinal + 1 ||
                        row.wire_bytes !== (yield* Protocol.encodedBytesEffect(entity)) ||
                        row.wire_json !== (yield* Codec.stringify(entity))
                      ) {
                        return yield* new ReplicaError.StorageCorrupt({
                          message: `Snapshot ${manifest.snapshotId} durable entity ${row.ordinal} is inconsistent`
                        })
                      }
                      sourceOrdinal = entity.ordinal
                      if (!target.modelByName.has(entity.model)) continue
                      const migrated = yield* Evolution.migrateModelTo({
                        evolution,
                        source: manifest.schema,
                        target: target.schemaIdentity,
                        model: entity.model,
                        modelVersion: entity.modelVersion,
                        key: entity.key,
                        value: entity.value
                      })
                      const entityKey = yield* Codec.stringify(migrated.key)
                      const projected = {
                        ordinal: projectedOrdinal,
                        model: entity.model,
                        modelVersion: migrated.modelVersion,
                        key: migrated.key,
                        value: migrated.value
                      }
                      const entityBytes = yield* Protocol.encodedBytesEffect({
                        model: projected.model,
                        modelVersion: projected.modelVersion,
                        key: projected.key,
                        value: projected.value
                      })
                      const encoded = Protocol.SnapshotEntity.make({ ...projected, entityBytes })
                      const wireJson = yield* Codec.stringify(encoded)
                      projectedRows.push({
                        space_id: manifest.spaceId,
                        snapshot_id: manifest.snapshotId,
                        target_schema_version: target.schemaIdentity.version,
                        target_schema_hash: target.schemaIdentity.hash,
                        ordinal: projectedOrdinal,
                        model: entity.model,
                        model_version: migrated.modelVersion,
                        entity_key: entityKey,
                        wire_json: wireJson,
                        wire_bytes: yield* Protocol.encodedBytesEffect(encoded)
                      })
                      if (projectedRows.length === 100) yield* flushProjectedRows
                      contentBytes += entityBytes
                      digest = Protocol.SnapshotDigest.make(
                        yield* Canonical.digest({ previous: digest, entity: encoded }).pipe(
                          Effect.provideService(Crypto.Crypto, crypto)
                        )
                      )
                      projectedOrdinal += 1
                    }
                  }
                  yield* flushProjectedRows
                  yield* sql`INSERT INTO effect_local_server_snapshot_projections
                    (space_id, snapshot_id, target_schema_version, target_schema_hash, definition_hash,
                      entity_count, content_bytes, digest)
                    VALUES (${manifest.spaceId}, ${manifest.snapshotId}, ${target.schemaIdentity.version},
                      ${target.schemaIdentity.hash}, ${target.hash}, ${projectedOrdinal}, ${contentBytes}, ${digest})`
                  return Protocol.SnapshotManifest.make({
                    ...manifest,
                    definitionHash: target.hash,
                    schema: target.schemaIdentity,
                    entityCount: projectedOrdinal,
                    contentBytes,
                    digest
                  })
                })))
              )
            )
          )
        })

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
        if (space.definition_hash === options.definition.hash) return Effect.void
        return Effect.fail(
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
          let evolutionOptions: SchemaEvolution.ServerOptions = {
            definition: options.definition,
            evolution,
            spaceId
          }
          if (options.schemaEvolutionBatchSize !== undefined) {
            evolutionOptions = { ...evolutionOptions, batchSize: options.schemaEvolutionBatchSize }
          }
          if (options.schemaEvolutionBatchBytes !== undefined) {
            evolutionOptions = { ...evolutionOptions, batchBytes: options.schemaEvolutionBatchBytes }
          }
          yield* SchemaEvolution.server(evolutionOptions).pipe(Effect.provideService(SqlClient.SqlClient, sql))
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
          hasMore: true,
          serverSchema: options.definition.schemaIdentity
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
              yield* sql`UPDATE effect_local_server_entities_data SET entity_bytes = ${entity.entityBytes}
                WHERE space_id = ${spaceId} AND generation = ${meta.active_schema_generation}
                  AND model = ${row.model} AND entity_key = ${row.entity_key}`
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
          const callerDefinition = yield* validateCallerSchema(request.schema)
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
              Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution)),
              Effect.flatMap((receipt) => projectReceipt(receipt, callerDefinition))
            )
          }
          const envelope = submittedEnvelope
          const membershipIncarnation = envelope.membershipIncarnation
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
            yield* sql`INSERT INTO effect_local_server_clients
              (space_id, client_id, membership_incarnation, last_local_sequence, expired_local_sequence)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, ${membershipIncarnation}, 0, 0)
              ON CONFLICT (space_id, client_id, membership_incarnation) DO NOTHING`
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
            const client = yield* lockClient({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              membershipIncarnation
            }).pipe(
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
                Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution)),
                Effect.flatMap((receipt) => projectReceipt(receipt, callerDefinition))
              )
            }
            const committedBySequence = yield* findReceiptBySequence({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              membershipIncarnation,
              localSequence: envelope.localSequence
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(committedBySequence)) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
            }
            yield* validateStoredSpace(storedSpace)
            if (envelope.localSequence <= client.expired_local_sequence) {
              const manifest = yield* currentManifest(envelope.spaceId, storedSpace)
              return yield* projectReceipt(
                Protocol.ExpiredReceipt.make({
                  spaceId: envelope.spaceId,
                  clientId: envelope.clientId,
                  membershipIncarnation,
                  mutationId: envelope.mutationId,
                  localSequence: envelope.localSequence,
                  name: mutation.name,
                  sourceSchema: options.definition.schemaIdentity,
                  mutationVersion: mutation.mutationVersion,
                  snapshotId: manifest.snapshotId,
                  snapshotSequence: manifest.sequence,
                  terminalSequenceThrough: manifest.terminalSequenceThrough
                }),
                callerDefinition
              )
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
                  SqlTransaction.server({
                    sql,
                    definition: options.definition,
                    spaceId: envelope.spaceId,
                    generation: storedSpace.active_schema_generation,
                    changes
                  }),
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
                        membershipIncarnation,
                        mutationId: envelope.mutationId,
                        localSequence: envelope.localSequence,
                        sourceSchema: options.definition.schemaIdentity,
                        digest: envelope.digest,
                        changes: result.success.changes
                      })
                      const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                      if (
                        (yield* Protocol.encodedBytesEffect(Protocol.PullPage.make({
                          entries: [entry],
                          hasMore: false,
                          serverSchema: options.definition.schemaIdentity
                        }))) >
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
                        membershipIncarnation,
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
                  (space_id, server_sequence, client_id, membership_incarnation, local_sequence,
                    mutation_id, digest, entry_bytes, entry_json, source_schema_version,
                    source_schema_hash, mutation_version)
                  VALUES (${envelope.spaceId}, ${entry.sequence}, ${envelope.clientId},
                    ${membershipIncarnation}, ${envelope.localSequence},
                    ${envelope.mutationId}, ${envelope.digest}, ${entryBytes}, ${entryJson},
                    ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash},
                    ${mutation.mutationVersion})`
                receipt = executed.success.receipt
              }
            }
            const terminalSequence = Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
            let receiptServerSequence: Identity.ServerSequence | null = null
            if (receipt._tag === "Accepted") receiptServerSequence = receipt.serverSequence
            let rejectionOrigin: Protocol.RejectionOrigin | null = null
            if (receipt._tag === "Rejected") rejectionOrigin = receipt.origin
            yield* sql`INSERT INTO effect_local_server_receipts
              (space_id, client_id, membership_incarnation, local_sequence, mutation_id, digest,
                terminal_sequence, server_sequence, receipt_json, digest_version, source_schema_version,
                source_schema_hash, mutation_version, mutation_name, rejection_origin)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, ${membershipIncarnation},
                ${envelope.localSequence}, ${envelope.mutationId},
                ${envelope.digest}, ${terminalSequence}, ${receiptServerSequence},
                ${yield* Codec.stringify(receipt)}, ${envelope.digestVersion},
                ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash}, ${receipt.mutationVersion},
                ${receipt.name}, ${rejectionOrigin})`
            yield* sql`UPDATE effect_local_server_spaces SET
              next_terminal_sequence = next_terminal_sequence + 1
              WHERE space_id = ${envelope.spaceId}`
            yield* sql`UPDATE effect_local_server_clients SET last_local_sequence = ${envelope.localSequence}
              WHERE space_id = ${envelope.spaceId} AND client_id = ${envelope.clientId}
                AND membership_incarnation = ${membershipIncarnation}`
            return yield* projectReceipt(receipt, callerDefinition)
          }))
        }).pipe(
          Effect.catchIf(
            SqlError.isSqlError,
            (cause) =>
              Effect.fail(new ReplicaError.UnknownCommitOutcome({ mutationId: submittedEnvelope.mutationId, cause }))
          ),
          Effect.tap((receipt) => {
            if (receipt._tag !== "Accepted") return Effect.void
            return RcMap.has(wakes, submittedEnvelope.spaceId).pipe(
              Effect.flatMap((hasWatchers) => {
                if (hasWatchers) {
                  return Effect.scoped(
                    RcMap.get(wakes, submittedEnvelope.spaceId).pipe(
                      Effect.flatMap((channel) =>
                        PubSub.publish(channel, {
                          spaceId: submittedEnvelope.spaceId,
                          sequence: receipt.serverSequence
                        })
                      )
                    )
                  )
                }
                return Effect.void
              }),
              Effect.asVoid
            )
          }),
          Effect.withSpan("ServerStore.submit", {
            attributes: {
              "mutation.name": submittedEnvelope.name,
              "mutation.id": submittedEnvelope.mutationId,
              "space.id": submittedEnvelope.spaceId
            }
          })
        )
      }

      const discard = (request: Protocol.DiscardRequest, principal: typeof Schema.Json.Type) => {
        const submittedEnvelope = request.envelope
        return Effect.gen(function*() {
          yield* authorizeAccess(submittedEnvelope, principal)
          const callerDefinition = yield* validateCallerSchema(request.schema)
          const exact = yield* findReceiptByMutation({
            spaceId: submittedEnvelope.spaceId,
            mutationId: submittedEnvelope.mutationId
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isSome(exact)) {
            if (exact.value.digest !== submittedEnvelope.digest) {
              return yield* new ReplicaError.MutationIdentityConflict({ mutationId: submittedEnvelope.mutationId })
            }
            return yield* decodeStoredReceipt(exact.value, submittedEnvelope).pipe(
              Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution)),
              Effect.flatMap((receipt) => projectReceipt(receipt, callerDefinition))
            )
          }
          const envelope = submittedEnvelope
          const membershipIncarnation = envelope.membershipIncarnation
          yield* prepareSpace(envelope.spaceId, request.schema)
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
              (space_id, client_id, membership_incarnation, last_local_sequence, expired_local_sequence)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, ${membershipIncarnation}, 0, 0)
              ON CONFLICT (space_id, client_id, membership_incarnation) DO NOTHING`
            let storedSpace = yield* lockSpace(envelope.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
            if (storedSpace.metadata_verified === 0) {
              storedSpace = yield* repairLockedSpace(envelope.spaceId, storedSpace)
            }
            yield* validateStoredSpace(storedSpace)
            const committed = yield* findReceiptByMutation({
              spaceId: envelope.spaceId,
              mutationId: envelope.mutationId
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (Option.isSome(committed)) {
              if (committed.value.digest !== envelope.digest) {
                return yield* new ReplicaError.MutationIdentityConflict({ mutationId: envelope.mutationId })
              }
              return yield* decodeStoredReceipt(committed.value, envelope).pipe(
                Effect.flatMap((receipt) => SchemaEvolution.migrateReceipt(receipt, evolution)),
                Effect.flatMap((receipt) => projectReceipt(receipt, callerDefinition))
              )
            }
            const client = yield* lockClient({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              membershipIncarnation
            }).pipe(Effect.mapError(StorageUnavailable.make))
            if (envelope.localSequence <= client.expired_local_sequence) {
              const manifest = yield* currentManifest(envelope.spaceId, storedSpace)
              return yield* projectReceipt(
                Protocol.ExpiredReceipt.make({
                  spaceId: envelope.spaceId,
                  clientId: envelope.clientId,
                  membershipIncarnation,
                  mutationId: envelope.mutationId,
                  localSequence: envelope.localSequence,
                  name: mutation.name,
                  sourceSchema: options.definition.schemaIdentity,
                  mutationVersion: mutation.mutationVersion,
                  snapshotId: manifest.snapshotId,
                  snapshotSequence: manifest.sequence,
                  terminalSequenceThrough: manifest.terminalSequenceThrough
                }),
                callerDefinition
              )
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
            if (storedSpace.next_terminal_sequence >= Number.MAX_SAFE_INTEGER) {
              return yield* new ReplicaError.CapacityExceeded({
                resource: "terminal sequence",
                limit: Number.MAX_SAFE_INTEGER - 1
              })
            }
            const terminalSequence = Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
            const receipt = Protocol.RejectedReceipt.make({
              spaceId: envelope.spaceId,
              clientId: envelope.clientId,
              membershipIncarnation,
              mutationId: envelope.mutationId,
              localSequence: envelope.localSequence,
              name: mutation.name,
              sourceSchema: options.definition.schemaIdentity,
              mutationVersion: mutation.mutationVersion,
              origin: "Quarantine",
              terminalSequence,
              rejection: { _tag: "Quarantined" }
            })
            yield* sql`INSERT INTO effect_local_server_receipts
              (space_id, client_id, membership_incarnation, local_sequence, mutation_id, digest,
                terminal_sequence, server_sequence, receipt_json, digest_version, source_schema_version,
                source_schema_hash, mutation_version, mutation_name, rejection_origin)
              VALUES (${envelope.spaceId}, ${envelope.clientId}, ${membershipIncarnation},
                ${envelope.localSequence}, ${envelope.mutationId}, ${envelope.digest},
                ${terminalSequence}, NULL, ${yield* Codec.stringify(receipt)},
                ${envelope.digestVersion}, ${receipt.sourceSchema.version}, ${receipt.sourceSchema.hash},
                ${receipt.mutationVersion}, ${receipt.name}, ${receipt.origin})`
            yield* sql`UPDATE effect_local_server_spaces SET next_terminal_sequence = next_terminal_sequence + 1
              WHERE space_id = ${envelope.spaceId}`
            yield* sql`UPDATE effect_local_server_clients SET last_local_sequence = ${envelope.localSequence}
              WHERE space_id = ${envelope.spaceId} AND client_id = ${envelope.clientId}
                AND membership_incarnation = ${membershipIncarnation}`
            return yield* projectReceipt(receipt, callerDefinition)
          }))
        }).pipe(
          Effect.catchIf(
            SqlError.isSqlError,
            (cause) =>
              Effect.fail(
                new ReplicaError.UnknownCommitOutcome({
                  mutationId: submittedEnvelope.mutationId,
                  cause
                })
              )
          ),
          Effect.withSpan("ServerStore.discard", {
            attributes: { "mutation.id": submittedEnvelope.mutationId, "space.id": submittedEnvelope.spaceId }
          })
        )
      }

      const pull = (request: Protocol.PullRequest) =>
        sql.withTransaction(Effect.gen(function*() {
          const callerDefinition = yield* validateCallerSchema(request.schema)
          const stored = yield* findSpace(request.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
          if (Option.isNone(stored)) {
            return Protocol.PullPage.make({
              entries: [],
              hasMore: false,
              serverSchema: options.definition.schemaIdentity
            })
          }
          const meta = stored.value
          yield* validateStoredSpace(meta)
          if (request.after < meta.history_floor) {
            const manifest = yield* currentManifest(request.spaceId, meta)
            if (
              callerDefinition.schemaIdentity.version === options.definition.schemaIdentity.version &&
              callerDefinition.schemaIdentity.hash === options.definition.schemaIdentity.hash
            ) {
              return Protocol.BootstrapRequired.make({
                manifest,
                serverSchema: options.definition.schemaIdentity
              })
            }
            const projected = yield* ensureSnapshotProjection(manifest, callerDefinition)
            return Protocol.BootstrapRequired.make({
              manifest: projected,
              serverSchema: options.definition.schemaIdentity
            })
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
          const selected: Array<typeof Rows.ServerLogMetadataRow.Type> = []
          let selectedSourceBytes = 0
          for (const row of metadata.slice(0, request.limit)) {
            if (selectedSourceBytes + row.entry_bytes > Protocol.maximumBatchBytes) break
            selected.push(row)
            selectedSourceBytes += row.entry_bytes
          }
          if (metadata.length > 0 && selected.length === 0) {
            return yield* new ReplicaError.StorageCorrupt({
              message: `Authoritative entry ${metadata[0].server_sequence} exceeds the durable byte limit`
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
          let pageBytes = yield* Protocol.encodedBytesEffect(Protocol.PullPage.make({
            entries: [],
            hasMore: false,
            serverSchema: options.definition.schemaIdentity
          }))
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
              row.membership_incarnation !== entry.membershipIncarnation ||
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
            const projected = yield* projectEntry(entry, callerDefinition)
            let separatorBytes = 1
            if (entries.length === 0) separatorBytes = 0
            const projectedBytes = yield* Protocol.encodedBytesEffect(projected)
            if (pageBytes + separatorBytes + projectedBytes > Protocol.maximumBatchBytes) break
            entries.push(projected)
            pageBytes += separatorBytes + projectedBytes
          }
          if (rows.length > 0 && entries.length === 0) {
            return yield* new ReplicaError.StaleSchema({
              expectedVersion: options.definition.schemaIdentity.version,
              expectedHash: options.definition.schemaIdentity.hash,
              actualVersion: request.schema.version,
              actualHash: request.schema.hash
            })
          }
          const page = Protocol.PullPage.make({
            entries,
            hasMore: metadata.length > entries.length || rows.length > entries.length,
            serverSchema: options.definition.schemaIdentity
          })
          return page
        })).pipe(
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("ServerStore.pull", {
            attributes: { "space.id": request.spaceId, "server.after": request.after, "page.limit": request.limit }
          })
        )
      const bootstrap = (request: Protocol.BootstrapRequest) =>
        sql.withTransaction(Effect.gen(function*() {
          const callerDefinition = yield* validateCallerSchema(request.schema)
          const stored = yield* findSnapshot({ spaceId: request.spaceId, snapshotId: request.snapshotId }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          let manifest: Protocol.SnapshotManifest
          let afterOrdinal = request.afterOrdinal
          let projectionTarget: Identity.SchemaIdentity | undefined
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
            callerDefinition.schemaIdentity.version !== options.definition.schemaIdentity.version ||
            callerDefinition.schemaIdentity.hash !== options.definition.schemaIdentity.hash
          ) {
            projectionTarget = callerDefinition.schemaIdentity
            manifest = yield* ensureSnapshotProjection(manifest, callerDefinition)
          }
          if (afterOrdinal >= manifest.entityCount) {
            return yield* new ReplicaError.CursorGap({
              expected: Math.max(-1, manifest.entityCount - 1),
              actual: afterOrdinal
            })
          }
          let metadata: ReadonlyArray<typeof Rows.SnapshotEntityMetadataRow.Type>
          if (projectionTarget === undefined) {
            metadata = yield* findSnapshotEntityMetadata({
              spaceId: request.spaceId,
              snapshotId: manifest.snapshotId,
              after: afterOrdinal,
              limit: request.limit + 1
            }).pipe(Effect.mapError(StorageUnavailable.make))
          } else {
            metadata = yield* findSnapshotProjectionEntityMetadata({
              spaceId: request.spaceId,
              snapshotId: manifest.snapshotId,
              target: projectionTarget,
              after: afterOrdinal,
              limit: request.limit + 1
            }).pipe(Effect.mapError(StorageUnavailable.make))
          }
          const emptyMoreBytes = yield* Protocol.encodedBytesEffect(
            Protocol.BootstrapPage.make({
              manifest,
              entities: [],
              hasMore: true,
              serverSchema: options.definition.schemaIdentity
            })
          )
          const emptyFinalBytes = yield* Protocol.encodedBytesEffect(
            Protocol.BootstrapPage.make({
              manifest,
              entities: [],
              hasMore: false,
              serverSchema: options.definition.schemaIdentity
            })
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
            if (projectionTarget === undefined) {
              rows = yield* findSnapshotEntityWire({
                spaceId: request.spaceId,
                snapshotId: manifest.snapshotId,
                after: afterOrdinal,
                through
              }).pipe(Effect.mapError(StorageUnavailable.make))
            } else {
              rows = yield* findSnapshotProjectionEntityWire({
                spaceId: request.spaceId,
                snapshotId: manifest.snapshotId,
                target: projectionTarget,
                after: afterOrdinal,
                through
              }).pipe(Effect.mapError(StorageUnavailable.make))
            }
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
          const page = Protocol.BootstrapPage.make({
            manifest,
            entities,
            hasMore,
            serverSchema: options.definition.schemaIdentity
          })
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
                      AND r.membership_incarnation = c.membership_incarnation
                      AND r.terminal_sequence <= ${through}
                  ), expired_local_sequence))
                  WHERE c.space_id = ${candidate.manifest.spaceId} AND EXISTS (
                    SELECT 1 FROM effect_local_server_receipts AS r
                    WHERE r.space_id = c.space_id AND r.client_id = c.client_id
                      AND r.membership_incarnation = c.membership_incarnation
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
      const watch = (request: Protocol.WatchRequest) =>
        Stream.unwrap(
          RcMap.get(wakes, request.spaceId).pipe(
            Effect.flatMap((channel) => PubSub.subscribe(channel)),
            Effect.flatMap((subscription) =>
              findSpace(request.spaceId).pipe(
                Effect.mapError(StorageUnavailable.make),
                Effect.flatMap((stored) =>
                  Effect.gen(function*() {
                    if (Option.isSome(stored)) yield* validateStoredSpace(stored.value)
                    let sequence = Identity.ServerSequence.make(0)
                    if (Option.isSome(stored)) {
                      sequence = Identity.ServerSequence.make(stored.value.next_server_sequence - 1)
                    }
                    return Stream.succeed({ spaceId: request.spaceId, sequence } satisfies Protocol.Wake).pipe(
                      Stream.concat(Stream.fromSubscription(subscription))
                    )
                  })
                )
              )
            )
          )
        )

      const trustedSubmitRequest = (
        request: Protocol.SubmitRequest | Protocol.MutationEnvelope
      ): Protocol.SubmitRequest => {
        if (Schema.is(Protocol.MutationEnvelope)(request)) {
          return { envelope: request, schema: options.definition.schemaIdentity }
        }
        return request
      }
      const trustedPullRequest = (
        request: Protocol.PullRequest | Omit<Protocol.PullRequest, "schema">
      ): Protocol.PullRequest => {
        if (Schema.is(Protocol.PullRequest)(request)) return request
        return { ...request, schema: options.definition.schemaIdentity }
      }
      const trustedWatchRequest = (
        request: Protocol.WatchRequest | Identity.SpaceId
      ): Protocol.WatchRequest => {
        if (typeof request === "string") {
          return { spaceId: request, schema: options.definition.schemaIdentity }
        }
        return request
      }
      const trustedBootstrapRequest = (
        request: Protocol.BootstrapRequest | Omit<Protocol.BootstrapRequest, "schema">
      ): Protocol.BootstrapRequest => {
        if (Schema.is(Protocol.BootstrapRequest)(request)) return request
        return { ...request, schema: options.definition.schemaIdentity }
      }

      return ServerStore.of({
        submit: (request) => admit(trustedSubmitRequest(request), null),
        admit,
        discard,
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
              Effect.as(watch(request))
            )
          )
        },
        watchAuthorized: (request, principal) =>
          authorizeRead(request.spaceId, principal).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.as(watch(request))
          )
      })
    })
  )

export const layerTrusted = (
  options: {
    readonly definition: Definition.Any
    readonly evolution?: Evolution.Evolution
    readonly schemaEvolutionBatchSize?: number
    readonly schemaEvolutionBatchBytes?: number
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
