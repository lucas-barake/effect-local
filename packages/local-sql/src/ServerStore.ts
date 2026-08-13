import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as RcMap from "effect/RcMap"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AcceptedLog from "./internal/acceptedLog.js"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as ReadAuthorization from "./internal/readAuthorization.js"
import * as Rows from "./internal/rows.js"
import * as ScopedReplication from "./internal/scopedReplication.js"
import * as ServerIndex from "./internal/serverIndex.js"
import * as ServerMetrics from "./internal/serverMetrics.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as TerminalRejection from "./internal/TerminalRejection.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as WakeVisibility from "./internal/wakeVisibility.js"
import * as WindowSchema from "./internal/windowSchema.js"
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
    request: Protocol.WatchRequest
  ) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
  readonly bootstrap: (
    request: Protocol.BootstrapRequest | Omit<Protocol.BootstrapRequest, "schema">
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly bootstrapAuthorized: (
    request: Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly prepareBootstrapAuthorized: (
    request: Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<
    Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>,
    ReplicaError.ReplicaError
  >
  readonly maintain: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly maintainAll: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly invalidateReadAuthorization: (
    spaceId: Identity.SpaceId
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watchAuthorized: (
    request: Protocol.WatchRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>, ReplicaError.ReplicaError>
}

export class ServerStore extends Context.Service<ServerStore, Service>()(
  "@lucas-barake/effect-local-sql/ServerStore"
) {}

type AuthorizationRejection = Schema.JsonObject & { readonly _tag: string }

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
  }) => Effect.Effect<void, AuthorizationRejection, R>
  readonly authorizeMutation: (input: {
    readonly mutation: MutationRuntime.CurrentMutationView
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, AuthorizationRejection, R>
  readonly authorizeRead: (input: ReadAuthorizationInput) => Effect.Effect<void, AuthorizationRejection, R>
  readonly readAuthorizationRefreshInterval: Duration.Input
  readonly wakeCapacity?: number
  readonly maximumWatchersPerSpace: number
  readonly maximumConcurrentReadAuthorizations: number
  readonly maximumPendingReadAuthorizations: number
  readonly readAuthorizationCacheCapacity: number
}

interface ReadAuthorizationCommon {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly scope: Protocol.ReplicationScope
  readonly principal: typeof Schema.Json.Type
}

export type ReadAuthorizationInput =
  | ReadAuthorizationCommon & { readonly _tag: "Scope" }
  | ReadAuthorizationCommon & {
    readonly _tag: "Entity"
    readonly entity: Protocol.EntityKey
    readonly value: typeof Schema.Json.Type
  }

interface GlobalSnapshotManifest {
  readonly spaceId: Identity.SpaceId
  readonly definitionHash: string
  readonly schema: Identity.SchemaIdentity
  readonly snapshotId: Identity.SnapshotId
  readonly sequence: Identity.ServerSequence
  readonly terminalSequenceThrough: Identity.TerminalSequence
  readonly entityCount: number
  readonly contentBytes: number
  readonly digest: Protocol.SnapshotDigest
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
  pipe(
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
      const readAuthorizationRefresh = yield* pipe(
        Duration.fromInput(options.readAuthorizationRefreshInterval),
        Option.match({
          onNone: () =>
            Effect.fail(
              new ReplicaError.InvalidConfiguration({
                option: "readAuthorizationRefreshInterval",
                message: "readAuthorizationRefreshInterval must be a valid positive finite duration"
              })
            ),
          onSome: (duration) => {
            if (Duration.isPositive(duration) && Duration.isFinite(duration)) return Effect.succeed(duration)
            return Effect.fail(
              new ReplicaError.InvalidConfiguration({
                option: "readAuthorizationRefreshInterval",
                message: "readAuthorizationRefreshInterval must be a valid positive finite duration"
              })
            )
          }
        })
      )
      const readAuthorizationRefreshMillis = Duration.toMillis(readAuthorizationRefresh)
      const readAuthorizationRefreshNanos = pipe(
        Math.round(readAuthorizationRefreshMillis * 1_000_000),
        BigInt
      )
      const wakeCapacity = options.wakeCapacity ?? 1_024
      for (
        const option of [
          "maximumWatchersPerSpace",
          "maximumConcurrentReadAuthorizations",
          "maximumPendingReadAuthorizations",
          "readAuthorizationCacheCapacity"
        ] as const
      ) {
        if (!Number.isSafeInteger(options[option]) || options[option] <= 0) {
          return yield* new ReplicaError.InvalidConfiguration({
            option,
            message: `${option} must be a positive safe integer`
          })
        }
      }
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
      const serverIndexes = yield* ServerIndex.make(sql, options.definition)
      const metrics = ServerMetrics.make({
        history: options.maximumHistoryEntries,
        receipts: options.maximumReceipts
      })
      const readMetricDepths = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Schema.Struct({ history: Schema.Number, receipts: Schema.Number }),
        execute: () =>
          sql`SELECT
          COALESCE((SELECT history_count FROM effect_local_server_space_counts
            ORDER BY history_count DESC LIMIT 1), 0) AS history,
          COALESCE((SELECT receipt_count FROM effect_local_server_space_counts
            ORDER BY receipt_count DESC LIMIT 1), 0) AS receipts`
      })
      const refreshMetricDepths = readMetricDepths(undefined).pipe(
        Effect.mapError(StorageUnavailable.make),
        Effect.flatMap((depths) => metrics.initializeDepths(depths.history, depths.receipts)),
        Effect.catchCause((cause) => {
          if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
          return Effect.logWarning("Server metric refresh failed", cause)
        })
      )
      yield* refreshMetricDepths
      const metricDepthRefreshRequests = yield* Queue.dropping<void>(1).pipe(
        (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
      )
      yield* Queue.take(metricDepthRefreshRequests).pipe(
        Effect.andThen(Effect.sleep("10 millis")),
        Effect.andThen(refreshMetricDepths),
        Effect.forever,
        Effect.forkScoped
      )
      const scheduleMetricDepthRefresh = Queue.offer(metricDepthRefreshRequests, undefined).pipe(Effect.asVoid)
      const readAuthorizations = yield* ReadAuthorization.make<
        string,
        void,
        ReplicaError.AuthorizationDenied | ReplicaError.CapacityExceeded
      >({
        refreshIntervalNanos: readAuthorizationRefreshNanos,
        lookupTimeoutNanos: readAuthorizationRefreshNanos / 2n,
        onLookupTimeout: () => new ReplicaError.AuthorizationDenied({ reason: { _tag: "ReadAuthorizationExpired" } }),
        maximumConcurrentLookups: options.maximumConcurrentReadAuthorizations,
        maximumPendingLookups: options.maximumPendingReadAuthorizations,
        onPendingCapacityExceeded: () =>
          new ReplicaError.CapacityExceeded({
            resource: "read authorizations",
            limit: options.maximumPendingReadAuthorizations
          }),
        completedCacheCapacity: options.readAuthorizationCacheCapacity
      })
      const wakeVisibilityLimiter = yield* WakeVisibility.makeLimiter<ReplicaError.ReplicaError>({
        lookupTimeoutNanos: readAuthorizationRefreshNanos / 2n,
        onLookupTimeout: () => new ReplicaError.AuthorizationDenied({ reason: { _tag: "ReadAuthorizationExpired" } }),
        maximumConcurrentLookups: options.maximumConcurrentReadAuthorizations,
        maximumPendingLookups: options.maximumPendingReadAuthorizations,
        onPendingCapacityExceeded: () =>
          new ReplicaError.CapacityExceeded({
            resource: "read authorizations",
            limit: options.maximumPendingReadAuthorizations
          })
      })
      const recordAdmissionMetrics = (receipt: Protocol.Receipt) => {
        let outcome: "accepted" | "rejected" | "expired" = "expired"
        if (receipt._tag === "Accepted") outcome = "accepted"
        if (receipt._tag === "Rejected") outcome = "rejected"
        let rejection = Effect.void
        if (receipt._tag === "Rejected") rejection = metrics.recordRejection(receipt.origin)
        return Effect.all([
          metrics.recordAdmission(outcome),
          scheduleMetricDepthRefresh,
          rejection
        ], { discard: true })
      }
      const recordAdmissionFailure = (error: ReplicaError.ReplicaError) =>
        Effect.all([
          metrics.recordAdmission("failed"),
          metrics.recordRejection(error._tag)
        ], { discard: true })
      interface PublishedWake {
        readonly changes: ReadonlyArray<Protocol.EntityChange>
        readonly publishedAtNanos: bigint
        readonly visibility: WakeVisibility.Coordinator<string, boolean, ReplicaError.ReplicaError>
      }
      const wakes = yield* RcMap.make({
        lookup: (_spaceId: Identity.SpaceId) =>
          Effect.gen(function*() {
            const channel = yield* PubSub.sliding<PublishedWake>(wakeCapacity).pipe(
              (acquire) => Effect.acquireRelease(acquire, PubSub.shutdown)
            )
            const watchers = yield* Semaphore.make(options.maximumWatchersPerSpace)
            return { channel, watchers }
          })
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
      const findAcceptedEntry = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          serverSequence: Identity.ServerSequence
        }),
        Result: Rows.ServerLogRow,
        execute: ({ spaceId, serverSequence }) =>
          sql`SELECT space_id, server_sequence, client_id, membership_incarnation, local_sequence,
            mutation_id, digest, entry_bytes, entry_json, source_schema_version, source_schema_hash
          FROM effect_local_authoritative_log
          WHERE space_id = ${spaceId} AND server_sequence = ${serverSequence}`
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
      const manifestFromRow = (row: typeof Rows.SnapshotManifestRow.Type): GlobalSnapshotManifest => ({
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
      const retryWakeChanges = (
        receipt: Protocol.Receipt,
        envelope: Protocol.MutationEnvelope
      ): Effect.Effect<ReadonlyArray<Protocol.EntityChange> | undefined, ReplicaError.ReplicaError> => {
        if (receipt._tag !== "Accepted") return Effect.succeed(undefined)
        return findAcceptedEntry({
          spaceId: envelope.spaceId,
          serverSequence: receipt.serverSequence
        }).pipe(
          Effect.mapError(StorageUnavailable.make),
          Effect.flatMap(Option.match({
            onNone: () => Effect.succeed(undefined),
            onSome: (row) =>
              Effect.gen(function*() {
                const entry = yield* AcceptedLog.decode(row)
                if (
                  row.space_id !== envelope.spaceId ||
                  row.server_sequence !== receipt.serverSequence ||
                  row.client_id !== envelope.clientId ||
                  row.membership_incarnation !== envelope.membershipIncarnation ||
                  row.local_sequence !== envelope.localSequence ||
                  row.mutation_id !== envelope.mutationId ||
                  row.digest !== envelope.digest
                ) {
                  return yield* new ReplicaError.StorageCorrupt({
                    message: `Accepted entry ${row.server_sequence} conflicts with its submitted mutation`
                  })
                }
                return entry.changes
              })
          }))
        )
      }
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
          Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason: { ...reason } }))
        )
      const authorizeReadScope = (
        request: Protocol.PullRequest | Protocol.BootstrapRequest | Protocol.WatchRequest,
        principal: typeof Schema.Json.Type
      ) =>
        options.authorizeRead({
          _tag: "Scope",
          spaceId: request.spaceId,
          clientId: request.clientId,
          scope: request.scope,
          principal
        }).pipe(
          Effect.provide(context),
          Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason: { ...reason } }))
        )
      const authorizeReadEntity = (
        request: Protocol.PullRequest | Protocol.BootstrapRequest | Protocol.WatchRequest,
        principal: typeof Schema.Json.Type,
        entity: Protocol.EntityKey,
        value: typeof Schema.Json.Type
      ) =>
        options.authorizeRead({
          _tag: "Entity",
          spaceId: request.spaceId,
          clientId: request.clientId,
          scope: request.scope,
          principal,
          entity,
          value
        }).pipe(Effect.provide(context), Effect.result, Effect.map(Result.isSuccess))

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
          return yield* SchemaEvolution.server(evolutionOptions).pipe(Effect.provideService(SqlClient.SqlClient, sql))
        }).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

      const bootstrapEntityFits = (spaceId: Identity.SpaceId, entity: Protocol.SnapshotEntity) =>
        Effect.gen(function*() {
          const change = Protocol.Upsert.make({
            entity: Protocol.EntityKey.make({
              model: entity.model,
              modelVersion: entity.modelVersion,
              key: entity.key
            }),
            value: entity.value
          })
          const entry = Protocol.SnapshotEntry.make({
            ordinal: 0,
            change,
            entryBytes: yield* Protocol.encodedBytesEffect(change)
          })
          const page = Protocol.BootstrapPage.make({
            manifest: {
              spaceId,
              clientId: Identity.ClientId.make("cli_ffffffff-ffff-4fff-bfff-ffffffffffff"),
              definitionHash: options.definition.hash,
              schema: options.definition.schemaIdentity,
              scopeDigest: pipe("f".repeat(64), (value) => Protocol.MutationDigest.make(value)),
              scopeGeneration: Identity.ReplicationScopeGeneration.make(Number.MAX_SAFE_INTEGER),
              cursor: Protocol.ReplicationCursor.make({
                viewId: Identity.ReplicationViewId.make("viw_ffffffff-ffff-4fff-bfff-ffffffffffff"),
                revision: Identity.ReplicationViewRevision.make(Number.MAX_SAFE_INTEGER)
              }),
              snapshotId: Identity.SnapshotId.make("snp_ffffffff-ffff-4fff-bfff-ffffffffffff"),
              sequence: Identity.ServerSequence.make(Number.MAX_SAFE_INTEGER),
              terminalSequenceThrough: Identity.TerminalSequence.make(Number.MAX_SAFE_INTEGER),
              entityCount: options.maximumSnapshotEntities,
              contentBytes: options.maximumSnapshotBytes,
              digest: pipe("f".repeat(64), (value) => Protocol.SnapshotDigest.make(value))
            },
            entries: [entry],
            hasMore: true,
            serverSchema: options.definition.schemaIdentity
          })
          return (yield* Protocol.encodedBytesEffect(page)) <= options.maximumBootstrapPageBytes
        })

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
        let wakeChanges: ReadonlyArray<Protocol.EntityChange> | undefined
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
            const receipt = yield* decodeStoredReceipt(exactReceipt.value, submittedEnvelope)
            wakeChanges = yield* retryWakeChanges(receipt, submittedEnvelope)
            return yield* SchemaEvolution.migrateReceipt(receipt, evolution).pipe(
              Effect.flatMap((migratedReceipt) => projectReceipt(migratedReceipt, callerDefinition))
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
          return yield* Effect.gen(function*() {
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
              const receipt = yield* decodeStoredReceipt(committedByMutation.value, envelope)
              wakeChanges = yield* retryWakeChanges(receipt, envelope)
              return yield* SchemaEvolution.migrateReceipt(receipt, evolution).pipe(
                Effect.flatMap((migratedReceipt) => projectReceipt(migratedReceipt, callerDefinition))
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
              return yield* pipe(
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
                (receipt) => projectReceipt(receipt, callerDefinition)
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
                ...(yield* rejectedReceipt(envelope, mutation, { ...authorization.failure }, "Authorization")),
                terminalSequence: Identity.TerminalSequence.make(storedSpace.next_terminal_sequence)
              }
            } else {
              const changes: Array<Protocol.EntityChange> = []
              const mutationName = mutation.name
              const mutationPayload = mutation.payload
              const transaction = SqlTransaction.server({
                sql,
                definition: options.definition,
                spaceId: envelope.spaceId,
                generation: storedSpace.active_schema_generation,
                changes
              })
              const executed = yield* runtime.execute(
                mutationName,
                mutationPayload,
                transaction,
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
                ),
                (effect) => sql.withTransaction(effect),
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
                yield* serverIndexes.apply(
                  envelope.spaceId,
                  storedSpace.active_schema_generation,
                  entry.sequence,
                  entry.changes
                )
                wakeChanges = entry.changes
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
          }).pipe((effect) => sql.withTransaction(effect))
        }).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(new ReplicaError.UnknownCommitOutcome({ mutationId: submittedEnvelope.mutationId, cause }))),
          Effect.tap((receipt) => {
            const changes = wakeChanges
            if (receipt._tag !== "Accepted" || changes === undefined) {
              return Effect.void
            }
            return RcMap.has(wakes, submittedEnvelope.spaceId).pipe(
              Effect.flatMap((hasWatchers) => {
                if (hasWatchers) {
                  return Effect.scoped(
                    RcMap.get(wakes, submittedEnvelope.spaceId).pipe(
                      Effect.flatMap(({ channel }) =>
                        Clock.monotonicTimeNanos.pipe(
                          Effect.flatMap((publishedAtNanos) =>
                            PubSub.publish(channel, {
                              changes,
                              publishedAtNanos,
                              visibility: WakeVisibility.make<string, boolean, ReplicaError.ReplicaError>(
                                wakeVisibilityLimiter
                              )
                            })
                          )
                        )
                      )
                    )
                  )
                }
                return Effect.void
              }),
              Effect.andThen(recordAdmissionMetrics(receipt)),
              Effect.asVoid
            )
          }),
          Effect.tap((receipt) => {
            if (receipt._tag === "Accepted" && wakeChanges !== undefined) {
              return Effect.void
            }
            return recordAdmissionMetrics(receipt)
          }),
          Effect.tapError(recordAdmissionFailure),
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
              Effect.flatMap((receipt) =>
                SchemaEvolution.migrateReceipt(receipt, evolution)
              ),
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
          return yield* Effect.gen(function*() {
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
              return yield* pipe(
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
                (receipt) => projectReceipt(receipt, callerDefinition)
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
          }).pipe((effect) => sql.withTransaction(effect))
        }).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ReplicaError.UnknownCommitOutcome({
                mutationId: submittedEnvelope.mutationId,
                cause
              })
            )),
          Effect.tap(recordAdmissionMetrics),
          Effect.tapError(recordAdmissionFailure),
          Effect.withSpan("ServerStore.discard", {
            attributes: { "mutation.id": submittedEnvelope.mutationId, "space.id": submittedEnvelope.spaceId }
          })
        )
      }

      const prepareSnapshot = (spaceId: Identity.SpaceId) =>
        Effect.gen(function*() {
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
            manifest: {
              spaceId,
              definitionHash: meta.definition_hash,
              schema: Identity.SchemaIdentity.make({ version: meta.schema_version, hash: meta.schema_hash }),
              snapshotId,
              sequence: Identity.ServerSequence.make(meta.next_server_sequence - 1),
              terminalSequenceThrough: Identity.TerminalSequence.make(meta.next_terminal_sequence - 1),
              entityCount: decoded.entities.length,
              contentBytes: decoded.contentBytes,
              digest: decoded.digest
            },
            entities: decoded.entities
          })
        }).pipe(
          (effect) => sql.withTransaction(effect),
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )

      const publishAndPrune = Option.match({
        onNone: () => Effect.succeed({ history: 0, receipts: 0 }),
        onSome: (
          candidate: Effect.Success<ReturnType<typeof prepareSnapshot>> extends Option.Option<infer A> ? A : never
        ) =>
          Effect.gen(function*() {
            const meta = yield* lockSpace(candidate.manifest.spaceId).pipe(Effect.mapError(StorageUnavailable.make))
            yield* validateStoredSpace(meta)
            if (
              meta.next_server_sequence !== candidate.observedNextServer ||
              meta.next_terminal_sequence !== candidate.observedNextTerminal ||
              meta.schema_generation !== candidate.observedSchemaGeneration
            ) return { history: 0, receipts: 0 }
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
                const snapshotBatch = snapshotRows.slice(offset, offset + 100)
                yield* sql`INSERT INTO effect_local_server_snapshot_entities
                    ${sql.insert(snapshotBatch)}`
              }
            }
            const historyFloor = pipe(
              Math.max(meta.history_floor, candidate.manifest.sequence - options.retainedHistoryEntries),
              (value) => Identity.ServerSequence.make(value)
            )
            const receiptFloor = pipe(
              Math.max(
                meta.receipt_floor,
                candidate.manifest.terminalSequenceThrough - options.retainedReceipts
              ),
              (value) => Identity.TerminalSequence.make(value)
            )
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
              yield* sql`DELETE FROM effect_local_server_index_partition_log
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
            return { history: history.length, receipts: receipts.length }
          }).pipe(
            (effect) => sql.withTransaction(effect),
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
      })

      const maintainSpace = (spaceId: Identity.SpaceId) =>
        prepareSnapshot(spaceId).pipe(
          Effect.flatMap(publishAndPrune),
          Effect.tap((pruned) =>
            Effect.all([
              metrics.recordMaintenance("completed"),
              metrics.recordPruned("history", pruned.history),
              metrics.recordPruned("receipt", pruned.receipts)
            ], { discard: true })
          ),
          Effect.tapError(() => metrics.recordMaintenance("failed")),
          Effect.asVoid,
          Effect.withSpan("ServerStore.maintain", { attributes: { "space.id": spaceId } })
        )
      const maintain = (spaceId: Identity.SpaceId) =>
        maintainSpace(spaceId).pipe(Effect.ensuring(scheduleMetricDepthRefresh))
      const maintainAll = Effect.gen(function*() {
        let after = ""
        while (true) {
          const spaces = yield* findSpaces({ after, limit: options.maintenanceSpaceBatchSize }).pipe(
            Effect.mapError(StorageUnavailable.make)
          )
          if (spaces.length === 0) return
          yield* Effect.forEach(spaces, ({ space_id }) => maintainSpace(space_id), {
            concurrency: options.maintenanceConcurrency,
            discard: true
          })
          after = spaces.at(-1)!.space_id
        }
      }).pipe(Effect.ensuring(scheduleMetricDepthRefresh))
      const projectScopedEntity = (
        target: Definition.Any,
        entity: Protocol.EntityKey,
        value: typeof Schema.Json.Type
      ) =>
        Effect.gen(function*() {
          if (!target.modelByName.has(entity.model)) return Option.none()
          if (
            target.schemaIdentity.version === options.definition.schemaIdentity.version &&
            target.schemaIdentity.hash === options.definition.schemaIdentity.hash
          ) return Option.some({ entity, value })
          const migrated = yield* Evolution.migrateModelTo({
            evolution,
            source: options.definition.schemaIdentity,
            target: target.schemaIdentity,
            model: entity.model,
            modelVersion: entity.modelVersion,
            key: entity.key,
            value
          })
          if (migrated.value === undefined) {
            return yield* new ReplicaError.SchemaEvolutionUnsupported({
              sourceVersion: options.definition.schemaIdentity.version,
              sourceHash: options.definition.schemaIdentity.hash,
              targetVersion: target.schemaIdentity.version,
              targetHash: target.schemaIdentity.hash
            })
          }
          return Option.some({
            entity: Protocol.EntityKey.make({
              model: entity.model,
              modelVersion: migrated.modelVersion,
              key: migrated.key
            }),
            value: migrated.value
          })
        })
      const scopedReplication = ScopedReplication.make({
        sql,
        crypto,
        definition: options.definition,
        maximumSnapshotEntities: options.maximumSnapshotEntities,
        maximumSnapshotBytes: options.maximumSnapshotBytes,
        maximumBootstrapPageBytes: options.maximumBootstrapPageBytes,
        authorization: {
          scope: authorizeReadScope,
          entity: authorizeReadEntity
        },
        resolveDefinition: validateCallerSchema,
        projectEntity: projectScopedEntity,
        windows: serverIndexes
      })
      const preauthorizedScopedReplication = ScopedReplication.make({
        sql,
        crypto,
        definition: options.definition,
        maximumSnapshotEntities: options.maximumSnapshotEntities,
        maximumSnapshotBytes: options.maximumSnapshotBytes,
        maximumBootstrapPageBytes: options.maximumBootstrapPageBytes,
        authorization: {
          scope: () => Effect.void,
          entity: authorizeReadEntity
        },
        resolveDefinition: validateCallerSchema,
        projectEntity: projectScopedEntity,
        windows: serverIndexes
      })
      const countAcknowledgedEntities = SqlSchema.findOne({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          clientId: Identity.ClientId,
          principalDigest: Protocol.MutationDigest,
          entitiesJson: Schema.String
        }),
        Result: Rows.CountRow,
        execute: ({ spaceId, clientId, principalDigest, entitiesJson }) =>
          sql`SELECT EXISTS(
            SELECT 1 FROM json_each(${entitiesJson}) AS requested
            WHERE EXISTS(
              SELECT 1 FROM effect_local_server_replication_view_entities AS acknowledged
              INNER JOIN effect_local_server_replication_views AS current
                ON current.space_id = acknowledged.space_id AND current.client_id = acknowledged.client_id
                AND current.principal_digest = acknowledged.principal_digest
                AND current.view_id = acknowledged.view_id
              WHERE acknowledged.space_id = ${spaceId} AND acknowledged.client_id = ${clientId}
                AND acknowledged.principal_digest = ${principalDigest}
                AND acknowledged.model = json_extract(requested.value, '$.model')
                AND acknowledged.entity_key = json_extract(requested.value, '$.key')
                AND acknowledged.disposition = 'Upsert'
            )
          ) AS count`
      })
      const mutationWakeVisible = (
        request: Protocol.WatchRequest,
        principal: typeof Schema.Json.Type,
        changes: ReadonlyArray<Protocol.EntityChange>
      ) =>
        Effect.gen(function*() {
          const scoped = changes.filter((change) =>
            Protocol.replicationScopeCoversModel(request.scope, change.entity.model)
          )
          if (scoped.length === 0) return false
          const identities = yield* Effect.forEach(scoped, (change) =>
            Codec.stringify(change.entity.key).pipe(
              Effect.map((key) => ({ model: change.entity.model, key }))
            ))
          const principalHash = yield* Canonical.digest({ format: 1, principal }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.map((value) => Protocol.MutationDigest.make(value))
          )
          const acknowledged = yield* countAcknowledgedEntities({
            spaceId: request.spaceId,
            clientId: request.clientId,
            principalDigest: principalHash,
            entitiesJson: yield* Codec.stringify(identities)
          }).pipe(Effect.mapError(StorageUnavailable.make))
          if (acknowledged.count > 0) return true
          for (const change of scoped) {
            if (
              change._tag === "Upsert" &&
              (yield* authorizeReadEntity(request, principal, change.entity, change.value))
            ) return true
          }
          return false
        })
      const captureReadAuthorization = (
        request: Protocol.WatchRequest,
        principal: typeof Schema.Json.Type
      ) =>
        Effect.gen(function*() {
          const scope = yield* Protocol.validateReplicationScope(options.definition, request.scope)
          const encoded = yield* Canonical.stringifyEffect({
            spaceId: request.spaceId,
            clientId: request.clientId,
            scope,
            principal
          })
          const captured = yield* Schema.Struct({
            spaceId: Identity.SpaceId,
            clientId: Identity.ClientId,
            scope: Protocol.ReplicationScope,
            principal: Schema.Json
          }).pipe(
            Schema.fromJsonString,
            (schema) => Codec.decode(schema, encoded)
          )
          const capturedRequest = { ...request, ...captured }
          return {
            key: encoded,
            request: capturedRequest,
            principal: captured.principal,
            lookup: () => authorizeReadScope(capturedRequest, captured.principal)
          }
        })
      const watch = (
        request: Protocol.WatchRequest,
        principal: typeof Schema.Json.Type,
        authorization?: Effect.Success<ReturnType<typeof captureReadAuthorization>>
      ) =>
        Effect.gen(function*() {
          const parentScope = yield* Effect.scope
          let authorized: ReadAuthorization.Success<void> | undefined
          const capturedVisibility = authorization ?? (yield* captureReadAuthorization(request, principal))
          if (authorization !== undefined) {
            authorized = yield* readAuthorizations.authorize(authorization.key, authorization.lookup).pipe(
              Effect.tapErrorTag("CapacityExceeded", () => metrics.recordRejection("CapacityExceeded"))
            )
          }
          const childScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(childScope, Exit.void))
          const state = yield* RcMap.get(wakes, request.spaceId).pipe(Scope.provide(childScope))
          yield* Effect.gen(function*() {
            if (!(yield* state.watchers.takeIfAvailable(1))) {
              yield* metrics.recordRejection("CapacityExceeded")
              yield* new ReplicaError.CapacityExceeded({
                resource: "sync watchers",
                limit: options.maximumWatchersPerSpace
              })
            }
          }).pipe(
            (acquire) => Effect.acquireRelease(acquire, () => state.watchers.release(1).pipe(Effect.asVoid)),
            Scope.provide(childScope)
          )
          yield* metrics.changeWatchers(1).pipe(
            (acquire) => Effect.acquireRelease(acquire, () => metrics.changeWatchers(-1)),
            Scope.provide(childScope)
          )
          const subscription = yield* PubSub.subscribe(state.channel).pipe(Scope.provide(childScope))
          const mutations = Stream.fromSubscription(subscription).pipe(Stream.map(Option.some))
          const refreshes = Stream.tick(readAuthorizationRefreshMillis).pipe(
            Stream.drop(1),
            Stream.map(() => Option.none<PublishedWake>())
          )
          const outputWakes = Option.none<PublishedWake>().pipe(
            Stream.succeed,
            Stream.concat(Stream.merge(mutations, refreshes)),
            Stream.filterEffect((signal) => {
              if (Option.isNone(signal)) return Effect.succeed(true)
              return signal.value.visibility.evaluate(
                capturedVisibility.key,
                () =>
                  mutationWakeVisible(
                    capturedVisibility.request,
                    capturedVisibility.principal,
                    signal.value.changes
                  )
              )
            }),
            Stream.mapEffect((signal) => {
              if (Option.isNone(signal)) return Effect.succeed(Protocol.Wake.make({ spaceId: request.spaceId }))
              return Clock.monotonicTimeNanos.pipe(
                Effect.tap((deliveredAtNanos) =>
                  metrics.recordWakeFanout(deliveredAtNanos - signal.value.publishedAtNanos)
                ),
                Effect.as(Protocol.Wake.make({ spaceId: request.spaceId }))
              )
            })
          )
          if (authorization === undefined || authorized === undefined) return outputWakes
          const revoked = yield* Deferred.make<never, ReplicaError.ReplicaError>()
          const failClosed = (error: ReplicaError.AuthorizationDenied) =>
            Deferred.fail(revoked, error).pipe(
              Effect.tap(() => Scope.close(childScope, Exit.void).pipe(Effect.forkIn(parentScope))),
              Effect.asVoid
            )
          const monitor = (current: ReadAuthorization.Success<void>): Effect.Effect<void> =>
            Effect.gen(function*() {
              const now = yield* Clock.monotonicTimeNanos
              const refreshAt = current.expiresAtNanos -
                pipe(Math.floor(readAuthorizationRefreshMillis * 500_000), BigInt)
              if (refreshAt > now) {
                const refreshDelay = Duration.nanos(refreshAt - now)
                yield* Effect.sleep(refreshDelay)
              }
              const refreshStartedAt = yield* Clock.monotonicTimeNanos
              const remaining = current.expiresAtNanos - refreshStartedAt
              if (remaining <= 0n) {
                return yield* failClosed(
                  new ReplicaError.AuthorizationDenied({ reason: { _tag: "ReadAuthorizationExpired" } })
                )
              }
              const refreshed = yield* readAuthorizations.refresh(
                authorization.key,
                current.generation,
                authorization.lookup
              ).pipe(
                Effect.timeoutOption(Duration.nanos(remaining)),
                Effect.exit
              )
              if (Exit.isFailure(refreshed)) {
                return yield* Deferred.failCause(revoked, refreshed.cause).pipe(
                  Effect.tap(() => Scope.close(childScope, Exit.void).pipe(Effect.forkIn(parentScope))),
                  Effect.asVoid
                )
              }
              if (Option.isNone(refreshed.value)) {
                return yield* failClosed(
                  new ReplicaError.AuthorizationDenied({ reason: { _tag: "ReadAuthorizationExpired" } })
                )
              }
              return yield* monitor(refreshed.value.value)
            })
          yield* monitor(authorized).pipe(Effect.forkScoped)
          return Deferred.await(revoked).pipe(
            Stream.fromEffect,
            (revocation) => Stream.merge(outputWakes, revocation)
          )
        }).pipe((effect) => Stream.unwrap(effect))

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
      const trustedBootstrapRequest = (
        request: Protocol.BootstrapRequest | Omit<Protocol.BootstrapRequest, "schema">
      ): Protocol.BootstrapRequest => {
        if (Schema.is(Protocol.BootstrapRequest)(request)) return request
        return { ...request, schema: options.definition.schemaIdentity }
      }

      const prepareBootstrapAuthorized = (
        request: Protocol.BootstrapRequest,
        principal: typeof Schema.Json.Type
      ) =>
        Effect.gen(function*() {
          const authorizationSchema = Schema.Struct({
            request: Protocol.BootstrapRequest,
            principal: Schema.Json
          }).pipe(Schema.fromJsonString)
          const captured = yield* Codec.decode(
            authorizationSchema,
            yield* Canonical.stringifyEffect({ request, principal })
          )
          const callerDefinition = yield* validateCallerSchema(captured.request.schema)
          const normalizedRequest = {
            ...captured.request,
            scope: yield* Protocol.validateReplicationScope(callerDefinition, captured.request.scope)
          }
          yield* authorizeReadScope(normalizedRequest, captured.principal)
          return prepareSpace(normalizedRequest.spaceId, normalizedRequest.schema).pipe(
            Effect.flatMap((generation) =>
              preauthorizedScopedReplication.bootstrap(normalizedRequest, captured.principal, generation)
            )
          )
        })

      return ServerStore.of({
        submit: (request) => pipe(trustedSubmitRequest(request), (trusted) => admit(trusted, null)),
        admit,
        discard,
        pull: (input) => {
          const request = trustedPullRequest(input)
          return prepareSpace(request.spaceId, request.schema).pipe(
            Effect.flatMap((generation) => scopedReplication.pull(request, null, generation))
          )
        },
        pullAuthorized: (request, principal) =>
          authorizeReadScope(request, principal).pipe(
            Effect.andThen(prepareSpace(request.spaceId, request.schema)),
            Effect.flatMap((generation) => scopedReplication.pull(request, principal, generation))
          ),
        bootstrap: (input) => {
          const request = trustedBootstrapRequest(input)
          return prepareSpace(request.spaceId, request.schema).pipe(
            Effect.flatMap((generation) => scopedReplication.bootstrap(request, null, generation))
          )
        },
        bootstrapAuthorized: (request, principal) =>
          prepareBootstrapAuthorized(request, principal).pipe(Effect.flatten),
        prepareBootstrapAuthorized,
        maintain,
        maintainAll,
        invalidateReadAuthorization: (spaceId) =>
          sql`UPDATE effect_local_server_spaces SET read_auth_epoch = read_auth_epoch + 1
            WHERE space_id = ${spaceId}`.pipe(
            Effect.asVoid,
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
            Effect.withSpan("ServerStore.invalidateReadAuthorization", { attributes: { "space.id": spaceId } })
          ),
        watch: (request) => {
          return Protocol.validateReplicationScope(options.definition, request.scope).pipe(
            Effect.tap((scope) => WindowSchema.validate(scope, request.schema, options.definition.schemaIdentity)),
            Effect.flatMap((scope) => prepareSpace(request.spaceId, request.schema).pipe(Effect.as(scope))),
            Effect.map((scope) => watch({ ...request, scope }, null)),
            (effect) => Stream.unwrap(effect)
          )
        },
        watchAuthorized: (request, principal) =>
          Effect.gen(function*() {
            const authorization = yield* captureReadAuthorization(request, principal)
            yield* readAuthorizations.authorize(authorization.key, authorization.lookup).pipe(
              Effect.tapErrorTag("CapacityExceeded", () => metrics.recordRejection("CapacityExceeded"))
            )
            yield* WindowSchema.validate(
              authorization.request.scope,
              authorization.request.schema,
              options.definition.schemaIdentity
            )
            yield* prepareSpace(request.spaceId, request.schema)
            return watch(authorization.request, authorization.principal, authorization)
          })
      })
    }),
    Layer.effect(ServerStore)
  )

export const layerTrusted = (
  options: {
    readonly definition: Definition.Any
    readonly evolution?: Evolution.Evolution
    readonly schemaEvolutionBatchSize?: number
    readonly schemaEvolutionBatchBytes?: number
    readonly readAuthorizationRefreshInterval: Duration.Input
    readonly wakeCapacity?: number
    readonly maximumWatchersPerSpace: number
    readonly maximumConcurrentReadAuthorizations: number
    readonly maximumPendingReadAuthorizations: number
    readonly readAuthorizationCacheCapacity: number
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
  pipe(
    Effect.gen(function*() {
      const store = yield* ServerStore
      const intervalMillis = yield* Configuration.positiveFiniteDurationMillis(
        "historyMaintenance.interval",
        options.interval
      )
      if (options.runOnStart === true) yield* store.maintainAll
      yield* Effect.sleep(intervalMillis).pipe(
        Effect.andThen(store.maintainAll),
        Effect.catch((error) => Effect.logError("History maintenance failed", error)),
        Effect.forever,
        Effect.forkScoped
      )
      return HistoryMaintenance.of({ run: store.maintainAll })
    }),
    Layer.effect(HistoryMaintenance)
  )
