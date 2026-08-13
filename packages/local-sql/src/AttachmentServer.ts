import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AttachmentStorage from "./AttachmentStorage.js"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

type AuthorizationRejection = Schema.JsonObject & { readonly _tag: string }

interface AuthorizationInput {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly membershipIncarnation: Identity.MembershipIncarnation
  readonly reference: Attachment.Reference
  readonly principal: typeof Schema.Json.Type
}

export interface Options<R = never,> {
  readonly maximumObjectBytes: number
  readonly maximumObjectsPerSpace: number
  readonly maximumBytesPerSpace: number
  readonly uploadGrantLifetime: Duration.Input
  readonly uploadLeaseLifetime: Duration.Input
  readonly stagingLifetime: Duration.Input
  readonly garbageCollectionGracePeriod: Duration.Input
  readonly deletionBatchSize: number
  readonly authorizeAccess: (input: AuthorizationInput) => Effect.Effect<void, AuthorizationRejection, R>
  readonly authorizeUpload: (input: AuthorizationInput) => Effect.Effect<void, AuthorizationRejection, R>
  readonly authorizeRead: (
    input: AuthorizationInput & {
      readonly entity: {
        readonly model: string
        readonly modelVersion: Identity.SchemaVersion
        readonly key: Schema.Json
      }
      readonly value: Schema.Json
    }
  ) => Effect.Effect<void, AuthorizationRejection, R>
}

export interface PreparedUpload {
  readonly objectKey: AttachmentStorage.ObjectKey
  readonly offset: number
  readonly complete: boolean
}

export interface Service {
  readonly prepareUpload: (input: AuthorizationInput) => Effect.Effect<PreparedUpload, ReplicaError.ReplicaError>
  readonly appendUpload: <E extends { readonly _tag: string }, R,>(
    input: AuthorizationInput & {
      readonly expectedOffset: number
      readonly bytes: Stream.Stream<Uint8Array, E, R>
    }
  ) => Effect.Effect<PreparedUpload, E | ReplicaError.ReplicaError, R>
  readonly replaceEntityReferences: (input: {
    readonly spaceId: Identity.SpaceId
    readonly schemaGeneration: number
    readonly model: string
    readonly modelVersion: Identity.SchemaVersion
    readonly entityKey: string
    readonly value?: Schema.Json
  }) => Effect.Effect<void, ReplicaError.StorageError>
  readonly read: (
    input: AuthorizationInput & { readonly range?: Attachment.Range }
  ) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly prepareRead: (
    input: AuthorizationInput & { readonly range?: Attachment.Range }
  ) => Effect.Effect<Stream.Stream<Uint8Array, ReplicaError.ReplicaError>, ReplicaError.ReplicaError>
  readonly maintain: (spaceId: Identity.SpaceId) => Effect.Effect<number, ReplicaError.ReplicaError>
}

export class AttachmentServer extends Context.Service<AttachmentServer, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentServer"
) {}

const Lookup = Schema.Struct({ spaceId: Identity.SpaceId, digest: Attachment.Digest })
const Count = Schema.Struct({ objects: Schema.Number, bytes: Schema.Number })
const ReadableEntity = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String
})

const readError = (message: string) => (cause: unknown): ReplicaError.StorageError => {
  if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
  return new ReplicaError.StorageCorrupt({ message, cause })
}

const authorizationDenied = (reason: AuthorizationRejection) =>
  new ReplicaError.AuthorizationDenied({ reason: { ...reason } })

export const layer = <R = never,>(options: Options<R>): Layer.Layer<
  AttachmentServer,
  ReplicaError.InvalidConfiguration,
  SqlClient.SqlClient | AttachmentStorage.AttachmentStorage | Crypto.Crypto | R
> =>
  Layer.effect(
    AttachmentServer,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const storage = yield* AttachmentStorage.AttachmentStorage
      const crypto = yield* Crypto.Crypto
      const context = yield* Effect.context<R>()
      const maximumObjectBytes = yield* Configuration.positiveSafeInteger(
        "attachments.maximumObjectBytes",
        options.maximumObjectBytes
      )
      const maximumObjectsPerSpace = yield* Configuration.positiveSafeInteger(
        "attachments.maximumObjectsPerSpace",
        options.maximumObjectsPerSpace
      )
      const maximumBytesPerSpace = yield* Configuration.positiveSafeInteger(
        "attachments.maximumBytesPerSpace",
        options.maximumBytesPerSpace
      )
      const uploadGrantLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.uploadGrantLifetime",
        options.uploadGrantLifetime
      )
      const uploadLeaseLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.uploadLeaseLifetime",
        options.uploadLeaseLifetime
      )
      const stagingLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.stagingLifetime",
        options.stagingLifetime
      )
      const garbageCollectionGracePeriod = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.garbageCollectionGracePeriod",
        options.garbageCollectionGracePeriod
      )
      const deletionBatchSize = yield* Configuration.positiveSafeInteger(
        "attachments.deletionBatchSize",
        options.deletionBatchSize
      )
      const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })
      const withLock = <A, E extends { readonly _tag: string }, R2,>(
        spaceId: Identity.SpaceId,
        digest: Attachment.Digest,
        effect: Effect.Effect<A, E, R2>
      ): Effect.Effect<A, E, R2> =>
        RcMap.get(locks, `${spaceId}:${digest}`).pipe(
          Effect.flatMap((lock) => lock.withPermit(effect)),
          Effect.scoped
        )
      const findObject = SqlSchema.findOneOption({
        Request: Lookup,
        Result: Rows.ServerAttachmentObjectRow,
        execute: ({ digest, spaceId }) =>
          sql`SELECT space_id, digest, bytes, object_key, state, storage_offset,
        lease_token, lease_expires_at, garbage_collect_after, created_at, last_accessed_at
        FROM effect_local_server_attachment_objects WHERE space_id = ${spaceId} AND digest = ${digest}`
      })
      const spaceUsage = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Count,
        execute: (spaceId) =>
          sql`SELECT COUNT(*) AS objects, COALESCE(SUM(bytes), 0) AS bytes
        FROM effect_local_server_attachment_objects WHERE space_id = ${spaceId}`
      })
      const readableEntities = SqlSchema.findAll({
        Request: Lookup,
        Result: ReadableEntity,
        execute: ({ digest, spaceId }) =>
          sql`SELECT e.model, e.model_version, e.entity_key, e.value_json
        FROM effect_local_server_attachment_references AS r
        JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
          AND s.active_schema_generation = r.schema_generation
        JOIN effect_local_server_entities_data AS e ON e.space_id = r.space_id
          AND e.generation = r.schema_generation AND e.model = r.model AND e.entity_key = r.entity_key
        WHERE r.space_id = ${spaceId} AND r.digest = ${digest}
        ORDER BY e.model, e.entity_key`
      })
      const acquireUploadLease = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          digest: Attachment.Digest,
          token: Schema.String,
          now: Schema.Number,
          expiresAt: Schema.Number
        }),
        Result: Rows.ServerAttachmentObjectRow,
        execute: ({ digest, expiresAt, now, spaceId, token }) =>
          sql`UPDATE effect_local_server_attachment_objects SET lease_token = ${token},
          lease_expires_at = ${expiresAt}
          WHERE space_id = ${spaceId} AND digest = ${digest} AND state = 'Staging'
            AND (lease_token IS NULL OR lease_expires_at <= ${now})
          RETURNING space_id, digest, bytes, object_key, state, storage_offset, lease_token,
            lease_expires_at, garbage_collect_after, created_at, last_accessed_at`
      })
      const find = (spaceId: Identity.SpaceId, digest: Attachment.Digest) =>
        findObject({ spaceId, digest }).pipe(Effect.mapError(readError("Server attachment metadata is corrupt")))
      const authorize = (input: AuthorizationInput, upload: boolean) => {
        let uploadAuthorization: Effect.Effect<void, AuthorizationRejection> = Effect.void
        if (upload) uploadAuthorization = options.authorizeUpload(input).pipe(Effect.provide(context))
        return options.authorizeAccess(input).pipe(
          Effect.provide(context),
          Effect.andThen(uploadAuthorization),
          Effect.mapError(authorizationDenied)
        )
      }
      const validateReference = (reference: Attachment.Reference) => {
        if (reference.bytes <= maximumObjectBytes) return Effect.void
        return Effect.fail(new Attachment.AttachmentTooLarge({ limit: maximumObjectBytes }))
      }
      const notFound = (reference: Attachment.Reference) =>
        new Attachment.AttachmentUnavailable({ digest: reference.digest })

      const prepareUpload: Service["prepareUpload"] = Effect.fnUntraced(function*(input) {
        yield* validateReference(input.reference)
        yield* authorize(input, true)
        return yield* withLock(
          input.spaceId,
          input.reference.digest,
          Effect.gen(function*() {
            const now = yield* Clock.currentTimeMillis
            let found = yield* find(input.spaceId, input.reference.digest)
            if (Option.isNone(found)) {
              const objectKey = yield* storage.create()
              const inserted = yield* sql.withTransaction(Effect.gen(function*() {
                yield* sql`UPDATE effect_local_server_spaces SET space_id = space_id
              WHERE space_id = ${input.spaceId}`
                const concurrent = yield* find(input.spaceId, input.reference.digest)
                if (Option.isSome(concurrent)) return false
                const usage = yield* spaceUsage(input.spaceId).pipe(
                  Effect.mapError(readError("Server attachment quota state is corrupt"))
                )
                if (usage.objects >= maximumObjectsPerSpace) {
                  return yield* new ReplicaError.CapacityExceeded({
                    resource: "attachment objects per space",
                    limit: maximumObjectsPerSpace
                  })
                }
                if (usage.bytes + input.reference.bytes > maximumBytesPerSpace) {
                  return yield* new ReplicaError.CapacityExceeded({
                    resource: "attachment bytes per space",
                    limit: maximumBytesPerSpace
                  })
                }
                yield* sql`INSERT INTO effect_local_server_attachment_objects
              (space_id, digest, bytes, object_key, state, storage_offset, created_at, last_accessed_at)
              VALUES (${input.spaceId}, ${input.reference.digest}, ${input.reference.bytes}, ${objectKey},
                'Staging', 0, ${now}, ${now})`
                return true
              })).pipe(
                Effect.onError(() =>
                  storage.remove(objectKey).pipe(
                    Effect.tapError((error) =>
                      Effect.logWarning("Unclaimed attachment file could not be removed").pipe(
                        Effect.annotateLogs("error", error._tag)
                      )
                    ),
                    Effect.ignore
                  )
                ),
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
              )
              if (!inserted) yield* storage.remove(objectKey)
              found = yield* find(input.spaceId, input.reference.digest)
            }
            if (Option.isNone(found)) return yield* notFound(input.reference)
            if (found.value.bytes !== input.reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: found.value.bytes,
                actual: input.reference.bytes
              })
            }
            let offset = found.value.storage_offset
            if (found.value.state === "Staging") {
              offset = yield* storage.offset(found.value.object_key)
              if (offset > input.reference.bytes) {
                return yield* new Attachment.AttachmentLengthMismatch({
                  expected: input.reference.bytes,
                  actual: offset
                })
              }
              const leased = found.value.lease_token !== null &&
                found.value.lease_expires_at !== null && found.value.lease_expires_at > now
              if (offset === input.reference.bytes && !leased) {
                yield* storage.verify(found.value.object_key, input.reference)
                yield* sql`UPDATE effect_local_server_attachment_objects SET state = 'Complete',
              storage_offset = ${offset}, garbage_collect_after = ${now + garbageCollectionGracePeriod},
              last_accessed_at = ${now}
              WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}`
              } else {
                yield* sql`UPDATE effect_local_server_attachment_objects SET storage_offset = ${offset},
              last_accessed_at = ${now}
              WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}`
              }
            }
            yield* sql`INSERT INTO effect_local_server_attachment_upload_grants
          (space_id, digest, client_id, membership_incarnation, expires_at)
          VALUES (${input.spaceId}, ${input.reference.digest}, ${input.clientId},
            ${input.membershipIncarnation}, ${now + uploadGrantLifetime})
          ON CONFLICT (space_id, digest, client_id, membership_incarnation) DO UPDATE SET
            expires_at = excluded.expires_at`
            return {
              objectKey: found.value.object_key,
              offset,
              complete: found.value.state === "Complete" || (
                offset === input.reference.bytes &&
                (found.value.lease_token === null || found.value.lease_expires_at === null ||
                  found.value.lease_expires_at <= now)
              )
            }
          }).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        )
      })

      const appendUpload: Service["appendUpload"] = Effect.fnUntraced(function*<
        E extends { readonly _tag: string },
        R2,
      >(
        input: AuthorizationInput & {
          readonly expectedOffset: number
          readonly bytes: Stream.Stream<Uint8Array, E, R2>
        }
      ): Effect.fn.Return<PreparedUpload, E | ReplicaError.ReplicaError, R2> {
        yield* validateReference(input.reference)
        yield* authorize(input, true)
        return yield* withLock(
          input.spaceId,
          input.reference.digest,
          Effect.gen(function*() {
            const found = yield* find(input.spaceId, input.reference.digest)
            if (Option.isNone(found)) return yield* notFound(input.reference)
            if (found.value.bytes !== input.reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: found.value.bytes,
                actual: input.reference.bytes
              })
            }
            if (found.value.state === "Complete") {
              if (input.expectedOffset !== input.reference.bytes) {
                return yield* new Attachment.AttachmentOffsetConflict({
                  expected: input.expectedOffset,
                  actual: input.reference.bytes
                })
              }
              return { objectKey: found.value.object_key, offset: input.reference.bytes, complete: true }
            }
            const now = yield* Clock.currentTimeMillis
            const leaseToken = yield* crypto.randomUUIDv4.pipe(
              Effect.mapError((cause) =>
                new Attachment.AttachmentStorageError({
                  operation: "upload.leaseToken",
                  cause
                })
              )
            )
            const lease = yield* acquireUploadLease({
              spaceId: input.spaceId,
              digest: input.reference.digest,
              token: leaseToken,
              now,
              expiresAt: now + uploadLeaseLifetime
            }).pipe(Effect.mapError(readError("Attachment upload lease state is corrupt")))
            if (Option.isNone(lease)) {
              return yield* new Attachment.AttachmentUploadBusy({ digest: input.reference.digest })
            }
            const releaseLease = sql`UPDATE effect_local_server_attachment_objects
          SET lease_token = NULL, lease_expires_at = NULL
          WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}
            AND lease_token = ${leaseToken}`.pipe(
              Effect.tapError((error) =>
                Effect.logWarning("Attachment upload lease will expire").pipe(
                  Effect.annotateLogs("error", error._tag)
                )
              ),
              Effect.ignore
            )
            return yield* Effect.gen(function*() {
              const appended = yield* storage.append(
                found.value.object_key,
                input.reference,
                input.expectedOffset,
                input.bytes
              ).pipe(
                Effect.tapError(() =>
                  storage.offset(found.value.object_key).pipe(
                    Effect.flatMap((offset) =>
                      sql`UPDATE effect_local_server_attachment_objects
                SET storage_offset = ${offset} WHERE space_id = ${input.spaceId}
                  AND digest = ${input.reference.digest}`
                    ),
                    Effect.catch(() => Effect.void)
                  )
                )
              )
              const completedAt = yield* Clock.currentTimeMillis
              if (appended === input.reference.bytes) {
                yield* storage.verify(found.value.object_key, input.reference)
                yield* sql`UPDATE effect_local_server_attachment_objects SET state = 'Complete',
              storage_offset = ${appended}, garbage_collect_after = ${completedAt + garbageCollectionGracePeriod},
              last_accessed_at = ${completedAt}
              WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}
                AND lease_token = ${leaseToken}`
                return { objectKey: found.value.object_key, offset: appended, complete: true }
              }
              yield* sql`UPDATE effect_local_server_attachment_objects SET storage_offset = ${appended},
            last_accessed_at = ${completedAt} WHERE space_id = ${input.spaceId}
              AND digest = ${input.reference.digest} AND lease_token = ${leaseToken}`
              return { objectKey: found.value.object_key, offset: appended, complete: false }
            }).pipe(Effect.ensuring(releaseLease))
          }).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        )
      })

      const replaceEntityReferences: Service["replaceEntityReferences"] = Effect.fnUntraced(function*(input) {
        let references: ReadonlyArray<Attachment.Reference> = []
        if (input.value !== undefined) references = yield* Attachment.collect(input.value)
        const now = yield* Clock.currentTimeMillis
        yield* sql.withTransaction(Effect.gen(function*() {
          for (const reference of references) {
            const found = yield* find(input.spaceId, reference.digest)
            if (Option.isNone(found) || found.value.state !== "Complete") return yield* notFound(reference)
            if (found.value.bytes !== reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: found.value.bytes,
                actual: reference.bytes
              })
            }
          }
          yield* sql`DELETE FROM effect_local_server_attachment_references
          WHERE space_id = ${input.spaceId} AND schema_generation = ${input.schemaGeneration}
            AND model = ${input.model} AND entity_key = ${input.entityKey}`
          for (const reference of references) {
            yield* sql`INSERT INTO effect_local_server_attachment_references
            (space_id, schema_generation, digest, model, model_version, entity_key)
            VALUES (${input.spaceId}, ${input.schemaGeneration}, ${reference.digest}, ${input.model},
              ${input.modelVersion}, ${input.entityKey})`
            yield* sql`UPDATE effect_local_server_attachment_objects SET garbage_collect_after = NULL
            WHERE space_id = ${input.spaceId} AND digest = ${reference.digest}`
          }
          yield* sql`UPDATE effect_local_server_attachment_objects SET
          garbage_collect_after = ${now + garbageCollectionGracePeriod}
          WHERE space_id = ${input.spaceId} AND state = 'Complete' AND garbage_collect_after IS NULL
            AND NOT EXISTS (SELECT 1 FROM effect_local_server_attachment_references AS r
              JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
                AND s.active_schema_generation = r.schema_generation
              WHERE r.space_id = effect_local_server_attachment_objects.space_id
                AND r.digest = effect_local_server_attachment_objects.digest)`
          return yield* Effect.void
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        return yield* Effect.void
      })

      const prepareRead: Service["prepareRead"] = Effect.fnUntraced(function*(input) {
        yield* validateReference(input.reference)
        yield* authorize(input, false)
        const entities = yield* readableEntities({ spaceId: input.spaceId, digest: input.reference.digest }).pipe(
          Effect.mapError(readError("Attachment reference authorization state is corrupt"))
        )
        let authorized = false
        for (const row of entities) {
          const key = yield* Codec.parse(row.entity_key).pipe(
            Effect.flatMap((value) => Codec.decode(Schema.Json, value))
          )
          const value = yield* Codec.parse(row.value_json).pipe(
            Effect.flatMap((parsed) => Codec.decode(Schema.Json, parsed))
          )
          const result = yield* options.authorizeRead({
            ...input,
            entity: { model: row.model, modelVersion: row.model_version, key },
            value
          }).pipe(Effect.provide(context), Effect.result)
          if (Result.isSuccess(result)) {
            authorized = true
            break
          }
        }
        if (!authorized) {
          return yield* new ReplicaError.AuthorizationDenied({
            reason: { _tag: "AttachmentReadDenied" }
          })
        }
        const found = yield* find(input.spaceId, input.reference.digest)
        if (Option.isNone(found) || found.value.state !== "Complete") return yield* notFound(input.reference)
        if (found.value.bytes !== input.reference.bytes) {
          return yield* new Attachment.AttachmentLengthMismatch({
            expected: found.value.bytes,
            actual: input.reference.bytes
          })
        }
        return storage.read(found.value.object_key, input.reference, input.range)
      })

      const read: Service["read"] = (input) => Stream.unwrap(prepareRead(input))

      const maintain: Service["maintain"] = Effect.fnUntraced(function*(spaceId) {
        const now = yield* Clock.currentTimeMillis
        yield* sql`DELETE FROM effect_local_server_attachment_upload_grants WHERE expires_at <= ${now}`.pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        const due = yield* SqlSchema.findAll({
          Request: Schema.Struct({ spaceId: Identity.SpaceId, now: Schema.Number, maximum: Schema.Number }),
          Result: Rows.ServerAttachmentObjectRow,
          execute: ({ maximum, now: requestedNow, spaceId: requestedSpaceId }) =>
            sql`SELECT space_id, digest, bytes, object_key, state,
          storage_offset, lease_token, lease_expires_at, garbage_collect_after, created_at, last_accessed_at
          FROM effect_local_server_attachment_objects AS o
          WHERE o.space_id = ${requestedSpaceId} AND (
            (o.state = 'Complete' AND o.garbage_collect_after IS NOT NULL
              AND o.garbage_collect_after <= ${requestedNow}
              AND NOT EXISTS (SELECT 1 FROM effect_local_server_attachment_references AS r
                JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
                  AND s.active_schema_generation = r.schema_generation
                WHERE r.space_id = o.space_id AND r.digest = o.digest)
              AND NOT EXISTS (SELECT 1 FROM effect_local_server_attachment_upload_grants AS g
                WHERE g.space_id = o.space_id AND g.digest = o.digest AND g.expires_at > ${requestedNow}))
            OR (o.state = 'Staging' AND o.last_accessed_at + ${stagingLifetime} <= ${requestedNow}
              AND (o.lease_expires_at IS NULL OR o.lease_expires_at <= ${requestedNow})))
          ORDER BY COALESCE(o.garbage_collect_after, o.last_accessed_at), o.digest LIMIT ${maximum}`
        })({ spaceId, now, maximum: deletionBatchSize }).pipe(
          Effect.mapError(readError("Attachment garbage collection state is corrupt"))
        )
        for (const object of due) {
          yield* sql.withTransaction(Effect.gen(function*() {
            yield* sql`INSERT OR IGNORE INTO effect_local_server_attachment_deletions
            (object_key, space_id, digest, bytes, next_attempt_at, created_at)
            VALUES (${object.object_key}, ${object.space_id}, ${object.digest}, ${object.bytes}, ${now}, ${now})`
            yield* sql`DELETE FROM effect_local_server_attachment_objects
            WHERE space_id = ${object.space_id} AND digest = ${object.digest}
              AND object_key = ${object.object_key}`
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        }
        const deletions = yield* SqlSchema.findAll({
          Request: Schema.Struct({ now: Schema.Number, maximum: Schema.Number }),
          Result: Rows.ServerAttachmentDeletionRow,
          execute: ({ maximum, now: requestedNow }) =>
            sql`SELECT object_key, space_id, digest, bytes, attempt_count,
          next_attempt_at, claim_token, claimed_until, created_at
          FROM effect_local_server_attachment_deletions WHERE next_attempt_at <= ${requestedNow}
          ORDER BY next_attempt_at, object_key LIMIT ${maximum}`
        })({ now, maximum: deletionBatchSize }).pipe(
          Effect.mapError(readError("Attachment deletion state is corrupt"))
        )
        for (const deletion of deletions) {
          yield* storage.remove(deletion.object_key)
          yield* sql`DELETE FROM effect_local_server_attachment_deletions
          WHERE object_key = ${deletion.object_key}`.pipe(Effect.mapError(StorageUnavailable.make))
        }
        return deletions.length
      })

      return AttachmentServer.of({ prepareUpload, appendUpload, replaceEntityReferences, prepareRead, read, maintain })
    })
  )
