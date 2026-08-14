import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
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
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AttachmentObjectStore from "./AttachmentObjectStore.js"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

type AuthorizationRejection = Schema.JsonObject & { readonly _tag: string }

export interface AuthorizationInput {
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
  readonly maximumReferencesPerObject: number
  readonly uploadGrantLifetime: Duration.Input
  readonly uploadLeaseLifetime: Duration.Input
  readonly readLeaseLifetime: Duration.Input
  readonly stagingLifetime: Duration.Input
  readonly garbageCollectionGracePeriod: Duration.Input
  readonly deletionBatchSize: number
  readonly verificationChunkBytes?: number
  readonly maximumVerificationChunks: number
  readonly deletionRetryDelay?: Duration.Input
  readonly grantSafetyInterval?: Duration.Input
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

export interface Service {
  readonly prepareUpload: (
    input: AuthorizationInput
  ) => Effect.Effect<AttachmentTransfer.PrepareUploadResult, ReplicaError.ReplicaError>
  readonly finalizeUpload: (
    input: AuthorizationInput & { readonly attemptId: AttachmentTransfer.AttemptId }
  ) => Effect.Effect<AttachmentTransfer.UploadComplete, ReplicaError.ReplicaError>
  readonly prepareDownload: (
    input: AuthorizationInput & { readonly range?: Attachment.Range }
  ) => Effect.Effect<AttachmentTransfer.DownloadGrant, ReplicaError.ReplicaError>
  readonly replaceEntityReferences: (input: {
    readonly spaceId: Identity.SpaceId
    readonly schemaGeneration: number
    readonly model: string
    readonly modelVersion: Identity.SchemaVersion
    readonly entityKey: string
    readonly value?: Schema.Json
    readonly authority:
      | {
        readonly _tag: "Mutation"
        readonly clientId: Identity.ClientId
        readonly membershipIncarnation: Identity.MembershipIncarnation
      }
      | { readonly _tag: "SchemaEvolution" }
  }) => Effect.Effect<void, ReplicaError.StorageError>
  readonly activateGeneration: (
    spaceId: Identity.SpaceId,
    schemaGeneration: number
  ) => Effect.Effect<void, ReplicaError.StorageError>
  readonly expireGrants: Effect.Effect<void, ReplicaError.StorageError>
  readonly sweepSpace: (spaceId: Identity.SpaceId) => Effect.Effect<number, ReplicaError.ReplicaError>
  readonly drainOutbox: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly maintain: (spaceId: Identity.SpaceId) => Effect.Effect<number, ReplicaError.ReplicaError>
}

export class AttachmentServer extends Context.Service<AttachmentServer, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentServer"
) {}

const Lookup = Schema.Struct({ spaceId: Identity.SpaceId, digest: Attachment.Digest })
const AttemptLookup = Schema.Struct({
  spaceId: Identity.SpaceId,
  digest: Attachment.Digest,
  clientId: Identity.ClientId,
  membershipIncarnation: Identity.MembershipIncarnation
})
const ReadableEntity = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String
})
const DigestRow = Schema.Struct({ digest: Attachment.Digest })
const AttemptIdRow = Schema.Struct({ attempt_id: Schema.String })

export const layer = <R = never,>(options: Options<R>): Layer.Layer<
  AttachmentServer,
  ReplicaError.InvalidConfiguration,
  SqlClient.SqlClient | AttachmentObjectStore.AttachmentObjectStore | Crypto.Crypto | R
> =>
  Layer.effect(
    AttachmentServer,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const objectStores = yield* AttachmentObjectStore.AttachmentObjectStore
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
      const maximumReferencesPerObject = yield* Configuration.positiveSafeInteger(
        "attachments.maximumReferencesPerObject",
        options.maximumReferencesPerObject
      )
      const uploadGrantLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.uploadGrantLifetime",
        options.uploadGrantLifetime
      )
      const finalizationLeaseLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.uploadLeaseLifetime",
        options.uploadLeaseLifetime
      )
      const downloadGrantLifetime = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.readLeaseLifetime",
        options.readLeaseLifetime
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
      const verificationChunkBytes = yield* Configuration.positiveSafeInteger(
        "attachments.verificationChunkBytes",
        options.verificationChunkBytes ?? 5 * 1024 * 1024
      )
      const maximumVerificationChunks = yield* Configuration.positiveSafeInteger(
        "attachments.maximumVerificationChunks",
        options.maximumVerificationChunks
      )
      if (Math.ceil(maximumObjectBytes / verificationChunkBytes) > maximumVerificationChunks) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "attachments.maximumVerificationChunks",
          message: "attachments.maximumVerificationChunks cannot cover attachments.maximumObjectBytes"
        })
      }
      const deletionRetryDelay = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.deletionRetryDelay",
        options.deletionRetryDelay ?? "1 second"
      )
      const grantSafetyInterval = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.grantSafetyInterval",
        options.grantSafetyInterval ?? "30 seconds"
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

      const newId = (operation: string) =>
        crypto.randomUUIDv4.pipe(
          Effect.catchTag(
            "PlatformError",
            (cause) => Effect.fail(new Attachment.AttachmentStorageError({ operation, cause }))
          )
        )
      const newPhysicalKey = (operation: string) =>
        newId(operation).pipe(
          Effect.map((id) => AttachmentObjectStore.PhysicalKey.make(id.replaceAll("-", "")))
        )
      const authorize = (input: AuthorizationInput, upload: boolean) => {
        let uploadAuthorization: Effect.Effect<void, AuthorizationRejection> = Effect.void
        if (upload) uploadAuthorization = options.authorizeUpload(input).pipe(Effect.provide(context))
        return options.authorizeAccess(input).pipe(
          Effect.provide(context),
          Effect.andThen(uploadAuthorization),
          Effect.catch((reason: AuthorizationRejection) =>
            Effect.fail(new ReplicaError.AuthorizationDenied({ reason: { ...reason } }))
          )
        )
      }
      const validateReference = (reference: Attachment.Reference) => {
        if (reference.bytes <= maximumObjectBytes) return Effect.void
        return Effect.fail(new Attachment.AttachmentTooLarge({ limit: maximumObjectBytes }))
      }

      const findObjectQuery = SqlSchema.findOneOption({
        Request: Lookup,
        Result: Rows.ServerAttachmentObjectRow,
        execute: ({ digest, spaceId }) =>
          sql`SELECT space_id, digest, bytes, object_version, state, provider_namespace, provider_object_id,
            provider_object_version, chunk_bytes, chunk_count, garbage_collect_after,
            created_at, last_accessed_at
          FROM effect_local_server_attachment_objects
          WHERE space_id = ${spaceId} AND digest = ${digest}`
      })
      const findObject = (spaceId: Identity.SpaceId, digest: Attachment.Digest) =>
        findObjectQuery({ spaceId, digest }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Server attachment object metadata is corrupt",
                  cause
                })
              )
          })
        )
      const findAttemptQuery = SqlSchema.findOneOption({
        Request: AttemptLookup,
        Result: Rows.ServerAttachmentAttemptRow,
        execute: ({ clientId, digest, membershipIncarnation, spaceId }) =>
          sql`SELECT attempt_id, space_id, digest, bytes, client_id, membership_incarnation,
            provider_namespace, physical_key, provider_upload_id, part_size, state,
            finalization_token, finalization_expires_at, created_at, last_accessed_at
          FROM effect_local_server_attachment_attempts
          WHERE space_id = ${spaceId} AND digest = ${digest} AND client_id = ${clientId}
            AND membership_incarnation = ${membershipIncarnation}`
      })
      const findAttempt = (input: typeof AttemptLookup.Type) =>
        findAttemptQuery(input).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Server attachment upload attempt is corrupt",
                  cause
                })
              )
          })
        )
      const retireMissingAttempt = Effect.fnUntraced(function*(
        attempt: typeof Rows.ServerAttachmentAttemptRow.Type
      ) {
        const finalizationRecovery = yield* sql.withTransaction(Effect.gen(function*() {
          const retired = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: AttemptIdRow,
            execute: () =>
              sql`DELETE FROM effect_local_server_attachment_attempts
                WHERE attempt_id = ${attempt.attempt_id}
                  AND provider_namespace = ${attempt.provider_namespace}
                  AND (
                    (${attempt.provider_upload_id} IS NULL AND provider_upload_id IS NULL) OR
                    provider_upload_id = ${attempt.provider_upload_id}
                  )
                  AND state <> 'Finalizing'
                RETURNING attempt_id`
          })(undefined)
          if (Option.isSome(retired)) return false
          const active = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: AttemptIdRow,
            execute: () =>
              sql`SELECT attempt_id FROM effect_local_server_attachment_attempts
                WHERE attempt_id = ${attempt.attempt_id}
                  AND provider_namespace = ${attempt.provider_namespace}
                  AND state = 'Finalizing'`
          })(undefined)
          return Option.isSome(active)
        })).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment missing upload retirement state is corrupt",
                  cause
                })
              )
          })
        )
        if (finalizationRecovery) {
          yield* new Attachment.AttachmentUploadBusy({ digest: attempt.digest })
        }
      })
      const retireOwnedFinalization = (
        attempt: typeof Rows.ServerAttachmentAttemptRow.Type,
        token: string
      ) =>
        sql`DELETE FROM effect_local_server_attachment_attempts
          WHERE attempt_id = ${attempt.attempt_id}
            AND provider_namespace = ${attempt.provider_namespace}
            AND provider_upload_id = ${attempt.provider_upload_id}
            AND finalization_token = ${token}`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )

      const finalizationHeartbeat = (
        attemptId: string,
        digest: Attachment.Digest,
        token: string
      ) => {
        const heartbeatInterval = finalizationLeaseLifetime / 2
        return Effect.sleep(Math.max(1, heartbeatInterval)).pipe(
          Effect.andThen(Effect.gen(function*() {
            const now = yield* Clock.currentTimeMillis
            const renewed = yield* SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: AttemptIdRow,
              execute: () =>
                sql`UPDATE effect_local_server_attachment_attempts
                  SET finalization_expires_at = ${now + finalizationLeaseLifetime},
                    last_accessed_at = ${now}
                  WHERE attempt_id = ${attemptId} AND finalization_token = ${token}
                  RETURNING attempt_id`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment finalization renewal is corrupt",
                      cause
                    })
                  )
              })
            )
            if (Option.isNone(renewed)) {
              yield* new Attachment.AttachmentUploadBusy({ digest })
            }
          })),
          Effect.forever
        )
      }

      const retireRejectedBegin = Effect.fnUntraced(function*(
        attempt: typeof Rows.ServerAttachmentAttemptRow.Type,
        providerId: AttachmentObjectStore.ProviderId
      ) {
        yield* sql.withTransaction(Effect.gen(function*() {
          const retired = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: AttemptIdRow,
            execute: () =>
              sql`DELETE FROM effect_local_server_attachment_attempts
                WHERE attempt_id = ${attempt.attempt_id} AND state = 'Reserved'
                  AND provider_upload_id IS NULL
                RETURNING attempt_id`
          })(undefined)
          const current = yield* findAttempt({
            spaceId: attempt.space_id,
            digest: attempt.digest,
            clientId: attempt.client_id,
            membershipIncarnation: attempt.membership_incarnation
          })
          if (
            Option.isNone(retired) && Option.isSome(current) &&
            current.value.provider_upload_id === providerId
          ) {
            return
          }
          const outboxId = yield* newId("upload.invalidBeginAbort")
          const queuedAt = yield* Clock.currentTimeMillis
          yield* sql`INSERT INTO effect_local_server_attachment_deletions
              (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                provider_id, next_attempt_at, created_at)
              SELECT ${outboxId}, ${attempt.space_id}, ${attempt.digest}, ${attempt.bytes},
                'AbortUpload', ${attempt.provider_namespace}, ${providerId}, ${queuedAt}, ${queuedAt}
              WHERE NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_deletions
                WHERE operation = 'AbortUpload' AND provider_namespace = ${attempt.provider_namespace}
                  AND provider_id = ${providerId}
              )`
        })).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment rejected begin retirement state is corrupt",
                  cause
                })
              )
          })
        )
      })

      const hasPossession = SqlSchema.findOneOption({
        Request: AttemptLookup,
        Result: DigestRow,
        execute: ({ clientId, digest, membershipIncarnation, spaceId }) =>
          sql`SELECT digest FROM effect_local_server_attachment_possessions
          WHERE space_id = ${spaceId} AND digest = ${digest} AND client_id = ${clientId}
            AND membership_incarnation = ${membershipIncarnation}`
      })
      const spaceUsage = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.ServerAttachmentUsageRow,
        execute: (spaceId) =>
          sql`SELECT object_count, byte_count FROM effect_local_server_attachment_usage
            WHERE space_id = ${spaceId}`
      })

      const reserveAttempt = Effect.fnUntraced(function*(input: AuthorizationInput) {
        const attemptId = AttachmentTransfer.AttemptId.make(yield* newId("upload.attemptId"))
        const physicalKey = yield* newPhysicalKey("upload.physicalKey")
        const now = yield* Clock.currentTimeMillis
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_server_spaces SET space_id = space_id
            WHERE space_id = ${input.spaceId}`
          const concurrent = yield* findAttempt({
            spaceId: input.spaceId,
            digest: input.reference.digest,
            clientId: input.clientId,
            membershipIncarnation: input.membershipIncarnation
          })
          if (Option.isSome(concurrent)) return
          yield* sql`INSERT OR IGNORE INTO effect_local_server_attachment_usage
            (space_id, object_count, byte_count) VALUES (${input.spaceId}, 0, 0)`
          const usage = yield* spaceUsage(input.spaceId).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              NoSuchElementError: () =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Server attachment quota row is missing"
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Server attachment quota state is corrupt",
                    cause
                  })
                )
            })
          )
          if (usage.object_count >= maximumObjectsPerSpace) {
            yield* new ReplicaError.CapacityExceeded({
              resource: "attachment physical objects per space",
              limit: maximumObjectsPerSpace
            })
          }
          if (usage.byte_count + input.reference.bytes > maximumBytesPerSpace) {
            yield* new ReplicaError.CapacityExceeded({
              resource: "attachment physical bytes per space",
              limit: maximumBytesPerSpace
            })
          }
          yield* sql`INSERT INTO effect_local_server_attachment_attempts
            (attempt_id, space_id, digest, bytes, client_id, membership_incarnation,
              provider_namespace, physical_key, state, created_at, last_accessed_at)
            VALUES (${attemptId}, ${input.spaceId}, ${input.reference.digest}, ${input.reference.bytes},
              ${input.clientId}, ${input.membershipIncarnation},
              ${objectStores.namespaceForNewObjects}, ${physicalKey}, 'Reserved', ${now}, ${now})`
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        const found = yield* findAttempt({
          spaceId: input.spaceId,
          digest: input.reference.digest,
          clientId: input.clientId,
          membershipIncarnation: input.membershipIncarnation
        })
        if (Option.isNone(found)) {
          return yield* new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
        }
        return found.value
      })

      const beginAttempt = Effect.fnUntraced(function*(attempt: typeof Rows.ServerAttachmentAttemptRow.Type) {
        if (attempt.provider_upload_id !== null && attempt.part_size !== null) return attempt
        const adapter = yield* objectStores.resolve(attempt.provider_namespace).pipe(
          Effect.catchTag("AttachmentObjectStoreUnavailable", (cause) =>
            Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.resolve", cause })))
        )
        const reference = Attachment.Reference.make({
          digest: attempt.digest,
          bytes: attempt.bytes
        })
        const begun = yield* adapter.beginUpload({
          spaceId: attempt.space_id,
          attemptId: AttachmentTransfer.AttemptId.make(attempt.attempt_id),
          physicalKey: attempt.physical_key,
          reference,
          verificationChunkBytes
        }).pipe(
          Effect.catchTags({
            AttachmentObjectStoreUnavailable: (cause) =>
              Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.begin", cause })),
            AttachmentProviderUploadNotFound: () =>
              retireMissingAttempt(attempt).pipe(
                Effect.andThen(Effect.fail(new Attachment.AttachmentUnavailable({ digest: attempt.digest })))
              ),
            AttachmentProviderObjectNotFound: () =>
              retireMissingAttempt(attempt).pipe(
                Effect.andThen(Effect.fail(new Attachment.AttachmentUnavailable({ digest: attempt.digest })))
              )
          }),
          Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.BegunUpload)),
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new Attachment.AttachmentStorageError({
                operation: "upload.begin.decode",
                cause
              })
            ))
        )
        if (begun.upload.namespace !== attempt.provider_namespace) {
          yield* retireRejectedBegin(attempt, begun.upload.id)
          return yield* new Attachment.AttachmentStorageError({
            operation: "upload.begin.namespace",
            cause: begun
          })
        }
        if (Math.ceil(attempt.bytes / begun.partSize) > 10_000) {
          yield* retireRejectedBegin(attempt, begun.upload.id)
          return yield* new Attachment.AttachmentStorageError({
            operation: "upload.begin.partCount",
            cause: { bytes: attempt.bytes, partSize: begun.partSize }
          })
        }
        const current = yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_server_attachment_attempts
            SET provider_upload_id = ${begun.upload.id}, part_size = ${begun.partSize},
              state = 'Uploading'
            WHERE attempt_id = ${attempt.attempt_id} AND state = 'Reserved'
              AND provider_upload_id IS NULL`
          const persisted = yield* findAttempt({
            spaceId: attempt.space_id,
            digest: attempt.digest,
            clientId: attempt.client_id,
            membershipIncarnation: attempt.membership_incarnation
          })
          if (Option.isNone(persisted) || persisted.value.provider_upload_id !== begun.upload.id) {
            const outboxId = yield* newId("upload.beginRaceAbort")
            const queuedAt = yield* Clock.currentTimeMillis
            yield* sql`INSERT INTO effect_local_server_attachment_deletions
              (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                provider_id, next_attempt_at, created_at)
              SELECT ${outboxId}, ${attempt.space_id}, ${attempt.digest}, ${attempt.bytes},
                'AbortUpload', ${attempt.provider_namespace}, ${begun.upload.id}, ${queuedAt}, ${queuedAt}
              WHERE NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_deletions
                WHERE operation = 'AbortUpload' AND provider_namespace = ${attempt.provider_namespace}
                  AND provider_id = ${begun.upload.id}
              )`
          }
          return persisted
        })).pipe(Effect.catchTag("SqlError", (cause) =>
          Effect.fail(StorageUnavailable.make(cause))))
        if (
          Option.isNone(current) || current.value.provider_upload_id !== begun.upload.id ||
          current.value.part_size !== begun.partSize
        ) {
          return yield* new Attachment.AttachmentUploadBusy({ digest: attempt.digest })
        }
        return current.value
      })

      const inspectParts = Effect.fnUntraced(function*(
        adapter: AttachmentObjectStore.Adapter,
        attempt: typeof Rows.ServerAttachmentAttemptRow.Type
      ) {
        const upload = AttachmentObjectStore.UploadIdentity.make({
          namespace: attempt.provider_namespace,
          id: AttachmentObjectStore.ProviderId.make(attempt.provider_upload_id!)
        })
        let afterPartNumber = 0
        let expectedPartNumber = 1
        let offset = 0
        while (true) {
          const page = yield* adapter.listUploadedParts({
            spaceId: attempt.space_id,
            upload,
            afterPartNumber,
            limit: AttachmentObjectStore.maximumUploadedPartPageSize
          }).pipe(
            Effect.catchTags({
              AttachmentObjectStoreUnavailable: (cause) =>
                Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.parts", cause })),
              AttachmentProviderUploadNotFound: () =>
                retireMissingAttempt(attempt).pipe(
                  Effect.andThen(Effect.fail(new Attachment.AttachmentUnavailable({ digest: attempt.digest })))
                ),
              AttachmentProviderObjectNotFound: () =>
                retireMissingAttempt(attempt).pipe(
                  Effect.andThen(Effect.fail(new Attachment.AttachmentUnavailable({ digest: attempt.digest })))
                )
            }),
            Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.UploadedPartPage)),
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new Attachment.AttachmentStorageError({
                  operation: "upload.parts.decode",
                  cause
                })
              ))
          )
          for (const part of page.parts) {
            if (
              part.partNumber !== expectedPartNumber ||
              attempt.part_size === null ||
              (offset + part.bytes < attempt.bytes && part.bytes !== attempt.part_size) ||
              offset + part.bytes > attempt.bytes
            ) {
              return yield* new Attachment.AttachmentStorageError({
                operation: "upload.parts.invalid",
                cause: { expectedPartNumber, offset, part }
              })
            }
            expectedPartNumber++
            offset += part.bytes
          }
          if (page.nextPartNumber === null) break
          if (page.nextPartNumber <= afterPartNumber) {
            return yield* new Attachment.AttachmentStorageError({
              operation: "upload.parts.pagination",
              cause: page
            })
          }
          afterPartNumber = page.nextPartNumber - 1
        }
        return { upload, offset, partNumber: expectedPartNumber }
      })

      const prepareUpload: Service["prepareUpload"] = Effect.fnUntraced(function*(input) {
        yield* validateReference(input.reference)
        yield* authorize(input, true)
        return yield* withLock(
          input.spaceId,
          input.reference.digest,
          Effect.gen(function*() {
            const lookup = {
              spaceId: input.spaceId,
              digest: input.reference.digest,
              clientId: input.clientId,
              membershipIncarnation: input.membershipIncarnation
            }
            const possession = yield* hasPossession(lookup).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Server attachment possession is corrupt",
                      cause
                    })
                  )
              })
            )
            if (Option.isSome(possession)) {
              const object = yield* findObject(input.spaceId, input.reference.digest)
              if (Option.isSome(object) && object.value.state === "Available") {
                return AttachmentTransfer.UploadComplete.make({})
              }
            }
            const attemptOption = yield* findAttempt(lookup)
            let attempt: typeof Rows.ServerAttachmentAttemptRow.Type
            if (Option.isSome(attemptOption)) {
              attempt = attemptOption.value
            } else {
              attempt = yield* reserveAttempt(input)
            }
            if (attempt.bytes !== input.reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: attempt.bytes,
                actual: input.reference.bytes
              })
            }
            attempt = yield* beginAttempt(attempt)
            const adapter = yield* objectStores.resolve(attempt.provider_namespace).pipe(
              Effect.catchTag(
                "AttachmentObjectStoreUnavailable",
                (cause) => Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.resolve", cause }))
              )
            )
            const progress = yield* inspectParts(adapter, attempt)
            if (progress.offset === input.reference.bytes) {
              const readyAt = yield* Clock.currentTimeMillis
              yield* sql`UPDATE effect_local_server_attachment_attempts
                SET last_accessed_at = ${readyAt}
                WHERE attempt_id = ${attempt.attempt_id}
                  AND provider_upload_id = ${attempt.provider_upload_id}`.pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
              )
              return AttachmentTransfer.UploadReady.make({
                attemptId: AttachmentTransfer.AttemptId.make(attempt.attempt_id)
              })
            }
            const bytes = Math.min(attempt.part_size!, input.reference.bytes - progress.offset)
            const expiresAt = (yield* Clock.currentTimeMillis) + uploadGrantLifetime
            const grant = yield* adapter.grantUploadPart({
              spaceId: attempt.space_id,
              upload: progress.upload,
              partNumber: progress.partNumber,
              offset: progress.offset,
              bytes,
              expiresAt
            }).pipe(
              Effect.catchTags({
                AttachmentObjectStoreUnavailable: (cause) =>
                  Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.grant", cause })),
                AttachmentProviderUploadNotFound: () =>
                  retireMissingAttempt(attempt).pipe(
                    Effect.andThen(
                      Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                    )
                  ),
                AttachmentProviderObjectNotFound: () =>
                  retireMissingAttempt(attempt).pipe(
                    Effect.andThen(
                      Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                    )
                  )
              }),
              Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.DirectUploadGrant)),
              Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(
                  new Attachment.AttachmentStorageError({
                    operation: "upload.grant.decode",
                    cause
                  })
                ))
            )
            if (grant.expiresAt > expiresAt || grant.expiresAt <= (yield* Clock.currentTimeMillis)) {
              return yield* new Attachment.AttachmentStorageError({
                operation: "upload.grant.expiry",
                cause: grant
              })
            }
            const grantId = AttachmentTransfer.GrantId.make(yield* newId("upload.grantId"))
            const now = yield* Clock.currentTimeMillis
            yield* sql`INSERT INTO effect_local_server_attachment_upload_grants
            (grant_id, attempt_id, client_id, membership_incarnation, part_number,
              byte_offset, bytes, expires_at, created_at)
            VALUES (${grantId}, ${attempt.attempt_id}, ${input.clientId},
              ${input.membershipIncarnation}, ${progress.partNumber}, ${progress.offset},
              ${bytes}, ${grant.expiresAt}, ${now})`.pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
            )
            yield* sql`UPDATE effect_local_server_attachment_attempts
              SET last_accessed_at = ${now}
              WHERE attempt_id = ${attempt.attempt_id}
                AND provider_upload_id = ${attempt.provider_upload_id}`.pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
            )
            return AttachmentTransfer.UploadPart.make({
              attemptId: AttachmentTransfer.AttemptId.make(attempt.attempt_id),
              partNumber: progress.partNumber,
              offset: progress.offset,
              bytes,
              expiresAt: grant.expiresAt,
              request: grant.request
            })
          })
        )
      }, Effect.withSpan("AttachmentServer.prepareUpload"))

      const finalizeUpload: Service["finalizeUpload"] = Effect.fnUntraced(function*(input) {
        yield* validateReference(input.reference)
        yield* authorize(input, true)
        return yield* withLock(
          input.spaceId,
          input.reference.digest,
          Effect.gen(function*() {
            const possession = yield* hasPossession({
              spaceId: input.spaceId,
              digest: input.reference.digest,
              clientId: input.clientId,
              membershipIncarnation: input.membershipIncarnation
            }).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Server attachment possession is corrupt",
                      cause
                    })
                  )
              })
            )
            if (Option.isSome(possession)) {
              const object = yield* findObject(input.spaceId, input.reference.digest)
              if (Option.isSome(object) && object.value.state === "Available") {
                return AttachmentTransfer.UploadComplete.make({})
              }
            }
            const attemptOption = yield* findAttempt({
              spaceId: input.spaceId,
              digest: input.reference.digest,
              clientId: input.clientId,
              membershipIncarnation: input.membershipIncarnation
            })
            if (Option.isNone(attemptOption) || attemptOption.value.attempt_id !== input.attemptId) {
              return yield* new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
            }
            const attempt = attemptOption.value
            if (attempt.bytes !== input.reference.bytes || attempt.provider_upload_id === null) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: attempt.bytes,
                actual: input.reference.bytes
              })
            }
            const adapter = yield* objectStores.resolve(attempt.provider_namespace).pipe(
              Effect.catchTag("AttachmentObjectStoreUnavailable", (cause) =>
                Effect.fail(
                  new Attachment.AttachmentStorageError({
                    operation: "upload.finalize.resolve",
                    cause
                  })
                ))
            )
            const upload = AttachmentObjectStore.UploadIdentity.make({
              namespace: attempt.provider_namespace,
              id: AttachmentObjectStore.ProviderId.make(attempt.provider_upload_id)
            })
            const now = yield* Clock.currentTimeMillis
            let ownsFinalization = false
            let finalizationToken: string | null = null
            if (
              attempt.finalization_token === null || attempt.finalization_expires_at === null ||
              attempt.finalization_expires_at <= now
            ) {
              const token = yield* newId("upload.finalizationToken")
              finalizationToken = token
              const claimed = yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: AttemptIdRow,
                execute: () =>
                  sql`UPDATE effect_local_server_attachment_attempts
                SET state = 'Finalizing', finalization_token = ${token},
                  finalization_expires_at = ${now + finalizationLeaseLifetime}, last_accessed_at = ${now}
                WHERE attempt_id = ${attempt.attempt_id}
                  AND (finalization_token IS NULL OR finalization_expires_at <= ${now})
                RETURNING attempt_id`
              })(undefined).pipe(
                Effect.catchTags({
                  SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                  SchemaError: (cause) =>
                    Effect.fail(
                      new ReplicaError.StorageCorrupt({
                        message: "Attachment finalization claim is corrupt",
                        cause
                      })
                    )
                })
              )
              ownsFinalization = Option.isSome(claimed)
            }
            if (!ownsFinalization || finalizationToken === null) {
              return yield* new Attachment.AttachmentUploadBusy({ digest: input.reference.digest })
            }
            const ownedToken = finalizationToken
            return yield* Effect.gen(function*() {
              let verified = yield* adapter.inspectFinalized({
                spaceId: attempt.space_id,
                upload,
                reference: input.reference
              }).pipe(
                Effect.catchTags({
                  AttachmentObjectStoreUnavailable: (cause) =>
                    Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.inspect", cause })),
                  AttachmentProviderUploadNotFound: () =>
                    retireOwnedFinalization(attempt, ownedToken).pipe(
                      Effect.andThen(
                        Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                      )
                    ),
                  AttachmentProviderObjectNotFound: () =>
                    retireOwnedFinalization(attempt, ownedToken).pipe(
                      Effect.andThen(
                        Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                      )
                    )
                }),
                Effect.flatMap((value) => {
                  if (value === null) return Effect.succeed(null)
                  return Schema.decodeUnknownEffect(AttachmentObjectStore.VerifiedObject)(value)
                }),
                Effect.catchTag("SchemaError", (cause) =>
                  Effect.fail(
                    new Attachment.AttachmentStorageError({
                      operation: "upload.inspect.decode",
                      cause
                    })
                  ))
              )
              if (verified === null) {
                verified = yield* adapter.finalizeUpload({
                  spaceId: attempt.space_id,
                  upload,
                  reference: input.reference
                }).pipe(
                  Effect.catchTags({
                    AttachmentObjectStoreUnavailable: (cause) =>
                      Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.finalize", cause })),
                    AttachmentProviderUploadNotFound: () =>
                      retireOwnedFinalization(attempt, ownedToken).pipe(
                        Effect.andThen(
                          Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                        )
                      ),
                    AttachmentProviderObjectNotFound: () =>
                      retireOwnedFinalization(attempt, ownedToken).pipe(
                        Effect.andThen(
                          Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                        )
                      )
                  }),
                  Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.VerifiedObject)),
                  Effect.catchTag("SchemaError", (cause) =>
                    Effect.fail(
                      new Attachment.AttachmentStorageError({
                        operation: "upload.finalize.decode",
                        cause
                      })
                    ))
                )
              }
              if (
                verified.object.namespace !== attempt.provider_namespace ||
                verified.reference.digest !== input.reference.digest ||
                verified.reference.bytes !== input.reference.bytes ||
                verified.chunkBytes !== verificationChunkBytes ||
                verified.chunkCount !== Math.ceil(input.reference.bytes / verificationChunkBytes) ||
                verified.chunkCount > maximumVerificationChunks
              ) {
                return yield* new Attachment.AttachmentStorageError({
                  operation: "upload.finalize.verification",
                  cause: verified
                })
              }
              const chunks: Array<AttachmentTransfer.VerifiedChunk> = []
              let nextIndex = 0
              while (true) {
                const page = yield* adapter.listVerifiedChunks({
                  spaceId: attempt.space_id,
                  object: verified.object,
                  afterIndex: nextIndex,
                  limit: AttachmentObjectStore.maximumManifestPageChunks
                }).pipe(
                  Effect.catchTags({
                    AttachmentObjectStoreUnavailable: (cause) =>
                      Effect.fail(new Attachment.AttachmentStorageError({ operation: "upload.manifest", cause })),
                    AttachmentProviderUploadNotFound: () =>
                      retireOwnedFinalization(attempt, ownedToken).pipe(
                        Effect.andThen(
                          Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                        )
                      ),
                    AttachmentProviderObjectNotFound: () =>
                      retireOwnedFinalization(attempt, ownedToken).pipe(
                        Effect.andThen(
                          Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest }))
                        )
                      )
                  }),
                  Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.VerifiedChunkPage)),
                  Effect.catchTag("SchemaError", (cause) =>
                    Effect.fail(
                      new Attachment.AttachmentStorageError({
                        operation: "upload.manifest.decode",
                        cause
                      })
                    ))
                )
                if (
                  page.chunks.length > verified.chunkCount - chunks.length ||
                  page.chunks.length > maximumVerificationChunks - chunks.length
                ) {
                  return yield* new Attachment.AttachmentStorageError({
                    operation: "upload.manifest.tooLarge",
                    cause: {
                      actual: chunks.length + page.chunks.length,
                      expected: Math.min(verified.chunkCount, maximumVerificationChunks)
                    }
                  })
                }
                chunks.push(...page.chunks)
                if (page.nextIndex === null) {
                  break
                }
                if (
                  page.chunks.length === 0 || page.nextIndex <= nextIndex ||
                  page.nextIndex > verified.chunkCount || page.nextIndex > maximumVerificationChunks
                ) {
                  return yield* new Attachment.AttachmentStorageError({
                    operation: "upload.manifest.pagination",
                    cause: page
                  })
                }
                nextIndex = page.nextIndex
              }
              let offset = 0
              for (let index = 0; index < chunks.length; index++) {
                const chunk = chunks[index]
                if (
                  chunk.index !== index || chunk.offset !== offset ||
                  chunk.bytes > verified.chunkBytes ||
                  (index < chunks.length - 1 && chunk.bytes !== verified.chunkBytes)
                ) {
                  return yield* new Attachment.AttachmentStorageError({
                    operation: "upload.manifest.invalid",
                    cause: { index, offset, chunk }
                  })
                }
                offset += chunk.bytes
              }
              if (chunks.length !== verified.chunkCount || offset !== input.reference.bytes) {
                return yield* new Attachment.AttachmentStorageError({
                  operation: "upload.manifest.incomplete",
                  cause: { chunks: chunks.length, expectedChunks: verified.chunkCount, offset }
                })
              }
              const committedAt = yield* Clock.currentTimeMillis
              const chunkRows = chunks.map((chunk) => ({
                space_id: input.spaceId,
                digest: input.reference.digest,
                chunk_index: chunk.index,
                chunk_offset: chunk.offset,
                chunk_bytes: chunk.bytes,
                chunk_digest: chunk.digest
              }))
              yield* sql.withTransaction(Effect.gen(function*() {
                const claim = yield* SqlSchema.findOneOption({
                  Request: Schema.Void,
                  Result: AttemptIdRow,
                  execute: () =>
                    sql`SELECT attempt_id FROM effect_local_server_attachment_attempts
                  WHERE attempt_id = ${attempt.attempt_id}
                    AND provider_upload_id = ${attempt.provider_upload_id}
                    AND finalization_token = ${ownedToken}`
                })(undefined).pipe(
                  Effect.catchTags({
                    SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                    SchemaError: (cause) =>
                      Effect.fail(
                        new ReplicaError.StorageCorrupt({
                          message: "Attachment finalization fence is corrupt",
                          cause
                        })
                      )
                  })
                )
                if (Option.isNone(claim)) {
                  yield* new Attachment.AttachmentUploadBusy({ digest: input.reference.digest })
                }
                const claimedDeletion = yield* SqlSchema.findOne({
                  Request: Schema.Void,
                  Result: Rows.CountRow,
                  execute: () =>
                    sql`SELECT COUNT(*) AS count FROM effect_local_server_attachment_deletions
                  WHERE operation = 'DeleteObject'
                    AND provider_namespace = ${verified.object.namespace}
                    AND provider_id = ${verified.object.id}
                    AND provider_version = ${verified.object.version}
                    AND (claim_token IS NOT NULL OR attempt_count > 0)`
                })(undefined).pipe(
                  Effect.catchTags({
                    SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                    NoSuchElementError: () =>
                      Effect.fail(
                        new ReplicaError.StorageCorrupt({
                          message: "Attachment deletion fence query returned no result"
                        })
                      ),
                    SchemaError: (cause) =>
                      Effect.fail(
                        new ReplicaError.StorageCorrupt({
                          message: "Attachment deletion fence state is corrupt",
                          cause
                        })
                      )
                  })
                )
                if (claimedDeletion.count > 0) {
                  yield* new Attachment.AttachmentUploadBusy({ digest: input.reference.digest })
                }
                yield* sql`DELETE FROM effect_local_server_attachment_deletions
                WHERE operation = 'DeleteObject'
                  AND provider_namespace = ${verified.object.namespace}
                  AND provider_id = ${verified.object.id}
                  AND provider_version = ${verified.object.version}
                  AND claim_token IS NULL AND attempt_count = 0`
                const existing = yield* findObject(input.spaceId, input.reference.digest)
                if (Option.isNone(existing)) {
                  const objectVersion = AttachmentTransfer.ObjectVersion.make(
                    yield* newId("upload.objectVersion")
                  )
                  yield* sql`INSERT INTO effect_local_server_attachment_objects
                  (space_id, digest, bytes, object_version, state, provider_namespace, provider_object_id,
                  provider_object_version, chunk_bytes, chunk_count, garbage_collect_after,
                  created_at, last_accessed_at)
                VALUES (${input.spaceId}, ${input.reference.digest}, ${input.reference.bytes},
                  ${objectVersion}, 'Available',
                  ${verified.object.namespace}, ${verified.object.id}, ${verified.object.version},
                  ${verified.chunkBytes}, ${verified.chunkCount},
                  ${committedAt + garbageCollectionGracePeriod}, ${committedAt}, ${committedAt})`
                  for (let batchOffset = 0; batchOffset < chunkRows.length; batchOffset += 100) {
                    yield* sql`INSERT INTO effect_local_server_attachment_chunks
                    ${sql.insert(chunkRows.slice(batchOffset, batchOffset + 100))}`
                  }
                } else if (existing.value.state === "Missing") {
                  const objectVersion = AttachmentTransfer.ObjectVersion.make(
                    yield* newId("upload.repairObjectVersion")
                  )
                  yield* sql`DELETE FROM effect_local_server_attachment_chunks
                  WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}`
                  yield* sql`UPDATE effect_local_server_attachment_objects
                  SET object_version = ${objectVersion}, state = 'Available',
                    provider_namespace = ${verified.object.namespace},
                    provider_object_id = ${verified.object.id},
                    provider_object_version = ${verified.object.version},
                    chunk_bytes = ${verified.chunkBytes}, chunk_count = ${verified.chunkCount},
                    last_accessed_at = ${committedAt}
                  WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}
                    AND state = 'Missing'`
                  for (let batchOffset = 0; batchOffset < chunkRows.length; batchOffset += 100) {
                    yield* sql`INSERT INTO effect_local_server_attachment_chunks
                    ${sql.insert(chunkRows.slice(batchOffset, batchOffset + 100))}`
                  }
                } else {
                  if (existing.value.bytes !== input.reference.bytes) {
                    yield* new Attachment.AttachmentLengthMismatch({
                      expected: existing.value.bytes,
                      actual: input.reference.bytes
                    })
                  }
                  if (
                    existing.value.provider_namespace !== verified.object.namespace ||
                    existing.value.provider_object_id !== verified.object.id ||
                    existing.value.provider_object_version !== verified.object.version
                  ) {
                    const outboxId = yield* newId("upload.proofDeletion")
                    yield* sql`INSERT INTO effect_local_server_attachment_deletions
                    (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                      provider_id, provider_version, next_attempt_at, created_at)
                    VALUES (${outboxId}, ${input.spaceId}, ${input.reference.digest},
                      ${input.reference.bytes}, 'DeleteObject', ${verified.object.namespace},
                      ${verified.object.id}, ${verified.object.version}, ${committedAt}, ${committedAt})`
                  }
                }
                yield* sql`INSERT OR IGNORE INTO effect_local_server_attachment_possessions
              (space_id, digest, client_id, membership_incarnation)
              VALUES (${input.spaceId}, ${input.reference.digest}, ${input.clientId},
                ${input.membershipIncarnation})`
                yield* sql`DELETE FROM effect_local_server_attachment_attempts
                WHERE attempt_id = ${attempt.attempt_id}
                  AND provider_upload_id = ${attempt.provider_upload_id}
                  AND finalization_token = ${ownedToken}`
              })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
              return AttachmentTransfer.UploadComplete.make({})
            }).pipe(
              Effect.raceFirst(finalizationHeartbeat(attempt.attempt_id, input.reference.digest, ownedToken))
            )
          })
        )
      }, Effect.withSpan("AttachmentServer.finalizeUpload"))

      const readableEntityPage = SqlSchema.findAll({
        Request: Schema.Struct({
          spaceId: Identity.SpaceId,
          digest: Attachment.Digest,
          afterModel: Schema.NullOr(Schema.String),
          afterKey: Schema.NullOr(Schema.String),
          limit: Schema.Number
        }),
        Result: ReadableEntity,
        execute: ({ afterKey, afterModel, digest, limit, spaceId }) =>
          sql`SELECT e.model, e.model_version, e.entity_key, e.value_json
          FROM effect_local_server_attachment_references AS r
          JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
            AND s.active_schema_generation = r.schema_generation
          JOIN effect_local_server_entities_data AS e ON e.space_id = r.space_id
            AND e.generation = r.schema_generation AND e.model = r.model AND e.entity_key = r.entity_key
          WHERE r.space_id = ${spaceId} AND r.digest = ${digest}
            AND (${afterModel} IS NULL OR e.model > ${afterModel}
              OR (e.model = ${afterModel} AND e.entity_key > ${afterKey}))
          ORDER BY e.model, e.entity_key LIMIT ${limit}`
      })

      const authorizeReadableEntity = Effect.fnUntraced(function*(input: AuthorizationInput) {
        let afterModel: string | null = null
        let afterKey: string | null = null
        while (true) {
          const entities: ReadonlyArray<typeof ReadableEntity.Type> = yield* readableEntityPage({
            spaceId: input.spaceId,
            digest: input.reference.digest,
            afterModel,
            afterKey,
            limit: 64
          }).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Attachment read authorization state is corrupt",
                    cause
                  })
                )
            })
          )
          if (entities.length === 0) break
          for (const row of entities) {
            const key = yield* Codec.parse(row.entity_key).pipe(
              Effect.flatMap((value) => Codec.decode(Schema.Json, value))
            )
            const value = yield* Codec.parse(row.value_json).pipe(
              Effect.flatMap((parsed) => Codec.decode(Schema.Json, parsed))
            )
            const allowed = yield* options.authorizeRead({
              ...input,
              entity: { model: row.model, modelVersion: row.model_version, key },
              value
            }).pipe(Effect.provide(context), Effect.option)
            if (Option.isSome(allowed)) return row
          }
          const last: typeof ReadableEntity.Type = entities[entities.length - 1]
          afterModel = last.model
          afterKey = last.entity_key
        }
        return yield* new ReplicaError.AuthorizationDenied({
          reason: { _tag: "AttachmentReadDenied" }
        })
      })

      const prepareDownload: Service["prepareDownload"] = Effect.fnUntraced(function*(input) {
        yield* validateReference(input.reference)
        yield* authorize(input, false)
        const authorizedEntity = yield* authorizeReadableEntity(input)
        const objectOption = yield* findObject(input.spaceId, input.reference.digest)
        if (Option.isNone(objectOption)) {
          return yield* new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
        }
        const object = objectOption.value
        if (object.state !== "Available") {
          return yield* new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
        }
        if (object.bytes !== input.reference.bytes) {
          return yield* new Attachment.AttachmentLengthMismatch({
            expected: object.bytes,
            actual: input.reference.bytes
          })
        }
        const rangeOffset = input.range?.offset ?? 0
        const rangeLength = input.range?.length ?? object.bytes - rangeOffset
        if (
          rangeOffset < 0 || rangeOffset >= object.bytes || rangeLength <= 0 ||
          rangeOffset + rangeLength > object.bytes
        ) {
          if (input.range?.length === undefined) {
            return yield* new Attachment.InvalidAttachmentRange({
              bytes: object.bytes,
              offset: rangeOffset
            })
          }
          return yield* new Attachment.InvalidAttachmentRange({
            bytes: object.bytes,
            offset: rangeOffset,
            length: input.range.length
          })
        }
        const chunkOption = yield* SqlSchema.findOneOption({
          Request: Schema.Void,
          Result: Rows.ServerAttachmentChunkRow,
          execute: () =>
            sql`SELECT chunk_index, chunk_offset, chunk_bytes, chunk_digest
            FROM effect_local_server_attachment_chunks
            WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}
              AND chunk_offset <= ${rangeOffset} AND chunk_offset + chunk_bytes > ${rangeOffset}
            ORDER BY chunk_index LIMIT 1`
        })(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment verification chunk is corrupt",
                  cause
                })
              )
          })
        )
        if (Option.isNone(chunkOption)) {
          return yield* new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
        }
        const row = chunkOption.value
        const chunk = AttachmentTransfer.VerifiedChunk.make({
          index: row.chunk_index,
          offset: row.chunk_offset,
          bytes: row.chunk_bytes,
          digest: row.chunk_digest
        })
        const sliceOffset = rangeOffset - chunk.offset
        const sliceLength = Math.min(rangeLength, chunk.bytes - sliceOffset)
        const adapter = yield* objectStores.resolve(object.provider_namespace).pipe(
          Effect.catchTag(
            "AttachmentObjectStoreUnavailable",
            (cause) => Effect.fail(new Attachment.AttachmentStorageError({ operation: "download.resolve", cause }))
          )
        )
        const exactObject = AttachmentObjectStore.ObjectIdentity.make({
          namespace: object.provider_namespace,
          id: object.provider_object_id,
          version: object.provider_object_version
        })
        const requestedExpiry = (yield* Clock.currentTimeMillis) + downloadGrantLifetime
        const granted = yield* adapter.grantDownload({
          spaceId: object.space_id,
          object: exactObject,
          chunk,
          expiresAt: requestedExpiry
        }).pipe(
          Effect.catchTags({
            AttachmentObjectStoreUnavailable: (cause) =>
              Effect.fail(new Attachment.AttachmentStorageError({ operation: "download.grant", cause })),
            AttachmentProviderUploadNotFound: () =>
              Effect.fail(new Attachment.AttachmentUnavailable({ digest: input.reference.digest })),
            AttachmentProviderObjectNotFound: () =>
              sql`UPDATE effect_local_server_attachment_objects SET state = 'Missing'
                WHERE space_id = ${input.spaceId} AND digest = ${input.reference.digest}
                  AND object_version = ${object.object_version}
                  AND provider_namespace = ${object.provider_namespace}
                  AND provider_object_id = ${object.provider_object_id}
                  AND provider_object_version = ${object.provider_object_version}`.pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
                Effect.andThen(Effect.fail(
                  new Attachment.AttachmentUnavailable({ digest: input.reference.digest })
                ))
              )
          }),
          Effect.flatMap(Schema.decodeUnknownEffect(AttachmentObjectStore.DirectDownloadGrant)),
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new Attachment.AttachmentStorageError({
                operation: "download.grant.decode",
                cause
              })
            ))
        )
        const now = yield* Clock.currentTimeMillis
        if (granted.expiresAt > requestedExpiry || granted.expiresAt <= now) {
          return yield* new Attachment.AttachmentStorageError({
            operation: "download.grant.expiry",
            cause: granted
          })
        }
        const grantId = AttachmentTransfer.GrantId.make(yield* newId("download.grantId"))
        const revalidated = yield* SqlSchema.findOneOption({
          Request: Schema.Void,
          Result: DigestRow,
          execute: () =>
            sql`INSERT INTO effect_local_server_attachment_download_grants
              (grant_id, space_id, digest, object_version, provider_namespace, provider_object_id,
                provider_object_version, chunk_index, expires_at, created_at)
            SELECT ${grantId}, o.space_id, o.digest, o.object_version, o.provider_namespace, o.provider_object_id,
              o.provider_object_version, ${chunk.index},
              ${granted.expiresAt + grantSafetyInterval}, ${now}
            FROM effect_local_server_attachment_objects AS o
            JOIN effect_local_server_attachment_references AS r
              ON r.space_id = o.space_id AND r.digest = o.digest
            JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
              AND s.active_schema_generation = r.schema_generation
            JOIN effect_local_server_entities_data AS e ON e.space_id = r.space_id
              AND e.generation = r.schema_generation AND e.model = r.model AND e.entity_key = r.entity_key
            WHERE o.space_id = ${input.spaceId} AND o.digest = ${input.reference.digest}
              AND o.provider_namespace = ${object.provider_namespace}
              AND o.provider_object_id = ${object.provider_object_id}
              AND o.provider_object_version = ${object.provider_object_version}
              AND e.model = ${authorizedEntity.model} AND e.entity_key = ${authorizedEntity.entity_key}
              AND e.value_json = ${authorizedEntity.value_json}
            LIMIT 1
            RETURNING digest`
        })(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment download grant state is corrupt",
                  cause
                })
              )
          })
        )
        if (Option.isNone(revalidated)) {
          return yield* new ReplicaError.AuthorizationDenied({
            reason: { _tag: "AttachmentAuthorizationChanged" }
          })
        }
        return AttachmentTransfer.DownloadGrant.make({
          grantId,
          objectVersion: object.object_version,
          objectBytes: object.bytes,
          expiresAt: granted.expiresAt,
          chunk,
          slice: { offset: sliceOffset, length: sliceLength },
          request: granted.request
        })
      }, Effect.withSpan("AttachmentServer.prepareDownload"))

      const replaceEntityReferences: Service["replaceEntityReferences"] = Effect.fnUntraced(function*(input) {
        let references: ReadonlyArray<Attachment.Reference> = []
        if (input.value !== undefined) references = yield* Attachment.collect(input.value)
        const now = yield* Clock.currentTimeMillis
        yield* sql.withTransaction(Effect.gen(function*() {
          const previous = yield* SqlSchema.findAll({
            Request: Schema.Void,
            Result: DigestRow,
            execute: () =>
              sql`SELECT digest FROM effect_local_server_attachment_references
              WHERE space_id = ${input.spaceId} AND schema_generation = ${input.schemaGeneration}
                AND model = ${input.model} AND entity_key = ${input.entityKey}`
          })(undefined).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Attachment reference state is corrupt",
                    cause
                  })
                )
            })
          )
          const previousDigests = new Set(previous.map(({ digest }) => digest))
          for (const reference of references) {
            const object = yield* findObject(input.spaceId, reference.digest)
            if (Option.isNone(object)) {
              yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
              continue
            }
            if (object.value.bytes !== reference.bytes) {
              yield* new Attachment.AttachmentLengthMismatch({
                expected: object.value.bytes,
                actual: reference.bytes
              })
            }
            if (input.authority._tag === "Mutation" && !previousDigests.has(reference.digest)) {
              const authority = input.authority
              const possession = yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: DigestRow,
                execute: () =>
                  sql`SELECT digest FROM effect_local_server_attachment_possessions
                  WHERE space_id = ${input.spaceId} AND digest = ${reference.digest}
                    AND client_id = ${authority.clientId}
                    AND membership_incarnation = ${authority.membershipIncarnation}`
              })(undefined).pipe(
                Effect.catchTags({
                  SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                  SchemaError: (cause) =>
                    Effect.fail(
                      new ReplicaError.StorageCorrupt({
                        message: "Attachment possession state is corrupt",
                        cause
                      })
                    )
                })
              )
              if (Option.isNone(possession)) {
                yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
              }
            }
            const count = yield* SqlSchema.findOne({
              Request: Schema.Void,
              Result: Rows.CountRow,
              execute: () =>
                sql`SELECT COUNT(*) AS count FROM effect_local_server_attachment_references
                WHERE space_id = ${input.spaceId} AND schema_generation = ${input.schemaGeneration}
                  AND digest = ${reference.digest}
                  AND NOT (model = ${input.model} AND entity_key = ${input.entityKey})`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                NoSuchElementError: () =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment reference quota query returned no result"
                    })
                  ),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment reference quota state is corrupt",
                      cause
                    })
                  )
              })
            )
            if (count.count >= maximumReferencesPerObject) {
              yield* new ReplicaError.CapacityExceeded({
                resource: "attachment references per object",
                limit: maximumReferencesPerObject
              })
            }
          }
          yield* sql`DELETE FROM effect_local_server_attachment_references
            WHERE space_id = ${input.spaceId} AND schema_generation = ${input.schemaGeneration}
              AND model = ${input.model} AND entity_key = ${input.entityKey}`
          for (const reference of references) {
            yield* sql`INSERT INTO effect_local_server_attachment_references
              (space_id, schema_generation, digest, model, model_version, entity_key)
              VALUES (${input.spaceId}, ${input.schemaGeneration}, ${reference.digest},
                ${input.model}, ${input.modelVersion}, ${input.entityKey})`
            yield* sql`UPDATE effect_local_server_attachment_objects SET garbage_collect_after = NULL
              WHERE space_id = ${input.spaceId} AND digest = ${reference.digest}`
          }
          for (const { digest } of previous) {
            yield* sql`UPDATE effect_local_server_attachment_objects SET
              garbage_collect_after = ${now + garbageCollectionGracePeriod}
            WHERE space_id = ${input.spaceId} AND digest = ${digest}
              AND garbage_collect_after IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_references AS r
                JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
                  AND s.active_schema_generation = r.schema_generation
                WHERE r.space_id = effect_local_server_attachment_objects.space_id
                  AND r.digest = effect_local_server_attachment_objects.digest
              )`
          }
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
      }, Effect.withSpan("AttachmentServer.replaceEntityReferences"))

      const activateGeneration: Service["activateGeneration"] = Effect.fnUntraced(function*(
        spaceId,
        schemaGeneration
      ) {
        const now = yield* Clock.currentTimeMillis
        yield* sql`UPDATE effect_local_server_attachment_objects AS o SET
          garbage_collect_after = CASE
            WHEN EXISTS (
              SELECT 1 FROM effect_local_server_attachment_references AS r
              WHERE r.space_id = o.space_id AND r.schema_generation = ${schemaGeneration}
                AND r.digest = o.digest
            ) THEN NULL
            WHEN o.garbage_collect_after IS NULL THEN ${now + garbageCollectionGracePeriod}
            ELSE o.garbage_collect_after
          END
          WHERE o.space_id = ${spaceId}`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
      }, Effect.withSpan("AttachmentServer.activateGeneration"))

      const expireGrants: Service["expireGrants"] = Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        yield* sql`DELETE FROM effect_local_server_attachment_upload_grants
          WHERE expires_at <= ${now}`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
        yield* sql`DELETE FROM effect_local_server_attachment_download_grants
          WHERE expires_at <= ${now}`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
      }).pipe(Effect.withSpan("AttachmentServer.expireGrants"))

      const sweepSpace: Service["sweepSpace"] = Effect.fnUntraced(function*(spaceId) {
        const now = yield* Clock.currentTimeMillis
        const staleAttempts = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Rows.ServerAttachmentAttemptRow,
          execute: () =>
            sql`SELECT attempt_id, space_id, digest, bytes, client_id, membership_incarnation,
              provider_namespace, physical_key, provider_upload_id, part_size, state,
              finalization_token, finalization_expires_at, created_at, last_accessed_at
            FROM effect_local_server_attachment_attempts AS a
            WHERE a.space_id = ${spaceId} AND a.last_accessed_at <= ${now - stagingLifetime}
              AND (a.finalization_expires_at IS NULL OR a.finalization_expires_at <= ${now})
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_upload_grants AS g
                WHERE g.attempt_id = a.attempt_id AND g.expires_at > ${now}
              )
            ORDER BY a.last_accessed_at, a.attempt_id LIMIT ${deletionBatchSize}`
        })(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment stale attempt state is corrupt",
                  cause
                })
              )
          })
        )
        let swept = 0
        for (const selected of staleAttempts) {
          if (selected.state === "Finalizing") {
            if (
              selected.provider_upload_id === null || selected.finalization_token === null ||
              selected.finalization_expires_at === null
            ) {
              continue
            }
            const deferFinalizing = sql`UPDATE effect_local_server_attachment_attempts
              SET last_accessed_at = ${now}
              WHERE attempt_id = ${selected.attempt_id}
                AND state = 'Finalizing'
                AND provider_namespace = ${selected.provider_namespace}
                AND provider_upload_id = ${selected.provider_upload_id}
                AND finalization_token = ${selected.finalization_token}
                AND finalization_expires_at = ${selected.finalization_expires_at}
                AND finalization_expires_at <= ${now}
                AND last_accessed_at <= ${now - stagingLifetime}
                AND NOT EXISTS (
                  SELECT 1 FROM effect_local_server_attachment_upload_grants
                  WHERE attempt_id = ${selected.attempt_id} AND expires_at > ${now}
                )`.pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
            )
            const adapter = yield* objectStores.resolve(selected.provider_namespace).pipe(
              Effect.map(Option.some),
              Effect.catchTag("AttachmentObjectStoreUnavailable", () => Effect.succeed(Option.none()))
            )
            if (Option.isNone(adapter)) {
              yield* deferFinalizing
              continue
            }
            const upload = AttachmentObjectStore.UploadIdentity.make({
              namespace: selected.provider_namespace,
              id: AttachmentObjectStore.ProviderId.make(selected.provider_upload_id)
            })
            const reference = Attachment.Reference.make({
              digest: selected.digest,
              bytes: selected.bytes
            })
            let providerUnavailable = false
            const verified = yield* adapter.value.inspectFinalized({
              spaceId: selected.space_id,
              upload,
              reference
            }).pipe(
              Effect.catchTags({
                AttachmentObjectStoreUnavailable: () => {
                  providerUnavailable = true
                  return Effect.succeed(null)
                },
                AttachmentProviderUploadNotFound: () => Effect.succeed(null),
                AttachmentProviderObjectNotFound: () => Effect.succeed(null)
              }),
              Effect.flatMap((value) => {
                if (value === null) return Effect.succeed(null)
                return Schema.decodeUnknownEffect(AttachmentObjectStore.VerifiedObject)(value)
              }),
              Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(
                  new Attachment.AttachmentStorageError({
                    operation: "upload.sweepInspect.decode",
                    cause
                  })
                ))
            )
            if (providerUnavailable || verified !== null) {
              yield* deferFinalizing
              continue
            }
            const outboxId = yield* newId("upload.finalizingAbortOutbox")
            const removed = yield* sql.withTransaction(Effect.gen(function*() {
              yield* sql`INSERT INTO effect_local_server_attachment_deletions
                (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                  provider_id, next_attempt_at, created_at)
                SELECT ${outboxId}, a.space_id, a.digest, a.bytes, 'AbortUpload',
                  a.provider_namespace, a.provider_upload_id, ${now}, ${now}
                FROM effect_local_server_attachment_attempts AS a
                WHERE a.attempt_id = ${selected.attempt_id}
                  AND a.state = 'Finalizing'
                  AND a.provider_namespace = ${selected.provider_namespace}
                  AND a.provider_upload_id = ${selected.provider_upload_id}
                  AND a.finalization_token = ${selected.finalization_token}
                  AND a.finalization_expires_at = ${selected.finalization_expires_at}
                  AND a.finalization_expires_at <= ${now}
                  AND a.last_accessed_at <= ${now - stagingLifetime}
                  AND NOT EXISTS (
                    SELECT 1 FROM effect_local_server_attachment_upload_grants
                    WHERE attempt_id = a.attempt_id AND expires_at > ${now}
                  )`
              return yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: AttemptIdRow,
                execute: () =>
                  sql`DELETE FROM effect_local_server_attachment_attempts
                    WHERE attempt_id = ${selected.attempt_id}
                      AND state = 'Finalizing'
                      AND provider_namespace = ${selected.provider_namespace}
                      AND provider_upload_id = ${selected.provider_upload_id}
                      AND finalization_token = ${selected.finalization_token}
                      AND finalization_expires_at = ${selected.finalization_expires_at}
                      AND finalization_expires_at <= ${now}
                      AND last_accessed_at <= ${now - stagingLifetime}
                      AND EXISTS (
                        SELECT 1 FROM effect_local_server_attachment_deletions
                        WHERE outbox_id = ${outboxId}
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM effect_local_server_attachment_upload_grants
                        WHERE attempt_id = ${selected.attempt_id} AND expires_at > ${now}
                      )
                    RETURNING attempt_id`
              })(undefined).pipe(
                Effect.catchTags({
                  SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                  SchemaError: (cause) =>
                    Effect.fail(
                      new ReplicaError.StorageCorrupt({
                        message: "Attachment Finalizing attempt removal is corrupt",
                        cause
                      })
                    )
                })
              )
            })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
            if (Option.isSome(removed)) swept++
            continue
          }
          let attempt = selected
          if (selected.provider_upload_id === null) attempt = yield* beginAttempt(selected)
          const removed = yield* sql.withTransaction(Effect.gen(function*() {
            let outboxId: string | null = null
            if (attempt.provider_upload_id !== null) {
              outboxId = yield* newId("upload.abortOutbox")
              yield* sql`INSERT INTO effect_local_server_attachment_deletions
                (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                  provider_id, next_attempt_at, created_at)
                SELECT ${outboxId}, a.space_id, a.digest, a.bytes, 'AbortUpload',
                  a.provider_namespace, a.provider_upload_id, ${now}, ${now}
                FROM effect_local_server_attachment_attempts AS a
                WHERE a.attempt_id = ${attempt.attempt_id}
                  AND a.provider_upload_id = ${attempt.provider_upload_id}
                  AND a.state <> 'Finalizing'
                  AND a.last_accessed_at <= ${now - stagingLifetime}
                  AND (a.finalization_expires_at IS NULL OR a.finalization_expires_at <= ${now})
                  AND NOT EXISTS (
                    SELECT 1 FROM effect_local_server_attachment_upload_grants
                    WHERE attempt_id = a.attempt_id AND expires_at > ${now}
                  )`
            }
            return yield* SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: AttemptIdRow,
              execute: () =>
                sql`DELETE FROM effect_local_server_attachment_attempts
                WHERE attempt_id = ${attempt.attempt_id}
                  AND state <> 'Finalizing'
                  AND last_accessed_at <= ${now - stagingLifetime}
                  AND (finalization_expires_at IS NULL OR finalization_expires_at <= ${now})
                  AND (
                    provider_upload_id IS NULL OR EXISTS (
                      SELECT 1 FROM effect_local_server_attachment_deletions
                      WHERE outbox_id = ${outboxId}
                    )
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM effect_local_server_attachment_upload_grants
                    WHERE attempt_id = ${attempt.attempt_id} AND expires_at > ${now}
                  )
                RETURNING attempt_id`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment stale attempt removal is corrupt",
                      cause
                    })
                  )
              })
            )
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
          if (Option.isSome(removed)) swept++
        }
        const dueObjects = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Rows.ServerAttachmentObjectRow,
          execute: () =>
            sql`SELECT space_id, digest, bytes, object_version, state, provider_namespace, provider_object_id,
              provider_object_version, chunk_bytes, chunk_count, garbage_collect_after,
              created_at, last_accessed_at
            FROM effect_local_server_attachment_objects AS o
            WHERE o.space_id = ${spaceId} AND o.garbage_collect_after IS NOT NULL
              AND o.garbage_collect_after <= ${now}
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_references AS r
                JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
                  AND s.active_schema_generation = r.schema_generation
                WHERE r.space_id = o.space_id AND r.digest = o.digest
              )
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_server_attachment_download_grants AS g
                WHERE g.space_id = o.space_id AND g.digest = o.digest
                  AND g.object_version = o.object_version
                  AND g.provider_namespace = o.provider_namespace
                  AND g.provider_object_id = o.provider_object_id
                  AND g.provider_object_version = o.provider_object_version
                  AND g.expires_at > ${now}
              )
            ORDER BY o.garbage_collect_after, o.digest
            LIMIT ${Math.max(0, deletionBatchSize - staleAttempts.length)}`
        })(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Attachment garbage collection state is corrupt",
                  cause
                })
              )
          })
        )
        for (const object of dueObjects) {
          const outboxId = yield* newId("object.deleteOutbox")
          yield* sql.withTransaction(Effect.gen(function*() {
            yield* sql`INSERT INTO effect_local_server_attachment_deletions
              (outbox_id, space_id, digest, bytes, operation, provider_namespace,
                provider_id, provider_version, next_attempt_at, created_at)
              SELECT ${outboxId}, space_id, digest, bytes, 'DeleteObject', provider_namespace,
                provider_object_id, provider_object_version, ${now}, ${now}
              FROM effect_local_server_attachment_objects AS o
              WHERE o.space_id = ${object.space_id} AND o.digest = ${object.digest}
                AND o.provider_namespace = ${object.provider_namespace}
                AND o.provider_object_id = ${object.provider_object_id}
                AND o.provider_object_version = ${object.provider_object_version}
                AND o.garbage_collect_after IS NOT NULL AND o.garbage_collect_after <= ${now}
                AND NOT EXISTS (
                  SELECT 1 FROM effect_local_server_attachment_references AS r
                  JOIN effect_local_server_spaces AS s ON s.space_id = r.space_id
                    AND s.active_schema_generation = r.schema_generation
                  WHERE r.space_id = o.space_id AND r.digest = o.digest
                )
                AND NOT EXISTS (
                  SELECT 1 FROM effect_local_server_attachment_download_grants AS g
                  WHERE g.space_id = o.space_id AND g.digest = o.digest
                    AND g.object_version = o.object_version
                    AND g.provider_namespace = o.provider_namespace
                    AND g.provider_object_id = o.provider_object_id
                    AND g.provider_object_version = o.provider_object_version
                    AND g.expires_at > ${now}
                )`
            yield* sql`DELETE FROM effect_local_server_attachment_objects
              WHERE space_id = ${object.space_id} AND digest = ${object.digest}
                AND EXISTS (
                  SELECT 1 FROM effect_local_server_attachment_deletions
                  WHERE outbox_id = ${outboxId}
                )`
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
          swept++
        }
        return swept
      }, Effect.withSpan("AttachmentServer.sweepSpace"))

      const drainOutbox: Service["drainOutbox"] = Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const claimToken = yield* newId("attachment.outboxClaim")
        const claimed = yield* sql.withTransaction(Effect.gen(function*() {
          const candidates = yield* SqlSchema.findAll({
            Request: Schema.Void,
            Result: Rows.ServerAttachmentDeletionRow,
            execute: () =>
              sql`SELECT outbox_id, space_id, digest, bytes, operation, provider_namespace,
                provider_id, provider_version, charge_usage, attempt_count, next_attempt_at,
                claim_token, claimed_until, created_at
              FROM effect_local_server_attachment_deletions
              WHERE next_attempt_at <= ${now}
                AND (claim_token IS NULL OR claimed_until <= ${now})
              ORDER BY next_attempt_at, outbox_id LIMIT ${deletionBatchSize}`
          })(undefined).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Attachment deletion outbox is corrupt",
                    cause
                  })
                )
            })
          )
          const rows: Array<typeof Rows.ServerAttachmentDeletionRow.Type> = []
          for (const candidate of candidates) {
            const row = yield* SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: Rows.ServerAttachmentDeletionRow,
              execute: () =>
                sql`UPDATE effect_local_server_attachment_deletions
                SET claim_token = ${claimToken}, claimed_until = ${now + finalizationLeaseLifetime}
                WHERE outbox_id = ${candidate.outbox_id}
                  AND (claim_token IS NULL OR claimed_until <= ${now})
                RETURNING outbox_id, space_id, digest, bytes, operation, provider_namespace,
                  provider_id, provider_version, charge_usage, attempt_count, next_attempt_at,
                  claim_token, claimed_until, created_at`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment deletion claim is corrupt",
                      cause
                    })
                  )
              })
            )
            if (Option.isSome(row)) rows.push(row.value)
          }
          return rows
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        let completed = 0
        for (const row of claimed) {
          if (row.operation === "DeleteObject" && row.provider_version !== null) {
            const live = yield* SqlSchema.findOne({
              Request: Schema.Void,
              Result: Rows.CountRow,
              execute: () =>
                sql`SELECT COUNT(*) AS count FROM effect_local_server_attachment_objects
                WHERE provider_namespace = ${row.provider_namespace}
                  AND provider_object_id = ${row.provider_id}
                  AND provider_object_version = ${row.provider_version}`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                NoSuchElementError: () =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment deletion identity query returned no result"
                    })
                  ),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Attachment deletion identity state is corrupt",
                      cause
                    })
                  )
              })
            )
            if (live.count > 0) {
              yield* sql`DELETE FROM effect_local_server_attachment_deletions
                WHERE outbox_id = ${row.outbox_id} AND claim_token = ${claimToken}`.pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
              )
              completed++
              continue
            }
          }
          const adapterResult = yield* objectStores.resolve(row.provider_namespace).pipe(Effect.result)
          let result: Effect.Effect<void, AttachmentObjectStore.AttachmentObjectStoreUnavailable>
          if (adapterResult._tag === "Failure") {
            result = Effect.fail(adapterResult.failure)
          } else if (row.operation === "AbortUpload") {
            result = adapterResult.success.abortUpload({
              spaceId: row.space_id,
              upload: AttachmentObjectStore.UploadIdentity.make({
                namespace: row.provider_namespace,
                id: row.provider_id
              })
            })
          } else if (row.provider_version === null) {
            result = Effect.fail(
              new AttachmentObjectStore.AttachmentObjectStoreUnavailable({
                namespace: row.provider_namespace,
                operation: "delete.invalidVersion"
              })
            )
          } else {
            result = adapterResult.success.deleteObject({
              spaceId: row.space_id,
              object: AttachmentObjectStore.ObjectIdentity.make({
                namespace: row.provider_namespace,
                id: row.provider_id,
                version: row.provider_version
              })
            })
          }
          const outcome = yield* result.pipe(Effect.result)
          if (outcome._tag === "Success") {
            yield* sql`DELETE FROM effect_local_server_attachment_deletions
              WHERE outbox_id = ${row.outbox_id} AND claim_token = ${claimToken}`.pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
            )
            completed++
          } else {
            const attempts = row.attempt_count + 1
            const delay = Math.min(deletionRetryDelay * 2 ** Math.min(attempts - 1, 10), 60 * 60 * 1000)
            yield* sql`UPDATE effect_local_server_attachment_deletions
              SET attempt_count = ${attempts}, next_attempt_at = ${now + delay},
                claim_token = NULL, claimed_until = NULL
              WHERE outbox_id = ${row.outbox_id} AND claim_token = ${claimToken}`.pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
            )
          }
        }
        return completed
      }).pipe(Effect.withSpan("AttachmentServer.drainOutbox"))

      const maintain = (spaceId: Identity.SpaceId) =>
        expireGrants.pipe(
          Effect.andThen(sweepSpace(spaceId)),
          Effect.flatMap((swept) => drainOutbox.pipe(Effect.map((drained) => swept + drained)))
        )

      return AttachmentServer.of({
        prepareUpload,
        finalizeUpload,
        prepareDownload,
        replaceEntityReferences,
        activateGeneration,
        expireGrants,
        sweepSpace,
        drainOutbox,
        maintain
      })
    })
  )
