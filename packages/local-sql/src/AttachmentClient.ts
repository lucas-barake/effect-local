import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AttachmentStorage from "./AttachmentStorage.js"
import * as AttachmentTransfer from "./AttachmentTransfer.js"
import * as Configuration from "./internal/configuration.js"
import * as Rows from "./internal/rows.js"

export type ClientFailure =
  | AttachmentStorage.StorageFailure
  | Attachment.AttachmentLengthMismatch
  | Attachment.InvalidAttachmentRange
  | ReplicaError.ReplicaError

export interface Service {
  readonly stage: <E extends { readonly _tag: string }, R,>(
    spaceId: Identity.SpaceId,
    bytes: Stream.Stream<Uint8Array, E, R>
  ) => Effect.Effect<
    Attachment.Reference,
    | E
    | Attachment.AttachmentTooLarge
    | Attachment.AttachmentLengthMismatch
    | Attachment.AttachmentStorageError
    | ReplicaError.ReplicaError,
    R
  >
  readonly associatePending: (
    spaceId: Identity.SpaceId,
    mutationId: Identity.MutationId,
    references: ReadonlyArray<Attachment.Reference>
  ) => Effect.Effect<void, ClientFailure>
  readonly release: (
    spaceId: Identity.SpaceId,
    reference: Attachment.Reference
  ) => Effect.Effect<void, ClientFailure>
  readonly objectKey: (
    spaceId: Identity.SpaceId,
    reference: Attachment.Reference
  ) => Effect.Effect<AttachmentStorage.ObjectKey, ClientFailure>
  readonly read: (
    spaceId: Identity.SpaceId,
    clientId: Identity.ClientId,
    membershipIncarnation: Identity.MembershipIncarnation,
    reference: Attachment.Reference,
    range?: Attachment.Range
  ) => Stream.Stream<Uint8Array, ClientFailure>
  readonly markRemoteAvailable: (
    spaceId: Identity.SpaceId,
    reference: Attachment.Reference
  ) => Effect.Effect<void, ClientFailure>
  readonly ensureUploaded: (
    spaceId: Identity.SpaceId,
    clientId: Identity.ClientId,
    membershipIncarnation: Identity.MembershipIncarnation,
    references: ReadonlyArray<Attachment.Reference>
  ) => Effect.Effect<void, ClientFailure>
  readonly maintain: Effect.Effect<number, Attachment.AttachmentStorageError | ReplicaError.ReplicaError>
  readonly drainDeletions: (
    maximum: number
  ) => Effect.Effect<number, Attachment.AttachmentStorageError | ReplicaError.ReplicaError>
}

export class AttachmentClient extends Context.Service<AttachmentClient, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentClient"
) {}

export interface Options {
  readonly maximumLocalBytes: number
  readonly maximumLocalObjects: number
  readonly maximumCacheBytes: number
  readonly maximumCacheObjects: number
  readonly maximumCacheAge: Duration.Input
  readonly evictionBatchSize: number
}

const Lookup = Schema.Struct({ spaceId: Identity.SpaceId, digest: Attachment.Digest })
const CacheUsage = Schema.Struct({ object_count: Schema.Number, byte_count: Schema.Number })
const CacheCandidate = Schema.Struct({
  space_id: Identity.SpaceId,
  digest: Attachment.Digest,
  bytes: Attachment.ByteLength,
  object_key: AttachmentStorage.ObjectKey,
  last_accessed_at: Schema.Number
})
const CacheCandidateRequest = Schema.Struct({
  before: Schema.Number,
  maximum: Schema.Number
})

export const layer = (options: Options): Layer.Layer<
  AttachmentClient,
  ReplicaError.InvalidConfiguration,
  SqlClient.SqlClient | AttachmentStorage.AttachmentStorage | AttachmentTransfer.AttachmentTransfer
> =>
  Layer.effect(
    AttachmentClient,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const storage = yield* AttachmentStorage.AttachmentStorage
      const transfer = yield* AttachmentTransfer.AttachmentTransfer
      const maximumLocalBytes = yield* Configuration.positiveSafeInteger(
        "attachments.maximumLocalBytes",
        options.maximumLocalBytes
      )
      const maximumLocalObjects = yield* Configuration.positiveSafeInteger(
        "attachments.maximumLocalObjects",
        options.maximumLocalObjects
      )
      const maximumCacheBytes = yield* Configuration.positiveSafeInteger(
        "attachments.maximumCacheBytes",
        options.maximumCacheBytes
      )
      const maximumCacheObjects = yield* Configuration.positiveSafeInteger(
        "attachments.maximumCacheObjects",
        options.maximumCacheObjects
      )
      const maximumCacheAgeMillis = yield* Configuration.positiveIntegerDurationMillis(
        "attachments.maximumCacheAge",
        options.maximumCacheAge
      )
      const evictionBatchSize = yield* Configuration.positiveSafeInteger(
        "attachments.evictionBatchSize",
        options.evictionBatchSize
      )
      const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })
      const cacheGate = yield* Semaphore.make(1)
      const reservations = new Map<string, Attachment.Reference>()
      const activeReads = new Map<string, number>()
      const cacheUsage = SqlSchema.findOne({
        Request: Schema.Void,
        Result: CacheUsage,
        execute: () =>
          sql`SELECT COUNT(*) AS object_count, COALESCE(SUM(bytes), 0) AS byte_count
          FROM effect_local_client_attachments WHERE cache_managed = 1 AND remote_available = 1`
      })
      const localUsage = SqlSchema.findOne({
        Request: Schema.Void,
        Result: CacheUsage,
        execute: () =>
          sql`SELECT COUNT(*) AS object_count, COALESCE(SUM(bytes), 0) AS byte_count
          FROM effect_local_client_attachments`
      })
      const cacheCandidates = SqlSchema.findAll({
        Request: CacheCandidateRequest,
        Result: CacheCandidate,
        execute: ({ before, maximum }) =>
          sql`SELECT a.space_id, a.digest, a.bytes, a.object_key, a.last_accessed_at
          FROM effect_local_client_attachments AS a
          WHERE a.cache_managed = 1 AND a.remote_available = 1 AND a.last_accessed_at <= ${before}
            AND NOT EXISTS (
            SELECT 1 FROM effect_local_client_attachment_owners AS o
            WHERE o.space_id = a.space_id AND o.digest = a.digest
          )
          ORDER BY a.last_accessed_at, a.space_id, a.digest LIMIT ${maximum}`
      })
      const findAttachment = SqlSchema.findOneOption({
        Request: Lookup,
        Result: Rows.ClientAttachmentRow,
        execute: ({ digest, spaceId }) =>
          sql`SELECT space_id, digest, bytes, object_key, remote_available, cache_managed, created_at, last_accessed_at
          FROM effect_local_client_attachments WHERE space_id = ${spaceId} AND digest = ${digest}`
      })
      const findDeletions = SqlSchema.findAll({
        Request: Schema.Struct({ now: Schema.Number, maximum: Schema.Number }),
        Result: Rows.ClientAttachmentDeletionRow,
        execute: ({ maximum, now }) =>
          sql`SELECT object_key, attempt_count, next_attempt_at, created_at
          FROM effect_local_client_attachment_deletions
          WHERE next_attempt_at <= ${now} ORDER BY next_attempt_at, object_key LIMIT ${maximum}`
      })
      const find = (spaceId: Identity.SpaceId, digest: Attachment.Digest) =>
        findAttachment({ spaceId, digest }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment metadata is corrupt",
                  cause
                })
              )
          })
        )
      const withLock = <A, E extends { readonly _tag: string }, R,>(
        spaceId: Identity.SpaceId,
        digest: Attachment.Digest,
        effect: Effect.Effect<A, E, R>
      ): Effect.Effect<A, E, R> =>
        RcMap.get(locks, `${spaceId}:${digest}`).pipe(
          Effect.flatMap((lock) => lock.withPermit(effect)),
          Effect.scoped
        )
      const cacheKey = (spaceId: Identity.SpaceId, digest: Attachment.Digest) => `${spaceId}:${digest}`

      const drainDeletions: Service["drainDeletions"] = Effect.fnUntraced(function*(maximum) {
        const batchSize = yield* Configuration.positiveSafeInteger("attachments.deletionBatchSize", maximum)
        const now = yield* Clock.currentTimeMillis
        const pending = yield* findDeletions({ now, maximum: batchSize }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment deletion state is corrupt",
                  cause
                })
              )
          })
        )
        for (const deletion of pending) {
          yield* storage.remove(deletion.object_key).pipe(Effect.withSpan("AttachmentClient.removeObject"))
          yield* sql`DELETE FROM effect_local_client_attachment_deletions
          WHERE object_key = ${deletion.object_key}`.pipe(
            Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
          )
        }
        return pending.length
      }, Effect.withSpan("AttachmentClient.drainDeletions"))

      const drainInBackground = drainDeletions(8).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Attachment deletion will be retried").pipe(
            Effect.annotateLogs("error", error._tag)
          )
        ),
        Effect.ignore
      )

      const drainAllDeletions = Effect.fnUntraced(function*() {
        while (true) {
          const drained = yield* drainDeletions(evictionBatchSize)
          if (drained < evictionBatchSize) break
        }
      }, Effect.withSpan("AttachmentClient.drainAllDeletions"))

      const evictCache = Effect.fnUntraced(function*(
        admission?: Attachment.Reference,
        localAlreadyCounted = false,
        includeAdmissionInCache = true,
        cacheAlreadyCounted = localAlreadyCounted
      ) {
        if (admission !== undefined && admission.bytes > maximumLocalBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment storage",
            limit: maximumLocalBytes
          })
        }
        if (admission !== undefined && includeAdmissionInCache && admission.bytes > maximumCacheBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment cache",
            limit: maximumCacheBytes
          })
        }
        const now = yield* Clock.currentTimeMillis
        const cache = yield* cacheUsage(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment cache usage is missing",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment cache usage is corrupt",
                  cause
                })
              )
          })
        )
        const local = yield* localUsage(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment local usage is missing",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment local usage is corrupt",
                  cause
                })
              )
          })
        )
        let localAdmissionCount = 0
        let localAdmissionBytes = 0
        let cacheAdmissionCount = 0
        let cacheAdmissionBytes = 0
        if (admission !== undefined) {
          if (!localAlreadyCounted) {
            localAdmissionCount = 1
            localAdmissionBytes = admission.bytes
          }
          if (includeAdmissionInCache && !cacheAlreadyCounted) {
            cacheAdmissionCount = 1
            cacheAdmissionBytes = admission.bytes
          }
        }
        const ageOnly = admission === undefined
        const reservationBytes = Array.from(reservations.values()).reduce(
          (total, reference) => total + reference.bytes,
          0
        )
        let cacheObjectCount = cache.object_count + reservations.size + cacheAdmissionCount
        let cacheByteCount = cache.byte_count + reservationBytes + cacheAdmissionBytes
        let localObjectCount = local.object_count + reservations.size + localAdmissionCount
        let localByteCount = local.byte_count + reservationBytes + localAdmissionBytes
        let evicted = 0
        while (true) {
          const cacheCapacityExceeded = cacheObjectCount > maximumCacheObjects || cacheByteCount > maximumCacheBytes
          const localCapacityExceeded = localObjectCount > maximumLocalObjects || localByteCount > maximumLocalBytes
          const capacityExceeded = cacheCapacityExceeded || localCapacityExceeded
          if (!ageOnly && !capacityExceeded) break
          let before = now - maximumCacheAgeMillis
          if (capacityExceeded) before = now
          const candidates = yield* cacheCandidates({
            before,
            maximum: evictionBatchSize + reservations.size + activeReads.size + 1
          }).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment cache eviction state is corrupt",
                    cause
                  })
                )
            })
          )
          if (candidates.length === 0) break
          let removedAny = false
          for (const candidate of candidates) {
            const candidateKey = cacheKey(candidate.space_id, candidate.digest)
            if (reservations.has(candidateKey) || activeReads.has(candidateKey)) continue
            const deleted = yield* sql.withTransaction(Effect.gen(function*() {
              const removed = yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: CacheCandidate,
                execute: () =>
                  sql`DELETE FROM effect_local_client_attachments
                WHERE space_id = ${candidate.space_id} AND digest = ${candidate.digest}
                  AND remote_available = 1 AND NOT EXISTS (
                    SELECT 1 FROM effect_local_client_attachment_owners AS o
                    WHERE o.space_id = effect_local_client_attachments.space_id
                      AND o.digest = effect_local_client_attachments.digest
                  ) RETURNING space_id, digest, bytes, object_key, last_accessed_at`
              })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment cache eviction result is corrupt",
                    cause
                  })
                )))
              if (Option.isSome(removed)) {
                yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
                (object_key, next_attempt_at, created_at) VALUES (${candidate.object_key}, ${now}, ${now})`
              }
              return removed
            })).pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
            )
            if (Option.isSome(deleted)) {
              cacheObjectCount -= 1
              cacheByteCount -= candidate.bytes
              localObjectCount -= 1
              localByteCount -= candidate.bytes
              evicted += 1
              removedAny = true
            }
            const cacheWithinCapacity = cacheObjectCount <= maximumCacheObjects && cacheByteCount <= maximumCacheBytes
            const localWithinCapacity = localObjectCount <= maximumLocalObjects && localByteCount <= maximumLocalBytes
            if (admission !== undefined && cacheWithinCapacity && localWithinCapacity) break
          }
          if (!removedAny) break
          const cacheWithinCapacity = cacheObjectCount <= maximumCacheObjects && cacheByteCount <= maximumCacheBytes
          const localWithinCapacity = localObjectCount <= maximumLocalObjects && localByteCount <= maximumLocalBytes
          if (admission !== undefined && cacheWithinCapacity && localWithinCapacity) break
        }
        if (
          admission !== undefined &&
          (localObjectCount > maximumLocalObjects || localByteCount > maximumLocalBytes)
        ) {
          let limit = maximumLocalObjects
          if (localByteCount > maximumLocalBytes) limit = maximumLocalBytes
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment storage",
            limit
          })
        }
        if (
          admission !== undefined &&
          (cacheObjectCount > maximumCacheObjects || cacheByteCount > maximumCacheBytes)
        ) {
          let limit = maximumCacheObjects
          if (cacheByteCount > maximumCacheBytes) limit = maximumCacheBytes
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment cache",
            limit
          })
        }
        return evicted
      }, Effect.withSpan("AttachmentClient.evictCache"))

      const maintain: Service["maintain"] = cacheGate.withPermit(Effect.gen(function*() {
        yield* drainAllDeletions()
        const evicted = yield* evictCache()
        yield* drainAllDeletions()
        return evicted
      })).pipe(Effect.withSpan("AttachmentClient.maintain"))

      const stage: Service["stage"] = Effect.fnUntraced(
        function*<E extends { readonly _tag: string }, R,>(
          spaceId: Identity.SpaceId,
          bytes: Stream.Stream<Uint8Array, E, R>
        ): Effect.fn.Return<
          Attachment.Reference,
          | E
          | Attachment.AttachmentTooLarge
          | Attachment.AttachmentLengthMismatch
          | Attachment.AttachmentStorageError
          | ReplicaError.ReplicaError,
          R
        > {
          return yield* cacheGate.withPermit(Effect.gen(function*() {
            yield* drainAllDeletions()
            const staged = yield* storage.stage(bytes)
            const persist = withLock(
              spaceId,
              staged.reference.digest,
              Effect.gen(function*() {
                const now = yield* Clock.currentTimeMillis
                const existing = yield* find(spaceId, staged.reference.digest)
                if (Option.isSome(existing) && existing.value.bytes !== staged.reference.bytes) {
                  return yield* new Attachment.AttachmentLengthMismatch({
                    expected: existing.value.bytes,
                    actual: staged.reference.bytes
                  })
                }
                if (Option.isNone(existing)) {
                  yield* evictCache(staged.reference, false, false)
                  yield* drainAllDeletions()
                }
                let keepStaged = true
                let retiredKey: AttachmentStorage.ObjectKey | undefined
                if (Option.isSome(existing)) {
                  const exists = yield* storage.exists(existing.value.object_key)
                  if (exists) keepStaged = false
                  else retiredKey = existing.value.object_key
                }
                yield* sql.withTransaction(Effect.gen(function*() {
                  if (Option.isNone(existing)) {
                    yield* sql`INSERT INTO effect_local_client_attachments
              (space_id, digest, bytes, object_key, created_at, last_accessed_at)
              VALUES (${spaceId}, ${staged.reference.digest}, ${staged.reference.bytes},
                ${staged.key}, ${now}, ${now})`
                  } else if (keepStaged) {
                    yield* sql`UPDATE effect_local_client_attachments
              SET object_key = ${staged.key}, last_accessed_at = ${now}
              WHERE space_id = ${spaceId} AND digest = ${staged.reference.digest}`
                  } else {
                    yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
              (object_key, next_attempt_at, created_at) VALUES (${staged.key}, ${now}, ${now})`
                  }
                  if (retiredKey !== undefined) {
                    yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
              (object_key, next_attempt_at, created_at) VALUES (${retiredKey}, ${now}, ${now})`
                  }
                  yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_owners
            (space_id, digest, owner_kind, owner_id, created_at)
            VALUES (${spaceId}, ${staged.reference.digest}, 'Staged', 'staged', ${now})`
                })).pipe(
                  Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
                )
                return staged.reference
              })
            )
            const reference = yield* persist.pipe(
              Effect.onError(() =>
                storage.remove(staged.key).pipe(
                  Effect.tapError((error) =>
                    Effect.logWarning("Unused staged attachment could not be removed").pipe(
                      Effect.annotateLogs("error", error._tag)
                    )
                  ),
                  Effect.ignore
                )
              ),
              Effect.withSpan("AttachmentClient.persistStage", {
                attributes: {
                  "space.id": spaceId,
                  "attachment.digest": staged.reference.digest
                }
              })
            )
            yield* drainInBackground
            return reference
          }))
        },
        Effect.withSpan("AttachmentClient.stage", (spaceId) => ({
          attributes: { "space.id": spaceId }
        }))
      )

      const objectKey: Service["objectKey"] = Effect.fnUntraced(
        function*(spaceId, reference) {
          const found = yield* find(spaceId, reference.digest)
          if (Option.isNone(found)) {
            return yield* new Attachment.AttachmentNotFound({ key: reference.digest })
          }
          if (found.value.bytes !== reference.bytes) {
            return yield* new Attachment.AttachmentLengthMismatch({
              expected: found.value.bytes,
              actual: reference.bytes
            })
          }
          return found.value.object_key
        },
        Effect.withSpan("AttachmentClient.objectKey", (spaceId, reference) => ({
          attributes: {
            "space.id": spaceId,
            "attachment.digest": reference.digest
          }
        }))
      )

      const associatePending: Service["associatePending"] = Effect.fnUntraced(
        function*(
          spaceId,
          mutationId,
          references
        ) {
          const now = yield* Clock.currentTimeMillis
          for (const reference of references) {
            const key = yield* objectKey(spaceId, reference)
            if (!(yield* storage.exists(key))) {
              return yield* new Attachment.AttachmentNotFound({ key: reference.digest })
            }
          }
          yield* Effect.forEach(
            references,
            (reference) =>
              sql`INSERT OR IGNORE INTO effect_local_client_attachment_owners
            (space_id, digest, owner_kind, owner_id, created_at)
            VALUES (${spaceId}, ${reference.digest}, 'Pending', ${mutationId}, ${now})`.pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause }))),
                Effect.withSpan("AttachmentClient.associatePendingReference", {
                  attributes: {
                    "space.id": spaceId,
                    "attachment.digest": reference.digest
                  }
                })
              ),
            { discard: true }
          )
          return undefined
        },
        Effect.withSpan("AttachmentClient.associatePending", (spaceId) => ({
          attributes: { "space.id": spaceId }
        }))
      )

      const release: Service["release"] = Effect.fnUntraced(
        function*(spaceId, reference) {
          yield* withLock(
            spaceId,
            reference.digest,
            Effect.gen(function*() {
              yield* objectKey(spaceId, reference)
              const now = yield* Clock.currentTimeMillis
              yield* sql.withTransaction(Effect.gen(function*() {
                yield* sql`DELETE FROM effect_local_client_attachment_owners
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND owner_kind = 'Staged' AND owner_id = 'staged'`
                yield* sql`UPDATE effect_local_client_attachments SET cache_managed = 1
                WHERE space_id = ${spaceId} AND digest = ${reference.digest} AND remote_available = 1`
                yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
              (object_key, next_attempt_at, created_at)
              SELECT a.object_key, ${now}, ${now} FROM effect_local_client_attachments AS a
              WHERE a.space_id = ${spaceId} AND a.digest = ${reference.digest}
                AND a.remote_available = 0 AND NOT EXISTS (
                  SELECT 1 FROM effect_local_client_attachment_owners AS o
                  WHERE o.space_id = a.space_id AND o.digest = a.digest
                )`
                yield* sql`DELETE FROM effect_local_client_attachments
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND remote_available = 0 AND NOT EXISTS (
                  SELECT 1 FROM effect_local_client_attachment_owners AS o
                  WHERE o.space_id = effect_local_client_attachments.space_id
                    AND o.digest = effect_local_client_attachments.digest
                )`
              })).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
              )
            })
          )
          yield* cacheGate.withPermit(evictCache())
          yield* drainInBackground
        },
        Effect.withSpan("AttachmentClient.release", (spaceId, reference) => ({
          attributes: {
            "space.id": spaceId,
            "attachment.digest": reference.digest
          }
        }))
      )

      const downloadRemote = Effect.fnUntraced(
        function*(
          spaceId: Identity.SpaceId,
          clientId: Identity.ClientId,
          membershipIncarnation: Identity.MembershipIncarnation,
          reference: Attachment.Reference
        ) {
          const existing = yield* find(spaceId, reference.digest)
          if (Option.isSome(existing)) {
            if (existing.value.bytes !== reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: existing.value.bytes,
                actual: reference.bytes
              })
            }
            if (yield* storage.exists(existing.value.object_key)) {
              const now = yield* Clock.currentTimeMillis
              yield* sql`UPDATE effect_local_client_attachments SET last_accessed_at = ${now}
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}`.pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
              )
              return existing.value.object_key
            }
          }
          const reservationKey = cacheKey(spaceId, reference.digest)
          if (Option.isNone(existing)) reservations.set(reservationKey, reference)
          yield* drainAllDeletions()
          const staged = yield* storage.stage(transfer.download({
            spaceId,
            clientId,
            membershipIncarnation,
            reference
          })).pipe(Effect.onError(() => Effect.sync(() => reservations.delete(reservationKey))))
          const persist = Effect.gen(function*() {
            if (staged.reference.bytes !== reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: reference.bytes,
                actual: staged.reference.bytes
              })
            }
            if (staged.reference.digest !== reference.digest) {
              return yield* new Attachment.AttachmentDigestMismatch({
                expected: reference.digest,
                actual: staged.reference.digest
              })
            }
            let cacheAlreadyCounted = Option.isNone(existing)
            if (
              Option.isSome(existing) && existing.value.cache_managed === 1 && existing.value.remote_available === 1
            ) cacheAlreadyCounted = true
            yield* evictCache(reference, true, true, cacheAlreadyCounted)
            yield* drainAllDeletions()
            const now = yield* Clock.currentTimeMillis
            yield* sql.withTransaction(Effect.gen(function*() {
              if (Option.isSome(existing)) {
                yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
                (object_key, next_attempt_at, created_at)
                VALUES (${existing.value.object_key}, ${now}, ${now})`
                yield* sql`UPDATE effect_local_client_attachments SET object_key = ${staged.key},
                remote_available = 1, cache_managed = 1, last_accessed_at = ${now}
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
              } else {
                yield* sql`INSERT INTO effect_local_client_attachments
                (space_id, digest, bytes, object_key, remote_available, cache_managed, created_at, last_accessed_at)
                VALUES (${spaceId}, ${reference.digest}, ${reference.bytes}, ${staged.key}, 1, 1, ${now}, ${now})`
              }
            })).pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
            )
            return staged.key
          })
          return yield* persist.pipe(
            Effect.onError(() =>
              storage.remove(staged.key).pipe(
                Effect.tapError((error) =>
                  Effect.logWarning("Unused attachment cache file could not be removed").pipe(
                    Effect.annotateLogs("error", error._tag)
                  )
                ),
                Effect.ignore
              )
            ),
            Effect.ensuring(Effect.sync(() => reservations.delete(reservationKey)))
          )
        },
        Effect.withSpan("AttachmentClient.cacheRemote", (spaceId, _clientId, _membershipIncarnation, reference) => ({
          attributes: {
            "space.id": spaceId,
            "attachment.digest": reference.digest
          }
        }))
      )

      const cacheRemote = (
        spaceId: Identity.SpaceId,
        clientId: Identity.ClientId,
        membershipIncarnation: Identity.MembershipIncarnation,
        reference: Attachment.Reference
      ) => {
        const activeKey = cacheKey(spaceId, reference.digest)
        const downloaded = downloadRemote(spaceId, clientId, membershipIncarnation, reference).pipe(
          Effect.tap(() => Effect.sync(() => activeReads.set(activeKey, (activeReads.get(activeKey) ?? 0) + 1)))
        )
        return cacheGate.withPermit(withLock(spaceId, reference.digest, downloaded))
      }

      const read: Service["read"] = (spaceId, clientId, membershipIncarnation, reference, range) =>
        Stream.unwrap(
          Effect.acquireRelease(
            cacheRemote(spaceId, clientId, membershipIncarnation, reference),
            () => {
              const activeKey = cacheKey(spaceId, reference.digest)
              return Effect.sync(() => {
                const count = activeReads.get(activeKey)
                if (count === undefined || count === 1) activeReads.delete(activeKey)
                else activeReads.set(activeKey, count - 1)
              })
            }
          ).pipe(
            Effect.map((key) => storage.read(key, reference, range)),
            Effect.withSpan("AttachmentClient.prepareRead", {
              attributes: {
                "space.id": spaceId,
                "attachment.digest": reference.digest
              }
            })
          )
        ).pipe(
          Stream.scoped,
          Stream.withSpan("AttachmentClient.read", {
            attributes: {
              "space.id": spaceId,
              "attachment.digest": reference.digest
            }
          })
        )

      const markRemoteAvailable: Service["markRemoteAvailable"] = Effect.fnUntraced(
        function*(spaceId, reference) {
          yield* objectKey(spaceId, reference)
          const now = yield* Clock.currentTimeMillis
          yield* sql`UPDATE effect_local_client_attachments
        SET remote_available = 1, cache_managed = 1, last_accessed_at = ${now}
        WHERE space_id = ${spaceId} AND digest = ${reference.digest}`.pipe(
            Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
          )
        },
        Effect.withSpan("AttachmentClient.markRemoteAvailable", (spaceId, reference) => ({
          attributes: {
            "space.id": spaceId,
            "attachment.digest": reference.digest
          }
        }))
      )

      const ensureUploaded: Service["ensureUploaded"] = Effect.fnUntraced(
        function*(
          spaceId,
          clientId,
          membershipIncarnation,
          references
        ) {
          for (const reference of references) {
            yield* withLock(
              spaceId,
              reference.digest,
              Effect.gen(function*() {
                const found = yield* find(spaceId, reference.digest)
                if (Option.isNone(found)) {
                  return yield* new Attachment.AttachmentNotFound({ key: reference.digest })
                }
                if (found.value.bytes !== reference.bytes) {
                  return yield* new Attachment.AttachmentLengthMismatch({
                    expected: found.value.bytes,
                    actual: reference.bytes
                  })
                }
                yield* transfer.upload({
                  spaceId,
                  clientId,
                  membershipIncarnation,
                  reference,
                  bytes: (offset) => {
                    if (offset === 0) return storage.read(found.value.object_key, reference)
                    return storage.read(found.value.object_key, reference, {
                      offset,
                      length: reference.bytes - offset
                    })
                  }
                }).pipe(Effect.withSpan("AttachmentClient.uploadReference", {
                  attributes: {
                    "space.id": spaceId,
                    "attachment.digest": reference.digest
                  }
                }))
                const now = yield* Clock.currentTimeMillis
                yield* sql`UPDATE effect_local_client_attachments
              SET remote_available = 1, cache_managed = 1, last_accessed_at = ${now}
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}`.pipe(
                  Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
                )
                return yield* Effect.void
              })
            )
          }
        },
        Effect.withSpan("AttachmentClient.ensureUploaded", (spaceId) => ({
          attributes: { "space.id": spaceId }
        }))
      )

      return AttachmentClient.of({
        stage,
        associatePending,
        release,
        objectKey,
        read,
        markRemoteAvailable,
        ensureUploaded,
        maintain,
        drainDeletions
      })
    })
  )
