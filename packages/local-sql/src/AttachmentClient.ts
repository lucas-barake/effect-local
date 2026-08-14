import * as Attachment from "@lucas-barake/effect-local/Attachment"
import type * as AttachmentProtocol from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
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
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AttachmentStorage from "./AttachmentStorage.js"
import * as AttachmentTransfer from "./AttachmentTransfer.js"
import * as Configuration from "./internal/configuration.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

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
const CacheCandidate = Schema.Struct({
  space_id: Identity.SpaceId,
  digest: Attachment.Digest,
  bytes: Attachment.ByteLength,
  object_key: AttachmentStorage.ObjectKey,
  last_accessed_at: Schema.Number
})
const CacheCandidateRequest = Schema.Struct({
  before: Schema.Number,
  now: Schema.Number,
  maximum: Schema.Number
})
const GeneratedChunkClaim = Schema.Struct({
  object_key: AttachmentStorage.ObjectKey,
  claim_token: Schema.String
})
const ChunkLookup = Schema.Struct({
  spaceId: Identity.SpaceId,
  digest: Attachment.Digest,
  offset: Attachment.ByteLength
})
const CountRow = Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })
const ObjectKeyRow = Schema.Struct({ object_key: AttachmentStorage.ObjectKey })

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
      const protectedKeys = new Set<string>()
      let activeReadsChanged = yield* Deferred.make<void>()
      const usage = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.ClientAttachmentUsageRow,
        execute: () =>
          sql`SELECT local_object_count, local_byte_count, cache_object_count, cache_byte_count
          FROM effect_local_client_attachment_usage WHERE id = 1`
      })
      const cacheCandidates = SqlSchema.findAll({
        Request: CacheCandidateRequest,
        Result: CacheCandidate,
        execute: ({ before, maximum, now }) =>
          sql`SELECT a.space_id, a.digest, a.bytes, a.object_key, a.last_accessed_at
          FROM effect_local_client_attachments AS a
          WHERE a.cache_managed = 1 AND a.remote_available = 1 AND a.last_accessed_at <= ${before}
            AND NOT EXISTS (
              SELECT 1 FROM effect_local_client_attachment_read_claims AS r
              WHERE r.object_key = a.object_key AND r.expires_at > ${now}
            )
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
          sql`SELECT space_id, digest, bytes, object_version, object_key, remote_available, cache_managed,
            active_reads, created_at, last_accessed_at
          FROM effect_local_client_attachments WHERE space_id = ${spaceId} AND digest = ${digest}`
      })
      const generateChunkClaim = SqlSchema.findOne({
        Request: Schema.Void,
        Result: GeneratedChunkClaim,
        execute: () =>
          sql`SELECT lower(hex(randomblob(16))) AS object_key,
          lower(hex(randomblob(16))) AS claim_token`
      })
      const findVerifiedChunk = SqlSchema.findOneOption({
        Request: ChunkLookup,
        Result: Rows.ClientAttachmentChunkRow,
        execute: ({ digest, offset, spaceId }) =>
          sql`SELECT space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
            chunk_digest, object_key, state, claim_token, claim_expires_at, promotion_token, active_reads,
            created_at, last_accessed_at
          FROM effect_local_client_attachment_chunks
          WHERE space_id = ${spaceId} AND digest = ${digest} AND state = 'Verified'
            AND chunk_offset <= ${offset} AND chunk_offset + chunk_bytes > ${offset}
          ORDER BY last_accessed_at DESC, object_version, chunk_index LIMIT 1`
      })
      const findDeletions = SqlSchema.findAll({
        Request: Schema.Struct({ now: Schema.Number, maximum: Schema.Number }),
        Result: Rows.ClientAttachmentDeletionRow,
        execute: ({ maximum, now }) =>
          sql`SELECT object_key, bytes, cache_managed, attempt_count, next_attempt_at, created_at
          FROM effect_local_client_attachment_deletions
          WHERE next_attempt_at <= ${now} AND NOT EXISTS (
            SELECT 1 FROM effect_local_client_attachment_read_claims AS r
            WHERE r.object_key = effect_local_client_attachment_deletions.object_key
              AND r.expires_at > ${now}
          ) ORDER BY next_attempt_at, object_key LIMIT ${maximum}`
      })
      const findChunkDeletions = SqlSchema.findAll({
        Request: Schema.Struct({ now: Schema.Number, maximum: Schema.Number }),
        Result: Rows.ClientAttachmentChunkRow,
        execute: ({ maximum, now }) =>
          sql`SELECT space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
            chunk_digest, object_key, state, claim_token, claim_expires_at, promotion_token, active_reads,
            created_at, last_accessed_at
          FROM effect_local_client_attachment_chunks
          WHERE state = 'Deleting' AND NOT EXISTS (
            SELECT 1 FROM effect_local_client_attachment_read_claims AS r
            WHERE r.object_key = effect_local_client_attachment_chunks.object_key
              AND r.expires_at > ${now}
          )
          ORDER BY last_accessed_at, space_id, digest, object_version, chunk_index LIMIT ${maximum}`
      })
      const findPromotionDeletions = SqlSchema.findAll({
        Request: Schema.Struct({ maximum: Schema.Number }),
        Result: Rows.ClientAttachmentPromotionRow,
        execute: ({ maximum }) =>
          sql`SELECT space_id, digest, object_version, bytes, object_key, state,
            claim_token, claim_expires_at, created_at, last_accessed_at
          FROM effect_local_client_attachment_promotions
          WHERE state = 'Deleting'
          ORDER BY last_accessed_at, space_id, digest, object_version LIMIT ${maximum}`
      })
      const find = (spaceId: Identity.SpaceId, digest: Attachment.Digest) =>
        findAttachment({ spaceId, digest }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
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

      const renewReadClaim = (
        claimToken: string,
        digest: Attachment.Digest
      ): Effect.Effect<never, ClientFailure> =>
        Effect.sleep("10 seconds").pipe(
          Effect.andThen(Clock.currentTimeMillis),
          Effect.flatMap((now) =>
            SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: ObjectKeyRow,
              execute: () =>
                sql`UPDATE effect_local_client_attachment_read_claims
                SET expires_at = ${now + 30_000}
                WHERE claim_token = ${claimToken}
                RETURNING object_key`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Client attachment read claim renewal is corrupt",
                      cause
                    })
                  )
              }),
              Effect.flatMap((renewed) => {
                if (Option.isNone(renewed)) {
                  return Effect.fail(new Attachment.AttachmentUnavailable({ digest }))
                }
                return Effect.suspend(() => renewReadClaim(claimToken, digest))
              })
            )
          )
        )

      const drainDeletions: Service["drainDeletions"] = Effect.fnUntraced(function*(maximum) {
        const batchSize = yield* Configuration.positiveSafeInteger("attachments.deletionBatchSize", maximum)
        const now = yield* Clock.currentTimeMillis
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`DELETE FROM effect_local_client_attachment_read_claims WHERE expires_at <= ${now}`
          yield* sql`UPDATE effect_local_client_attachments SET active_reads = (
            SELECT COUNT(*) FROM effect_local_client_attachment_read_claims AS r
            WHERE r.object_key = effect_local_client_attachments.object_key
          )`
          yield* sql`UPDATE effect_local_client_attachment_chunks SET active_reads = (
            SELECT COUNT(*) FROM effect_local_client_attachment_read_claims AS r
            WHERE r.object_key = effect_local_client_attachment_chunks.object_key
          )`
          yield* sql`UPDATE effect_local_client_attachment_chunks SET promotion_token = NULL
            WHERE promotion_token IN (
              SELECT claim_token FROM effect_local_client_attachment_promotions
              WHERE state = 'Filling' AND claim_expires_at <= ${now}
            )`
          yield* sql`UPDATE effect_local_client_attachment_promotions SET
            state = 'Deleting', claim_token = NULL, claim_expires_at = NULL
            WHERE state = 'Filling' AND claim_expires_at <= ${now}`
          yield* sql`UPDATE effect_local_client_attachment_chunks SET
            state = 'Deleting', claim_token = NULL, claim_expires_at = NULL
            WHERE state = 'Filling' AND claim_expires_at <= ${now}`
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        const pending = yield* findDeletions({ now, maximum: batchSize }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
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
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
        }
        const chunkDeletions = yield* findChunkDeletions({ now, maximum: batchSize }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment chunk deletion state is corrupt",
                  cause
                })
              )
          })
        )
        for (const deletion of chunkDeletions) {
          yield* storage.remove(deletion.object_key).pipe(Effect.withSpan("AttachmentClient.removeChunk"))
          yield* sql`DELETE FROM effect_local_client_attachment_chunks
            WHERE space_id = ${deletion.space_id} AND digest = ${deletion.digest}
              AND object_version = ${deletion.object_version} AND chunk_index = ${deletion.chunk_index}
              AND state = 'Deleting' AND NOT EXISTS (
                SELECT 1 FROM effect_local_client_attachment_read_claims AS r
                WHERE r.object_key = effect_local_client_attachment_chunks.object_key
                  AND r.expires_at > ${now}
              )`.pipe(
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
        }
        const promotionDeletions = yield* findPromotionDeletions({ maximum: batchSize }).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion deletion state is corrupt",
                  cause
                })
              )
          })
        )
        for (const deletion of promotionDeletions) {
          yield* storage.remove(deletion.object_key).pipe(Effect.withSpan("AttachmentClient.removePromotion"))
          yield* sql`DELETE FROM effect_local_client_attachment_promotions
            WHERE space_id = ${deletion.space_id} AND digest = ${deletion.digest}
              AND object_version = ${deletion.object_version} AND state = 'Deleting'`.pipe(
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
          )
        }
        if (
          pending.length === batchSize || chunkDeletions.length === batchSize ||
          promotionDeletions.length === batchSize
        ) return batchSize
        return Math.max(pending.length, chunkDeletions.length, promotionDeletions.length)
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
        if (admission !== undefined && !localAlreadyCounted && admission.bytes > maximumLocalBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment storage",
            limit: maximumLocalBytes
          })
        }
        if (
          admission !== undefined && includeAdmissionInCache && !cacheAlreadyCounted &&
          admission.bytes > maximumCacheBytes
        ) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment cache",
            limit: maximumCacheBytes
          })
        }
        const now = yield* Clock.currentTimeMillis
        const current = yield* usage(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment usage is missing",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment usage is corrupt",
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
        let cacheObjectCount = current.cache_object_count + reservations.size + cacheAdmissionCount
        let cacheByteCount = current.cache_byte_count + reservationBytes + cacheAdmissionBytes
        let localObjectCount = current.local_object_count + reservations.size + localAdmissionCount
        let localByteCount = current.local_byte_count + reservationBytes + localAdmissionBytes
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
            now,
            maximum: evictionBatchSize + reservations.size + activeReads.size + 1
          }).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
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
            if (
              reservations.has(candidateKey) || activeReads.has(candidateKey) || protectedKeys.has(candidateKey)
            ) continue
            const deleted = yield* sql.withTransaction(Effect.gen(function*() {
              const removed = yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: CacheCandidate,
                execute: () =>
                  sql`DELETE FROM effect_local_client_attachments
                WHERE space_id = ${candidate.space_id} AND digest = ${candidate.digest}
                  AND remote_available = 1 AND NOT EXISTS (
                    SELECT 1 FROM effect_local_client_attachment_read_claims AS r
                    WHERE r.object_key = effect_local_client_attachments.object_key
                      AND r.expires_at > ${now}
                  ) AND NOT EXISTS (
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
                (object_key, bytes, cache_managed, next_attempt_at, created_at)
                VALUES (${candidate.object_key}, ${candidate.bytes}, 1, ${now}, ${now})`
              }
              return removed
            })).pipe(
              Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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

      const admitRemote = (
        reference: Attachment.Reference,
        localAlreadyCounted: boolean,
        cacheAlreadyCounted: boolean
      ): Effect.Effect<number, ReplicaError.ReplicaError> => {
        if (!localAlreadyCounted && reference.bytes > maximumLocalBytes) {
          return Effect.fail(
            new ReplicaError.CapacityExceeded({
              resource: "client attachment storage",
              limit: maximumLocalBytes
            })
          )
        }
        if (!cacheAlreadyCounted && reference.bytes > maximumCacheBytes) {
          return Effect.fail(
            new ReplicaError.CapacityExceeded({
              resource: "client attachment cache",
              limit: maximumCacheBytes
            })
          )
        }
        return evictCache(reference, localAlreadyCounted, true, cacheAlreadyCounted).pipe(
          Effect.catchTag("CapacityExceeded", (error) => {
            if (activeReads.size === 0) return Effect.fail(error)
            const changed = activeReadsChanged
            return Deferred.await(changed).pipe(
              Effect.andThen(Effect.suspend(() => admitRemote(reference, localAlreadyCounted, cacheAlreadyCounted)))
            )
          })
        )
      }

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
                let retiredCacheManaged: 0 | 1 = 0
                if (Option.isSome(existing)) {
                  const exists = yield* storage.exists(existing.value.object_key)
                  if (exists) keepStaged = false
                  else {
                    retiredKey = existing.value.object_key
                    retiredCacheManaged = existing.value.cache_managed
                  }
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
              (object_key, bytes, cache_managed, next_attempt_at, created_at)
              VALUES (${staged.key}, ${staged.reference.bytes}, 0, ${now}, ${now})`
                  }
                  if (retiredKey !== undefined) {
                    yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_deletions
              (object_key, bytes, cache_managed, next_attempt_at, created_at)
              VALUES (${retiredKey}, ${staged.reference.bytes},
                ${retiredCacheManaged}, ${now}, ${now})`
                  }
                  yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_owners
            (space_id, digest, owner_kind, owner_id, created_at)
            VALUES (${spaceId}, ${staged.reference.digest}, 'Staged', 'staged', ${now})`
                })).pipe(
                  Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
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
              (object_key, bytes, cache_managed, next_attempt_at, created_at)
              SELECT a.object_key, a.bytes, a.cache_managed, ${now}, ${now}
              FROM effect_local_client_attachments AS a
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
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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

      const readVerifiedChunk = (
        row: Rows.ClientAttachmentChunkRow,
        requestedOffset: number,
        requestedEnd: number
      ) => {
        const sliceOffset = Math.max(requestedOffset, row.chunk_offset)
        const sliceEnd = Math.min(requestedEnd, row.chunk_offset + row.chunk_bytes)
        const chunkReference = Attachment.Reference.make({
          digest: row.chunk_digest,
          bytes: row.chunk_bytes
        })
        const acquire = sql.withTransaction(Effect.gen(function*() {
          const generated = yield* generateChunkClaim(undefined).pipe(Effect.catchTags({
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment read token generation returned no row",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment read token generation is corrupt",
                  cause
                })
              )
          }))
          const now = yield* Clock.currentTimeMillis
          const chunk = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: ObjectKeyRow,
            execute: () =>
              sql`UPDATE effect_local_client_attachment_chunks SET
              active_reads = active_reads + 1
              WHERE space_id = ${row.space_id} AND digest = ${row.digest}
                AND object_version = ${row.object_version} AND chunk_index = ${row.chunk_index}
                AND state = 'Verified'
              RETURNING object_key`
          })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client attachment chunk read claim is corrupt",
                cause
              })
            )))
          if (Option.isSome(chunk)) {
            yield* sql`INSERT INTO effect_local_client_attachment_read_claims
              (claim_token, object_key, expires_at, created_at)
              VALUES (${generated.claim_token}, ${chunk.value.object_key}, ${now + 30_000}, ${now})`
            return { kind: "Chunk" as const, key: chunk.value.object_key, token: generated.claim_token }
          }
          const complete = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: ObjectKeyRow,
            execute: () =>
              sql`UPDATE effect_local_client_attachments SET
              active_reads = active_reads + 1
              WHERE space_id = ${row.space_id} AND digest = ${row.digest}
                AND object_key = ${row.object_key}
              RETURNING object_key`
          })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client attachment complete read claim is corrupt",
                cause
              })
            )))
          if (Option.isSome(complete)) {
            yield* sql`INSERT INTO effect_local_client_attachment_read_claims
              (claim_token, object_key, expires_at, created_at)
              VALUES (${generated.claim_token}, ${complete.value.object_key}, ${now + 30_000}, ${now})`
            return { kind: "Complete" as const, key: complete.value.object_key, token: generated.claim_token }
          }
          return yield* new Attachment.AttachmentUnavailable({ digest: row.digest })
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        return Stream.unwrap(
          Effect.acquireRelease(
            acquire,
            (claim) => {
              if (claim.kind === "Chunk") {
                return sql.withTransaction(Effect.gen(function*() {
                  yield* sql`DELETE FROM effect_local_client_attachment_read_claims
                    WHERE claim_token = ${claim.token}`
                  yield* sql`UPDATE effect_local_client_attachment_chunks SET active_reads = active_reads - 1
                WHERE space_id = ${row.space_id} AND digest = ${row.digest}
                  AND object_version = ${row.object_version} AND chunk_index = ${row.chunk_index}
                    AND active_reads > 0`
                })).pipe(Effect.catchTag("SqlError", () => Effect.void))
              }
              return sql.withTransaction(Effect.gen(function*() {
                yield* sql`DELETE FROM effect_local_client_attachment_read_claims
                  WHERE claim_token = ${claim.token}`
                yield* sql`UPDATE effect_local_client_attachments SET active_reads = active_reads - 1
              WHERE space_id = ${row.space_id} AND digest = ${row.digest}
                  AND object_key = ${row.object_key} AND active_reads > 0`
              })).pipe(Effect.catchTag("SqlError", () => Effect.void))
            }
          ).pipe(Effect.map((claim) => {
            const renewal = Stream.fromEffect(renewReadClaim(claim.token, row.digest))
            return storage.read(claim.key, chunkReference, {
              offset: sliceOffset - row.chunk_offset,
              length: sliceEnd - sliceOffset
            }).pipe(
              Stream.merge(renewal, { haltStrategy: "left" })
            )
          }))
        ).pipe(Stream.scoped)
      }

      const evictOneVerifiedChunk = Effect.fnUntraced(function*() {
        const candidate = yield* sql.withTransaction(
          SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: Rows.ClientAttachmentChunkRow,
            execute: () =>
              sql`UPDATE effect_local_client_attachment_chunks SET state = 'Deleting'
                WHERE (space_id, digest, object_version, chunk_index) = (
                  SELECT space_id, digest, object_version, chunk_index
                  FROM effect_local_client_attachment_chunks
                  WHERE state = 'Verified' AND active_reads = 0 AND promotion_token IS NULL
                  ORDER BY last_accessed_at, space_id, digest, object_version, chunk_index LIMIT 1
                ) AND state = 'Verified' AND active_reads = 0 AND promotion_token IS NULL
                RETURNING space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
                  chunk_digest, object_key, state, claim_token, claim_expires_at, promotion_token, active_reads,
                  created_at, last_accessed_at`
          })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client attachment chunk eviction state is corrupt",
                cause
              })
            )))
        ).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (Option.isNone(candidate)) return false
        yield* storage.remove(candidate.value.object_key)
        yield* sql`DELETE FROM effect_local_client_attachment_chunks
          WHERE space_id = ${candidate.value.space_id} AND digest = ${candidate.value.digest}
            AND object_version = ${candidate.value.object_version}
            AND chunk_index = ${candidate.value.chunk_index} AND state = 'Deleting'`.pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )
        return true
      })

      const admitDownloadedChunk = (
        chunk: AttachmentProtocol.VerifiedChunk
      ): Effect.Effect<void, ClientFailure> => {
        const reference = Attachment.Reference.make({ digest: chunk.digest, bytes: chunk.bytes })
        return cacheGate.withPermit(evictCache(
          reference,
          false,
          true,
          false
        )).pipe(
          Effect.catchTag("CapacityExceeded", (error) =>
            evictOneVerifiedChunk().pipe(
              Effect.flatMap((evicted) => {
                if (evicted) return admitDownloadedChunk(chunk)
                return Effect.fail(error)
              })
            )),
          Effect.asVoid
        )
      }

      const promoteVerifiedChunks = Effect.fnUntraced(function*(
        spaceId: Identity.SpaceId,
        reference: Attachment.Reference,
        objectVersion: AttachmentProtocol.ObjectVersion
      ) {
        const chunks = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Rows.ClientAttachmentChunkRow,
          execute: () =>
            sql`SELECT space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
              chunk_digest, object_key, state, claim_token, claim_expires_at, promotion_token, active_reads,
              created_at, last_accessed_at
            FROM effect_local_client_attachment_chunks
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${objectVersion} AND state = 'Verified'
              AND promotion_token IS NULL AND active_reads = 0
            ORDER BY chunk_offset, chunk_index`
        })(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion sources are corrupt",
                  cause
                })
              )
          })
        )
        if (chunks.length < 2) return
        let covered = 0
        for (const chunk of chunks) {
          if (chunk.chunk_offset !== covered) return
          covered += chunk.chunk_bytes
        }
        if (covered !== reference.bytes) return
        const now = yield* Clock.currentTimeMillis
        const generated = yield* generateChunkClaim(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion claim generation returned no row",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion claim generation is corrupt",
                  cause
                })
              )
          })
        )
        const claimExpiresAt = now + 30_000
        const reserved = yield* sql.withTransaction(Effect.gen(function*() {
          const currentAttachment = yield* find(spaceId, reference.digest)
          if (Option.isSome(currentAttachment)) return false
          const currentUsage = yield* usage(undefined).pipe(
            Effect.catchTags({
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment usage is missing",
                    cause
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment usage is corrupt",
                    cause
                  })
                )
            })
          )
          if (
            currentUsage.local_object_count + 1 > maximumLocalObjects ||
            currentUsage.local_byte_count + reference.bytes > maximumLocalBytes ||
            currentUsage.cache_object_count + 1 > maximumCacheObjects ||
            currentUsage.cache_byte_count + reference.bytes > maximumCacheBytes
          ) return false
          yield* sql`INSERT OR IGNORE INTO effect_local_client_attachment_promotions
            (space_id, digest, object_version, bytes, object_key, state, claim_token,
              claim_expires_at, created_at, last_accessed_at)
            VALUES (${spaceId}, ${reference.digest}, ${objectVersion}, ${reference.bytes},
              ${generated.object_key}, 'Filling', ${generated.claim_token}, ${claimExpiresAt}, ${now}, ${now})`
          const owned = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: Rows.ClientAttachmentPromotionRow,
            execute: () =>
              sql`SELECT space_id, digest, object_version, bytes, object_key, state,
                claim_token, claim_expires_at, created_at, last_accessed_at
              FROM effect_local_client_attachment_promotions
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${objectVersion} AND state = 'Filling'
                AND claim_token = ${generated.claim_token}`
          })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client attachment promotion reservation is corrupt",
                cause
              })
            )))
          if (Option.isNone(owned)) return false
          yield* sql`UPDATE effect_local_client_attachment_chunks SET promotion_token = ${generated.claim_token}
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${objectVersion} AND state = 'Verified'
              AND promotion_token IS NULL AND active_reads = 0`
          const pinned = yield* SqlSchema.findOne({
            Request: Schema.Void,
            Result: CountRow,
            execute: () =>
              sql`SELECT COUNT(*) AS count FROM effect_local_client_attachment_chunks
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${objectVersion} AND state = 'Verified'
                AND promotion_token = ${generated.claim_token}`
          })(undefined).pipe(Effect.catchTags({
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion pin count is missing",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment promotion pin count is corrupt",
                  cause
                })
              )
          }))
          if (pinned.count === chunks.length) return true
          yield* sql`UPDATE effect_local_client_attachment_chunks SET promotion_token = NULL
            WHERE promotion_token = ${generated.claim_token}`
          yield* sql`DELETE FROM effect_local_client_attachment_promotions
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${objectVersion} AND claim_token = ${generated.claim_token}`
          return false
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (!reserved) return
        const failPromotion = sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_client_attachment_chunks SET promotion_token = NULL
            WHERE promotion_token = ${generated.claim_token}`
          yield* sql`UPDATE effect_local_client_attachment_promotions SET
            state = 'Deleting', claim_token = NULL, claim_expires_at = NULL, last_accessed_at = ${now}
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${objectVersion} AND state = 'Filling'
              AND claim_token = ${generated.claim_token}`
        })).pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.ignore
        )
        const rejectPromotion = sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_client_attachment_chunks SET
            state = 'Deleting', promotion_token = NULL
            WHERE promotion_token = ${generated.claim_token}`
          yield* sql`UPDATE effect_local_client_attachment_promotions SET
            state = 'Deleting', claim_token = NULL, claim_expires_at = NULL, last_accessed_at = ${now}
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${objectVersion} AND state = 'Filling'
              AND claim_token = ${generated.claim_token}`
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        const promoteBytes = Effect.gen(function*() {
          yield* storage.remove(generated.object_key)
          yield* storage.create(generated.object_key)
          for (const chunk of chunks) {
            const chunkReference = Attachment.Reference.make({
              digest: chunk.chunk_digest,
              bytes: chunk.chunk_bytes
            })
            yield* storage.append(
              generated.object_key,
              reference,
              chunk.chunk_offset,
              storage.read(chunk.object_key, chunkReference)
            )
          }
          yield* storage.verify(generated.object_key, reference)
          yield* sql.withTransaction(Effect.gen(function*() {
            const owned = yield* SqlSchema.findOne({
              Request: Schema.Void,
              Result: CountRow,
              execute: () =>
                sql`SELECT COUNT(*) AS count
                FROM effect_local_client_attachment_promotions
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                  AND object_version = ${objectVersion} AND state = 'Filling'
                  AND claim_token = ${generated.claim_token}`
            })(undefined).pipe(Effect.catchTags({
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment promotion ownership is missing",
                    cause
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment promotion ownership is corrupt",
                    cause
                  })
                )
            }))
            if (owned.count !== 1) {
              return yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
            }
            const pinned = yield* SqlSchema.findOne({
              Request: Schema.Void,
              Result: CountRow,
              execute: () =>
                sql`SELECT COUNT(*) AS count
                FROM effect_local_client_attachment_chunks
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                  AND object_version = ${objectVersion} AND state = 'Verified'
                  AND promotion_token = ${generated.claim_token}`
            })(undefined).pipe(Effect.catchTags({
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment promotion source count is missing",
                    cause
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment promotion source count is corrupt",
                    cause
                  })
                )
            }))
            if (pinned.count !== chunks.length) {
              return yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
            }
            yield* sql`INSERT INTO effect_local_client_attachments
              (space_id, digest, bytes, object_version, object_key, remote_available, cache_managed,
                created_at, last_accessed_at)
              VALUES (${spaceId}, ${reference.digest}, ${reference.bytes}, ${objectVersion},
                ${generated.object_key}, 1, 1, ${now}, ${now})`
            yield* sql`DELETE FROM effect_local_client_attachment_promotions
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${objectVersion} AND claim_token = ${generated.claim_token}`
            yield* sql`UPDATE effect_local_client_attachment_chunks SET
              state = 'Deleting', promotion_token = NULL
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${objectVersion} AND state = 'Verified'
                AND promotion_token = ${generated.claim_token}`
            return undefined
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        })
        const renewPromotion: Effect.Effect<never, ClientFailure> = Effect.sleep("10 seconds").pipe(
          Effect.andThen(Clock.currentTimeMillis),
          Effect.flatMap((renewedAt) =>
            SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: ObjectKeyRow,
              execute: () =>
                sql`UPDATE effect_local_client_attachment_promotions SET
                claim_expires_at = ${renewedAt + 30_000}, last_accessed_at = ${renewedAt}
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                  AND object_version = ${objectVersion} AND state = 'Filling'
                  AND claim_token = ${generated.claim_token}
                RETURNING object_key`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Client attachment promotion renewal is corrupt",
                      cause
                    })
                  )
              }),
              Effect.flatMap((renewed) => {
                if (Option.isSome(renewed)) return Effect.suspend(() => renewPromotion)
                return Effect.fail(new Attachment.AttachmentUnavailable({ digest: reference.digest }))
              })
            )
          )
        )
        const promote = Effect.raceFirst(promoteBytes, renewPromotion)
        yield* promote.pipe(
          Effect.catchTag("AttachmentDigestMismatch", (error) =>
            rejectPromotion.pipe(
              Effect.andThen(drainInBackground),
              Effect.andThen(Effect.fail(error))
            )),
          Effect.onError(() => failPromotion)
        )
        yield* drainInBackground
      })

      const persistDownloadedChunk = Effect.fnUntraced(function*(
        spaceId: Identity.SpaceId,
        reference: Attachment.Reference,
        response: AttachmentTransfer.DownloadResponse
      ) {
        const chunk = response.chunk
        if (response.objectBytes !== reference.bytes) {
          return yield* new Attachment.AttachmentLengthMismatch({
            expected: reference.bytes,
            actual: response.objectBytes
          })
        }
        if (chunk.offset + chunk.bytes > reference.bytes) {
          return yield* new Attachment.InvalidAttachmentRange({
            bytes: reference.bytes,
            offset: chunk.offset,
            length: chunk.bytes
          })
        }
        yield* admitDownloadedChunk(chunk)
        yield* drainAllDeletions()
        const now = yield* Clock.currentTimeMillis
        const generated = yield* generateChunkClaim(undefined).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
            NoSuchElementError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment chunk claim generation returned no row",
                  cause
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment chunk claim generation is corrupt",
                  cause
                })
              )
          })
        )
        const claimExpiresAt = now + 30_000
        const claimed = yield* sql.withTransaction(Effect.gen(function*() {
          const currentUsage = yield* usage(undefined).pipe(
            Effect.catchTags({
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment usage is missing",
                    cause
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment usage is corrupt",
                    cause
                  })
                )
            })
          )
          const existing = yield* SqlSchema.findOneOption({
            Request: Schema.Void,
            Result: Rows.ClientAttachmentChunkRow,
            execute: () =>
              sql`SELECT space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
              chunk_digest, object_key, state, claim_token, claim_expires_at, promotion_token, active_reads,
              created_at, last_accessed_at
            FROM effect_local_client_attachment_chunks
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}`
          })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client attachment chunk state is corrupt",
                cause
              })
            )))
          if (Option.isSome(existing)) {
            const row = existing.value
            if (
              row.chunk_offset !== chunk.offset || row.chunk_bytes !== chunk.bytes ||
              row.chunk_digest !== chunk.digest
            ) {
              return yield* new ReplicaError.StorageCorrupt({
                message: "Client attachment chunk identity conflicts with its object version",
                cause: row
              })
            }
            if (row.state === "Verified") return row
            if (row.state === "Filling" && row.claim_expires_at !== null && row.claim_expires_at > now) {
              return undefined
            }
            yield* sql`UPDATE effect_local_client_attachment_chunks SET
              state = 'Filling', claim_token = ${generated.claim_token},
              claim_expires_at = ${claimExpiresAt}, last_accessed_at = ${now}
            WHERE space_id = ${spaceId} AND digest = ${reference.digest}
              AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}
              AND (state = 'Deleting' OR claim_expires_at <= ${now})`
            return {
              ...row,
              state: "Filling" as const,
              claim_token: generated.claim_token,
              claim_expires_at: claimExpiresAt,
              promotion_token: row.promotion_token,
              last_accessed_at: now
            }
          }
          if (
            currentUsage.local_object_count + 1 > maximumLocalObjects ||
            currentUsage.local_byte_count + chunk.bytes > maximumLocalBytes
          ) {
            let limit = maximumLocalObjects
            if (currentUsage.local_byte_count + chunk.bytes > maximumLocalBytes) limit = maximumLocalBytes
            return yield* new ReplicaError.CapacityExceeded({ resource: "client attachment storage", limit })
          }
          if (
            currentUsage.cache_object_count + 1 > maximumCacheObjects ||
            currentUsage.cache_byte_count + chunk.bytes > maximumCacheBytes
          ) {
            let limit = maximumCacheObjects
            if (currentUsage.cache_byte_count + chunk.bytes > maximumCacheBytes) limit = maximumCacheBytes
            return yield* new ReplicaError.CapacityExceeded({ resource: "client attachment cache", limit })
          }
          yield* sql`INSERT INTO effect_local_client_attachment_chunks
            (space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes,
              chunk_digest, object_key, state, claim_token, claim_expires_at, created_at, last_accessed_at)
            VALUES (${spaceId}, ${reference.digest}, ${response.objectVersion}, ${chunk.index},
              ${chunk.offset}, ${chunk.bytes}, ${chunk.digest}, ${generated.object_key},
              'Filling', ${generated.claim_token}, ${claimExpiresAt}, ${now}, ${now})`
          return Rows.ClientAttachmentChunkRow.make({
            space_id: spaceId,
            digest: reference.digest,
            object_version: response.objectVersion,
            chunk_index: chunk.index,
            chunk_offset: chunk.offset,
            chunk_bytes: chunk.bytes,
            chunk_digest: chunk.digest,
            object_key: generated.object_key,
            state: "Filling",
            claim_token: generated.claim_token,
            claim_expires_at: claimExpiresAt,
            promotion_token: null,
            active_reads: 0,
            created_at: now,
            last_accessed_at: now
          })
        })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        if (claimed === undefined || claimed.state === "Verified") return claimed
        const chunkReference = Attachment.Reference.make({ digest: chunk.digest, bytes: chunk.bytes })
        const fillBytes = Effect.gen(function*() {
          yield* storage.remove(claimed.object_key)
          yield* storage.create(claimed.object_key)
          yield* storage.append(claimed.object_key, chunkReference, 0, response.bytes)
          yield* storage.verify(claimed.object_key, chunkReference)
          yield* sql.withTransaction(Effect.gen(function*() {
            const finalized = yield* SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: ObjectKeyRow,
              execute: () =>
                sql`UPDATE effect_local_client_attachment_chunks SET
                state = 'Verified', claim_token = NULL, claim_expires_at = NULL, last_accessed_at = ${now}
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}
                AND state = 'Filling' AND claim_token = ${generated.claim_token}
              RETURNING object_key`
            })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client attachment chunk finalization is corrupt",
                  cause
                })
              )))
            if (Option.isNone(finalized)) {
              return yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
            }
            if (chunk.offset === 0 && chunk.bytes === reference.bytes && chunk.digest === reference.digest) {
              yield* sql`INSERT OR IGNORE INTO effect_local_client_attachments
                (space_id, digest, bytes, object_version, object_key, remote_available, cache_managed,
                  created_at, last_accessed_at)
                VALUES (${spaceId}, ${reference.digest}, ${reference.bytes}, ${response.objectVersion},
                  ${claimed.object_key}, 1, 1, ${now}, ${now})`
              yield* sql`DELETE FROM effect_local_client_attachment_chunks
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                  AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}
                  AND state = 'Verified'`
            }
            return undefined
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
          return { ...claimed, state: "Verified" as const, claim_token: null, claim_expires_at: null }
        })
        const renewClaim: Effect.Effect<never, ClientFailure> = Effect.sleep("10 seconds").pipe(
          Effect.andThen(Clock.currentTimeMillis),
          Effect.flatMap((renewedAt) =>
            SqlSchema.findOneOption({
              Request: Schema.Void,
              Result: ObjectKeyRow,
              execute: () =>
                sql`UPDATE effect_local_client_attachment_chunks
                SET claim_expires_at = ${renewedAt + 30_000}, last_accessed_at = ${renewedAt}
                WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                  AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}
                  AND state = 'Filling' AND claim_token = ${generated.claim_token}
                RETURNING object_key`
            })(undefined).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Client attachment chunk renewal is corrupt",
                      cause
                    })
                  )
              }),
              Effect.flatMap((renewed) => {
                if (Option.isSome(renewed)) return Effect.suspend(() => renewClaim)
                return Effect.fail(new Attachment.AttachmentUnavailable({ digest: reference.digest }))
              })
            )
          )
        )
        const fill = Effect.raceFirst(fillBytes, renewClaim)
        const verified = yield* fill.pipe(Effect.onError(() =>
          storage.remove(claimed.object_key).pipe(
            Effect.andThen(sql`DELETE FROM effect_local_client_attachment_chunks
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                AND object_version = ${response.objectVersion} AND chunk_index = ${chunk.index}
                AND state = 'Filling' AND claim_token = ${generated.claim_token}`),
            Effect.catchTags({
              AttachmentStorageError: () => Effect.void,
              SqlError: () => Effect.void
            })
          )
        ))
        const promotion = yield* promoteVerifiedChunks(spaceId, reference, response.objectVersion).pipe(Effect.result)
        if (Result.isFailure(promotion)) {
          if (promotion.failure._tag === "AttachmentDigestMismatch") return yield* promotion.failure
          yield* Effect.logWarning("Attachment chunk promotion will be retried").pipe(
            Effect.annotateLogs("error", promotion.failure._tag)
          )
        }
        const promoted = yield* find(spaceId, reference.digest)
        if (
          Option.isSome(promoted) && promoted.value.object_version === response.objectVersion &&
          (yield* storage.exists(promoted.value.object_key))
        ) {
          return Rows.ClientAttachmentChunkRow.make({
            space_id: spaceId,
            digest: reference.digest,
            object_version: response.objectVersion,
            chunk_index: 0,
            chunk_offset: 0,
            chunk_bytes: reference.bytes,
            chunk_digest: reference.digest,
            object_key: promoted.value.object_key,
            state: "Verified",
            claim_token: null,
            claim_expires_at: null,
            promotion_token: null,
            active_reads: 0,
            created_at: promoted.value.created_at,
            last_accessed_at: promoted.value.last_accessed_at
          })
        }
        return verified
      })

      const ensureVerifiedChunk = Effect.fnUntraced(function*(
        spaceId: Identity.SpaceId,
        clientId: Identity.ClientId,
        membershipIncarnation: Identity.MembershipIncarnation,
        reference: Attachment.Reference,
        offset: number,
        end: number
      ): Effect.fn.Return<Rows.ClientAttachmentChunkRow, ClientFailure> {
        const requestedBytes = end - offset
        if (requestedBytes > maximumLocalBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment storage",
            limit: maximumLocalBytes
          })
        }
        if (requestedBytes > maximumCacheBytes) {
          return yield* new ReplicaError.CapacityExceeded({
            resource: "client attachment cache",
            limit: maximumCacheBytes
          })
        }
        for (let attempt = 0; attempt < 128; attempt++) {
          const existing = yield* findVerifiedChunk({ spaceId, digest: reference.digest, offset }).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment verified chunk is corrupt",
                    cause
                  })
                )
            })
          )
          if (Option.isSome(existing)) return existing.value
          const promoted = yield* find(spaceId, reference.digest)
          if (
            Option.isSome(promoted) && promoted.value.object_version !== null &&
            promoted.value.bytes === reference.bytes && (yield* storage.exists(promoted.value.object_key))
          ) {
            return Rows.ClientAttachmentChunkRow.make({
              space_id: spaceId,
              digest: reference.digest,
              object_version: promoted.value.object_version,
              chunk_index: 0,
              chunk_offset: 0,
              chunk_bytes: reference.bytes,
              chunk_digest: reference.digest,
              object_key: promoted.value.object_key,
              state: "Verified",
              claim_token: null,
              claim_expires_at: null,
              promotion_token: null,
              active_reads: 0,
              created_at: promoted.value.created_at,
              last_accessed_at: promoted.value.last_accessed_at
            })
          }
          const response = yield* transfer.download({
            spaceId,
            clientId,
            membershipIncarnation,
            reference,
            range: { offset, length: end - offset }
          })
          if (response.chunk.offset > offset || response.chunk.offset + response.chunk.bytes <= offset) {
            return yield* new Attachment.InvalidAttachmentRange({
              bytes: reference.bytes,
              offset: response.chunk.offset,
              length: response.chunk.bytes
            })
          }
          const persisted = yield* persistDownloadedChunk(spaceId, reference, response)
          if (persisted !== undefined) return persisted
          yield* Effect.sleep("25 millis")
        }
        return yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
      })

      const validateReadRange = (reference: Attachment.Reference, range?: Attachment.Range) => {
        const offset = range?.offset ?? 0
        const length = range?.length ?? reference.bytes - offset
        if (
          !Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length) || length <= 0 ||
          offset >= reference.bytes || offset + length > reference.bytes
        ) {
          return Effect.fail(
            new Attachment.InvalidAttachmentRange({
              bytes: reference.bytes,
              offset,
              length
            })
          )
        }
        return Effect.succeed({ offset, end: offset + length })
      }

      const read: Service["read"] = (spaceId, clientId, membershipIncarnation, reference, range) =>
        Stream.unwrap(Effect.gen(function*() {
          if (reference.bytes === 0 && range === undefined) return Stream.empty
          const validated = yield* validateReadRange(reference, range)
          const complete = yield* find(spaceId, reference.digest)
          if (Option.isSome(complete) && complete.value.bytes !== reference.bytes) {
            return yield* new Attachment.AttachmentLengthMismatch({
              expected: complete.value.bytes,
              actual: reference.bytes
            })
          }
          if (Option.isSome(complete) && (yield* storage.exists(complete.value.object_key))) {
            const now = yield* Clock.currentTimeMillis
            const generated = yield* generateChunkClaim(undefined).pipe(Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment read token generation returned no row",
                    cause
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment read token generation is corrupt",
                    cause
                  })
                )
            }))
            const claimed = yield* sql.withTransaction(Effect.gen(function*() {
              const row = yield* SqlSchema.findOneOption({
                Request: Schema.Void,
                Result: ObjectKeyRow,
                execute: () =>
                  sql`UPDATE effect_local_client_attachments SET
                  active_reads = active_reads + 1, last_accessed_at = ${now}
                  WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                RETURNING object_key`
              })(undefined).pipe(Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client attachment read claim is corrupt",
                    cause
                  })
                )))
              if (Option.isSome(row)) {
                yield* sql`INSERT INTO effect_local_client_attachment_read_claims
                  (claim_token, object_key, expires_at, created_at)
                  VALUES (${generated.claim_token}, ${row.value.object_key}, ${now + 30_000}, ${now})`
              }
              return row
            })).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause))
              })
            )
            if (Option.isNone(claimed)) {
              return yield* new Attachment.AttachmentUnavailable({ digest: reference.digest })
            }
            const activeKey = cacheKey(spaceId, reference.digest)
            activeReads.set(activeKey, (activeReads.get(activeKey) ?? 0) + 1)
            const renewal = Stream.fromEffect(renewReadClaim(generated.claim_token, reference.digest))
            return storage.read(claimed.value.object_key, reference, range).pipe(
              Stream.merge(renewal, { haltStrategy: "left" }),
              Stream.ensuring(Effect.gen(function*() {
                yield* sql.withTransaction(Effect.gen(function*() {
                  yield* sql`DELETE FROM effect_local_client_attachment_read_claims
                    WHERE claim_token = ${generated.claim_token}`
                  yield* sql`UPDATE effect_local_client_attachments SET active_reads = active_reads - 1
                    WHERE space_id = ${spaceId} AND digest = ${reference.digest}
                      AND object_key = ${claimed.value.object_key} AND active_reads > 0`
                })).pipe(Effect.catchTag("SqlError", () => Effect.void))
                const count = activeReads.get(activeKey)
                if (count === undefined || count === 1) activeReads.delete(activeKey)
                else activeReads.set(activeKey, count - 1)
                const changed = activeReadsChanged
                activeReadsChanged = yield* Deferred.make<void>()
                yield* Deferred.succeed(changed, undefined)
              }))
            )
          }
          return Stream.unfold(validated.offset, (offset) => {
            if (offset >= validated.end) return Effect.succeed(undefined)
            return ensureVerifiedChunk(
              spaceId,
              clientId,
              membershipIncarnation,
              reference,
              offset,
              validated.end
            ).pipe(
              Effect.flatMap((row) =>
                readVerifiedChunk(row, offset, validated.end).pipe(
                  Stream.runCollect,
                  Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk)))),
                  Effect.map((bytes) =>
                    [
                      bytes,
                      Math.min(validated.end, row.chunk_offset + row.chunk_bytes)
                    ] as const
                  )
                )
              )
            )
          })
        })).pipe(
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
            Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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
                  Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause)))
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
