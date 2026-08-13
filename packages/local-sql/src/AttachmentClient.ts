import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
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
    reference: Attachment.Reference,
    range?: Attachment.Range
  ) => Stream.Stream<Uint8Array, ClientFailure>
  readonly markRemoteAvailable: (
    spaceId: Identity.SpaceId,
    reference: Attachment.Reference
  ) => Effect.Effect<void, ClientFailure>
  readonly ensureUploaded: (
    spaceId: Identity.SpaceId,
    references: ReadonlyArray<Attachment.Reference>
  ) => Effect.Effect<void, ClientFailure>
  readonly drainDeletions: (
    maximum: number
  ) => Effect.Effect<number, Attachment.AttachmentStorageError | ReplicaError.ReplicaError>
}

export class AttachmentClient extends Context.Service<AttachmentClient, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentClient"
) {}

const Lookup = Schema.Struct({ spaceId: Identity.SpaceId, digest: Attachment.Digest })

const readError = (message: string) => (cause: unknown): ReplicaError.StorageError => {
  if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
  return new ReplicaError.StorageCorrupt({ message, cause })
}

const write = <A, R,>(
  self: Effect.Effect<A, SqlError.SqlError, R>
): Effect.Effect<A, ReplicaError.StorageUnavailable, R> => Effect.mapError(self, StorageUnavailable.make)

export const layer: Layer.Layer<
  AttachmentClient,
  never,
  SqlClient.SqlClient | AttachmentStorage.AttachmentStorage | AttachmentTransfer.AttachmentTransfer
> = Layer.effect(
  AttachmentClient,
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const storage = yield* AttachmentStorage.AttachmentStorage
    const transfer = yield* AttachmentTransfer.AttachmentTransfer
    const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })
    const findAttachment = SqlSchema.findOneOption({
      Request: Lookup,
      Result: Rows.ClientAttachmentRow,
      execute: ({ digest, spaceId }) =>
        sql`SELECT space_id, digest, bytes, object_key, remote_available, created_at, last_accessed_at
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
        Effect.mapError(readError("Client attachment metadata is corrupt"))
      )
    const notFound = (reference: Attachment.Reference) => new Attachment.AttachmentNotFound({ key: reference.digest })
    const withLock = <A, E extends { readonly _tag: string }, R,>(
      spaceId: Identity.SpaceId,
      digest: Attachment.Digest,
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<A, E, R> =>
      RcMap.get(locks, `${spaceId}:${digest}`).pipe(
        Effect.flatMap((lock) => lock.withPermit(effect)),
        Effect.scoped
      )

    const drainDeletions: Service["drainDeletions"] = Effect.fnUntraced(function*(maximum) {
      const batchSize = yield* Configuration.positiveSafeInteger("attachments.deletionBatchSize", maximum)
      const now = yield* Clock.currentTimeMillis
      const pending = yield* findDeletions({ now, maximum: batchSize }).pipe(
        Effect.mapError(readError("Client attachment deletion state is corrupt"))
      )
      for (const deletion of pending) {
        yield* storage.remove(deletion.object_key)
        yield* write(sql`DELETE FROM effect_local_client_attachment_deletions
          WHERE object_key = ${deletion.object_key}`)
      }
      return pending.length
    })

    const drainInBackground = drainDeletions(8).pipe(
      Effect.tapError((error) =>
        Effect.logWarning("Attachment deletion will be retried").pipe(
          Effect.annotateLogs("error", error._tag)
        )
      ),
      Effect.ignore
    )

    const stage: Service["stage"] = Effect.fnUntraced(function*<E extends { readonly _tag: string }, R,>(
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
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
          return staged.reference
        })
      )
      const reference = yield* Effect.catch(
        persist,
        (failure) => storage.remove(staged.key).pipe(Effect.andThen(Effect.fail(failure)))
      )
      yield* drainInBackground
      return reference
    })

    const objectKey: Service["objectKey"] = Effect.fnUntraced(function*(spaceId, reference) {
      const found = yield* find(spaceId, reference.digest)
      if (Option.isNone(found)) return yield* notFound(reference)
      if (found.value.bytes !== reference.bytes) {
        return yield* new Attachment.AttachmentLengthMismatch({
          expected: found.value.bytes,
          actual: reference.bytes
        })
      }
      return found.value.object_key
    })

    const associatePending: Service["associatePending"] = Effect.fnUntraced(function*(
      spaceId,
      mutationId,
      references
    ) {
      const now = yield* Clock.currentTimeMillis
      for (const reference of references) {
        const key = yield* objectKey(spaceId, reference)
        if (!(yield* storage.exists(key))) return yield* notFound(reference)
      }
      yield* Effect.forEach(
        references,
        (reference) =>
          write(sql`INSERT OR IGNORE INTO effect_local_client_attachment_owners
          (space_id, digest, owner_kind, owner_id, created_at)
          VALUES (${spaceId}, ${reference.digest}, 'Pending', ${mutationId}, ${now})`),
        { discard: true }
      )
      return undefined
    })

    const release: Service["release"] = Effect.fnUntraced(function*(spaceId, reference) {
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
          })).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
        })
      )
      yield* drainInBackground
    })

    const read: Service["read"] = (spaceId, reference, range) =>
      Stream.unwrap(
        objectKey(spaceId, reference).pipe(
          Effect.map((key) => storage.read(key, reference, range))
        )
      )

    const markRemoteAvailable: Service["markRemoteAvailable"] = Effect.fnUntraced(function*(spaceId, reference) {
      yield* objectKey(spaceId, reference)
      const now = yield* Clock.currentTimeMillis
      yield* write(sql`UPDATE effect_local_client_attachments
        SET remote_available = 1, last_accessed_at = ${now}
        WHERE space_id = ${spaceId} AND digest = ${reference.digest}`)
    })

    const ensureUploaded: Service["ensureUploaded"] = Effect.fnUntraced(function*(spaceId, references) {
      for (const reference of references) {
        yield* withLock(
          spaceId,
          reference.digest,
          Effect.gen(function*() {
            const found = yield* find(spaceId, reference.digest)
            if (Option.isNone(found)) return yield* notFound(reference)
            if (found.value.bytes !== reference.bytes) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: found.value.bytes,
                actual: reference.bytes
              })
            }
            if (found.value.remote_available === 1) return yield* Effect.void
            if (!(yield* storage.exists(found.value.object_key))) return yield* notFound(reference)
            yield* transfer.upload({
              spaceId,
              reference,
              bytes: storage.read(found.value.object_key, reference)
            })
            const now = yield* Clock.currentTimeMillis
            yield* write(sql`UPDATE effect_local_client_attachments
              SET remote_available = 1, last_accessed_at = ${now}
              WHERE space_id = ${spaceId} AND digest = ${reference.digest}`)
            return yield* Effect.void
          })
        )
      }
    })

    return AttachmentClient.of({
      stage,
      associatePending,
      release,
      objectKey,
      read,
      markRemoteAvailable,
      ensureUploaded,
      drainDeletions
    })
  })
)
