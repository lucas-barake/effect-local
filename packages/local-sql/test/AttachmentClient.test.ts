import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentProtocol from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const hello = Uint8Array.from([104, 101, 108, 108, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)
const attachmentCacheOptions = {
  maximumLocalBytes: 64,
  maximumLocalObjects: 8,
  maximumCacheBytes: 64,
  maximumCacheObjects: 8,
  maximumCacheAge: "1 day",
  evictionBatchSize: 4
} as const

const AttachmentMessage = Model.make("AttachmentClientMessage", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    attachment: Attachment.Reference
  })
})
const PutAttachmentMessage = Mutation.make("PutAttachmentClientMessage", {
  version: 1,
  payload: AttachmentMessage.schema
})
const attachmentDefinition = Definition.make({
  version: 1,
  models: [AttachmentMessage],
  mutations: [PutAttachmentMessage]
})
const layerAttachmentHandlers = PutAttachmentMessage.toLayer(({ payload, transaction }) =>
  transaction.set(AttachmentMessage, payload.id, payload)
)
const attachmentReplicaOptions = {
  definition: attachmentDefinition,
  clientId,
  initialSpaces: [spaceId],
  defaultScope: Protocol.ReplicationScope.make({ models: [AttachmentMessage.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
} satisfies SqlReplica.Options<typeof attachmentDefinition>

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

const objectVersion = AttachmentProtocol.ObjectVersion.make("object-v1")
const downloadResponse = (
  reference: Attachment.Reference,
  bytes: Uint8Array,
  stream: Stream.Stream<Uint8Array, ReplicaError.ReplicaError> = Stream.make(bytes)
): AttachmentTransfer.DownloadResponse => ({
  objectVersion,
  objectBytes: reference.bytes,
  chunk: AttachmentProtocol.VerifiedChunk.make({
    index: 0,
    offset: 0,
    bytes: bytes.length,
    digest: reference.digest
  }),
  bytes: stream
})

describe("attachment client", () => {
  it.effect(
    "allocates a new physical incarnation when another runtime reclaims a deleting chunk",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-chunk-incarnation-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const objectBytes = Uint8Array.from({ length: 8 }, (_, index) => index)
      const chunkBytes = objectBytes.slice(0, 4)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(objectBytes)))
      const chunkReference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(chunkBytes)))
      const oldKey = AttachmentStorage.ObjectKey.make("11111111111111111111111111111111")
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Effect.succeed({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index: 0,
                offset: 0,
                bytes: chunkBytes.length,
                digest: chunkReference.digest
              }),
              bytes: Stream.make(chunkBytes)
            })
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 16 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer({
          ...attachmentCacheOptions,
          maximumLocalBytes: 8,
          maximumCacheBytes: 8
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const sql = Context.get(first, SqlClient.SqlClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      yield* sql`INSERT INTO effect_local_client_attachment_chunks
        (space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes, chunk_digest,
          object_key, state, created_at, last_accessed_at)
        VALUES (${spaceId}, ${reference.digest}, ${objectVersion}, 0, 0, ${chunkBytes.length},
          ${chunkReference.digest}, ${oldKey}, 'Deleting', 0, 0)`
      yield* sql`INSERT INTO effect_local_client_attachment_read_claims
        (claim_token, object_key, expires_at, created_at)
        VALUES ('first-runtime-read', ${oldKey}, 30000, 0)`

      const second = yield* build()
      const secondClient = Context.get(second, AttachmentClient.AttachmentClient)
      assert.deepStrictEqual(
        yield* collectBytes(secondClient.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 0,
          length: 4
        })),
        chunkBytes
      )
      const rows = yield* sql<{ readonly object_key: string; readonly state: string }>`SELECT object_key, state
        FROM effect_local_client_attachment_chunks`
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].state, "Verified")
      assert.notStrictEqual(rows[0].object_key, oldKey)
    }, provideNodeServices)
  )

  it.effect(
    "repairs missing verified chunk files by redownloading them",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-missing-chunk-" })
      const objectBytes = Uint8Array.from({ length: 8 }, (_, index) => index)
      const chunkBytes = objectBytes.slice(0, 4)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(objectBytes)))
      const chunkReference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(chunkBytes)))
      const missingKey = AttachmentStorage.ObjectKey.make("11111111111111111111111111111111")
      const downloads = yield* Ref.make(0)
      const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
      const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 16 })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Ref.update(downloads, (count) => count + 1).pipe(Effect.as({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index: 0,
                offset: 0,
                bytes: chunkBytes.length,
                digest: chunkReference.digest
              }),
              bytes: Stream.make(chunkBytes)
            }))
        })
      )
      const context = yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const sql = Context.get(context, SqlClient.SqlClient)
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      yield* sql`INSERT INTO effect_local_client_attachment_chunks
        (space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes, chunk_digest,
          object_key, state, created_at, last_accessed_at)
        VALUES (${spaceId}, ${reference.digest}, ${objectVersion}, 0, 0, ${chunkBytes.length},
          ${chunkReference.digest}, ${missingKey}, 'Verified', 0, 0)`

      assert.deepStrictEqual(
        yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 0,
          length: 4
        })),
        chunkBytes
      )
      assert.strictEqual(yield* Ref.get(downloads), 1)
      const keys = yield* sql<{ readonly object_key: string }>`SELECT object_key
        FROM effect_local_client_attachment_chunks`
      assert.strictEqual(keys.length, 1)
      assert.notStrictEqual(keys[0].object_key, missingKey)
    }, provideNodeServices)
  )

  it.effect(
    "streams a cached provider range without aggregating its file chunks",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-streaming-range-" })
      const objectBytes = Uint8Array.from({ length: 256 * 1024 }, (_, index) => index % 251)
      const rangeBytes = objectBytes.slice(0, 128 * 1024)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(objectBytes)))
      const rangeReference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(rangeBytes)))
      const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
      const layerStorage = FileSystemAttachmentStorage.layer({
        directory: `${root}/objects`,
        maximumBytes: objectBytes.length
      })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Effect.succeed({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index: 0,
                offset: 0,
                bytes: rangeBytes.length,
                digest: rangeReference.digest
              }),
              bytes: Stream.make(rangeBytes)
            })
        })
      )
      const context = yield* AttachmentClient.layer({
        ...attachmentCacheOptions,
        maximumLocalBytes: objectBytes.length,
        maximumCacheBytes: objectBytes.length
      }).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const sql = Context.get(context, SqlClient.SqlClient)
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      const chunks = yield* client.read(spaceId, clientId, membershipIncarnation, reference, {
        offset: 0,
        length: rangeBytes.length
      }).pipe(Stream.runCollect)

      assert.isAbove(chunks.length, 1)
      const flattened = chunks.flatMap((chunk) => Array.from(chunk))
      assert.deepStrictEqual(Uint8Array.from(flattened), rangeBytes)
    }, provideNodeServices)
  )

  it.effect(
    "replaces stale complete metadata before publishing a redownloaded object",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-missing-complete-" })
      const bytes = Uint8Array.of(1, 2, 3, 4)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(bytes)))
      const missingKey = AttachmentStorage.ObjectKey.make("11111111111111111111111111111111")
      const downloads = yield* Ref.make(0)
      const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
      const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Ref.update(downloads, (count) => count + 1).pipe(
              Effect.as(downloadResponse(reference, bytes))
            )
        })
      )
      const context = yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const sql = Context.get(context, SqlClient.SqlClient)
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      yield* sql`INSERT INTO effect_local_client_attachments
        (space_id, digest, bytes, object_version, object_key, remote_available, cache_managed,
          created_at, last_accessed_at)
        VALUES (${spaceId}, ${reference.digest}, ${reference.bytes}, ${objectVersion}, ${missingKey}, 1, 1, 0, 0)`

      assert.deepStrictEqual(
        yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference)),
        bytes
      )
      assert.deepStrictEqual(
        yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference)),
        bytes
      )
      assert.strictEqual(yield* Ref.get(downloads), 1)
      const keys = yield* sql<{ readonly object_key: string }>`SELECT object_key
        FROM effect_local_client_attachments`
      assert.strictEqual(keys.length, 1)
      assert.notStrictEqual(keys[0].object_key, missingKey)
    }, provideNodeServices)
  )

  it.effect(
    "keeps one maintenance sweep constant across one thousand cached rows",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-maintenance-count-" })
      const actualSql = yield* SqliteClient.make({ filename: `${root}/client.sqlite`, disableWAL: true }).pipe(
        Effect.provide(Reactivity.layer)
      )
      const statements: Array<string> = []
      const observedSql = new Proxy(actualSql, {
        apply: (target, thisArgument, argumentsList) => {
          if (Array.isArray(argumentsList[0])) {
            statements.push(argumentsList[0].join("?").replace(/\s+/g, " ").trim())
          }
          return Reflect.apply(target, thisArgument, argumentsList)
        }
      })
      const layerDatabase = Layer.succeed(SqlClient.SqlClient, observedSql)
      const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 1 })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () => Effect.die("unexpected download")
        })
      )
      const context = yield* AttachmentClient.layer({
        ...attachmentCacheOptions,
        maximumLocalObjects: 2_000,
        maximumCacheObjects: 2_000
      }).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, observedSql)
      )
      for (let index = 0; index < 1_000; index++) {
        const digest = `sha256:${index.toString(16).padStart(64, "0")}`
        const key = index.toString(16).padStart(32, "0")
        yield* observedSql`INSERT INTO effect_local_client_attachments
          (space_id, digest, bytes, object_key, created_at, last_accessed_at)
          VALUES (${spaceId}, ${digest}, 0, ${key}, 0, 0)`
      }
      statements.length = 0

      yield* client.maintain

      assert.isBelow(statements.length, 50)
      assert.isFalse(statements.some((statement) => statement.includes("SET active_reads")))
    }, provideNodeServices)
  )

  it.effect(
    "recovers an interrupted promotion reservation after restart without releasing source occupancy",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-promotion-recovery-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const bytes = Uint8Array.of(1, 2, 3, 4)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(bytes)))
      const sourceKey = AttachmentStorage.ObjectKey.make("11111111111111111111111111111111")
      const targetKey = AttachmentStorage.ObjectKey.make("22222222222222222222222222222222")
      const claim = "promotion-claim"
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () => Effect.die("unexpected download")
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 16 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer({
          ...attachmentCacheOptions,
          maximumLocalBytes: 16,
          maximumCacheBytes: 16
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const sql = Context.get(first, SqlClient.SqlClient)
      const storage = Context.get(first, AttachmentStorage.AttachmentStorage)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      yield* storage.create(sourceKey)
      yield* storage.append(sourceKey, reference, 0, Stream.make(bytes))
      yield* storage.verify(sourceKey, reference)
      yield* sql`INSERT INTO effect_local_client_attachment_chunks
        (space_id, digest, object_version, chunk_index, chunk_offset, chunk_bytes, chunk_digest,
          object_key, state, promotion_token, created_at, last_accessed_at)
        VALUES (${spaceId}, ${reference.digest}, ${objectVersion}, 0, 0, ${bytes.length},
          ${reference.digest}, ${sourceKey}, 'Verified', ${claim}, 0, 0)`
      yield* sql`INSERT INTO effect_local_client_attachment_promotions
        (space_id, digest, object_version, bytes, object_key, state, claim_token,
          claim_expires_at, created_at, last_accessed_at)
        VALUES (${spaceId}, ${reference.digest}, ${objectVersion}, ${bytes.length}, ${targetKey},
          'Filling', ${claim}, 100, 0, 0)`
      yield* storage.create(targetKey)
      const promotionPrefix = Uint8Array.of(1)
      yield* storage.append(targetKey, reference, 0, Stream.make(promotionPrefix))
      const reserved = yield* sql<{ readonly local_byte_count: number }>`
        SELECT local_byte_count FROM effect_local_client_attachment_usage WHERE id = 1`
      assert.deepStrictEqual(reserved, [{ local_byte_count: 8 }])

      const restarted = yield* build()
      const restartedClient = Context.get(restarted, AttachmentClient.AttachmentClient)
      yield* TestClock.adjust("101 millis")
      yield* restartedClient.maintain
      assert.isFalse(yield* storage.exists(targetKey))
      const recovered = yield* sql<{
        readonly promotions: number
        readonly promotion_token: string | null
        readonly local_byte_count: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_attachment_promotions) AS promotions,
        (SELECT promotion_token FROM effect_local_client_attachment_chunks LIMIT 1) AS promotion_token,
        local_byte_count
        FROM effect_local_client_attachment_usage WHERE id = 1`
      assert.deepStrictEqual(recovered, [{ promotions: 0, promotion_token: null, local_byte_count: 4 }])

      yield* sql`UPDATE effect_local_client_attachment_chunks SET active_reads = 1`
      yield* sql`INSERT INTO effect_local_client_attachment_read_claims
        (claim_token, object_key, expires_at, created_at)
        VALUES ('expired-read', ${sourceKey}, 100, 0)`
      yield* restartedClient.maintain
      const recoveredReads = yield* sql<{ readonly claims: number }>`SELECT COUNT(*) AS claims
        FROM effect_local_client_attachment_read_claims`
      assert.deepStrictEqual(recoveredReads, [{ claims: 0 }])
    }, provideNodeServices)
  )

  it.effect(
    "promotes complete fixed chunks through a separately charged durable reservation",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-promotion-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const bytes = Uint8Array.from({ length: 8 }, (_, index) => index)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(bytes)))
      const chunks = [bytes.slice(0, 4), bytes.slice(4)]
      const hashes = yield* Effect.forEach(chunks, (chunk) => Attachment.hash(Stream.make(chunk)))
      const downloads = yield* Ref.make(0)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: ({ range }) => {
            let index = 0
            if (range?.offset === 4) index = 1
            return Ref.update(downloads, (count) => count + 1).pipe(Effect.as({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index,
                offset: index * 4,
                bytes: chunks[index].length,
                digest: hashes[index].digest
              }),
              bytes: Stream.make(chunks[index])
            }))
          }
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 16 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer({
          ...attachmentCacheOptions,
          maximumLocalBytes: 16,
          maximumCacheBytes: 16
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const sql = Context.get(first, SqlClient.SqlClient)
      const client = Context.get(first, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      assert.deepStrictEqual(
        yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference)),
        bytes
      )
      const state = yield* sql<{
        readonly attachments: number
        readonly chunks: number
        readonly promotions: number
        readonly local_bytes: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_attachments) AS attachments,
        (SELECT COUNT(*) FROM effect_local_client_attachment_chunks) AS chunks,
        (SELECT COUNT(*) FROM effect_local_client_attachment_promotions) AS promotions,
        local_byte_count AS local_bytes
        FROM effect_local_client_attachment_usage WHERE id = 1`
      assert.deepStrictEqual(state, [{ attachments: 1, chunks: 0, promotions: 0, local_bytes: 8 }])

      const restarted = yield* build()
      const restartedClient = Context.get(restarted, AttachmentClient.AttachmentClient)
      assert.deepStrictEqual(
        yield* collectBytes(restartedClient.read(spaceId, clientId, membershipIncarnation, reference)),
        bytes
      )
      assert.strictEqual(yield* Ref.get(downloads), 2)
    }, provideNodeServices)
  )

  it.effect(
    "coalesces overlapping verified chunk fills across client runtimes",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-coalesced-range-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const bytes = Uint8Array.of(1, 2, 3, 4)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(bytes)))
      const bodyStarted = yield* Deferred.make<void>()
      const secondGrant = yield* Deferred.make<void>()
      const releaseBody = yield* Deferred.make<void>()
      const bodies = yield* Ref.make(0)
      const grants = yield* Ref.make(0)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Ref.updateAndGet(grants, (count) => count + 1).pipe(
              Effect.tap((count) => {
                if (count === 2) return Deferred.succeed(secondGrant, undefined)
                return Effect.void
              }),
              Effect.as({
                ...downloadResponse(reference, bytes),
                bytes: Stream.fromEffect(
                  Ref.update(bodies, (count) => count + 1).pipe(
                    Effect.andThen(Deferred.succeed(bodyStarted, undefined)),
                    Effect.andThen(Deferred.await(releaseBody)),
                    Effect.as(bytes)
                  )
                )
              })
            )
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const firstSql = Context.get(first, SqlClient.SqlClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, firstSql)
      )
      const second = yield* build()
      const firstClient = Context.get(first, AttachmentClient.AttachmentClient)
      const secondClient = Context.get(second, AttachmentClient.AttachmentClient)
      const read = (client: AttachmentClient.Service) =>
        collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 1,
          length: 2
        }))
      const firstRead = yield* read(firstClient).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(bodyStarted)
      const secondRead = yield* read(secondClient).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(secondGrant)
      yield* Deferred.succeed(releaseBody, undefined)
      const firstBytes = yield* Fiber.join(firstRead)
      yield* Effect.yieldNow
      yield* TestClock.adjust("1 second")

      assert.deepStrictEqual(firstBytes, Uint8Array.of(2, 3))
      assert.deepStrictEqual(yield* Fiber.join(secondRead), Uint8Array.of(2, 3))
      assert.strictEqual(yield* Ref.get(bodies), 1)
    }, provideNodeServices)
  )

  it.effect(
    "rejects individually verified chunks whose promoted object digest is corrupt",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-flat-digest-" })
      const expected = Uint8Array.from({ length: 8 }, (_, index) => index)
      const remote = Uint8Array.from({ length: 8 }, (_, index) => index + 1)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(expected)))
      const chunks = [remote.slice(0, 4), remote.slice(4)]
      const hashes = yield* Effect.forEach(chunks, (chunk) => Attachment.hash(Stream.make(chunk)))
      const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
      const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 16 })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: ({ range }) => {
            let index = 0
            if (range?.offset === 4) index = 1
            return Effect.succeed({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index,
                offset: index * 4,
                bytes: chunks[index].length,
                digest: hashes[index].digest
              }),
              bytes: Stream.make(chunks[index])
            })
          }
        })
      )
      const context = yield* AttachmentClient.layer({
        ...attachmentCacheOptions,
        maximumLocalBytes: 16,
        maximumCacheBytes: 16
      }).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const sql = Context.get(context, SqlClient.SqlClient)
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )

      const observed = yield* client.read(spaceId, clientId, membershipIncarnation, reference).pipe(
        Stream.runCollect,
        Effect.result
      )

      assert.isTrue(Result.isFailure(observed))
      if (Result.isFailure(observed)) assert.strictEqual(observed.failure._tag, "AttachmentDigestMismatch")
      const state = yield* sql<{
        readonly attachments: number
        readonly chunks: number
        readonly promotions: number
        readonly localBytes: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_attachments) AS attachments,
        (SELECT COUNT(*) FROM effect_local_client_attachment_chunks) AS chunks,
        (SELECT COUNT(*) FROM effect_local_client_attachment_promotions) AS promotions,
        local_byte_count AS localBytes
        FROM effect_local_client_attachment_usage WHERE id = 1`
      assert.deepStrictEqual(state, [{ attachments: 0, chunks: 0, promotions: 0, localBytes: 0 }])
    }, provideNodeServices)
  )

  it.effect(
    "rejects a corrupt granted chunk before exposing any caller bytes",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-corrupt-range-" })
      const expected = Uint8Array.of(1, 2, 3, 4)
      const corrupt = Uint8Array.of(1, 2, 3, 5)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(expected)))
      const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
      const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
      const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: () =>
            Effect.succeed({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index: 0,
                offset: 0,
                bytes: expected.length,
                digest: reference.digest
              }),
              bytes: Stream.make(corrupt)
            })
        })
      )
      const context = yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
        Layer.provide(layerInfrastructure),
        Layer.provide(layerTransfer),
        Layer.provideMerge(layerInfrastructure),
        Layer.build
      )
      const sql = Context.get(context, SqlClient.SqlClient)
      const client = Context.get(context, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      const observed = yield* client.read(spaceId, clientId, membershipIncarnation, reference, {
        offset: 1,
        length: 2
      }).pipe(Stream.runCollect, Effect.result)

      assert.isTrue(Result.isFailure(observed))
      if (Result.isFailure(observed)) assert.strictEqual(observed.failure._tag, "AttachmentDigestMismatch")
      const rows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_client_attachment_chunks`
      assert.deepStrictEqual(rows, [{ count: 0 }])
    }, provideNodeServices)
  )

  it.effect(
    "persists a verified provider chunk and serves exact range slices across restart",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-range-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const remoteBytes = Uint8Array.from({ length: 8 }, (_, index) => index)
      const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(remoteBytes)))
      const firstChunk = remoteBytes.slice(0, 4)
      const firstChunkHash = yield* Attachment.hash(Stream.make(firstChunk))
      const secondChunk = remoteBytes.slice(4)
      const secondChunkHash = yield* Attachment.hash(Stream.make(secondChunk))
      const downloads = yield* Ref.make(0)
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: ({ range }) => {
            const second = range?.offset === 4
            let chunk = firstChunk
            let hashed = firstChunkHash
            let index = 0
            let offset = 0
            if (second) {
              chunk = secondChunk
              hashed = secondChunkHash
              index = 1
              offset = 4
            }
            return Ref.update(downloads, (count) => count + 1).pipe(Effect.as({
              objectVersion,
              objectBytes: reference.bytes,
              chunk: AttachmentProtocol.VerifiedChunk.make({
                index,
                offset,
                bytes: chunk.length,
                digest: hashed.digest
              }),
              bytes: Stream.make(chunk)
            }))
          }
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer({
          ...attachmentCacheOptions,
          maximumLocalBytes: 4,
          maximumCacheBytes: 4
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const firstSql = Context.get(first, SqlClient.SqlClient)
      const firstClient = Context.get(first, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, firstSql)
      )

      assert.deepStrictEqual(
        yield* collectBytes(firstClient.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 2,
          length: 2
        })),
        Uint8Array.of(2, 3)
      )
      const chunks = yield* firstSql<{
        readonly state: string
        readonly chunk_bytes: number
      }>`SELECT state, chunk_bytes FROM effect_local_client_attachment_chunks`
      assert.deepStrictEqual(chunks, [{ state: "Verified", chunk_bytes: 4 }])

      const restarted = yield* build()
      const restartedClient = Context.get(restarted, AttachmentClient.AttachmentClient)
      assert.deepStrictEqual(
        yield* collectBytes(restartedClient.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 2,
          length: 2
        })),
        Uint8Array.of(2, 3)
      )
      assert.strictEqual(yield* Ref.get(downloads), 1)
      assert.deepStrictEqual(
        yield* collectBytes(restartedClient.read(spaceId, clientId, membershipIncarnation, reference, {
          offset: 3,
          length: 3
        })),
        Uint8Array.of(3, 4, 5)
      )
      assert.strictEqual(yield* Ref.get(downloads), 2)
      const bounded = yield* firstSql<{
        readonly attachments: number
        readonly chunks: number
        readonly promotions: number
        readonly local_bytes: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_client_attachments) AS attachments,
        (SELECT COUNT(*) FROM effect_local_client_attachment_chunks) AS chunks,
        (SELECT COUNT(*) FROM effect_local_client_attachment_promotions) AS promotions,
        local_byte_count AS local_bytes
        FROM effect_local_client_attachment_usage WHERE id = 1`
      assert.deepStrictEqual(bounded, [{ attachments: 0, chunks: 1, promotions: 0, local_bytes: 4 }])
    }, provideNodeServices)
  )

  it.effect(
    "bounds the lazy cache by LRU while retaining owned objects",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-cache-" })
        const payloads = [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)]
        const references = yield* Effect.forEach(payloads, (bytes) =>
          Attachment.hash(Stream.make(bytes)).pipe(
            Effect.map((hashed) => Attachment.Reference.make(hashed))
          ))
        const downloads = yield* Ref.make(new Map<Attachment.Digest, number>())
        const bytesByDigest = new Map(references.map((reference, index) => [reference.digest, payloads[index]]))
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: ({ reference: remoteReference }) =>
              Ref.update(downloads, (current) => {
                const updated = new Map(current)
                updated.set(remoteReference.digest, (updated.get(remoteReference.digest) ?? 0) + 1)
                return updated
              }).pipe(Effect.map(() => {
                const bytes = bytesByDigest.get(remoteReference.digest)!
                return downloadResponse(remoteReference, bytes)
              }))
          })
        )
        const context = yield* AttachmentClient.layer({
          maximumLocalBytes: 8,
          maximumLocalObjects: 8,
          maximumCacheBytes: 2,
          maximumCacheObjects: 2,
          maximumCacheAge: "1 day",
          evictionBatchSize: 2
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const read = (reference: Attachment.Reference) =>
          collectBytes(client.read(spaceId, clientId, membershipIncarnation, reference))

        yield* read(references[0])
        yield* TestClock.adjust("1 second")
        yield* read(references[1])
        yield* TestClock.adjust("1 second")
        yield* read(references[0])
        yield* TestClock.adjust("1 second")
        yield* read(references[2])

        const cached = yield* sql<{ readonly digest: string }>`SELECT digest FROM effect_local_client_attachments
          WHERE cache_managed = 1 ORDER BY digest`
        assert.deepStrictEqual(
          cached.map((row) => row.digest).toSorted(),
          [references[0].digest, references[2].digest].toSorted()
        )
        assert.strictEqual((yield* Ref.get(downloads)).get(references[0].digest), 1)

        yield* client.associatePending(
          spaceId,
          Identity.MutationId.make(
            "mut_00000000-0000-4000-8000-000000000010"
          ),
          [references[0]]
        )
        yield* TestClock.adjust("1 second")
        yield* read(references[1])
        const retained = yield* sql<{ readonly digest: string }>`SELECT digest FROM effect_local_client_attachments
          WHERE cache_managed = 1 ORDER BY digest`
        assert.include(retained.map((row) => row.digest), references[0].digest)
        assert.notInclude(retained.map((row) => row.digest), references[2].digest)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "fences cross-runtime eviction while a complete cached object is being read",
    Effect.fnUntraced(function*() {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-read-lease-" })
      const database = `${root}/client.sqlite`
      const objects = `${root}/objects`
      const payloads = [Uint8Array.of(1), Uint8Array.of(2)]
      const references = yield* Effect.forEach(
        payloads,
        (bytes) => Attachment.hash(Stream.make(bytes)).pipe(Effect.map((hashed) => Attachment.Reference.make(hashed)))
      )
      const bytesByDigest = new Map(references.map((reference, index) => [reference.digest, payloads[index]]))
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.die("unexpected upload"),
          download: ({ reference }) => {
            const bytes = bytesByDigest.get(reference.digest)!
            return Effect.succeed(downloadResponse(reference, bytes))
          }
        })
      )
      const build = Effect.fnUntraced(function*() {
        const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 4 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        return yield* AttachmentClient.layer({
          maximumLocalBytes: 1,
          maximumLocalObjects: 1,
          maximumCacheBytes: 1,
          maximumCacheObjects: 1,
          maximumCacheAge: "1 day",
          evictionBatchSize: 1
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
      })
      const first = yield* build()
      const sql = Context.get(first, SqlClient.SqlClient)
      const firstClient = Context.get(first, AttachmentClient.AttachmentClient)
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql)
      )
      const second = yield* build()
      const secondClient = Context.get(second, AttachmentClient.AttachmentClient)
      yield* collectBytes(firstClient.read(spaceId, clientId, membershipIncarnation, references[0]))
      const readStarted = yield* Deferred.make<void>()
      const releaseRead = yield* Deferred.make<void>()
      const active = yield* firstClient.read(spaceId, clientId, membershipIncarnation, references[0]).pipe(
        Stream.runForEach(() =>
          Deferred.succeed(readStarted, undefined).pipe(Effect.andThen(Deferred.await(releaseRead)))
        ),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(readStarted)
      yield* TestClock.adjust("11 seconds")
      const renewed = yield* sql<{ readonly expires_at: number }>`SELECT expires_at
        FROM effect_local_client_attachment_read_claims`
      assert.strictEqual(renewed.length, 1)
      assert.isAbove(renewed[0].expires_at, 30_000)

      const blocked = yield* collectBytes(
        secondClient.read(spaceId, clientId, membershipIncarnation, references[1])
      ).pipe(Effect.result)
      assert.isTrue(Result.isFailure(blocked))
      if (Result.isFailure(blocked)) assert.strictEqual(blocked.failure._tag, "CapacityExceeded")

      yield* Deferred.succeed(releaseRead, undefined)
      yield* Fiber.join(active)
      assert.deepStrictEqual(
        yield* collectBytes(secondClient.read(spaceId, clientId, membershipIncarnation, references[1])),
        payloads[1]
      )
      const cached = yield* sql<{ readonly digest: string }>`SELECT digest
        FROM effect_local_client_attachments WHERE cache_managed = 1`
      assert.deepStrictEqual(cached, [{ digest: references[1].digest }])
    }, provideNodeServices)
  )

  it.effect(
    "serializes concurrent cache admissions and counts zero byte objects",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-cache-admission-" })
        const emptyReference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(new Uint8Array())))
        const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: () => Effect.die("zero byte attachments do not require transfer")
          })
        )
        const context = yield* AttachmentClient.layer({
          maximumLocalBytes: 8,
          maximumLocalObjects: 8,
          maximumCacheBytes: 1,
          maximumCacheObjects: 1,
          maximumCacheAge: "1 day",
          evictionBatchSize: 1
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        yield* Migrations.client({ definition: Domain.definition, spaceId: otherSpaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )

        yield* Effect.all([
          collectBytes(client.read(spaceId, clientId, membershipIncarnation, emptyReference)),
          collectBytes(client.read(otherSpaceId, clientId, membershipIncarnation, emptyReference))
        ], { concurrency: "unbounded" })

        const cached = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_client_attachments WHERE cache_managed = 1`
        assert.strictEqual(cached[0].count, 0)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "rejects an impossible remote object while an unrelated read remains active",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-read-capacity-" })
        const remoteBytes = Uint8Array.from({ length: 8 }, (_, index) => index)
        const reference = Attachment.Reference.make(yield* Attachment.hash(Stream.make(remoteBytes)))
        const consumed = yield* Ref.make(0)
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 16 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: ({ reference: remoteReference }) =>
              Ref.update(consumed, (count) => count + remoteBytes.length).pipe(
                Effect.as(downloadResponse(remoteReference, remoteBytes))
              )
          })
        )
        const context = yield* AttachmentClient.layer({
          maximumLocalBytes: 1,
          maximumLocalObjects: 1,
          maximumCacheBytes: 1,
          maximumCacheObjects: 1,
          maximumCacheAge: "1 day",
          evictionBatchSize: 1
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const activeBytes = Uint8Array.of(1)
        const activeReference = yield* client.stage(spaceId, Stream.make(activeBytes))
        const activeReadStarted = yield* Deferred.make<void>()
        const releaseActiveRead = yield* Deferred.make<void>()
        const activeRead = yield* client.read(spaceId, clientId, membershipIncarnation, activeReference).pipe(
          Stream.runForEach(() =>
            Deferred.succeed(activeReadStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseActiveRead))
            )
          ),
          Effect.forkChild
        )
        yield* Deferred.await(activeReadStarted)

        const rejectedRead = yield* collectBytes(
          client.read(spaceId, clientId, membershipIncarnation, reference)
        ).pipe(
          Effect.result,
          Effect.timeoutOption("1 second"),
          Effect.forkChild({ startImmediately: true })
        )
        yield* TestClock.adjust("1 second")
        const completed = yield* Fiber.join(rejectedRead)
        yield* Deferred.succeed(releaseActiveRead, undefined)
        yield* Fiber.join(activeRead)

        assert.isTrue(Option.isSome(completed))
        if (Option.isNone(completed)) return
        const result = completed.value
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "CapacityExceeded")
        assert.strictEqual(yield* Ref.get(consumed), 0)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "bounds offline staging by bytes and objects while preserving deduplication",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-stage-capacity-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: () => Effect.die("unexpected download")
          })
        )
        const context = yield* AttachmentClient.layer({
          maximumLocalBytes: 1,
          maximumLocalObjects: 3,
          maximumCacheBytes: 2,
          maximumCacheObjects: 2,
          maximumCacheAge: "1 day",
          evictionBatchSize: 1
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )

        const firstBytes = Uint8Array.of(1)
        const secondBytes = Uint8Array.of(2)
        const empty = yield* client.stage(spaceId, Stream.make(new Uint8Array()))
        const one = yield* client.stage(spaceId, Stream.make(firstBytes))
        const duplicate = yield* client.stage(spaceId, Stream.make(firstBytes))
        const rejected = yield* client.stage(spaceId, Stream.make(secondBytes)).pipe(Effect.result)

        assert.strictEqual(duplicate.digest, one.digest)
        assert.isTrue(Result.isFailure(rejected))
        if (Result.isFailure(rejected)) {
          assert.strictEqual(rejected.failure._tag, "CapacityExceeded")
          if (rejected.failure._tag === "CapacityExceeded") {
            assert.strictEqual(rejected.failure.resource, "client attachment storage")
            assert.strictEqual(rejected.failure.limit, 1)
          }
        }
        const usage = yield* sql<{ readonly object_count: number; readonly byte_count: number }>`
          SELECT COUNT(*) AS object_count, COALESCE(SUM(bytes), 0) AS byte_count
          FROM effect_local_client_attachments`
        assert.deepStrictEqual(usage, [{ object_count: 2, byte_count: 1 }])
        const counterUsage = yield* sql<{
          readonly local_object_count: number
          readonly local_byte_count: number
        }>`SELECT local_object_count, local_byte_count
          FROM effect_local_client_attachment_usage WHERE id = 1`
        assert.deepStrictEqual(counterUsage, [{ local_object_count: 2, local_byte_count: 1 }])
        assert.deepStrictEqual(
          yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, empty)),
          new Uint8Array()
        )
        assert.deepStrictEqual(
          yield* collectBytes(client.read(spaceId, clientId, membershipIncarnation, one)),
          Uint8Array.of(1)
        )
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "serializes concurrent offline staging admissions",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-stage-concurrent-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 8 })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: () => Effect.die("unexpected download")
          })
        )
        const context = yield* AttachmentClient.layer({
          maximumLocalBytes: 1,
          maximumLocalObjects: 1,
          maximumCacheBytes: 1,
          maximumCacheObjects: 1,
          maximumCacheAge: "1 day",
          evictionBatchSize: 1
        }).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )

        const firstBytes = Uint8Array.of(1)
        const secondBytes = Uint8Array.of(2)
        const staged = yield* Effect.all([
          client.stage(spaceId, Stream.make(firstBytes)).pipe(Effect.result),
          client.stage(spaceId, Stream.make(secondBytes)).pipe(Effect.result)
        ], { concurrency: "unbounded" })

        assert.strictEqual(staged.filter(Result.isSuccess).length, 1)
        assert.strictEqual(staged.filter(Result.isFailure).length, 1)
        const failed = staged.find(Result.isFailure)
        assert.isDefined(failed)
        if (failed !== undefined && Result.isFailure(failed)) {
          assert.strictEqual(failed.failure._tag, "CapacityExceeded")
        }
        const usage = yield* sql<{ readonly object_count: number; readonly byte_count: number }>`
          SELECT COUNT(*) AS object_count, COALESCE(SUM(bytes), 0) AS byte_count
          FROM effect_local_client_attachments`
        assert.deepStrictEqual(usage, [{ object_count: 1, byte_count: 1 }])
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "preserves the persistence failure when staged byte cleanup also fails",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-client-cleanup-" })
        const reference = Attachment.Reference.make({
          digest: Attachment.Digest.make(
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
          ),
          bytes: hello.length
        })
        const key = AttachmentStorage.ObjectKey.make("a".repeat(32))
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = Layer.succeed(
          AttachmentStorage.AttachmentStorage,
          AttachmentStorage.AttachmentStorage.of({
            create: () => Effect.succeed(key),
            stage: () => Effect.succeed({ key, reference }),
            append: () => Effect.die("unexpected append"),
            offset: () => Effect.die("unexpected offset"),
            verify: () => Effect.die("unexpected verify"),
            read: () => Stream.die("unexpected read"),
            exists: () => Effect.succeed(true),
            remove: () =>
              Effect.fail(new Attachment.AttachmentStorageError({ operation: "remove", cause: "cleanup failed" }))
          })
        )
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.die("unexpected upload"),
            download: () => Effect.die("unexpected download")
          })
        )
        const client = yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
          Layer.provide(Layer.merge(layerDatabase, layerStorage)),
          Layer.provide(layerTransfer),
          Layer.build,
          Effect.map(Context.get(AttachmentClient.AttachmentClient))
        )

        const staged = yield* client.stage(spaceId, Stream.make(hello)).pipe(Effect.result)

        assert.isTrue(Result.isFailure(staged))
        if (Result.isFailure(staged)) assert.strictEqual(staged.failure._tag, "StorageUnavailable")
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "revalidates remote custody before retrying a pending attachment",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-client-custody-" })
        const remote = yield* Ref.make<Uint8Array | undefined>(undefined)
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true }).pipe(
          Layer.provide(Reactivity.layer)
        )
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: (request) =>
              collectBytes(request.bytes(0)).pipe(
                Effect.flatMap((bytes) => Ref.set(remote, bytes))
              ),
            download: () => Effect.fail(new ReplicaError.ServerUnavailable())
          })
        )
        const context = yield* AttachmentClient.layer(attachmentCacheOptions).pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer),
          Layer.provideMerge(layerInfrastructure),
          Layer.build
        )
        const sql = Context.get(context, SqlClient.SqlClient)
        const client = Context.get(context, AttachmentClient.AttachmentClient)
        yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql)
        )
        const reference = yield* client.stage(spaceId, Stream.make(hello))

        yield* client.ensureUploaded(spaceId, clientId, membershipIncarnation, [reference])
        assert.deepStrictEqual(yield* Ref.get(remote), hello)

        yield* Ref.set(remote, undefined)
        yield* client.ensureUploaded(spaceId, clientId, membershipIncarnation, [reference])

        assert.deepStrictEqual(yield* Ref.get(remote), hello)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "keeps offline staged bytes across restart and releases them after pending settlement",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-client-" })
        const database = `${root}/client.sqlite`
        const objects = `${root}/objects`
        const downloads = yield* Ref.make(0)
        const layerServerDatabase = SqliteClient.layer({
          filename: `${root}/server.sqlite`,
          disableWAL: true
        })
        const layerServerRuntime = MutationRuntime.layer(attachmentDefinition).pipe(
          Layer.provide(layerAttachmentHandlers)
        )
        const layerServer = ServerStore.layerTrusted({
          definition: attachmentDefinition,
          retainedHistoryEntries: 256,
          maximumHistoryEntries: 10_000,
          retainedReceipts: 256,
          maximumReceipts: 10_000,
          maximumSnapshotEntities: 10_000,
          maximumSnapshotBytes: 64 * 1024 * 1024,
          maximumBootstrapPageBytes: 4 * 1024 * 1024,
          pruneBatchSize: 1_000,
          retainedSnapshots: 2,
          maintenanceConcurrency: 1,
          maintenanceSpaceBatchSize: 128,
          maximumWatchersPerSpace: 1_024,
          readAuthorizationRefreshInterval: "30 seconds",
          maximumConcurrentReadAuthorizations: 64,
          maximumPendingReadAuthorizations: 4_096,
          readAuthorizationCacheCapacity: 4_096,
          migration: { retryDelay: "1 millis", maximumAttempts: 8 }
        }).pipe(
          Layer.provide(layerServerRuntime),
          Layer.provide(layerServerDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const server = yield* layerServer.pipe(
          Layer.build,
          Effect.map(Context.get(ServerStore.ServerStore))
        )
        const layerOfflineRemote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            discard: () => Effect.die("unexpected discard"),
            pull: () => Effect.never,
            bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            watch: () => Stream.never
          })
        )
        const layerConnectedRemote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            submit: (request) => TestClock.adjust("2 millis").pipe(Effect.andThen(server.submit(request))),
            discard: (request) => server.discard(request, null),
            pull: server.pull,
            bootstrap: server.bootstrap,
            watch: server.watch
          })
        )
        const replicaLayer = (
          layerRemote: Layer.Layer<SyncEngine.SyncEngine>,
          layerTransfer: Layer.Layer<AttachmentTransfer.AttachmentTransfer>
        ) => {
          const layerDatabase = SqliteClient.layer({ filename: database, disableWAL: true })
          const layerStorage = FileSystemAttachmentStorage.layer({ directory: objects, maximumBytes: 8 })
          const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
          const layerAttachments = AttachmentClient.layer({
            ...attachmentCacheOptions,
            maximumCacheBytes: 5,
            maximumCacheAge: "1 milli"
          }).pipe(
            Layer.provide(layerInfrastructure),
            Layer.provide(layerTransfer)
          )
          const layerReplica = SqlReplica.layer(attachmentReplicaOptions).pipe(
            Layer.provide(layerAttachments),
            Layer.provide(layerAttachmentHandlers),
            Layer.provide(layerRemote),
            Layer.provide(layerDatabase),
            Layer.provide(NodeCrypto.layer),
            Layer.provide(Reactivity.layer)
          )
          return Layer.mergeAll(layerReplica, layerAttachments, layerInfrastructure)
        }
        const layerOfflineTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            download: () => Effect.fail(new ReplicaError.ServerUnavailable())
          })
        )

        const firstScope = yield* Scope.make()
        const first = yield* Layer.buildWithScope(replicaLayer(layerOfflineRemote, layerOfflineTransfer), firstScope)
        const firstReplica = Context.get(first, Replica.Replica)
        const firstSpace = yield* firstReplica.space(spaceId)
        const reference = yield* firstSpace.stageAttachment(Stream.make(hello))
        const pending = yield* firstSpace.mutate(PutAttachmentMessage, {
          id: "restart-message",
          attachment: reference
        })
        yield* firstSpace.releaseAttachment(reference)
        yield* Scope.close(firstScope, Exit.void)

        const connected = yield* Deferred.make<void>()
        const uploaded = yield* Ref.make<Uint8Array | undefined>(undefined)
        const layerConnectedTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: (request) =>
              Deferred.await(connected).pipe(
                Effect.andThen(request.bytes(0).pipe(collectBytes)),
                Effect.flatMap((bytes) => Ref.set(uploaded, bytes))
              ),
            download: ({ reference: remoteReference }) =>
              Ref.update(downloads, (count) => count + 1).pipe(
                Effect.as(downloadResponse(remoteReference, hello))
              )
          })
        )
        const secondScope = yield* Scope.make()
        const second = yield* Layer.buildWithScope(
          replicaLayer(layerConnectedRemote, layerConnectedTransfer),
          secondScope
        )
        const restartedSql = Context.get(second, SqlClient.SqlClient)
        const restartedReplica = Context.get(second, Replica.Replica)
        const restartedSpace = yield* restartedReplica.space(spaceId)
        assert.deepStrictEqual(
          yield* collectBytes(restartedSpace.readAttachment(reference)),
          hello
        )
        const ownersBefore = yield* restartedSql<{ readonly owner_kind: string }>`
          SELECT owner_kind FROM effect_local_client_attachment_owners
          WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
        assert.deepStrictEqual(ownersBefore.map((row) => row.owner_kind), ["Pending"])

        const settlementFiber = yield* restartedSpace.settlementsFor(PutAttachmentMessage).pipe(
          Stream.runHead,
          Effect.forkChild
        )
        yield* Deferred.succeed(connected, undefined)
        const settlement = Option.getOrThrow(yield* Fiber.join(settlementFiber))

        assert.strictEqual(settlement.pending.envelope.mutationId, pending.envelope.mutationId)
        assert.deepStrictEqual(yield* Ref.get(uploaded), hello)
        assert.deepStrictEqual(yield* restartedSpace.pendingFor(PutAttachmentMessage), [])
        const ownersAfter = yield* restartedSql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_client_attachment_owners
          WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
        assert.strictEqual(ownersAfter[0].count, 0)
        yield* TestClock.adjust("1 minute")
        const cachedAfterSettlement = yield* restartedSql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_client_attachments
          WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
        assert.strictEqual(cachedAfterSettlement[0].count, 0)
        assert.deepStrictEqual(yield* collectBytes(restartedSpace.readAttachment(reference)), hello)
        assert.strictEqual(yield* Ref.get(downloads), 1)
        yield* Scope.close(secondScope, Exit.void)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
