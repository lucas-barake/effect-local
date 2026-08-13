import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
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
  scope: Protocol.ReplicationScope.make({ models: [AttachmentMessage.name] }),
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

describe("attachment client", () => {
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
            download: ({ reference }) =>
              Stream.fromEffect(Ref.update(downloads, (current) => {
                const updated = new Map(current)
                updated.set(reference.digest, (updated.get(reference.digest) ?? 0) + 1)
                return updated
              })).pipe(Stream.flatMap(() => Stream.make(bytesByDigest.get(reference.digest)!)))
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
            download: () => Stream.make(new Uint8Array())
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
        assert.strictEqual(cached[0].count, 1)
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "rejects an impossible known-size remote object before consuming its bytes",
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
            download: () =>
              Stream.fromEffect(Ref.update(consumed, (count) => count + remoteBytes.length)).pipe(
                Stream.flatMap(() => Stream.make(remoteBytes))
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

        const result = yield* collectBytes(
          client.read(spaceId, clientId, membershipIncarnation, reference)
        ).pipe(Effect.result)

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
            download: () => Stream.die("unexpected download")
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
            download: () => Stream.die("unexpected download")
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
            download: () => Stream.die("unexpected download")
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
            download: () => Stream.fail(new ReplicaError.ServerUnavailable())
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
            download: () => Stream.fail(new ReplicaError.ServerUnavailable())
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
            download: () =>
              Stream.fromEffect(
                Ref.update(downloads, (count) => count + 1).pipe(Effect.as(hello))
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
