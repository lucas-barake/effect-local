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
        const client = yield* AttachmentClient.layer.pipe(
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
        const context = yield* AttachmentClient.layer.pipe(
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
            submit: server.submit,
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
          const layerAttachments = AttachmentClient.layer.pipe(
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
        assert.deepStrictEqual(yield* collectBytes(restartedSpace.readAttachment(reference)), hello)
        assert.strictEqual(yield* Ref.get(downloads), 0)
        yield* Scope.close(secondScope, Exit.void)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
