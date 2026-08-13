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
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"

const Message = Model.make("AttachmentRecipientMessage", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    body: Schema.String,
    attachment: Attachment.Reference
  })
})
const PutMessage = Mutation.make("PutAttachmentRecipientMessage", {
  version: 1,
  payload: Message.schema
})
const definition = Definition.make({ version: 1, models: [Message], mutations: [PutMessage] })
const layerHandlers = PutMessage.toLayer(({ payload, transaction }) => transaction.set(Message, payload.id, payload))

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const senderId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const recipientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const bytes = Uint8Array.from([112, 104, 111, 116, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const attachmentCacheOptions = {
  maximumLocalBytes: 64 * 1024 * 1024,
  maximumLocalObjects: 8,
  maximumCacheBytes: 64,
  maximumCacheObjects: 8,
  maximumCacheAge: "1 day",
  evictionBatchSize: 4
} as const
const provideNodeServices = Effect.provide(layerNodeServices)

const serverOptions = {
  definition,
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
} as const

const replicaOptions = (clientId: Identity.ClientId): SqlReplica.Options<typeof definition> => ({
  definition,
  clientId,
  initialSpaces: [spaceId],
  scope: Protocol.ReplicationScope.make({ models: [Message.name] }),
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
})

const transferLayer = (
  server: AttachmentServer.Service,
  events: Ref.Ref<ReadonlyArray<string>>
) =>
  Layer.succeed(
    AttachmentTransfer.AttachmentTransfer,
    AttachmentTransfer.AttachmentTransfer.of({
      upload: Effect.fnUntraced(function*({
        bytes: readBytes,
        clientId,
        membershipIncarnation,
        reference,
        spaceId: requestedSpaceId
      }) {
        const identity = { spaceId: requestedSpaceId, clientId, membershipIncarnation, reference, principal: null }
        const prepared = yield* server.prepareUpload(identity)
        if (!prepared.complete) {
          yield* server.appendUpload({
            ...identity,
            expectedOffset: prepared.offset,
            bytes: readBytes(prepared.offset)
          })
        }
        yield* Ref.update(events, (current) => [...current, "upload"])
      }),
      download: ({ clientId, membershipIncarnation, range, reference, spaceId: requestedSpaceId }) => {
        let request: Parameters<AttachmentServer.Service["read"]>[0] = {
          spaceId: requestedSpaceId,
          clientId,
          membershipIncarnation,
          reference,
          principal: null
        }
        if (range !== undefined) request = { ...request, range }
        return Stream.fromEffect(Ref.update(events, (current) => [...current, "download"])).pipe(
          Stream.flatMap(() => server.read(request))
        )
      }
    })
  )

const clientLayer = (options: {
  readonly clientId: Identity.ClientId
  readonly database: string
  readonly directory: string
  readonly layerRemote: Layer.Layer<SyncEngine.SyncEngine>
  readonly layerTransfer: Layer.Layer<AttachmentTransfer.AttachmentTransfer>
}) => {
  const layerDatabase = SqliteClient.layer({ filename: options.database, disableWAL: true })
  const layerStorage = FileSystemAttachmentStorage.layer({
    directory: options.directory,
    maximumBytes: 16
  })
  const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
  const layerAttachments = AttachmentClient.layer(attachmentCacheOptions).pipe(
    Layer.provide(layerInfrastructure),
    Layer.provide(options.layerTransfer)
  )
  const layerReplica = SqlReplica.layer(replicaOptions(options.clientId)).pipe(
    Layer.provide(layerAttachments),
    Layer.provide(layerHandlers),
    Layer.provide(options.layerRemote),
    Layer.provide(layerDatabase),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(Reactivity.layer)
  )
  return Layer.mergeAll(layerReplica, layerAttachments, layerInfrastructure)
}

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("attachment recipients", () => {
  it.effect(
    "syncs attachment references before lazily downloading their bytes",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-recipient-" })
        const connected = yield* Deferred.make<void>()
        const senderReachedRemote = yield* Deferred.make<void>()
        const submitted = yield* Deferred.make<void>()
        const recipientCaughtUp = yield* Deferred.make<void>()
        const recipientPulls = yield* Ref.make(0)
        const events = yield* Ref.make<ReadonlyArray<string>>([])

        const layerServerDatabase = SqliteClient.layer({
          filename: `${root}/server.sqlite`,
          disableWAL: true
        })
        const layerServerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/server-objects`,
          maximumBytes: 16
        })
        const layerServerInfrastructure = Layer.merge(layerServerDatabase, layerServerStorage)
        const layerServerAttachments = AttachmentServer.layer({
          maximumObjectBytes: 16,
          maximumObjectsPerSpace: 4,
          maximumBytesPerSpace: 64,
          maximumReferencesPerObject: 8,
          uploadGrantLifetime: "1 hour",
          uploadLeaseLifetime: "1 minute",
          readLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 hour",
          deletionBatchSize: 8,
          authorizeAccess: () => Effect.void,
          authorizeUpload: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(Layer.provide(layerServerInfrastructure))
        const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
        const layerServer = ServerStore.layerTrusted(serverOptions).pipe(
          Layer.provide(layerServerAttachments),
          Layer.provide(layerRuntime),
          Layer.provide(layerServerDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const serverContext = yield* Layer.mergeAll(
          layerServer,
          layerServerAttachments,
          layerServerInfrastructure
        ).pipe(Layer.build)
        const serverStore = Context.get(serverContext, ServerStore.ServerStore)
        const serverAttachments = Context.get(serverContext, AttachmentServer.AttachmentServer)
        const layerTransfer = transferLayer(serverAttachments, events)

        const layerSenderRemote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            submit: (request) =>
              Deferred.await(connected).pipe(
                Effect.andThen(Ref.update(events, (current) => [...current, "submit"])),
                Effect.andThen(serverStore.submit(request)),
                Effect.tap(() => Deferred.succeed(submitted, undefined))
              ),
            discard: (request) => serverStore.discard(request, null),
            pull: (request) =>
              Deferred.succeed(senderReachedRemote, undefined).pipe(
                Effect.andThen(Deferred.await(connected)),
                Effect.andThen(serverStore.pull(request))
              ),
            bootstrap: (request) => Deferred.await(connected).pipe(Effect.andThen(serverStore.bootstrap(request))),
            watch: (request) =>
              Stream.fromEffect(Deferred.await(connected)).pipe(
                Stream.flatMap(() => serverStore.watch(request))
              )
          })
        )
        const senderContext = yield* clientLayer({
          clientId: senderId,
          database: `${root}/sender.sqlite`,
          directory: `${root}/sender-objects`,
          layerRemote: layerSenderRemote,
          layerTransfer
        }).pipe(Layer.build)
        const senderReplica = Context.get(senderContext, Replica.Replica)
        const senderSpace = yield* senderReplica.space(spaceId)
        yield* Deferred.await(senderReachedRemote)

        const reference = yield* senderSpace.stageAttachment(Stream.make(bytes))
        yield* senderSpace.mutate(PutMessage, {
          id: "photo-message",
          body: "queued while offline",
          attachment: reference
        })
        yield* senderSpace.releaseAttachment(reference)

        const optimistic = Option.getOrThrow(yield* senderSpace.get(Message, "photo-message"))
        assert.deepStrictEqual(optimistic.attachment, reference)
        assert.deepStrictEqual(yield* Ref.get(events), [])

        yield* Deferred.succeed(connected, undefined)
        yield* Deferred.await(submitted)
        assert.deepStrictEqual(yield* Ref.get(events), ["upload", "submit"])

        const layerRecipientRemote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            submit: serverStore.submit,
            discard: (request) => serverStore.discard(request, null),
            pull: (request) =>
              Ref.updateAndGet(recipientPulls, (count) => count + 1).pipe(
                Effect.tap((count) => {
                  if (count === 2) return Deferred.succeed(recipientCaughtUp, undefined)
                  return Effect.void
                }),
                Effect.andThen(serverStore.pull(request))
              ),
            bootstrap: serverStore.bootstrap,
            watch: serverStore.watch
          })
        )
        const recipientContext = yield* clientLayer({
          clientId: recipientId,
          database: `${root}/recipient.sqlite`,
          directory: `${root}/recipient-objects`,
          layerRemote: layerRecipientRemote,
          layerTransfer
        }).pipe(Layer.build)
        const recipientReplica = Context.get(recipientContext, Replica.Replica)
        const recipientSpace = yield* recipientReplica.space(spaceId)
        const recipientSql = Context.get(recipientContext, SqlClient.SqlClient)
        const recipientStorage = Context.get(recipientContext, AttachmentStorage.AttachmentStorage)
        const recipientAttachments = Context.get(recipientContext, AttachmentClient.AttachmentClient)
        yield* Deferred.await(recipientCaughtUp)

        const visible = Option.getOrThrow(yield* recipientSpace.get(Message, "photo-message"))
        assert.deepStrictEqual(visible.attachment, reference)
        const beforeRead = yield* recipientSql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_client_attachments
        WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
        assert.strictEqual(beforeRead[0].count, 0)
        assert.deepStrictEqual(yield* Ref.get(events), ["upload", "submit"])

        assert.deepStrictEqual(yield* collectBytes(recipientSpace.readAttachment(reference)), bytes)

        const cachedKey = yield* recipientAttachments.objectKey(spaceId, reference)
        assert.isTrue(yield* recipientStorage.exists(cachedKey))
        const afterRead = yield* recipientSql<{ readonly remote_available: number }>`
        SELECT remote_available FROM effect_local_client_attachments
        WHERE space_id = ${spaceId} AND digest = ${reference.digest}`
        assert.deepStrictEqual(afterRead.map((row) => row.remote_available), [1])
        assert.deepStrictEqual(yield* Ref.get(events), ["upload", "submit", "download"])
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
