import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
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
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"

const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: Schema.String,
    body: Schema.String,
    attachment: Attachment.Reference
  })
})
const PutMessage = Mutation.make("PutMessage", { version: 1, payload: Message.schema })
const definition = Definition.make({ version: 1, models: [Message], mutations: [PutMessage] })
const layerHandlers = PutMessage.toLayer(({ payload, transaction }) => transaction.set(Message, payload.id, payload))
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const hello = Uint8Array.from([104, 101, 108, 108, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)
const layerUnavailableTransfer = Layer.succeed(
  AttachmentTransfer.AttachmentTransfer,
  AttachmentTransfer.AttachmentTransfer.of({
    upload: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    download: () => Stream.fail(new ReplicaError.ServerUnavailable())
  })
)
const layerRemote = Layer.succeed(
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
const replicaOptions = {
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
} satisfies SqlReplica.Options<typeof definition>
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

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("replica attachments", () => {
  it.effect(
    "commits an offline reference without putting bytes on the JSON wire",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-replica-attachments-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerAttachments = AttachmentClient.layer.pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerUnavailableTransfer)
        )
        const layerReplica = SqlReplica.layer(replicaOptions).pipe(
          Layer.provide(layerAttachments),
          Layer.provide(layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(layerDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const context = yield* Layer.build(layerReplica)
        const replica = Context.get(context, Replica.Replica)
        const space = yield* replica.space(spaceId)

        const reference = yield* space.stageAttachment(Stream.make(hello))
        const pending = yield* space.mutate(PutMessage, {
          id: "message-1",
          body: "offline",
          attachment: reference
        })
        yield* space.releaseAttachment(reference)

        const visible = Option.getOrThrow(yield* space.get(Message, "message-1"))
        assert.deepStrictEqual(visible.attachment, reference)
        assert.deepStrictEqual(yield* collectBytes(space.readAttachment(reference)), hello)
        assert.deepStrictEqual(pending.envelope.payload, {
          id: "message-1",
          body: "offline",
          attachment: { _tag: "Attachment", digest: reference.digest, bytes: reference.bytes }
        })
        const serialized = Canonical.stringify(pending)
        assert.notInclude(serialized, "aGVsbG8=")
        assert.notInclude(serialized, "104,101,108,108,111")
        assert.lengthOf(yield* fs.readDirectory(`${root}/objects`), 1)
        yield* replica.leave(spaceId)
        assert.deepStrictEqual(yield* fs.readDirectory(`${root}/objects`), [])
      },
      provideNodeServices,
      Effect.scoped
    )
  )

  it.effect(
    "uploads durable bytes after connectivity returns and before submitting the mutation",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-replica-upload-" })
        const connected = yield* Deferred.make<void>()
        const submitted = yield* Deferred.make<void>()
        const events = yield* Ref.make<ReadonlyArray<string>>([])
        const layerServerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true })
        const layerServerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/server-objects`,
          maximumBytes: 8
        })
        const layerServerInfrastructure = Layer.merge(layerServerDatabase, layerServerStorage)
        const layerServerAttachments = AttachmentServer.layer({
          maximumObjectBytes: 8,
          maximumObjectsPerSpace: 4,
          maximumBytesPerSpace: 32,
          uploadGrantLifetime: "1 hour",
          uploadLeaseLifetime: "1 minute",
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
        const server = Context.get(serverContext, ServerStore.ServerStore)
        const serverAttachments = Context.get(serverContext, AttachmentServer.AttachmentServer)
        const layerTransfer = Layer.succeed(
          AttachmentTransfer.AttachmentTransfer,
          AttachmentTransfer.AttachmentTransfer.of({
            upload: Effect.fnUntraced(function*({
              bytes,
              clientId: requestedClientId,
              membershipIncarnation,
              reference,
              spaceId: requestedSpaceId
            }) {
              yield* Deferred.await(connected)
              const identity = {
                spaceId: requestedSpaceId,
                clientId: requestedClientId,
                membershipIncarnation,
                reference,
                principal: null
              }
              const prepared = yield* serverAttachments.prepareUpload(identity)
              yield* serverAttachments.appendUpload({
                ...identity,
                expectedOffset: prepared.offset,
                bytes: bytes(prepared.offset)
              })
              yield* Ref.update(events, (current) => [...current, "upload"])
            }),
            download: ({
              clientId: requestedClientId,
              membershipIncarnation,
              range,
              reference,
              spaceId: requestedSpaceId
            }) => {
              let request: Parameters<AttachmentServer.Service["read"]>[0] = {
                spaceId: requestedSpaceId,
                clientId: requestedClientId,
                membershipIncarnation,
                reference,
                principal: null
              }
              if (range !== undefined) request = { ...request, range }
              return serverAttachments.read(request)
            }
          })
        )
        const layerConnectedRemote = Layer.succeed(
          SyncEngine.SyncEngine,
          SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
            submit: Effect.fnUntraced(function*(request) {
              yield* Ref.update(events, (current) => [...current, "submit"])
              const receipt = yield* server.submit(request)
              yield* Deferred.succeed(submitted, undefined)
              return receipt
            }),
            discard: (request) => server.discard(request, null),
            pull: server.pull,
            bootstrap: server.bootstrap,
            watch: server.watch
          })
        )
        const layerClientDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerClientDatabase, layerStorage)
        const layerAttachments = AttachmentClient.layer.pipe(
          Layer.provide(layerInfrastructure),
          Layer.provide(layerTransfer)
        )
        const layerReplica = SqlReplica.layer(replicaOptions).pipe(
          Layer.provide(layerAttachments),
          Layer.provide(layerHandlers),
          Layer.provide(layerConnectedRemote),
          Layer.provide(layerClientDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const context = yield* Layer.build(layerReplica)
        const replica = Context.get(context, Replica.Replica)
        const space = yield* replica.space(spaceId)
        const reference = yield* space.stageAttachment(Stream.make(hello))

        const pending = yield* space.mutate(PutMessage, {
          id: "message-upload",
          body: "queued offline",
          attachment: reference
        })
        assert.deepStrictEqual(yield* Ref.get(events), [])

        yield* Deferred.succeed(connected, undefined)
        yield* Deferred.await(submitted)

        assert.deepStrictEqual(yield* Ref.get(events), ["upload", "submit"])
        const submitWithoutGrant = Effect.fnUntraced(function*(
          requestedClientId: Identity.ClientId,
          requestedIncarnation: Identity.MembershipIncarnation,
          requestedId: string,
          body: string,
          mutationId: Identity.MutationId
        ) {
          const attachment: typeof Schema.Json.Type = {
            _tag: "Attachment",
            digest: reference.digest,
            bytes: reference.bytes
          }
          const identity = {
            spaceId,
            clientId: requestedClientId,
            membershipIncarnation: requestedIncarnation,
            mutationId,
            localSequence: Identity.LocalSequence.make(1),
            basis: Identity.ServerSequence.make(0),
            name: PutMessage.name,
            payload: { id: requestedId, body, attachment },
            digestVersion: 3 as const,
            sourceSchema: definition.schemaIdentity,
            mutationVersion: PutMessage.version
          }
          return yield* server.submit(
            Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
          )
        })
        const retaining = yield* submitWithoutGrant(
          Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
          Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000002"),
          "message-upload",
          "retained by another writer",
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000002")
        )
        assert.strictEqual(retaining._tag, "Accepted")
        const unprovenClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000003")
        const unprovenIncarnation = Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000003"
        )
        const alreadyComplete = yield* serverAttachments.prepareUpload({
          spaceId,
          clientId: unprovenClientId,
          membershipIncarnation: unprovenIncarnation,
          reference,
          principal: null
        })
        assert.isTrue(alreadyComplete.complete)
        const stolen = yield* submitWithoutGrant(
          unprovenClientId,
          unprovenIncarnation,
          "message-stolen",
          "new binding without upload possession",
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000003")
        ).pipe(Effect.result)
        assert.isTrue(Result.isFailure(stolen))
        if (Result.isFailure(stolen)) assert.strictEqual(stolen.failure._tag, "AttachmentUnavailable")
        assert.deepStrictEqual(
          yield* collectBytes(serverAttachments.read({
            spaceId,
            clientId,
            membershipIncarnation: pending.envelope.membershipIncarnation,
            reference,
            principal: null
          })),
          hello
        )
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
