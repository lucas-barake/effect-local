import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentProtocol from "@lucas-barake/effect-local/AttachmentTransfer"
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
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentClient from "../src/AttachmentClient.js"
import * as AttachmentObjectStore from "../src/AttachmentObjectStore.js"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as Codec from "../src/internal/codec.js"
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
const attachmentCacheOptions = {
  maximumLocalBytes: 64 * 1024 * 1024,
  maximumLocalObjects: 8,
  maximumCacheBytes: 64,
  maximumCacheObjects: 8,
  maximumCacheAge: "1 day",
  evictionBatchSize: 4
} as const
const layerUnavailableTransfer = Layer.succeed(
  AttachmentTransfer.AttachmentTransfer,
  AttachmentTransfer.AttachmentTransfer.of({
    upload: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    download: () => Effect.fail(new ReplicaError.ServerUnavailable())
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
  defaultScope: Protocol.ReplicationScope.make({ models: [Message.name] }),
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

const objectStoreNamespace = AttachmentObjectStore.Namespace.make("integration")

const makeDelegatedObjectStore = () => {
  const uploads = new Map<string, {
    readonly reference: Attachment.Reference
    readonly verificationChunkBytes: number
    readonly bytes: Uint8Array
    readonly parts: Map<number, number>
  }>()
  const objects = new Map<string, {
    readonly identity: AttachmentObjectStore.ObjectIdentity
    readonly reference: Attachment.Reference
    readonly verificationChunkBytes: number
    readonly bytes: Uint8Array
  }>()
  const uploadGrants = new Map<string, {
    readonly uploadId: string
    readonly partNumber: number
    readonly offset: number
    readonly bytes: number
  }>()
  const downloadGrants = new Map<string, {
    readonly objectId: string
    readonly chunk: AttachmentProtocol.VerifiedChunk
  }>()
  const adapter: AttachmentObjectStore.Adapter = {
    namespace: objectStoreNamespace,
    beginUpload: ({ attemptId, reference, verificationChunkBytes }) => {
      if (!uploads.has(attemptId)) {
        uploads.set(attemptId, {
          reference,
          verificationChunkBytes,
          bytes: new Uint8Array(reference.bytes),
          parts: new Map()
        })
      }
      return Effect.succeed(AttachmentObjectStore.BegunUpload.make({
        upload: AttachmentObjectStore.UploadIdentity.make({
          namespace: objectStoreNamespace,
          id: AttachmentObjectStore.ProviderId.make(attemptId)
        }),
        partSize: reference.bytes
      }))
    },
    listUploadedParts: ({ afterPartNumber, limit, upload }) => {
      const state = uploads.get(upload.id)
      if (state === undefined) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderUploadNotFound({ upload }))
      }
      const parts = [...state.parts]
        .filter(([partNumber]) => partNumber > afterPartNumber)
        .toSorted(([left], [right]) => left - right)
        .slice(0, limit)
        .map(([partNumber, bytes]) => ({ partNumber, bytes }))
      return Effect.succeed(AttachmentObjectStore.UploadedPartPage.make({ parts, nextPartNumber: null }))
    },
    grantUploadPart: ({ bytes, expiresAt, offset, partNumber, upload }) => {
      const url = `https://objects.example/upload/${upload.id}/${partNumber}`
      uploadGrants.set(url, { uploadId: upload.id, partNumber, offset, bytes })
      return Effect.succeed(AttachmentObjectStore.DirectUploadGrant.make({
        expiresAt,
        request: { method: "PUT", url: AttachmentProtocol.GrantUrl.make(url), headers: [] }
      }))
    },
    finalizeUpload: ({ upload }) => {
      const state = uploads.get(upload.id)
      if (state === undefined) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderUploadNotFound({ upload }))
      }
      const identity = AttachmentObjectStore.ObjectIdentity.make({
        namespace: objectStoreNamespace,
        id: AttachmentObjectStore.ProviderId.make(`object-${upload.id}`),
        version: AttachmentObjectStore.ProviderVersion.make("v1")
      })
      objects.set(identity.id, { identity, ...state })
      return Effect.succeed(AttachmentObjectStore.VerifiedObject.make({
        object: identity,
        reference: state.reference,
        chunkBytes: state.verificationChunkBytes,
        chunkCount: Math.ceil(state.reference.bytes / state.verificationChunkBytes)
      }))
    },
    inspectFinalized: ({ upload }) => {
      const state = objects.get(`object-${upload.id}`)
      if (state === undefined) return Effect.succeed(null)
      return Effect.succeed(AttachmentObjectStore.VerifiedObject.make({
        object: state.identity,
        reference: state.reference,
        chunkBytes: state.verificationChunkBytes,
        chunkCount: Math.ceil(state.reference.bytes / state.verificationChunkBytes)
      }))
    },
    listVerifiedChunks: ({ object }) => {
      const state = objects.get(object.id)
      if (state === undefined) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderObjectNotFound({ object }))
      }
      return Effect.succeed(AttachmentObjectStore.VerifiedChunkPage.make({
        chunks: [{ index: 0, offset: 0, bytes: state.reference.bytes, digest: state.reference.digest }],
        nextIndex: null
      }))
    },
    grantDownload: ({ chunk, expiresAt, object }) => {
      const url = `https://objects.example/download/${object.id}/${chunk.index}`
      downloadGrants.set(url, { objectId: object.id, chunk })
      return Effect.succeed(AttachmentObjectStore.DirectDownloadGrant.make({
        expiresAt,
        request: { method: "GET", url: AttachmentProtocol.GrantUrl.make(url), headers: [] }
      }))
    },
    abortUpload: () => Effect.void,
    deleteObject: ({ object }) =>
      Effect.sync(() => {
        objects.delete(object.id)
      })
  }
  const upload = Effect.fnUntraced(function*<E extends { readonly _tag: string }, R,>(
    request: AttachmentProtocol.DirectUploadRequest,
    source: Stream.Stream<Uint8Array, E, R>
  ) {
    const grant = uploadGrants.get(request.url)
    if (grant === undefined) return yield* Effect.die("unknown attachment upload grant")
    const state = uploads.get(grant.uploadId)
    if (state === undefined) return yield* Effect.die("unknown attachment upload")
    const body = yield* collectBytes(source)
    if (body.byteLength < grant.bytes) return yield* Effect.die("short attachment upload")
    state.bytes.set(body.subarray(0, grant.bytes), grant.offset)
    state.parts.set(grant.partNumber, grant.bytes)
    return undefined
  })
  const download = (
    request: AttachmentProtocol.DirectDownloadRequest
  ) => {
    const grant = downloadGrants.get(request.url)
    if (grant === undefined) return Effect.die("unknown attachment download grant")
    const state = objects.get(grant.objectId)
    if (state === undefined) return Effect.die("unknown attachment object")
    return Effect.succeed(state.bytes.slice(grant.chunk.offset, grant.chunk.offset + grant.chunk.bytes))
  }
  return {
    layer: AttachmentObjectStore.layer({
      namespaceForNewObjects: objectStoreNamespace,
      adapters: [adapter]
    }),
    upload,
    download
  }
}

const uploadDirect = Effect.fnUntraced(function*<E extends { readonly _tag: string }, R,>(
  server: AttachmentServer.Service,
  provider: ReturnType<typeof makeDelegatedObjectStore>,
  identity: Parameters<AttachmentServer.Service["prepareUpload"]>[0],
  bytes: (offset: number) => Stream.Stream<Uint8Array, E, R>
) {
  while (true) {
    const prepared = yield* server.prepareUpload(identity)
    if (prepared._tag === "UploadComplete") return
    if (prepared._tag === "UploadReady") {
      yield* server.finalizeUpload({ ...identity, attemptId: prepared.attemptId })
      return
    }
    yield* provider.upload(prepared.request, bytes(prepared.offset))
  }
})

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
        const layerAttachments = AttachmentClient.layer(attachmentCacheOptions).pipe(
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
    "keeps large media out of pending, authoritative, entity, and snapshot JSON",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-wire-" })
        const media = new Uint8Array(512 * 1024)
        let state = 0x9e3779b9
        for (let index = 0; index < media.length; index++) {
          state ^= state << 13
          state ^= state >>> 17
          state ^= state << 5
          media[index] = state & 0xff
        }
        assert.isAbove(media.byteLength, Protocol.maximumMutationBytes)

        const layerClientDatabase = SqliteClient.layer({ filename: `${root}/client.sqlite`, disableWAL: true })
        const layerClientStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/client-objects`,
          maximumBytes: media.byteLength
        })
        const layerClientInfrastructure = Layer.merge(layerClientDatabase, layerClientStorage)
        const layerClientAttachments = AttachmentClient.layer(attachmentCacheOptions).pipe(
          Layer.provide(layerClientInfrastructure),
          Layer.provide(layerUnavailableTransfer)
        )
        const layerReplica = SqlReplica.layer(replicaOptions).pipe(
          Layer.provide(layerClientAttachments),
          Layer.provide(layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(layerClientDatabase),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(Reactivity.layer)
        )
        const clientContext = yield* Layer.mergeAll(
          layerReplica,
          layerClientAttachments,
          layerClientInfrastructure
        ).pipe(Layer.build)
        const replica = Context.get(clientContext, Replica.Replica)
        const clientAttachments = Context.get(clientContext, AttachmentClient.AttachmentClient)
        const clientStorage = Context.get(clientContext, AttachmentStorage.AttachmentStorage)
        const clientSql = Context.get(clientContext, SqlClient.SqlClient)
        const space = yield* replica.space(spaceId)
        const reference = yield* space.stageAttachment(Stream.make(media))
        const pending = yield* space.mutate(PutMessage, {
          id: "large-media",
          body: "reference only",
          attachment: reference
        })
        yield* space.releaseAttachment(reference)
        const expectedValue = {
          id: "large-media",
          body: "reference only",
          attachment: { _tag: "Attachment" as const, digest: reference.digest, bytes: media.byteLength }
        }
        assert.deepStrictEqual(pending.envelope.payload, expectedValue)
        assert.isBelow(yield* Protocol.encodedBytesEffect(pending.envelope), Protocol.maximumMutationBytes)
        const pendingRows = yield* clientSql<{ readonly payload_json: string }>`
          SELECT payload_json FROM effect_local_client_pending_data
          WHERE space_id = ${spaceId} AND mutation_id = ${pending.envelope.mutationId}`
        assert.lengthOf(pendingRows, 1)
        assert.deepStrictEqual(yield* Codec.parse(pendingRows[0].payload_json), expectedValue)
        assert.isBelow(pendingRows[0].payload_json.length, 1_024)

        const layerServerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true })
        const objectStore = makeDelegatedObjectStore()
        const layerServerInfrastructure = Layer.merge(layerServerDatabase, objectStore.layer)
        const layerServerAttachments = AttachmentServer.layer({
          maximumObjectBytes: media.byteLength,
          maximumObjectsPerSpace: 4,
          maximumBytesPerSpace: media.byteLength * 2,
          maximumReferencesPerObject: 8,
          uploadGrantLifetime: "1 hour",
          uploadLeaseLifetime: "1 minute",
          readLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 hour",
          deletionBatchSize: 8,
          verificationChunkBytes: media.byteLength,
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
        const serverSql = Context.get(serverContext, SqlClient.SqlClient)
        yield* server.pull(Protocol.PullRequest.make({
          spaceId,
          clientId,
          schema: definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [Message.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null,
          limit: 10
        }))
        const identity = {
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          reference,
          principal: null
        }
        const localKey = yield* clientAttachments.objectKey(spaceId, reference)
        yield* uploadDirect(
          serverAttachments,
          objectStore,
          identity,
          (offset) =>
            clientStorage.read(localKey, reference, {
              offset,
              length: reference.bytes - offset
            })
        )
        assert.strictEqual((yield* server.submit(pending.envelope))._tag, "Accepted")
        yield* server.maintain(spaceId)

        const logRows = yield* serverSql<{ readonly entry_json: string }>`
          SELECT entry_json FROM effect_local_authoritative_log WHERE space_id = ${spaceId}`
        assert.lengthOf(logRows, 1)
        const accepted = yield* Codec.parse(logRows[0].entry_json).pipe(
          Effect.flatMap((json) => Codec.decode(Protocol.AcceptedMutation, json))
        )
        assert.lengthOf(accepted.changes, 1)
        const change = accepted.changes[0]
        assert.strictEqual(change._tag, "Upsert")
        if (change._tag === "Upsert") assert.deepStrictEqual(change.value, expectedValue)
        assert.isBelow(logRows[0].entry_json.length, 2_048)

        const entityRows = yield* serverSql<{ readonly value_json: string }>`
          SELECT value_json FROM effect_local_server_entities_data
          WHERE space_id = ${spaceId} AND model = ${Message.name}`
        assert.lengthOf(entityRows, 1)
        assert.deepStrictEqual(yield* Codec.parse(entityRows[0].value_json), expectedValue)
        assert.isBelow(entityRows[0].value_json.length, 1_024)

        const snapshotRows = yield* serverSql<{
          readonly value_json: string
          readonly wire_json: string
        }>`SELECT value_json, wire_json FROM effect_local_server_snapshot_entities
          WHERE space_id = ${spaceId} AND model = ${Message.name}`
        assert.lengthOf(snapshotRows, 1)
        assert.deepStrictEqual(yield* Codec.parse(snapshotRows[0].value_json), expectedValue)
        const snapshot = yield* Codec.parse(snapshotRows[0].wire_json).pipe(
          Effect.flatMap((json) => Codec.decode(Protocol.SnapshotEntity, json))
        )
        assert.deepStrictEqual(snapshot.value, expectedValue)
        assert.isBelow(snapshotRows[0].wire_json.length, 2_048)
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
        const objectStore = makeDelegatedObjectStore()
        const layerServerInfrastructure = Layer.merge(layerServerDatabase, objectStore.layer)
        const layerServerAttachments = AttachmentServer.layer({
          maximumObjectBytes: 8,
          maximumObjectsPerSpace: 4,
          maximumBytesPerSpace: 32,
          maximumReferencesPerObject: 8,
          uploadGrantLifetime: "1 hour",
          uploadLeaseLifetime: "1 minute",
          readLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 hour",
          deletionBatchSize: 8,
          verificationChunkBytes: 8,
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
              yield* uploadDirect(serverAttachments, objectStore, identity, bytes)
              yield* Ref.update(events, (current) => [...current, "upload"])
            }),
            download: ({
              clientId: requestedClientId,
              membershipIncarnation,
              range,
              reference,
              spaceId: requestedSpaceId
            }) =>
              Effect.fnUntraced(function*() {
                let request: Parameters<AttachmentServer.Service["prepareDownload"]>[0] = {
                  spaceId: requestedSpaceId,
                  clientId: requestedClientId,
                  membershipIncarnation,
                  reference,
                  principal: null
                }
                if (range !== undefined) request = { ...request, range }
                const grant = yield* serverAttachments.prepareDownload(request)
                const downloaded = yield* objectStore.download(grant.request)
                return {
                  objectVersion: grant.objectVersion,
                  objectBytes: reference.bytes,
                  chunk: grant.chunk,
                  bytes: Stream.make(downloaded)
                }
              })()
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
        const layerAttachments = AttachmentClient.layer(attachmentCacheOptions).pipe(
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
        assert.strictEqual(alreadyComplete._tag, "UploadPart")
        const stolen = yield* submitWithoutGrant(
          unprovenClientId,
          unprovenIncarnation,
          "message-stolen",
          "new binding without upload possession",
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000003")
        ).pipe(Effect.result)
        assert.isTrue(Result.isFailure(stolen))
        if (Result.isFailure(stolen)) assert.strictEqual(stolen.failure._tag, "AttachmentUnavailable")
        const downloadGrant = yield* serverAttachments.prepareDownload({
          spaceId,
          clientId,
          membershipIncarnation: pending.envelope.membershipIncarnation,
          reference,
          principal: null
        })
        assert.deepStrictEqual(yield* objectStore.download(downloadGrant.request), hello)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
