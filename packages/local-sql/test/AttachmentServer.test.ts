import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Statement from "effect/unstable/sql/Statement"
import * as AttachmentObjectStore from "../src/AttachmentObjectStore.js"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as Codec from "../src/internal/codec.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const otherClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const otherMembershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000002"
)
const digest = Attachment.Digest.make(
  "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
)
const reference = Attachment.Reference.make({ digest, bytes: 5 })
const namespace = AttachmentObjectStore.Namespace.make("test")

class Denied extends Schema.TaggedErrorClass<Denied, Schema.JsonObject>(
  "@lucas-barake/effect-local-sql/test/AttachmentDenied"
)("AttachmentDenied", { reason: Schema.String }) {}

const history = {
  definition: Domain.definition,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
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

const makeProvider = (
  providerReference: Attachment.Reference = reference,
  verificationChunkBytes = 5
) => {
  const uploaded = new Map<string, number>()
  const objects = new Map<string, AttachmentObjectStore.ObjectIdentity>()
  const missingUploads = new Set<string>()
  const missingObjects = new Set<string>()
  const deleted: Array<string> = []
  const aborted: Array<string> = []
  const abortSpaces: Array<{ readonly spaceId: Identity.SpaceId; readonly uploadId: string }> = []
  const downloadRequests: Array<string> = []
  const adapter: AttachmentObjectStore.Adapter = {
    namespace,
    beginUpload: ({ attemptId }) => {
      uploaded.set(attemptId, uploaded.get(attemptId) ?? 0)
      return Effect.succeed(AttachmentObjectStore.BegunUpload.make({
        upload: AttachmentObjectStore.UploadIdentity.make({
          namespace,
          id: AttachmentObjectStore.ProviderId.make(attemptId)
        }),
        partSize: providerReference.bytes
      }))
    },
    listUploadedParts: ({ upload }) => {
      if (missingUploads.has(upload.id)) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderUploadNotFound({ upload }))
      }
      let parts: ReadonlyArray<AttachmentObjectStore.UploadedPart> = []
      if (uploaded.get(upload.id) === providerReference.bytes) {
        parts = [{ partNumber: 1, bytes: providerReference.bytes }]
      }
      return Effect.succeed(AttachmentObjectStore.UploadedPartPage.make({
        parts,
        nextPartNumber: null
      }))
    },
    grantUploadPart: ({ expiresAt }) =>
      Effect.succeed(AttachmentObjectStore.DirectUploadGrant.make({
        expiresAt,
        request: AttachmentTransfer.DirectUploadRequest.make({
          method: "PUT",
          url: AttachmentTransfer.GrantUrl.make("https://objects.example/upload"),
          headers: []
        })
      })),
    finalizeUpload: ({ upload }) => {
      const object = AttachmentObjectStore.ObjectIdentity.make({
        namespace,
        id: AttachmentObjectStore.ProviderId.make(`object-${upload.id}`),
        version: AttachmentObjectStore.ProviderVersion.make("v1")
      })
      objects.set(upload.id, object)
      return Effect.succeed(AttachmentObjectStore.VerifiedObject.make({
        object,
        reference: providerReference,
        chunkBytes: verificationChunkBytes,
        chunkCount: Math.ceil(providerReference.bytes / verificationChunkBytes)
      }))
    },
    inspectFinalized: ({ upload }) => {
      const object = objects.get(upload.id)
      if (object === undefined) return Effect.succeed(null)
      if (missingObjects.has(object.id)) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderObjectNotFound({ object }))
      }
      return Effect.succeed(
        AttachmentObjectStore.VerifiedObject.make({
          object,
          reference: providerReference,
          chunkBytes: verificationChunkBytes,
          chunkCount: Math.ceil(providerReference.bytes / verificationChunkBytes)
        })
      )
    },
    listVerifiedChunks: ({ afterIndex, limit }) => {
      const chunkCount = Math.ceil(providerReference.bytes / verificationChunkBytes)
      const end = Math.min(chunkCount, afterIndex + limit)
      const chunks = Array.from({ length: end - afterIndex }, (_, offset) => {
        const index = afterIndex + offset
        const chunkOffset = index * verificationChunkBytes
        return {
          index,
          offset: chunkOffset,
          bytes: Math.min(verificationChunkBytes, providerReference.bytes - chunkOffset),
          digest: providerReference.digest
        }
      })
      let nextIndex: number | null = null
      if (end < chunkCount) nextIndex = end
      return Effect.succeed(AttachmentObjectStore.VerifiedChunkPage.make({
        chunks,
        nextIndex
      }))
    },
    grantDownload: ({ expiresAt, object }) => {
      downloadRequests.push(object.id)
      if (missingObjects.has(object.id)) {
        return Effect.fail(new AttachmentObjectStore.AttachmentProviderObjectNotFound({ object }))
      }
      return Effect.succeed(AttachmentObjectStore.DirectDownloadGrant.make({
        expiresAt,
        request: AttachmentTransfer.DirectDownloadRequest.make({
          method: "GET",
          url: AttachmentTransfer.GrantUrl.make("https://objects.example/download"),
          headers: []
        })
      }))
    },
    abortUpload: ({ spaceId: authoritativeSpaceId, upload }) =>
      Effect.sync(() => {
        aborted.push(upload.id)
        abortSpaces.push({ spaceId: authoritativeSpaceId, uploadId: upload.id })
      }),
    deleteObject: ({ object }) =>
      Effect.sync(() => {
        deleted.push(object.id)
        missingObjects.add(object.id)
      })
  }
  return {
    adapter,
    uploaded,
    missingUploads,
    missingObjects,
    deleted,
    aborted,
    abortSpaces,
    downloadRequests,
    objects
  }
}

const makeContext = Effect.fnUntraced(function*(
  initialAccess = true,
  limits: { readonly maximumObjectsPerSpace: number; readonly maximumBytesPerSpace: number } = {
    maximumObjectsPerSpace: 4,
    maximumBytesPerSpace: 32
  },
  verification: {
    readonly maximumObjectBytes: number
    readonly verificationChunkBytes: number
    readonly maximumVerificationChunks: number
    readonly reference: Attachment.Reference
  } = {
    maximumObjectBytes: 8,
    verificationChunkBytes: 5,
    maximumVerificationChunks: 8,
    reference
  }
) {
  const fs = yield* FileSystem.FileSystem
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-delegated-attachment-" })
  const layerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true }).pipe(
    Layer.provide(Reactivity.layer)
  )
  const provider = makeProvider(verification.reference, verification.verificationChunkBytes)
  const layerObjectStore = AttachmentObjectStore.layer({
    namespaceForNewObjects: namespace,
    adapters: [provider.adapter]
  })
  const layerInfrastructure = Layer.mergeAll(layerDatabase, layerObjectStore, NodeCrypto.layer)
  const authorization = { access: initialAccess, read: true }
  const attachmentOptions = {
    maximumObjectBytes: verification.maximumObjectBytes,
    maximumObjectsPerSpace: limits.maximumObjectsPerSpace,
    maximumBytesPerSpace: limits.maximumBytesPerSpace,
    maximumReferencesPerObject: 4,
    uploadGrantLifetime: "1 minute",
    uploadLeaseLifetime: "1 minute",
    readLeaseLifetime: "2 minutes",
    stagingLifetime: "1 minute",
    garbageCollectionGracePeriod: "1 minute",
    deletionBatchSize: 8,
    verificationChunkBytes: verification.verificationChunkBytes,
    maximumVerificationChunks: verification.maximumVerificationChunks,
    authorizeAccess: () => {
      if (authorization.access) return Effect.void
      return Effect.fail(new Denied({ reason: "denied" }))
    },
    authorizeUpload: () => Effect.void,
    authorizeRead: () => {
      if (authorization.read) return Effect.void
      return Effect.fail(new Denied({ reason: "read denied" }))
    }
  } as const satisfies AttachmentServer.Options
  const layerAttachments = AttachmentServer.layer(attachmentOptions).pipe(Layer.provide(layerInfrastructure))
  const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
  const layerServer = ServerStore.layerTrusted(history).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerAttachments),
    Layer.provide(layerDatabase),
    Layer.provide(NodeCrypto.layer)
  )
  const context = yield* Layer.mergeAll(layerInfrastructure, layerAttachments, layerServer).pipe(Layer.build)
  const server = Context.get(context, ServerStore.ServerStore)
  yield* server.pull({
    spaceId,
    clientId,
    schema: Domain.definition.schemaIdentity,
    scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor: null,
    limit: 10
  })
  return {
    attachments: Context.get(context, AttachmentServer.AttachmentServer),
    server,
    sql: Context.get(context, SqlClient.SqlClient),
    provider,
    authorization
  }
})

const identity = {
  spaceId,
  clientId,
  membershipIncarnation,
  reference,
  principal: { user: "owner" }
}
const provideTestPlatform = Effect.provide(
  Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
)

describe("delegated attachment server", () => {
  it.effect(
    "rejects verification limits that cannot cover the maximum object size",
    Effect.fnUntraced(
      function*() {
        const outcome = yield* makeContext(true, undefined, {
          maximumObjectBytes: 8,
          verificationChunkBytes: 5,
          maximumVerificationChunks: 1,
          reference
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(outcome))
        if (Result.isFailure(outcome)) {
          assert.strictEqual(outcome.failure._tag, "InvalidConfiguration")
        }
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "persists a bounded verification manifest in bulk pages",
    Effect.fnUntraced(
      function*() {
        const largeReference = Attachment.Reference.make({ digest, bytes: 250 })
        const { attachments, provider } = yield* makeContext(
          true,
          { maximumObjectsPerSpace: 4, maximumBytesPerSpace: 512 },
          {
            maximumObjectBytes: 250,
            verificationChunkBytes: 1,
            maximumVerificationChunks: 300,
            reference: largeReference
          }
        )
        const largeIdentity = { ...identity, reference: largeReference }
        const prepared = yield* attachments.prepareUpload(largeIdentity)
        assert.strictEqual(prepared._tag, "UploadPart")
        if (prepared._tag !== "UploadPart") return
        provider.uploaded.set(prepared.attemptId, largeReference.bytes)
        const inserts = yield* Ref.make(0)
        const transformer: Statement.Transformer = (statement) => {
          const [query] = statement.compile()
          if (query.startsWith("INSERT INTO effect_local_server_attachment_chunks")) {
            return Ref.update(inserts, (count) => count + 1).pipe(Effect.as(statement))
          }
          return Effect.succeed(statement)
        }
        yield* attachments.finalizeUpload({ ...largeIdentity, attemptId: prepared.attemptId }).pipe(
          Effect.provideService(Statement.CurrentTransformer, transformer)
        )
        assert.strictEqual(yield* Ref.get(inserts), 3)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "resumes direct multipart upload and lazily grants a verified chunk",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider, server, sql } = yield* makeContext()
        const part = yield* attachments.prepareUpload(identity)
        assert.strictEqual(part._tag, "UploadPart")
        if (part._tag !== "UploadPart") return
        assert.strictEqual(part.offset, 0)
        assert.strictEqual(part.bytes, 5)
        assert.strictEqual(part.request.url, "https://objects.example/upload")

        provider.uploaded.set(part.attemptId, 5)
        const resumed = yield* attachments.prepareUpload(identity)
        assert.deepStrictEqual(resumed, { _tag: "UploadReady", attemptId: part.attemptId })
        yield* sql.unsafe(`CREATE TRIGGER fail_attachment_finalize
        BEFORE INSERT ON effect_local_server_attachment_objects
        BEGIN SELECT RAISE(ABORT, 'injected finalize commit failure'); END`)
        const failedFinalize = yield* attachments.finalizeUpload({
          ...identity,
          attemptId: part.attemptId
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(failedFinalize))
        if (Result.isFailure(failedFinalize)) {
          assert.strictEqual(failedFinalize.failure._tag, "StorageUnavailable")
        }
        yield* sql.unsafe("DROP TRIGGER fail_attachment_finalize")
        yield* TestClock.adjust("2 minutes")
        assert.deepStrictEqual(
          yield* attachments.finalizeUpload({ ...identity, attemptId: part.attemptId }),
          { _tag: "UploadComplete" }
        )
        assert.deepStrictEqual(
          yield* attachments.finalizeUpload({ ...identity, attemptId: part.attemptId }),
          { _tag: "UploadComplete" }
        )

        const encodedReference = yield* Schema.encodeEffect(Attachment.Reference)(reference)
        const value = {
          id: "message",
          title: "hello",
          count: 0,
          labels: [],
          attachment: encodedReference
        }
        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          entityKey: "\"message\"",
          value,
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        const valueJson = yield* Codec.stringify(value)
        const entityBytes = yield* Protocol.encodedBytesEffect({
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          key: "message",
          value
        })
        yield* sql`INSERT INTO effect_local_server_entities_data
        (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
        VALUES (${spaceId}, 0, ${Domain.Todo.name}, ${Domain.Todo.version}, ${"\"message\""},
          ${valueJson}, 0)`
        yield* sql`UPDATE effect_local_server_spaces
        SET entity_count = 1, entity_bytes = ${entityBytes}
        WHERE space_id = ${spaceId}`
        const grant = yield* attachments.prepareDownload({ ...identity, range: { offset: 1, length: 3 } })
        assert.strictEqual(grant.request.url, "https://objects.example/download")
        assert.deepStrictEqual(grant.slice, { offset: 1, length: 3 })
        assert.deepStrictEqual(grant.chunk, { index: 0, offset: 0, bytes: 5, digest })

        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          entityKey: "\"message\"",
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        yield* TestClock.adjust("90 seconds")
        yield* server.maintainAll
        assert.lengthOf(provider.deleted, 0)
        yield* TestClock.adjust("1 minute")
        yield* server.maintainAll
        assert.lengthOf(provider.deleted, 1)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "denies control grants before contacting the provider",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider } = yield* makeContext(false)
        const denied = yield* attachments.prepareUpload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(denied))
        if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
        assert.strictEqual(provider.uploaded.size, 0)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "denies finalization after access is revoked without contacting the provider",
    Effect.fnUntraced(
      function*() {
        const { attachments, authorization, provider } = yield* makeContext()
        const prepared = yield* attachments.prepareUpload(identity)
        assert.strictEqual(prepared._tag, "UploadPart")
        if (prepared._tag !== "UploadPart") return
        provider.uploaded.set(prepared.attemptId, 5)
        authorization.access = false

        const denied = yield* attachments.finalizeUpload({
          ...identity,
          attemptId: prepared.attemptId
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(denied))
        if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
        assert.strictEqual(provider.objects.size, 0)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "denies download when no current referencing entity passes read authorization",
    Effect.fnUntraced(
      function*() {
        const { attachments, authorization, provider, sql } = yield* makeContext()
        const prepared = yield* attachments.prepareUpload(identity)
        assert.strictEqual(prepared._tag, "UploadPart")
        if (prepared._tag !== "UploadPart") return
        provider.uploaded.set(prepared.attemptId, 5)
        yield* attachments.finalizeUpload({ ...identity, attemptId: prepared.attemptId })

        const encodedReference = yield* Schema.encodeEffect(Attachment.Reference)(reference)
        const value = {
          id: "denied-message",
          title: "denied",
          count: 0,
          labels: [],
          attachment: encodedReference
        }
        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          entityKey: "\"denied-message\"",
          value,
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        const valueJson = yield* Codec.stringify(value)
        yield* sql`INSERT INTO effect_local_server_entities_data
          (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
          VALUES (${spaceId}, 0, ${Domain.Todo.name}, ${Domain.Todo.version}, ${"\"denied-message\""},
            ${valueJson}, 0)`
        authorization.read = false

        const denied = yield* attachments.prepareDownload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(denied))
        if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")
        assert.lengthOf(provider.downloadRequests, 0)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "requires an independent proof upload from a second membership",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider } = yield* makeContext()
        const first = yield* attachments.prepareUpload(identity)
        assert.strictEqual(first._tag, "UploadPart")
        if (first._tag !== "UploadPart") return
        provider.uploaded.set(first.attemptId, 5)
        yield* attachments.finalizeUpload({ ...identity, attemptId: first.attemptId })

        const secondIdentity = {
          ...identity,
          clientId: otherClientId,
          membershipIncarnation: otherMembershipIncarnation
        }
        const second = yield* attachments.prepareUpload(secondIdentity)
        assert.strictEqual(second._tag, "UploadPart")
        assert.strictEqual(provider.uploaded.size, 2)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "fences an expired upload attempt before issuing a replacement",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider } = yield* makeContext()
        const first = yield* attachments.prepareUpload(identity)
        assert.strictEqual(first._tag, "UploadPart")
        if (first._tag !== "UploadPart") return
        yield* TestClock.adjust("2 minutes")
        assert.strictEqual(yield* attachments.sweepSpace(spaceId), 1)
        assert.strictEqual(yield* attachments.drainOutbox, 1)
        assert.deepStrictEqual(provider.aborted, [first.attemptId])

        const replacement = yield* attachments.prepareUpload(identity)
        assert.strictEqual(replacement._tag, "UploadPart")
        if (replacement._tag !== "UploadPart") return
        assert.notStrictEqual(replacement.attemptId, first.attemptId)
        const stale = yield* attachments.finalizeUpload({
          ...identity,
          attemptId: first.attemptId
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(stale))
        if (Result.isFailure(stale)) assert.strictEqual(stale.failure._tag, "AttachmentUnavailable")
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "rotates an upload attempt when the provider has lost its multipart state",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider } = yield* makeContext()
        const first = yield* attachments.prepareUpload(identity)
        assert.strictEqual(first._tag, "UploadPart")
        if (first._tag !== "UploadPart") return
        provider.missingUploads.add(first.attemptId)

        const missing = yield* attachments.prepareUpload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(missing))
        if (Result.isFailure(missing)) assert.strictEqual(missing.failure._tag, "AttachmentUnavailable")

        const replacement = yield* attachments.prepareUpload(identity)
        assert.strictEqual(replacement._tag, "UploadPart")
        if (replacement._tag !== "UploadPart") return
        assert.notStrictEqual(replacement.attemptId, first.attemptId)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "recovers the provider identity before sweeping an ambiguously begun upload",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider, sql } = yield* makeContext()
        yield* sql.unsafe(`CREATE TRIGGER fail_attachment_begin_commit
        BEFORE UPDATE OF provider_upload_id ON effect_local_server_attachment_attempts
        BEGIN SELECT RAISE(ABORT, 'injected begin commit failure'); END`)

        const failed = yield* attachments.prepareUpload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(failed))
        if (Result.isFailure(failed)) assert.strictEqual(failed.failure._tag, "StorageUnavailable")
        assert.strictEqual(provider.uploaded.size, 1)

        yield* sql.unsafe("DROP TRIGGER fail_attachment_begin_commit")
        yield* TestClock.adjust("2 minutes")
        assert.strictEqual(yield* attachments.sweepSpace(spaceId), 1)
        assert.strictEqual(yield* attachments.drainOutbox, 1)
        assert.lengthOf(provider.aborted, 1)
        assert.strictEqual(provider.aborted[0], [...provider.uploaded.keys()][0])
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "repairs a canonical object the provider no longer has",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider, sql } = yield* makeContext(true, {
          maximumObjectsPerSpace: 1,
          maximumBytesPerSpace: 5
        })
        const first = yield* attachments.prepareUpload(identity)
        assert.strictEqual(first._tag, "UploadPart")
        if (first._tag !== "UploadPart") return
        provider.uploaded.set(first.attemptId, 5)
        yield* attachments.finalizeUpload({ ...identity, attemptId: first.attemptId })

        const encodedReference = yield* Schema.encodeEffect(Attachment.Reference)(reference)
        const value = {
          id: "repair-message",
          title: "repair",
          count: 0,
          labels: [],
          attachment: encodedReference
        }
        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          entityKey: "\"repair-message\"",
          value,
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        const valueJson = yield* Codec.stringify(value)
        yield* sql`INSERT INTO effect_local_server_entities_data
        (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
        VALUES (${spaceId}, 0, ${Domain.Todo.name}, ${Domain.Todo.version}, ${"\"repair-message\""},
          ${valueJson}, 0)`
        const originalGrant = yield* attachments.prepareDownload(identity)
        provider.missingObjects.add(`object-${first.attemptId}`)
        const missing = yield* attachments.prepareDownload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(missing))
        if (Result.isFailure(missing)) assert.strictEqual(missing.failure._tag, "AttachmentUnavailable")

        const repair = yield* attachments.prepareUpload(identity)
        assert.strictEqual(repair._tag, "UploadPart")
        if (repair._tag !== "UploadPart") return
        provider.uploaded.set(repair.attemptId, 5)
        yield* attachments.finalizeUpload({ ...identity, attemptId: repair.attemptId })
        const repairedGrant = yield* attachments.prepareDownload(identity)
        assert.notStrictEqual(repairedGrant.objectVersion, originalGrant.objectVersion)
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "does not promote an object identity while its deletion is claimed",
    Effect.fnUntraced(
      function*() {
        const { attachments, provider, sql } = yield* makeContext()
        const prepared = yield* attachments.prepareUpload(identity)
        assert.strictEqual(prepared._tag, "UploadPart")
        if (prepared._tag !== "UploadPart") return
        provider.uploaded.set(prepared.attemptId, 5)
        yield* sql`INSERT INTO effect_local_server_attachment_deletions
        (outbox_id, space_id, digest, bytes, operation, provider_namespace, provider_id,
          provider_version, next_attempt_at, claim_token, claimed_until, created_at)
        VALUES ('claimed-object-delete', ${spaceId}, ${digest}, 5, 'DeleteObject', ${namespace},
          ${`object-${prepared.attemptId}`}, 'v1', 0, 'active-claim', 60000, 0)`

        const fenced = yield* attachments.finalizeUpload({ ...identity, attemptId: prepared.attemptId }).pipe(
          Effect.result
        )
        assert.isTrue(Result.isFailure(fenced))
        if (Result.isFailure(fenced)) assert.strictEqual(fenced.failure._tag, "AttachmentUploadBusy")

        yield* TestClock.adjust("2 minutes")
        const expiredClaim = yield* attachments.finalizeUpload({
          ...identity,
          attemptId: prepared.attemptId
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(expiredClaim))
        if (Result.isFailure(expiredClaim)) {
          assert.strictEqual(expiredClaim.failure._tag, "AttachmentUploadBusy")
        }
        assert.strictEqual(yield* attachments.drainOutbox, 1)
        assert.lengthOf(provider.deleted, 1)
        yield* TestClock.adjust("2 minutes")
        const deletedProof = yield* attachments.finalizeUpload({
          ...identity,
          attemptId: prepared.attemptId
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(deletedProof))
        if (Result.isFailure(deletedProof)) {
          assert.strictEqual(deletedProof.failure._tag, "AttachmentUnavailable")
        }

        const replacement = yield* attachments.prepareUpload(identity)
        assert.strictEqual(replacement._tag, "UploadPart")
        if (replacement._tag !== "UploadPart") return
        provider.uploaded.set(replacement.attemptId, 5)
        assert.deepStrictEqual(
          yield* attachments.finalizeUpload({ ...identity, attemptId: replacement.attemptId }),
          { _tag: "UploadComplete" }
        )
        const deletion = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_server_attachment_deletions WHERE outbox_id = 'claimed-object-delete'`
        assert.deepStrictEqual(deletion, [{ count: 0 }])
      },
      provideTestPlatform,
      Effect.scoped
    )
  )

  it.effect(
    "bounds provider cleanup while maintaining one thousand attachment spaces",
    Effect.fnUntraced(
      function*() {
        const { provider, server, sql } = yield* makeContext()
        const spaces = Array.from({ length: 1_000 }, (_, index) => ({
          spaceId: Identity.SpaceId.make(
            `spc_00000000-0000-4000-9000-${String(index + 1).padStart(12, "0")}`
          ),
          attemptId: `maintenance-attempt-${index + 1}`,
          physicalKey: (index + 1).toString(16).padStart(32, "0"),
          providerId: `maintenance-upload-${index + 1}`
        }))
        const insertSpace = Effect.fnUntraced(function*(space: (typeof spaces)[number]) {
          yield* sql`INSERT INTO effect_local_server_spaces
              (space_id, definition_hash, next_server_sequence, schema_version, schema_hash,
                schema_generation, next_terminal_sequence, history_floor, receipt_floor,
                retained_history_count, retained_receipt_count, entity_count, entity_bytes,
                snapshot_sequence, snapshot_terminal_sequence, metadata_verified)
              VALUES (${space.spaceId}, ${Domain.definition.hash}, 1,
                ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash},
                0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1)`
          yield* sql`INSERT INTO effect_local_server_attachment_attempts
              (attempt_id, space_id, digest, bytes, client_id, membership_incarnation,
                provider_namespace, physical_key, provider_upload_id, part_size, state,
                created_at, last_accessed_at)
              VALUES (${space.attemptId}, ${space.spaceId}, ${digest}, 5, ${clientId},
                ${membershipIncarnation}, ${namespace}, ${space.physicalKey}, ${space.providerId},
                5, 'Uploading', 0, 0)`
        })
        yield* Effect.forEach(spaces, insertSpace, { discard: true }).pipe(sql.withTransaction)

        yield* TestClock.adjust("2 minutes")
        const expiryStatements = yield* Ref.make<Array<string>>([])
        const transformer: Statement.Transformer = (statement) => {
          const [query] = statement.compile()
          if (
            query.startsWith("DELETE FROM effect_local_server_attachment_upload_grants") ||
            query.startsWith("DELETE FROM effect_local_server_attachment_download_grants")
          ) {
            return Ref.update(expiryStatements, (queries) => [...queries, query]).pipe(Effect.as(statement))
          }
          return Effect.succeed(statement)
        }
        yield* server.maintainAll.pipe(Effect.provideService(Statement.CurrentTransformer, transformer))
        assert.lengthOf(yield* Ref.get(expiryStatements), 2)
        assert.lengthOf(provider.aborted, 8)
        for (const request of provider.abortSpaces) {
          const expected = spaces.find((space) => space.providerId === request.uploadId)
          assert.isDefined(expected)
          assert.strictEqual(request.spaceId, expected.spaceId)
        }
        const remainingAttempts = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_attachment_attempts WHERE attempt_id LIKE 'maintenance-attempt-%'`
        assert.deepStrictEqual(remainingAttempts, [{ count: 0 }])
        const remainingOutbox = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_attachment_deletions WHERE operation = 'AbortUpload'`
        assert.deepStrictEqual(remainingOutbox, [{ count: 992 }])

        yield* server.maintainAll
        assert.lengthOf(provider.aborted, 16)
      },
      provideTestPlatform,
      Effect.scoped
    ),
    30_000
  )
})
