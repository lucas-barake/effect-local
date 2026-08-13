import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as AttachmentServer from "@lucas-barake/effect-local-sql/AttachmentServer"
import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as AttachmentTransfer from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import * as FileSystemAttachmentStorage from "@lucas-barake/effect-local-sql/FileSystemAttachmentStorage"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentHttpClient from "../src/AttachmentHttpClient.js"
import * as AttachmentHttpServer from "../src/AttachmentHttpServer.js"
import * as Authentication from "../src/Authentication.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, attachment: Attachment.Reference })
})
const definition = Definition.make({ version: 1, models: [Message], mutations: [] })
const bytes = Uint8Array.from([104, 101, 108, 108, 111])
const reference = Attachment.Reference.make({
  _tag: "Attachment",
  digest: Attachment.Digest.make(
    "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
  ),
  bytes: bytes.length
})

const history = {
  definition,
  retainedHistoryEntries: 16,
  maximumHistoryEntries: 128,
  retainedReceipts: 16,
  maximumReceipts: 128,
  maximumSnapshotEntities: 128,
  maximumSnapshotBytes: 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 16,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 16,
  maximumWatchersPerSpace: 16,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 4,
  maximumPendingReadAuthorizations: 16,
  readAuthorizationCacheCapacity: 16,
  migration: { retryDelay: "1 millis", maximumAttempts: 2 }
} as const

const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)

describe("attachment HTTP transport", () => {
  it.effect(
    "uploads and lazily downloads through the production byte plane",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-http-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true }).pipe(
          Layer.provide(Reactivity.layer)
        )
        const layerStorage = FileSystemAttachmentStorage.layer({ directory: `${root}/objects`, maximumBytes: 16 })
        const storageContext = yield* layerStorage.pipe(Layer.build)
        const storage = Context.get(storageContext, AttachmentStorage.AttachmentStorage)
        const readStarted = yield* Latch.make()
        const releaseRead = yield* Latch.make()
        const gatedStorage = AttachmentStorage.AttachmentStorage.of({
          ...storage,
          read: (objectKey, requestedReference, range) =>
            Stream.unwrap(Effect.gen(function*() {
              yield* readStarted.open
              yield* releaseRead.await
              return storage.read(objectKey, requestedReference, range)
            }))
        })
        const layerInfrastructure = Layer.merge(
          layerDatabase,
          Layer.succeed(AttachmentStorage.AttachmentStorage, gatedStorage)
        )
        const attachmentOptions = {
          maximumObjectBytes: 16,
          maximumObjectsPerSpace: 8,
          maximumBytesPerSpace: 64,
          maximumReferencesPerObject: 8,
          uploadGrantLifetime: "1 minute",
          uploadLeaseLifetime: "1 minute",
          readLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 minute",
          deletionBatchSize: 8,
          authorizeAccess: () => Effect.void,
          authorizeUpload: () => Effect.void,
          authorizeRead: () => Effect.void
        } as const
        const layerAttachments = AttachmentServer.layer(attachmentOptions).pipe(Layer.provide(layerInfrastructure))
        const layerRuntime = MutationRuntime.layer(definition)
        const layerServer = ServerStore.layerTrusted(history).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(layerDatabase),
          Layer.provide(NodeCrypto.layer)
        )
        const services = yield* Layer.mergeAll(layerInfrastructure, layerAttachments, layerServer).pipe(Layer.build)
        const server = Context.get(services, ServerStore.ServerStore)
        const attachments = Context.get(services, AttachmentServer.AttachmentServer)
        const sql = Context.get(services, SqlClient.SqlClient)
        yield* server.pull({
          spaceId,
          clientId,
          schema: definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: [Message.name] }),
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null,
          limit: 8
        })

        const authenticator = Authentication.Authenticator.of({
          authenticate: (credential) => {
            if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "reader" })
            return Effect.fail(new ReplicaError.CredentialRejected())
          }
        })
        const layerRoute = AttachmentHttpServer.layer().pipe(
          Layer.provide(Layer.succeed(AttachmentServer.AttachmentServer, attachments)),
          Layer.provide(Layer.succeed(Authentication.Authenticator, authenticator))
        )
        const web = HttpRouter.toWebHandler(layerRoute, { disableLogger: true })
        yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
        let downloadCacheControl: string | null | undefined
        const requests: Array<{ readonly method: string; readonly uploadOffset: string | null }> = []
        const fetch = ((input: URL | RequestInfo, init?: RequestInit) => {
          const request = new Request(input, init)
          requests.push({ method: request.method, uploadOffset: request.headers.get("upload-offset") })
          return web.handler(request).then((response) => {
            if (request.method === "GET") downloadCacheControl = response.headers.get("cache-control")
            return response
          })
        }) satisfies typeof globalThis.fetch
        const provider = Authentication.CredentialProvider.of({
          acquire: Effect.succeed({ generation: 0, bearer: Redacted.make("secret") }),
          awaitChange: () => Effect.never
        })
        const transfer = yield* AttachmentHttpClient.layer({ baseUrl: "http://effect-local.test" }).pipe(
          Layer.provide(FetchHttpClient.layer),
          Layer.provide(Layer.succeed(Authentication.CredentialProvider, provider)),
          Layer.build,
          Effect.map(Context.get(AttachmentTransfer.AttachmentTransfer)),
          Effect.provideService(FetchHttpClient.Fetch, fetch)
        )
        const identity = { spaceId, clientId, membershipIncarnation, reference }
        let emittedPrefix = false
        const interruptedBody = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (!emittedPrefix) {
              emittedPrefix = true
              controller.enqueue(bytes.slice(0, 2))
              return
            }
            controller.error("connection interrupted")
          }
        })
        const attachmentUrl = `http://effect-local.test/effect-local/attachments/${spaceId}/${reference.digest}`
        const attachmentHeaders = {
          authorization: "Bearer secret",
          "x-effect-local-client-id": clientId,
          "x-effect-local-membership-incarnation": membershipIncarnation,
          "x-effect-local-attachment-bytes": String(reference.bytes)
        }
        const initial = yield* Effect.promise(() =>
          fetch(attachmentUrl, { method: "HEAD", headers: attachmentHeaders })
        )
        assert.strictEqual(initial.status, 204)
        assert.strictEqual(initial.headers.get("upload-offset"), "0")
        const interruptedInit = {
          method: "PATCH",
          headers: {
            ...attachmentHeaders,
            "upload-offset": "0"
          },
          body: interruptedBody,
          duplex: "half"
        } satisfies RequestInit & { readonly duplex: "half" }
        const interrupted = yield* Effect.promise(() => fetch(attachmentUrl, interruptedInit))
        assert.strictEqual(interrupted.status, 503)
        requests.length = 0
        let resumedAt = -1
        yield* transfer.upload({
          ...identity,
          bytes: (offset) => {
            resumedAt = offset
            return Stream.make(bytes.slice(offset))
          }
        }).pipe(
          Effect.provideService(FetchHttpClient.Fetch, fetch)
        )
        assert.strictEqual(resumedAt, 2)
        assert.deepStrictEqual(requests.slice(0, 2), [
          { method: "HEAD", uploadOffset: null },
          { method: "PATCH", uploadOffset: "2" }
        ])
        yield* sql`DELETE FROM effect_local_server_attachment_upload_grants WHERE space_id = ${spaceId}`

        const denied = yield* Stream.mkUint8Array(transfer.download(identity)).pipe(
          Effect.provideService(FetchHttpClient.Fetch, fetch),
          Effect.result
        )
        assert.isTrue(Result.isFailure(denied))
        if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")

        const encodedReference = yield* Schema.encodeEffect(Attachment.Reference)(reference)
        const entityValue = { id: "message", attachment: encodedReference }
        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Message.name,
          modelVersion: Message.version,
          entityKey: "\"message\"",
          value: entityValue,
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        const valueJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(entityValue)
        yield* sql`INSERT INTO effect_local_server_entities_data
      (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
      VALUES (${spaceId}, 0, ${Message.name}, ${Message.version}, ${"\"message\""},
        ${valueJson}, 1)`

        const download = yield* Stream.mkUint8Array(transfer.download(identity)).pipe(
          Effect.provideService(FetchHttpClient.Fetch, fetch),
          Effect.forkChild
        )
        yield* readStarted.await
        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Message.name,
          modelVersion: Message.version,
          entityKey: "\"message\"",
          authority: { _tag: "Mutation", clientId, membershipIncarnation }
        })
        const layerSecondStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 16
        })
        const layerSecondInfrastructure = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient, sql),
          layerSecondStorage,
          NodeCrypto.layer
        )
        const secondContext = yield* AttachmentServer.layer(attachmentOptions).pipe(
          Layer.provide(layerSecondInfrastructure),
          Layer.build
        )
        const secondServer = Context.get(secondContext, AttachmentServer.AttachmentServer)
        yield* TestClock.adjust("2 minutes")
        assert.strictEqual(yield* secondServer.maintain(spaceId), 0)
        yield* releaseRead.open
        assert.deepStrictEqual(yield* Fiber.join(download), bytes)
        assert.strictEqual(downloadCacheControl, "private, no-store")
        const leases = yield* sql<{ readonly expires_at: number }>`SELECT expires_at
          FROM effect_local_server_attachment_read_leases WHERE space_id = ${spaceId}`
        assert.lengthOf(leases, 0)
        const candidates = yield* sql<{
          readonly garbage_collect_after: number | null
          readonly reference_count: number
          readonly grant_count: number
        }>`SELECT o.garbage_collect_after,
            (SELECT COUNT(*) FROM effect_local_server_attachment_references r
              WHERE r.space_id = o.space_id AND r.digest = o.digest) AS reference_count,
            (SELECT COUNT(*) FROM effect_local_server_attachment_upload_grants g
              WHERE g.space_id = o.space_id AND g.digest = o.digest) AS grant_count
          FROM effect_local_server_attachment_objects o WHERE o.space_id = ${spaceId}`
        assert.deepStrictEqual(
          candidates.map(({ grant_count, reference_count }) => ({ grant_count, reference_count })),
          [{
            grant_count: 0,
            reference_count: 0
          }]
        )
        assert.isNotNull(candidates[0].garbage_collect_after)
        yield* TestClock.adjust("2 minutes")
        const maintenanceNow = yield* Clock.currentTimeMillis
        assert.isAtMost(candidates[0].garbage_collect_after, maintenanceNow)
        assert.strictEqual(yield* secondServer.maintain(spaceId), 1)
        const storageService = Context.get(services, AttachmentStorage.AttachmentStorage)
        const prepared = yield* attachments.prepareUpload({ ...identity, principal: { subject: "reader" } })
        const object = yield* storageService.exists(prepared.objectKey)
        assert.isTrue(object)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
