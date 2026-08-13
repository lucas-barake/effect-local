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
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
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

const collect = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

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
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerAttachments = AttachmentServer.layer({
          maximumObjectBytes: 16,
          maximumObjectsPerSpace: 8,
          maximumBytesPerSpace: 64,
          uploadGrantLifetime: "1 minute",
          uploadLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 minute",
          deletionBatchSize: 8,
          authorizeAccess: () => Effect.void,
          authorizeUpload: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(Layer.provide(layerInfrastructure))
        const layerRuntime = MutationRuntime.layer(definition)
        const layerServer = ServerStore.layerTrusted(history).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(layerDatabase),
          Layer.provide(NodeCrypto.layer)
        )
        const services = yield* Layer.mergeAll(layerInfrastructure, layerAttachments, layerServer).pipe(Layer.build)
        const server = Context.get(services, ServerStore.ServerStore)
        const attachments = Context.get(services, AttachmentServer.AttachmentServer)
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
        const fetch =
          ((input: URL | RequestInfo, init?: RequestInit) =>
            web.handler(new Request(input, init))) satisfies typeof globalThis.fetch
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
        const prefix = yield* attachments.prepareUpload({ ...identity, principal: { subject: "reader" } })
        yield* attachments.appendUpload({
          ...identity,
          principal: { subject: "reader" },
          expectedOffset: prefix.offset,
          bytes: Stream.make(bytes.slice(0, 2))
        })
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

        const denied = yield* collect(transfer.download(identity)).pipe(
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
          value: entityValue
        })
        const valueJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(entityValue)
        const sql = Context.get(services, SqlClient.SqlClient)
        yield* sql`INSERT INTO effect_local_server_entities_data
      (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
      VALUES (${spaceId}, 0, ${Message.name}, ${Message.version}, ${"\"message\""},
        ${valueJson}, 1)`

        assert.deepStrictEqual(
          yield* collect(transfer.download(identity)).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch)),
          bytes
        )
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
