import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"
import * as Codec from "../src/internal/codec.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const principal = { user: "reader" }
const hello = Uint8Array.from([104, 101, 108, 108, 111])
const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)

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

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("attachment server", () => {
  it.effect(
    "resumes uploads, authorizes lazy reads, and collects the last reference",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-server-" })
        const layerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true }).pipe(
          Layer.provide(Reactivity.layer)
        )
        const layerStorage = FileSystemAttachmentStorage.layer({
          directory: `${root}/objects`,
          maximumBytes: 8
        })
        const layerInfrastructure = Layer.merge(layerDatabase, layerStorage)
        const layerAttachments = AttachmentServer.layer({
          maximumObjectBytes: 8,
          maximumObjectsPerSpace: 1,
          maximumBytesPerSpace: 32,
          uploadGrantLifetime: "1 minute",
          uploadLeaseLifetime: "1 minute",
          stagingLifetime: "1 day",
          garbageCollectionGracePeriod: "1 minute",
          deletionBatchSize: 8,
          authorizeAccess: () => Effect.void,
          authorizeUpload: ({ principal: requestedPrincipal }) => {
            if (requestedPrincipal === "denied-upload") {
              return Effect.fail(new Denied({ reason: "denied" }))
            }
            return Effect.void
          },
          authorizeRead: ({ principal: requestedPrincipal }) => {
            if (requestedPrincipal === "denied") {
              return Effect.fail(new Denied({ reason: "denied" }))
            }
            return Effect.void
          }
        }).pipe(Layer.provide(layerInfrastructure))
        const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
        const layerServer = ServerStore.layerTrusted(history).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(layerDatabase),
          Layer.provide(NodeCrypto.layer)
        )
        const context = yield* Layer.mergeAll(layerInfrastructure, layerAttachments, layerServer).pipe(Layer.build)
        const attachments = Context.get(context, AttachmentServer.AttachmentServer)
        const storage = Context.get(context, AttachmentStorage.AttachmentStorage)
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
        const reference = Attachment.Reference.make({
          _tag: "Attachment",
          digest: Attachment.Digest.make(
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
          ),
          bytes: hello.length
        })
        const identity = { spaceId, clientId, membershipIncarnation, reference, principal }
        const deniedUpload = yield* attachments.prepareUpload({
          ...identity,
          principal: "denied-upload"
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(deniedUpload))
        if (Result.isFailure(deniedUpload)) {
          assert.strictEqual(deniedUpload.failure._tag, "AuthorizationDenied")
        }
        const prefix = hello.slice(0, 2)
        const interrupted = Stream.concat(
          Stream.make(prefix),
          Stream.fail(new ReplicaError.ServerUnavailable())
        )

        const first = yield* attachments.prepareUpload(identity)
        const interruptedUpload = attachments.appendUpload({
          ...identity,
          expectedOffset: first.offset,
          bytes: interrupted
        })
        const failure = yield* interruptedUpload.pipe(Effect.result)
        assert.isTrue(Result.isFailure(failure))
        if (Result.isFailure(failure)) assert.strictEqual(failure.failure._tag, "ServerUnavailable")
        const resumed = yield* attachments.prepareUpload(identity)
        assert.strictEqual(resumed.offset, 2)
        const complete = yield* attachments.appendUpload({
          ...identity,
          expectedOffset: resumed.offset,
          bytes: Stream.make(hello.slice(2))
        })
        assert.isTrue(complete.complete)
        const otherReference = Attachment.Reference.make({
          _tag: "Attachment",
          digest: Attachment.Digest.make(
            "sha256:486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"
          ),
          bytes: hello.length
        })
        const quota = yield* attachments.prepareUpload({
          ...identity,
          reference: otherReference
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(quota))
        if (Result.isFailure(quota)) assert.strictEqual(quota.failure._tag, "CapacityExceeded")

        const encodedReference = yield* Schema.encodeEffect(Attachment.Reference)(reference)
        const entityValue = {
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
          value: entityValue
        })
        const sql = Context.get(context, SqlClient.SqlClient)
        const valueJson = yield* Codec.stringify(entityValue)
        yield* sql`INSERT INTO effect_local_server_entities_data
          (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
          VALUES (${spaceId}, 0, ${Domain.Todo.name}, ${Domain.Todo.version}, ${"\"message\""},
            ${valueJson}, 1)`
        assert.deepStrictEqual(
          yield* collectBytes(attachments.read({ ...identity, range: { offset: 1, length: 3 } })),
          hello.slice(1, 4)
        )
        const deniedRead = collectBytes(attachments.read({ ...identity, principal: "denied" }))
        const denied = yield* deniedRead.pipe(Effect.result)
        assert.isTrue(Result.isFailure(denied))
        if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "AuthorizationDenied")

        yield* attachments.replaceEntityReferences({
          spaceId,
          schemaGeneration: 0,
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          entityKey: "\"message\""
        })
        yield* TestClock.adjust("2 minutes")
        assert.strictEqual(yield* attachments.maintain(spaceId), 1)
        assert.strictEqual(yield* storage.exists(first.objectKey), false)
      },
      provideNodeServices,
      Effect.scoped
    )
  )
})
