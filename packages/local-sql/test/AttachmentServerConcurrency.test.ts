import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as AttachmentObjectStore from "../src/AttachmentObjectStore.js"
import * as AttachmentServer from "../src/AttachmentServer.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const deletionDigest = Attachment.Digest.make(
  "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
)
const uploadDigest = Attachment.Digest.make(
  "sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
)
const namespace = AttachmentObjectStore.Namespace.make("concurrency-test")

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

type DeleteHandler = (
  object: AttachmentObjectStore.ObjectIdentity
) => Effect.Effect<void, AttachmentObjectStore.AttachmentObjectStoreUnavailable>

const makeProvider = () => {
  let deleteHandler: DeleteHandler = () => Effect.void
  const deleted: Array<AttachmentObjectStore.ObjectIdentity> = []
  const uploads = new Map<string, Attachment.Reference>()
  const adapter: AttachmentObjectStore.Adapter = {
    namespace,
    beginUpload: ({ attemptId, reference }) => {
      uploads.set(attemptId, reference)
      return Effect.succeed(AttachmentObjectStore.BegunUpload.make({
        upload: AttachmentObjectStore.UploadIdentity.make({
          namespace,
          id: AttachmentObjectStore.ProviderId.make(attemptId)
        }),
        partSize: Math.max(1, reference.bytes)
      }))
    },
    listUploadedParts: () =>
      Effect.succeed(AttachmentObjectStore.UploadedPartPage.make({
        parts: [],
        nextPartNumber: null
      })),
    grantUploadPart: ({ expiresAt }) =>
      Effect.succeed(AttachmentObjectStore.DirectUploadGrant.make({
        expiresAt,
        request: AttachmentTransfer.DirectUploadRequest.make({
          method: "PUT",
          url: AttachmentTransfer.GrantUrl.make("https://objects.example/upload"),
          headers: []
        })
      })),
    finalizeUpload: ({ reference, upload }) =>
      Effect.succeed(AttachmentObjectStore.VerifiedObject.make({
        object: AttachmentObjectStore.ObjectIdentity.make({
          namespace,
          id: AttachmentObjectStore.ProviderId.make(`object-${upload.id}`),
          version: AttachmentObjectStore.ProviderVersion.make("v1")
        }),
        reference,
        chunkBytes: Math.max(1, reference.bytes),
        chunkCount: Math.ceil(reference.bytes / Math.max(1, reference.bytes))
      })),
    inspectFinalized: () => Effect.succeed(null),
    listVerifiedChunks: () =>
      Effect.succeed(AttachmentObjectStore.VerifiedChunkPage.make({
        chunks: [],
        nextIndex: null
      })),
    grantDownload: ({ expiresAt }) =>
      Effect.succeed(AttachmentObjectStore.DirectDownloadGrant.make({
        expiresAt,
        request: AttachmentTransfer.DirectDownloadRequest.make({
          method: "GET",
          url: AttachmentTransfer.GrantUrl.make("https://objects.example/download"),
          headers: []
        })
      })),
    abortUpload: () => Effect.void,
    deleteObject: Effect.fnUntraced(function*({ object }) {
      deleted.push(object)
      yield* deleteHandler(object)
    })
  }
  return {
    adapter,
    deleted,
    setDeleteHandler: (handler: DeleteHandler) => {
      deleteHandler = handler
    }
  }
}

const attachmentOptions = (maximumBytesPerSpace: number): AttachmentServer.Options => ({
  maximumObjectBytes: 8,
  maximumObjectsPerSpace: 4,
  maximumBytesPerSpace,
  maximumReferencesPerObject: 4,
  uploadGrantLifetime: "1 minute",
  uploadLeaseLifetime: "1 minute",
  readLeaseLifetime: "1 minute",
  stagingLifetime: "1 minute",
  garbageCollectionGracePeriod: "1 minute",
  deletionBatchSize: 8,
  verificationChunkBytes: 5,
  deletionRetryDelay: "1 second",
  authorizeAccess: () => Effect.void,
  authorizeUpload: () => Effect.void,
  authorizeRead: () => Effect.void
})

const makeHarness = Effect.fnUntraced(function*(maximumBytesPerSpace = 32) {
  const fs = yield* FileSystem.FileSystem
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachment-outbox-" })
  const layerDatabase = SqliteClient.layer({ filename: `${root}/server.sqlite`, disableWAL: true }).pipe(
    Layer.provide(Reactivity.layer)
  )
  const provider = makeProvider()
  const layerObjectStore = AttachmentObjectStore.layer({
    namespaceForNewObjects: namespace,
    adapters: [provider.adapter]
  })
  const layerInfrastructure = Layer.mergeAll(layerDatabase, layerObjectStore, NodeCrypto.layer)
  const options = attachmentOptions(maximumBytesPerSpace)
  const layerInitializationAttachments = AttachmentServer.layer(options).pipe(
    Layer.provide(layerInfrastructure)
  )
  const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
  const layerServer = ServerStore.layerTrusted(history).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerInitializationAttachments),
    Layer.provide(layerDatabase),
    Layer.provide(NodeCrypto.layer)
  )
  const initializationContext = yield* Layer.mergeAll(
    layerInfrastructure,
    layerInitializationAttachments,
    layerServer
  ).pipe(Layer.build)
  const server = Context.get(initializationContext, ServerStore.ServerStore)
  yield* server.pull({
    spaceId,
    clientId,
    schema: Domain.definition.schemaIdentity,
    scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor: null,
    limit: 10
  })

  const sql = Context.get(initializationContext, SqlClient.SqlClient)
  const stores = Context.get(initializationContext, AttachmentObjectStore.AttachmentObjectStore)
  const layerSharedInfrastructure = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, sql),
    Layer.succeed(AttachmentObjectStore.AttachmentObjectStore, stores),
    NodeCrypto.layer
  )
  const buildRuntime = AttachmentServer.layer(options).pipe(
    Layer.provide(layerSharedInfrastructure),
    Layer.build,
    Effect.map(Context.get(AttachmentServer.AttachmentServer))
  )
  const first = yield* buildRuntime
  const second = yield* buildRuntime
  return { first, provider, second, sql }
})

const insertDeletion = (
  sql: SqlClient.SqlClient,
  outboxId: string,
  bytes = 5
) =>
  sql`INSERT INTO effect_local_server_attachment_deletions
    (outbox_id, space_id, digest, bytes, operation, provider_namespace,
      provider_id, provider_version, next_attempt_at, created_at)
    VALUES (${outboxId}, ${spaceId}, ${deletionDigest}, ${bytes}, 'DeleteObject', ${namespace},
      ${AttachmentObjectStore.ProviderId.make(`object-${outboxId}`)},
      ${AttachmentObjectStore.ProviderVersion.make("v1")}, 0, 0)`

const deletionState = (sql: SqlClient.SqlClient, outboxId: string) =>
  SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: Schema.Struct({
      attempt_count: Schema.Int,
      next_attempt_at: Schema.Int,
      claim_token: Schema.NullOr(Schema.String),
      claimed_until: Schema.NullOr(Schema.Int)
    }),
    execute: () =>
      sql`SELECT attempt_count, next_attempt_at, claim_token, claimed_until
        FROM effect_local_server_attachment_deletions WHERE outbox_id = ${outboxId}`
  })(undefined)

const usageState = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: Schema.Struct({ object_count: Schema.Int, byte_count: Schema.Int }),
    execute: () =>
      sql`SELECT object_count, byte_count FROM effect_local_server_attachment_usage
        WHERE space_id = ${spaceId}`
  })(undefined)

const identity = {
  spaceId,
  clientId,
  membershipIncarnation,
  reference: Attachment.Reference.make({ digest: uploadDigest, bytes: 1 }),
  principal: { user: "owner" }
}

const layerPlatform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const providePlatform = Effect.provide(layerPlatform)

describe("delegated attachment deletion concurrency", () => {
  it.effect(
    "globally claims one outbox row across independent server runtimes",
    Effect.fnUntraced(
      function*() {
        const { first, provider, second, sql } = yield* makeHarness()
        yield* insertDeletion(sql, "delete-concurrent")
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        provider.setDeleteHandler(() =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
        )

        const firstDrain = yield* first.drainOutbox.pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        assert.strictEqual(yield* second.drainOutbox, 0)
        assert.lengthOf(provider.deleted, 1)
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual(yield* Fiber.join(firstDrain), 1)
        assert.isTrue(Option.isNone(yield* deletionState(sql, "delete-concurrent")))
      },
      providePlatform,
      Effect.scoped
    )
  )

  it.effect(
    "backs off a provider failure and retries only when due",
    Effect.fnUntraced(
      function*() {
        const { first, provider, sql } = yield* makeHarness()
        yield* insertDeletion(sql, "delete-retry")
        let fail = true
        provider.setDeleteHandler(() => {
          if (!fail) return Effect.void
          fail = false
          return Effect.fail(
            new AttachmentObjectStore.AttachmentObjectStoreUnavailable({
              namespace,
              operation: "delete.injected"
            })
          )
        })

        assert.strictEqual(yield* first.drainOutbox, 0)
        const failed = yield* deletionState(sql, "delete-retry")
        assert.isTrue(Option.isSome(failed))
        if (failed._tag === "Some") {
          assert.deepStrictEqual(failed.value, {
            attempt_count: 1,
            next_attempt_at: 1_000,
            claim_token: null,
            claimed_until: null
          })
        }
        assert.strictEqual(yield* first.drainOutbox, 0)
        assert.lengthOf(provider.deleted, 1)
        yield* TestClock.adjust("999 millis")
        assert.strictEqual(yield* first.drainOutbox, 0)
        assert.lengthOf(provider.deleted, 1)
        yield* TestClock.adjust("1 millis")
        assert.strictEqual(yield* first.drainOutbox, 1)
        assert.lengthOf(provider.deleted, 2)
        assert.isTrue(Option.isNone(yield* deletionState(sql, "delete-retry")))
      },
      providePlatform,
      Effect.scoped
    )
  )

  it.effect(
    "recovers after provider success precedes SQL acknowledgement",
    Effect.fnUntraced(
      function*() {
        const { first, provider, second, sql } = yield* makeHarness()
        yield* insertDeletion(sql, "delete-ack-recovery")
        yield* sql.unsafe(`CREATE TRIGGER fail_attachment_delete_ack
        BEFORE DELETE ON effect_local_server_attachment_deletions
        BEGIN SELECT RAISE(ABORT, 'injected deletion acknowledgement failure'); END`)

        const failed = yield* first.drainOutbox.pipe(Effect.result)
        assert.isTrue(Result.isFailure(failed))
        if (Result.isFailure(failed)) assert.strictEqual(failed.failure._tag, "StorageUnavailable")
        assert.lengthOf(provider.deleted, 1)
        const claimed = yield* deletionState(sql, "delete-ack-recovery")
        assert.isTrue(Option.isSome(claimed))
        if (claimed._tag === "Some") {
          assert.isNotNull(claimed.value.claim_token)
          assert.strictEqual(claimed.value.claimed_until, 60_000)
        }

        yield* sql.unsafe("DROP TRIGGER fail_attachment_delete_ack")
        assert.strictEqual(yield* second.drainOutbox, 0)
        yield* TestClock.adjust("1 minute")
        assert.strictEqual(yield* second.drainOutbox, 1)
        assert.lengthOf(provider.deleted, 2)
        assert.isTrue(Option.isNone(yield* deletionState(sql, "delete-ack-recovery")))
      },
      providePlatform,
      Effect.scoped
    )
  )

  it.effect(
    "keeps physical occupancy charged until provider acknowledgement",
    Effect.fnUntraced(
      function*() {
        const { first, provider, sql } = yield* makeHarness(5)
        yield* insertDeletion(sql, "delete-quota", 5)
        let fail = true
        provider.setDeleteHandler(() => {
          if (!fail) return Effect.void
          fail = false
          return Effect.fail(
            new AttachmentObjectStore.AttachmentObjectStoreUnavailable({
              namespace,
              operation: "delete.injected"
            })
          )
        })

        assert.deepStrictEqual(yield* usageState(sql), { object_count: 1, byte_count: 5 })
        const beforeDelete = yield* first.prepareUpload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(beforeDelete))
        if (Result.isFailure(beforeDelete)) assert.strictEqual(beforeDelete.failure._tag, "CapacityExceeded")

        assert.strictEqual(yield* first.drainOutbox, 0)
        assert.deepStrictEqual(yield* usageState(sql), { object_count: 1, byte_count: 5 })
        const beforeAcknowledgement = yield* first.prepareUpload(identity).pipe(Effect.result)
        assert.isTrue(Result.isFailure(beforeAcknowledgement))
        if (Result.isFailure(beforeAcknowledgement)) {
          assert.strictEqual(beforeAcknowledgement.failure._tag, "CapacityExceeded")
        }

        yield* TestClock.adjust("1 second")
        assert.strictEqual(yield* first.drainOutbox, 1)
        assert.deepStrictEqual(yield* usageState(sql), { object_count: 0, byte_count: 0 })
        const afterAcknowledgement = yield* first.prepareUpload(identity)
        assert.strictEqual(afterAcknowledgement._tag, "UploadPart")
      },
      providePlatform,
      Effect.scoped
    )
  )
})
