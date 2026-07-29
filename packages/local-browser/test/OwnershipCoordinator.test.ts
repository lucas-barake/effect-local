import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import { RpcClient } from "effect/unstable/rpc"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Worker from "effect/unstable/workers/Worker"
import * as NodeFs from "node:fs"
import * as NodeOs from "node:os"
import * as NodePath from "node:path"
import * as OwnershipProtocol from "../src/internal/ownershipProtocol.js"
import * as OwnershipCoordinator from "../src/OwnershipCoordinator.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { Task } from "./fixtures.js"

const definition = ReplicaDefinition.make({
  name: "ownership-test",
  documents: DocumentSet.make(Task),
  mutations: [],
  projections: [],
  queries: []
})

const limits = {
  maxBackupBytes: 1024,
  maxChunkBytes: 128,
  maxArchiveRecords: 100,
  maxJsonDepth: 16,
  maxConflictDepth: 16,
  maxConflictNodes: 10_000,
  maxConflictAlternatives: 1_000,
  maxConflictPathSegments: 16,
  maxConflictValueBytes: 1024 * 1024,
  maxConflictSourceChanges: 10_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
  maxSyncMessageBytes: 1024,
  maxPeerSendMillis: 1_000,
  maxSyncChangesPerMessage: 10,
  maxSyncDependencyEdgesPerMessage: 20,
  maxSyncOperationsPerMessage: 100,
  maxPendingBytesPerDocument: 1024,
  maxPendingBytesPerPeer: 2048,
  maxPendingBytesPerReplica: 4096,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 10,
  maxPendingChangesPerPeer: 20,
  maxPendingChangesPerReplica: 40,
  maxPendingDependencyEdgesPerDocument: 100,
  maxPendingDependencyEdgesPerPeer: 200,
  maxPendingDependencyEdgesPerReplica: 400,
  maxSessions: 8,
  maxStreamsPerSession: 2,
  maxInFlightPerSession: 2,
  maxQueuedRpc: 4,
  maxQueuedPermits: 4,
  maxActiveRestores: 4,
  maxRestoresPerSession: 2,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
} satisfies ReplicaLimits.Values

const MetadataRow = Schema.Struct({
  replica_id: Schema.String,
  writer_generation: Schema.Int
})

const OwnerInfo = Schema.Struct({
  replicaId: Schema.String,
  writerGeneration: Schema.Int
})
type OwnerInfo = typeof OwnerInfo.Type

const ownerInfo = {
  schema: OwnerInfo,
  make: Effect.flatMap(SqlClient.SqlClient, (sql) =>
    SqlSchema.findOne({
      Request: Schema.Void,
      Result: MetadataRow,
      execute: () => sql`SELECT replica_id, writer_generation FROM effect_local_metadata WHERE singleton = 1`
    })(undefined).pipe(
      Effect.map((row) => ({ replicaId: row.replica_id, writerGeneration: row.writer_generation }))
    ))
} satisfies OwnershipCoordinator.SharedWorkerOptions<never, OwnerInfo, unknown>["info"]

interface StartedEngine {
  readonly sqlite: ManagedRuntime.ManagedRuntime<SqliteClient.SqliteClient | SqlClient.SqlClient, never>
  readonly runtime: ManagedRuntime.ManagedRuntime<OwnershipCoordinator.EngineServices, unknown>
}

const makeEngineFactory = (
  started: Array<StartedEngine>,
  filename: string
) => {
  const engine = (databasePort: MessagePort) => {
    void databasePort
    const sqlite = ManagedRuntime.make(SqliteClient.layer({ filename, disableWAL: true }))
    const EngineLive = Layer.merge(
      SqlReplica.layerWithBindings(definition, { projections: [] }),
      SessionManager.layer
    ).pipe(
      Layer.provideMerge(Layer.mergeAll(
        Layer.unwrap(Effect.map(sqlite.contextEffect, Layer.succeedContext)),
        NodeCrypto.layer,
        ReplicaLimits.layer(limits)
      ))
    )
    const runtime = ManagedRuntime.make(EngineLive)
    started.push({ sqlite, runtime })
    return runtime
  }
  return engine
}

const coordinatorLayer = (
  started: Array<StartedEngine>,
  filename = ":memory:"
) =>
  OwnershipCoordinator.layerSharedWorker({
    name: "effect-local-ownership-test",
    definition,
    engine: makeEngineFactory(started, filename),
    info: ownerInfo,
    provisionTimeout: "500 millis",
    engineStartTimeout: "5 seconds",
    engineDisposeTimeout: "100 millis",
    healthCheck: { interval: "100 millis", timeout: "500 millis" }
  })

interface TestTab {
  readonly controlPort: MessagePort
  readonly receivedTags: Array<string>
  readonly frames: Queue.Queue<OwnershipProtocol.OwnerToPageFrame>
  readonly rpcChannel: MessageChannel
}

const postToOwner = (tab: TestTab, frame: OwnershipProtocol.PageToOwnerFrame) => {
  tab.controlPort.postMessage(
    Schema.encodeSync(OwnershipProtocol.PageToOwnerFrame)(frame),
    [...OwnershipProtocol.transferOf(frame)]
  )
}

const attachTab = Effect.gen(function*() {
  const coordinator = yield* OwnershipCoordinator.OwnershipCoordinator
  const control = new MessageChannel()
  yield* coordinator.attach(control.port1)
  const frames = yield* Queue.unbounded<OwnershipProtocol.OwnerToPageFrame>()
  const receivedTags: Array<string> = []
  control.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
    const frame = Schema.decodeUnknownSync(OwnershipProtocol.OwnerToPageFrame)(event.data)
    receivedTags.push(frame._tag)
    Queue.offerUnsafe(frames, frame)
  })
  control.port2.start()
  const rpcChannel = new MessageChannel()
  const tab: TestTab = { controlPort: control.port2, receivedTags, frames, rpcChannel }
  postToOwner(tab, {
    _tag: "Attach",
    protocolVersion: OwnershipProtocol.protocolVersion,
    rpcPort: rpcChannel.port1
  })
  return tab
})

const takeFrame = <Tag extends OwnershipProtocol.OwnerToPageFrame["_tag"],>(
  tab: TestTab,
  tag: Tag
): Effect.Effect<Extract<OwnershipProtocol.OwnerToPageFrame, { readonly _tag: Tag }>, Error> =>
  Queue.take(tab.frames).pipe(
    Effect.filterOrFail(
      (frame): frame is Extract<OwnershipProtocol.OwnerToPageFrame, { readonly _tag: Tag }> => frame._tag === tag,
      (frame) => new Error(`expected ${tag}, received ${frame._tag}`)
    )
  )

const acceptNextProvision = (tab: TestTab) =>
  Effect.gen(function*() {
    const provision = yield* takeFrame(tab, "Provision")
    const database = new MessageChannel()
    postToOwner(tab, { _tag: "Provision", nonce: provision.nonce, databasePort: database.port1 })
    database.port2.close()
    return provision.nonce
  })

const attachedInfo = (
  frame: Extract<OwnershipProtocol.OwnerToPageFrame, { readonly _tag: "Attached" }>
): OwnerInfo => Schema.decodeUnknownSync(OwnerInfo)(frame.info)

const openSession = (rpcPort: MessagePort) =>
  Effect.gen(function*() {
    const client = yield* RpcClient.make(ReplicaRpc.group)
    const sessionId = yield* Identity.makeSessionId
    return yield* client.OpenSession({
      sessionId,
      protocolVersion: ReplicaRpc.protocolVersion,
      definitionHash: definition.hash
    })
  }).pipe(
    Effect.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 4 })),
    Effect.provide(Worker.layerSpawner(() => rpcPort)),
    Effect.provide(BrowserWorker.layerPlatform)
  )

it.layer(NodeCrypto.layer)("OwnershipCoordinator", (it) => {
  it.effect("provisions the first attach and serves an RPC session round trip", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        const attached = yield* takeFrame(tab, "Attached")
        assert.isTrue(attached._tag === "Attached" && attached.provider)
        const info = attachedInfo(attached)
        assert.isTrue(info.replicaId.length > 0)

        const lease = yield* openSession(tab.rpcChannel.port2)
        assert.strictEqual(lease.definitionHash, definition.hash)
        assert.strictEqual(lease.protocolVersion, ReplicaRpc.protocolVersion)
        assert.isTrue(lease.ownerEpoch.length > 0)
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))

  it.effect("re-attaches every served tab after the database worker dies", () =>
    Effect.gen(function*() {
      const filename = NodePath.join(
        NodeOs.tmpdir(),
        `effect-local-ownership-${crypto.randomUUID()}.sqlite`
      )
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const provider = yield* attachTab
        yield* acceptNextProvision(provider)
        yield* takeFrame(provider, "ProvisionAccepted")
        const firstAttached = yield* takeFrame(provider, "Attached")
        const firstOwnerId = firstAttached._tag === "Attached" ? firstAttached.ownerId : undefined
        const firstInfo = attachedInfo(firstAttached)

        const secondary = yield* attachTab
        const secondaryAttached = yield* takeFrame(secondary, "Attached")
        assert.isTrue(secondaryAttached._tag === "Attached" && !secondaryAttached.provider)
        assert.isTrue(
          secondaryAttached._tag === "Attached" && secondaryAttached.ownerId === firstOwnerId
        )

        const lease = yield* openSession(secondary.rpcChannel.port2)
        assert.isTrue(lease.ownerEpoch.length > 0)

        // The database worker dies while both tab control channels stay alive. Only a database
        // round trip can observe this; no frame is ever sent to a tab to ask about it.
        assert.strictEqual(started.length, 1)
        yield* started[0].sqlite.disposeEffect
        yield* TestClock.adjust("100 millis")

        yield* takeFrame(provider, "Reattach")
        yield* takeFrame(secondary, "Reattach")

        yield* acceptNextProvision(provider)
        yield* takeFrame(provider, "ProvisionAccepted")
        const providerReattached = yield* takeFrame(provider, "Attached")
        assert.isTrue(providerReattached._tag === "Attached" && providerReattached.provider)
        assert.isTrue(providerReattached._tag === "Attached" && providerReattached.ownerId !== firstOwnerId)

        // The previously stranded tab re-attaches with a fresh channel and is served by the new
        // owner without any manual intervention.
        const freshChannel = new MessageChannel()
        postToOwner(secondary, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: freshChannel.port1
        })
        const secondaryReattached = yield* takeFrame(secondary, "Attached")
        assert.isTrue(secondaryReattached._tag === "Attached" && !secondaryReattached.provider)
        assert.isTrue(
          secondaryReattached._tag === "Attached" &&
            secondaryReattached.ownerId === providerReattached.ownerId
        )

        const reopened = yield* openSession(freshChannel.port2)
        assert.isTrue(reopened.ownerEpoch.length > 0)
        assert.notStrictEqual(reopened.ownerEpoch, lease.ownerEpoch)

        const nextInfo = attachedInfo(providerReattached)
        assert.strictEqual(nextInfo.replicaId, firstInfo.replicaId)
        assert.isTrue(nextInfo.writerGeneration > firstInfo.writerGeneration)

        // The complete frame vocabulary both tabs observed: provisioning, attachment, and the
        // takeover signal. Liveness is never checked through the tab.
        assert.deepStrictEqual(
          [...provider.receivedTags].toSorted(),
          ["Attached", "Attached", "Provision", "Provision", "ProvisionAccepted", "ProvisionAccepted", "Reattach"]
            .toSorted()
        )
      }).pipe(
        Effect.provide(coordinatorLayer(started, filename)),
        Effect.scoped,
        Effect.ensuring(Effect.sync(() => {
          for (const suffix of ["", "-wal", "-shm"]) {
            NodeFs.rmSync(`${filename}${suffix}`, { force: true })
          }
        }))
      )
    }))

  it.effect("verifies the engine before serving a new attach", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const provider = yield* attachTab
        yield* acceptNextProvision(provider)
        yield* takeFrame(provider, "ProvisionAccepted")
        const firstAttached = yield* takeFrame(provider, "Attached")
        const firstOwnerId = firstAttached.ownerId

        // The database worker dies while every control channel stays alive and responsive, and
        // the periodic health loop has not fired yet. The attach verification round trip goes to
        // the database, never to the tab, so the new attach triggers a takeover instead of being
        // served by the dead engine.
        assert.strictEqual(started.length, 1)
        yield* started[0].sqlite.disposeEffect

        const late = yield* attachTab

        yield* takeFrame(provider, "Reattach")
        yield* acceptNextProvision(provider)
        yield* takeFrame(provider, "ProvisionAccepted")
        const providerReattached = yield* takeFrame(provider, "Attached")
        assert.notStrictEqual(providerReattached.ownerId, firstOwnerId)

        // The late tab's very first frame is an attach to the new owner: it was never handed a
        // stale one, and a pending tab is never told to re-attach.
        const lateAttached = yield* takeFrame(late, "Attached")
        assert.strictEqual(lateAttached.ownerId, providerReattached.ownerId)
        assert.isFalse(lateAttached.provider)
        assert.deepStrictEqual(late.receivedTags, ["Attached"])

        const reopened = yield* openSession(late.rpcChannel.port2)
        assert.isTrue(reopened.ownerEpoch.length > 0)
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))

  it.effect("expires a stalled provisioning candidate and provisions the next attach", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const stalled = yield* attachTab
        yield* takeFrame(stalled, "Provision")
        yield* TestClock.adjust("500 millis")
        yield* takeFrame(stalled, "ProvisionRejected")

        const healthy = yield* attachTab
        yield* acceptNextProvision(healthy)
        yield* takeFrame(healthy, "ProvisionAccepted")
        const attached = yield* takeFrame(healthy, "Attached")
        assert.isTrue(attached._tag === "Attached" && attached.provider)

        const lease = yield* openSession(healthy.rpcChannel.port2)
        assert.strictEqual(lease.definitionHash, definition.hash)
        assert.strictEqual(lease.protocolVersion, ReplicaRpc.protocolVersion)
        assert.isTrue(lease.ownerEpoch.length > 0)

        // A rejected candidate is dropped, not quietly served by the next engine. It may
        // re-register only by attaching again.
        assert.isTrue(Option.isNone(yield* Queue.poll(stalled.frames)))
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))

  it.effect("refuses a mismatched ownership protocol version", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const coordinator = yield* OwnershipCoordinator.OwnershipCoordinator
        const control = new MessageChannel()
        yield* coordinator.attach(control.port1)
        const frames = yield* Queue.unbounded<OwnershipProtocol.OwnerToPageFrame>()
        control.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
          Queue.offerUnsafe(frames, Schema.decodeUnknownSync(OwnershipProtocol.OwnerToPageFrame)(event.data))
        })
        control.port2.start()
        const rpcChannel = new MessageChannel()
        control.port2.postMessage(
          Schema.encodeSync(OwnershipProtocol.PageToOwnerFrame)({
            _tag: "Attach",
            protocolVersion: OwnershipProtocol.protocolVersion + 1,
            rpcPort: rpcChannel.port1
          }),
          [rpcChannel.port1]
        )
        const error = yield* takeFrame(
          { controlPort: control.port2, receivedTags: [], frames, rpcChannel },
          "OwnerError"
        )
        assert.isTrue(error._tag === "OwnerError" && error.message.includes("protocol version"))
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))

  it.effect("resets immediately when the database provider detaches", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const provider = yield* attachTab
        yield* acceptNextProvision(provider)
        yield* takeFrame(provider, "ProvisionAccepted")
        yield* takeFrame(provider, "Attached")

        const secondary = yield* attachTab
        yield* takeFrame(secondary, "Attached")

        postToOwner(provider, { _tag: "Detach" })
        yield* takeFrame(secondary, "Reattach")

        yield* acceptNextProvision(secondary)
        yield* takeFrame(secondary, "ProvisionAccepted")
        const promoted = yield* takeFrame(secondary, "Attached")
        assert.isTrue(promoted._tag === "Attached" && promoted.provider)
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))
})
