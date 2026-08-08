import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
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
import * as ReplicaClient from "../src/ReplicaClient.js"
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
  filename: string,
  beforeStart?: ((attempt: number) => Effect.Effect<void>) | undefined,
  onRelease?: ((attempt: number) => void) | undefined
) => {
  let attempt = 0
  const engine = (databasePort: MessagePort) => {
    void databasePort
    attempt++
    const current = attempt
    const sqlite = ManagedRuntime.make(SqliteClient.layer({ filename, disableWAL: true }))
    const services = Layer.merge(
      SqlReplica.layerWithBindings(definition, { projections: [] }),
      SessionManager.layer
    ).pipe(
      Layer.provideMerge(Layer.mergeAll(
        Layer.unwrap(Effect.map(sqlite.contextEffect, Layer.succeedContext)),
        NodeCrypto.layer,
        ReplicaLimits.layer(limits)
      ))
    )
    const withBeforeStart = beforeStart === undefined
      ? services
      : Layer.merge(services, Layer.effectDiscard(beforeStart(attempt)))
    const EngineLive = onRelease === undefined
      ? withBeforeStart
      : Layer.merge(
        withBeforeStart,
        Layer.effectDiscard(
          Effect.acquireRelease(Effect.void, () => Effect.sync(() => onRelease(current)))
        )
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

class TestErrorEvent extends Event implements ErrorEvent {
  readonly colno = 0
  readonly error: unknown
  readonly filename = ""
  readonly lineno = 0
  readonly message: string

  constructor(type: string, init?: ErrorEventInit) {
    super(type)
    this.error = init?.error
    this.message = init?.message ?? ""
  }
}

it.layer(NodeCrypto.layer)("OwnershipCoordinator", (it) => {
  it.effect("fails a pending client acquisition and retries after provisioning is rejected", () =>
    Effect.gen(function*() {
      const control = new MessageChannel()
      const frames = yield* Queue.unbounded<OwnershipProtocol.PageToOwnerFrame>()
      control.port2.addEventListener("message", (event: MessageEvent<unknown>) => {
        Queue.offerUnsafe(frames, Schema.decodeUnknownSync(OwnershipProtocol.PageToOwnerFrame)(event.data))
      })
      control.port2.start()

      const created = yield* Queue.unbounded<{
        readonly id: number
        readonly worker: globalThis.Worker
      }>()
      const terminated = yield* Queue.unbounded<number>()
      let nextWorkerId = 0
      const databaseWorker = () => {
        const id = ++nextWorkerId
        const worker = {
          postMessage() {},
          terminate() {
            Queue.offerUnsafe(terminated, id)
          }
        } as unknown as globalThis.Worker
        Queue.offerUnsafe(created, { id, worker })
        return worker
      }
      const sharedWorker = Object.assign(new EventTarget(), {
        port: control.port1,
        onerror: null
      }) as unknown as SharedWorker
      const previousErrorEvent = globalThis.ErrorEvent
      globalThis.ErrorEvent = TestErrorEvent as typeof ErrorEvent
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.ErrorEvent = previousErrorEvent
        })
      )

      const Ownership = OwnershipCoordinator.layerTab({
        name: "effect-local-rejected-provision-test",
        sharedWorker: () => sharedWorker,
        databaseWorker
      })
      const protocolScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(protocolScope, Exit.void))
      const protocolContext = yield* Layer.buildWithScope(
        RpcClient.layerProtocolWorker({ size: 1, concurrency: 4 }).pipe(
          Layer.provide(Ownership)
        ),
        protocolScope
      )
      const takePageFrame = <Tag extends OwnershipProtocol.PageToOwnerFrame["_tag"],>(
        tag: Tag
      ) =>
        Queue.take(frames).pipe(
          Effect.filterOrFail(
            (frame): frame is Extract<OwnershipProtocol.PageToOwnerFrame, { readonly _tag: Tag }> => frame._tag === tag,
            (frame) => new Error(`expected ${tag}, received ${frame._tag}`)
          )
        )

      const firstAttach = yield* takePageFrame("Attach")
      const rpcRequests = yield* Queue.unbounded<unknown>()
      firstAttach.rpcPort.addEventListener("message", (event) => {
        Queue.offerUnsafe(rpcRequests, event.data)
      })
      firstAttach.rpcPort.start()

      const clientScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(clientScope, Exit.void))
      const acquisition = yield* Layer.buildWithScope(
        ReplicaClient.layer(definition, { sessionTimeout: "10 seconds" }).pipe(
          Layer.provide(Layer.succeedContext(protocolContext))
        ),
        clientScope
      ).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      assert.isUndefined(acquisition.pollUnsafe())

      const firstNonce = Schema.decodeUnknownSync(OwnershipProtocol.ProvisionNonce)("rejected-provision")
      control.port2.postMessage(
        Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)({ _tag: "Provision", nonce: firstNonce })
      )
      const firstProvision = yield* takePageFrame("Provision")
      assert.strictEqual(firstProvision.nonce, firstNonce)
      const firstWorker = yield* Queue.take(created)
      control.port2.postMessage(
        Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)({
          _tag: "ProvisionRejected",
          nonce: firstNonce
        })
      )

      yield* Queue.take(rpcRequests)
      assert.strictEqual(yield* Queue.take(terminated), firstWorker.id)
      const error = yield* Fiber.join(acquisition).pipe(Effect.flip)
      if (!ReplicaError.isReplicaError(error)) {
        assert.fail(`expected ReplicaError, received ${String(error)}`)
      }
      assert.strictEqual(error.reason._tag, "StorageUnavailable")

      yield* TestClock.adjust("1 second")
      const retryAttach = yield* takePageFrame("Attach")
      // The worker waits uninterruptibly for readiness, so release the retry before closing its scope.
      retryAttach.rpcPort.postMessage([0])
    }).pipe(Effect.scoped))

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

  it.effect("backs off between re-provisions when the engine start keeps failing", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const engine = makeEngineFactory(
        started,
        ":memory:",
        () => Effect.die(new Error("corrupt replica database"))
      )
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-backoff-test",
        definition,
        engine,
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "500 millis" },
        provisionRetry: {
          schedule: Schedule.andThen(Schedule.recurs(1), Schedule.exponential("1 second")),
          failureBudget: 3
        }
      })

      // MessagePort delivery is a macrotask, so absence assertions must pump the task queue
      // before polling; TestClock adjustments alone never deliver a posted frame.
      const pumpTasks = Effect.promise(() => new Promise((resolve) => setImmediate(resolve))).pipe(
        Effect.repeat({ times: 4 }),
        Effect.asVoid
      )
      const reattach = (tab: TestTab) => {
        const channel = new MessageChannel()
        postToOwner(tab, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: channel.port1
        })
      }

      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")

        // The first failure re-provisions immediately: a failed candidate is dropped, so the
        // tab re-registers with a fresh attach and is asked to provide again with no delay.
        reattach(tab)
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")

        // The second consecutive failure opens the backoff window. A fresh attach inside the
        // window registers the tab but must not trigger provisioning early.
        reattach(tab)
        yield* TestClock.adjust("500 millis")
        yield* pumpTasks
        assert.isTrue(Option.isNone(yield* Queue.poll(tab.frames)))

        yield* TestClock.adjust("500 millis")
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")

        // The third failure crosses the budget: the failing condition is surfaced to the app
        // while retrying continues, so a recoverable cause still heals without intervention.
        const surfaced = yield* takeFrame(tab, "OwnerError")
        assert.include(surfaced.message, "3 consecutive")
        assert.deepStrictEqual(surfaced.reason, { _tag: "EngineProvisionFailing", failures: 3 })

        reattach(tab)
        yield* TestClock.adjust("2 seconds")
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")
        yield* takeFrame(tab, "OwnerError")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))

  it.effect("restarts the retry schedule after a healthy interval", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const engine = makeEngineFactory(
        started,
        ":memory:",
        (attempt) => attempt <= 2 ? Effect.die(new Error("transient start failure")) : Effect.void
      )
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-healthy-reset-test",
        definition,
        engine,
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "500 millis" },
        provisionRetry: { schedule: Schedule.exponential("1 second"), failureBudget: 5 }
      })
      const reattach = (tab: TestTab) => {
        const channel = new MessageChannel()
        postToOwner(tab, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: channel.port1
        })
      }

      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")
        reattach(tab)
        yield* TestClock.adjust("1 second")
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionRejected")

        reattach(tab)
        yield* TestClock.adjust("2 seconds")
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")

        // Two passing periodic probes prove a healthy interval, which ends the failure streak.
        yield* TestClock.adjust("200 millis")

        assert.strictEqual(started.length, 3)
        yield* started[2].sqlite.disposeEffect
        yield* TestClock.adjust("100 millis")
        yield* takeFrame(tab, "Reattach")

        // The reset after a healthy interval starts a fresh schedule run: the delay is the
        // schedule's first delay again, not a continuation of the pre-recovery streak (4s here).
        yield* TestClock.adjust("1 second")
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        const reattached = yield* takeFrame(tab, "Attached")
        assert.isTrue(reattached.provider)
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))

  it.effect("stops provisioning when the retry schedule completes and restarts on a fresh attach", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const engine = makeEngineFactory(
        started,
        ":memory:",
        () => Effect.die(new Error("corrupt replica database"))
      )
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-exhaustion-test",
        definition,
        engine,
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "500 millis" },
        provisionRetry: { schedule: Schedule.recurs(0), failureBudget: 5 }
      })
      const pumpTasks = Effect.promise(() => new Promise((resolve) => setImmediate(resolve))).pipe(
        Effect.repeat({ times: 4 }),
        Effect.asVoid
      )

      yield* Effect.gen(function*() {
        const candidate = yield* attachTab
        yield* takeFrame(candidate, "Provision").pipe(
          Effect.flatMap((provision) =>
            Effect.sync(() => {
              const database = new MessageChannel()
              postToOwner(candidate, { _tag: "Provision", nonce: provision.nonce, databasePort: database.port1 })
              database.port2.close()
            })
          )
        )
        const bystander = yield* attachTab
        yield* takeFrame(candidate, "ProvisionRejected")

        // recurs(0) completes on the first failure: the bystander tab is told provisioning
        // stopped, with a reason the app can discriminate on to offer a repair flow.
        const surfaced = yield* takeFrame(bystander, "OwnerError")
        assert.include(surfaced.message, "retry schedule is exhausted")
        assert.deepStrictEqual(surfaced.reason, { _tag: "EngineProvisionExhausted", failures: 1 })

        // Stopped means stopped: no amount of elapsed time provisions another engine.
        yield* TestClock.adjust("1 minute")
        yield* pumpTasks
        assert.isTrue(Option.isNone(yield* Queue.poll(bystander.frames)))

        // A fresh attach is the recovery signal and starts a new schedule run.
        const channel = new MessageChannel()
        postToOwner(bystander, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: channel.port1
        })
        yield* takeFrame(bystander, "Provision")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))

  it.effect("releases the engine when the starting candidate detaches", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const released: Array<number> = []
      const firstStartEntered = yield* Deferred.make<void>()
      const releaseFirstStart = yield* Deferred.make<void>()
      const engine = makeEngineFactory(
        started,
        ":memory:",
        (attempt) =>
          attempt === 1
            ? Deferred.succeed(firstStartEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFirstStart))
            )
            : Effect.void,
        (attempt) => released.push(attempt)
      )
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-starting-detach-release-test",
        definition,
        engine,
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "500 millis" }
      })

      yield* Effect.gen(function*() {
        const provider = yield* attachTab
        yield* acceptNextProvision(provider)
        yield* Deferred.await(firstStartEntered)

        const secondary = yield* attachTab
        postToOwner(provider, { _tag: "Detach" })

        // Joining the frame is also the reprovision assertion: without the detach handling the
        // secondary is never provisioned and this never completes.
        const provisioned = yield* takeFrame(secondary, "Provision").pipe(Effect.forkChild)
        yield* TestClock.adjust("100 millis")
        yield* Fiber.join(provisioned)

        // The interrupted start fiber is the only holder of that engine: it never reached
        // `EngineStarted`, so nothing else can dispose it.
        yield* Effect.yieldNow
        assert.deepStrictEqual(released, [1])
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))

  it.effect("keeps serving while a long transaction delays the health round trip", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")

        // A backlog drain holds the client's single connection permit through long write
        // transactions. The health probe queues behind it: late, but the database is alive.
        assert.strictEqual(started.length, 1)
        const context = yield* started[0].runtime.contextEffect
        const release = yield* Deferred.make<void>()
        const held = yield* Deferred.make<void>()
        const busy = yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql.withTransaction(
            Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release)))
          )
        }).pipe(Effect.provide(context), Effect.forkChild)
        yield* Deferred.await(held)

        // Far past the probe timeout, still well under the round-trip deadline.
        yield* TestClock.adjust("1 second")
        yield* TestClock.adjust("1 second")
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(busy)
        yield* TestClock.adjust("100 millis")

        assert.isFalse(tab.receivedTags.includes("Reattach"))
        const lease = yield* openSession(tab.rpcChannel.port2)
        assert.isTrue(lease.ownerEpoch.length > 0)
      }).pipe(
        Effect.provide(coordinatorLayer(started)),
        Effect.scoped
      )
    }))

  it.effect("resets an engine whose round trips never complete within the deadline", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-deadline-test",
        definition,
        engine: makeEngineFactory(started, ":memory:"),
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "200 millis", deadline: "1 second" }
      })
      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")

        // The connection wedges: the transaction never commits and never fails, so the probe
        // can only hang. This is indistinguishable from a dead database worker and must reset.
        assert.strictEqual(started.length, 1)
        const context = yield* started[0].runtime.contextEffect
        const held = yield* Deferred.make<void>()
        yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql.withTransaction(
            Deferred.succeed(held, undefined).pipe(Effect.andThen(Effect.never))
          )
        }).pipe(Effect.provide(context), Effect.forkChild)
        yield* Deferred.await(held)

        yield* TestClock.adjust("1 second")
        yield* TestClock.adjust("200 millis")
        yield* takeFrame(tab, "Reattach")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))
})
