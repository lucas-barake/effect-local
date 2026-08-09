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
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import { TestClock } from "effect/testing"
import { RpcClient } from "effect/unstable/rpc"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Worker from "effect/unstable/workers/Worker"
import { getEventListeners } from "node:events"
import * as NodeFs from "node:fs"
import * as NodeOs from "node:os"
import * as NodePath from "node:path"
import { DatabaseSync } from "node:sqlite"
import * as BrowserSqlite from "../src/BrowserSqlite.js"
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

type SqliteRequest = [id: number, sql: string, params: ReadonlyArray<unknown>]

const makeSqliteWorker = (onIgnoredRequest?: (request: SqliteRequest) => void) => {
  const database = new DatabaseSync(":memory:")
  const channel = new MessageChannel()
  const workerPort = channel.port2
  let responding = true
  const respond = ([id, statementSql, params]: SqliteRequest) => {
    try {
      const statement = database.prepare(statementSql)
      const columns = statement.columns().map((column) => column.column ?? column.name)
      const rows = statement.all(...(params ?? []) as Array<never>) as Array<Record<string, unknown>>
      workerPort.postMessage([id, undefined, [columns, rows.map((row) => columns.map((name) => row[name!]))]])
    } catch (error) {
      workerPort.postMessage([id, String(error)])
    }
  }
  workerPort.addEventListener("message", (event) => {
    const request = event.data as ReadonlyArray<unknown>
    if (!Array.isArray(request) || typeof request[0] !== "number") return
    const sqliteRequest = request as SqliteRequest
    if (!responding) {
      onIgnoredRequest?.(sqliteRequest)
      return
    }
    respond(sqliteRequest)
  })
  workerPort.start()
  workerPort.postMessage(["ready", undefined, undefined])
  return {
    databasePort: channel.port1,
    workerPort,
    stopResponding() {
      responding = false
    },
    startResponding() {
      responding = true
    },
    respond,
    close() {
      channel.port1.close()
      workerPort.close()
      database.close()
    }
  }
}

const makeBrowserEngineRuntime = (databasePort: MessagePort) =>
  ManagedRuntime.make(
    Layer.merge(
      SqlReplica.layerWithBindings(definition, { projections: [] }),
      SessionManager.layer
    ).pipe(
      Layer.provideMerge(Layer.mergeAll(
        BrowserSqlite.layerMessagePort(databasePort),
        NodeCrypto.layer,
        ReplicaLimits.layer(limits)
      ))
    )
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

const makeTestControlPort = () => {
  let onMessage: ((event: MessageEvent<unknown>) => void) | undefined
  const port = {
    addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
      if (type !== "message") return
      onMessage = typeof listener === "function"
        ? listener as (event: MessageEvent<unknown>) => void
        : (event) => listener.handleEvent(event)
    },
    removeEventListener() {},
    postMessage() {},
    start() {},
    close() {}
  } as unknown as MessagePort
  return {
    port,
    dispatch(frame: OwnershipProtocol.OwnerToPageFrame) {
      onMessage?.(
        new MessageEvent("message", {
          data: Schema.encodeSync(OwnershipProtocol.OwnerToPageFrame)(frame)
        })
      )
    }
  }
}

it.layer(NodeCrypto.layer)("OwnershipCoordinator", (it) => {
  it.effect("terminates a rejected provisional worker before RPC failure listeners can tear down the tab", () =>
    Effect.gen(function*() {
      const control = makeTestControlPort()
      const sharedWorker = Object.assign(new EventTarget(), {
        port: control.port,
        onerror: null
      }) as unknown as SharedWorker
      let terminated = 0
      const databaseWorker = {
        postMessage() {},
        terminate() {
          terminated++
        }
      } as unknown as globalThis.Worker
      const previousErrorEvent = globalThis.ErrorEvent
      globalThis.ErrorEvent = TestErrorEvent as typeof ErrorEvent
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.ErrorEvent = previousErrorEvent
        })
      )

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* Layer.buildWithScope(
        OwnershipCoordinator.layerTab({
          name: "effect-local-rejected-provision-reentrancy-test",
          sharedWorker: () => sharedWorker,
          databaseWorker: () => databaseWorker
        }),
        scope
      )
      const spawn = yield* Worker.Spawner.pipe(Effect.provide(context))
      const rpcPort = spawn(0) as MessagePort
      rpcPort.addEventListener("error", () => {
        sharedWorker.dispatchEvent(new TestErrorEvent("error", { message: "connection closed" }))
      })

      const nonce = Schema.decodeUnknownSync(OwnershipProtocol.ProvisionNonce)("reentrant-rejection")
      control.dispatch({ _tag: "Provision", nonce })
      control.dispatch({ _tag: "ProvisionRejected", nonce })

      assert.strictEqual(terminated, 1)
    }).pipe(Effect.scoped))

  it.effect("fails stale RPC ports when reset error reporting throws", () =>
    Effect.gen(function*() {
      const control = makeTestControlPort()
      const sharedWorker = Object.assign(new EventTarget(), {
        port: control.port,
        onerror: null
      }) as unknown as SharedWorker
      const previousErrorEvent = globalThis.ErrorEvent
      globalThis.ErrorEvent = TestErrorEvent as typeof ErrorEvent
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          globalThis.ErrorEvent = previousErrorEvent
        })
      )

      const context = yield* Layer.build(
        OwnershipCoordinator.layerTab({
          name: "effect-local-reset-error-callback-test",
          sharedWorker: () => sharedWorker,
          databaseWorker: () => ({ postMessage() {}, terminate() {} }) as unknown as globalThis.Worker,
          onOwnerError: () => {
            throw new Error("consumer callback failed")
          }
        })
      )
      const spawn = yield* Worker.Spawner.pipe(Effect.provide(context))
      const rpcPort = spawn(0) as MessagePort
      let rpcErrors = 0
      rpcPort.addEventListener("error", () => {
        rpcErrors++
      })

      let callbackError: unknown
      try {
        control.dispatch({
          _tag: "Reattach",
          ownerId: "previous-owner",
          reason: "the database worker health check failed"
        })
      } catch (error) {
        callbackError = error
      }

      assert.isTrue(callbackError instanceof Error)
      assert.strictEqual(callbackError.message, "consumer callback failed")
      assert.strictEqual(rpcErrors, 1)
    }).pipe(Effect.scoped))

  it.effect("does not expose RPC defects on the browser global", () =>
    Effect.gen(function*() {
      const control = makeTestControlPort()
      const sharedWorker = Object.assign(new EventTarget(), {
        port: control.port,
        onerror: null
      }) as unknown as SharedWorker
      const target = globalThis as typeof globalThis & { __rpcDefects?: Array<unknown> }
      delete target.__rpcDefects
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          delete target.__rpcDefects
        })
      )

      const context = yield* Layer.build(
        OwnershipCoordinator.layerTab({
          name: "effect-local-private-defect-test",
          sharedWorker: () => sharedWorker,
          databaseWorker: () => ({ postMessage() {}, terminate() {} }) as unknown as globalThis.Worker
        })
      )
      const spawn = yield* Worker.Spawner.pipe(Effect.provide(context))
      const rpcPort = spawn(0) as MessagePort
      rpcPort.dispatchEvent(
        new MessageEvent("message", {
          data: [1, {
            _tag: "Defect",
            defect: { message: "private value", stack: "private stack" }
          }]
        })
      )

      assert.isFalse("__rpcDefects" in target)
    }).pipe(Effect.scoped))

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

  it.effect("removes the database observer when the engine factory defects", () =>
    Effect.gen(function*() {
      const factoryCalled = yield* Deferred.make<MessagePort>()
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-factory-defect-test",
        definition,
        engine: (databasePort) => {
          Deferred.doneUnsafe(factoryCalled, Effect.succeed(databasePort))
          throw new Error("engine factory failed")
        },
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "500 millis" }
      })
      const database = new MessageChannel()

      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        const provision = yield* takeFrame(tab, "Provision")
        postToOwner(tab, { _tag: "Provision", nonce: provision.nonce, databasePort: database.port1 })
        const observedPort = yield* Deferred.await(factoryCalled)
        assert.strictEqual(getEventListeners(observedPort, "message").length, 0)
        observedPort.close()
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped,
        Effect.ensuring(Effect.sync(() => database.port2.close()))
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

  it.effect("keeps a live engine whose transaction outlives the deadline while the database keeps answering", () =>
    Effect.gen(function*() {
      // Use the real driver protocol so port replies remain observable while SQL holds its permit.
      const database = makeSqliteWorker()

      const runtimes: Array<ManagedRuntime.ManagedRuntime<OwnershipCoordinator.EngineServices, unknown>> = []
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-busy-liveness-test",
        definition,
        engine: (databasePort: MessagePort) => {
          const runtime = makeBrowserEngineRuntime(databasePort)
          runtimes.push(runtime)
          return runtime
        },
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "200 millis", deadline: "1 second" }
      })
      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        const provision = yield* takeFrame(tab, "Provision")
        postToOwner(tab, { _tag: "Provision", nonce: provision.nonce, databasePort: database.databasePort })
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")
        assert.strictEqual(runtimes.length, 1)

        // Hold the SQL permit across replies so the health probe remains queued.
        const context = yield* runtimes[0].contextEffect
        const steps = yield* Effect.forEach(
          Array.from({ length: 12 }),
          () =>
            Effect.all({
              go: Deferred.make<void>(),
              done: Deferred.make<void>()
            })
        )
        const busy = yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql.withTransaction(
            Effect.forEach(steps, (step) =>
              Deferred.await(step.go).pipe(
                Effect.andThen(sql`SELECT 1 AS ok`),
                Effect.andThen(Deferred.succeed(step.done, undefined))
              ), { discard: true })
          )
        }).pipe(Effect.provide(context), Effect.forkChild({ startImmediately: true }))

        for (const step of steps) {
          yield* Deferred.succeed(step.go, undefined)
          yield* Deferred.await(step.done)
          yield* TestClock.adjust("200 millis")
        }
        yield* Fiber.join(busy)
        yield* TestClock.adjust("100 millis")

        assert.isFalse(tab.receivedTags.includes("Reattach"))
        assert.strictEqual(runtimes.length, 1)
        const lease = yield* openSession(tab.rpcChannel.port2)
        assert.isTrue(lease.ownerEpoch.length > 0)
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped,
        Effect.ensuring(Effect.sync(database.close))
      )
    }))

  it.effect("resets a worker that emits malformed frames without completing round trips", () =>
    Effect.gen(function*() {
      const ignoredRequests = yield* Queue.unbounded<SqliteRequest>()
      const malformedFrames = yield* Queue.unbounded<void>()
      const database = makeSqliteWorker((request) => Queue.offerUnsafe(ignoredRequests, request))
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-malformed-liveness-test",
        definition,
        engine: (databasePort: MessagePort) => {
          databasePort.addEventListener("message", (event) => {
            if (Array.isArray(event.data) && event.data[0] === "garbage") {
              Queue.offerUnsafe(malformedFrames, undefined)
            }
          })
          return makeBrowserEngineRuntime(databasePort)
        },
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "200 millis", deadline: "1 second" }
      })

      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        const provision = yield* takeFrame(tab, "Provision")
        postToOwner(tab, { _tag: "Provision", nonce: provision.nonce, databasePort: database.databasePort })
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")
        database.stopResponding()

        yield* TestClock.adjust("100 millis")
        yield* Queue.take(ignoredRequests)
        for (let index = 0; index < 6; index++) {
          database.workerPort.postMessage(["garbage", undefined, undefined])
          yield* Queue.take(malformedFrames)
          yield* TestClock.adjust("200 millis")
        }

        const reattached = yield* takeFrame(tab, "Reattach")
        assert.strictEqual(reattached.reason, "the database worker health check failed")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped,
        Effect.ensuring(Effect.sync(database.close))
      )
    }))

  it.effect("keeps a worker live while it emits valid update hooks", () =>
    Effect.gen(function*() {
      const ignoredRequests = yield* Queue.unbounded<SqliteRequest>()
      const updateHooks = yield* Queue.unbounded<void>()
      const numericReplies = yield* Queue.unbounded<void>()
      const database = makeSqliteWorker((request) => Queue.offerUnsafe(ignoredRequests, request))
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-update-hook-liveness-test",
        definition,
        engine: (databasePort: MessagePort) => {
          databasePort.addEventListener("message", (event) => {
            if (!Array.isArray(event.data)) return
            if (event.data[0] === "update_hook") Queue.offerUnsafe(updateHooks, undefined)
            if (typeof event.data[0] === "number") Queue.offerUnsafe(numericReplies, undefined)
          })
          return makeBrowserEngineRuntime(databasePort)
        },
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "200 millis", deadline: "1 second" }
      })

      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        const provision = yield* takeFrame(tab, "Provision")
        postToOwner(tab, { _tag: "Provision", nonce: provision.nonce, databasePort: database.databasePort })
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")
        database.stopResponding()

        yield* TestClock.adjust("100 millis")
        const blockedProbe = yield* Queue.take(ignoredRequests)
        for (let index = 0; index < 6; index++) {
          database.workerPort.postMessage(["update_hook", "tasks", index])
          yield* Queue.take(updateHooks)
          yield* TestClock.adjust("200 millis")
        }

        database.startResponding()
        database.respond(blockedProbe)
        yield* Queue.take(numericReplies)
        const rpcChannel = new MessageChannel()
        postToOwner(tab, {
          _tag: "Attach",
          protocolVersion: OwnershipProtocol.protocolVersion,
          rpcPort: rpcChannel.port1
        })
        yield* takeFrame(tab, "Attached")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped,
        Effect.ensuring(Effect.sync(database.close))
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
        const reattached = yield* takeFrame(tab, "Reattach")
        assert.strictEqual(reattached.reason, "the database worker health check failed")
      }).pipe(
        Effect.provide(Coordinator),
        Effect.scoped
      )
    }))

  it.effect("keeps a live engine when the dispatcher is starved past the deadline", () =>
    Effect.gen(function*() {
      const started: Array<StartedEngine> = []
      const Coordinator = OwnershipCoordinator.layerSharedWorker({
        name: "effect-local-ownership-starvation-test",
        definition,
        engine: makeEngineFactory(started, ":memory:"),
        info: ownerInfo,
        provisionTimeout: "500 millis",
        engineStartTimeout: "5 seconds",
        engineDisposeTimeout: "100 millis",
        healthCheck: { interval: "100 millis", timeout: "200 millis", deadline: "1 second" }
      })

      // Browser workers have no setImmediate, so every dispatcher hop is a setTimeout — and when
      // a SharedWorker's clients are all hidden, the browser throttles those together with
      // ordinary timers while the wall clock runs on. This scheduler reproduces that: while
      // frozen, the coordinator's tasks queue instead of running, exactly like a throttled
      // worker, and the wall clock (TestClock) races ahead of them.
      const pending: Array<() => void> = []
      let frozen = false
      const scheduler = new Scheduler.MixedScheduler("async", (task) => {
        if (frozen) {
          pending.push(task)
          return () => {}
        }
        const handle = setImmediate(task)
        return () => clearImmediate(handle)
      })
      const pump = Effect.promise(() => new Promise((resolve) => setImmediate(resolve))).pipe(
        Effect.andThen(Effect.sync(() => {
          for (const task of pending.splice(0)) task()
        })),
        Effect.repeat({ times: 40 }),
        Effect.asVoid
      )

      const scope = yield* Scope.make()
      const context = yield* Layer.buildWithScope(Coordinator, scope).pipe(
        Effect.provideService(Scheduler.Scheduler, scheduler)
      )
      yield* Effect.gen(function*() {
        const tab = yield* attachTab
        yield* acceptNextProvision(tab)
        yield* takeFrame(tab, "ProvisionAccepted")
        yield* takeFrame(tab, "Attached")
        assert.strictEqual(started.length, 1)

        // Healthy round trips complete while the dispatcher runs normally.
        yield* TestClock.adjust("300 millis")
        yield* pump

        // The freeze: wall clock advances far past the deadline while not one coordinator task
        // runs. The engine is untouched and perfectly healthy the whole time.
        frozen = true
        yield* TestClock.adjust("5 seconds")
        frozen = false
        yield* pump
        yield* TestClock.adjust("100 millis")
        yield* pump

        // Starvation of the coordinator is evidence about the environment, not the engine: the
        // engine must survive it and keep serving.
        assert.isFalse(tab.receivedTags.includes("Reattach"))
        assert.strictEqual(started.length, 1)
        const engineContext = yield* started[0].runtime.contextEffect
        const alive = yield* Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return yield* sql`SELECT 1 AS ok`
        }).pipe(Effect.provide(engineContext))
        assert.strictEqual(alive.length, 1)
      }).pipe(
        Effect.provide(context),
        Effect.onExit((exit) => Scope.close(scope, exit))
      )
    }))
})
