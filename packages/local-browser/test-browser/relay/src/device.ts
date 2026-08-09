import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserSocket from "@effect/platform-browser/BrowserSocket"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerCredentials from "@lucas-barake/effect-local-rpc/PeerCredentials"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as ReplicaOperationScheduler from "@lucas-barake/effect-local-sql/ReplicaOperationScheduler"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { AddLabel, definition, DomainLive, limits, TaskDocument } from "./domain.ts"
import { deviceByName, type DeviceIdentity, devices } from "./identities.ts"

const relayUrl = `ws://127.0.0.1:${import.meta.env.VITE_RELAY_PORT ?? "4176"}/relay`

const identity = deviceByName(new URL(window.location.href).searchParams.get("device") ?? "alpha")

const remote = devices.find((device) => device.name !== identity.name)!

const deferred = <A,>() => {
  let resolve!: (value: A) => void
  const promise = new Promise<A>((resume) => {
    resolve = resume
  })
  return { promise, resolve }
}

const databaseOperations = new Map<string, ReturnType<typeof deferred<void>>>()

export const databaseBridge = (() => {
  const engine = new MessageChannel()
  const database = new MessageChannel()
  const worker = new Worker(new URL("./opfs.worker.ts", import.meta.url), { type: "module" })
  const requests = new Array<ReturnType<typeof deferred<unknown>>>()
  let gate: {
    readonly request: ReturnType<typeof deferred<void>>
    readonly response: ReturnType<typeof deferred<void>>
    requestObserved: boolean
    held?: MessageEvent
  } | undefined
  const forward = (target: MessagePort, event: MessageEvent) => {
    target.postMessage(event.data, [...event.ports])
  }
  engine.port1.addEventListener("message", (event) => {
    requests.shift()?.resolve(event.data)
    if (gate !== undefined && !gate.requestObserved) {
      gate.requestObserved = true
      gate.request.resolve()
    }
    forward(database.port2, event)
  })
  database.port2.addEventListener("message", (event) => {
    if (gate !== undefined && gate.requestObserved && gate.held === undefined) {
      gate.held = event
      gate.response.resolve()
      return
    }
    forward(engine.port1, event)
  })
  engine.port1.start()
  database.port2.start()
  worker.postMessage({ databasePort: database.port1, dbName: `relay-${identity.name}.sqlite` }, [database.port1])
  return {
    port: engine.port2,
    arm: () => {
      if (gate !== undefined) throw new Error("a database response gate is already armed")
      gate = { request: deferred(), response: deferred(), requestObserved: false }
    },
    waitForRequest: () => {
      if (gate === undefined) throw new Error("no database response gate is armed")
      return gate.request.promise
    },
    waitForResponse: () => {
      if (gate === undefined) throw new Error("no database response gate is armed")
      return gate.response.promise
    },
    nextRequest: () => {
      const request = deferred<unknown>()
      requests.push(request)
      return request.promise
    },
    release: () => {
      if (gate?.held === undefined) throw new Error("no database response is held")
      forward(engine.port1, gate.held)
      gate = undefined
    }
  }
})()

const Base = Layer.mergeAll(
  BrowserSqlite.layerMessagePort(databaseBridge.port),
  BrowserCrypto.layer,
  ReplicaLimits.layer(limits),
  PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults),
  PeerRelayOutboxLimits.layer(PeerRelayOutboxLimits.defaults)
)

// `layerRelayWithBindings`, not `layerWithBindings`: the direct constructor builds a direct
// `PeerSync`, and `PeerRelayClientRuntime` refuses to build on one.
const ReplicaLive = SqlReplica.layerRelayWithBindings(definition, { projections: [] }).pipe(
  Layer.provide(DomainLive),
  Layer.provideMerge(Base),
  Layer.orDie
)

class RelayClient extends Context.Service<RelayClient, PeerRpc.RpcClient>()(
  "relay-fixture/RelayClient"
) {}

/**
 * One socket for the device, not one per session.
 *
 * The relay multiplexes sessions over a single connection, so a socket per session pays for a TCP
 * and WebSocket handshake per document set and gains nothing. The cap that matters is
 * `maxSessionsPerSubject`, which the relay counts per tenant and subject regardless of how many
 * connections carry them.
 *
 * `RelayConnectionStatus.layerProtocolSocket` rather than the plain Effect one, because
 * the status reader has to come from the same build as the protocol it is reporting on.
 *
 * The socket still has to outlive any session built over it. Providing these around the session
 * constructor alone built the socket and its pump, handed back a client, then released both when
 * the constructor returned, leaving every request waiting on a transport that was gone.
 */
const RelayLink = Layer.effect(RelayClient)(PeerRpc.makeRpcClient).pipe(
  Layer.provideMerge(RelayConnectionStatus.layerProtocolSocket()),
  Layer.provide(BrowserSocket.layerWebSocket(relayUrl)),
  Layer.provide(RpcSerialization.layerJson),
  Layer.provide(PeerAuthentication.layerClient),
  Layer.provide(
    Layer.succeed(PeerCredentials.PeerCredentials)({
      get: Effect.succeed(Redacted.make(identity.token))
    })
  )
)

const DeviceLive = PeerRelayClientRuntime.layerSql.pipe(
  Layer.provideMerge(ReplicaLive),
  Layer.provideMerge(RelayLink),
  Layer.orDie
)

const runtime = Atom.runtime(DeviceLive)

export const peerConnectionStatus = ReplicaAtom.peerConnectionStatus(runtime, remote.principal.peerId)

export const relayConnectionStatus = ReplicaAtom.relayConnectionStatus(runtime)

/**
 * The documents this device syncs with its peer, as a stable key.
 *
 * A session covers a fixed set of documents chosen when it opens, so this is the set - not a
 * "currently selected" document. Held as a sorted joined string because the atom re-runs, and so
 * tears the session down and rebuilds it, whenever this value changes; a fresh array would do that
 * on every render, and editing a document must not reopen its session.
 */
export const syncedDocumentsAtom = Atom.make("")

/**
 * Adds to the set rather than replacing it. Assigning would drop every document already being
 * synced, and the session would reopen covering only the newest one - after which the next push for
 * any of the others is refused as an unselected document.
 */
const addSyncedDocument = (get: Atom.FnContext, documentId: Identity.DocumentId) => {
  const current = get(syncedDocumentsAtom)
  const next = new Set(current === "" ? [] : current.split(","))
  next.add(documentId)
  get.set(syncedDocumentsAtom, [...next].toSorted().join(","))
}

/**
 * Resolves once the session covering the document is open, rather than when the set has been
 * written. A caller that returns before the socket exists has no way to know when its next write
 * will actually be sent, and nothing else reports it.
 */
export const syncDocument = runtime.fn<Identity.DocumentId>()(
  Effect.fnUntraced(function*(documentId, get) {
    addSyncedDocument(get, documentId)
    yield* get.result(sessionAtom, { suspendOnWaiting: true })
  })
)

export const createTask = runtime.fn<string>()(
  Effect.fnUntraced(function*(title, get) {
    const replica = yield* Replica.Replica
    const documentId = yield* replica.create(TaskDocument, {
      commandId: yield* Identity.makeCommandId,
      value: { title, labels: [] }
    })
    addSyncedDocument(get, documentId)
    yield* get.result(sessionAtom, { suspendOnWaiting: true })
    return documentId
  })
)

export const addLabel = runtime.fn<{
  readonly documentId: Identity.DocumentId
  readonly label: string
}>()(
  Effect.fnUntraced(function*({ documentId, label }) {
    const replica = yield* Replica.Replica
    yield* replica.mutate(AddLabel, {
      commandId: yield* Identity.makeCommandId,
      documentId,
      payload: label
    })
  })
)

// `concurrent`, because a polled read must not interrupt the read before it. An `Atom.fn` cancels
// its in-flight call when set again, so a poll that outpaces the read never resolves at all.
export const readTask = runtime.fn<Identity.DocumentId>()(
  Effect.fnUntraced(function*(documentId) {
    const replica = yield* Replica.Replica
    const snapshot = yield* replica.get(TaskDocument, documentId)
    return { title: snapshot.value.title, labels: [...snapshot.value.labels] }
  }),
  { concurrent: true }
)

export const makeCommandId = runtime.fn<void>()(
  Effect.fnUntraced(function*() {
    return yield* Identity.makeCommandId
  })
)

export const probeInteractiveDatabase = runtime.fn<Identity.CommandId>()(
  Effect.fnUntraced(function*(commandId) {
    databaseOperations.get("interactive")?.resolve()
    const replica = yield* Replica.Replica
    yield* replica.lookupCommandDelivery(commandId)
  })
)

export const exportBackup = runtime.fn<void>()(
  Effect.fnUntraced(function*() {
    const replica = yield* Replica.Replica
    const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
    const joined = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.byteLength, 0))
    let offset = 0
    for (const chunk of chunks) {
      joined.set(chunk, offset)
      offset += chunk.byteLength
    }
    return [...joined]
  })
)

export const restoreBackup = runtime.fn<ReadonlyArray<number>>()(
  Effect.fnUntraced(function*(bytes) {
    const replica = yield* Replica.Replica
    yield* replica.restoreBackup({
      expectedDefinitionHash: definition.hash,
      installationId: yield* Identity.makeBackupInstallationId,
      maxBytes: limits.maxBackupBytes,
      mode: "clone",
      source: Stream.make(new Uint8Array(bytes))
    })
  })
)

export const runBackgroundDatabaseOperation = runtime.fn<string>()(
  Effect.fnUntraced(function*(label) {
    databaseOperations.get(label)?.resolve()
    yield* Effect.scoped(Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      yield* scheduler.background
      const sql = yield* SqlClient.SqlClient
      yield* sql.unsafe(`SELECT '${label}' AS label`)
    }))
  }),
  { concurrent: true }
)

export const waitForDatabaseOperation = (label: string) => {
  const operation = deferred<void>()
  databaseOperations.set(label, operation)
  return operation.promise.finally(() => databaseOperations.delete(label))
}

export const waitForInteractiveReservation = runtime.fn<void>()(
  Effect.fnUntraced(function*() {
    const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
    yield* scheduler.reservationChanges.pipe(
      Stream.filter((reservations) => reservations.interactive > 0),
      Stream.runHead
    )
  })
)

export const waitForNoInteractiveReservations = runtime.fn<void>()(
  Effect.fnUntraced(function*() {
    const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
    yield* scheduler.reservationChanges.pipe(
      Stream.filter((reservations) => reservations.interactive === 0),
      Stream.runHead
    )
  })
)

export const waitForBackgroundReservations = runtime.fn<number>()(
  Effect.fnUntraced(function*(minimum) {
    const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
    yield* scheduler.reservationChanges.pipe(
      Stream.filter((reservations) => reservations.background >= minimum),
      Stream.runHead
    )
  })
)

const transportOptions = (
  self: DeviceIdentity,
  documentIds: ReadonlyArray<Identity.DocumentId>,
  senderReplicaIncarnation: Identity.ReplicaIncarnation
): RpcPeerTransport.Options => ({
  expectedLocal: self.principal,
  senderReplicaIncarnation,
  expectedRelayPeerId: self.relayPeerId,
  remote: { subjectId: remote.principal.subjectId, peerId: remote.principal.peerId },
  documents: documentIds.map((documentId) => ({ document: TaskDocument, documentId })),
  definition,
  receiptRetentionMillis: PeerRelayReceiptLimits.defaults.receiptRetentionMillis,
  senderRetryHorizonMillis: Duration.toMillis(Duration.days(7)),
  replayBatchSize: 16
})

class RelaySession extends Context.Service<RelaySession, PeerSession.SupervisedPeerSession>()(
  "relay-fixture/RelaySession"
) {}

/**
 * `makeSession` stays here rather than moving up with the socket. Opening a session is what
 * registers the peer attempt, so hoisting it would report a peer as connecting before the page has
 * asked to sync anything.
 */
const layerSession = (documentIds: ReadonlyArray<Identity.DocumentId>) =>
  Layer.effect(RelaySession)(
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const incarnation = (yield* gate.current).incarnation
      const client = yield* RelayClient
      return yield* RpcPeerTransport.makeSession(client, transportOptions(identity, documentIds, incarnation))
    })
  )

/**
 * Mounted by the root, so the session's lifetime is the component's.
 *
 * `Layer.build` rather than `Effect.provide`: the latter closes the layer's scope as soon as this
 * effect returns, taking the socket and the receive loop with it and leaving a session object that
 * can still be called and can no longer deliver anything.
 */
export const sessionAtom = runtime.atom((get) => {
  const key = get(syncedDocumentsAtom)
  if (key === "") return Effect.never
  const documentIds = key.split(",").map((id) => Identity.DocumentId.make(id))
  return Effect.gen(function*() {
    const context = yield* Layer.build(layerSession(documentIds))
    const session = Context.get(context, RelaySession)
    for (const documentId of documentIds) yield* session.markDirty(documentId)
    yield* session.flush
    return session
  })
})

export const push = runtime.fn<Identity.DocumentId>()(
  Effect.fnUntraced(function*(documentId, get) {
    const session = yield* get.result(sessionAtom, { suspendOnWaiting: true })
    yield* session.markDirty(documentId)
    yield* session.flush
  })
)
