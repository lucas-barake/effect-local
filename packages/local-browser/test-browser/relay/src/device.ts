import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserSocket from "@effect/platform-browser/BrowserSocket"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as PeerAuthentication from "@lucas-barake/effect-local-rpc/PeerAuthentication"
import * as PeerCredentials from "@lucas-barake/effect-local-rpc/PeerCredentials"
import * as PeerRpc from "@lucas-barake/effect-local-rpc/PeerRpc"
import * as RpcPeerTransport from "@lucas-barake/effect-local-rpc/RpcPeerTransport"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import type * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
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
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import { AddLabel, definition, DomainLive, limits, TaskDocument } from "./domain.ts"
import { deviceByName, type DeviceIdentity, devices } from "./identities.ts"

const relayUrl = `ws://127.0.0.1:${import.meta.env.VITE_RELAY_PORT ?? "4176"}/relay`

const identity = deviceByName(new URL(window.location.href).searchParams.get("device") ?? "alpha")

const remote = devices.find((device) => device.name !== identity.name)!

const databasePort = (dbName: string) => {
  const channel = new MessageChannel()
  const worker = new Worker(new URL("./opfs.worker.ts", import.meta.url), { type: "module" })
  worker.postMessage({ databasePort: channel.port2, dbName }, [channel.port2])
  return channel.port1
}

const Base = Layer.mergeAll(
  BrowserSqlite.layerMessagePort(databasePort(`relay-${identity.name}.sqlite`)),
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

const DeviceLive = PeerRelayClientRuntime.layerSql.pipe(Layer.provideMerge(ReplicaLive), Layer.orDie)

const runtime = Atom.runtime(DeviceLive)

/**
 * The documents this device syncs with its peer, as a stable key.
 *
 * A session covers a fixed set of documents chosen when it opens, so this is the set - not a
 * "currently selected" document. Held as a sorted joined string because the atom re-runs, and so
 * tears the session down and rebuilds it, whenever this value changes; a fresh array would do that
 * on every render, and editing a document must not reopen its session.
 */
export const syncedDocumentsAtom = Atom.make("")

export const syncDocument = Atom.fnSync((documentId: Identity.DocumentId, get) => {
  const current = get(syncedDocumentsAtom)
  const next = new Set(current === "" ? [] : current.split(","))
  next.add(documentId)
  get.set(syncedDocumentsAtom, [...next].toSorted().join(","))
})

export const createTask = runtime.fn<string>()(
  Effect.fnUntraced(function*(title, get) {
    const replica = yield* Replica.Replica
    const documentId = yield* CommandOutcome.committedOrFail(
      yield* replica.create(TaskDocument, {
        commandId: yield* Identity.makeCommandId,
        value: { title, labels: [] }
      })
    )
    get.set(syncDocument, documentId)
    return documentId
  })
)

export const addLabel = runtime.fn<{
  readonly documentId: Identity.DocumentId
  readonly label: string
}>()(
  Effect.fnUntraced(function*({ documentId, label }) {
    const replica = yield* Replica.Replica
    yield* CommandOutcome.committedOrFail(
      yield* replica.mutate(AddLabel, {
        commandId: yield* Identity.makeCommandId,
        documentId,
        payload: label
      })
    )
  })
)

export const readTask = runtime.fn<Identity.DocumentId>()(
  Effect.fnUntraced(function*(documentId) {
    const replica = yield* Replica.Replica
    const snapshot = yield* replica.get(TaskDocument, documentId)
    return { title: snapshot.value.title, labels: [...snapshot.value.labels] }
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
 * The transport layers wrap the whole session, not just `makeRpcClient`. Providing them around the
 * constructor alone builds the socket and its pump, hands back a client, then releases both when
 * the constructor returns, leaving every request waiting forever on a transport that is gone.
 */
const layerSession = (documentIds: ReadonlyArray<Identity.DocumentId>) =>
  Layer.effect(RelaySession)(
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const incarnation = (yield* gate.current).incarnation
      const client = yield* PeerRpc.makeRpcClient
      return yield* RpcPeerTransport.makeSession(client, transportOptions(identity, documentIds, incarnation))
    })
  ).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide(BrowserSocket.layerWebSocket(relayUrl)),
    Layer.provide(RpcSerialization.layerJson),
    Layer.provide(PeerAuthentication.layerClient),
    Layer.provide(
      Layer.succeed(PeerCredentials.PeerCredentials)({
        get: Effect.succeed(Redacted.make(identity.token))
      })
    )
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
    const session = yield* get.result(sessionAtom)
    yield* session.markDirty(documentId)
    yield* session.flush
  })
)
