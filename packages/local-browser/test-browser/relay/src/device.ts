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
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import { AddLabel, definition, DomainLive, limits, TaskDocument } from "./domain.ts"
import type { DeviceIdentity } from "./identities.ts"
import { deviceByName, devices } from "./identities.ts"

/**
 * One device, whole.
 *
 * This is the composition the store and forward document describes, written out as a real browser
 * consumer rather than as prose: a relay flavoured replica, the SQL relay client runtime on top of
 * it, and a `PeerRpc.RpcClient` over the platform's WebSocket. Nothing here is a test double - the
 * only thing the fixture supplies that a product would supply differently is where the credential
 * comes from.
 */

const relayUrl = `ws://127.0.0.1:${import.meta.env.VITE_RELAY_PORT ?? "4176"}/relay`

/** The OPFS worker on the far end of the SQL client's `MessagePort`. */
const databasePort = (dbName: string) => {
  const channel = new MessageChannel()
  const worker = new Worker(new URL("./opfs.worker.ts", import.meta.url), { type: "module" })
  worker.postMessage({ databasePort: channel.port2, dbName }, [channel.port2])
  return channel.port1
}

const makeRuntime = (identity: DeviceIdentity) => {
  const Base = Layer.mergeAll(
    BrowserSqlite.layerMessagePort(databasePort(`relay-${identity.name}.sqlite`)),
    BrowserCrypto.layer,
    ReplicaLimits.layer(limits),
    PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults),
    PeerRelayOutboxLimits.layer(PeerRelayOutboxLimits.defaults)
  )

  // `layerRelayWithBindings`, not `layerWithBindings`. The direct constructor builds a direct
  // `PeerSync`, and `PeerRelayClientRuntime` refuses to build on one.
  const ReplicaLive = SqlReplica.layerRelayWithBindings(definition, { projections: [] }).pipe(
    Layer.provide(DomainLive),
    Layer.provideMerge(Base),
    Layer.orDie
  )

  return ManagedRuntime.make(
    PeerRelayClientRuntime.layerSql.pipe(Layer.provideMerge(ReplicaLive), Layer.orDie)
  )
}

const remoteOf = (identity: DeviceIdentity) => {
  const other = devices.find((device) => device.name !== identity.name)
  if (other === undefined) throw new Error("the fixture needs two devices")
  return other
}

export interface DeviceApi {
  readonly createTask: (title: string) => Promise<string>
  readonly addLabel: (documentId: string, label: string) => Promise<void>
  readonly readTask: (documentId: string) => Promise<{ title: string; labels: Array<string> }>
  readonly exportBackup: () => Promise<Array<number>>
  readonly restoreBackup: (bytes: Array<number>) => Promise<void>
  readonly connect: (documentId: string) => Promise<void>
  readonly push: (documentId: string) => Promise<void>
}

export const start = (name: string): DeviceApi => {
  const identity = deviceByName(name)
  const remote = remoteOf(identity)
  const runtime = makeRuntime(identity)

  // Held so `push` can reach the live session that `connect` opened. The session's own scope stays
  // open for the lifetime of the page, which is what a real consumer wants too.
  let session: PeerSession.SupervisedPeerSession | undefined

  return {
    createTask: (title) =>
      runtime.runPromise(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const outcome = yield* replica.create(TaskDocument, {
          commandId: yield* Identity.makeCommandId,
          value: { title, labels: [] }
        })
        if (outcome._tag !== "DurablyCommittedLocal") throw new Error(`create: ${outcome._tag}`)
        return outcome.value as string
      })),

    addLabel: (documentId, label) =>
      runtime.runPromise(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        yield* replica.mutate(AddLabel, {
          commandId: yield* Identity.makeCommandId,
          documentId: Identity.DocumentId.make(documentId),
          payload: label
        })
      })),

    readTask: (documentId) =>
      runtime.runPromise(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const snapshot = yield* replica.get(TaskDocument, Identity.DocumentId.make(documentId))
        return {
          title: snapshot.value.title,
          labels: [...snapshot.value.labels]
        }
      })),

    exportBackup: () =>
      runtime.runPromise(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const chunks = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
        const joined = new Uint8Array(total)
        let offset = 0
        for (const chunk of chunks) {
          joined.set(chunk, offset)
          offset += chunk.byteLength
        }
        return [...joined]
      })),

    restoreBackup: (bytes) =>
      runtime.runPromise(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "clone",
          source: Stream.make(new Uint8Array(bytes))
        })
      })),

    connect: (documentId) =>
      runtime.runPromise(Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const incarnation = (yield* gate.current).incarnation
        const ready = yield* Deferred.make<PeerSession.SupervisedPeerSession, unknown>()

        // The transport layers are provided to the whole session-running effect, not to
        // `makeRpcClient` alone. Providing them only around the constructor builds the socket and
        // its pump, hands back a client, and then releases both as soon as the constructor returns
        // - leaving a client whose every request waits forever on a transport that is already gone,
        // with no socket ever opened and nothing to see in a log.
        //
        // Forked and left running: the session owns a live websocket and a receive loop, so it has
        // to outlive the call that opened it.
        yield* Effect.gen(function*() {
          const client = yield* PeerRpc.makeRpcClient
          const live = yield* RpcPeerTransport.makeSession(client, {
            expectedLocal: identity.principal,
            senderReplicaIncarnation: incarnation,
            expectedRelayPeerId: identity.relayPeerId,
            remote: { subjectId: remote.principal.subjectId, peerId: remote.principal.peerId },
            documents: [{ document: TaskDocument, documentId: Identity.DocumentId.make(documentId) }],
            definition,
            receiptRetentionMillis: PeerRelayReceiptLimits.defaults.receiptRetentionMillis,
            senderRetryHorizonMillis: Duration.toMillis(Duration.days(7)),
            replayBatchSize: 16
          })
          yield* Deferred.succeed(ready, live)
          yield* Effect.never
        }).pipe(
          Effect.provide(RpcClient.layerProtocolSocket()),
          Effect.provide(BrowserSocket.layerWebSocket(relayUrl)),
          Effect.provide(RpcSerialization.layerJson),
          Effect.provide(PeerAuthentication.layerClient),
          Effect.provideService(PeerCredentials.PeerCredentials, {
            get: Effect.succeed(Redacted.make(identity.token))
          }),
          Effect.scoped,
          // Without this a session that fails to open leaves `connect` waiting forever instead of
          // reporting why, which is the difference between a diagnosable failure and a hang.
          Effect.onError((cause) => Deferred.failCause(ready, cause)),
          Effect.forkDetach
        )

        session = yield* Deferred.await(ready)
      })),

    push: (documentId) =>
      runtime.runPromise(Effect.gen(function*() {
        const live = session
        if (live === undefined) throw new Error("connect() first")
        yield* live.markDirty(Identity.DocumentId.make(documentId))
        yield* live.flush
      }))
  }
}
