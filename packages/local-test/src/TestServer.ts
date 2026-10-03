import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as FaultInjection from "./FaultInjection.js"

export const layer: Layer.Layer<
  SyncEngine.SyncEngine,
  never,
  ServerStore.ServerStore | FaultInjection.FaultInjection | Crypto.Crypto
> = Layer.effect(
  SyncEngine.SyncEngine,
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const faults = yield* FaultInjection.FaultInjection
    const crypto = yield* Crypto.Crypto
    const online = Effect.fnUntraced(function*(spaceId: Protocol.SubmitRequest["envelope"]["spaceId"]) {
      if (!(yield* faults.state(spaceId)).online) {
        yield* faults.emit({ _tag: "RequestRejectedOffline", spaceId })
        yield* new ReplicaError.ServerUnavailable()
      }
    })
    return SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      transportGeneration: Effect.succeed(0),
      waitForTransportChange: () => Effect.never,
      submitBatch: Effect.fnUntraced(function*(request) {
        const spaceId = request.envelopes[0].spaceId
        yield* online(spaceId)
        const result = yield* server.admitBatch(request, null)
        yield* Effect.forEach(
          result.receipts,
          (receipt) => faults.emit({ _tag: "ReceiptCommitted", spaceId, receipt }),
          { discard: true }
        )
        if (yield* faults.takeDroppedReceipt(spaceId)) {
          yield* Effect.forEach(
            result.receipts,
            (receipt) => faults.emit({ _tag: "ReceiptDropped", spaceId, receipt }),
            { discard: true }
          )
          return yield* new ReplicaError.ServerUnavailable()
        }
        yield* faults.awaitReceiptRelease(spaceId)
        if (yield* faults.takePartitionAfterReceipt(spaceId)) {
          yield* faults.partition(spaceId)
        }
        yield* Effect.forEach(
          result.receipts,
          (receipt) => faults.emit({ _tag: "ReceiptReturned", spaceId, receipt }),
          { discard: true }
        )
        yield* faults.markReceiptReturned(spaceId)
        return result
      }),
      discard: (request) => online(request.envelope.spaceId).pipe(Effect.andThen(server.discard(request, null))),
      pull: Effect.fnUntraced(function*(request) {
        yield* online(request.spaceId)
        const page = yield* server.pull(request)
        yield* faults.awaitPullEvidenceRelease(request.spaceId)
        if (yield* faults.takePostReceiptPull(request.spaceId)) {
          yield* faults.emit({ _tag: "PullCompletedAfterReceipt", spaceId: request.spaceId })
        }
        if (
          "_tag" in page ||
          !(yield* faults.takeDuplicatePage(request.spaceId)) ||
          page.changes.length === 0 ||
          page.changes.length === Protocol.maximumBatchEntries
        ) return page
        const changes = [page.changes[0], ...page.changes]
        const duplicate = Protocol.PullPage.make({
          ...page,
          changes,
          contentBytes: yield* Protocol.encodedBytesEffect(changes),
          digest: yield* Protocol.viewChangesDigest(changes).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
        })
        if (Protocol.encodedBytes(duplicate) > Protocol.maximumBatchBytes) return page
        return duplicate
      }),
      bootstrap: (request) => online(request.spaceId).pipe(Effect.andThen(server.bootstrap(request))),
      watch: (request) =>
        online(request.spaceId).pipe(
          Effect.as(
            server.watch(request).pipe(
              Stream.filterEffect(() => faults.state(request.spaceId).pipe(Effect.map((state) => state.online)))
            )
          ),
          Stream.unwrap
        )
    })
  })
)
