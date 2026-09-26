import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
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
    const visibleSequences = yield* Ref.make(new Map<string, Protocol.PullPage["serverSequence"]>())
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
        let page = yield* server.pull(request)
        if (yield* faults.takePostReceiptPull(request.spaceId)) {
          yield* faults.emit({ _tag: "PullCompletedAfterReceipt", spaceId: request.spaceId })
        }
        if (!("_tag" in page)) {
          const key = `${request.spaceId}:${request.clientId}`
          const serverSequence = page.serverSequence
          if (yield* faults.shouldWithholdPullEvidence(request.spaceId)) {
            const changes: ReadonlyArray<Protocol.ViewChange> = []
            const previous = (yield* Ref.get(visibleSequences)).get(key) ??
              Identity.ServerSequence.make(0)
            page = Protocol.PullPage.make({
              ...page,
              serverSequence: previous,
              changes,
              contentBytes: yield* Protocol.encodedBytesEffect(changes),
              digest: yield* Protocol.viewChangesDigest(changes).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              ),
              hasMore: false
            })
          } else {
            yield* Ref.update(visibleSequences, (sequences) => {
              const next = new Map(sequences)
              next.set(key, serverSequence)
              return next
            })
          }
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
        server.watch(request).pipe(
          Stream.filterEffect(() => faults.state(request.spaceId).pipe(Effect.map((state) => state.online))),
          Stream.mapError(() => new ReplicaError.ServerUnavailable())
        )
    })
  })
)
