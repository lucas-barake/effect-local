import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
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
    const online = faults.state.pipe(Effect.filterOrFail(
      (state) => state.online,
      () => new ReplicaError.ProtocolInvalid({ message: "The test network is partitioned" })
    ))
    return SyncEngine.SyncEngine.of({
      submit: (envelope) =>
        Effect.gen(function*() {
          yield* online
          const receipt = yield* server.submit(envelope)
          if (yield* faults.takeDroppedReceipt) {
            return yield* new ReplicaError.ProtocolInvalid({ message: "The test network is partitioned" })
          }
          return receipt
        }),
      pull: (request) =>
        Effect.gen(function*() {
          yield* online
          const page = yield* server.pull(request)
          if (
            "_tag" in page || !(yield* faults.takeDuplicatePage) || page.changes.length === 0 ||
            page.changes.length === Protocol.maximumBatchEntries
          ) return page
          const changes = [page.changes[0], ...page.changes]
          const contentBytes = yield* Protocol.encodedBytesEffect(changes)
          const duplicate = Protocol.PullPage.make({
            ...page,
            changes,
            contentBytes,
            digest: Protocol.MutationDigest.make(
              yield* Canonical.digest({ format: 1, changes }).pipe(
                Effect.provideService(Crypto.Crypto, crypto)
              )
            )
          })
          if (Protocol.encodedBytes(duplicate) > Protocol.maximumBatchBytes) return page
          return duplicate
        }),
      bootstrap: (request) => online.pipe(Effect.andThen(server.bootstrap(request))),
      watch: (spaceId) =>
        server.watch(spaceId).pipe(
          Stream.filterEffect(() => faults.state.pipe(Effect.map((state) => state.online))),
          Stream.mapError(() => new ReplicaError.ProtocolInvalid({ message: "The test network is partitioned" }))
        )
    })
  })
)
