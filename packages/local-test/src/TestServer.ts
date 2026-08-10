import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as FaultInjection from "./FaultInjection.js"

const offline = () => new ReplicaError.ProtocolInvalid({ message: "The test network is partitioned" })

export const layer: Layer.Layer<SyncEngine.SyncEngine, never, ServerStore.ServerStore | FaultInjection.FaultInjection> =
  Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const server = yield* ServerStore.ServerStore
      const faults = yield* FaultInjection.FaultInjection
      const online = faults.state.pipe(Effect.filterOrFail((state) => state.online, offline))
      return SyncEngine.SyncEngine.of({
        submit: (envelope) =>
          Effect.gen(function*() {
            yield* online
            const receipt = yield* server.submit(envelope)
            if (yield* faults.takeDroppedReceipt) return yield* offline()
            return receipt
          }),
        pull: (request) =>
          Effect.gen(function*() {
            yield* online
            const page = yield* server.pull(request)
            if (!(yield* faults.takeDuplicatePage) || page.entries.length === 0) return page
            return {
              entries: [page.entries[0]!, ...page.entries].slice(0, Protocol.maximumBatchEntries),
              hasMore: page.hasMore
            }
          }),
        watch: (spaceId) =>
          server.watch(spaceId).pipe(
            Stream.filterEffect(() => faults.state.pipe(Effect.map((state) => state.online))),
            Stream.mapError(() => offline())
          )
      })
    })
  )
