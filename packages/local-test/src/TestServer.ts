import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as FaultInjection from "./FaultInjection.js"

export const layer: Layer.Layer<SyncEngine.SyncEngine, never, ServerStore.ServerStore | FaultInjection.FaultInjection> =
  Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const server = yield* ServerStore.ServerStore
      const faults = yield* FaultInjection.FaultInjection
      const online = (spaceId: Protocol.SubmitRequest["envelope"]["spaceId"]) =>
        faults.state(spaceId).pipe(Effect.filterOrFail(
          (state) => state.online,
          () => new ReplicaError.ServerUnavailable()
        ))
      return SyncEngine.SyncEngine.of({
        submit: (request) =>
          Effect.gen(function*() {
            yield* online(request.envelope.spaceId)
            const receipt = yield* server.submit(request)
            if (yield* faults.takeDroppedReceipt(request.envelope.spaceId)) {
              return yield* new ReplicaError.ServerUnavailable()
            }
            return receipt
          }),
        pull: (request) =>
          Effect.gen(function*() {
            yield* online(request.spaceId)
            const page = yield* server.pull(request)
            if (
              "_tag" in page ||
              !(yield* faults.takeDuplicatePage(request.spaceId)) ||
              page.entries.length === 0
            ) return page
            return {
              entries: [page.entries[0], ...page.entries].slice(0, Protocol.maximumBatchEntries),
              hasMore: page.hasMore
            }
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
