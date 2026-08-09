import { assert, describe, it } from "@effect/vitest"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as AppLifecycle from "../src/AppLifecycle.js"
import { AppState } from "./helpers/ReactNative.js"

// The lifecycle module's contract with its environment is exactly `Replica.flush`,
// so the replica service is stubbed at that boundary and the AppState native module
// is faked by the aliased helper.
const replicaStub = (flush: Effect.Effect<void, ReplicaError.ReplicaError>) =>
  Layer.succeed(Replica.Replica, { flush } as unknown as Replica.Replica["Service"])

const lifecycleLayer = (flush: Effect.Effect<void, ReplicaError.ReplicaError>) =>
  AppLifecycle.layerFlushOnBackground.pipe(Layer.provide(replicaStub(flush)))

describe("AppLifecycle", () => {
  it.effect("flushes the replica when the app transitions to background", () =>
    Effect.gen(function*() {
      const flushed = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(Deferred.succeed(flushed, undefined))), scope)
      AppState.__setState("background")
      yield* Deferred.await(flushed)
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("does not flush on active or inactive transitions", () =>
    Effect.gen(function*() {
      // Stream.runForEach processes statuses sequentially, so awaiting the background
      // flush proves every earlier transition was already processed without flushing.
      let flushes = 0
      const flushed = yield* Deferred.make<void>()
      const flush = Effect.sync(() => {
        flushes++
      }).pipe(Effect.andThen(Deferred.succeed(flushed, undefined)))
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(flush)), scope)
      AppState.__setState("inactive")
      AppState.__setState("active")
      AppState.__setState("inactive")
      AppState.__setState("background")
      yield* Deferred.await(flushed)
      assert.strictEqual(flushes, 1)
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("survives a failed flush and flushes again on the next background transition", () =>
    Effect.gen(function*() {
      const attempted = yield* Deferred.make<void>()
      const succeeded = yield* Deferred.make<void>()
      let failed = false
      const flush = Effect.suspend(() => {
        if (!failed) {
          failed = true
          return Deferred.succeed(attempted, undefined).pipe(
            Effect.andThen(Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause: new Error("simulated storage outage") })
              })
            ))
          )
        }
        return Deferred.succeed(succeeded, undefined)
      })
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(flush)), scope)
      AppState.__setState("active")
      AppState.__setState("background")
      yield* Deferred.await(attempted)
      AppState.__setState("active")
      AppState.__setState("background")
      yield* Deferred.await(succeeded)
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("removes the AppState subscription when the layer scope closes", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const before = AppState.__listenerCount()
      yield* Scope.provide(Layer.build(lifecycleLayer(Effect.void)), scope)
      assert.strictEqual(AppState.__listenerCount(), before + 1)
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(AppState.__listenerCount(), before)
    }))
})
