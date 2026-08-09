import { assert, describe, it } from "@effect/vitest"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as AppLifecycle from "../src/AppLifecycle.js"
import * as ExpoSqlite from "../src/ExpoSqlite.js"
import * as ReactNativeCrypto from "../src/ReactNativeCrypto.js"
import * as ReactNativeReplica from "../src/ReactNativeReplica.js"
import { AddLabelLive, definition, LabelsSql, limits, ListLabelsLive } from "./fixtures.js"
import { AppState } from "./helpers/ReactNative.js"

const Database = ExpoSqlite.layer({ databaseName: ":memory:" })
const Dependencies = Layer.mergeAll(Database, ReactNativeCrypto.layer, ReplicaLimits.layer(limits))
const DomainLive = Layer.mergeAll(AddLabelLive, ListLabelsLive.pipe(Layer.provide(Database)))
const ReplicaLive = ReactNativeReplica.layer(definition, { projections: [LabelsSql] }).pipe(
  Layer.provide(DomainLive),
  Layer.provideMerge(Dependencies)
)

const lifecycleLayer = (
  observe: (
    flush: Effect.Effect<void, ReplicaError.ReplicaError>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
) => {
  const observedReplica = Layer.effect(
    Replica.Replica,
    Replica.Replica.pipe(
      Effect.map((replica) => Replica.Replica.of({ ...replica, flush: observe(replica.flush) }))
    )
  ).pipe(Layer.provide(ReplicaLive))
  return AppLifecycle.layerFlushOnBackground.pipe(Layer.provide(observedReplica))
}

describe("AppLifecycle", () => {
  it.effect("flushes the replica when the app transitions to background", () =>
    Effect.gen(function*() {
      const flushed = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Scope.provide(
        Layer.build(lifecycleLayer((flush) => flush.pipe(Effect.tap(() => Deferred.succeed(flushed, undefined))))),
        scope
      )
      AppState.__setState("background")
      yield* Deferred.await(flushed)
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("does not flush on active or inactive transitions", () =>
    Effect.gen(function*() {
      let flushes = 0
      const flushed = yield* Deferred.make<void>()
      const observe = (flush: Effect.Effect<void, ReplicaError.ReplicaError>) =>
        flush.pipe(
          Effect.tap(() => Effect.sync(() => flushes++)),
          Effect.tap(() => Deferred.succeed(flushed, undefined))
        )
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(observe)), scope)
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
      const observe = (flush: Effect.Effect<void, ReplicaError.ReplicaError>) =>
        Effect.suspend(() => {
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
          return flush.pipe(Effect.tap(() => Deferred.succeed(succeeded, undefined)))
        })
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(observe)), scope)
      AppState.__setState("active")
      AppState.__setState("background")
      yield* Deferred.await(attempted)
      AppState.__setState("active")
      AppState.__setState("background")
      yield* Deferred.await(succeeded)
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("coalesces background transitions while a flush is running", () =>
    Effect.gen(function*() {
      const firstStarted = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const secondFinished = yield* Deferred.make<void>()
      const extraFlush = yield* Deferred.make<void>()
      let flushes = 0
      const observe = (flush: Effect.Effect<void, ReplicaError.ReplicaError>) =>
        Effect.suspend(() => {
          flushes++
          if (flushes === 1) {
            return Deferred.succeed(firstStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFirst)),
              Effect.andThen(flush)
            )
          }
          if (flushes === 2) return flush.pipe(Effect.tap(() => Deferred.succeed(secondFinished, undefined)))
          return flush.pipe(Effect.tap(() => Deferred.succeed(extraFlush, undefined)))
        })
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(lifecycleLayer(observe)), scope)
      AppState.__setState("background")
      yield* Deferred.await(firstStarted)
      AppState.__setState("background")
      AppState.__setState("background")
      AppState.__setState("background")
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Deferred.await(secondFinished)
      const extra = yield* Deferred.await(extraFlush).pipe(
        Effect.timeoutOption("1 second"),
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust("1 second")
      assert.isTrue(Option.isNone(yield* Fiber.join(extra)))
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("removes the AppState subscription when the layer scope closes", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const before = AppState.__listenerCount()
      yield* Scope.provide(Layer.build(lifecycleLayer((flush) => flush)), scope)
      assert.strictEqual(AppState.__listenerCount(), before + 1)
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(AppState.__listenerCount(), before)
    }))
})
