import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as PresenceHub from "../src/PresenceHub.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const spaceD = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000004")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const spoofedClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

const update = (spaceId: Identity.SpaceId): Protocol.PresenceUpdate => ({
  spaceId,
  clientId,
  value: { cursor: 1 },
  ttlMillis: 5_000
})

describe("PresenceHub", () => {
  it.effect("authorizes tagged operations and includes the publishing client identity", () => {
    const inputs: Array<PresenceHub.AuthorizationInput> = []
    const live = PresenceHub.layer({
      maximumWatchersPerSpace: 16,
      authorize: (input) =>
        Effect.sync(() => {
          inputs.push(input)
        })
    })

    return Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const watcher = yield* hub.watch(spaceA, { subject: "reader" }).pipe(
        Stream.runHead,
        Effect.forkChild({ startImmediately: true })
      )
      yield* pipe(update(spaceA), (presence) => hub.publish(presence, { subject: "writer" }))
      const received = yield* Fiber.join(watcher)
      pipe(
        Option.getOrUndefined(received),
        (actual) => pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected))
      )

      assert.deepStrictEqual(inputs, [
        {
          _tag: "Watch",
          spaceId: spaceA,
          principal: { subject: "reader" }
        },
        {
          _tag: "Publish",
          spaceId: spaceA,
          clientId,
          principal: { subject: "writer" }
        }
      ])
    }).pipe(Effect.provide(live))
  })

  it.effect("uses an explicit trusted layer for applications without authorization", () =>
    Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
    }).pipe(Effect.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 16 }))))

  it.effect("rejects an invalid channel capacity during layer construction", () =>
    Effect.gen(function*() {
      const failure = yield* PresenceHub.layerTrusted({
        maximumWatchersPerSpace: 16,
        capacity: 0
      }).pipe(Layer.build, failureOf)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") assert.strictEqual(failure.option, "capacity")
    }))

  it.effect("rejects an invalid per-space watcher limit during layer construction", () =>
    Effect.gen(function*() {
      const failure = yield* PresenceHub.layerTrusted({
        maximumWatchersPerSpace: 0
      }).pipe(Layer.build, failureOf)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") {
        assert.strictEqual(failure.option, "maximumWatchersPerSpace")
      }
    }))

  it.effect("lets policy reject a spoofed publishing client identity", () => {
    const live = PresenceHub.layer({
      maximumWatchersPerSpace: 16,
      authorize: (input) => {
        if (input._tag === "Publish" && input.clientId !== clientId) {
          return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "client identity mismatch" }))
        }
        return Effect.void
      }
    })

    return Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const failure = yield* hub.publish({
        ...update(spaceA),
        clientId: spoofedClientId
      }, { subject: "writer" }).pipe(failureOf)

      assert.strictEqual(failure._tag, "AuthorizationDenied")
      if (failure._tag === "AuthorizationDenied") {
        assert.strictEqual(failure.reason, "client identity mismatch")
      }
    }).pipe(Effect.provide(live))
  })

  it.effect("routes a publish only through the matching space channel", () => {
    const live = PresenceHub.layer({
      maximumWatchersPerSpace: 16,
      authorize: () => Effect.void
    })

    return Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const watchers = yield* Effect.forEach(
        [spaceA, spaceB, spaceC, spaceD],
        (spaceId) =>
          hub.watch(spaceId, null).pipe(
            Stream.runHead,
            Effect.forkChild({ startImmediately: true })
          )
      )

      yield* hub.publish({ ...update(spaceA), value: { cursor: 2 } }, null)

      const received = yield* Fiber.join(watchers[0])
      pipe(Option.getOrUndefined(received)?.value, (value) => assert.deepStrictEqual(value, { cursor: 2 }))
      yield* Effect.forEach(
        [spaceB, spaceC, spaceD],
        (spaceId, index) => hub.publish({ ...update(spaceId), value: { cursor: index + 3 } }, null),
        { discard: true }
      )
      const isolated = yield* pipe(watchers.slice(1), Fiber.joinAll)
      pipe(
        isolated.map((entry) => Option.getOrUndefined(entry)?.value),
        (values) => assert.deepStrictEqual(values, [{ cursor: 3 }, { cursor: 4 }, { cursor: 5 }])
      )
    }).pipe(Effect.provide(live))
  })

  it.effect("rejects a presence payload beyond the protocol limit", () =>
    Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const failure = yield* hub.publish({
        ...update(spaceA),
        value: "x".repeat(Protocol.maximumPresenceBytes)
      }, null).pipe(failureOf)

      assert.strictEqual(failure._tag, "CapacityExceeded")
      if (failure._tag === "CapacityExceeded") {
        assert.strictEqual(failure.resource, "presence bytes")
        assert.strictEqual(failure.limit, Protocol.maximumPresenceBytes)
      }
    }).pipe(Effect.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 16 }))))

  it.effect("caps active presence watchers and releases slots on interruption", () =>
    Effect.gen(function*() {
      const live = PresenceHub.layerTrusted({
        capacity: 1,
        maximumWatchersPerSpace: 2
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const startActive = (spaceId: Identity.SpaceId, cursor: number) =>
          Effect.gen(function*() {
            const delivered = yield* Deferred.make<Protocol.PresenceUpdate>()
            const watcher = yield* hub.watch(spaceId, null).pipe(
              Stream.tap((presence) => Deferred.succeed(delivered, presence)),
              Stream.runDrain,
              Effect.forkChild({ startImmediately: true })
            )
            const expected = { ...update(spaceId), value: { cursor } }
            yield* hub.publish(expected, null)
            assert.deepStrictEqual(yield* Deferred.await(delivered), expected)
            return watcher
          })

        const first = yield* startActive(spaceA, 1)
        const second = yield* startActive(spaceA, 2)

        const failure = yield* hub.watch(spaceA, null).pipe(
          Stream.runDrain,
          failureOf
        )
        assert.strictEqual(failure._tag, "CapacityExceeded")
        if (failure._tag === "CapacityExceeded") {
          assert.strictEqual(failure.resource, "presence watchers")
          assert.strictEqual(failure.limit, 2)
        }

        const otherSpace = yield* startActive(spaceB, 3)

        yield* Fiber.interrupt(first)
        const replacement = yield* startActive(spaceA, 4)
        yield* Fiber.interruptAll([second, otherSpace, replacement])
      }).pipe(Effect.provide(live))
    }))

  it.effect("releases an acquired watcher permit when metric registration defects", () => {
    const registry = new Map<string, Metric.Metric.Metadata<any, any>>()
    return Effect.gen(function*() {
      const live = PresenceHub.layerTrusted({ maximumWatchersPerSpace: 2 })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const firstDelivered = yield* Deferred.make<Protocol.PresenceUpdate>()
        const first = yield* hub.watch(spaceA, null).pipe(
          Stream.tap((presence) => Deferred.succeed(firstDelivered, presence)),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
        pipe(
          yield* Deferred.await(firstDelivered),
          (actual) => pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected))
        )
        const active = (yield* Metric.snapshot).find(
          (snapshot) => snapshot.id === "effect_local_server_presence_watcher_count"
        )
        assert.isDefined(active)

        const metadata = pipe(registry.values(), (values) => Array.from(values)).find(
          (entry) => entry.id === "effect_local_server_presence_watcher_count"
        )
        assert.isDefined(metadata)
        const modify = metadata.hooks.modify.bind(metadata.hooks)
        let defectNextIncrement = true
        Object.defineProperty(metadata.hooks, "modify", {
          configurable: true,
          value: (input: number, context: never) => {
            if (input === 1 && defectNextIncrement) {
              defectNextIncrement = false
              assert.fail("metric registry defect")
            }
            return modify(input, context)
          }
        })

        const defective = yield* hub.watch(spaceA, null).pipe(Stream.runDrain, Effect.exit)
        pipe(Exit.isFailure(defective), (isFailure) => assert.isTrue(isFailure))

        const replacement = yield* hub.watch(spaceA, null).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
        const received = yield* Fiber.join(replacement)
        pipe(
          Option.getOrUndefined(received),
          (actual) => pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected))
        )
        yield* Fiber.interrupt(first)
      }).pipe(Effect.provide(live))
    }).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("releases presence watcher capacity after establishment authorization denial", () => {
    return Effect.gen(function*() {
      const live = PresenceHub.layer({
        maximumWatchersPerSpace: 1,
        authorize: (input) => {
          if (input._tag !== "Watch") return Effect.void
          if (input.principal === "denied") {
            return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "denied" }))
          }
          return Effect.void
        }
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const denied = yield* hub.watch(spaceA, "denied").pipe(Stream.runDrain, failureOf)
        assert.strictEqual(denied._tag, "AuthorizationDenied")

        const replacement = yield* hub.watch(spaceA, "allowed").pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
        pipe(
          Option.getOrUndefined(yield* Fiber.join(replacement)),
          (actual) => pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected))
        )
      }).pipe(Effect.provide(live))
    })
  })

  it.effect("does not reveal presence watcher occupancy to unauthorized principals", () =>
    Effect.gen(function*() {
      const live = PresenceHub.layer({
        maximumWatchersPerSpace: 1,
        authorize: (input) => {
          if (input._tag !== "Watch") return Effect.void
          if (input.principal === "denied") {
            return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "denied" }))
          }
          return Effect.void
        }
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const delivered = yield* Deferred.make<Protocol.PresenceUpdate>()
        const first = yield* hub.watch(spaceA, "allowed").pipe(
          Stream.tap((presence) => Deferred.succeed(delivered, presence)),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
        pipe(yield* Deferred.await(delivered), (actual) =>
          pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected)))

        const excess = yield* hub.watch(spaceA, "allowed").pipe(Stream.runHead, Effect.flip)
        assert.strictEqual(excess._tag, "CapacityExceeded")
        if (excess._tag === "CapacityExceeded") {
          assert.strictEqual(excess.resource, "presence watchers")
          assert.strictEqual(excess.limit, 1)
        }

        const denied = yield* hub.watch(spaceA, "denied").pipe(Stream.runHead, Effect.flip)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
        yield* Fiber.interrupt(first)
      }).pipe(Effect.provide(live))
    }))

  it.effect("releases presence watcher capacity after stream completion", () =>
    Effect.gen(function*() {
      const live = PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1 })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const first = yield* hub.watch(spaceA, null).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
        pipe(Option.getOrUndefined(yield* Fiber.join(first)), (actual) =>
          pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected)))

        const replacement = yield* hub.watch(spaceA, null).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        const next = { ...update(spaceA), value: { cursor: 2 } }
        yield* hub.publish(next, null)
        pipe(Option.getOrUndefined(yield* Fiber.join(replacement)), (actual) =>
          assert.deepStrictEqual(actual, next))
      }).pipe(Effect.provide(live))
    }))

  it.effect("records live presence watchers in each layer's metric registry", () => {
    const exercise = (registry: Map<string, Metric.Metric.Metadata<any, any>>) =>
      Effect.gen(function*() {
        const live = PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1 })

        yield* Effect.gen(function*() {
          const hub = yield* PresenceHub.PresenceHub
          const delivered = yield* Deferred.make<Protocol.PresenceUpdate>()
          const watcher = yield* hub.watch(spaceA, null).pipe(
            Stream.tap((presence) => Deferred.succeed(delivered, presence)),
            Stream.runDrain,
            Effect.forkChild({ startImmediately: true })
          )
          yield* pipe(update(spaceA), (presence) => hub.publish(presence, null))
          pipe(yield* Deferred.await(delivered), (actual) =>
            pipe(update(spaceA), (expected) => assert.deepStrictEqual(actual, expected)))

          const active = (yield* Metric.snapshot).find(
            (snapshot) =>
              snapshot.id === "effect_local_server_presence_watcher_count"
          )
          assert.isDefined(active)
          assert.strictEqual(active.type, "Gauge")
          if (active.type === "Gauge") assert.strictEqual(active.state.value, 1)

          yield* Fiber.interrupt(watcher)
          const released = (yield* Metric.snapshot).find(
            (snapshot) => snapshot.id === "effect_local_server_presence_watcher_count"
          )
          assert.isDefined(released)
          assert.strictEqual(released.type, "Gauge")
          if (released.type === "Gauge") assert.strictEqual(released.state.value, 0)
        }).pipe(Effect.provide(live))
      }).pipe(Effect.provideService(Metric.MetricRegistry, registry))

    return exercise(new Map()).pipe(Effect.andThen(exercise(new Map())))
  })
})
