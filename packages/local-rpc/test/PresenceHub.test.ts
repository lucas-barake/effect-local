import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import * as PresenceHub from "../src/PresenceHub.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const spaceD = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000004")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const spoofedClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")

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
      yield* Fiber.interrupt(watcher)
      yield* hub.publish(update(spaceA), { subject: "writer" })

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
      yield* hub.publish(update(spaceA), null)
    }).pipe(Effect.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 16 }))))

  it.effect("rejects an invalid channel capacity during layer construction", () =>
    Effect.gen(function*() {
      const failure = yield* Layer.build(PresenceHub.layerTrusted({
        maximumWatchersPerSpace: 16,
        capacity: 0
      })).pipe(Effect.flip)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") assert.strictEqual(failure.option, "capacity")
    }))

  it.effect("rejects an invalid per-space watcher limit during layer construction", () =>
    Effect.gen(function*() {
      const failure = yield* Layer.build(PresenceHub.layerTrusted({
        maximumWatchersPerSpace: 0
      })).pipe(Effect.flip)
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
          return Effect.fail("client identity mismatch")
        }
        return Effect.void
      }
    })

    return Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const failure = yield* hub.publish({
        ...update(spaceA),
        clientId: spoofedClientId
      }, { subject: "writer" }).pipe(Effect.flip)

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
      assert.deepStrictEqual(Option.getOrUndefined(received)?.value, { cursor: 2 })
      yield* Effect.forEach(
        [spaceB, spaceC, spaceD],
        (spaceId, index) => hub.publish({ ...update(spaceId), value: { cursor: index + 3 } }, null),
        { discard: true }
      )
      const isolated = yield* Fiber.joinAll(watchers.slice(1))
      assert.deepStrictEqual(
        isolated.map((entry) => Option.getOrUndefined(entry)?.value),
        [{ cursor: 3 }, { cursor: 4 }, { cursor: 5 }]
      )
    }).pipe(Effect.provide(live))
  })

  it.effect("rejects a presence payload beyond the protocol limit", () =>
    Effect.gen(function*() {
      const hub = yield* PresenceHub.PresenceHub
      const failure = yield* hub.publish({
        ...update(spaceA),
        value: "x".repeat(Protocol.maximumPresenceBytes)
      }, null).pipe(Effect.flip)

      assert.strictEqual(failure._tag, "CapacityExceeded")
      if (failure._tag === "CapacityExceeded") {
        assert.strictEqual(failure.resource, "presence bytes")
        assert.strictEqual(failure.limit, Protocol.maximumPresenceBytes)
      }
    }).pipe(Effect.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 16 }))))

  it.effect("caps active presence watchers and releases slots on interruption", () =>
    Effect.gen(function*() {
      const authorized = yield* Queue.unbounded<void>()
      const live = PresenceHub.layer({
        capacity: 1,
        maximumWatchersPerSpace: 2,
        authorize: (input) => {
          if (input._tag !== "Watch") return Effect.void
          return Queue.offer(authorized, undefined).pipe(Effect.asVoid)
        }
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const first = yield* hub.watch(spaceA, { subject: "first" }).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)
        const second = yield* hub.watch(spaceA, { subject: "second" }).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)

        const failure = yield* hub.watch(spaceA, { subject: "excess" }).pipe(
          Stream.runDrain,
          Effect.flip
        )
        assert.strictEqual(failure._tag, "CapacityExceeded")
        if (failure._tag === "CapacityExceeded") {
          assert.strictEqual(failure.resource, "presence watchers")
          assert.strictEqual(failure.limit, 2)
        }

        const otherSpace = yield* hub.watch(spaceB, { subject: "other-space" }).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)

        yield* Fiber.interrupt(first)
        const replacement = yield* hub.watch(spaceA, { subject: "replacement" }).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)
        yield* Fiber.interruptAll([second, otherSpace, replacement])
      }).pipe(Effect.provide(live))
    }))

  it.effect("releases presence watcher capacity after establishment authorization denial", () => {
    return Effect.gen(function*() {
      const replacementAuthorized = yield* Deferred.make<void>()
      const live = PresenceHub.layer({
        maximumWatchersPerSpace: 1,
        authorize: (input) => {
          if (input._tag !== "Watch") return Effect.void
          if (input.principal === "denied") return Effect.fail("denied")
          return Deferred.succeed(replacementAuthorized, undefined).pipe(Effect.asVoid)
        }
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const denied = yield* hub.watch(spaceA, "denied").pipe(Stream.runDrain, Effect.flip)
        assert.strictEqual(denied._tag, "AuthorizationDenied")

        const replacement = yield* hub.watch(spaceA, "allowed").pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(replacementAuthorized)
        yield* Fiber.interrupt(replacement)
      }).pipe(Effect.provide(live))
    })
  })

  it.effect("releases presence watcher capacity after stream completion", () =>
    Effect.gen(function*() {
      const authorized = yield* Queue.unbounded<void>()
      const live = PresenceHub.layer({
        maximumWatchersPerSpace: 1,
        authorize: (input) => {
          if (input._tag !== "Watch") return Effect.void
          return Queue.offer(authorized, undefined).pipe(Effect.asVoid)
        }
      })

      yield* Effect.gen(function*() {
        const hub = yield* PresenceHub.PresenceHub
        const first = yield* hub.watch(spaceA, null).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)
        yield* hub.publish(update(spaceA), null)
        yield* Fiber.join(first)

        const replacement = yield* hub.watch(spaceA, null).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(authorized)
        yield* Fiber.interrupt(replacement)
      }).pipe(Effect.provide(live))
    }))

  it.effect("records live presence watchers in each layer's metric registry", () => {
    const exercise = (registry: Map<string, Metric.Metric.Metadata<any, any>>) =>
      Effect.gen(function*() {
        const authorized = yield* Deferred.make<void>()
        const live = PresenceHub.layer({
          maximumWatchersPerSpace: 1,
          authorize: (input) => {
            if (input._tag !== "Watch") return Effect.void
            return Deferred.succeed(authorized, undefined).pipe(Effect.asVoid)
          }
        })

        yield* Effect.gen(function*() {
          const hub = yield* PresenceHub.PresenceHub
          const watcher = yield* hub.watch(spaceA, null).pipe(
            Stream.runDrain,
            Effect.forkChild({ startImmediately: true })
          )
          yield* Deferred.await(authorized)

          const active = (yield* Metric.snapshot).find(
            (snapshot) => snapshot.id === "effect_local_server_presence_watcher_count"
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
