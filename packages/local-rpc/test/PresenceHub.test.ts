import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
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
    }).pipe(Effect.provide(PresenceHub.layerTrusted())))

  it.effect("rejects an invalid channel capacity during layer construction", () =>
    Effect.gen(function*() {
      const failure = yield* Layer.build(PresenceHub.layerTrusted({ capacity: 0 })).pipe(Effect.flip)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") assert.strictEqual(failure.option, "capacity")
    }))

  it.effect("lets policy reject a spoofed publishing client identity", () => {
    const live = PresenceHub.layer({
      authorize: (input) =>
        input._tag === "Publish" && input.clientId !== clientId
          ? Effect.fail("client identity mismatch")
          : Effect.void
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
      assert.deepStrictEqual(received._tag === "Some" ? received.value.value : undefined, { cursor: 2 })
      yield* Effect.forEach(
        [spaceB, spaceC, spaceD],
        (spaceId, index) => hub.publish({ ...update(spaceId), value: { cursor: index + 3 } }, null),
        { discard: true }
      )
      const isolated = yield* Fiber.joinAll(watchers.slice(1))
      assert.deepStrictEqual(
        isolated.map((entry) => entry._tag === "Some" ? entry.value.value : undefined),
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
    }).pipe(Effect.provide(PresenceHub.layerTrusted())))
})
