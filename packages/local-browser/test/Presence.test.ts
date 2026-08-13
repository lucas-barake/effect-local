import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as TestClock from "effect/testing/TestClock"
import * as Presence from "../src/Presence.js"

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const Payload = Schema.Struct({ cursor: Schema.Number, status: Schema.Literals(["active", "idle"]) })

const gatedPayload = Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const schema = Payload.pipe(Schema.decode({
    decode: SchemaGetter.transformOrFail((value: typeof Payload.Type) => {
      if (value.cursor !== 1) return Effect.succeed(value)
      return Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as(value)
      )
    }),
    encode: SchemaGetter.passthrough()
  }))
  return { schema, started, release }
})

describe("Presence", () => {
  it.effect("validates time to live", () =>
    Effect.gen(function*() {
      const result = yield* Presence.make(Payload, { timeToLive: Number.POSITIVE_INFINITY }).pipe(Effect.exit)
      const error = pipe(
        result,
        Exit.match({
          onFailure: Cause.findErrorOption,
          onSuccess: () => Option.none()
        }),
        Option.getOrThrow
      )
      assert.strictEqual(error._tag, "ProtocolInvalid")
    }))

  it.effect("decodes and expires values", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const expiresAtMillis = (yield* Clock.currentTimeMillis) + 1_000
      yield* presence.receive(clientId, { cursor: 1, status: "active" })
      assert.deepStrictEqual(yield* presence.values, [{
        clientId,
        value: { cursor: 1, status: "active" },
        expiresAtMillis
      }])
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("keeps the newer value when an older decode completes later", () =>
    Effect.gen(function*() {
      const gate = yield* gatedPayload
      const presence = yield* Presence.make(gate.schema, { timeToLive: "1 second" })
      const older = yield* presence.receive(clientId, { cursor: 1, status: "active" }).pipe(Effect.forkChild)
      yield* Deferred.await(gate.started)
      yield* presence.receive(clientId, { cursor: 2, status: "idle" })
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(older)
      pipe(
        (yield* presence.values).map((entry) => entry.value),
        (values) => assert.deepStrictEqual(values, [{ cursor: 2, status: "idle" }])
      )
    }))

  it.effect("does not let an in-flight value survive explicit removal", () =>
    Effect.gen(function*() {
      const gate = yield* gatedPayload
      const presence = yield* Presence.make(gate.schema, { timeToLive: "1 second" })
      const receive = yield* presence.receive(clientId, { cursor: 1, status: "active" }).pipe(Effect.forkChild)
      yield* Deferred.await(gate.started)
      yield* presence.remove(clientId)
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(receive)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("keeps a receive that arrived after a scoped publication", () =>
    Effect.gen(function*() {
      const gate = yield* gatedPayload
      const presence = yield* Presence.make(gate.schema, { timeToLive: "1 second" })
      yield* Effect.gen(function*() {
        yield* presence.publish(clientId, { cursor: 0, status: "active" })
        const receive = yield* presence.receive(clientId, { cursor: 1, status: "idle" }).pipe(Effect.forkChild)
        yield* Deferred.await(gate.started)
        yield* Effect.addFinalizer(() => Fiber.join(receive).pipe(Effect.orDie, Effect.asVoid))
        yield* Effect.addFinalizer(() => Deferred.succeed(gate.release, undefined).pipe(Effect.asVoid))
      }).pipe(Effect.scoped)
      pipe(
        (yield* presence.values).map((entry) => entry.value),
        (values) => assert.deepStrictEqual(values, [{ cursor: 1, status: "idle" }])
      )
    }))

  it.effect("does not let an invalid later payload suppress an earlier valid one", () =>
    Effect.gen(function*() {
      const gate = yield* gatedPayload
      const presence = yield* Presence.make(gate.schema, { timeToLive: "1 second" })
      const valid = yield* presence.receive(clientId, { cursor: 1, status: "active" }).pipe(Effect.forkChild)
      yield* Deferred.await(gate.started)
      const result = yield* presence.receive(clientId, { cursor: "invalid", status: "idle" }).pipe(Effect.exit)
      const error = pipe(
        result,
        Exit.match({
          onFailure: Cause.findErrorOption,
          onSuccess: () => Option.none()
        }),
        Option.getOrThrow
      )
      assert.strictEqual(error._tag, "ProtocolInvalid")
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(valid)
      pipe(
        (yield* presence.values).map((entry) => entry.value),
        (values) => assert.deepStrictEqual(values, [{ cursor: 1, status: "active" }])
      )
    }))
})
