import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Redacted from "effect/Redacted"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Authentication from "../src/Authentication.js"

describe("credential providers", () => {
  it.effect(
    "static provider serves the bearer at generation 0 and never observes a change",
    Effect.fnUntraced(function*() {
      const layerStatic = Authentication.layerCredentialProviderStatic(Redacted.make("secret"))
      const provider = yield* Authentication.CredentialProvider.pipe(Effect.provide(layerStatic))
      const credential = yield* provider.acquire
      assert.strictEqual(credential.generation, 0)
      assert.strictEqual(Redacted.value(credential.bearer), "secret")

      const waiting = yield* Effect.forkChild(provider.awaitChange(0), { startImmediately: true })
      yield* Effect.yieldNow
      assert.strictEqual(waiting.pollUnsafe(), undefined)
      yield* Fiber.interrupt(waiting)
    })
  )

  it.effect(
    "ref provider resolves awaitChange only once the generation differs from the rejected one",
    Effect.fnUntraced(function*() {
      const credentials = yield* SubscriptionRef.make<Authentication.Credential>({
        generation: 0,
        bearer: Redacted.make("first")
      })
      const provider = yield* Authentication.CredentialProvider.pipe(
        Effect.provide(Authentication.layerCredentialProvider(credentials))
      )
      assert.strictEqual(Redacted.value((yield* provider.acquire).bearer), "first")

      const waiting = yield* Effect.forkChild(provider.awaitChange(0), { startImmediately: true })
      yield* Effect.yieldNow
      assert.strictEqual(waiting.pollUnsafe(), undefined)

      // Same generation, different bearer: still the rejected credential.
      yield* SubscriptionRef.set(credentials, { generation: 0, bearer: Redacted.make("still-first") })
      yield* Effect.yieldNow
      assert.strictEqual(waiting.pollUnsafe(), undefined)

      yield* SubscriptionRef.set(credentials, { generation: 1, bearer: Redacted.make("second") })
      const rotated = yield* Fiber.join(waiting)
      assert.strictEqual(rotated.generation, 1)
      assert.strictEqual(Redacted.value(rotated.bearer), "second")
      assert.strictEqual(Redacted.value((yield* provider.acquire).bearer), "second")

      // A rejection of the current generation waits for the next rotation, not the replayed value.
      const again = yield* Effect.forkChild(provider.awaitChange(1), { startImmediately: true })
      yield* Effect.yieldNow
      assert.strictEqual(again.pollUnsafe(), undefined)
      yield* SubscriptionRef.set(credentials, { generation: 2, bearer: Redacted.make("third") })
      assert.strictEqual((yield* Fiber.join(again)).generation, 2)
    })
  )
})
