import { assert, describe, it } from "@effect/vitest"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Hex from "effect/encoding/Hex"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as ExpoCrypto from "../src/ExpoCrypto.js"
import { cryptoProbe } from "./fixtures/nativeCrypto.js"

const provideExpoCrypto = Effect.provide(ExpoCrypto.layer)

describe("ExpoCrypto", () => {
  it.effect(
    "draws random bytes of any size from the native source",
    Effect.fnUntraced(function*() {
      cryptoProbe.reset()
      const crypto = yield* Crypto.Crypto
      const bytes = yield* crypto.randomBytes(100_000)
      assert.strictEqual(bytes.length, 100_000)
      assert.deepStrictEqual(bytes, cryptoProbe.filled)
    }, provideExpoCrypto)
  )

  it.effect(
    "computes digests through the native module",
    Effect.fnUntraced(function*() {
      cryptoProbe.reset()
      const crypto = yield* Crypto.Crypto
      const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode("abc"))
      assert.strictEqual(Hex.encode(digest), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
    }, provideExpoCrypto)
  )

  it.effect(
    "fails a rejected native digest with a PlatformError",
    Effect.fnUntraced(function*() {
      cryptoProbe.reset()
      cryptoProbe.failNextDigest = true
      const crypto = yield* Crypto.Crypto
      const exit = yield* crypto.digest("SHA-512", new Uint8Array([1])).pipe(Effect.exit)
      assert.strictEqual(Option.getOrUndefined(Exit.findErrorOption(exit))?._tag, "PlatformError")
    }, provideExpoCrypto)
  )
})
