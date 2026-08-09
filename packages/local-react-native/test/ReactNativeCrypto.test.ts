import { assert, describe, it } from "@effect/vitest"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ExpoCrypto from "expo-crypto"
import * as ReactNativeCrypto from "../src/ReactNativeCrypto.js"
import * as ReactNativePolyfills from "../src/ReactNativePolyfills.js"

describe("ReactNativeCrypto", () => {
  it.effect("generates random bytes through the Crypto service", () =>
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      const first = yield* crypto.randomBytes(32)
      const second = yield* crypto.randomBytes(32)
      assert.strictEqual(first.length, 32)
      assert.notStrictEqual(Buffer.from(first).toString("hex"), Buffer.from(second).toString("hex"))
    }).pipe(Effect.provide(ReactNativeCrypto.layer)))

  it.effect("computes SHA-256 digests matching the known vector", () =>
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode("hello"))
      const hex = [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("")
      assert.strictEqual(hex, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
    }).pipe(Effect.provide(ReactNativeCrypto.layer)))

  it.effect("derives UUIDv4 and UUIDv7 from the native CSPRNG", () =>
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      const v4 = yield* crypto.randomUUIDv4
      const v7 = yield* crypto.randomUUIDv7
      assert.match(v4, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
      assert.match(v7, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
      assert.notStrictEqual(yield* crypto.randomUUIDv4, v4)
    }).pipe(Effect.provide(ReactNativeCrypto.layer)))

  it.effect("uses getRandomValues without the development Math.random fallback", () =>
    Effect.gen(function*() {
      let getRandomValuesCalls = 0
      const native = {
        ...ExpoCrypto,
        getRandomBytes: () => {
          throw new Error("getRandomBytes must not be used")
        },
        getRandomValues: <T extends Uint8Array,>(bytes: T): T => {
          getRandomValuesCalls++
          bytes.fill(0x5a)
          return bytes
        }
      } as typeof ExpoCrypto
      const crypto = yield* Crypto.Crypto.pipe(
        Effect.provide(
          ReactNativeCrypto.layer.pipe(
            Layer.provide(Layer.succeed(ReactNativeCrypto.ExpoCryptoModule, native))
          )
        )
      )
      const bytes = yield* crypto.randomBytes(2_048)
      assert.strictEqual(bytes.length, 2_048)
      assert.isTrue(bytes.every((byte) => byte === 0x5a))
      assert.strictEqual(getRandomValuesCalls, 1)
    }))

  it.effect("maps native random failures to PlatformError", () =>
    Effect.gen(function*() {
      const native = {
        ...ExpoCrypto,
        getRandomBytes: () => {
          throw new Error("native entropy unavailable")
        },
        getRandomValues: () => {
          throw new Error("native entropy unavailable")
        }
      } as typeof ExpoCrypto
      const crypto = yield* Crypto.Crypto.pipe(
        Effect.provide(
          ReactNativeCrypto.layer.pipe(
            Layer.provide(Layer.succeed(ReactNativeCrypto.ExpoCryptoModule, native))
          )
        )
      )
      const result = yield* Effect.result(crypto.randomBytes(32))
      assert.strictEqual(result._tag, "Failure")
      if (result._tag === "Failure") assert.strictEqual(result.failure._tag, "PlatformError")
    }))
})

describe("ReactNativePolyfills", () => {
  it("keeps globals the runtime already provides", () => {
    const nativeAtob = globalThis.atob
    const nativeTextEncoder = globalThis.TextEncoder
    ReactNativePolyfills.install()
    assert.strictEqual(globalThis.atob, nativeAtob)
    assert.strictEqual(globalThis.TextEncoder, nativeTextEncoder)
  })

  it("installs missing globals with working implementations", () => {
    const saved = {
      atob: globalThis.atob,
      btoa: globalThis.btoa,
      TextEncoder: globalThis.TextEncoder,
      TextDecoder: globalThis.TextDecoder,
      crypto: (globalThis as Record<string, unknown>).crypto
    }
    try {
      const target = globalThis as Record<string, unknown>
      delete target.atob
      delete target.btoa
      delete target.TextEncoder
      delete target.TextDecoder
      delete target.crypto

      ReactNativePolyfills.install()

      assert.strictEqual(globalThis.atob("aGVsbG8="), "hello")
      assert.strictEqual(globalThis.btoa("hello"), "aGVsbG8=")
      const encoded = new TextEncoder().encode("héllo ✓")
      assert.deepStrictEqual([...encoded], [104, 195, 169, 108, 108, 111, 32, 226, 156, 147])
      assert.strictEqual(new TextDecoder().decode(encoded), "héllo ✓")
      const bytes = new Uint8Array(16)
      globalThis.crypto.getRandomValues(bytes)
      assert.isTrue(bytes.some((byte) => byte !== 0))
      assert.match(globalThis.crypto.randomUUID(), /^[0-9a-f-]{36}$/)
    } finally {
      const target = globalThis as Record<string, unknown>
      target.atob = saved.atob
      target.btoa = saved.btoa
      target.TextEncoder = saved.TextEncoder
      target.TextDecoder = saved.TextDecoder
      target.crypto = saved.crypto
    }
  })

  it("decodes large base64 payloads without stack overflow", () => {
    const saved = globalThis.atob
    try {
      const target = globalThis as Record<string, unknown>
      delete target.atob
      ReactNativePolyfills.install()
      const size = 2 * 1024 * 1024
      const source = new Uint8Array(size)
      const base64 = globalThis.btoa([...source].map((code) => String.fromCharCode(code)).join(""))
      const decoded = globalThis.atob(base64)
      assert.strictEqual(decoded.length, size)
    } finally {
      ;(globalThis as Record<string, unknown>).atob = saved
    }
  })

  it("handles lone surrogates, astral characters, encodeInto, and btoa range errors", () => {
    const saved = {
      TextEncoder: globalThis.TextEncoder,
      btoa: globalThis.btoa
    }
    try {
      const target = globalThis as Record<string, unknown>
      delete target.TextEncoder
      delete target.btoa
      ReactNativePolyfills.install()

      // astral characters roundtrip
      const astral = new TextEncoder().encode("𐍈")
      assert.deepStrictEqual([...astral], [0xF0, 0x90, 0x8D, 0x88])
      // lone surrogates encode as U+FFFD like the native implementation
      assert.deepStrictEqual([...new TextEncoder().encode("\uD800")], [0xEF, 0xBF, 0xBD])
      assert.deepStrictEqual([...new TextEncoder().encode("a\uDC00b")], [97, 0xEF, 0xBF, 0xBD, 98])
      // encodeInto stops on a code point boundary and reports units read
      const destination = new Uint8Array(4)
      const outcome = new TextEncoder().encodeInto("✓✓✓", destination)
      assert.strictEqual(outcome.written, 3)
      assert.strictEqual(outcome.read, 1)
      // btoa rejects out-of-range code units like the native implementation
      assert.throws(() => globalThis.btoa("✓"))
    } finally {
      const target = globalThis as Record<string, unknown>
      target.TextEncoder = saved.TextEncoder
      target.btoa = saved.btoa
    }
  })
})
