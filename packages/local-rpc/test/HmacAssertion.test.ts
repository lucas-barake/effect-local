import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Hmac from "../src/internal/hmac.js"
import * as PrincipalAssertion from "../src/PrincipalAssertion.js"

const bytes = (text: string) => new TextEncoder().encode(text)
const repeated = (byte: number, length: number) => new Uint8Array(length).fill(byte)

const rfc4231: ReadonlyArray<readonly [string, Uint8Array, Uint8Array, string]> = [
  [
    "test case 1",
    repeated(0x0b, 20),
    bytes("Hi There"),
    "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
  ],
  [
    "test case 2",
    bytes("Jefe"),
    bytes("what do ya want for nothing?"),
    "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
  ],
  [
    "test case 6",
    repeated(0xaa, 131),
    bytes("Test Using Larger Than Block-Size Key - Hash Key First"),
    "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
  ]
]

const ClaimsJson = Schema.fromJsonString(Schema.Struct({ principal: Schema.Json, expiresAtMillis: Schema.Number }))

const secret = Redacted.make("a shared secret of at least thirty two bytes")
const otherSecret = Redacted.make("another shared secret of at least thirty two bytes")

const assertions = (options: PrincipalAssertion.HmacOptions) =>
  Layer.build(PrincipalAssertion.layerHmac(options)).pipe(
    Effect.map((context) => ({
      issuer: Context.get(context, PrincipalAssertion.Issuer),
      verifier: Context.get(context, PrincipalAssertion.Verifier)
    }))
  )

const denialOf = <A,>(effect: Effect.Effect<A, ReplicaError.ReplicaError>) =>
  effect.pipe(
    Effect.as("verified"),
    Effect.catchTag("AuthorizationDenied", (error) => Effect.succeed(error.reason))
  )

const provideCrypto = Effect.provide(NodeCrypto.layer)

describe("HMAC principal assertions", () => {
  it.effect(
    "computes HMAC-SHA256 as specified by RFC 4231",
    Effect.fnUntraced(function*() {
      const crypto = yield* Crypto.Crypto
      for (const [name, key, message, expected] of rfc4231) {
        const mac = yield* Hmac.sha256(crypto, key, message)
        assert.strictEqual(Encoding.encodeHex(mac), expected, name)
      }
    }, provideCrypto)
  )

  it.effect(
    "verifies an assertion it issued back to the same principal",
    Effect.fnUntraced(
      function*() {
        const { issuer, verifier } = yield* assertions({ secret })
        const principal = { userId: "alice", roles: ["member"] }
        const assertion = yield* issuer.issue(principal)
        assert.deepStrictEqual(yield* verifier.verify(assertion), principal)
      },
      Effect.scoped,
      provideCrypto
    )
  )

  it.effect(
    "rejects a tampered principal, a tampered signature, and a foreign secret",
    Effect.fnUntraced(
      function*() {
        const { issuer, verifier } = yield* assertions({ secret })
        const foreign = yield* assertions({ secret: otherSecret })
        const assertion = yield* issuer.issue({ userId: "alice" })
        const [payload, signature] = assertion.split(".")
        const forgedClaims = yield* Schema.encodeEffect(ClaimsJson)({
          principal: { userId: "mallory" },
          expiresAtMillis: Number.MAX_SAFE_INTEGER
        })
        const forgedPayload = Encoding.encodeBase64Url(forgedClaims)
        const tamperedPayload = PrincipalAssertion.PrincipalAssertion.make(`${forgedPayload}.${signature}`)
        const tamperedSignature = PrincipalAssertion.PrincipalAssertion.make(`${payload}.${signature.slice(1)}A`)
        assert.strictEqual(yield* denialOf(verifier.verify(tamperedPayload)), "invalid principal assertion")
        assert.strictEqual(yield* denialOf(verifier.verify(tamperedSignature)), "invalid principal assertion")
        assert.strictEqual(yield* denialOf(foreign.verifier.verify(assertion)), "invalid principal assertion")
      },
      Effect.scoped,
      provideCrypto
    )
  )

  it.effect(
    "rejects an assertion after its time to live",
    Effect.fnUntraced(
      function*() {
        const { issuer, verifier } = yield* assertions({ secret, timeToLive: "30 seconds" })
        const assertion = yield* issuer.issue({ userId: "alice" })
        yield* TestClock.adjust("29 seconds")
        assert.strictEqual(yield* denialOf(verifier.verify(assertion)), "verified")
        yield* TestClock.adjust("2 seconds")
        assert.strictEqual(yield* denialOf(verifier.verify(assertion)), "expired principal assertion")
      },
      Effect.scoped,
      provideCrypto
    )
  )

  it.effect(
    "refuses a secret shorter than 32 bytes",
    Effect.fnUntraced(
      function*() {
        const outcome = yield* assertions({ secret: Redacted.make("too short") }).pipe(
          Effect.as("built"),
          Effect.catchTag("InvalidConfiguration", (error) => Effect.succeed(error.option))
        )
        assert.strictEqual(outcome, "secret")
      },
      Effect.scoped,
      provideCrypto
    )
  )
})
