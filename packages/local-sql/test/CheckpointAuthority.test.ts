import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as CheckpointAuthority from "../src/CheckpointAuthority.js"
import * as WriterProvenance from "../src/internal/writerProvenance.js"
import { encodeJson } from "./helpers/json.js"

const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const priorLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000002")
const resultingLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000003")
const priorCheckpointHash = "1".repeat(64)
const anchorCheckpointHash = "2".repeat(64)
const writerDefinitionHash = "def_0123456789abcdef"
const oversizedToken: CheckpointAuthority.AuthorizationToken = Reflect.construct(Uint8Array, [
  CheckpointAuthority.maximumAuthorizationTokenBytes + 1
])

const manifestClaims: CheckpointAuthority.ManifestClaims = {
  purpose: CheckpointAuthority.manifestPurpose,
  documentId,
  lineage: priorLineage,
  checkpointHash: priorCheckpointHash,
  heads: ["head-b", "head-a", "head-a"],
  base: { _tag: "Heads", baseHeads: ["base-b", "base-a", "base-a"] },
  schemaVersion: 1,
  writerDefinitionHash
}

const transitionClaims: CheckpointAuthority.TransitionClaims = {
  purpose: CheckpointAuthority.transitionPurpose,
  documentId,
  priorLineage,
  priorCheckpointHash,
  priorHeads: ["prior-b", "prior-a", "prior-a"],
  resultingLineage,
  anchorCheckpointHash,
  resultingHeads: ["result-b", "result-a", "result-a"],
  schemaVersion: 2,
  writerDefinitionHash
}

describe("CheckpointAuthority", () => {
  it.effect("canonicalizes every bound heads collection before invoking the authority", () =>
    Effect.gen(function*() {
      const authority = yield* CheckpointAuthority.CheckpointAuthority
      yield* authority.verifyManifest(manifestClaims, Uint8Array.of(1))
      yield* authority.verifyTransition(transitionClaims, Uint8Array.of(2))
    }).pipe(
      Effect.provide(CheckpointAuthority.layer({
        signManifest: () => Effect.succeed(Option.none()),
        verifyManifest: (claims) =>
          Effect.sync(() => {
            assert.deepStrictEqual(claims.heads, ["head-a", "head-b"])
            assert.deepStrictEqual(claims.base, {
              _tag: "Heads",
              baseHeads: ["base-a", "base-b"]
            })
          }),
        signTransition: () => Effect.succeed(Option.none()),
        verifyTransition: (claims) =>
          Effect.sync(() => {
            assert.deepStrictEqual(claims.priorHeads, ["prior-a", "prior-b"])
            assert.deepStrictEqual(claims.resultingHeads, ["result-a", "result-b"])
          })
      }))
    ))

  it.effect("makes signing optional and rejects verification by default", () =>
    Effect.gen(function*() {
      const authority = yield* CheckpointAuthority.CheckpointAuthority
      assert.isTrue(Option.isNone(yield* authority.signManifest(manifestClaims)))
      assert.isTrue(Option.isNone(yield* authority.signTransition(transitionClaims)))

      for (
        const error of [
          yield* Effect.flip(authority.verifyManifest(manifestClaims, Uint8Array.of(1))),
          yield* Effect.flip(authority.verifyTransition(transitionClaims, Uint8Array.of(1)))
        ]
      ) {
        assert.strictEqual(error._tag, "ReplicaError")
        assert.strictEqual(error.reason._tag, "CheckpointRejected")
      }
    }).pipe(Effect.provide(CheckpointAuthority.layerRejectAll)))

  it.effect("rejects absent and oversized tokens before a permissive verifier runs", () => {
    let verificationCalls = 0
    const Permissive = CheckpointAuthority.layer({
      signManifest: () => Effect.succeed(Option.some(oversizedToken)),
      verifyManifest: () => Effect.sync(() => verificationCalls++).pipe(Effect.asVoid),
      signTransition: () => Effect.succeed(Option.none()),
      verifyTransition: () => Effect.sync(() => verificationCalls++).pipe(Effect.asVoid)
    })

    return Effect.gen(function*() {
      const authority = yield* CheckpointAuthority.CheckpointAuthority

      for (
        const error of [
          yield* Effect.flip(
            authority.verifyManifest(
              manifestClaims,
              Reflect.apply(authority.verifyManifest, authority, [manifestClaims, undefined])
            )
          ),
          yield* Effect.flip(authority.verifyTransition(transitionClaims, oversizedToken)),
          yield* Effect.flip(authority.signManifest(manifestClaims))
        ]
      ) {
        assert.strictEqual(error.reason._tag, "CheckpointRejected")
      }
      assert.strictEqual(verificationCalls, 0)
    }).pipe(Effect.provide(Permissive))
  })

  it.effect("rejects oversized authorization in durable compact provenance", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(
        Schema.decodeUnknownEffect(WriterProvenance.StoredCheckpointProvenance)(encodeJson({
          _tag: "Compact",
          checkpointHash: priorCheckpointHash,
          lineage: priorLineage,
          heads: ["a".repeat(64)],
          base: { _tag: "Bootstrap" },
          schemaVersion: 1,
          writerDefinitionHash,
          authorization: Buffer.from(oversizedToken).toString("base64")
        }))
      )
      assert.strictEqual(error._tag, "SchemaError")
    }))

  it("distinguishes an explicit bootstrap base from bound base heads", () => {
    const bootstrap = Schema.decodeUnknownSync(CheckpointAuthority.ManifestClaims)({
      ...manifestClaims,
      base: { _tag: "Bootstrap" }
    })
    assert.deepStrictEqual(bootstrap.base, { _tag: "Bootstrap" })
  })
})
