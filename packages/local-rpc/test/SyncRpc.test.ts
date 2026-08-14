import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SyncRpc from "../src/SyncRpc.js"

const serialization = (maximumFrameBytes: number) =>
  Layer.build(SyncRpc.layerJson({ maximumFrameBytes })).pipe(
    Effect.map(Context.get(RpcSerialization.RpcSerialization))
  )

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

describe("SyncRpc serialization", () => {
  it("exposes bounded attachment control operations without byte routes", () => {
    assert.property(SyncRpc, "PrepareAttachmentUpload")
    assert.property(SyncRpc, "FinalizeAttachmentUpload")
    assert.property(SyncRpc, "PrepareAttachmentDownload")
  })

  it.effect(
    "bounds complete WebSocket JSON frames before parse and after encode",
    Effect.fnUntraced(function*() {
      const service = yield* serialization(64)
      const parser = service.makeUnsafe()

      assert.isFalse(service.includesFraming)
      assert.deepStrictEqual(parser.decode("[{\"_tag\":\"Ping\"}]"), [{ _tag: "Ping" }])
      assert.throws(() => parser.decode(`"${"x".repeat(64)}"`), RangeError)
      assert.throws(() => parser.encode({ value: "x".repeat(64) }), RangeError)
    })
  )

  it.effect(
    "removes defect and typed error causes from serialized responses",
    Effect.fnUntraced(function*() {
      const service = yield* serialization(4_096)
      const parser = service.makeUnsafe()
      const protocolDefect = parser.encode({
        _tag: "Defect",
        defect: "protocol secret"
      })
      const typedFailure = parser.encode({
        _tag: "Exit",
        requestId: 1,
        exit: {
          _tag: "Failure",
          cause: [{
            _tag: "Fail",
            error: {
              _tag: "StorageUnavailable",
              cause: { message: "database secret" }
            }
          }]
        }
      })

      assert.notInclude(String(protocolDefect), "protocol secret")
      assert.include(String(protocolDefect), "RemoteDefect")
      assert.notInclude(String(typedFailure), "database secret")
      assert.include(String(typedFailure), "Remote internal error")
    })
  )

  it.effect(
    "rejects an invalid frame bound while building the Layer",
    Effect.fnUntraced(function*() {
      const error = yield* serialization(0).pipe(failureOf)
      assert.strictEqual(error._tag, "InvalidConfiguration")
      if (error._tag === "InvalidConfiguration") assert.strictEqual(error.option, "maximumFrameBytes")
    })
  )
})
