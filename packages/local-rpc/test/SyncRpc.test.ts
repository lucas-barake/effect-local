import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SyncRpc from "../src/SyncRpc.js"

const serialization = (maximumFrameBytes: number) =>
  Layer.build(SyncRpc.layerJson({ maximumFrameBytes })).pipe(
    Effect.map((context) => Context.get(context, RpcSerialization.RpcSerialization))
  )

describe("SyncRpc serialization", () => {
  it.effect("bounds complete WebSocket JSON frames before parse and after encode", () =>
    Effect.gen(function*() {
      const service = yield* serialization(64)
      const parser = service.makeUnsafe()

      assert.isFalse(service.includesFraming)
      assert.deepStrictEqual(parser.decode("[{\"_tag\":\"Ping\"}]"), [{ _tag: "Ping" }])
      assert.throws(() => parser.decode(`"${"x".repeat(64)}"`), RangeError)
      assert.throws(() => parser.encode({ value: "x".repeat(64) }), RangeError)
    }))

  it.effect("removes defect and typed error causes from serialized responses", () =>
    Effect.gen(function*() {
      const service = yield* serialization(4_096)
      const parser = service.makeUnsafe()
      const protocolDefect = parser.encode({
        _tag: "Defect",
        defect: new Error("protocol secret")
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
    }))

  it.effect("rejects an invalid frame bound while building the Layer", () =>
    Effect.gen(function*() {
      const error = yield* serialization(0).pipe(Effect.flip)
      assert.strictEqual(error._tag, "InvalidConfiguration")
      if (error._tag === "InvalidConfiguration") assert.strictEqual(error.option, "maximumFrameBytes")
    }))
})
