import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SyncRpc from "../src/SyncRpc.js"

const serialization = (maximumFrameBytes: number) =>
  SyncRpc.layerJson({ maximumFrameBytes }).pipe(
    Layer.build,
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
  it.effect("bounds complete WebSocket JSON frames before parse and after encode", () =>
    Effect.gen(function*() {
      const service = yield* serialization(64)
      const parser = service.makeUnsafe()

      assert.isFalse(service.includesFraming)
      pipe(parser.decode("[{\"_tag\":\"Ping\"}]"), (decoded) => assert.deepStrictEqual(decoded, [{ _tag: "Ping" }]))
      assert.throws(() => parser.decode(`"${"x".repeat(64)}"`), RangeError)
      assert.throws(() => parser.encode({ value: "x".repeat(64) }), RangeError)
    }))

  it.effect("removes defect and typed error causes from serialized responses", () =>
    Effect.gen(function*() {
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

      pipe(String(protocolDefect), (serialized) => assert.notInclude(serialized, "protocol secret"))
      pipe(String(protocolDefect), (serialized) => assert.include(serialized, "RemoteDefect"))
      pipe(String(typedFailure), (serialized) => assert.notInclude(serialized, "database secret"))
      pipe(String(typedFailure), (serialized) => assert.include(serialized, "Remote internal error"))
    }))

  it.effect("rejects an invalid frame bound while building the Layer", () =>
    Effect.gen(function*() {
      const error = yield* serialization(0).pipe(failureOf)
      assert.strictEqual(error._tag, "InvalidConfiguration")
      if (error._tag === "InvalidConfiguration") assert.strictEqual(error.option, "maximumFrameBytes")
    }))
})
