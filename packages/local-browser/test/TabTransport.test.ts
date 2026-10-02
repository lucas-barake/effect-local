import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Rpc from "effect/rpc/Rpc"
import * as Schema from "effect/Schema"
import * as TabTransport from "../src/internal/tabTransport.js"

const encodeExit = Rpc.make("probe").pipe(Rpc.exitSchema, Schema.toCodecJson, Schema.encodeEffect)
describe("tab transport frames", () => {
  it.effect(
    "carry an interrupt exit as Effect's JSON RPC codec encodes it",
    Effect.fnUntraced(function*() {
      const exit = yield* encodeExit(Exit.interrupt())
      const frame = {
        _tag: "ToClient",
        from: "host",
        connection: 0,
        message: { _tag: "Exit", requestId: "1", exit }
      }
      const decoded = yield* Schema.decodeUnknownEffect(TabTransport.Frame)(frame)
      assert.deepStrictEqual<unknown>(decoded, frame)
    })
  )
})
