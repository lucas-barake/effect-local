import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as PeerConnectionStatus from "../src/PeerConnectionStatus.js"

const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")

const current = (service: PeerConnectionStatus.PeerConnectionStatus["Service"]) =>
  Stream.runHead(service.status(peerId)).pipe(Effect.map(Option.getOrThrow))

describe("PeerConnectionStatus", () => {
  it.effect("reports one scoped connection lifecycle", () =>
    Effect.gen(function*() {
      const service = yield* PeerConnectionStatus.PeerConnectionStatus
      const reporter = yield* PeerConnectionStatus.Reporter

      assert.deepStrictEqual(yield* current(service), { _tag: "Disconnected" })
      yield* Effect.scoped(Effect.gen(function*() {
        const attempt = yield* reporter.connecting(peerId)
        assert.deepStrictEqual(yield* current(service), { _tag: "Connecting" })
        yield* attempt.connected
        assert.deepStrictEqual(yield* current(service), { _tag: "Connected" })
      }))
      assert.deepStrictEqual(yield* current(service), { _tag: "Disconnected" })
    }).pipe(Effect.provide(PeerConnectionStatus.layer())))

  it.effect("keeps a peer connected while another attempt is opening", () =>
    Effect.gen(function*() {
      const service = yield* PeerConnectionStatus.PeerConnectionStatus
      const reporter = yield* PeerConnectionStatus.Reporter
      yield* Effect.scoped(Effect.gen(function*() {
        const connected = yield* reporter.connecting(peerId)
        yield* connected.connected
        yield* Effect.scoped(Effect.gen(function*() {
          yield* reporter.connecting(peerId)
          assert.deepStrictEqual(yield* current(service), { _tag: "Connected" })
        }))
        assert.deepStrictEqual(yield* current(service), { _tag: "Connected" })
      }))
      assert.deepStrictEqual(yield* current(service), { _tag: "Disconnected" })
    }).pipe(Effect.provide(PeerConnectionStatus.layer())))
})
