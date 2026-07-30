import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
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

      assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.disconnected)
      yield* Effect.scoped(Effect.gen(function*() {
        const attempt = yield* reporter.connecting(peerId)
        assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.connecting)
        yield* attempt.connected
        assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.connected)
      }))
      assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.disconnected)
    }).pipe(Effect.provide(PeerConnectionStatus.layer)))

  it.effect("keeps a peer connected while another attempt is opening", () =>
    Effect.gen(function*() {
      const service = yield* PeerConnectionStatus.PeerConnectionStatus
      const reporter = yield* PeerConnectionStatus.Reporter
      yield* Effect.scoped(Effect.gen(function*() {
        const connected = yield* reporter.connecting(peerId)
        yield* connected.connected
        yield* Effect.scoped(Effect.gen(function*() {
          yield* reporter.connecting(peerId)
          assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.connected)
        }))
        assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.connected)
      }))
      assert.deepStrictEqual(yield* current(service), PeerConnectionStatus.disconnected)
    }).pipe(Effect.provide(PeerConnectionStatus.layer)))

  // A live subscriber has to be told the peer is gone and then be released. If the stream were
  // interrupted instead, a browser Atom observing it over the owner RPC would see a defect rather
  // than a final status, and if it never ended the fiber would leak past its own Layer.
  it.effect("delivers a terminal status and ends cleanly when the Layer scope closes", () =>
    Effect.gen(function*() {
      // Two scopes, and the split is load-bearing. If the attempt's own finalizer ran first it would
      // publish `Disconnected` itself, the terminal value would dedupe away, and the concat that
      // produces it would go untested.
      const layerScope = yield* Scope.make()
      const attemptScope = yield* Scope.make()
      const context = yield* Scope.provide(Layer.build(PeerConnectionStatus.layer), layerScope)
      const service = Context.get(context, PeerConnectionStatus.PeerConnectionStatus)
      const reporter = Context.get(context, PeerConnectionStatus.Reporter)
      const seen = yield* Queue.unbounded<PeerConnectionStatus.Status>()
      yield* Effect.addFinalizer(() => Queue.shutdown(seen))
      const fiber = yield* Stream.runForEach(service.status(peerId), (status) => Queue.offer(seen, status))
        .pipe(Effect.forkChild)

      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.disconnected)
      const attempt = yield* Scope.provide(reporter.connecting(peerId), attemptScope)
      yield* attempt.connected
      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.connecting)
      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.connected)

      yield* Scope.close(layerScope, Exit.void)

      yield* Fiber.join(fiber)
      // The peer was Connected, so a terminal Disconnected is a real transition rather than a
      // repeat of the seed that `changesWith` would swallow.
      assert.deepStrictEqual(yield* Queue.poll(seen), Option.some(PeerConnectionStatus.disconnected))
      yield* Scope.close(attemptScope, Exit.void)
    }).pipe(Effect.scoped))

  // The Reporter and the reader are two projections of one owner, so they have to come from one
  // build. `Layer.merge` cannot expose this: it ends in `Context.mergeAll`, which is last write wins
  // per key, so both projections would come from the second build and agree by accident. Only an
  // explicit memo map puts one projection from each build in the same test.
  it.effect("resolves one owner when the Layer is referenced twice under one memo map", () =>
    Effect.gen(function*() {
      const memoMap = yield* Layer.makeMemoMap
      const scope = yield* Scope.make()
      const first = yield* Layer.buildWithMemoMap(PeerConnectionStatus.layer, memoMap, scope)
      const second = yield* Layer.buildWithMemoMap(PeerConnectionStatus.layer, memoMap, scope)
      const reporter = Context.get(first, PeerConnectionStatus.Reporter)
      const reader = Context.get(second, PeerConnectionStatus.PeerConnectionStatus)

      yield* Effect.scoped(Effect.gen(function*() {
        yield* reporter.connecting(peerId)
        assert.deepStrictEqual(yield* current(reader), PeerConnectionStatus.connecting)
      }))

      yield* Scope.close(scope, Exit.void)
    }))
})
