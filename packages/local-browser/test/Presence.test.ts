import { NodeCrypto } from "@effect/platform-node"
import { assert, it as layeredIt } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as Presence from "../src/Presence.js"

layeredIt.layer(NodeCrypto.layer)("Presence", (it) => {
  const Payload = Schema.Struct({ cursor: Schema.Number, status: Schema.Literals(["active", "idle"]) })

  interface Gate {
    readonly started: Deferred.Deferred<void>
    readonly release: Deferred.Deferred<void>
  }

  const makeGate = Effect.gen(function*() {
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    return { started, release } satisfies Gate
  })

  // Forks a write and returns once its decode has parked on `gate`, so everything after it is guaranteed to
  // arrive later. Racing the gate against the fiber turns an early exit into a real failure instead of a hang.
  const forkParked = <E,>(gate: Gate, write: Effect.Effect<void, E>) =>
    Effect.gen(function*() {
      const fiber = yield* Effect.forkChild(write)
      yield* Effect.raceFirst(Deferred.await(gate.started), Fiber.join(fiber))
      return fiber
    })

  // A consumer supplied schema whose decode parks on `gate` for one specific payload. The schema is a
  // public argument of `Presence.make`, so this drives the real production composition; it only makes
  // the decode step suspend, which `SchemaGetter.transformOrFail` explicitly supports.
  const gatedOn = (gates: ReadonlyArray<readonly [number, Gate]>) =>
    Payload.pipe(Schema.decode({
      decode: SchemaGetter.transformOrFail((value: typeof Payload.Type) => {
        const found = gates.find(([cursor]) => cursor === value.cursor)
        if (found === undefined) return Effect.succeed(value)
        return Deferred.succeed(found[1].started, undefined).pipe(
          Effect.andThen(Deferred.await(found[1].release)),
          Effect.as(value)
        )
      }),
      encode: SchemaGetter.passthrough()
    }))

  it.effect("rejects a nonfinite normalized time to live", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(Presence.make(Payload, { timeToLive: Number.POSITIVE_INFINITY }))
      assert.strictEqual(error._tag, "ReplicaError")
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    }))

  it.effect("expires schema-valid transport peer state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const expiresAtMillis = (yield* Clock.currentTimeMillis) + 1_000
      yield* presence.receive(peerId, { cursor: 3, status: "active" })
      assert.deepStrictEqual(yield* presence.values, [{
        peerId,
        value: { cursor: 3, status: "active" },
        expiresAtMillis,
        identity: "transport-peer"
      }])
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("removes scoped publications without removing newer state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      yield* Effect.scoped(Effect.gen(function*() {
        yield* presence.publish(peerId, { cursor: 1, status: "active" })
        yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      }))
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 2, status: "idle" })
      assert.strictEqual(entries[0]?.identity, "transport-peer")
      assert.isFalse("userId" in entries[0])
    }))

  it.effect("removes received state explicitly and treats repeated removal as a no-op", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      yield* presence.receive(peerId, { cursor: 1, status: "active" })
      assert.strictEqual((yield* presence.values).length, 1)
      yield* presence.remove(peerId)
      assert.deepStrictEqual(yield* presence.values, [])
      yield* presence.remove(peerId)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("rejects invalid payloads without replacing active state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      yield* presence.receive(peerId, { cursor: 1, status: "active" })
      const error = yield* Effect.flip(presence.receive(peerId, { cursor: "bad", status: "active" }))
      assert.strictEqual(error._tag, "ReplicaError")
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.deepStrictEqual((yield* presence.values)[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("keeps the newer publication when an earlier one decodes slowly", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 2, status: "idle" })
      assert.strictEqual(entries[0]?.peerId, peerId)
    }))

  it.effect("keeps a losing scoped publication from removing the newer entry", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, Effect.scoped(presence.publish(peerId, { cursor: 1, status: "active" })))
      yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 2, status: "idle" })
    }))

  it.effect("rejects a stale write without touching another peer", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const first = yield* Identity.makePeerId
      const second = yield* Identity.makePeerId
      yield* presence.receive(second, { cursor: 90, status: "idle" })
      const fiber = yield* forkParked(gate, presence.receive(first, { cursor: 1, status: "active" }))
      yield* presence.receive(first, { cursor: 2, status: "idle" })
      yield* presence.receive(second, { cursor: 91, status: "active" })
      // `it.layer` shares one TestClock across the block, so derive the deadline instead of hardcoding it.
      const expiresAtMillis = (yield* Clock.currentTimeMillis) + 1_000
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 2)
      assert.deepStrictEqual(entries.find((entry) => entry.peerId === first), {
        peerId: first,
        value: { cursor: 2, status: "idle" },
        expiresAtMillis,
        identity: "transport-peer"
      })
      assert.deepStrictEqual(entries.find((entry) => entry.peerId === second), {
        peerId: second,
        value: { cursor: 91, status: "active" },
        expiresAtMillis,
        identity: "transport-peer"
      })
    }))

  it.effect("prunes only expired peers and leaves survivors intact", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerIds: Array<Identity.PeerId> = []
      for (let index = 0; index < 4; index++) {
        const peerId = yield* Identity.makePeerId
        peerIds.push(peerId)
        yield* presence.receive(peerId, { cursor: index, status: "active" })
      }
      yield* TestClock.adjust("600 millis")
      // `it.layer` shares one TestClock across the block, so derive the deadline instead of hardcoding it.
      const expiresAtMillis = (yield* Clock.currentTimeMillis) + 1_000
      yield* presence.receive(peerIds[1], { cursor: 11, status: "idle" })
      yield* presence.receive(peerIds[3], { cursor: 33, status: "idle" })
      yield* TestClock.adjust("500 millis")
      const survivors = [
        {
          peerId: peerIds[1],
          value: { cursor: 11, status: "idle" },
          expiresAtMillis,
          identity: "transport-peer"
        },
        {
          peerId: peerIds[3],
          value: { cursor: 33, status: "idle" },
          expiresAtMillis,
          identity: "transport-peer"
        }
      ]
      assert.deepStrictEqual(yield* presence.values, survivors)
      // Pruning mutates the live map, so a second read must return exactly the same thing.
      assert.deepStrictEqual(yield* presence.values, survivors)
    }))

  it.effect("drops a stale write against a resident expired entry", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      yield* TestClock.adjust("2 seconds")
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("drops a stale write after a read reclaimed the newer entry", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      yield* TestClock.adjust("2 seconds")
      // Reading is the ordinary consumer path and is the only thing that reclaims expired entries.
      assert.deepStrictEqual(yield* presence.values, [])
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("orders a removal against a write that is still in flight", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* presence.remove(peerId)
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("closing a publication scope does not suppress a write that arrived while it was open", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const scope = yield* Scope.make()
      yield* Scope.provide(presence.publish(peerId, { cursor: 0, status: "active" }), scope)
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* Scope.close(scope, Exit.void)
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      // Ending a publication retires only that publication. The receive arrived while the scope was still
      // open and is newer than it, so it stands, exactly as it would have if it had landed before the close.
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("a values read never changes the final presence state", () =>
    Effect.gen(function*() {
      // `values` is an observation. Running it at any point of a fixed sequence must not change where that
      // sequence ends up, which is what makes lazy expiry safe to combine with arrival ordering.
      const run = (readPoints: ReadonlyArray<number>) =>
        Effect.gen(function*() {
          const gate = yield* makeGate
          const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
          const peerId = yield* Identity.makePeerId
          const scope = yield* Scope.make()
          const read = (point: number) => {
            if (readPoints.includes(point)) return Effect.asVoid(presence.values)
            return Effect.void
          }
          yield* Scope.provide(presence.publish(peerId, { cursor: 0, status: "active" }), scope)
          yield* read(0)
          yield* TestClock.adjust("2 seconds")
          yield* read(1)
          const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
          yield* read(2)
          yield* Scope.close(scope, Exit.void)
          yield* read(3)
          yield* Deferred.succeed(gate.release, undefined)
          yield* Fiber.join(fiber)
          // Each run uses a fresh peer and a shared clock that has moved on, so compare the payloads that
          // survived rather than the whole entry.
          return (yield* presence.values).map((entry) => entry.value)
        })
      const baseline = yield* run([])
      for (const point of [0, 1, 2, 3]) {
        assert.deepStrictEqual(yield* run([point]), baseline, `read at point ${point}`)
      }
      assert.deepStrictEqual(yield* run([0, 1, 2, 3]), baseline, "read at every point")
    }))

  it.effect("closing a publication scope removes only that peer", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const resident = yield* Identity.makePeerId
      const publisher = yield* Identity.makePeerId
      yield* presence.receive(resident, { cursor: 1, status: "active" })
      yield* Effect.scoped(Effect.gen(function*() {
        yield* presence.publish(publisher, { cursor: 2, status: "idle" })
        assert.strictEqual((yield* presence.values).length, 2)
      }))
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.strictEqual(entries[0]?.peerId, resident)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("removes only the target peer", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const target = yield* Identity.makePeerId
      const other = yield* Identity.makePeerId
      yield* presence.receive(target, { cursor: 1, status: "active" })
      yield* presence.receive(other, { cursor: 2, status: "idle" })
      yield* presence.remove(target)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.strictEqual(entries[0]?.peerId, other)
    }))

  it.effect("keeps an in-flight write for one peer while another peer writes", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const slow = yield* Identity.makePeerId
      const other = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(slow, { cursor: 1, status: "active" }))
      yield* presence.receive(other, { cursor: 2, status: "idle" })
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 2)
      assert.deepStrictEqual(
        entries.find((entry) => entry.peerId === slow)?.value,
        { cursor: 1, status: "active" }
      )
      assert.deepStrictEqual(
        entries.find((entry) => entry.peerId === other)?.value,
        { cursor: 2, status: "idle" }
      )
    }))

  it.effect("keeps an in-flight write when a later payload fails to decode", () =>
    Effect.gen(function*() {
      const gate = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, gate]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const fiber = yield* forkParked(gate, presence.receive(peerId, { cursor: 1, status: "active" }))
      const error = yield* Effect.flip(presence.receive(peerId, { cursor: "bad", status: "idle" }))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      yield* Deferred.succeed(gate.release, undefined)
      yield* Fiber.join(fiber)
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("orders a removal against every write that arrived before it", () =>
    Effect.gen(function*() {
      const early = yield* makeGate
      const late = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, early], [3, late]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const earlyFiber = yield* forkParked(early, presence.receive(peerId, { cursor: 1, status: "active" }))
      yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      const lateFiber = yield* forkParked(late, presence.receive(peerId, { cursor: 3, status: "active" }))
      yield* presence.remove(peerId)
      yield* Deferred.succeed(late.release, undefined)
      yield* Fiber.join(lateFiber)
      yield* Deferred.succeed(early.release, undefined)
      yield* Fiber.join(earlyFiber)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("keeps ordering state while any write for the peer is still in flight", () =>
    Effect.gen(function*() {
      // The first write settling must not discard the peer's ordering state while a second is still in
      // flight, otherwise the removal below has nothing to record against and the second write resurrects
      // the peer.
      const first = yield* makeGate
      const second = yield* makeGate
      const presence = yield* Presence.make(gatedOn([[1, first], [2, second]]), { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      const firstFiber = yield* forkParked(first, presence.receive(peerId, { cursor: 1, status: "active" }))
      const secondFiber = yield* forkParked(second, presence.receive(peerId, { cursor: 2, status: "idle" }))
      yield* Deferred.succeed(first.release, undefined)
      yield* Fiber.join(firstFiber)
      yield* presence.remove(peerId)
      yield* Deferred.succeed(second.release, undefined)
      yield* Fiber.join(secondFiber)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("does not carry state into a new presence instance", () =>
    Effect.gen(function*() {
      const first = yield* Presence.make(Payload, { timeToLive: "1 second" })
      yield* first.receive(yield* Identity.makePeerId, { cursor: 1, status: "active" })
      const restarted = yield* Presence.make(Payload, { timeToLive: "1 second" })
      assert.deepStrictEqual(yield* restarted.values, [])
    }))
})
