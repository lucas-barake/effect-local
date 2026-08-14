import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as EphemeralHub from "../src/EphemeralHub.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const memberA = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})
const memberB = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000002")
})
const memberC = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000003"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000003")
})

const options = {
  capacity: 32,
  maximumSpaces: 4,
  maximumWatchersPerSpace: 8,
  maximumMembersPerSpace: 8,
  maximumEventKeysPerMember: 8,
  maximumEventKeysPerSpace: 32,
  maximumStateKeysPerMember: 8,
  maximumStateKeysPerSpace: 32,
  maximumBytesPerMember: 64 * 1024,
  maximumBytesPerSpace: 256 * 1024,
  memberTtl: 10_000,
  maximumEventTtl: 10_000,
  maximumStateTtl: 60_000,
  spaceIdleTtl: 60_000
} as const

const joinRequest = (
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  value: typeof Protocol.EphemeralJoinRequest.Type["value"] = null
): Protocol.EphemeralJoinRequest => ({ spaceId, member, value, ttlMillis: 10_000 })

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

const startJoin = Effect.fnUntraced(function*(
  hub: EphemeralHub.Service,
  request: Protocol.EphemeralJoinRequest,
  onMessage: (message: Protocol.EphemeralMessage) => Effect.Effect<void> = () => Effect.void
) {
  const snapshot = yield* Deferred.make<Protocol.EphemeralSnapshot>()
  const fiber = yield* hub.join(request, null).pipe(
    Stream.tap((message) => {
      let completeSnapshot = Effect.void
      if (message._tag === "Snapshot") completeSnapshot = Deferred.succeed(snapshot, message)
      return Effect.andThen(completeSnapshot, onMessage(message))
    }),
    Stream.runDrain,
    Effect.forkChild({ startImmediately: true })
  )
  return { fiber, snapshot: yield* Deferred.await(snapshot) }
})

describe("EphemeralHub", () => {
  it.effect(
    "replays the roster and emits departure when a joined stream closes",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* Deferred.make<Protocol.EphemeralMemberUpserted>()
        const left = yield* Deferred.make<Protocol.EphemeralMemberLeft>()
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA), (message) => {
          if (message._tag === "MemberUpserted" && message.entry.member.clientId === memberB.clientId) {
            return Deferred.succeed(joined, message)
          }
          if (message._tag === "MemberLeft" && message.member.clientId === memberB.clientId) {
            return Deferred.succeed(left, message)
          }
          return Effect.void
        })
        assert.deepStrictEqual(first.snapshot.members.map((entry) => entry.member), [memberA])

        const second = yield* startJoin(hub, joinRequest(spaceA, memberB))
        assert.deepStrictEqual(second.snapshot.members.map((entry) => entry.member), [memberA, memberB])
        yield* Deferred.await(joined)

        yield* Fiber.interrupt(second.fiber)
        assert.deepStrictEqual((yield* Deferred.await(left)).member, memberB)
        yield* Fiber.interrupt(first.fiber)
      }).pipe(Effect.provide(EphemeralHub.layerTrusted(options)))
    })
  )

  it.effect(
    "delivers live events, clears them at server expiry, and never replays them",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const typing = yield* Deferred.make<Protocol.EphemeralEvent>()
        const cleared = yield* Deferred.make<Protocol.EphemeralEventCleared>()
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA), (message) => {
          if (message._tag === "Event") return Deferred.succeed(typing, message)
          if (message._tag === "EventCleared") return Deferred.succeed(cleared, message)
          return Effect.void
        })

        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "typing",
            value: { active: true },
            ttlMillis: 1_000
          }),
          null
        )
        assert.deepStrictEqual((yield* Deferred.await(typing)).entry.value, { active: true })

        const late = yield* startJoin(hub, joinRequest(spaceA, memberB))
        assert.deepStrictEqual(late.snapshot.states, [])
        yield* TestClock.adjust("1 second")
        assert.strictEqual((yield* Deferred.await(cleared)).channel, "typing")

        yield* Fiber.interruptAll([first.fiber, late.fiber])
      }).pipe(Effect.provide(EphemeralHub.layerTrusted(options)))
    })
  )

  it.effect(
    "replays retained state to a late member and isolates spaces",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA))
        yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-1",
            value: { message: 42 },
            ttlMillis: 30_000
          }),
          null
        )

        const late = yield* startJoin(hub, joinRequest(spaceA, memberB))
        assert.deepStrictEqual(
          late.snapshot.states.map((entry) => ({
            member: entry.member,
            channel: entry.channel,
            key: entry.key,
            value: entry.value
          })),
          [{ member: memberA, channel: "read", key: "conversation-1", value: { message: 42 } }]
        )

        const isolated = yield* startJoin(hub, joinRequest(spaceB, memberC))
        assert.deepStrictEqual(isolated.snapshot.members.map((entry) => entry.member), [memberC])
        assert.deepStrictEqual(isolated.snapshot.states, [])
        yield* Fiber.interruptAll([first.fiber, late.fiber, isolated.fiber])
      }).pipe(Effect.provide(EphemeralHub.layerTrusted(options)))
    })
  )

  it.effect(
    "rejects oversized and over-count retained state with typed errors",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* startJoin(hub, joinRequest(spaceA, memberA))
        const oversized = yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "oversized",
            value: "x".repeat(Protocol.maximumEphemeralPayloadBytes),
            ttlMillis: 30_000
          }),
          null
        ).pipe(failureOf)
        assert.strictEqual(oversized._tag, "CapacityExceeded")
        if (oversized._tag === "CapacityExceeded") {
          assert.strictEqual(oversized.resource, "ephemeral payload bytes")
        }

        yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "one",
            value: 1,
            ttlMillis: 30_000
          }),
          null
        )
        const excess = yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "two",
            value: 2,
            ttlMillis: 30_000
          }),
          null
        ).pipe(failureOf)
        assert.strictEqual(excess._tag, "CapacityExceeded")
        if (excess._tag === "CapacityExceeded") {
          assert.strictEqual(excess.resource, "ephemeral state keys per member")
          assert.strictEqual(excess.limit, 1)
        }
        yield* Fiber.interrupt(joined.fiber)
      }).pipe(Effect.provide(EphemeralHub.layerTrusted({
        ...options,
        maximumStateKeysPerMember: 1,
        maximumStateKeysPerSpace: 1
      })))
    })
  )

  it.effect(
    "bounds live event keys per member",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* startJoin(hub, joinRequest(spaceA, memberA))
        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "typing",
            value: true,
            ttlMillis: 5_000
          }),
          null
        )
        const excess = yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "recording",
            value: true,
            ttlMillis: 5_000
          }),
          null
        ).pipe(failureOf)
        assert.strictEqual(excess._tag, "CapacityExceeded")
        if (excess._tag === "CapacityExceeded") {
          assert.strictEqual(excess.resource, "ephemeral event keys per member")
          assert.strictEqual(excess.limit, 1)
        }
        yield* Fiber.interrupt(joined.fiber)
      }).pipe(Effect.provide(EphemeralHub.layerTrusted({
        ...options,
        maximumEventKeysPerMember: 1,
        maximumEventKeysPerSpace: 1
      })))
    })
  )

  it.effect(
    "expires roster members and retained state using the server clock",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const memberLeft = yield* Deferred.make<void>()
        const stateRemoved = yield* Deferred.make<void>()
        const observer = yield* startJoin(hub, joinRequest(spaceA, memberA), (message) => {
          if (message._tag === "MemberLeft" && message.member.clientId === memberB.clientId) {
            return Deferred.succeed(memberLeft, undefined)
          }
          if (message._tag === "StateRemoved" && message.member.clientId === memberA.clientId) {
            return Deferred.succeed(stateRemoved, undefined)
          }
          return Effect.void
        })
        const expiring = yield* startJoin(hub, {
          ...joinRequest(spaceA, memberB),
          ttlMillis: 1_000
        })
        yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-1",
            value: 42,
            ttlMillis: 1_000
          }),
          null
        )

        yield* TestClock.adjust("1 second")
        yield* Deferred.await(memberLeft)
        yield* Deferred.await(stateRemoved)
        yield* Fiber.interruptAll([observer.fiber, expiring.fiber])
      }).pipe(Effect.provide(EphemeralHub.layerTrusted(options)))
    })
  )

  it.effect(
    "authorizes before revealing capacity and requires an active member session",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA))
        const denied = yield* hub.join(joinRequest(spaceA, memberB), "denied").pipe(
          Stream.runDrain,
          failureOf
        )
        assert.strictEqual(denied._tag, "AuthorizationDenied")

        const unavailable = yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceB,
            member: memberB,
            channel: "typing",
            value: true,
            ttlMillis: 1_000
          }),
          "allowed"
        ).pipe(failureOf)
        assert.strictEqual(unavailable._tag, "EphemeralSessionUnavailable")
        yield* Fiber.interrupt(first.fiber)
      }).pipe(Effect.provide(EphemeralHub.layer({
        ...options,
        maximumWatchersPerSpace: 1,
        authorize: (input) => {
          if (input.principal === "denied") {
            return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "denied" }))
          }
          return Effect.void
        }
      })))
    })
  )
})
