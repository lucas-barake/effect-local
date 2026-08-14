import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as EphemeralHub from "../src/EphemeralHub.js"
import * as SyncRpc from "../src/SyncRpc.js"

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

const layerTrusted = (input: EphemeralHub.Options) =>
  EphemeralHub.layerTrusted(input).pipe(Layer.provide(NodeCrypto.layer))

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
  const session = yield* Deferred.make<Protocol.EphemeralSessionStarted>()
  const snapshot = yield* Deferred.make<Protocol.EphemeralSnapshot>()
  const fiber = yield* hub.join(request, null).pipe(
    Stream.tap((message) => {
      if (message._tag === "SessionStarted") return Deferred.succeed(session, message)
      let completeSnapshot = Effect.void
      if (message._tag === "Snapshot") completeSnapshot = Deferred.succeed(snapshot, message)
      return Effect.andThen(completeSnapshot, onMessage(message))
    }),
    Stream.runDrain,
    Effect.forkChild({ startImmediately: true })
  )
  return {
    fiber,
    session: yield* Deferred.await(session),
    snapshot: yield* Deferred.await(snapshot)
  }
})

describe("EphemeralHub", () => {
  it.effect(
    "rejects a member lease that cannot renew before expiry",
    Effect.fnUntraced(function*() {
      const failure = yield* Layer.build(EphemeralHub.layerTrusted({
        maximumWatchersPerSpace: 8,
        memberTtl: "1 millis"
      })).pipe(Effect.provide(NodeCrypto.layer), failureOf)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") assert.strictEqual(failure.option, "memberTtl")
    })
  )

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
      }).pipe(Effect.provide(layerTrusted(options)))
    })
  )

  it.effect(
    "ends only the subscriber that falls behind",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const snapshotReady = yield* Deferred.make<void>()
        const releaseSnapshot = yield* Deferred.make<void>()
        const fast = yield* startJoin(hub, joinRequest(spaceA, memberB))
        const slow = yield* hub.join(joinRequest(spaceA, memberA), null).pipe(
          Stream.tap((message) => {
            if (message._tag !== "Snapshot") return Effect.void
            return Deferred.succeed(snapshotReady, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSnapshot))
            )
          }),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(snapshotReady)

        for (let message = 1; message <= 6; message++) {
          yield* hub.publish(
            Protocol.EphemeralSetStateRequest.make({
              spaceId: spaceA,
              member: memberB,
              channel: "read",
              key: "conversation-1",
              value: { message },
              ttlMillis: 30_000
            }),
            fast.session.sessionToken,
            null
          )
        }
        assert.strictEqual(fast.fiber.pollUnsafe(), undefined)
        yield* Deferred.succeed(releaseSnapshot, undefined)
        yield* Fiber.join(slow)

        const late = yield* startJoin(hub, joinRequest(spaceA, memberC))
        assert.deepStrictEqual(late.snapshot.states.map((entry) => entry.value), [{ message: 6 }])
        yield* Fiber.interrupt(fast.fiber)
        yield* Fiber.interrupt(late.fiber)
      }).pipe(Effect.provide(layerTrusted({ ...options, capacity: 4 })))
    })
  )

  it.effect(
    "terminates a replaced session and clears its live event quota",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const cleared = yield* Deferred.make<Protocol.EphemeralEventCleared>()
        const observer = yield* startJoin(hub, joinRequest(spaceA, memberB), (message) => {
          if (message._tag === "EventCleared" && message.member.clientId === memberA.clientId) {
            return Deferred.succeed(cleared, message)
          }
          return Effect.void
        })
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA))
        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "typing",
            value: true,
            ttlMillis: 10_000
          }),
          first.session.sessionToken,
          null
        )

        const replacement = yield* startJoin(hub, joinRequest(spaceA, memberA))
        const replaced = yield* Fiber.join(first.fiber).pipe(Effect.result)
        assert.isTrue(Result.isFailure(replaced))
        if (Result.isFailure(replaced)) assert.strictEqual(replaced.failure._tag, "EphemeralSessionUnavailable")
        yield* Deferred.await(cleared)
        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "recording",
            value: true,
            ttlMillis: 10_000
          }),
          replacement.session.sessionToken,
          null
        )
        yield* Fiber.interruptAll([observer.fiber, replacement.fiber])
      }).pipe(Effect.provide(layerTrusted({
        ...options,
        maximumEventKeysPerMember: 1
      })))
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
          first.session.sessionToken,
          null
        )
        assert.deepStrictEqual((yield* Deferred.await(typing)).entry.value, { active: true })

        const observedAfterSnapshot = yield* Deferred.make<Protocol.EphemeralMessage>()
        const late = yield* startJoin(hub, joinRequest(spaceA, memberB), (message) => {
          if (message._tag === "Event" || message._tag === "StateSet") {
            return Deferred.succeed(observedAfterSnapshot, message)
          }
          return Effect.void
        })
        yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "sentinel",
            value: true,
            ttlMillis: 30_000
          }),
          first.session.sessionToken,
          null
        )
        assert.strictEqual((yield* Deferred.await(observedAfterSnapshot))._tag, "StateSet")
        assert.deepStrictEqual(late.snapshot.states, [])
        yield* TestClock.adjust("1 second")
        assert.strictEqual((yield* Deferred.await(cleared)).channel, "typing")

        yield* Fiber.interruptAll([first.fiber, late.fiber])
      }).pipe(Effect.provide(layerTrusted(options)))
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
          first.session.sessionToken,
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
      }).pipe(Effect.provide(layerTrusted(options)))
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
          joined.session.sessionToken,
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
          joined.session.sessionToken,
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
          joined.session.sessionToken,
          null
        ).pipe(failureOf)
        assert.strictEqual(excess._tag, "CapacityExceeded")
        if (excess._tag === "CapacityExceeded") {
          assert.strictEqual(excess.resource, "ephemeral state keys per member")
          assert.strictEqual(excess.limit, 1)
        }
        yield* Fiber.interrupt(joined.fiber)
      }).pipe(Effect.provide(layerTrusted({
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
          joined.session.sessionToken,
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
          joined.session.sessionToken,
          null
        ).pipe(failureOf)
        assert.strictEqual(excess._tag, "CapacityExceeded")
        if (excess._tag === "CapacityExceeded") {
          assert.strictEqual(excess.resource, "ephemeral event keys per member")
          assert.strictEqual(excess.limit, 1)
        }
        yield* Fiber.interrupt(joined.fiber)
      }).pipe(Effect.provide(layerTrusted({
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
          observer.session.sessionToken,
          null
        )

        yield* TestClock.adjust("1 second")
        yield* Deferred.await(memberLeft)
        yield* Deferred.await(stateRemoved)
        assert.isDefined(expiring.fiber.pollUnsafe())
        yield* Fiber.interruptAll([observer.fiber, expiring.fiber])
      }).pipe(Effect.provide(layerTrusted(options)))
    })
  )

  it.effect(
    "publishes heartbeat lease renewals to every joined observer",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const observedExpiry = yield* Ref.make(0)
        const observer = yield* startJoin(hub, joinRequest(spaceA, memberA), (message) => {
          if (message._tag !== "MemberUpserted" || message.entry.member.clientId !== memberB.clientId) {
            return Effect.void
          }
          return Ref.set(observedExpiry, message.entry.expiresAtMillis)
        })
        const renewed = yield* startJoin(hub, joinRequest(spaceA, memberB))
        yield* TestClock.adjust("5 seconds")
        yield* hub.heartbeat(
          { spaceId: spaceA, member: memberB },
          renewed.session.sessionToken,
          null
        )
        const late = yield* startJoin(hub, joinRequest(spaceA, memberC))
        const lateExpiry = late.snapshot.members.find((entry) => entry.member.clientId === memberB.clientId)
          ?.expiresAtMillis
        assert.strictEqual(yield* Ref.get(observedExpiry), lateExpiry)
        yield* Fiber.interruptAll([observer.fiber, renewed.fiber, late.fiber])
      }).pipe(Effect.provide(layerTrusted(options)))
    })
  )

  it.effect(
    "releases departed event quota before its original TTL",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const first = yield* startJoin(hub, joinRequest(spaceA, memberA))
        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "typing",
            value: true,
            ttlMillis: 10_000
          }),
          first.session.sessionToken,
          null
        )
        yield* Fiber.interrupt(first.fiber)

        const second = yield* startJoin(hub, joinRequest(spaceA, memberB))
        yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberB,
            channel: "recording",
            value: true,
            ttlMillis: 10_000
          }),
          second.session.sessionToken,
          null
        )
        yield* Fiber.interrupt(second.fiber)
      }).pipe(Effect.provide(layerTrusted({
        ...options,
        maximumEventKeysPerSpace: 1
      })))
    })
  )

  it.effect(
    "rejects a public roster identity without its private session capability",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* startJoin(hub, joinRequest(spaceA, memberA))
        yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-1",
            value: 42,
            ttlMillis: 10_000
          }),
          joined.session.sessionToken,
          null
        )
        const invalidSessionToken = Identity.EphemeralSessionToken.make(
          "eps_00000000-0000-4000-8000-000000000099"
        )
        const failure = yield* hub.publish(
          Protocol.EphemeralEventRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "typing",
            value: true,
            ttlMillis: 1_000
          }),
          invalidSessionToken,
          null
        ).pipe(failureOf)
        assert.strictEqual(failure._tag, "EphemeralSessionUnavailable")
        const removeFailure = yield* hub.publish(
          Protocol.EphemeralRemoveStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-1"
          }),
          invalidSessionToken,
          null
        ).pipe(failureOf)
        assert.strictEqual(removeFailure._tag, "EphemeralSessionUnavailable")
        const late = yield* startJoin(hub, joinRequest(spaceA, memberB))
        assert.strictEqual(late.snapshot.states.length, 1)
        yield* Fiber.interruptAll([joined.fiber, late.fiber])
      }).pipe(Effect.provide(layerTrusted(options)))
    })
  )

  it.effect(
    "rejects retained state before the encoded snapshot can exceed its bound",
    Effect.fnUntraced(function*() {
      return yield* Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* startJoin(hub, joinRequest(spaceA, memberA))
        const failure = yield* hub.publish(
          Protocol.EphemeralSetStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-1",
            value: "x".repeat(400),
            ttlMillis: 30_000
          }),
          joined.session.sessionToken,
          null
        ).pipe(failureOf)
        assert.strictEqual(failure._tag, "CapacityExceeded")
        if (failure._tag === "CapacityExceeded") {
          assert.strictEqual(failure.resource, "ephemeral snapshot bytes")
        }
        yield* Fiber.interrupt(joined.fiber)
      }).pipe(Effect.provide(layerTrusted({
        ...options,
        maximumSnapshotBytes: 512
      })))
    })
  )

  it.effect(
    "keeps every admitted late-join snapshot below the default RPC frame",
    Effect.fnUntraced(function*() {
      const program = Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const joined = yield* startJoin(hub, joinRequest(spaceA, memberA))
        let rejected = false
        for (let index = 0; index < 400; index++) {
          const result = yield* hub.publish(
            Protocol.EphemeralSetStateRequest.make({
              spaceId: spaceA,
              member: memberA,
              channel: "read",
              key: `conversation-${index}`,
              value: "x".repeat(14_200),
              ttlMillis: 30_000
            }),
            joined.session.sessionToken,
            null
          ).pipe(Effect.result)
          if (Result.isSuccess(result)) continue
          assert.strictEqual(result.failure._tag, "CapacityExceeded")
          if (result.failure._tag === "CapacityExceeded") {
            assert.strictEqual(result.failure.resource, "ephemeral snapshot bytes")
          }
          rejected = true
          break
        }
        assert.isTrue(rejected)
        yield* hub.publish(
          Protocol.EphemeralRemoveStateRequest.make({
            spaceId: spaceA,
            member: memberA,
            channel: "read",
            key: "conversation-0"
          }),
          joined.session.sessionToken,
          null
        )
        const late = yield* startJoin(hub, joinRequest(spaceA, memberB))
        const serialization = yield* Layer.build(SyncRpc.layerJson()).pipe(
          Effect.map(Context.get(RpcSerialization.RpcSerialization))
        )
        assert.doesNotThrow(() =>
          serialization.makeUnsafe().encode({
            _tag: "Chunk",
            requestId: 1,
            values: [late.snapshot]
          })
        )
        yield* Fiber.interruptAll([joined.fiber, late.fiber])
      })
      yield* program.pipe(Effect.provide(layerTrusted({
        maximumWatchersPerSpace: 8,
        maximumStateKeysPerMember: 400,
        maximumStateKeysPerSpace: 400,
        maximumBytesPerMember: 8 * 1024 * 1024,
        maximumBytesPerSpace: 16 * 1024 * 1024
      })))
    })
  )

  it.effect(
    "closes an established stream when periodic authorization is revoked",
    Effect.fnUntraced(function*() {
      const allowed = yield* Ref.make(true)
      const program = Effect.gen(function*() {
        const hub = yield* EphemeralHub.EphemeralHub
        const ready = yield* Deferred.make<void>()
        const joined = yield* hub.join(joinRequest(spaceA, memberA), "principal").pipe(
          Stream.tap((message) => {
            if (message._tag === "Snapshot") return Deferred.succeed(ready, undefined)
            return Effect.void
          }),
          Stream.runDrain,
          Effect.result,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(ready)
        yield* Ref.set(allowed, false)
        yield* TestClock.adjust("1 second")
        const result = yield* Fiber.join(joined)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "AuthorizationDenied")
      })
      yield* program.pipe(Effect.provide(
        EphemeralHub.layer({
          ...options,
          authorizationRefreshInterval: "1 second",
          authorize: () =>
            Ref.get(allowed).pipe(
              Effect.flatMap((isAllowed) => {
                if (isAllowed) return Effect.void
                return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "revoked" }))
              })
            )
        }).pipe(Layer.provide(NodeCrypto.layer))
      ))
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
          Identity.EphemeralSessionToken.make("eps_00000000-0000-4000-8000-000000000098"),
          "allowed"
        ).pipe(failureOf)
        assert.strictEqual(unavailable._tag, "EphemeralSessionUnavailable")
        yield* Fiber.interrupt(first.fiber)
      }).pipe(Effect.provide(
        EphemeralHub.layer({
          ...options,
          maximumWatchersPerSpace: 1,
          authorize: (input) => {
            if (input.principal === "denied") {
              return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "denied" }))
            }
            return Effect.void
          }
        }).pipe(Layer.provide(NodeCrypto.layer))
      ))
    })
  )
})
