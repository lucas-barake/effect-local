import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as EphemeralClient from "../src/EphemeralClient.js"
import * as ProtocolSession from "../src/ProtocolSession.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const member = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

describe("EphemeralClient", () => {
  it.effect(
    "normalizes duration inputs before the RPC boundary",
    Effect.fnUntraced(function*() {
      const messages = yield* Queue.unbounded<Protocol.EphemeralJoinMessage>()
      const joinedTtlMillis = yield* Deferred.make<number>()
      const publishedTtlMillis = yield* Deferred.make<number>()
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The RPC client is an external boundary. This test implements only the three ephemera calls it exercises.
      const fakeClient = {
        JoinEphemeral: (request: typeof Protocol.VersionedEphemeralJoinRequest.Type) =>
          Deferred.succeed(joinedTtlMillis, request.ttlMillis).pipe(Effect.as(messages)),
        HeartbeatEphemeral: () => Effect.succeed(null),
        PublishEphemeral: (request: typeof Protocol.VersionedEphemeralPublishRequest.Type) => {
          let ttlMillis = 0
          if ("ttlMillis" in request.request) ttlMillis = request.request.ttlMillis
          return Deferred.succeed(publishedTtlMillis, ttlMillis).pipe(Effect.as(null))
        }
      } as unknown as ProtocolSession.Service["client"]
      const session = ProtocolSession.ProtocolSession.of({
        client: fakeClient,
        version: Effect.succeed(Protocol.currentProtocolVersion),
        rejected: () => Effect.succeed(Protocol.currentProtocolVersion)
      })
      const layerClient = EphemeralClient.layerFromSession().pipe(
        Layer.provide(Layer.succeed(ProtocolSession.ProtocolSession, session))
      )
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const snapshot = yield* Deferred.make<void>()
        yield* Queue.offer(
          messages,
          Protocol.EphemeralSessionStarted.make({
            spaceId,
            member,
            sessionToken: Identity.EphemeralSessionToken.make(
              "eps_00000000-0000-4000-8000-000000000001"
            ),
            leaseMillis: 60_000
          })
        )
        yield* Queue.offer(
          messages,
          Protocol.EphemeralSnapshot.make({
            spaceId,
            revision: Identity.EphemeralRevision.make(1),
            members: [{ member, value: null, expiresAtMillis: 60_000 }],
            states: []
          })
        )
        const joined = yield* client.join({ spaceId, member, value: null, ttl: "1 minute" }).pipe(
          Stream.tap((message) => {
            if (message._tag === "Snapshot") return Deferred.succeed(snapshot, undefined)
            return Effect.void
          }),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        const joinedTtl = yield* Deferred.await(joinedTtlMillis).pipe(
          Effect.raceFirst(Fiber.join(joined).pipe(Effect.as(-1)))
        )
        assert.strictEqual(joinedTtl, 60_000)
        yield* Deferred.await(snapshot)
        yield* client.publish({
          _tag: "Event",
          spaceId,
          member,
          channel: "typing",
          value: true,
          ttl: "5 seconds"
        })
        assert.strictEqual(yield* Deferred.await(publishedTtlMillis), 5_000)
        yield* Fiber.interrupt(joined)
      })
      yield* program.pipe(Effect.provide(layerClient))
    })
  )

  it.effect(
    "schedules heartbeat from the server accepted lease",
    Effect.fnUntraced(function*() {
      const messages = yield* Queue.unbounded<Protocol.EphemeralJoinMessage>()
      const heartbeated = yield* Deferred.make<void>()
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The RPC client is an external boundary. This test implements only the three ephemera calls it exercises.
      const fakeClient = {
        JoinEphemeral: () => Effect.succeed(messages),
        HeartbeatEphemeral: () => Deferred.succeed(heartbeated, undefined).pipe(Effect.as(null)),
        PublishEphemeral: () => Effect.succeed(null)
      } as unknown as ProtocolSession.Service["client"]
      const session = ProtocolSession.ProtocolSession.of({
        client: fakeClient,
        version: Effect.succeed(Protocol.currentProtocolVersion),
        rejected: () => Effect.succeed(Protocol.currentProtocolVersion)
      })
      const layerClient = EphemeralClient.layerFromSession({ heartbeatInterval: "20 seconds" }).pipe(
        Layer.provide(Layer.succeed(ProtocolSession.ProtocolSession, session))
      )
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const snapshot = yield* Deferred.make<void>()
        yield* Queue.offer(
          messages,
          Protocol.EphemeralSessionStarted.make({
            spaceId,
            member,
            sessionToken: Identity.EphemeralSessionToken.make(
              "eps_00000000-0000-4000-8000-000000000001"
            ),
            leaseMillis: 1_000
          })
        )
        yield* Queue.offer(
          messages,
          Protocol.EphemeralSnapshot.make({
            spaceId,
            revision: Identity.EphemeralRevision.make(1),
            members: [{ member, value: null, expiresAtMillis: 1_000 }],
            states: []
          })
        )
        const joined = yield* client.join({ spaceId, member, value: null, ttl: "1 minute" }).pipe(
          Stream.tap((message) => {
            if (message._tag === "Snapshot") return Deferred.succeed(snapshot, undefined)
            return Effect.void
          }),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(snapshot)
        yield* TestClock.adjust("500 millis")
        yield* Deferred.await(heartbeated)
        assert.strictEqual(joined.pollUnsafe(), undefined)
        yield* Fiber.interrupt(joined)
      })
      yield* program.pipe(Effect.provide(layerClient))
    })
  )

  it.effect(
    "rejoins after a normal stream completion and exposes the replacement snapshot",
    Effect.fnUntraced(function*() {
      const joins = yield* Ref.make(0)
      const recovered = yield* Deferred.make<void>()
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The RPC client is an external boundary. This test implements only the three ephemera calls it exercises.
      const fakeClient = {
        JoinEphemeral: Effect.fnUntraced(function*() {
          const join = yield* Ref.updateAndGet(joins, (count) => count + 1)
          const messages = yield* Queue.unbounded<Protocol.EphemeralJoinMessage, Cause.Done>()
          let sessionToken = Identity.EphemeralSessionToken.make(
            "eps_00000000-0000-4000-8000-000000000001"
          )
          if (join === 2) {
            sessionToken = Identity.EphemeralSessionToken.make(
              "eps_00000000-0000-4000-8000-000000000002"
            )
          }
          yield* Queue.offer(
            messages,
            Protocol.EphemeralSessionStarted.make({
              spaceId,
              member,
              sessionToken,
              leaseMillis: 10_000
            })
          )
          yield* Queue.offer(
            messages,
            Protocol.EphemeralSnapshot.make({
              spaceId,
              revision: Identity.EphemeralRevision.make(join),
              members: [{ member, value: null, expiresAtMillis: 10_000 }],
              states: []
            })
          )
          if (join === 1) yield* Queue.end(messages)
          return messages
        }),
        HeartbeatEphemeral: () => Effect.succeed(null),
        PublishEphemeral: () => Effect.succeed(null)
      } as unknown as ProtocolSession.Service["client"]
      const session = ProtocolSession.ProtocolSession.of({
        client: fakeClient,
        version: Effect.succeed(Protocol.currentProtocolVersion),
        rejected: () => Effect.succeed(Protocol.currentProtocolVersion)
      })
      const layerClient = EphemeralClient.layerFromSession().pipe(
        Layer.provide(Layer.succeed(ProtocolSession.ProtocolSession, session))
      )
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const joined = yield* client.join({ spaceId, member, value: null, ttl: "10 seconds" }).pipe(
          Stream.tap((message) => {
            if (message._tag === "Snapshot" && message.revision === 2) {
              return Deferred.succeed(recovered, undefined)
            }
            return Effect.void
          }),
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(recovered)
        assert.strictEqual(yield* Ref.get(joins), 2)
        yield* Fiber.interrupt(joined)
      })
      yield* program.pipe(Effect.provide(layerClient))
    })
  )

  it.effect(
    "does not rejoin after the server rejects a replaced session",
    Effect.fnUntraced(function*() {
      const joins = yield* Ref.make(0)
      // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The RPC client is an external boundary. This test implements only the three ephemera calls it exercises.
      const fakeClient = {
        JoinEphemeral: Effect.fnUntraced(function*() {
          yield* Ref.update(joins, (count) => count + 1)
          const messages = yield* Queue.unbounded<
            Protocol.EphemeralJoinMessage,
            ReplicaError.ReplicaError
          >()
          yield* Queue.offer(
            messages,
            Protocol.EphemeralSessionStarted.make({
              spaceId,
              member,
              sessionToken: Identity.EphemeralSessionToken.make(
                "eps_00000000-0000-4000-8000-000000000001"
              ),
              leaseMillis: 10_000
            })
          )
          yield* Queue.offer(
            messages,
            Protocol.EphemeralSnapshot.make({
              spaceId,
              revision: Identity.EphemeralRevision.make(1),
              members: [{ member, value: null, expiresAtMillis: 10_000 }],
              states: []
            })
          )
          yield* Queue.fail(
            messages,
            new ReplicaError.EphemeralSessionUnavailable({
              spaceId,
              clientId: member.clientId,
              membershipIncarnation: member.membershipIncarnation
            })
          )
          return messages
        }),
        HeartbeatEphemeral: () => Effect.succeed(null),
        PublishEphemeral: () => Effect.succeed(null)
      } as unknown as ProtocolSession.Service["client"]
      const session = ProtocolSession.ProtocolSession.of({
        client: fakeClient,
        version: Effect.succeed(Protocol.currentProtocolVersion),
        rejected: () => Effect.succeed(Protocol.currentProtocolVersion)
      })
      const layerClient = EphemeralClient.layerFromSession().pipe(
        Layer.provide(Layer.succeed(ProtocolSession.ProtocolSession, session))
      )
      const result = yield* EphemeralClient.EphemeralClient.use((client) =>
        client.join({ spaceId, member, value: null, ttl: "10 seconds" }).pipe(Stream.runDrain)
      ).pipe(Effect.provide(layerClient), Effect.result)
      assert.isTrue(Result.isFailure(result))
      assert.strictEqual(yield* Ref.get(joins), 1)
    })
  )
})
