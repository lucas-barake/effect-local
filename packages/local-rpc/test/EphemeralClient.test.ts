import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
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
        const joined = yield* client.join({ spaceId, member, value: null, ttlMillis: 60_000 }).pipe(
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
})
