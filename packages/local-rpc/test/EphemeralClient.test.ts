import { assert, describe, it } from "@effect/vitest"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
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
        yield* client.session(Anonymous, { spaceId, member, value: undefined, ttl: "1 minute" })
        assert.strictEqual(yield* Deferred.await(joinedTtlMillis), 60_000)
        yield* client.publish(Typing, {
          spaceId,
          member,
          payload: { conversationId: ConversationId.make("conversation-1"), active: true },
          ttl: "5 seconds"
        })
        assert.strictEqual(yield* Deferred.await(publishedTtlMillis), 5_000)
      })
      yield* program.pipe(Effect.provide(layerClient), Effect.scoped)
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
        yield* client.session(Anonymous, { spaceId, member, value: undefined, ttl: "1 minute" })
        yield* TestClock.adjust("500 millis")
        yield* Deferred.await(heartbeated)
      })
      yield* program.pipe(Effect.provide(layerClient), Effect.scoped)
    })
  )

  it.effect(
    "rejoins after a normal stream completion and exposes the replacement snapshot",
    Effect.fnUntraced(function*() {
      const joins = yield* Ref.make(0)
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
          const roster = [{ member, value: null, expiresAtMillis: 10_000 }]
          if (join === 2) roster.push({ member: memberB, value: null, expiresAtMillis: 10_000 })
          yield* Queue.offer(
            messages,
            Protocol.EphemeralSnapshot.make({
              spaceId,
              revision: Identity.EphemeralRevision.make(join),
              members: roster,
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
        const opened = yield* client.session(Anonymous, {
          spaceId,
          member,
          value: undefined,
          ttl: "10 seconds"
        })
        const rejoined = yield* opened.members.pipe(
          Stream.filter((roster) => roster.some((entry) => entry.member.clientId === memberB.clientId)),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Fiber.join(rejoined)
        assert.strictEqual(yield* Ref.get(joins), 2)
      })
      yield* program.pipe(Effect.provide(layerClient), Effect.scoped)
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
        client.session(Anonymous, { spaceId, member, value: undefined, ttl: "10 seconds" }).pipe(
          Effect.flatMap((opened) => opened.members.pipe(Stream.runDrain))
        )
      ).pipe(Effect.provide(layerClient), Effect.scoped, Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "EphemeralSessionUnavailable")
      }
      assert.strictEqual(yield* Ref.get(joins), 1)
    })
  )
})

const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const memberB = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000002")
})

const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"))
const Typing = Ephemeral.make("Typing", {
  kind: "event",
  payload: { conversationId: ConversationId, active: Schema.Boolean }
})
const Pings = Ephemeral.make("Pings", { kind: "event", payload: { count: Schema.Number } })
const ReadPosition = Ephemeral.make("ReadPosition", {
  kind: "state",
  key: ConversationId,
  payload: { messageId: Schema.String }
})
const Sentinel = Ephemeral.make("Sentinel", { kind: "state", key: Schema.String, payload: { marker: Schema.String } })
const Profile = Ephemeral.member({ displayName: Schema.String })
const Anonymous = Ephemeral.member()

const sessionStarted = (space: Identity.SpaceId, joiner: Protocol.EphemeralMember) =>
  Protocol.EphemeralSessionStarted.make({
    spaceId: space,
    member: joiner,
    sessionToken: Identity.EphemeralSessionToken.make("eps_00000000-0000-4000-8000-000000000001"),
    leaseMillis: 60_000
  })

const snapshot = (
  space: Identity.SpaceId,
  revision: number,
  options?: {
    readonly members?: ReadonlyArray<Protocol.EphemeralMemberEntry>
    readonly states?: ReadonlyArray<Protocol.EphemeralStateEntry>
  }
) =>
  Protocol.EphemeralSnapshot.make({
    spaceId: space,
    revision: Identity.EphemeralRevision.make(revision),
    members: options?.members ?? [{ member, value: { displayName: "Ada" }, expiresAtMillis: 60_000 }],
    states: options?.states ?? []
  })

const eventMessage = (
  space: Identity.SpaceId,
  revision: number,
  channel: string,
  value: typeof Schema.Json.Type,
  sender: Protocol.EphemeralMember = member
) =>
  Protocol.EphemeralEvent.make({
    spaceId: space,
    revision: Identity.EphemeralRevision.make(revision),
    entry: {
      member: sender,
      channel: Protocol.EphemeralChannel.make(channel),
      value,
      expiresAtMillis: 60_000
    }
  })

const stateSet = (
  space: Identity.SpaceId,
  revision: number,
  channel: string,
  key: string,
  value: typeof Schema.Json.Type
) =>
  Protocol.EphemeralStateSet.make({
    spaceId: space,
    revision: Identity.EphemeralRevision.make(revision),
    entry: {
      member,
      channel: Protocol.EphemeralChannel.make(channel),
      key: Protocol.EphemeralKey.make(key),
      value,
      expiresAtMillis: 60_000
    }
  })

const makeTypedHarness = Effect.fnUntraced(function*() {
  const messagesA = yield* Queue.unbounded<Protocol.EphemeralJoinMessage>()
  const messagesB = yield* Queue.unbounded<Protocol.EphemeralJoinMessage>()
  const published = yield* Queue.unbounded<typeof Protocol.VersionedEphemeralPublishRequest.Type>()
  const joins = yield* Ref.make(0)
  // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The RPC client is an external boundary. This test implements only the three ephemera calls it exercises.
  const fakeClient = {
    JoinEphemeral: (request: typeof Protocol.VersionedEphemeralJoinRequest.Type) =>
      Ref.update(joins, (count) => count + 1).pipe(
        Effect.andThen(Effect.sync(() => {
          if (request.spaceId === spaceB) return messagesB
          return messagesA
        }))
      ),
    HeartbeatEphemeral: () => Effect.succeed(null),
    PublishEphemeral: (request: typeof Protocol.VersionedEphemeralPublishRequest.Type) =>
      Queue.offer(published, request).pipe(Effect.as(null))
  } as unknown as ProtocolSession.Service["client"]
  const session = ProtocolSession.ProtocolSession.of({
    client: fakeClient,
    version: Effect.succeed(Protocol.currentProtocolVersion),
    rejected: () => Effect.succeed(Protocol.currentProtocolVersion)
  })
  const layerClient = EphemeralClient.layerFromSession().pipe(
    Layer.provide(Layer.succeed(ProtocolSession.ProtocolSession, session))
  )
  return { messagesA, messagesB, published, joins, layerClient }
})

const sessionOptions = { spaceId, member, value: { displayName: "Ada" }, ttl: "1 minute" } as const

describe("EphemeralClient typed definitions", () => {
  it.effect(
    "publishes typed events and state through the generic wire protocol",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        yield* client.session(Profile, sessionOptions)
        yield* client.publish(Typing, {
          spaceId,
          member,
          payload: { conversationId: ConversationId.make("conversation-1"), active: true },
          ttl: "5 seconds"
        })
        const event = yield* Queue.take(harness.published)
        assert.strictEqual(event.request._tag, "Event")
        if (event.request._tag === "Event") {
          assert.strictEqual(event.request.channel, "Typing")
          assert.deepStrictEqual(event.request.value, { conversationId: "conversation-1", active: true })
          assert.strictEqual(event.request.ttlMillis, 5_000)
        }
        yield* client.publish(ReadPosition, {
          spaceId,
          member,
          key: ConversationId.make("conversation-1"),
          payload: { messageId: "message-9" },
          ttl: "10 seconds"
        })
        const state = yield* Queue.take(harness.published)
        assert.strictEqual(state.request._tag, "SetState")
        if (state.request._tag === "SetState") {
          assert.strictEqual(state.request.channel, "ReadPosition")
          assert.strictEqual(state.request.key, "conversation-1")
          assert.deepStrictEqual(state.request.value, { messageId: "message-9" })
          assert.strictEqual(state.request.ttlMillis, 10_000)
        }
        yield* client.clear(Typing, { spaceId, member })
        const cleared = yield* Queue.take(harness.published)
        assert.strictEqual(cleared.request._tag, "ClearEvent")
        if (cleared.request._tag === "ClearEvent") assert.strictEqual(cleared.request.channel, "Typing")
        yield* client.remove(ReadPosition, { spaceId, member, key: ConversationId.make("conversation-1") })
        const removed = yield* Queue.take(harness.published)
        assert.strictEqual(removed.request._tag, "RemoveState")
        if (removed.request._tag === "RemoveState") assert.strictEqual(removed.request.key, "conversation-1")
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "shares one join across typed projections and identical session requests",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const other = yield* client.session(Profile, sessionOptions)
        const events = yield* session.events(Typing).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        const states = yield* session.state(ReadPosition).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        const membersHead = yield* other.members.pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        assert.isDefined(yield* Fiber.join(membersHead).pipe(Effect.map(Option.getOrUndefined)))
        assert.isDefined(
          yield* session.state(Sentinel).pipe(Stream.runHead, Effect.map(Option.getOrUndefined))
        )
        yield* Queue.offer(
          harness.messagesA,
          eventMessage(spaceId, 2, "Typing", { conversationId: "conversation-1", active: true })
        )
        assert.isDefined(yield* Fiber.join(events).pipe(Effect.map(Option.getOrUndefined)))
        yield* Fiber.interrupt(states)
        assert.strictEqual(yield* Ref.get(harness.joins), 1)
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "rejects a mismatched session request for a live member",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        yield* client.session(Profile, sessionOptions)
        const mismatched = yield* client.session(Profile, {
          ...sessionOptions,
          value: { displayName: "Grace" }
        }).pipe(Effect.result)
        assert.isTrue(Result.isFailure(mismatched))
        if (Result.isFailure(mismatched)) {
          assert.strictEqual(mismatched.failure._tag, "InvalidConfiguration")
        }
        assert.strictEqual(yield* Ref.get(harness.joins), 1)
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "decodes typed events without manual schema calls",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const received = yield* session.events(Typing).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(
          harness.messagesA,
          eventMessage(spaceId, 2, "Typing", { conversationId: "conversation-1", active: true }, memberB)
        )
        const event = Option.getOrUndefined(yield* Fiber.join(received))
        assert.deepStrictEqual(event, {
          member: memberB,
          payload: { conversationId: ConversationId.make("conversation-1"), active: true }
        })
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "never delivers events to projections subscribed after publication",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const probe = yield* session.events(Pings).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(
          harness.messagesA,
          eventMessage(spaceId, 2, "Typing", { conversationId: "conversation-1", active: true })
        )
        yield* Queue.offer(harness.messagesA, eventMessage(spaceId, 3, "Pings", { count: 1 }))
        assert.isDefined(Option.getOrUndefined(yield* Fiber.join(probe)))
        const late = yield* session.events(Typing).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(
          harness.messagesA,
          eventMessage(spaceId, 4, "Typing", { conversationId: "conversation-2", active: false })
        )
        const received = Option.getOrUndefined(yield* Fiber.join(late))
        assert.deepStrictEqual(received?.payload, {
          conversationId: ConversationId.make("conversation-2"),
          active: false
        })
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "replays current typed state to late projection subscribers",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(
        harness.messagesA,
        snapshot(spaceId, 1, {
          states: [{
            member,
            channel: Protocol.EphemeralChannel.make("ReadPosition"),
            key: Protocol.EphemeralKey.make("conversation-1"),
            value: { messageId: "message-1" },
            expiresAtMillis: 60_000
          }]
        })
      )
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const probe = yield* session.state(Sentinel).pipe(
          Stream.filter((entries) => entries.length > 0),
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(
          harness.messagesA,
          stateSet(spaceId, 2, "ReadPosition", "conversation-2", { messageId: "message-2" })
        )
        yield* Queue.offer(harness.messagesA, stateSet(spaceId, 3, "Sentinel", "probe", { marker: "done" }))
        assert.isDefined(Option.getOrUndefined(yield* Fiber.join(probe)))
        const entries = Option.getOrUndefined(
          yield* session.state(ReadPosition).pipe(Stream.runHead)
        )
        assert.isDefined(entries)
        assert.strictEqual(entries?.length, 2)
        const keys = entries?.map((entry) => entry.key).toSorted()
        assert.deepStrictEqual(keys, [
          ConversationId.make("conversation-1"),
          ConversationId.make("conversation-2")
        ])
        assert.deepStrictEqual(
          entries?.map((entry) => entry.value.messageId).toSorted(),
          ["message-1", "message-2"]
        )
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "isolates malformed payloads to the affected projection",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const poisoned = yield* session.events(Typing).pipe(
          Stream.runDrain,
          Effect.result,
          Effect.forkChild({ startImmediately: true })
        )
        const healthy = yield* session.events(Pings).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(harness.messagesA, eventMessage(spaceId, 2, "Typing", 42))
        const failure = yield* Fiber.join(poisoned)
        assert.isTrue(Result.isFailure(failure))
        if (Result.isFailure(failure)) {
          assert.strictEqual(failure.failure._tag, "EphemeralDecodeError")
        }
        yield* Queue.offer(harness.messagesA, eventMessage(spaceId, 3, "Pings", { count: 7 }))
        assert.isDefined(Option.getOrUndefined(yield* Fiber.join(healthy)))
        yield* Queue.offer(
          harness.messagesA,
          stateSet(spaceId, 4, "ReadPosition", "conversation-1", { messageId: "message-1" })
        )
        const recovered = yield* session.state(ReadPosition).pipe(
          Stream.filter((entries) => entries.length > 0),
          Stream.runHead
        )
        assert.strictEqual(Option.getOrUndefined(recovered)?.length, 1)
        assert.strictEqual(yield* Ref.get(harness.joins), 1)
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "keeps definitions with different schemas apart",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const pings = yield* session.events(Pings).pipe(
          Stream.runHead,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.offer(
          harness.messagesA,
          eventMessage(spaceId, 2, "Typing", { conversationId: "conversation-1", active: true })
        )
        yield* Queue.offer(harness.messagesA, eventMessage(spaceId, 3, "Pings", { count: 5 }))
        const received = Option.getOrUndefined(yield* Fiber.join(pings))
        assert.deepStrictEqual(received?.payload, { count: 5 })
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "decodes roster values through the member definition",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(harness.messagesA, snapshot(spaceId, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const session = yield* client.session(Profile, sessionOptions)
        const roster = Option.getOrUndefined(yield* session.members.pipe(Stream.runHead))
        assert.deepStrictEqual(roster, [{ member, value: { displayName: "Ada" }, expiresAtMillis: 60_000 }])
        yield* session.updateMember({ displayName: "Grace" })
        const updated = yield* Queue.take(harness.published)
        assert.strictEqual(updated.request._tag, "UpdateMember")
        if (updated.request._tag === "UpdateMember") {
          assert.deepStrictEqual(updated.request.value, { displayName: "Grace" })
        }
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )

  it.effect(
    "isolates typed sessions per space",
    Effect.fnUntraced(function*() {
      const harness = yield* makeTypedHarness()
      yield* Queue.offer(harness.messagesA, sessionStarted(spaceId, member))
      yield* Queue.offer(
        harness.messagesA,
        snapshot(spaceId, 1, {
          states: [{
            member,
            channel: Protocol.EphemeralChannel.make("ReadPosition"),
            key: Protocol.EphemeralKey.make("conversation-1"),
            value: { messageId: "message-1" },
            expiresAtMillis: 60_000
          }]
        })
      )
      yield* Queue.offer(harness.messagesB, sessionStarted(spaceB, member))
      yield* Queue.offer(harness.messagesB, snapshot(spaceB, 1))
      const program = Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        const sessionA = yield* client.session(Profile, sessionOptions)
        const sessionB = yield* client.session(Profile, { ...sessionOptions, spaceId: spaceB })
        const entriesA = Option.getOrUndefined(yield* sessionA.state(ReadPosition).pipe(Stream.runHead))
        const entriesB = Option.getOrUndefined(yield* sessionB.state(ReadPosition).pipe(Stream.runHead))
        assert.strictEqual(entriesA?.length, 1)
        assert.strictEqual(entriesB?.length, 0)
        assert.strictEqual(yield* Ref.get(harness.joins), 2)
      })
      yield* program.pipe(Effect.provide(harness.layerClient))
    })
  )
})
