import { makeFailedMessages, makeSettlementDaemonBody } from "@effect-local/example-chat-client/settlementDaemon"
import { makeServerLayer } from "@effect-local/example-chat-server/server"
import { LoginRequest, LoginResponse } from "@effect-local/example-chat-shared/auth"
import {
  AdvanceDelivery,
  AdvanceRead,
  type ChatUser,
  Conversation,
  ConversationReadState,
  definition,
  dmConversationId,
  groupConversationId,
  Message,
  MessageId,
  MessagesWindow,
  PresenceProfile,
  ReadStates,
  SendMessage,
  spaceId,
  StartConversation,
  tickState,
  tokenFor,
  Typing,
  users
} from "@effect-local/example-chat-shared/domain"
import { layerDomain } from "@effect-local/example-chat-shared/handlers"
import { NodeCrypto, NodeSocket } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"

/**
 * In-process end-to-end smoke tests: the real production server composition
 * (`makeServerLayer`, ephemeral port, in-memory SQLite) plus real SyncClient +
 * SqlReplica stacks per user, wired over loopback WebSockets. No fakes; all
 * waits rendezvous on reactivity streams and settlement streams - no sleeps.
 */

const alice = users[0]
const bob = users[1]
const carol = users[2]
const conversationId = dmConversationId(alice.id, bob.id)

// Identity ids are validated (cli_/inc_ + UUID), so tests mint fixed UUIDs per user.
const identitySuffix = (user: ChatUser) => {
  if (user.id === alice.id) return "1"
  return "2"
}

const clientIdFor = (user: ChatUser) =>
  Identity.ClientId.make(`cli_00000000-0000-4000-8000-00000000000${identitySuffix(user)}`)

const memberFor = (user: ChatUser) =>
  Protocol.EphemeralMember.make({
    clientId: clientIdFor(user),
    membershipIncarnation: Identity.MembershipIncarnation.make(
      `inc_00000000-0000-4000-8000-00000000000${identitySuffix(user)}`
    )
  })

const serverUrl = Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer
  const address = server.address
  if (address._tag === "UnixAddress") return yield* Effect.die("Expected the test server to use a TCP address")
  return `http://127.0.0.1:${address.port}/sync`
})

const layerReplicaFor = (user: ChatUser, bearer: string = tokenFor(user.id)) => {
  const credential = Redacted.make(bearer)
  const layerSync = SyncClient.layerWebSocket({ url: serverUrl }).pipe(
    Layer.provide(NodeSocket.layerWebSocketConstructor),
    Layer.provide(Authentication.layerCredentialProviderStatic(credential))
  )
  const layerDatabase = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )
  return SqlReplica.layer({
    definition,
    clientId: clientIdFor(user),
    defaultScope: Protocol.ReplicationScope.make({
      models: [Conversation.name, Message.name, ConversationReadState.name]
    }),
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "100 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerDomain),
    Layer.provideMerge(layerDatabase),
    Layer.provideMerge(layerSync)
  )
}

type ServerContext = Context.Context<HttpServer.HttpServer>

interface BootedUser {
  readonly space: Replica.Space
  readonly reactivity: Reactivity.Reactivity["Service"]
  readonly ephemeral: EphemeralClient.EphemeralClient["Service"]
  readonly queryReactivity: QueryReactivity.QueryReactivity["Service"]
}

const layerServer = makeServerLayer({ port: 0, databaseFile: ":memory:" })

const bootServer = Effect.map(
  Layer.build(layerServer),
  (context): ServerContext => context
)

const boot = Effect.fnUntraced(function*(serverContext: ServerContext, user: ChatUser, bearer?: string) {
  const context = yield* Layer.build(
    layerReplicaFor(user, bearer).pipe(Layer.provide(Layer.succeedContext(serverContext)))
  )
  const space = yield* Context.get(context, Replica.Replica).space(spaceId)
  const booted: BootedUser = {
    space,
    reactivity: Context.get(context, Reactivity.Reactivity),
    ephemeral: Context.get(context, EphemeralClient.EphemeralClient),
    queryReactivity: Context.get(context, QueryReactivity.QueryReactivity)
  }
  yield* space.activate
  yield* awaitStatus(booted, "Online")
  return booted
})

const awaitHead = <A, E extends { readonly _tag: string }, R,>(stream: Stream.Stream<A, E, R>) =>
  stream.pipe(
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const awaitStatus = (user: BootedUser, tag: "Online" | "Offline" | "NeedsAuthentication") =>
  awaitHead(
    user.reactivity.stream([ReactivityKey.status(spaceId)], user.space.status).pipe(
      Stream.filter((status) => status._tag === tag)
    )
  )

/**
 * Rendezvous on a local query result. Query reactivity tokens are only
 * invalidated while retained (`QueryReactivity.record` no-ops for unretained
 * keys, QueryReactivity.ts:54-57), and the browser's ReplicaAtom factory is
 * the usual retainer; tests retain the key directly, then stream
 * re-evaluations until the predicate holds. The retain leaks deliberately -
 * the QueryReactivity service is built fresh per test layer and dies with it.
 */
const awaitQuery = Effect.fnUntraced(function*<Q extends Query.Any,>(
  user: BootedUser,
  query: Q,
  payload: Q["payloadSchema"]["Type"],
  predicate: (value: Q["successSchema"]["Type"]) => boolean
) {
  const key = ReactivityKey.query(spaceId, query.name, payload)
  yield* user.queryReactivity.retain(key)
  return yield* awaitHead(
    user.reactivity.stream([key], user.space.query(query, payload)).pipe(Stream.filter(predicate))
  )
})

const startConversation = Effect.fnUntraced(function*(from: BootedUser) {
  const conversation: Conversation = {
    id: conversationId,
    kind: "dm",
    memberIds: [alice.id, bob.id],
    createdBy: alice.id,
    createdAt: yield* Clock.currentTimeMillis
  }
  const accepted = yield* Effect.forkChild(
    awaitHead(
      from.space.settlementsFor(StartConversation).pipe(
        Stream.filter((settled) =>
          !Replica.isLegacySettlement(settled.settlement) &&
          settled.settlement.pending.payload.id === conversationId &&
          settled.settlement.receipt._tag === "Accepted"
        )
      )
    ),
    { startImmediately: true }
  )
  yield* from.space.mutate(StartConversation, conversation)
  yield* Fiber.join(accepted)
  return conversation
})

describe("chat server", () => {
  it.effect(
    "exchanges credentials for a session token and rejects bad passwords",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const server = Context.get(serverContext, HttpServer.HttpServer)
      const address = server.address
      if (address._tag === "UnixAddress") {
        assert.fail("Expected the test server to use a TCP address")
      }
      const login = Effect.fnUntraced(function*(body: typeof LoginRequest.Type) {
        const encoded = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(LoginRequest))(body)
        return yield* Effect.promise(() =>
          fetch(`http://127.0.0.1:${address.port}/login`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: encoded
          })
        )
      })

      const ok = yield* login({ username: alice.id, password: alice.password })
      assert.strictEqual(ok.status, 200)
      const session = yield* Effect.promise(() => ok.json()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(LoginResponse))
      )
      assert.strictEqual(session.token, tokenFor(alice.id))
      assert.strictEqual(session.userId, alice.id)

      const rejected = yield* login({ username: alice.id, password: "wrong" })
      assert.strictEqual(rejected.status, 401)
    })
  )
})

describe("chat sync", () => {
  it.live(
    "sends a message and advances it to read across two clients",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const fromAlice = yield* boot(serverContext, alice)
      const fromBob = yield* boot(serverContext, bob)
      yield* startConversation(fromAlice)

      const createdAt = yield* Clock.currentTimeMillis
      const message: Message = {
        id: MessageId.make("msg_smoke_send"),
        conversationId,
        senderId: alice.id,
        text: "hello bob",
        createdAt
      }
      const accepted = yield* Effect.forkChild(
        awaitHead(
          fromAlice.space.settlementsFor(SendMessage).pipe(
            Stream.filter((settled) =>
              !Replica.isLegacySettlement(settled.settlement) &&
              settled.settlement.pending.payload.id === message.id &&
              settled.settlement.receipt._tag === "Accepted"
            )
          )
        ),
        { startImmediately: true }
      )
      yield* fromAlice.space.mutate(SendMessage, message)
      yield* Fiber.join(accepted)

      // Bob observes the replicated message.
      yield* awaitQuery(
        fromBob,
        MessagesWindow,
        { conversationId, limit: 10 },
        (window) => window.items.some((item) => item.id === message.id)
      )

      // Bob's client advances delivery, then read; Alice observes both rows
      // and derives the blue ticks.
      yield* fromBob.space.mutate(AdvanceDelivery, { conversationId, userId: bob.id, upTo: message.createdAt })
      const delivered = yield* awaitQuery(
        fromAlice,
        ReadStates,
        { conversationId },
        (rows) => rows.some((row) => row.userId === bob.id && row.deliveredUpTo >= message.createdAt)
      )
      assert.strictEqual(
        tickState({
          failed: false,
          pending: false,
          message,
          senderId: alice.id,
          readStates: delivered,
          memberIds: [alice.id, bob.id]
        }),
        "delivered"
      )

      yield* fromBob.space.mutate(AdvanceRead, { conversationId, userId: bob.id, upTo: message.createdAt })
      const read = yield* awaitQuery(
        fromAlice,
        ReadStates,
        { conversationId },
        (rows) => rows.some((row) => row.userId === bob.id && row.readUpTo >= message.createdAt)
      )
      assert.strictEqual(
        tickState({
          failed: false,
          pending: false,
          message,
          senderId: alice.id,
          readStates: read,
          memberIds: [alice.id, bob.id]
        }),
        "read"
      )
    })
  )

  it.live(
    "publishes and clears typing state through the ephemeral hub",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const fromAlice = yield* boot(serverContext, alice)
      const fromBob = yield* boot(serverContext, bob)

      // Publishing requires the sender's own ephemeral session to be active.
      yield* fromBob.ephemeral.session(PresenceProfile, {
        spaceId,
        member: memberFor(bob),
        value: { userId: bob.id, name: bob.name },
        ttl: "30 seconds"
      })
      const session = yield* fromAlice.ephemeral.session(PresenceProfile, {
        spaceId,
        member: memberFor(alice),
        value: { userId: alice.id, name: alice.name },
        ttl: "30 seconds"
      })
      const typing = session.state(Typing)
      const bobIsTyping = (entries: ReadonlyArray<EphemeralClient.StateEntry<typeof Typing>>) =>
        entries.some((entry) => entry.key === conversationId && entry.value.userId === bob.id)

      const seen = yield* Effect.forkChild(
        awaitHead(typing.pipe(Stream.filter(bobIsTyping))),
        { startImmediately: true }
      )
      yield* fromBob.ephemeral.publish(Typing, {
        spaceId,
        member: memberFor(bob),
        key: conversationId,
        payload: { userId: bob.id },
        ttl: "6 seconds"
      })
      yield* Fiber.join(seen)

      const cleared = yield* Effect.forkChild(
        awaitHead(typing.pipe(Stream.filter((entries) => !bobIsTyping(entries)))),
        { startImmediately: true }
      )
      yield* fromBob.ephemeral.remove(Typing, { spaceId, member: memberFor(bob), key: conversationId })
      yield* Fiber.join(cleared)
    })
  )

  it.live(
    "parks a rejected credential at NeedsAuthentication",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const context = yield* Layer.build(
        layerReplicaFor(alice, "chat-token-forged").pipe(Layer.provide(Layer.succeedContext(serverContext)))
      )
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      const booted: BootedUser = {
        space,
        reactivity: Context.get(context, Reactivity.Reactivity),
        ephemeral: Context.get(context, EphemeralClient.EphemeralClient),
        queryReactivity: Context.get(context, QueryReactivity.QueryReactivity)
      }
      yield* space.activate
      yield* awaitStatus(booted, "NeedsAuthentication")
    })
  )

  it.live(
    "rejects a message sent under another user's identity",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const fromAlice = yield* boot(serverContext, alice)
      yield* startConversation(fromAlice)

      const forged: Message = {
        id: MessageId.make("msg_smoke_forged"),
        conversationId,
        senderId: bob.id,
        text: "i am totally bob",
        createdAt: yield* Clock.currentTimeMillis
      }
      const rejected = yield* Effect.forkChild(
        awaitHead(
          fromAlice.space.settlementsFor(SendMessage).pipe(
            Stream.filter((settled) =>
              !Replica.isLegacySettlement(settled.settlement) &&
              settled.settlement.pending.payload.id === forged.id &&
              settled.settlement.receipt._tag === "Rejected"
            )
          )
        ),
        { startImmediately: true }
      )
      // The local handler accepts (bob is a member); the server authorizer
      // rejects because the principal is alice.
      yield* fromAlice.space.mutate(SendMessage, forged)
      const settled = yield* Fiber.join(rejected)
      assert.strictEqual(settled.settlement.receipt._tag, "Rejected")
      if (settled.settlement.receipt._tag === "Rejected") {
        assert.strictEqual(settled.settlement.receipt.origin, "Authorization")
      }

      // The terminal rejection rolled the optimistic write back.
      const window = yield* fromAlice.space.query(MessagesWindow, { conversationId, limit: 10 })
      assert.isFalse(window.items.some((item) => item.id === forged.id))
    })
  )

  it.live(
    "rejects a group conversation that does not include the full roster",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const fromAlice = yield* boot(serverContext, alice)

      // The group has a single deterministic id; whoever creates it first
      // would otherwise pin an arbitrary member subset forever.
      const partial: Conversation = {
        id: groupConversationId,
        kind: "group",
        memberIds: [alice.id],
        createdBy: alice.id,
        createdAt: yield* Clock.currentTimeMillis
      }
      // The shared handler rejects locally, so the mutation fails immediately
      // instead of reaching the server.
      const rejection = yield* Effect.match(fromAlice.space.mutate(StartConversation, partial), {
        onFailure: (error) => error,
        onSuccess: () => undefined
      })
      assert.isDefined(rejection)
      if (rejection !== undefined) {
        assert.strictEqual(rejection._tag, "StartConversationRejection")
        if (rejection._tag === "StartConversationRejection") {
          assert.strictEqual(rejection.reason, "InvalidGroupRoster")
        }
      }

      // A duplicate must not substitute for a missing member: length equality
      // plus "every member is on the roster" would otherwise pin the group
      // without dave forever (his SendMessage would NotAMember-reject with no
      // repair path, since the deterministic id is create-if-absent).
      const duplicated: Conversation = {
        id: groupConversationId,
        kind: "group",
        memberIds: [alice.id, alice.id, bob.id, carol.id],
        createdBy: alice.id,
        createdAt: yield* Clock.currentTimeMillis
      }
      const dupRejection = yield* Effect.match(fromAlice.space.mutate(StartConversation, duplicated), {
        onFailure: (error) => error,
        onSuccess: () => undefined
      })
      assert.isDefined(dupRejection)
      if (dupRejection !== undefined) {
        assert.strictEqual(dupRejection._tag, "StartConversationRejection")
        if (dupRejection._tag === "StartConversationRejection") {
          assert.strictEqual(dupRejection.reason, "InvalidGroupRoster")
        }
      }
    })
  )

  it.live(
    "rejects a conversation credited to another user",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      const fromAlice = yield* boot(serverContext, alice)

      const forged: Conversation = {
        id: conversationId,
        kind: "dm",
        memberIds: [alice.id, bob.id],
        createdBy: bob.id,
        createdAt: yield* Clock.currentTimeMillis
      }
      const settled = yield* Effect.forkChild(
        awaitHead(
          fromAlice.space.settlementsFor(StartConversation).pipe(
            Stream.filter((entry) =>
              !Replica.isLegacySettlement(entry.settlement) && entry.settlement.pending.payload.id === forged.id
            )
          )
        ),
        { startImmediately: true }
      )
      // The local handler accepts (well-formed dm); the server authorizer must
      // reject because createdBy does not match the principal.
      yield* fromAlice.space.mutate(StartConversation, forged)
      const outcome = yield* Fiber.join(settled)
      assert.strictEqual(outcome.settlement.receipt._tag, "Rejected")
      if (outcome.settlement.receipt._tag === "Rejected") {
        assert.strictEqual(outcome.settlement.receipt.origin, "Authorization")
      }
      const stored = yield* fromAlice.space.get(Conversation, forged.id)
      assert.isTrue(Option.isNone(stored))
    })
  )

  it.live(
    "settlement daemon surfaces a terminal rejection and advances the acknowledged floor",
    Effect.fnUntraced(function*() {
      const serverContext = yield* bootServer
      // Production atom wiring: the same ReplicaAtom factory the browser app
      // uses, over the Node test stack, mounted in a real registry.
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const graph = ReplicaAtom.make(
        layerReplicaFor(alice).pipe(Layer.provide(Layer.succeedContext(serverContext)))
      )
      const overlay = makeFailedMessages()
      const daemonAtom = graph.runtime.atom(makeSettlementDaemonBody(overlay))
      yield* AtomRegistry.mount(daemonAtom)(registry)

      // Boot the shared stack through the same runtime and take the space.
      const stackAtom = graph.runtime.atom(Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const space = yield* replica.space(spaceId)
        yield* space.activate
        const reactivity = yield* Reactivity.Reactivity
        yield* awaitHead(
          reactivity.stream([ReactivityKey.status(spaceId)], space.status).pipe(
            Stream.filter((status) => status._tag === "Online")
          )
        )
        return space
      }))
      const space = yield* AtomRegistry.getResult(registry, stackAtom, { suspendOnWaiting: true })

      const conversation: Conversation = {
        id: conversationId,
        kind: "dm",
        memberIds: [alice.id, bob.id],
        createdBy: alice.id,
        createdAt: yield* Clock.currentTimeMillis
      }
      yield* space.mutate(StartConversation, conversation)

      const forged: Message = {
        id: MessageId.make("msg_smoke_daemon_forged"),
        conversationId,
        senderId: bob.id,
        text: "i am totally bob",
        createdAt: yield* Clock.currentTimeMillis
      }
      const overlayHit = yield* Deferred.make<void>()
      const unsubscribe = registry.subscribe(
        overlay,
        (map: ReadonlyMap<MessageId, Message>) => {
          if (map.has(forged.id)) Effect.runSync(Deferred.succeed(overlayHit, void 0))
        },
        { immediate: true }
      )
      yield* Effect.addFinalizer(() => Effect.sync(unsubscribe))

      // The local handler accepts (bob is a member); the server authorizer
      // rejects because the principal is alice - a terminal rejection.
      yield* space.mutate(SendMessage, forged)
      yield* Deferred.await(overlayHit)

      // The daemon writes the overlay before acknowledging, so the observed
      // overlay entry does not guarantee the floor advanced: a fresh stream
      // from the acknowledged floor may still replay the forged rejection
      // (the daemon's re-add would be idempotent). Skip replays and assert
      // the next distinct settlement is the follow-up's acceptance.
      const followUp: Message = {
        id: MessageId.make("msg_smoke_daemon_followup"),
        conversationId,
        senderId: alice.id,
        text: "real",
        createdAt: yield* Clock.currentTimeMillis
      }
      const probe = yield* Effect.forkChild(
        space.settlementsFor(SendMessage, { from: "acknowledged" }).pipe(
          Stream.filter((entry) =>
            Replica.isLegacySettlement(entry.settlement) || entry.settlement.pending.payload.id !== forged.id
          ),
          awaitHead
        ),
        { startImmediately: true }
      )
      yield* space.mutate(SendMessage, followUp)
      const first = yield* Fiber.join(probe)
      assert.strictEqual(first.settlement.receipt._tag, "Accepted")
      if (!Replica.isLegacySettlement(first.settlement)) {
        assert.strictEqual(first.settlement.pending.payload.id, followUp.id)
      }
    })
  )
})
