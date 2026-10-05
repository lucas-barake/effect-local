import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as RpcSerialization from "effect/rpc/RpcSerialization"
import * as Schema from "effect/Schema"
import * as Socket from "effect/socket/Socket"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Authentication from "../src/Authentication.js"
import * as EphemeralClient from "../src/EphemeralClient.js"
import * as ProtocolSession from "../src/ProtocolSession.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncRpc from "../src/SyncRpc.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000911")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000911")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000911")
const member = Protocol.EphemeralMember.make({ clientId, membershipIncarnation })

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const definition = Definition.make({ version: 1, models: [Todo], mutations: [] })
const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const cursor = Protocol.ReplicationCursor.make({
  viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000911"),
  revision: Identity.ReplicationViewRevision.make(0)
})

const Presence = Ephemeral.member({ status: Schema.String })
const Reaction = Ephemeral.make("reaction", { kind: "event", payload: { emoji: Schema.String } })

const envelope = Effect.gen(function*() {
  const identity = {
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000911"),
    localSequence: Identity.LocalSequence.make(1),
    basis: Identity.ServerSequence.make(0),
    name: "PutTodo",
    payload: { id: "todo", title: "todo" },
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: definition.schemaIdentity,
    mutationVersion: Identity.SchemaVersion.make(1)
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
}).pipe(Effect.provide(NodeCrypto.layer))

type RequestId = string | number
type Reply = (requestId: RequestId) => ReadonlyArray<unknown>

const succeedWith = (value: unknown): Reply => (requestId) => [
  { _tag: "Exit", requestId, exit: { _tag: "Success", value } }
]
const failWith = (error: unknown): Reply => (requestId) => [
  { _tag: "Exit", requestId, exit: { _tag: "Failure", cause: [{ _tag: "Fail", error }] } }
]
const chunksOf = (...chunks: ReadonlyArray<ReadonlyArray<unknown>>): Reply => (requestId) =>
  chunks.map((values) => ({ _tag: "Chunk", requestId, values }))

const unknownSuccess = { skewed: true }
const unknownSuccessReply = succeedWith(unknownSuccess)
const unknownChunkReply = chunksOf([unknownSuccess])
const unknownErrorReply = failWith({ _tag: "CapacityExceeded", resource: "tenant storage quota", limit: 1 })
const malformedFrame: Reply = (requestId) => [`{"_tag":"Exit","requestId":${requestId},"exit":{"_tag":"Maybe"}}`]

const sessionStarted = Protocol.EphemeralSessionStarted.make({
  spaceId,
  member,
  sessionToken: Identity.EphemeralSessionToken.make("eps_00000000-0000-4000-8000-000000000911"),
  leaseMillis: 60_000
})
const snapshot = Protocol.EphemeralSnapshot.make({
  spaceId,
  revision: Identity.EphemeralRevision.make(1),
  members: [{ member, value: { status: "here" }, expiresAtMillis: 60_000 }],
  states: []
})
const joined = chunksOf([sessionStarted, snapshot])

const RequestFrame = Schema.Struct({
  _tag: Schema.Literal("Request"),
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.String
})
const isRequestFrame = Schema.is(RequestFrame)
const isPingFrame = Schema.is(Schema.Struct({ _tag: Schema.Literal("Ping") }))
const InterruptFrame = Schema.Struct({
  _tag: Schema.Literal("Interrupt"),
  requestId: Schema.Union([Schema.String, Schema.Number])
})
const isInterruptFrame = Schema.is(InterruptFrame)

const layerCredentialStatic = Authentication.layerCredentialProviderStatic(Redacted.make("secret"))

const layerCredentialDying = Layer.succeed(
  Authentication.CredentialProvider,
  Authentication.CredentialProvider.of({
    acquire: Effect.die("credential store crashed"),
    awaitChange: () => Effect.never
  })
)

const localSchemaError = Effect.gen(function*() {
  const decoded = yield* Effect.result(Schema.decodeUnknownEffect(Schema.Number)("not a number"))
  if (Result.isFailure(decoded)) return decoded.failure
  return yield* Effect.die("a string decoded as a number")
})

const layerCredentialDyingAt = (acquisition: number, defect: Schema.SchemaError) =>
  Layer.sync(Authentication.CredentialProvider, () => {
    let acquired = 0
    return Authentication.CredentialProvider.of({
      acquire: Effect.suspend(() => {
        acquired += 1
        if (acquired === acquisition) return Effect.die(defect)
        return Effect.succeed({ generation: 0, bearer: Redacted.make("secret") })
      }),
      awaitChange: () => Effect.never
    })
  })

interface WriteDefect {
  readonly tag: string
  readonly defect: Schema.SchemaError
}

const connect = Effect.fnUntraced(function*(
  script: ReadonlyMap<string, Reply>,
  layerCredential: Layer.Layer<Authentication.CredentialProvider>,
  writeDefect?: WriteDefect
) {
  const incoming = yield* Queue.unbounded<string | Uint8Array>()
  const outgoing = yield* Queue.unbounded<unknown>()
  const interrupted = yield* Queue.unbounded<RequestId>()
  const written: Array<string> = []
  const parser = RpcSerialization.json.makeUnsafe()
  const decoder = new TextDecoder()
  const deliver = (frame: unknown) => {
    if (typeof frame === "string") return Queue.offer(incoming, frame)
    const encoded = parser.encode(frame)
    if (encoded === undefined) return Effect.die("the JSON serializer did not encode a server frame")
    return Queue.offer(incoming, encoded)
  }
  const answer = (frame: unknown) => {
    if (isPingFrame(frame)) return deliver({ _tag: "Pong" })
    if (isInterruptFrame(frame)) return Queue.offer(interrupted, frame.requestId)
    if (!isRequestFrame(frame)) return Effect.void
    const reply = script.get(frame.tag)
    if (reply !== undefined) return Effect.forEach(reply(frame.id), deliver, { discard: true })
    if (frame.tag !== "Negotiate") return Effect.void
    return deliver({ _tag: "Exit", requestId: frame.id, exit: { _tag: "Success", value: { version: 1 } } })
  }
  yield* Queue.take(outgoing).pipe(Effect.flatMap(answer), Effect.forever, Effect.forkScoped)
  const socket = Socket.make({
    reader: Effect.succeed({
      pull: Queue.take(incoming).pipe(Effect.map((message) => [message] as const)),
      upgrade: () => Effect.void
    }),
    writer: Effect.succeed({
      write: (chunk: string | Uint8Array | Socket.CloseEvent) => {
        if (typeof chunk !== "string" && !(chunk instanceof Uint8Array)) return Effect.void
        let text = chunk
        if (typeof text !== "string") text = decoder.decode(text)
        const frames = parser.decode(text)
        for (const frame of frames) {
          if (!isRequestFrame(frame)) continue
          if (frame.tag === writeDefect?.tag) return Effect.die(writeDefect.defect)
          written.push(frame.tag)
        }
        return Queue.offerAll(outgoing, frames)
      },
      writeAll: () => Effect.void
    })
  })
  const layerSession = ProtocolSession.layer.pipe(
    Layer.provideMerge(SyncClient.layerProtocolSocket()),
    Layer.provide(Layer.fresh(Authentication.layerClient))
  )
  const context = yield* Layer.build(
    Layer.merge(
      SyncClient.layerFromSession(),
      EphemeralClient.layerFromSession({ heartbeatInterval: "1 second" })
    ).pipe(
      Layer.provide(layerSession),
      Layer.provide(layerCredential),
      Layer.provide(Layer.succeed(Socket.Socket, socket)),
      Layer.provide(SyncRpc.layerJson())
    )
  )
  yield* Effect.yieldNow
  return {
    engine: Context.get(context, SyncEngine.SyncEngine),
    ephemeral: Context.get(context, EphemeralClient.EphemeralClient),
    interrupted,
    written
  }
})

type Clients = Effect.Success<ReturnType<typeof connect>>

const outcome = <A, E extends { readonly _tag: string },>(exit: Exit.Exit<A, E>): string => {
  if (Exit.isSuccess(exit)) return "succeeded"
  const defect = Cause.findDefect(exit.cause)
  if (Result.isSuccess(defect)) {
    if (Schema.isSchemaError(defect.success)) return "defect: SchemaError"
    return `defect: ${String(defect.success)}`
  }
  const failure = Cause.findError(exit.cause)
  if (Result.isFailure(failure)) return "interrupted"
  const error = failure.success
  if (error._tag !== "ProtocolInvalid" || !("message" in error) || !("cause" in error)) return error._tag
  if (Schema.isSchemaError(error.cause)) return `ProtocolInvalid: ${String(error.message)} [cause: SchemaError]`
  return `ProtocolInvalid: ${String(error.message)}`
}

const pullRequest = {
  spaceId,
  clientId,
  membershipIncarnation,
  schema: definition.schemaIdentity,
  scope,
  scopeGeneration,
  cursor: null,
  limit: 1
}

const watchRequest = { spaceId, clientId, schema: definition.schemaIdentity, scope, scopeGeneration, cursor }

const openSession = (clients: Clients) =>
  clients.ephemeral.session(Presence, { spaceId, member, value: { status: "here" }, ttl: "1 minute" })

interface Call {
  readonly rpc: string
  readonly transportFailure: string
  readonly prelude: ReadonlyArray<readonly [string, Reply]>
  readonly streamed: boolean
  readonly acquisition: number
  readonly run: (clients: Clients) => Effect.Effect<string>
}

const exitOutcome = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.exit, Effect.map(outcome))

const calls: ReadonlyArray<Call> = [
  {
    rpc: "Negotiate",
    transportFailure: "ProtocolInvalid: The protocol negotiation failed",
    prelude: [],
    streamed: false,
    acquisition: 1,
    run: (clients) => exitOutcome(clients.engine.pull(pullRequest))
  },
  {
    rpc: "SubmitBatch",
    transportFailure: "ProtocolInvalid: The SubmitBatch RPC failed",
    prelude: [],
    streamed: false,
    acquisition: 2,
    run: (clients) =>
      envelope.pipe(
        Effect.flatMap((submitted) =>
          clients.engine.submitBatch({ envelopes: [submitted], schema: definition.schemaIdentity })
        ),
        exitOutcome
      )
  },
  {
    rpc: "Discard",
    transportFailure: "ProtocolInvalid: The Discard RPC failed",
    prelude: [],
    streamed: false,
    acquisition: 2,
    run: (clients) =>
      envelope.pipe(
        Effect.flatMap((discarded) =>
          clients.engine.discard({ envelope: discarded, schema: definition.schemaIdentity })
        ),
        exitOutcome
      )
  },
  {
    rpc: "Pull",
    transportFailure: "ProtocolInvalid: The Pull RPC failed",
    prelude: [],
    streamed: false,
    acquisition: 2,
    run: (clients) => exitOutcome(clients.engine.pull(pullRequest))
  },
  {
    rpc: "Bootstrap",
    transportFailure: "ProtocolInvalid: The Bootstrap RPC failed",
    prelude: [],
    streamed: false,
    acquisition: 2,
    run: (clients) =>
      exitOutcome(clients.engine.bootstrap({
        spaceId,
        clientId,
        membershipIncarnation,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor,
        snapshotId: Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000911"),
        afterOrdinal: -1,
        limit: 1
      }))
  },
  {
    rpc: "Watch",
    transportFailure: "ProtocolInvalid: The Watch RPC failed",
    prelude: [],
    streamed: true,
    acquisition: 2,
    run: (clients) => clients.engine.watch(watchRequest).pipe(Stream.runDrain, exitOutcome)
  },
  {
    rpc: "JoinEphemeral",
    transportFailure: "ProtocolInvalid: The JoinEphemeral RPC failed",
    prelude: [],
    streamed: true,
    acquisition: 2,
    run: (clients) => openSession(clients).pipe(Effect.scoped, exitOutcome)
  },
  {
    rpc: "PublishEphemeral",
    transportFailure: "ProtocolInvalid: The PublishEphemeral RPC failed",
    prelude: [["JoinEphemeral", joined]],
    streamed: false,
    acquisition: 3,
    run: (clients) =>
      openSession(clients).pipe(
        Effect.andThen(
          clients.ephemeral.publish(Reaction, { spaceId, member, payload: { emoji: "+1" }, ttl: "5 seconds" })
        ),
        Effect.scoped,
        exitOutcome
      )
  },
  {
    rpc: "HeartbeatEphemeral",
    transportFailure: "ProtocolInvalid: The JoinEphemeral RPC failed",
    prelude: [["JoinEphemeral", joined]],
    streamed: false,
    acquisition: 3,
    run: Effect.fnUntraced(
      function*(clients: Clients) {
        const session = yield* openSession(clients)
        const members = yield* session.members.pipe(Stream.runDrain, Effect.forkChild({ startImmediately: true }))
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(members)
      },
      Effect.scoped,
      exitOutcome
    )
  }
]

const replies = (call: Call, reply: Reply) => new Map<string, Reply>([...call.prelude, [call.rpc, reply]])

describe("a server reply that this client cannot decode", () => {
  for (const call of calls) {
    const undecodable = `ProtocolInvalid: The ${call.rpc} RPC response could not be decoded [cause: SchemaError]`

    if (call.streamed) {
      it.effect(
        `fails ${call.rpc} with ProtocolInvalid when a streamed value does not match its schema`,
        Effect.fnUntraced(function*() {
          const clients = yield* connect(replies(call, unknownChunkReply), layerCredentialStatic)
          assert.strictEqual(yield* call.run(clients), undecodable)
        }, Effect.scoped)
      )
    } else {
      it.effect(
        `fails ${call.rpc} with ProtocolInvalid when the success value does not match its schema`,
        Effect.fnUntraced(function*() {
          const clients = yield* connect(replies(call, unknownSuccessReply), layerCredentialStatic)
          assert.strictEqual(yield* call.run(clients), undecodable)
        }, Effect.scoped)
      )
    }

    it.effect(
      `fails ${call.rpc} with ProtocolInvalid when the error value does not match its schema`,
      Effect.fnUntraced(function*() {
        const clients = yield* connect(replies(call, unknownErrorReply), layerCredentialStatic)
        assert.strictEqual(yield* call.run(clients), undecodable)
      }, Effect.scoped)
    )

    it.effect(
      `leaves ${call.rpc} dying when the credential provider dies with a SchemaError before the request is written`,
      Effect.fnUntraced(function*() {
        const defect = yield* localSchemaError
        const clients = yield* connect(new Map(call.prelude), layerCredentialDyingAt(call.acquisition, defect))
        assert.strictEqual(yield* call.run(clients), "defect: SchemaError")
        assert.notInclude(clients.written, call.rpc)
      }, Effect.scoped)
    )

    it.effect(
      `leaves ${call.rpc} dying when the socket write of its request dies with a SchemaError`,
      Effect.fnUntraced(function*() {
        const defect = yield* localSchemaError
        const clients = yield* connect(new Map(call.prelude), layerCredentialStatic, { tag: call.rpc, defect })
        assert.strictEqual(yield* call.run(clients), "defect: SchemaError")
        assert.notInclude(clients.written, call.rpc)
      }, Effect.scoped)
    )

    it.effect(
      `fails ${call.rpc} with ProtocolInvalid when the frame is malformed`,
      Effect.fnUntraced(function*() {
        const clients = yield* connect(replies(call, malformedFrame), layerCredentialStatic)
        assert.strictEqual(yield* call.run(clients), call.transportFailure)
      }, Effect.scoped)
    )
  }

  it.effect(
    "ends a watch with ProtocolInvalid at the first wake it cannot decode and keeps the wakes before it",
    Effect.fnUntraced(function*() {
      const clients = yield* connect(
        new Map([["Watch", chunksOf([{ spaceId }], [unknownSuccess])]]),
        layerCredentialStatic
      )
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const ended = yield* clients.engine.watch(watchRequest).pipe(
        Stream.runForEach((wake) => Queue.offer(wakes, wake)),
        exitOutcome
      )
      assert.deepStrictEqual(yield* Queue.clear(wakes), [{ spaceId }])
      assert.strictEqual(ended, "ProtocolInvalid: The Watch RPC response could not be decoded [cause: SchemaError]")
    }, Effect.scoped)
  )

  it.effect(
    "ends an open ephemeral session with ProtocolInvalid at the first message it cannot decode",
    Effect.fnUntraced(function*() {
      const clients = yield* connect(
        new Map([["JoinEphemeral", chunksOf([sessionStarted, snapshot], [unknownSuccess])]]),
        layerCredentialStatic
      )
      const session = yield* openSession(clients)
      const ended = yield* session.members.pipe(Stream.runDrain, exitOutcome)
      assert.strictEqual(
        ended,
        "ProtocolInvalid: The JoinEphemeral RPC response could not be decoded [cause: SchemaError]"
      )
    }, Effect.scoped)
  )

  it.effect(
    "keeps answering other calls after a watch received a wake it cannot decode",
    Effect.fnUntraced(function*() {
      const clients = yield* connect(
        new Map([
          ["Watch", unknownChunkReply],
          ["Pull", failWith({ _tag: "SpaceNotJoined", spaceId })]
        ]),
        layerCredentialStatic
      )
      const watched = yield* clients.engine.watch(watchRequest).pipe(Stream.runDrain, exitOutcome)
      assert.strictEqual(watched, "ProtocolInvalid: The Watch RPC response could not be decoded [cause: SchemaError]")
      assert.strictEqual(yield* exitOutcome(clients.engine.pull(pullRequest)), "SpaceNotJoined")
    }, Effect.scoped)
  )

  it.effect(
    "tells the server to stop a watch whose wake it could not decode",
    Effect.fnUntraced(function*() {
      const watches: Array<RequestId> = []
      const clients = yield* connect(
        new Map([
          ["Watch", (requestId) => {
            watches.push(requestId)
            return unknownChunkReply(requestId)
          }],
          ["Pull", failWith({ _tag: "SpaceNotJoined", spaceId })]
        ]),
        layerCredentialStatic
      )
      yield* clients.engine.watch(watchRequest).pipe(Stream.runDrain, Effect.exit)
      yield* clients.engine.pull(pullRequest).pipe(Effect.exit)
      yield* clients.engine.pull(pullRequest).pipe(Effect.exit)
      assert.strictEqual(watches.length, 1)
      assert.deepStrictEqual(yield* Queue.clear(clients.interrupted), watches)
    }, Effect.scoped)
  )

  it.effect(
    "leaves a defect that is not a decode failure as a defect",
    Effect.fnUntraced(function*() {
      const clients = yield* connect(new Map(), layerCredentialDying)
      assert.strictEqual(yield* exitOutcome(clients.engine.pull(pullRequest)), "defect: credential store crashed")
    }, Effect.scoped)
  )
})
