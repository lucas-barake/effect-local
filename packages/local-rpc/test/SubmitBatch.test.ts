import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as LocalStore from "@lucas-barake/effect-local-sql/LocalStore"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Reconciler from "@lucas-barake/effect-local-sql/Reconciler"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Socket from "effect/unstable/socket/Socket"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Authentication from "../src/Authentication.js"
import * as ProtocolSession from "../src/ProtocolSession.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncRpc from "../src/SyncRpc.js"
import * as SyncServer from "../src/SyncServer.js"

class ForbiddenTitle extends Schema.TaggedError<ForbiddenTitle, Schema.JsonObject>(
  "@lucas-barake/effect-local-rpc/test/SubmitBatch/ForbiddenTitle"
)("ForbiddenTitle", { reason: Schema.String }) {}

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000701")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000701")

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const layerHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const secretBearer = Redacted.make("secret")

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "test" })
      return Effect.fail(new ReplicaError.CredentialRejected())
    }
  })
)
const layerAuthenticationServer = Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))
const layerAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(
  Layer.provide(Authentication.layerCredentialProviderStatic(secretBearer))
)
const layerWebsocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
  Layer.provide(HttpRouter.layer)
)
const serverUrl = Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer
  const address = server.address
  if (address._tag === "UnixPathAddress") return yield* Effect.die("Expected the test HTTP server to use a TCP address")
  return `http://127.0.0.1:${address.port}/sync`
})
const layerClientProtocol = SyncClient.layerProtocolSocket().pipe(
  Layer.provide(
    Effect.flatMap(serverUrl, (url) => Socket.makeWebSocket(url)).pipe(
      Layer.effect(Socket.Socket),
      Layer.provide(NodeSocket.layerWebSocketConstructor)
    )
  )
)

interface Submission {
  readonly tag: "Submit" | "SubmitBatch"
  readonly version: Protocol.ProtocolVersion
  readonly size: number
}

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

const LogRow = Schema.Struct({ server_sequence: Schema.Number, mutation_id: Schema.String })
const ReceiptRow = Schema.Struct({ mutation_id: Schema.String })
const CountRow = Schema.Struct({ count: Schema.Number })

const makeHarness = Effect.fnUntraced(function*(options: {
  readonly serverVersions?: ReadonlyArray<number>
  readonly clientVersions?: ReadonlyArray<number>
  readonly staleBatchVersion?: boolean
} = {}) {
  const submissions: Array<Submission> = []
  const gateArmed = MutableRef.make(false)
  const gateEntered = yield* Deferred.make<void>()
  const gateRelease = yield* Deferred.make<void>()

  const layerObservingAuthentication = Layer.effect(
    Authentication.Authentication,
    Effect.gen(function*() {
      const authenticate = yield* Authentication.Authentication
      return Authentication.Authentication.of((effect, rpcOptions) =>
        Effect.sync(() => {
          const payload = rpcOptions.payload
          if (rpcOptions.rpc._tag === "SubmitBatch" && Schema.is(Protocol.VersionedSubmitBatchRequest)(payload)) {
            submissions.push({ tag: "SubmitBatch", version: payload.protocolVersion, size: payload.envelopes.length })
          } else if (rpcOptions.rpc._tag === "Submit" && Schema.is(Protocol.VersionedSubmitRequest)(payload)) {
            submissions.push({ tag: "Submit", version: payload.protocolVersion, size: 1 })
          }
        }).pipe(Effect.andThen(authenticate(effect, rpcOptions)))
      )
    })
  ).pipe(Layer.provide(layerAuthenticationServer))

  let serverOptions: SyncServer.LayerOptions<typeof definition> = {
    definition,
    store: { migration },
    authorizeEphemeral: () => Effect.void,
    authorizeAccess: () => Effect.void,
    authorizeRead: () => Effect.void,
    authorizeMutation: ({ mutation }) => {
      if (!Schema.is(Todo.schema)(mutation.payload)) return Effect.void
      if (mutation.payload.title === "forbidden") return Effect.fail(new ForbiddenTitle({ reason: "forbidden" }))
      if (mutation.payload.title === "gate" && MutableRef.get(gateArmed)) {
        return Deferred.succeed(gateEntered, undefined).pipe(Effect.andThen(Deferred.await(gateRelease)))
      }
      return Effect.void
    }
  }
  if (options.serverVersions !== undefined) {
    serverOptions = { ...serverOptions, supportedProtocolVersions: options.serverVersions }
  }
  const layerServerDatabase = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )
  const layerServer = SyncServer.layer(serverOptions).pipe(
    Layer.provideMerge(layerWebsocketProtocol),
    Layer.provide(layerObservingAuthentication),
    Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
    Layer.provide(layerHandlers),
    Layer.provideMerge(layerServerDatabase),
    Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true }))
  )
  let sessionOptions: ProtocolSession.Options = {}
  if (options.clientVersions !== undefined) sessionOptions = { supportedProtocolVersions: options.clientVersions }
  const layerNegotiatedSession = ProtocolSession.layerWithOptions(sessionOptions)
  let layerSession = layerNegotiatedSession
  if (options.staleBatchVersion === true) {
    layerSession = Layer.effect(
      ProtocolSession.ProtocolSession,
      Effect.gen(function*() {
        const negotiated = yield* ProtocolSession.ProtocolSession
        return ProtocolSession.ProtocolSession.of({
          client: negotiated.client,
          version: Effect.succeed(Protocol.submitBatchProtocolVersion),
          rejected: () => negotiated.version
        })
      })
    ).pipe(Layer.provide(layerNegotiatedSession))
  }
  const layerRemote = SyncClient.layerFromSession().pipe(
    Layer.provideMerge(layerSession),
    Layer.provide(layerClientProtocol),
    Layer.provide(layerAuthenticationClient)
  )
  const live = yield* Layer.build(
    layerRemote.pipe(
      Layer.provideMerge(layerServer),
      Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
    )
  )

  const layerLocalDatabase = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
  const layerLocal = LocalStore.layer({
    definition,
    spaceId,
    clientId,
    scope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provideMerge(layerLocalDatabase)
  )
  const engine = Context.get(live, SyncEngine.SyncEngine)
  const client = yield* Layer.build(
    Reconciler.layerOnePass({ definition, spaceId }).pipe(
      Layer.provideMerge(layerLocal),
      Layer.provide(Layer.succeed(SyncEngine.SyncEngine, engine))
    )
  )

  const serverSql = Context.get(live, SqlClient.SqlClient)
  const clientSql = Context.get(client, SqlClient.SqlClient)
  const local = Context.get(client, LocalStore.Store)
  return {
    submissions,
    gateArmed,
    gateEntered,
    gateRelease,
    local,
    reconciliation: Context.get(client, Reconciler.Reconciliation),
    session: Context.get(live, ProtocolSession.ProtocolSession),
    put: (id: string, title = id) => local.mutate(PutTodo, { id, title }),
    serverLog: SqlSchema.findAll({
      Request: Schema.Void,
      Result: LogRow,
      execute: () =>
        serverSql`SELECT server_sequence, mutation_id FROM effect_local_authoritative_log
          WHERE space_id = ${spaceId} ORDER BY server_sequence`
    })(undefined),
    serverReceiptCount: SqlSchema.findOne({
      Request: Schema.Void,
      Result: CountRow,
      execute: () => serverSql`SELECT COUNT(*) AS count FROM effect_local_server_receipts WHERE space_id = ${spaceId}`
    })(undefined).pipe(Effect.map((row) => row.count)),
    clientReceipts: SqlSchema.findAll({
      Request: Schema.Void,
      Result: ReceiptRow,
      execute: () =>
        clientSql`SELECT mutation_id FROM effect_local_client_receipts_data
          WHERE space_id = ${spaceId} ORDER BY local_sequence`
    })(undefined).pipe(Effect.map((rows) => rows.map((row) => row.mutation_id))),
    receiptTags: Effect.forEach((mutation: Protocol.PendingMutation) =>
      local.receipt(mutation.envelope.mutationId).pipe(
        Effect.map(Option.match({ onNone: () => "Missing", onSome: (receipt) => receipt._tag }))
      )
    )
  }
})

const range = (count: number) => Array.from({ length: count }, (_, index) => index + 1)
const mutationIds = (pending: ReadonlyArray<Protocol.PendingMutation>) =>
  pending.map((mutation) => mutation.envelope.mutationId)

const PreBatchRpcs = SyncRpc.Rpcs.omit("SubmitBatch")

const makePreBatchServer = Effect.fnUntraced(function*(advertisedVersion: number) {
  const submits = MutableRef.make(0)
  const layerStoreDatabase = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )
  const layerStore = ServerStore.layerTrusted({ definition, migration }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerStoreDatabase)
  )
  const layerPreBatchHandlers = PreBatchRpcs.toLayer(Effect.gen(function*() {
    const store = yield* ServerStore.ServerStore
    const negotiated = Protocol.ProtocolVersion.make(advertisedVersion)
    return PreBatchRpcs.of({
      Negotiate: () => Effect.succeed({ version: negotiated }),
      Submit: ({ protocolVersion, ...request }) => {
        if (protocolVersion !== negotiated) {
          return Effect.fail(
            new ReplicaError.ProtocolVersionRejected({ version: protocolVersion, serverVersions: [negotiated] })
          )
        }
        MutableRef.update(submits, (count) => count + 1)
        return store.submit(request)
      },
      Discard: () => Effect.die("unused"),
      Pull: () => Effect.die("unused"),
      Bootstrap: () => Effect.die("unused"),
      Watch: () => Stream.die("unused"),
      JoinEphemeral: () => Stream.die("unused"),
      PublishEphemeral: () => Effect.die("unused"),
      HeartbeatEphemeral: () => Effect.die("unused")
    })
  })).pipe(Layer.provide(layerStore))
  const layerServer = RpcServer.layer(PreBatchRpcs, { disableFatalDefects: true }).pipe(
    Layer.provide(layerPreBatchHandlers),
    Layer.provideMerge(layerWebsocketProtocol),
    Layer.provide(layerAuthenticationServer),
    Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true }))
  )
  const layerCachedVersion2Session = Layer.effect(
    ProtocolSession.ProtocolSession,
    Effect.gen(function*() {
      const negotiated = yield* ProtocolSession.ProtocolSession
      const cached = MutableRef.make(Protocol.submitBatchProtocolVersion)
      return ProtocolSession.ProtocolSession.of({
        client: negotiated.client,
        version: Effect.sync(() => MutableRef.get(cached)),
        rejected: () =>
          negotiated.version.pipe(Effect.tap((version) => Effect.sync(() => MutableRef.set(cached, version))))
      })
    })
  ).pipe(Layer.provide(ProtocolSession.layer))
  const live = yield* Layer.build(
    SyncClient.layerFromSession().pipe(
      Layer.provide(layerCachedVersion2Session),
      Layer.provide(layerClientProtocol),
      Layer.provide(layerAuthenticationClient),
      Layer.provideMerge(layerServer),
      Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
    )
  )
  return { engine: Context.get(live, SyncEngine.SyncEngine), submits }
})

const envelopeAt = Effect.fnUntraced(function*(localSequence: number) {
  const identity = {
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: PutTodo.name,
    payload: { id: `todo-${localSequence}`, title: `todo-${localSequence}` },
    digestVersion: 3 as const,
    membershipIncarnation: Identity.legacyMembershipIncarnation,
    sourceSchema: definition.schemaIdentity,
    mutationVersion: PutTodo.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

describe("batched submission against a server built before SubmitBatch", () => {
  it.effect(
    "renegotiates and submits one mutation at a time when a cached version 2 reaches a server without SubmitBatch",
    Effect.fnUntraced(function*() {
      const { engine, submits } = yield* makePreBatchServer(1)
      const envelopes = [yield* envelopeAt(1), yield* envelopeAt(2)]

      const result = yield* engine.submitBatch({ envelopes, schema: definition.schemaIdentity })

      assert.deepStrictEqual(
        result.receipts.map((receipt) => [receipt._tag, receipt.mutationId]),
        envelopes.map((envelope) => ["Accepted", envelope.mutationId])
      )
      assert.strictEqual(MutableRef.get(submits), 2)
    }, (effect) => effect.pipe(Effect.provide(NodeCrypto.layer), Effect.scoped))
  )

  it.effect(
    "fails with ProtocolInvalid instead of a defect when the server answers with a defect at an unchanged version",
    Effect.fnUntraced(function*() {
      const { engine, submits } = yield* makePreBatchServer(2)
      const envelopes = [yield* envelopeAt(1)]

      const failure = yield* failureOf(engine.submitBatch({ envelopes, schema: definition.schemaIdentity }))

      assert.strictEqual(failure._tag, "ProtocolInvalid")
      assert.strictEqual(MutableRef.get(submits), 0)
    }, (effect) => effect.pipe(Effect.provide(NodeCrypto.layer), Effect.scoped))
  )
})

describe("batched submission over the WebSocket protocol", () => {
  it.effect(
    "submits N pending mutations in ceil(N / maximumSubmitBatchEntries) SubmitBatch round trips",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness()
      yield* harness.reconciliation.sync
      const pending = yield* Effect.forEach(range(130), (index) => harness.put(`todo-${index}`))
      harness.submissions.length = 0

      yield* harness.reconciliation.sync

      const batch = Protocol.maximumSubmitBatchEntries
      assert.deepStrictEqual(harness.submissions, [
        { tag: "SubmitBatch", version: Protocol.submitBatchProtocolVersion, size: batch },
        { tag: "SubmitBatch", version: Protocol.submitBatchProtocolVersion, size: batch },
        { tag: "SubmitBatch", version: Protocol.submitBatchProtocolVersion, size: 130 - 2 * batch }
      ])
      assert.strictEqual(harness.submissions.length, Math.ceil(130 / batch))
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
      assert.deepStrictEqual(
        yield* harness.serverLog,
        pending.map((mutation, index) => ({ server_sequence: index + 1, mutation_id: mutation.envelope.mutationId }))
      )
      assert.strictEqual(yield* harness.local.pendingCount, 0)
    })
  )

  it.effect(
    "records one receipt per mutation in order when a mutation in the middle of a batch is rejected",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness()
      yield* harness.reconciliation.sync
      const titles = ["todo-1", "todo-2", "forbidden", "todo-4", "todo-5"]
      const pending = yield* Effect.forEach(range(5), (index) => harness.put(`todo-${index}`, titles[index - 1]))
      harness.submissions.length = 0

      yield* harness.reconciliation.sync

      assert.deepStrictEqual(harness.submissions, [
        { tag: "SubmitBatch", version: Protocol.submitBatchProtocolVersion, size: 5 }
      ])
      assert.deepStrictEqual(yield* harness.receiptTags(pending), [
        "Accepted",
        "Accepted",
        "Rejected",
        "Accepted",
        "Accepted"
      ])
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
      assert.strictEqual(yield* harness.serverReceiptCount, 5)
      assert.deepStrictEqual(
        (yield* harness.serverLog).map((row) => row.mutation_id),
        mutationIds([pending[0], pending[1], pending[3], pending[4]])
      )
      assert.strictEqual(yield* harness.local.pendingCount, 0)
    })
  )

  it.effect(
    "resubmits idempotently after a batch is interrupted part way through admission",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness()
      yield* harness.reconciliation.sync
      const titles = ["todo-1", "todo-2", "todo-3", "gate", "todo-5", "todo-6"]
      const pending = yield* Effect.forEach(range(6), (index) => harness.put(`todo-${index}`, titles[index - 1]))
      harness.submissions.length = 0
      MutableRef.set(harness.gateArmed, true)

      const interrupted = yield* harness.reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(harness.gateEntered)
      yield* Fiber.interrupt(interrupted)
      MutableRef.set(harness.gateArmed, false)
      yield* Deferred.succeed(harness.gateRelease, undefined)

      yield* harness.reconciliation.sync

      assert.isAtLeast(harness.submissions.length, 2)
      assert.isTrue(harness.submissions.every((submission) => submission.tag === "SubmitBatch"))
      assert.strictEqual(harness.submissions[0]?.size, 6)
      assert.deepStrictEqual(
        yield* harness.serverLog,
        pending.map((mutation, index) => ({ server_sequence: index + 1, mutation_id: mutation.envelope.mutationId }))
      )
      assert.strictEqual(yield* harness.serverReceiptCount, 6)
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
      assert.deepStrictEqual(yield* harness.receiptTags(pending), pending.map(() => "Accepted"))
      assert.strictEqual(yield* harness.local.pendingCount, 0)
    })
  )

  it.effect(
    "falls back to one Submit per mutation when the server only supports protocol version 1",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness({ serverVersions: [1] })
      yield* harness.reconciliation.sync
      const pending = yield* Effect.forEach(range(3), (index) => harness.put(`todo-${index}`))
      harness.submissions.length = 0

      yield* harness.reconciliation.sync

      assert.deepStrictEqual(
        harness.submissions,
        pending.map(() => ({ tag: "Submit", version: Protocol.ProtocolVersion.make(1), size: 1 }))
      )
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
      assert.strictEqual(yield* harness.local.pendingCount, 0)
    })
  )

  it.effect(
    "serves a client that only supports protocol version 1 with one Submit per mutation",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness({ clientVersions: [1] })
      yield* harness.reconciliation.sync
      const pending = yield* Effect.forEach(range(3), (index) => harness.put(`todo-${index}`))
      harness.submissions.length = 0

      yield* harness.reconciliation.sync

      assert.deepStrictEqual(
        harness.submissions,
        pending.map(() => ({ tag: "Submit", version: Protocol.ProtocolVersion.make(1), size: 1 }))
      )
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
    })
  )

  it.effect(
    "renegotiates and falls back to Submit when the server rejects a cached batch protocol version",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness({ serverVersions: [1], staleBatchVersion: true })
      const pending = yield* Effect.forEach(range(3), (index) => harness.put(`todo-${index}`))

      yield* harness.reconciliation.sync

      assert.deepStrictEqual(harness.submissions, [
        { tag: "SubmitBatch", version: Protocol.submitBatchProtocolVersion, size: 3 },
        ...pending.map(() => ({ tag: "Submit" as const, version: Protocol.ProtocolVersion.make(1), size: 1 }))
      ])
      assert.deepStrictEqual(yield* harness.clientReceipts, mutationIds(pending))
      assert.strictEqual(yield* harness.local.pendingCount, 0)
    })
  )

  it.effect(
    "rejects a SubmitBatch that carries a protocol version without batch support",
    Effect.fnUntraced(function*() {
      const harness = yield* makeHarness()
      const pending = yield* harness.put("todo-1")
      const failure = yield* failureOf(harness.session.client.SubmitBatch({
        envelopes: [pending.envelope],
        schema: definition.schemaIdentity,
        protocolVersion: Protocol.ProtocolVersion.make(1)
      }))
      assert.strictEqual(failure._tag, "ProtocolInvalid")
      assert.deepStrictEqual(yield* harness.serverLog, [])
    })
  )
})
