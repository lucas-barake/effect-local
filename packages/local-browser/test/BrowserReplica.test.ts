import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import { AtomRegistry } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as platform from "../src/internal/platform.js"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import * as testKit from "./multiTabKit.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000401")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000401")
const TodoSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
const Todo = Model.make("Todo", { version: 1, key: Schema.String, schema: TodoSchema })
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema })
const ListTodos = Query.make("ListTodos", { success: Schema.Array(Todo.schema) })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo], queries: [ListTodos] })

const TodoRow = Schema.Struct({ value: Schema.fromJsonString(TodoSchema) })
const listTodos = (query: Transaction.Query) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: TodoRow,
    execute: () => query.sql([Todo], (sql) => sql`SELECT "value" FROM "Todo" ORDER BY "key"`)
  })(undefined).pipe(
    Effect.map((rows) => rows.map((row) => row.value)),
    Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
  )

const layerHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload)),
  ListTodos.toLayer(({ query }) => listTodos(query))
)

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))),
  Layer.provide(layerServerDatabase)
)

const layerEphemeralInactive = Layer.succeed(EphemeralClient.EphemeralClient, {
  session: () => Effect.never,
  publish: () => Effect.void,
  clear: () => Effect.void,
  remove: () => Effect.void
})

const StatusProfile = Ephemeral.member({ status: Schema.String })
const member = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000401"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000401")
})

const layerEphemeralOpening = (
  opening: Effect.Effect<void, ReplicaError.ReplicaError>,
  updates: Ref.Ref<ReadonlyArray<unknown>>
) =>
  Layer.succeed(EphemeralClient.EphemeralClient, {
    session: (_profile, options) =>
      opening.pipe(Effect.as({
        spaceId: options.spaceId,
        member: options.member,
        events: () => Stream.never,
        state: () => Stream.never,
        members: Stream.never,
        updateMember: (value: unknown) => Ref.update(updates, (values) => [...values, value])
      })),
    publish: () => Effect.void,
    clear: () => Effect.void,
    remove: () => Effect.void
  })

const settle = Effect.fnUntraced(function*<A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) {
  const fiber = yield* Effect.forkChild(effect)
  yield* TestClock.adjust("5 seconds")
  return yield* Fiber.join(fiber)
})

const provideFileSystem = Effect.provide(NodeFileSystem.layer)

interface EnvironmentOptions {
  readonly layerEphemeral: Layer.Layer<EphemeralClient.EphemeralClient>
  readonly submitAllowed: () => boolean
}

const makeEnvironmentWith = Effect.fnUntraced(function*(environmentOptions: EnvironmentOptions) {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const kit = yield* testKit.makeMemoryPlatform
  const store = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const databaseOpens = yield* Ref.make(0)
  const layerSync = Layer.merge(
    Layer.succeed(SyncEngine.SyncEngine, {
      waitForCredentialChange: () => Effect.never,
      submit: (request) =>
        Effect.suspend(() => {
          if (environmentOptions.submitAllowed()) return store.submit(request)
          return Effect.never
        }),
      discard: (request) => store.discard(request, null),
      pull: store.pull,
      bootstrap: store.bootstrap,
      watch: store.watch
    }),
    environmentOptions.layerEphemeral
  )
  const layerDatabase = SqliteClient.layer({ filename: `${directory}/replica.sqlite` }).pipe(
    Layer.tap(() => Ref.update(databaseOpens, (count) => count + 1))
  )
  const layerReplica = BrowserReplica.layer({
    name: "tabs",
    definition,
    layerDatabase,
    layerSync,
    spaces: [spaceId],
    profiles: { status: StatusProfile },
    layerPlatform: kit.layerAll,
    requestPersistence: false,
    retryDelay: "100 millis"
  }).pipe(Layer.provide(layerHandlers))
  const layerTab = layerReplica.pipe(Layer.provide(Layer.fresh(Reactivity.layer)))
  const openTab = Effect.gen(function*() {
    const scope = yield* Scope.make()
    const context = yield* Layer.buildWithScope(layerTab, scope)
    return { scope, context, replica: Context.get(context, Replica.Replica) }
  })
  return { openTab, databaseOpens, layerReplica }
})

const makeEnvironment = makeEnvironmentWith({ layerEphemeral: layerEphemeralInactive, submitAllowed: () => true })

const openStatusSession = (context: Context.Context<EphemeralClient.EphemeralClient>) =>
  Context.get(context, EphemeralClient.EphemeralClient).session(StatusProfile, {
    spaceId,
    member,
    value: { status: "online" },
    ttl: "30 seconds"
  })

const listFrom = (replica: Replica.Service) =>
  replica.space(spaceId).pipe(Effect.flatMap((space) => space.query(ListTodos, undefined)))

describe("BrowserReplica", () => {
  it.effect(
    "serves a follower tab's mutations and queries from the leader tab's replica",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "1", title: "from the follower" }))
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [{ id: "1", title: "from the follower" }])
        assert.deepStrictEqual(yield* settle(listFrom(leader.replica)), [{ id: "1", title: "from the follower" }])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "refreshes a follower's live query when the leader tab writes",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const graph = ReplicaAtom.make(environment.layerReplica)
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const todos = graph.query(spaceId, ListTodos)(undefined)
        const unmount = registry.mount(todos)
        yield* Effect.addFinalizer(() => Effect.sync(unmount))
        assert.deepStrictEqual(yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })), [])
        const space = yield* settle(leader.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "2", title: "from the leader" }))
        assert.deepStrictEqual(
          yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })),
          [{ id: "2", title: "from the leader" }]
        )
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps serving the same replica from the follower after the leader tab closes",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const before = yield* settle(space.mutate(PutTodo, { id: "3", title: "before" }, { mutationId }))
        yield* settle(Scope.close(leader.scope, Exit.void))
        const replayed = yield* settle(space.mutate(PutTodo, { id: "3", title: "before" }, { mutationId }))
        assert.deepStrictEqual(replayed.envelope, before.envelope)
        yield* settle(space.mutate(PutTodo, { id: "4", title: "after" }))
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [
          { id: "3", title: "before" },
          { id: "4", title: "after" }
        ])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "streams the leader's settlements to a follower",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const pending = yield* settle(space.mutate(PutTodo, { id: "5", title: "settles" }))
        const settled = yield* settle(space.settlements({ from: 0 }).pipe(Stream.runHead))
        assert.isTrue(settled._tag === "Some")
        if (settled._tag === "Some") {
          assert.strictEqual(settled.value.settlement.pending.envelope.mutationId, pending.envelope.mutationId)
          assert.strictEqual(settled.value.settlement.receipt._tag, "Accepted")
        }
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "delivers a settlement that lands during failover to a live settlement stream opened before it",
    Effect.fnUntraced(
      function*() {
        let submitAllowed = false
        const environment = yield* makeEnvironmentWith({
          layerEphemeral: layerEphemeralInactive,
          submitAllowed: () => submitAllowed
        })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const received = yield* Effect.forkChild(
          space.settlements({ from: "live" }).pipe(Stream.runHead, Effect.timeoutOption("60 seconds"))
        )
        yield* TestClock.adjust("5 seconds")
        const pending = yield* settle(space.mutate(PutTodo, { id: "6", title: "settles on the new leader" }))
        submitAllowed = true
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* TestClock.adjust("60 seconds")
        const settled = Option.flatten(yield* Fiber.join(received))
        assert.isTrue(Option.isSome(settled))
        if (Option.isSome(settled)) {
          assert.strictEqual(settled.value.settlement.pending.envelope.mutationId, pending.envelope.mutationId)
        }
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "updates a follower's ephemeral member after the leader tab closes",
    Effect.fnUntraced(
      function*() {
        const opens = yield* Ref.make(0)
        const reopened = yield* Deferred.make<void>()
        const opening = Ref.getAndUpdate(opens, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count === 0) return Effect.void
            return Deferred.await(reopened)
          })
        )
        const updates = yield* Ref.make<ReadonlyArray<unknown>>([])
        const environment = yield* makeEnvironmentWith({
          layerEphemeral: layerEphemeralOpening(opening, updates),
          submitAllowed: () => true
        })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const session = yield* settle(openStatusSession(follower.context).pipe(Scope.provide(yield* Effect.scope)))
        yield* settle(Scope.close(leader.scope, Exit.void))
        assert.strictEqual(yield* Ref.get(opens), 2)
        const updated = yield* settle(session.updateMember({ status: "away" }).pipe(Effect.exit))
        assert.isTrue(Exit.isSuccess(updated), String(updated))
        yield* settle(Deferred.succeed(reopened, undefined))
        assert.deepStrictEqual(yield* Ref.get(updates), [{ status: "away" }])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "stops opening an ephemeral session whose first open failed",
    Effect.fnUntraced(
      function*() {
        const opens = yield* Ref.make(0)
        const opening = Ref.getAndUpdate(opens, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count === 0) return Effect.fail(new ReplicaError.ServerUnavailable())
            return Effect.void
          })
        )
        const environment = yield* makeEnvironmentWith({
          layerEphemeral: layerEphemeralOpening(opening, yield* Ref.make<ReadonlyArray<unknown>>([])),
          submitAllowed: () => true
        })
        const tab = yield* environment.openTab
        const outcome = yield* settle(
          openStatusSession(tab.context).pipe(Scope.provide(yield* Effect.scope), Effect.exit)
        )
        assert.isTrue(Exit.isFailure(outcome), String(outcome))
        yield* TestClock.adjust("5 seconds")
        assert.strictEqual(yield* Ref.get(opens), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "rejects a corrupt durable client identity with a storage error",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const layerPlatform = Layer.mergeAll(
        Layer.succeed(platform.TabChannel, kit.tabChannel),
        Layer.succeed(platform.WebLocks, kit.webLocks),
        Layer.succeed(platform.ClientIdentityStore, {
          load: () => Effect.succeed("not-a-client-id"),
          store: () => Effect.void
        })
      )
      const outcome = yield* Layer.build(
        BrowserReplica.layer({
          name: "corrupt-identity",
          definition,
          layerDatabase: SqliteClient.layer({ filename: ":memory:" }),
          layerSync: Layer.merge(
            Layer.succeed(SyncEngine.SyncEngine, {
              waitForCredentialChange: () => Effect.never,
              submit: () => Effect.never,
              discard: () => Effect.never,
              pull: () => Effect.never,
              bootstrap: () => Effect.never,
              watch: () => Stream.never
            }),
            layerEphemeralInactive
          ),
          layerPlatform,
          requestPersistence: false
        }).pipe(Layer.provide(layerHandlers), Layer.provide(Reactivity.layer))
      ).pipe(
        Effect.as("built" as const),
        Effect.catchTag("BrowserStorageError", (error) => Effect.succeed(error.operation))
      )
      assert.strictEqual(outcome, "decode")
    }, Effect.scoped)
  )
})
