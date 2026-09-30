import { NodeCrypto, NodeFileSystem, NodeHttpServer } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as SyncServer from "@lucas-barake/effect-local-rpc/SyncServer"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SingleRunner from "effect/cluster/SingleRunner"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FileSystem from "effect/FileSystem"
import * as HttpRouter from "effect/http/HttpRouter"
import * as HttpServer from "effect/http/HttpServer"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as AtomRegistry from "effect/reactivity/AtomRegistry"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as ExpoReplica from "../src/ExpoReplica.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000901")
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
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "tester" })
      return Effect.fail(new ReplicaError.CredentialRejected())
    }
  })
)

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)
const layerCredential = Authentication.layerCredentialProviderStatic(Redacted.make("secret"))

const layerServer = HttpRouter.serve(
  SyncServer.layer({
    definition,
    store: { migration },
    authorizeAccess: () => Effect.void,
    authorizeRead: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeEphemeral: () => Effect.void
  }).pipe(Layer.provideMerge(SyncServer.layerProtocolWebSocket({ path: "/sync" }))),
  { disableListenLog: true, disableLogger: true }
).pipe(
  Layer.provide(Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))),
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
  Layer.provide(layerHandlers),
  Layer.provide(layerServerDatabase),
  Layer.provideMerge([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

const syncUrl = HttpServer.HttpServer.use((server) => {
  const address = server.address
  if (address._tag === "UnixPathAddress") return Effect.die("Expected the test HTTP server to use a TCP address")
  return Effect.succeed(`ws://127.0.0.1:${address.port}/sync`)
})

const layerReplica = (serverContext: Context.Context<HttpServer.HttpServer>, directory: string, filename: string) =>
  ExpoReplica.layer({
    definition,
    database: { filename, directory },
    initialSpaces: [spaceId],
    defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
    migration,
    layerSync: SyncClient.layerWebSocket({ url: syncUrl.pipe(Effect.provide(serverContext)) }).pipe(
      Layer.provide(layerCredential)
    )
  }).pipe(Layer.provide(layerHandlers), Layer.provideMerge(Reactivity.layer))

const openReplica = Effect.fnUntraced(function*(
  serverContext: Context.Context<HttpServer.HttpServer>,
  directory: string,
  filename: string
) {
  const context = yield* Layer.build(layerReplica(serverContext, directory, filename))
  const space = yield* Context.get(context, Replica.Replica).space(spaceId)
  return { context, space, reactivity: Context.get(context, Reactivity.Reactivity) }
})

const awaitTodo = (
  replica: Effect.Success<ReturnType<typeof openReplica>>,
  id: string
) =>
  replica.reactivity.stream([ReactivityKey.entity(spaceId, Todo.name, id)], replica.space.get(Todo, id)).pipe(
    Stream.filter(Option.isSome),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: (todo) => Effect.succeed(todo.value) }))
  )

const provideFileSystem = Effect.provide(NodeFileSystem.layer)

describe("ExpoReplica", () => {
  it.live(
    "replicates a mutation between two Expo replicas through the sync server",
    Effect.fnUntraced(
      function*() {
        const directory = yield* FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())
        const serverContext = yield* Layer.build(layerServer)
        const alice = yield* openReplica(serverContext, directory, "alice.db")
        const bob = yield* openReplica(serverContext, directory, "bob.db")
        yield* alice.space.mutate(PutTodo, { id: "todo-1", title: "from alice" })
        assert.deepStrictEqual(yield* awaitTodo(bob, "todo-1"), { id: "todo-1", title: "from alice" })
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.live(
    "keeps local mutations when the same database file is opened again",
    Effect.fnUntraced(
      function*() {
        const directory = yield* FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())
        const serverContext = yield* Layer.build(layerServer)
        const firstScope = yield* Scope.make()
        const first = yield* openReplica(serverContext, directory, "device.db").pipe(Scope.provide(firstScope))
        yield* first.space.mutate(PutTodo, { id: "todo-2", title: "persisted" })
        yield* Scope.close(firstScope, Exit.void)
        const reopened = yield* openReplica(serverContext, directory, "device.db")
        assert.deepStrictEqual(yield* awaitTodo(reopened, "todo-2"), { id: "todo-2", title: "persisted" })
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.live(
    "gives a ReplicaAtom graph one ephemeral member minted from expo-crypto",
    Effect.fnUntraced(
      function*() {
        const directory = yield* FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())
        const serverContext = yield* Layer.build(layerServer)
        const graph = ReplicaAtom.make(layerReplica(serverContext, directory, "atoms.db"))
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const unmount = registry.mount(graph.member)
        yield* Effect.addFinalizer(() => Effect.sync(unmount))
        const member = yield* AtomRegistry.getResult(registry, graph.member)
        assert.isTrue(Schema.is(Protocol.EphemeralMember)(member))
        assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, graph.member), member)
      },
      Effect.scoped,
      provideFileSystem
    )
  )
})
