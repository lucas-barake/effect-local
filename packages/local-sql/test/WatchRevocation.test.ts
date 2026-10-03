import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000d1")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000d1")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const principal = "reader"

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = (readDenied: MutableRef.MutableRef<boolean>) =>
  ServerStore.layer({
    definition: Domain.definition,
    readAuthorizationRefreshInterval: "30 seconds",
    migration,
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () =>
      Effect.suspend(() => {
        if (MutableRef.get(readDenied)) return Effect.fail({ _tag: "Forbidden" })
        return Effect.void
      })
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerServerDatabase)
  )

const makeRemote = Effect.gen(function*() {
  const server = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => server.admitBatch(request, principal),
    discard: (request) => server.discard(request, principal),
    pull: (request) => server.pullAuthorized(request, principal),
    bootstrap: (request) => server.bootstrapAuthorized(request, principal),
    watch: (request) =>
      server.watchAuthorized(request, principal).pipe(
        Stream.unwrap,
        Stream.filter(() => false)
      )
  })
})

const layerRemote = (readDenied: MutableRef.MutableRef<boolean>) =>
  Layer.effect(SyncEngine.SyncEngine, makeRemote).pipe(Layer.provide(layerServer(readDenied)))

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const replicaOptions = {
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceId],
  defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  migration,
  retryDelay: "1 minute",
  maximumRetryDelay: "1 minute"
} as const

const replicas = [
  {
    name: "SqlReplica.layer",
    layer: (readDenied: MutableRef.MutableRef<boolean>) =>
      SqlReplica.layer(replicaOptions).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote(readDenied))
      )
  },
  {
    name: "SqlReplica.layerWorkflow",
    layer: (readDenied: MutableRef.MutableRef<boolean>) =>
      SqlReplica.layerWorkflow(replicaOptions).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote(readDenied)),
        Layer.provide(WorkflowEngine.layerMemory)
      )
  }
] as const

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

describe("watch read revocation", () => {
  for (const replica of replicas) {
    it.effect(
      `removes replicated rows when the watch loses read authorization (${replica.name})`,
      Effect.fnUntraced(function*() {
        const readDenied = MutableRef.make(false)
        const database = yield* Layer.build(layerDatabase)
        const reactivity = Context.get(database, Reactivity.Reactivity)
        const context = yield* Layer.build(
          replica.layer(readDenied).pipe(Layer.provide(Layer.succeedContext(database)))
        )
        const space = yield* Context.get(context, Replica.Replica).space(spaceId)
        yield* space.activate
        yield* awaitStatus(reactivity, space, (status) => status._tag === "Online")
        yield* space.mutate(Domain.PutTodo, Domain.todo("replicated"))
        yield* awaitStatus(reactivity, space, (status) => status._tag === "Online" && status.pending === 0)
        assert.isTrue(Option.isSome(yield* space.get(Domain.Todo, "replicated")))

        MutableRef.set(readDenied, true)
        yield* TestClock.adjust("30 seconds")
        const failed = yield* awaitStatus(reactivity, space, (status) => status._tag === "Failed")

        assert.strictEqual(failed._tag === "Failed" && failed.message, "AuthorizationDenied")
        assert.isTrue(Option.isNone(yield* space.get(Domain.Todo, "replicated")))
      })
    )
  }
})
