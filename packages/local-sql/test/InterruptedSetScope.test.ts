import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Statement from "effect/sql/Statement"
import * as Stream from "effect/Stream"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000b1")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000b1")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({ definition: Domain.definition, migration }).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerServerDatabase)
)

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

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

const both = Protocol.ReplicationScope.make({ models: [Domain.Todo.name, Domain.Message.name] })
const todosOnly = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

describe("interrupted setScope", () => {
  it.effect(
    "keeps the committed scope in reports and runtime rebuilds after setScope is interrupted",
    Effect.fnUntraced(function*() {
      const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
      const pulls = yield* Queue.unbounded<Protocol.ReplicationScope>()
      const remote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        credentialGeneration: Effect.succeed(0),
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: (request) => server.admitBatch(request, null),
        discard: (request) => server.discard(request, null),
        pull: (request) => Queue.offer(pulls, request.scope).pipe(Effect.andThen(server.pull(request))),
        bootstrap: server.bootstrap,
        watch: () => Stream.never
      })
      const database = yield* Layer.build(layerDatabase)
      const reactivity = Context.get(database, Reactivity.Reactivity)
      const replica = Context.get(
        yield* Layer.build(
          SqlReplica.layer({
            definition: Domain.definition,
            clientId,
            initialSpaces: [spaceId],
            defaultScope: both,
            migration,
            retryDelay: "1 minute",
            maximumRetryDelay: "1 minute"
          }).pipe(
            Layer.provide(Domain.layerHandlers),
            Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
            Layer.provide(Layer.succeedContext(database))
          )
        ),
        Replica.Replica
      )
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* awaitStatus(reactivity, space, (status) => status._tag === "Online")

      const committed = yield* Deferred.make<void>()
      const resume = yield* Deferred.make<void>()
      let scopeWritten = false
      const pauseAfterCommit: Statement.Transformer = (statement) =>
        Effect.suspend(() => {
          const [text] = statement.compile()
          if (/SET\s+desired_scope_json/.test(text)) {
            scopeWritten = true
            return Effect.succeed(statement)
          }
          if (!scopeWritten || /DELETE FROM effect_local_client_scoped_bootstrap/.test(text)) {
            return Effect.succeed(statement)
          }
          scopeWritten = false
          return Deferred.succeed(committed, undefined).pipe(
            Effect.andThen(Deferred.await(resume)),
            Effect.as(statement)
          )
        })
      const change = yield* space.setScope(todosOnly).pipe(
        Effect.provideService(Statement.CurrentTransformer, pauseAfterCommit),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(committed)
      const interruption = yield* Fiber.interrupt(change).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(resume, undefined)
      yield* Fiber.join(interruption)

      yield* Queue.clear(pulls)
      yield* space.mutate(Domain.PutTodo, Domain.todo("after-interrupt"))
      const replicated = yield* Queue.take(pulls)
      assert.deepStrictEqual(yield* space.scope, replicated)

      yield* space.deactivate
      yield* space.activate
      yield* Queue.clear(pulls)
      yield* space.mutate(Domain.PutTodo, Domain.todo("after-rebuild"))
      assert.deepStrictEqual(yield* Queue.take(pulls), todosOnly)
      assert.deepStrictEqual(yield* space.scope, todosOnly)
    })
  )
})
