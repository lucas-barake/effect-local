import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const remote = Layer.succeed(
  SyncEngine.SyncEngine,
  SyncEngine.SyncEngine.of({
    submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    pull: () => Effect.succeed({ entries: [], hasMore: false }),
    bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
    watch: () => Stream.never
  })
)

const live = SqlReplica.layer({
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceB, spaceA],
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(Domain.handlers),
  Layer.provide(remote),
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.provide(Reactivity.layer)
)

describe("multi space Replica", () => {
  it.effect("isolates overlapping entity keys for two spaces in one database", () =>
    Effect.scoped(Effect.gen(function*() {
      const context = yield* Layer.build(live)
      const replica = Context.get(context, Replica.Replica)
      const spaces = yield* replica.spaces
      assert.deepStrictEqual(spaces.map((space) => space.spaceId), [spaceA, spaceB])
      const a = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)

      yield* a.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      assert.strictEqual(Option.getOrThrow(yield* a.get(Domain.Todo, "same")).title, "A")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")
      const aggregate = yield* replica.status
      assert.deepStrictEqual(aggregate.spaces.map((status) => status.spaceId), [spaceA, spaceB])
      assert.strictEqual(aggregate.totalPending, 2)
    })))

  it.effect("evicts one space and leaves retained handles stale across rejoin", () =>
    Effect.scoped(Effect.gen(function*() {
      const context = yield* Layer.build(live)
      const replica = Context.get(context, Replica.Replica)
      const stale = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)
      yield* stale.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      yield* replica.leave(spaceA)
      assert.strictEqual((yield* replica.space(spaceA).pipe(Effect.flip))._tag, "SpaceNotJoined")
      assert.strictEqual((yield* stale.get(Domain.Todo, "same").pipe(Effect.flip))._tag, "SpaceUnavailable")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")

      const rejoined = yield* replica.join(spaceA)
      assert.isTrue(Option.isNone(yield* rejoined.get(Domain.Todo, "same")))
      assert.strictEqual(
        (yield* stale.mutate(Domain.PutTodo, Domain.todo("stale")).pipe(Effect.flip))._tag,
        "SpaceUnavailable"
      )
    })))

  it.effect("restores durable memberships when the Replica runtime restarts", () =>
    Effect.scoped(Effect.gen(function*() {
      const databaseContext = yield* Layer.build(Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      ))
      const services = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, Context.get(databaseContext, SqlClient.SqlClient)),
        Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto)),
        Layer.succeed(Reactivity.Reactivity, Context.get(databaseContext, Reactivity.Reactivity))
      )
      const replicaLayer = (initialSpaces?: Iterable<Identity.SpaceId>) => {
        const options = {
          definition: Domain.definition,
          clientId,
          retainedReceipts: 256,
          maximumReceipts: 10_000,
          retainedHistoryEntries: 256,
          maximumBootstrapEntities: 10_000,
          maximumBootstrapBytes: 64 * 1024 * 1024,
          maximumBootstrapPageBytes: 4 * 1024 * 1024,
          migration: { retryDelay: "1 millis", maximumAttempts: 8 }
        } satisfies SqlReplica.Options<typeof Domain.definition>
        let replica = SqlReplica.layer(options)
        if (initialSpaces !== undefined) replica = SqlReplica.layer({ ...options, initialSpaces })
        return replica.pipe(
          Layer.provide(Domain.handlers),
          Layer.provide(remote),
          Layer.provide(services)
        )
      }

      const firstScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(replicaLayer([spaceA, spaceB]), firstScope)
      const first = Context.get(firstContext, Replica.Replica)
      yield* (yield* first.space(spaceA)).mutate(Domain.PutTodo, Domain.todo("restart", "retained"))
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const secondContext = yield* Layer.buildWithScope(replicaLayer(), secondScope)
      const second = Context.get(secondContext, Replica.Replica)
      assert.deepStrictEqual((yield* second.spaces).map((space) => space.spaceId), [spaceA, spaceB])
      assert.strictEqual(
        Option.getOrThrow(yield* (yield* second.space(spaceA)).get(Domain.Todo, "restart")).title,
        "retained"
      )
      yield* Scope.close(secondScope, Exit.void)
    })))
})
