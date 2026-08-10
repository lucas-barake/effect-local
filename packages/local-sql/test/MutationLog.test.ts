import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )

const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const localLayer = (id = clientId) =>
  LocalStore.layer({ definition: Domain.definition, spaceId, clientId: id }).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )

const serverLayer = (
  authorize?: (input: {
    readonly envelope: Protocol.MutationEnvelope
    readonly principal: Schema.Json
  }) => Effect.Effect<void, Schema.Json>
) =>
  ServerStore.layer(
    authorize === undefined ? { definition: Domain.definition } : {
      definition: Domain.definition,
      authorize
    }
  ).pipe(
    Layer.provide(runtime),
    Layer.provide(database())
  )

const service = <I, S, E, R,>(tag: Context.Service<I, S>, layer: Layer.Layer<I, E, R>) =>
  Layer.build(layer).pipe(Effect.map((context) => Context.get(context, tag)))

const directSync = (server: ServerStore.Service) =>
  Layer.succeed(
    SyncEngine.SyncEngine,
    SyncEngine.SyncEngine.of({ submit: server.submit, pull: server.pull, watch: server.watch })
  )

const clientServices = (id: Identity.ClientId, server: ServerStore.Service) => {
  const local = localLayer(id)
  const reconciler = Reconciler.layer({ spaceId, retryDelay: "10 millis" }).pipe(
    Layer.provide(local),
    Layer.provide(directSync(server))
  )
  return Layer.merge(local, reconciler)
}

describe("server reconciled mutation log", () => {
  it.effect("commits optimistically, admits in total order, and reconciles canonical state", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())

      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
      assert.strictEqual(yield* local.pendingCount, 1)

      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Accepted")
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      assert.deepStrictEqual(page.entries.map((entry) => entry.sequence), [1])
      yield* local.applyEntries(page.entries)

      assert.strictEqual(yield* local.pendingCount, 0)
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
      assert.strictEqual(yield* local.cursor, 1)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
    })))

  it.effect("rejects a first submission whose digest does not match its envelope", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const changed = { ...pending.envelope, payload: Domain.todo("1", "tampered") }
      const exit = yield* server.submit(changed).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      if (exit._tag === "Failure") {
        const failure = Cause.findErrorOption(exit.cause)
        assert.strictEqual(failure._tag, "Some")
        if (failure._tag === "Some") assert.strictEqual(failure.value._tag, "MutationIdentityConflict")
      }
    })))

  it.effect("returns the same terminal receipt for an exact retry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const first = yield* server.submit(pending.envelope)
      const retry = yield* server.submit(pending.envelope)
      assert.deepStrictEqual(retry, first)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      assert.strictEqual(page.entries.length, 1)
    })))

  it.effect("stores a terminal rejection without advancing the accepted cursor", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer(() => Effect.fail({ reason: "denied" })))
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Rejected")
      yield* local.applyReceipt(receipt)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.isTrue(Option.isNone(yield* local.get(Domain.Todo, "1")))
      assert.strictEqual(yield* local.cursor, 0)
    })))

  it.effect("invalidates the receipt dependency when a terminal receipt is stored", () =>
    Effect.scoped(Effect.gen(function*() {
      const clientDatabase = database()
      const local = LocalStore.layer({ definition: Domain.definition, spaceId, clientId }).pipe(
        Layer.provide(runtime),
        Layer.provide(clientDatabase)
      )
      const context = yield* Layer.build(Layer.merge(local, clientDatabase))
      const store = Context.get(context, LocalStore.Store)
      const reactivity = Context.get(context, Reactivity.Reactivity)
      const pending = yield* store.mutate(Domain.PutTodo, Domain.todo("1"))
      let invalidations = 0
      const cancel = reactivity.registerUnsafe(
        [`effect-local:receipt:${pending.envelope.mutationId}`],
        () => invalidations++
      )
      yield* Effect.addFinalizer(() => Effect.sync(cancel))
      yield* store.applyReceipt({
        _tag: "Rejected",
        spaceId,
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        rejection: "denied"
      })
      assert.strictEqual(invalidations, 1)
    })))

  it.effect("uses explicit field semantics without metadata on ordinary model values", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
      yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
      yield* local.mutate(Domain.AddLabel, { id: "1", label: "a" })
      const value = Option.getOrThrow(yield* local.get(Domain.Todo, "1"))
      assert.deepStrictEqual(value, { id: "1", title: "first", count: 2, labels: ["a"] })
      assert.strictEqual(Object.keys(value).some((key) => key.startsWith("$")), false)
    })))

  it.effect("canonicalizes the exact identity covered by the digest", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const local = yield* service(LocalStore.Store, localLayer())
        const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
        const { digest, ...identity } = pending.envelope
        assert.strictEqual(yield* Canonical.digest(identity), digest)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("refuses a mutation envelope beyond the configured protocol bound", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const error = yield* local.mutate(
        Domain.PutTodo,
        Domain.todo(
          "1",
          "x".repeat(Protocol.maximumMutationBytes)
        )
      ).pipe(Effect.flip)
      assert.strictEqual(error._tag, "CapacityExceeded")
      assert.strictEqual(yield* local.pendingCount, 0)
    })))

  it.effect("rejects invalid local and reconciliation configuration during layer construction", () =>
    Effect.scoped(Effect.gen(function*() {
      const localError = yield* service(
        LocalStore.Store,
        LocalStore.layer({
          definition: Domain.definition,
          spaceId,
          clientId,
          maximumPendingMutations: 0
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      ).pipe(Effect.flip)
      assert.strictEqual(localError._tag, "InvalidConfiguration")
      if (localError._tag === "InvalidConfiguration") {
        assert.strictEqual(localError.option, "maximumPendingMutations")
      }

      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const invalidPageSize = Reconciler.layer({ spaceId, pageSize: 0 }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(directSync(server))
      )
      const pageSizeError = yield* service(Reconciler.Reconciler, invalidPageSize).pipe(Effect.flip)
      assert.strictEqual(pageSizeError._tag, "InvalidConfiguration")
      if (pageSizeError._tag === "InvalidConfiguration") assert.strictEqual(pageSizeError.option, "pageSize")

      const invalidRetryDelay = Reconciler.layer({ spaceId, retryDelay: "0 millis" }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(directSync(server))
      )
      const retryDelayError = yield* service(Reconciler.Reconciler, invalidRetryDelay).pipe(Effect.flip)
      assert.strictEqual(retryDelayError._tag, "InvalidConfiguration")
      if (retryDelayError._tag === "InvalidConfiguration") assert.strictEqual(retryDelayError.option, "retryDelay")
    })))

  it.effect("rolls a rejected optimistic mutation back and replays later pending work", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* service(
        ServerStore.ServerStore,
        serverLayer(({ envelope }) =>
          envelope.name === Domain.RenameTodo.name ? Effect.fail({ reason: "denied" }) : Effect.void
        )
      )
      const services = yield* Layer.build(clientServices(clientId, server))
      const local = Context.get(services, LocalStore.Store)
      const reconciler = Context.get(services, Reconciler.Reconciler)

      yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* reconciler.sync
      const rename = yield* local.mutate(Domain.RenameTodo, { id: "1", title: "optimistic" })
      yield* local.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })

      yield* reconciler.sync
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), {
        id: "1",
        title: "first",
        count: 2,
        labels: []
      })
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(rename.envelope.mutationId))._tag, "Rejected")
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(yield* local.cursor, 2)
    })))

  it.effect("rejects a client sequence gap without consuming server order", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const second = yield* local.mutate(Domain.PutTodo, Domain.todo("2"))
      const error = yield* server.submit(second.envelope).pipe(Effect.flip)
      assert.strictEqual(error._tag, "OutOfOrderMutation")
      if (error._tag === "OutOfOrderMutation") assert.strictEqual(error.expected, 1)
      const receipt = yield* server.submit(first.envelope)
      assert.strictEqual(receipt._tag, "Accepted")
      if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
    })))

  it.effect("deduplicates overlapping catch up pages", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* server.submit(first.envelope)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      yield* local.applyEntries([...page.entries, ...page.entries])
      assert.strictEqual(yield* local.cursor, 1)
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
    })))

  it.effect("rejects a conflicting duplicate catch up entry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* server.submit(pending.envelope)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      const entry = page.entries[0]!
      yield* local.applyEntries(page.entries)

      const error = yield* local.applyEntries([{ ...entry, result: { conflicting: true } }]).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.cursor, 1)
    })))

  it.effect("rejects a receipt for another replica without settling pending work", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const error = yield* local.applyReceipt({
        _tag: "Rejected",
        spaceId: Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002"),
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        rejection: { reason: "wrong space" }
      }).pipe(Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
    })))

  it.effect("rejects a conflicting duplicate receipt", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt: Protocol.AcceptedReceipt = {
        _tag: "Accepted",
        spaceId,
        clientId,
        mutationId: pending.envelope.mutationId,
        localSequence: pending.envelope.localSequence,
        serverSequence: Identity.ServerSequence.make(1),
        result: pending.optimisticResult
      }
      yield* local.applyReceipt(receipt)

      const error = yield* local.applyReceipt({ ...receipt, result: { conflicting: true } }).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
    })))

  it.effect("converges concurrent clients through server assigned order", () =>
    Effect.scoped(Effect.gen(function*() {
      const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const firstContext = yield* Layer.build(clientServices(clientId, server))
      const secondContext = yield* Layer.build(clientServices(secondClientId, server))
      const first = Context.get(firstContext, LocalStore.Store)
      const firstReconciler = Context.get(firstContext, Reconciler.Reconciler)
      const second = Context.get(secondContext, LocalStore.Store)
      const secondReconciler = Context.get(secondContext, Reconciler.Reconciler)

      yield* first.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* firstReconciler.sync
      yield* secondReconciler.sync
      yield* first.mutate(Domain.IncrementTodo, { id: "1", delta: 1 })
      yield* second.mutate(Domain.IncrementTodo, { id: "1", delta: 2 })
      yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })
      yield* Effect.all([firstReconciler.sync, secondReconciler.sync], { concurrency: "unbounded" })

      assert.strictEqual(Option.getOrThrow(yield* first.get(Domain.Todo, "1")).count, 3)
      assert.strictEqual(Option.getOrThrow(yield* second.get(Domain.Todo, "1")).count, 3)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      assert.deepStrictEqual(page.entries.map((entry) => entry.sequence), [1, 2, 3])
    })))

  it.effect("caps catch up pages by encoded bytes as well as entry count", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      for (let index = 0; index < 24; index++) {
        const pending = yield* local.mutate(
          Domain.PutTodo,
          Domain.todo(
            String(index),
            `${index}:${"x".repeat(200_000)}`
          )
        )
        yield* server.submit(pending.envelope)
      }
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 1_000 })
      assert.isAtMost(Protocol.encodedBytes(page), Protocol.maximumBatchBytes)
      assert.isTrue(page.hasMore)
      assert.isBelow(page.entries.length, 24)
    })), 20_000)

  it.effect("restores optimistic state and its pending envelope after restart", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/replica.db`
        const persistentDatabase = () =>
          Layer.mergeAll(
            SqliteClient.layer({ filename, disableWAL: true }),
            NodeCrypto.layer,
            Reactivity.layer
          )

        const mutationId = yield* Effect.scoped(Effect.gen(function*() {
          const layer = LocalStore.layer({ definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
          const local = yield* service(LocalStore.Store, layer)
          const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
          return pending.envelope.mutationId
        }))

        yield* Effect.scoped(Effect.gen(function*() {
          const layer = LocalStore.layer({ definition: Domain.definition, spaceId, clientId }).pipe(
            Layer.provide(runtime),
            Layer.provide(persistentDatabase())
          )
          const local = yield* service(LocalStore.Store, layer)
          assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))
          const pending = yield* local.pending
          assert.deepStrictEqual(pending.map((item) => item.envelope.mutationId), [mutationId])
        }))
      }).pipe(Effect.provide(NodeFileSystem.layer))
    ))
})
