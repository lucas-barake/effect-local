import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const envelope = (
  name: string,
  payload: Schema.Json,
  localSequence: number,
  mutationId: Identity.MutationId
) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId,
      mutationId,
      localSequence: Identity.LocalSequence.make(localSequence),
      basis: Identity.ServerSequence.make(0),
      name,
      payload
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Canonical.digest(identity) })
  })

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
  authorizeMutation?: (input: {
    readonly envelope: Protocol.MutationEnvelope
    readonly principal: Schema.Json
  }) => Effect.Effect<void, Schema.Json>
) =>
  (authorizeMutation === undefined
    ? ServerStore.layerTrusted({ definition: Domain.definition })
    : ServerStore.layer({
      definition: Domain.definition,
      authorizeAccess: () => Effect.void,
      authorizeMutation,
      authorizeRead: () => Effect.void
    })).pipe(
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
      yield* local.applyReceipt(receipt)
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

  it.effect("reauthorizes an exact retry before returning its durable receipt", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const access = yield* Ref.make(true)
        const secured = ServerStore.layer({
          definition: Domain.definition,
          authorizeAccess: () =>
            Ref.get(access).pipe(
              Effect.flatMap((allowed) => allowed ? Effect.void : Effect.fail({ reason: "revoked" }))
            ),
          authorizeMutation: () => Effect.void,
          authorizeRead: () => Effect.void
        }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
        const server = yield* service(ServerStore.ServerStore, secured)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("1"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000041")
        )
        assert.strictEqual((yield* server.admit(submitted, { subject: "test" }))._tag, "Accepted")
        yield* Ref.set(access, false)

        const error = yield* server.admit(submitted, { subject: "test" }).pipe(Effect.flip)
        assert.strictEqual(error._tag, "AuthorizationDenied")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("keeps mutation payloads and private results out of the authoritative log", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("private", "secret"))
      const receipt = yield* server.submit(pending.envelope)
      assert.strictEqual(receipt._tag, "Accepted")

      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      const entry = page.entries[0] as object
      assert.isFalse(Object.hasOwn(entry, "envelope"))
      assert.isFalse(Object.hasOwn(entry, "result"))
    })))

  it.effect("pulls the public log without materializing private receipt payloads", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("public-with-private-receipt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000029")
        )
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts
      SET receipt_json = ${"x".repeat(Protocol.maximumReceiptBytes)}
      WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
        assert.deepStrictEqual(page.entries.map((entry) => entry.mutationId), [submitted.mutationId])
        assert.strictEqual((yield* server.submit(submitted).pipe(Effect.flip))._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("recovers an accepted commit whose private receipt was lost", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      const receipt = yield* server.submit(pending.envelope)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })

      yield* local.applyEntries(page.entries)
      assert.strictEqual(yield* local.pendingCount, 1)
      assert.isTrue(Option.isNone(yield* local.receipt(pending.envelope.mutationId)))
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Domain.Todo, "1")), Domain.todo("1"))

      yield* local.applyReceipt(receipt)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(pending.envelope.mutationId))._tag, "Accepted")
    })))

  it.effect("rejects authoritative log rows whose redundant identity was corrupted", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const serverLayerWithDatabase = ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(serverLayerWithDatabase, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("1"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000031")
        )
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_authoritative_log
        SET mutation_id = ${Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000032")}
        WHERE space_id = ${spaceId} AND server_sequence = 1`

        const error = yield* server.pull({
          spaceId,
          after: Identity.ServerSequence.make(0),
          limit: 10
        }).pipe(Effect.flip)
        assert.include(["ProtocolInvalid", "StorageCorrupt"], error._tag)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

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

  it.effect("rolls back server writes performed before a typed rejection", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const rejected = yield* envelope(
          Domain.RejectAfterWrite.name,
          Domain.todo("poison"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000011")
        )
        assert.strictEqual((yield* server.submit(rejected))._tag, "Rejected")

        const increment = yield* envelope(
          Domain.IncrementTodo.name,
          { id: "poison", delta: 1 },
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000012")
        )
        const receipt = yield* server.submit(increment)
        assert.strictEqual(receipt._tag, "Rejected")
        if (receipt._tag === "Rejected") assert.strictEqual(receipt.rejection, "TodoNotFound")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("terminally rejects an accepted entry that cannot fit in a pull page", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const oversized = yield* envelope(
          Domain.PutHugeTodo.name,
          { id: "huge" },
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000021")
        )
        assert.strictEqual((yield* server.submit(oversized))._tag, "Rejected")

        const next = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("next"),
          2,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000022")
        )
        const receipt = yield* server.submit(next)
        assert.strictEqual(receipt._tag, "Accepted")
        if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
        const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
        assert.deepStrictEqual(page.entries.map((entry) => entry.sequence), [1])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("stores a bounded terminal receipt when a private result exceeds the RPC response limit", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(ServerStore.ServerStore, serverLayer())
        const oversized = yield* envelope(
          Domain.ReturnHugeResult.name,
          null,
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000023")
        )
        const first = yield* server.submit(oversized)
        const retry = yield* server.submit(oversized)

        assert.strictEqual(first._tag, "Rejected")
        assert.deepStrictEqual(retry, first)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(first), Protocol.maximumReceiptBytes)
        if (first._tag === "Rejected") {
          assert.deepStrictEqual(first.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
        assert.deepStrictEqual(page.entries, [])
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("bounds an oversized authorization rejection before storing its terminal receipt", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          serverLayer(() => Effect.fail("x".repeat(Protocol.maximumReceiptBytes)))
        )
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("oversized-authorization"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        )
        const receipt = yield* server.submit(submitted)

        assert.strictEqual(receipt._tag, "Rejected")
        assert.isAtMost(yield* Protocol.encodedBytesEffect(receipt), Protocol.maximumReceiptBytes)
        if (receipt._tag === "Rejected") {
          assert.deepStrictEqual(receipt.rejection, {
            _tag: "CapacityExceeded",
            resource: "receipt bytes",
            limit: Protocol.maximumReceiptBytes
          })
        }
        assert.deepStrictEqual(yield* server.submit(submitted), receipt)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a pull when the durable authoritative log contains a sequence gap", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("gap-1"),
            1,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000024")
          )
        )
        yield* server.submit(
          yield* envelope(
            Domain.PutTodo.name,
            Domain.todo("gap-2"),
            2,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000025")
          )
        )
        yield* sql`DELETE FROM effect_local_authoritative_log
        WHERE space_id = ${spaceId} AND server_sequence = 1`

        const error = yield* server.pull({
          spaceId,
          after: Identity.ServerSequence.make(0),
          limit: 10
        }).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects an exact retry whose durable receipt conflicts with its SQL identity", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("retry-corrupt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000026")
        )
        yield* server.submit(submitted)
        const conflictingMutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000027")
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Rejected",
            spaceId,
            clientId,
            mutationId: conflictingMutationId,
            localSequence: submitted.localSequence,
            rejection: "corrupt"
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects an accepted retry whose receipt sequence conflicts with the authoritative log", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const serverDatabase = database()
        const live = ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
          Layer.provide(runtime),
          Layer.provide(serverDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, serverDatabase))
        const server = Context.get(context, ServerStore.ServerStore)
        const sql = Context.get(context, SqlClient.SqlClient)
        const submitted = yield* envelope(
          Domain.PutTodo.name,
          Domain.todo("retry-sequence-corrupt"),
          1,
          Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000028")
        )
        yield* server.submit(submitted)
        yield* sql`UPDATE effect_local_server_receipts SET receipt_json = ${
          Canonical.stringify({
            _tag: "Accepted",
            spaceId,
            clientId,
            mutationId: submitted.mutationId,
            localSequence: submitted.localSequence,
            serverSequence: 2,
            result: Domain.todo("retry-sequence-corrupt")
          })
        } WHERE space_id = ${spaceId} AND mutation_id = ${submitted.mutationId}`

        const error = yield* server.submit(submitted).pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("rejects a pending row whose durable digest does not match its reconstructed identity", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const clientDatabase = database()
        const live = LocalStore.layer({ definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(clientDatabase)
        )
        const context = yield* Layer.build(Layer.merge(live, clientDatabase))
        const local = Context.get(context, LocalStore.Store)
        const sql = Context.get(context, SqlClient.SqlClient)
        const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("digest-corrupt"))
        yield* sql`UPDATE effect_local_pending SET digest = ${"0".repeat(64)}
        WHERE mutation_id = ${pending.envelope.mutationId}`

        const error = yield* local.pending.pipe(Effect.flip)
        assert.strictEqual(error._tag, "StorageCorrupt")
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("persists each submitted receipt before submitting the next pending mutation", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("stream-1"))
      const second = yield* local.mutate(Domain.PutTodo, Domain.todo("stream-2"))
      let submissions = 0
      const remote = SyncEngine.SyncEngine.of({
        submit: (submitted) =>
          Effect.gen(function*() {
            submissions++
            if (submissions === 2) {
              assert.isTrue(Option.isSome(yield* local.receipt(first.envelope.mutationId)))
            }
            return Protocol.RejectedReceipt.make({
              spaceId,
              clientId,
              mutationId: submitted.mutationId,
              localSequence: submitted.localSequence,
              rejection: "denied"
            })
          }),
        pull: () => Effect.succeed(Protocol.PullPage.make({ entries: [], hasMore: false })),
        watch: () => Stream.never
      })
      const reconciler = yield* service(
        Reconciler.Reconciler,
        Reconciler.layer({ spaceId }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
        )
      )

      yield* reconciler.sync
      assert.strictEqual(submissions, 2)
      assert.isTrue(Option.isSome(yield* local.receipt(first.envelope.mutationId)))
      assert.isTrue(Option.isSome(yield* local.receipt(second.envelope.mutationId)))
      assert.strictEqual(yield* local.pendingCount, 0)
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

      const wakeCapacityError = yield* service(
        ServerStore.ServerStore,
        ServerStore.layerTrusted({ definition: Domain.definition, wakeCapacity: 0 }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      ).pipe(Effect.flip)
      assert.strictEqual(wakeCapacityError._tag, "InvalidConfiguration")
      if (wakeCapacityError._tag === "InvalidConfiguration") {
        assert.strictEqual(wakeCapacityError.option, "wakeCapacity")
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

      const error = yield* local.applyEntries([{ ...entry, changes: [] }]).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.cursor, 1)
    })))

  it.effect("does not settle pending work from a conflicting own-client entry", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* server.submit(pending.envelope)
      const page = yield* server.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      const entry = page.entries[0]!
      const error = yield* local.applyEntries([{
        ...entry,
        digest: "0".repeat(64)
      }]).pipe(Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
      assert.strictEqual(yield* local.pendingCount, 1)
      assert.strictEqual(yield* local.cursor, 0)
    })))

  it.effect("resubscribes after a watch stream terminates", () =>
    Effect.scoped(Effect.gen(function*() {
      const subscriptions = yield* Ref.make(0)
      const firstSubscribed = yield* Latch.make()
      const remote = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          submit: () => Effect.die("unexpected submit"),
          pull: () => Effect.succeed({ entries: [], hasMore: false }),
          watch: () =>
            Stream.unwrap(
              Ref.modify(subscriptions, (count) => [count, count + 1]).pipe(
                Effect.flatMap((count) =>
                  count === 0
                    ? firstSubscribed.open.pipe(Effect.as(Stream.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "disconnected"
                      })
                    )))
                    : Effect.succeed(Stream.never)
                )
              )
            )
        })
      )
      const reconciler = Reconciler.layer({ spaceId, retryDelay: "1 second" }).pipe(
        Layer.provide(localLayer()),
        Layer.provide(remote)
      )
      yield* service(Reconciler.Reconciler, reconciler)
      yield* firstSubscribed.await
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(subscriptions), 2)
    })))

  it.effect("retries pending mutations after an interrupted submit", () =>
    Effect.scoped(Effect.gen(function*() {
      const firstAttempt = yield* Deferred.make<void>()
      const secondAttempt = yield* Deferred.make<void>()
      let attempts = 0
      const remote = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          submit: (submitted) =>
            Effect.suspend(() => {
              attempts++
              if (attempts === 1) {
                return Deferred.succeed(firstAttempt, undefined).pipe(
                  Effect.andThen(Effect.interrupt)
                )
              }
              return Deferred.succeed(secondAttempt, undefined).pipe(
                Effect.as(Protocol.RejectedReceipt.make({
                  spaceId,
                  clientId,
                  mutationId: submitted.mutationId,
                  localSequence: submitted.localSequence,
                  rejection: "denied"
                }))
              )
            }),
          pull: () => Effect.succeed(Protocol.PullPage.make({ entries: [], hasMore: false })),
          watch: () => Stream.never
        })
      )
      const local = localLayer()
      const reconciler = Reconciler.layer({ spaceId, retryDelay: "1 second" }).pipe(
        Layer.provide(local),
        Layer.provide(remote)
      )
      const services = yield* Layer.build(Layer.merge(local, reconciler))
      const store = Context.get(services, LocalStore.Store)
      const scheduler = Context.get(services, Reconciler.Reconciler)
      yield* store.mutate(Domain.PutTodo, Domain.todo("interrupted"))

      yield* scheduler.notify
      yield* Deferred.await(firstAttempt)
      yield* Effect.yieldNow
      yield* scheduler.notify
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      assert.strictEqual(attempts, 2)
      yield* Deferred.await(secondAttempt)
    })))

  it.effect("emits a current watermark when a watch subscription becomes ready", () =>
    Effect.scoped(Effect.gen(function*() {
      const local = yield* service(LocalStore.Store, localLayer())
      const server = yield* service(ServerStore.ServerStore, serverLayer())
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("1"))
      yield* server.submit(pending.envelope)

      const wake = yield* server.watch(spaceId).pipe(Stream.runHead)
      assert.strictEqual(Option.getOrThrow(wake).sequence, 1)
    })))

  it.effect("isolates wake backpressure between spaces", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const server = yield* service(
          ServerStore.ServerStore,
          ServerStore.layerTrusted({ definition: Domain.definition, wakeCapacity: 1 }).pipe(
            Layer.provide(runtime),
            Layer.provide(database())
          )
        )
        const otherSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
        const ready = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let initial = true
        const wake = yield* server.watch(otherSpaceId).pipe(
          Stream.tap(() => {
            if (!initial) return Effect.void
            initial = false
            return Deferred.succeed(ready, undefined).pipe(Effect.andThen(Deferred.await(release)))
          }),
          Stream.drop(1),
          Stream.runHead,
          Effect.forkChild
        )
        yield* Deferred.await(ready)

        const makeForSpace = (
          targetSpaceId: Identity.SpaceId,
          localSequence: number,
          mutationId: Identity.MutationId
        ) =>
          Effect.gen(function*() {
            const identity = {
              spaceId: targetSpaceId,
              clientId,
              mutationId,
              localSequence: Identity.LocalSequence.make(localSequence),
              basis: Identity.ServerSequence.make(0),
              name: Domain.PutTodo.name,
              payload: Domain.todo(`${targetSpaceId}:${localSequence}`)
            }
            return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Canonical.digest(identity) })
          })
        yield* server.submit(
          yield* makeForSpace(
            otherSpaceId,
            1,
            Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000051")
          )
        )
        for (let index = 1; index <= 3; index++) {
          yield* server.submit(
            yield* makeForSpace(
              spaceId,
              index,
              Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(index + 51).padStart(12, "0")}`)
            )
          )
        }
        yield* Deferred.succeed(release, undefined)
        const result = yield* Fiber.join(wake)
        assert.strictEqual(Option.getOrThrow(result).spaceId, otherSpaceId)
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("runs multi-read queries against one committed visible snapshot", () =>
    Effect.scoped(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/snapshot.sqlite`
      const Item = Model.make("SnapshotItem", {
        version: 1,
        key: Schema.String,
        schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
      })
      const PutPair = Mutation.make("PutSnapshotPair", {
        version: 1,
        payload: { left: Schema.Number, right: Schema.Number }
      })
      const ReadPair = Query.make("ReadSnapshotPair", {
        success: Schema.Tuple([Schema.Number, Schema.Number]),
        dependsOn: [Item]
      })
      class QueryGate extends Context.Service<QueryGate, {
        readonly betweenReads: Effect.Effect<void>
      }>()("test/QueryGate") {}
      const definition = Definition.make({ version: 1, models: [Item], mutations: [PutPair], queries: [ReadPair] })
      const reached = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const writerStarted = yield* Deferred.make<void>()
      const handlers = Layer.mergeAll(
        PutPair.toLayer(({ payload, transaction }) =>
          Effect.gen(function*() {
            if (payload.left === 1) yield* Deferred.succeed(writerStarted, undefined)
            yield* transaction.set(Item, "left", { id: "left", value: payload.left })
            yield* transaction.set(Item, "right", { id: "right", value: payload.right })
          })
        ),
        ReadPair.toLayer(Effect.gen(function*() {
          const gate = yield* QueryGate
          return ({ query }) =>
            Effect.gen(function*() {
              const left = Option.getOrThrow(yield* query.get(Item, "left"))
              yield* gate.betweenReads
              const right = Option.getOrThrow(yield* query.get(Item, "right"))
              return [left.value, right.value] as const
            })
        }))
      )
      const gate = Layer.succeed(
        QueryGate,
        QueryGate.of({
          betweenReads: Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(release))
          )
        })
      )
      const pairDatabase = () =>
        Layer.mergeAll(
          SqliteClient.layer({ filename }),
          NodeCrypto.layer,
          Reactivity.layer
        )
      const pairRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers), Layer.provide(gate))
      const local = LocalStore.layer({ definition, spaceId, clientId }).pipe(
        Layer.provide(pairRuntime),
        Layer.provide(pairDatabase())
      )
      const queries = QueryExecutor.layer(definition).pipe(
        Layer.provide(handlers),
        Layer.provide(gate),
        Layer.provide(pairDatabase())
      )
      const context = yield* Layer.build(Layer.merge(local, queries))
      const store = Context.get(context, LocalStore.Store)
      const queryExecutor = Context.get(context, QueryExecutor.QueryExecutor)
      yield* store.mutate(PutPair, { left: 0, right: 0 })
      const query = yield* queryExecutor.execute(ReadPair, undefined).pipe(Effect.forkChild)
      yield* Deferred.await(reached)
      const mutation = yield* store.mutate(PutPair, { left: 1, right: 1 }).pipe(Effect.forkChild)
      yield* Deferred.await(writerStarted)
      yield* Fiber.join(mutation)
      yield* Deferred.succeed(release, undefined)
      assert.deepStrictEqual(yield* Fiber.join(query), [0, 0])
    })).pipe(Effect.provide(NodeFileSystem.layer)))

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

  it.effect("reconciles an offline mutation queue with linear handler work", () =>
    Effect.scoped(Effect.gen(function*() {
      const Item = Model.make("ReconciliationWorkItem", {
        version: 1,
        key: Schema.String,
        schema: Schema.Struct({ id: Schema.String, value: Schema.Number })
      })
      const PutItem = Mutation.make("PutReconciliationWorkItem", {
        version: 1,
        payload: Item.schema,
        success: Item.schema
      })
      const workDefinition = Definition.make({ version: 1, models: [Item], mutations: [PutItem] })
      const executions = yield* Ref.make(0)
      const workHandlers = PutItem.toLayer(({ payload, transaction }) =>
        Ref.update(executions, (count) => count + 1).pipe(
          Effect.andThen(transaction.set(Item, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const workRuntime = MutationRuntime.layer(workDefinition).pipe(Layer.provide(workHandlers))
      const authoritativeDatabase = database()
      const authoritativeLayer = ServerStore.layerTrusted({ definition: workDefinition }).pipe(
        Layer.provide(workRuntime),
        Layer.provide(authoritativeDatabase)
      )
      const server = yield* service(ServerStore.ServerStore, authoritativeLayer)
      const replicaDatabase = database()
      const local = LocalStore.layer({ definition: workDefinition, spaceId, clientId }).pipe(
        Layer.provide(workRuntime),
        Layer.provide(replicaDatabase)
      )
      const reconciler = Reconciler.layer({ spaceId }).pipe(
        Layer.provide(local),
        Layer.provide(directSync(server))
      )
      const context = yield* Layer.build(Layer.merge(local, reconciler))
      const store = Context.get(context, LocalStore.Store)
      const sync = Context.get(context, Reconciler.Reconciler)
      yield* sync.sync
      yield* Ref.set(executions, 0)

      const mutationCount = 10
      for (let index = 0; index < mutationCount; index++) {
        yield* store.mutate(PutItem, { id: String(index), value: index })
      }
      yield* sync.sync

      assert.strictEqual(yield* store.pendingCount, 0)
      assert.isAtMost(yield* Ref.get(executions), mutationCount * 3)
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
    })))

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
