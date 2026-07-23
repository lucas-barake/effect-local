import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import * as ReplicaClient from "../src/ReplicaClient.js"
import type * as ReplicaRpc from "../src/ReplicaRpc.js"
import { Rename, replica, Task } from "./fixtures.js"

describe("ReplicaAtom", () => {
  it.effect("reads documents through documentFamily", () =>
    Effect.gen(function*() {
      const requested = yield* Deferred.make<Identity.DocumentId>()
      const snapshot = {
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000010"),
        value: { title: "from atom" },
        version: 1,
        heads: [],
        tombstone: false,
        projection: "Ready" as const
      }
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        get: (_document, documentId) =>
          Deferred.succeed(requested, documentId).pipe(
            Effect.as(snapshot)
          ) as never
      }))
      const registry = AtomRegistry.make()
      const atom = ReplicaAtom.documentFamily(atomRuntime, Task)(snapshot.documentId)
      const unmount = registry.mount(atom)
      assert.strictEqual(yield* Deferred.await(requested), snapshot.documentId)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const value = registry.get(atom)
      assert.isTrue(AsyncResult.isSuccess(value))
      if (AsyncResult.isSuccess(value)) assert.deepStrictEqual(value.value, snapshot)
      unmount()
      registry.dispose()
    }))

  it.effect("executes mutations through mutation atoms", () =>
    Effect.gen(function*() {
      const called = yield* Deferred.make<{
        readonly commandId: Identity.CommandId
        readonly documentId: Identity.DocumentId
        readonly payload: { readonly title: string }
      }>()
      const release = yield* Deferred.make<void>()
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000010")
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000010")
      const options = { commandId, documentId, payload: { title: "renamed from atom" } }
      const outcome = CommandOutcome.durablyCommitted(commandId, "renamed from atom")
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        mutate: (_mutation, received) =>
          Deferred.succeed(called, received as typeof options).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(outcome)
          ) as never
      }))
      const registry = AtomRegistry.make()
      const atom = ReplicaAtom.mutation(atomRuntime, Rename)
      const unmount = registry.mount(atom)
      registry.set(atom, options)
      assert.deepStrictEqual(yield* Deferred.await(called), options)
      yield* Deferred.succeed(release, undefined)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const value = registry.get(atom)
      assert.isTrue(AsyncResult.isSuccess(value))
      if (AsyncResult.isSuccess(value)) assert.deepStrictEqual(value.value, outcome)
      unmount()
      registry.dispose()
    }))

  it.effect("streams replica status through status atoms", () =>
    Effect.gen(function*() {
      const consumed = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 2 }
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        status: Stream.make(ready).pipe(
          Stream.tap(() => Deferred.succeed(consumed, undefined))
        )
      }))
      const registry = AtomRegistry.make()
      const atom = ReplicaAtom.status(atomRuntime)
      const unmount = registry.mount(atom)
      yield* Deferred.await(consumed)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const value = registry.get(atom)
      assert.isTrue(AsyncResult.isSuccess(value))
      if (AsyncResult.isSuccess(value)) assert.deepStrictEqual(value.value, ready)
      unmount()
      registry.dispose()
    }))

  it.effect("refreshes query atoms when a dependency document is invalidated", () =>
    Effect.gen(function*() {
      const Task = Document.make("ReactiveTask", {
        schema: Schema.Struct({ title: Schema.String }),
        version: 1
      })
      const ByTitle = Projection.make("ReactiveTaskByTitle", {
        document: Task,
        version: 1,
        Row: Task.schema,
        key: (row) => row.title,
        project: (snapshot) => [snapshot.value]
      })
      const Search = Query.make("ReactiveTaskSearch", {
        success: Schema.Array(Task.schema),
        dependsOn: [ByTitle]
      })
      const first = yield* Deferred.make<void>()
      const second = yield* Deferred.make<void>()
      let executions = 0
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        query: () =>
          Effect.gen(function*() {
            executions++
            if (executions === 1) yield* Deferred.succeed(first, undefined)
            if (executions === 2) yield* Deferred.succeed(second, undefined)
            return []
          }) as never
      }))
      const query = ReplicaAtom.queryFamily(atomRuntime, Search)
      const invalidateDocument = atomRuntime.fn(
        Effect.fn(function*(_value: void) {}),
        { reactivityKeys: [Task.name] }
      )
      const registry = AtomRegistry.make()
      const atom = query()
      const unmount = registry.mount(atom)
      yield* Deferred.await(first)
      assert.strictEqual(executions, 1)
      registry.set(invalidateDocument, undefined)
      yield* Deferred.await(second)
      assert.strictEqual(executions, 2)
      unmount()
      registry.dispose()
    }))

  it.effect("refreshes native query atoms from owner invalidations", () =>
    Effect.gen(function*() {
      const Task = Document.make("RemoteReactiveTask", {
        schema: Schema.Struct({ title: Schema.String }),
        version: 1
      })
      const ByTitle = Projection.make("RemoteReactiveTaskByTitle", {
        document: Task,
        version: 1,
        Row: Task.schema,
        key: (row) => row.title,
        project: (snapshot) => [snapshot.value]
      })
      const Search = Query.make("RemoteReactiveTaskSearch", {
        success: Schema.Array(Task.schema),
        dependsOn: [ByTitle]
      })
      const events = yield* Queue.unbounded<ReplicaRpc.Invalidation>()
      const first = yield* Deferred.make<void>()
      const second = yield* Deferred.make<void>()
      const consumed = yield* Deferred.make<void>()
      let executions = 0
      const client: ReplicaClient.ReplicaClient["Service"] = {
        ...replica,
        ownerEpoch: "owner",
        invalidations: Stream.fromQueue(events).pipe(
          Stream.tap(() => Deferred.succeed(consumed, undefined))
        ),
        query: () =>
          Effect.gen(function*() {
            executions++
            if (executions === 1) yield* Deferred.succeed(first, undefined)
            if (executions === 2) yield* Deferred.succeed(second, undefined)
            return []
          }) as never
      }
      const Client = Layer.succeed(ReplicaClient.ReplicaClient, client)
      const atomRuntime = Atom.runtime(Layer.merge(
        Layer.succeed(Replica.Replica, client),
        ReplicaAtom.layerReactivity.pipe(Layer.provide(Client))
      ))
      const registry = AtomRegistry.make()
      const atom = ReplicaAtom.queryFamily(atomRuntime, Search)()
      const unmount = registry.mount(atom)
      yield* Deferred.await(first)
      yield* Queue.offer(events, {
        _tag: "Invalidation",
        ownerEpoch: client.ownerEpoch,
        sequence: Identity.CommitSequence.make(1),
        keys: [Task.name]
      })
      yield* Deferred.await(consumed)
      yield* Deferred.await(second)
      assert.strictEqual(executions, 2)
      unmount()
    }))

  it.effect("retries transient invalidation failures", () =>
    Effect.gen(function*() {
      const reactivity = yield* Reactivity.make
      const consumed = yield* Deferred.make<void>()
      let subscriptions = 0
      let invalidations = 0
      reactivity.registerUnsafe(["retry-key"], () => invalidations++)
      const client: ReplicaClient.ReplicaClient["Service"] = {
        ...replica,
        ownerEpoch: "owner",
        invalidations: Stream.unwrap(Effect.sync(() => {
          subscriptions++
          return subscriptions < 4
            ? Stream.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause: new Error("disconnected")
                })
              })
            )
            : Stream.make({
              _tag: "Invalidation" as const,
              ownerEpoch: "owner",
              sequence: Identity.CommitSequence.make(1),
              keys: ["retry-key"]
            }).pipe(Stream.tap(() => Deferred.succeed(consumed, undefined)))
        }))
      }
      yield* Effect.scoped(
        Effect.gen(function*() {
          yield* Layer.build(ReplicaAtom.layerReactivity)
          yield* TestClock.adjust(3_000)
          yield* Deferred.await(consumed)
          yield* Effect.yieldNow
          assert.strictEqual(subscriptions, 4)
          assert.strictEqual(invalidations, 1)
        }).pipe(
          Effect.provideService(ReplicaClient.ReplicaClient, client),
          Effect.provideService(Reactivity.Reactivity, reactivity)
        )
      )
    }))

  it.effect("recovers reactivity after a transient quota rejection", () =>
    Effect.gen(function*() {
      const reactivity = yield* Reactivity.make
      const firstAttempted = yield* Deferred.make<void>()
      const consumed = yield* Deferred.make<void>()
      let subscriptions = 0
      let invalidations = 0
      reactivity.registerUnsafe(["retry-key"], () => invalidations++)
      const client: ReplicaClient.ReplicaClient["Service"] = {
        ...replica,
        ownerEpoch: "owner",
        invalidations: Stream.unwrap(Effect.sync(() => {
          subscriptions++
          return subscriptions < 2
            ? Stream.fromEffect(Deferred.succeed(firstAttempted, undefined)).pipe(
              Stream.flatMap(() =>
                Stream.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.QuotaExceeded({
                      resource: "queued RPCs",
                      limit: 1
                    })
                  })
                )
              )
            )
            : Stream.make({
              _tag: "Invalidation" as const,
              ownerEpoch: "owner",
              sequence: Identity.CommitSequence.make(1),
              keys: ["retry-key"]
            }).pipe(Stream.tap(() => Deferred.succeed(consumed, undefined)))
        }))
      }
      yield* Effect.scoped(
        Effect.gen(function*() {
          yield* Layer.build(ReplicaAtom.layerReactivity)
          yield* Deferred.await(firstAttempted)
          yield* Effect.yieldNow
          yield* TestClock.adjust(1_000)
          yield* Effect.yieldNow
          assert.strictEqual(subscriptions, 2)
          yield* Deferred.await(consumed)
          assert.strictEqual(invalidations, 1)
        }).pipe(
          Effect.provideService(ReplicaClient.ReplicaClient, client),
          Effect.provideService(Reactivity.Reactivity, reactivity)
        )
      )
    }))
})
