import { assert, describe, it } from "@effect/vitest"
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
import { Read, replica } from "./fixtures.js"

describe("ReplicaAtom", () => {
  it.effect("runs query atoms in the provided registry", () =>
    Effect.gen(function*() {
      const atoms = yield* ReplicaAtom.ReplicaAtom
      const query = atoms.query(Read, "visible")
      const unmount = atoms.mount(query)
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)))
      const result = atoms.registry.get(query)
      assert.isTrue(AsyncResult.isSuccess(result))
      if (AsyncResult.isSuccess(result)) assert.deepStrictEqual(result.value, [{ title: "visible" }])
      unmount()
    }).pipe(
      Effect.provide(ReplicaAtom.layer),
      Effect.provide(Layer.succeed(Replica.Replica, replica)),
      Effect.provide(AtomRegistry.layer)
    ))

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
      let executions = 0
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        query: () =>
          Effect.sync(() => {
            executions++
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
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)))
      assert.strictEqual(executions, 1)
      registry.set(invalidateDocument, undefined)
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)))
      assert.strictEqual(executions, 2)
      unmount()
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
      const client: ReplicaClient.Service = {
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
        ReplicaAtom.reactivityLayer.pipe(Layer.provide(Client))
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
      const client: ReplicaClient.Service = {
        ...replica,
        ownerEpoch: "owner",
        invalidations: Stream.unwrap(Effect.sync(() => {
          subscriptions++
          return subscriptions === 1
            ? Stream.fail(
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "RpcCause", message: "disconnected" }
                }
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
          yield* Layer.build(ReplicaAtom.reactivityLayer)
          yield* TestClock.adjust(1_000)
          yield* Deferred.await(consumed)
          yield* Effect.yieldNow
          assert.strictEqual(subscriptions, 2)
          assert.strictEqual(invalidations, 1)
        }).pipe(
          Effect.provideService(ReplicaClient.ReplicaClient, client),
          Effect.provideService(Reactivity.Reactivity, reactivity)
        )
      )
    }))
})
