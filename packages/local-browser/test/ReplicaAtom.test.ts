import { assert, describe, it } from "@effect/vitest"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
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
      const committed = "renamed from atom"
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        mutate: (_mutation, received) =>
          Deferred.succeed(called, received as typeof options).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(committed)
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
      if (AsyncResult.isSuccess(value)) assert.deepStrictEqual(value.value, committed)
      unmount()
      registry.dispose()
    }))

  it.effect("scopes mutation refreshes to one document while refreshing dependent queries", () =>
    Effect.gen(function*() {
      const ByTitle = Projection.make("TaskByTitleForAtomInvalidation", {
        document: Task,
        version: 1,
        Row: Task.schema,
        key: (row) => row.title,
        project: (snapshot) => [snapshot.value]
      })
      const Search = Query.make("TaskSearchForAtomInvalidation", {
        success: Schema.Array(Task.schema),
        dependsOn: [ByTitle]
      })
      const targetId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000011")
      const otherId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000012")
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000011")
      const initialReads = yield* Deferred.make<void>()
      const targetDocumentRefresh = yield* Deferred.make<void>()
      const targetConflictRefresh = yield* Deferred.make<void>()
      const queryRefresh = yield* Deferred.make<void>()
      const fullRefresh = yield* Deferred.make<void>()
      const documentReads = new Map<Identity.DocumentId, number>()
      const conflictReads = new Map<Identity.DocumentId, number>()
      let initialized = 0
      let queryReads = 0
      const noteInitialRead = Effect.sync(() => {
        initialized++
        if (initialized === 4) Deferred.doneUnsafe(initialReads, Effect.void)
      })
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        get: (_document, documentId) =>
          Effect.gen(function*() {
            const reads = (documentReads.get(documentId) ?? 0) + 1
            documentReads.set(documentId, reads)
            if (reads === 1) yield* noteInitialRead
            if (documentId === targetId && reads === 2) {
              yield* Deferred.succeed(targetDocumentRefresh, undefined)
            }
            if (documentId === otherId && reads === 2) {
              yield* Deferred.succeed(fullRefresh, undefined)
            }
            return {
              documentId,
              value: { title: "stored" },
              version: 1,
              heads: [],
              tombstone: false,
              projection: "Ready" as const
            }
          }) as never,
        inspectConflicts: (_document, documentId) =>
          Effect.gen(function*() {
            const reads = (conflictReads.get(documentId) ?? 0) + 1
            conflictReads.set(documentId, reads)
            if (reads === 1) yield* noteInitialRead
            if (documentId === targetId && reads === 2) {
              yield* Deferred.succeed(targetConflictRefresh, undefined)
            }
            return {
              snapshot: {
                documentId,
                value: { title: "stored" },
                version: 1,
                heads: [],
                tombstone: false,
                projection: "Ready" as const
              },
              conflicts: []
            }
          }) as never,
        mutate: () => Effect.succeed("renamed") as never,
        query: () =>
          Effect.sync(() => {
            queryReads++
            if (queryReads === 2) Deferred.doneUnsafe(queryRefresh, Effect.void)
            return []
          }) as never
      }))
      const registry = AtomRegistry.make()
      const targetDocumentAtom = ReplicaAtom.documentFamily(atomRuntime, Task)(targetId)
      const otherDocumentAtom = ReplicaAtom.documentFamily(atomRuntime, Task)(otherId)
      const targetConflictAtom = ReplicaAtom.conflictFamily(atomRuntime, Task)(targetId)
      const otherConflictAtom = ReplicaAtom.conflictFamily(atomRuntime, Task)(otherId)
      const queryAtom = ReplicaAtom.queryFamily(atomRuntime, Search)()
      const mutationAtom = ReplicaAtom.mutation(atomRuntime, Rename)
      const refreshTypeAtom = atomRuntime.fn(
        (_: void) => Effect.void,
        { reactivityKeys: [ReplicaDefinition.documentTypeRefreshKey(Task.name)] }
      )
      const unmounts = [
        registry.mount(targetDocumentAtom),
        registry.mount(otherDocumentAtom),
        registry.mount(targetConflictAtom),
        registry.mount(otherConflictAtom),
        registry.mount(queryAtom),
        registry.mount(mutationAtom),
        registry.mount(refreshTypeAtom)
      ]
      yield* Deferred.await(initialReads)

      registry.set(mutationAtom, {
        commandId,
        documentId: targetId,
        payload: { title: "renamed" }
      })
      yield* AtomRegistry.getResult(registry, mutationAtom, { suspendOnWaiting: true })
      yield* Effect.all([
        Deferred.await(targetDocumentRefresh),
        Deferred.await(targetConflictRefresh),
        Deferred.await(queryRefresh)
      ], { discard: true })
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.strictEqual(documentReads.get(targetId), 2)
      assert.strictEqual(documentReads.get(otherId), 1)
      assert.strictEqual(conflictReads.get(targetId), 2)
      assert.strictEqual(conflictReads.get(otherId), 1)
      assert.strictEqual(queryReads, 2)

      registry.set(refreshTypeAtom, undefined)
      yield* AtomRegistry.getResult(registry, refreshTypeAtom, { suspendOnWaiting: true })
      yield* Deferred.await(fullRefresh)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.strictEqual(documentReads.get(targetId), 3)
      assert.strictEqual(documentReads.get(otherId), 2)
      assert.strictEqual(conflictReads.get(targetId), 3)
      assert.strictEqual(conflictReads.get(otherId), 2)
      assert.strictEqual(queryReads, 2)

      for (const unmount of unmounts.toReversed()) unmount()
      registry.dispose()
    }))

  it.effect("scopes conflict resolution refreshes to one document", () =>
    Effect.gen(function*() {
      const initialReads = yield* Deferred.make<void>()
      const targetDocumentRefresh = yield* Deferred.make<void>()
      const targetConflictRefresh = yield* Deferred.make<void>()
      const targetId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000020")
      const otherId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000022")
      const documentReads = new Map<Identity.DocumentId, number>()
      const conflictReads = new Map<Identity.DocumentId, number>()
      let initialized = 0
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000020")
      const resolution = Conflict.Resolution.make({
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "DeleteValue" }
      })
      const noteInitialRead = Effect.sync(() => {
        initialized++
        if (initialized === 4) Deferred.doneUnsafe(initialReads, Effect.void)
      })
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        get: (_document, documentId) =>
          Effect.gen(function*() {
            const reads = (documentReads.get(documentId) ?? 0) + 1
            documentReads.set(documentId, reads)
            if (reads === 1) yield* noteInitialRead
            if (documentId === targetId && reads === 2) {
              yield* Deferred.succeed(targetDocumentRefresh, undefined)
            }
            return {
              documentId,
              value: { title: "stored" },
              version: 1,
              heads: [],
              tombstone: false,
              projection: "Ready" as const
            }
          }) as never,
        inspectConflicts: (_document, documentId) =>
          Effect.gen(function*() {
            const reads = (conflictReads.get(documentId) ?? 0) + 1
            conflictReads.set(documentId, reads)
            if (reads === 1) yield* noteInitialRead
            if (documentId === targetId && reads === 2) {
              yield* Deferred.succeed(targetConflictRefresh, undefined)
            }
            return {
              snapshot: {
                documentId,
                value: { title: "stored" },
                version: 1,
                heads: [],
                tombstone: false,
                projection: "Ready" as const
              },
              conflicts: []
            }
          }) as never,
        resolveConflict: () => Effect.void
      }))
      const registry = AtomRegistry.make()
      const targetDocumentAtom = ReplicaAtom.documentFamily(atomRuntime, Task)(targetId)
      const otherDocumentAtom = ReplicaAtom.documentFamily(atomRuntime, Task)(otherId)
      const targetConflictAtom = ReplicaAtom.conflictFamily(atomRuntime, Task)(targetId)
      const otherConflictAtom = ReplicaAtom.conflictFamily(atomRuntime, Task)(otherId)
      const resolveAtom = ReplicaAtom.resolveConflict(atomRuntime, Task)
      const unmounts = [
        registry.mount(targetDocumentAtom),
        registry.mount(otherDocumentAtom),
        registry.mount(targetConflictAtom),
        registry.mount(otherConflictAtom),
        registry.mount(resolveAtom)
      ]
      yield* Deferred.await(initialReads)

      registry.set(resolveAtom, { commandId, documentId: targetId, resolution })
      yield* AtomRegistry.getResult(registry, resolveAtom, { suspendOnWaiting: true })
      yield* Effect.all([
        Deferred.await(targetDocumentRefresh),
        Deferred.await(targetConflictRefresh)
      ], { discard: true })
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.strictEqual(documentReads.get(targetId), 2)
      assert.strictEqual(documentReads.get(otherId), 1)
      assert.strictEqual(conflictReads.get(targetId), 2)
      assert.strictEqual(conflictReads.get(otherId), 1)

      for (const unmount of unmounts.toReversed()) unmount()
      registry.dispose()
    }))

  it.effect("does not refresh document state after a failed resolution", () =>
    Effect.gen(function*() {
      const documentRead = yield* Deferred.make<void>()
      const conflictRead = yield* Deferred.make<void>()
      const resolutionCalled = yield* Deferred.make<void>()
      const releaseResolution = yield* Deferred.make<void>()
      const resolutionFailed = yield* Deferred.make<void>()
      let documentReads = 0
      let conflictReads = 0
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000021")
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000021")
      const path = { parents: [], target: { _tag: "Key" as const, key: "title" } }
      const resolution = Conflict.Resolution.make({
        heads: [],
        path,
        choice: { _tag: "DeleteValue" }
      })
      const snapshot = {
        documentId,
        value: { title: "stored" },
        version: 1,
        heads: [],
        tombstone: false,
        projection: "Ready" as const
      }
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        get: () =>
          Effect.sync(() => ++documentReads).pipe(
            Effect.tap(() => Deferred.succeed(documentRead, undefined)),
            Effect.as(snapshot)
          ) as never,
        inspectConflicts: () =>
          Effect.sync(() => ++conflictReads).pipe(
            Effect.tap(() => Deferred.succeed(conflictRead, undefined)),
            Effect.as({ snapshot, conflicts: [] })
          ) as never,
        resolveConflict: () =>
          Deferred.succeed(resolutionCalled, undefined).pipe(
            Effect.andThen(Deferred.await(releaseResolution)),
            Effect.andThen(Effect.fail(new Conflict.ConflictNotFound({ path })))
          )
      }))
      const registry = AtomRegistry.make()
      const documentAtom = ReplicaAtom.documentFamily(atomRuntime, Task)(documentId)
      const conflictsAtom = ReplicaAtom.conflictFamily(atomRuntime, Task)(documentId)
      const resolveAtom = ReplicaAtom.resolveConflict(atomRuntime, Task)
      const unmountDocument = registry.mount(documentAtom)
      const unmountConflicts = registry.mount(conflictsAtom)
      const unmountResolve = registry.mount(resolveAtom)
      const unsubscribe = registry.subscribe(resolveAtom, (value) => {
        if (AsyncResult.isFailure(value)) {
          Deferred.doneUnsafe(resolutionFailed, Effect.void)
        }
      })
      yield* Effect.all([
        Deferred.await(documentRead),
        Deferred.await(conflictRead)
      ], { discard: true })

      registry.set(resolveAtom, { commandId, documentId, resolution })
      yield* Deferred.await(resolutionCalled)
      yield* Deferred.succeed(releaseResolution, undefined)
      yield* Deferred.await(resolutionFailed)
      assert.strictEqual(documentReads, 1)
      assert.strictEqual(conflictReads, 1)

      unsubscribe()
      unmountResolve()
      unmountConflicts()
      unmountDocument()
      registry.dispose()
    }))

  it.effect("streams degraded and recovered replica status through status atoms", () =>
    Effect.gen(function*() {
      const degradedConsumed = yield* Deferred.make<void>()
      const readyConsumed = yield* Deferred.make<void>()
      const recover = yield* Deferred.make<void>()
      const degraded = { _tag: "Degraded" as const, reason: "StorageUnavailable" as const }
      const ready = { _tag: "Ready" as const, pendingCommands: 2 }
      const atomRuntime = Atom.runtime(Layer.succeed(Replica.Replica, {
        ...replica,
        status: Stream.make(degraded).pipe(
          Stream.tap(() => Deferred.succeed(degradedConsumed, undefined)),
          Stream.concat(
            Stream.fromEffect(
              Deferred.await(recover).pipe(
                Effect.as(ready),
                Effect.tap(() => Deferred.succeed(readyConsumed, undefined))
              )
            )
          )
        )
      }))
      const registry = AtomRegistry.make()
      const atom = ReplicaAtom.status(atomRuntime)
      const unmount = registry.mount(atom)
      yield* Deferred.await(degradedConsumed)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const degradedValue = registry.get(atom)
      assert.isTrue(AsyncResult.isSuccess(degradedValue))
      if (AsyncResult.isSuccess(degradedValue)) assert.deepStrictEqual(degradedValue.value, degraded)
      yield* Deferred.succeed(recover, undefined)
      yield* Deferred.await(readyConsumed)
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

  it.effect("bridges invalidations from independently provided replica clients", () =>
    Effect.scoped(Effect.gen(function*() {
      const reactivity = yield* Reactivity.make
      const seen: Array<string> = []
      reactivity.registerUnsafe(["left"], () => seen.push("left"))
      reactivity.registerUnsafe(["right"], () => seen.push("right"))

      const client = (ownerEpoch: string, key: string): ReplicaClient.ReplicaClient["Service"] => ({
        ...replica,
        ownerEpoch,
        invalidations: Stream.make({
          _tag: "Invalidation" as const,
          ownerEpoch,
          sequence: Identity.CommitSequence.make(1),
          keys: [key]
        })
      })
      const bridge = (service: ReplicaClient.ReplicaClient["Service"]) =>
        ReplicaAtom.layerReactivity.pipe(
          Layer.provide(Layer.succeed(ReplicaClient.ReplicaClient, service))
        )

      yield* Layer.build(
        Layer.merge(
          bridge(client("left-owner", "left")),
          bridge(client("right-owner", "right"))
        )
      ).pipe(Effect.provideService(Reactivity.Reactivity, reactivity))
      yield* Effect.yieldNow
      yield* Effect.yieldNow

      assert.deepStrictEqual(Array.from(seen).toSorted(), ["left", "right"])
    })))

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
