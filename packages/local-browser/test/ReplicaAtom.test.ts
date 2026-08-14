import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as AttachmentClient from "@lucas-barake/effect-local-sql/AttachmentClient"
import type * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as AttachmentTransfer from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as FaultInjection from "@lucas-barake/effect-local-test/FaultInjection"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as TestServer from "@lucas-barake/effect-local-test/TestServer"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as BrowserAttachmentStorage from "../src/BrowserAttachmentStorage.js"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as AttachmentDirectory from "../src/internal/AttachmentDirectory.js"
import * as AttachmentWorkerProtocol from "../src/internal/AttachmentWorkerProtocol.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const TodoSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: TodoSchema,
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: typeof TodoSchema.Type) => todo.title
      }]
    }
  }
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const Numbered = Model.make("Numbered", {
  version: 1,
  key: Schema.NumberFromString,
  schema: Schema.Struct({ id: Schema.Number, value: Schema.String })
})
const PutNumbered = Mutation.make("PutNumbered", {
  version: 1,
  payload: Numbered.schema,
  success: Numbered.schema
})
const ListTodos = Query.make("ListTodos", {
  success: Schema.Array(Todo.schema)
})
const RangeTodos = Query.make("RangeTodos", {
  payload: { lower: Schema.String, upper: Schema.String },
  success: Schema.Array(Todo.schema)
})
const rangeReads = new Map<string, number>()
const definition = Definition.make({
  version: 1,
  models: [Todo, Numbered],
  mutations: [PutTodo, PutNumbered],
  queries: [ListTodos, RangeTodos]
})
const layerHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  PutNumbered.toLayer(({ payload, transaction }) =>
    transaction.set(Numbered, payload.id, payload).pipe(Effect.as(payload))
  ),
  ListTodos.toLayer(({ query }) =>
    query.from(Todo, "byTitle").limit(100).page().pipe(Effect.map((page) => page.items))
  ),
  RangeTodos.toLayer(({ payload, query }) => {
    const key = `${payload.lower}:${payload.upper}`
    rangeReads.set(key, (rangeReads.get(key) ?? 0) + 1)
    return query.from(Todo, "byTitle")
      .where({ title: { gte: payload.lower, lt: payload.upper } })
      .limit(20)
      .page()
      .pipe(Effect.map((page) => page.items))
  })
)
const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)
const layerMutationRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name, Numbered.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Todo.name, Numbered.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  settlementCapacity: 64,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const layerServer = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds" as const,
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
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}).pipe(
  Layer.provide(layerMutationRuntime),
  Layer.provide(layerDatabase)
)
const layerSync = Effect.gen(function*() {
  const store = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: store.submit,
    discard: (request) => store.discard(request, null),
    pull: store.pull,
    bootstrap: store.bootstrap,
    watch: store.watch
  })
}).pipe(
  Layer.effect(SyncEngine.SyncEngine),
  Layer.provide(layerServer)
)
const layerReplica = SqlReplica.layer({
  ...clientHistory,
  definition,
  initialSpaces: [spaceId, secondSpaceId],
  clientId,
  retryDelay: "10 millis"
}).pipe(
  Layer.provide(layerSync),
  Layer.provide(layerDatabase),
  Layer.provide(layerHandlers)
)

const faultedReplica = (faultsReady: Deferred.Deferred<FaultInjection.Service>) => {
  const layerFaults = FaultInjection.layer.pipe(
    Layer.tap((context) => Deferred.succeed(faultsReady, Context.get(context, FaultInjection.FaultInjection)))
  )
  const layerFaultedSync = TestServer.layer.pipe(
    Layer.provide(layerServer),
    Layer.provide(layerFaults),
    Layer.provide(NodeCrypto.layer)
  )
  return TestReplica.layer({
    ...clientHistory,
    definition,
    initialSpaces: [spaceId],
    clientId,
    retryDelay: "10 millis"
  }).pipe(
    Layer.provide(layerFaultedSync),
    Layer.provide(layerDatabase),
    Layer.provide(layerHandlers)
  )
}

describe("Replica Atom graph", () => {
  it.effect(
    "exposes attachment placeholder, failure, and lazily resolved bytes",
    Effect.fnUntraced(function*() {
      const bytes = Uint8Array.from([1, 2, 3])
      const availableReady = yield* Deferred.make<Attachment.Reference>()
      const allowRead = yield* Deferred.make<void>()
      const files = new Map<AttachmentStorage.ObjectKey, Uint8Array>()
      const directory = AttachmentDirectory.AttachmentDirectory.of({
        create: (key) => Effect.sync(() => files.set(key, new Uint8Array(0))),
        offset: (key) => {
          const stored = files.get(key)
          if (stored === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
          return Effect.succeed(stored.length)
        },
        write: (key, expectedOffset, chunk) => {
          const stored = files.get(key)
          if (stored === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
          if (stored.length !== expectedOffset) {
            return Effect.fail(
              new Attachment.AttachmentOffsetConflict({ expected: expectedOffset, actual: stored.length })
            )
          }
          const next = new Uint8Array(stored.length + chunk.length)
          next.set(stored)
          next.set(chunk, stored.length)
          files.set(key, next)
          return Effect.succeed(next.length)
        },
        read: (key, offset, length) => {
          const stored = files.get(key)
          if (stored === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
          return Deferred.await(allowRead).pipe(Effect.as(stored.slice(offset, offset + length)))
        },
        exists: (key) => Effect.succeed(files.has(key)),
        remove: (key) => Effect.sync(() => void files.delete(key))
      })
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          channel.port1.close()
          channel.port2.close()
        })
      )
      yield* AttachmentWorkerProtocol.serve(channel.port1, {
        maximumBytes: 8,
        maximumPendingRequests: 2
      }).pipe(
        Effect.provideService(AttachmentDirectory.AttachmentDirectory, directory),
        Effect.forkScoped({ startImmediately: true })
      )
      const layerStorage = BrowserAttachmentStorage.layerMessagePort(channel.port2, {
        maximumBytes: 8,
        readChunkBytes: 2,
        maximumPendingRequests: 2,
        cleanupRequestTimeout: "1 second"
      }).pipe(Layer.provide(NodeCrypto.layer))
      const layerTransfer = Layer.succeed(
        AttachmentTransfer.AttachmentTransfer,
        AttachmentTransfer.AttachmentTransfer.of({
          upload: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          download: ({ reference }) => Effect.fail(new Attachment.AttachmentUnavailable({ digest: reference.digest }))
        })
      )
      const layerAttachments = AttachmentClient.layer({
        maximumLocalBytes: 64,
        maximumLocalObjects: 8,
        maximumCacheBytes: 64,
        maximumCacheObjects: 8,
        maximumCacheAge: "1 day",
        evictionBatchSize: 4
      }).pipe(
        Layer.provide(layerStorage),
        Layer.provide(layerTransfer),
        Layer.provide(layerDatabase)
      )
      const layerOfflineSync = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          discard: () => Effect.die("unexpected discard"),
          pull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          watch: () => Stream.never
        })
      )
      const layerAttachmentReplica = SqlReplica.layer({
        ...clientHistory,
        definition,
        initialSpaces: [spaceId],
        clientId,
        retryDelay: "10 millis"
      }).pipe(
        Layer.provide(layerAttachments),
        Layer.provide(layerOfflineSync),
        Layer.provide(layerDatabase),
        Layer.provide(layerHandlers),
        Layer.tap((context) => {
          const replica = Context.get(context, Replica.Replica)
          return replica.space(spaceId).pipe(
            Effect.flatMap((space) => space.stageAttachment(Stream.make(bytes))),
            Effect.flatMap((reference) => Deferred.succeed(availableReady, reference))
          )
        })
      )
      const graph = BrowserReplica.make(layerAttachmentReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      yield* AtomRegistry.getResult(registry, graph.spaces)
      const available = yield* Deferred.await(availableReady)
      const unavailable = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"2".repeat(64)}`),
        bytes: bytes.length
      })
      const overflowing = Attachment.Reference.make({
        _tag: "Attachment",
        digest: available.digest,
        bytes: available.bytes + 1
      })
      const attachment = graph.attachment(spaceId, available)
      const unmount = registry.mount(attachment)
      yield* Effect.addFinalizer(() => Effect.sync(unmount))

      const placeholder = registry.get(attachment)
      assert.strictEqual(placeholder._tag, "Initial")
      assert.isTrue(placeholder.waiting)
      yield* Deferred.succeed(allowRead, undefined)
      assert.deepStrictEqual(
        yield* AtomRegistry.getResult(registry, attachment, { suspendOnWaiting: true }),
        bytes
      )
      const equivalent = Attachment.Reference.make({
        _tag: "Attachment",
        digest: available.digest,
        bytes: available.bytes
      })
      assert.strictEqual(graph.attachment(spaceId, equivalent), attachment)
      const unavailableAtom = graph.attachment(spaceId, unavailable)
      const failure = yield* Effect.exit(AtomRegistry.getResult(registry, unavailableAtom))
      assert.isTrue(Exit.isFailure(failure))
      if (Exit.isFailure(failure)) {
        const error = Option.getOrThrow(Cause.findErrorOption(failure.cause))
        assert.strictEqual(error._tag, "AttachmentUnavailable")
      }
      const overflowAtom = graph.attachment(spaceId, overflowing)
      const overflow = yield* Effect.exit(AtomRegistry.getResult(registry, overflowAtom))
      assert.isTrue(Exit.isFailure(overflow))
      if (Exit.isFailure(overflow)) {
        const error = Option.getOrThrow(Cause.findErrorOption(overflow.cause))
        assert.strictEqual(error._tag, "AttachmentLengthMismatch")
        if (error._tag === "AttachmentLengthMismatch") {
          assert.strictEqual(error.expected, available.bytes)
          assert.strictEqual(error.actual, overflowing.bytes)
        }
      }
    }, Effect.scoped)
  )

  it.effect(
    "reacts to pending mutation submission and settlement",
    Effect.fnUntraced(function*() {
      const faultsReady = yield* Deferred.make<FaultInjection.Service>()
      const graph = BrowserReplica.make(faultedReplica(faultsReady))
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const pending = graph.pendingFor(spaceId, PutTodo)
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountPending = registry.mount(pending)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountPending()
        })
      )
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, pending), [])
      const faults = yield* Deferred.await(faultsReady)

      const id = `pending-atom-${Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")}`
      yield* faults.holdNextReceipt(spaceId)
      const submitted = yield* AtomRegistry.toStreamResult(registry, pending).pipe(
        Stream.filter((items) =>
          items.some((item) => item.payload.id === id && item.submissionState === "Submitting" && item.attempts > 0)
        ),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      registry.set(mutation, { id, title: "0-pending" })
      yield* faults.awaitReceiptCommitted(spaceId)
      const pendingItems = Option.getOrThrow(yield* Fiber.join(submitted))
      const item = pendingItems.find((candidate) => candidate.payload.id === id)
      assert.isDefined(item)
      assert.deepStrictEqual(item.payload, { id, title: "0-pending" })
      assert.strictEqual(item.submissionState, "Submitting")
      assert.strictEqual(item.attempts, 1)

      const settled = yield* AtomRegistry.toStreamResult(registry, pending).pipe(
        Stream.filter((items) => !items.some((candidate) => candidate.payload.id === id)),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* faults.releaseHeldReceipt(spaceId)
      yield* faults.awaitReceiptReturned(spaceId)
      const settledItems = Option.getOrThrow(yield* Fiber.join(settled))
      assert.isFalse(settledItems.some((candidate) => candidate.payload.id === id))
    }, Effect.scoped)
  )

  it.effect(
    "reruns only an indexed query whose result range can change",
    Effect.fnUntraced(function*() {
      rangeReads.clear()
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const related = graph.query(spaceId, RangeTodos)({ lower: "a", upper: "m" })
      const unrelated = graph.query(spaceId, RangeTodos)({ lower: "n", upper: "z" })
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountRelated = registry.mount(related)
      const unmountUnrelated = registry.mount(unrelated)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountUnrelated()
          unmountRelated()
        })
      )
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, related), [])
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, unrelated), [])
      assert.strictEqual(rangeReads.get("a:m"), 1)
      assert.strictEqual(rangeReads.get("n:z"), 1)

      registry.set(mutation, { id: "range", title: "beta" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, related), [{ id: "range", title: "beta" }])
      assert.deepStrictEqual(
        yield* AtomRegistry.getResult(registry, unrelated, { suspendOnWaiting: true }),
        []
      )
      assert.isAtLeast(rangeReads.get("a:m") ?? 0, 2)
      assert.strictEqual(rangeReads.get("n:z"), 1)
    })
  )

  it.effect(
    "refreshes an entity atom when its key has a different encoded representation",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const entity = graph.entity(spaceId, Numbered)(1)
      const mutation = graph.mutation(spaceId, PutNumbered)
      const unmountEntity = registry.mount(entity)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountEntity()
        })
      )
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, entity)))
      registry.set(mutation, { id: 1, value: "encoded-key" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      ;(yield* AtomRegistry.getResult(registry, entity)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: 1,
            value: "encoded-key"
          })
      )
    })
  )

  it.effect(
    "does not rerun an unrelated mounted entity atom",
    Effect.fnUntraced(function*() {
      let unrelatedReads = 0
      const layerObserved = Effect.gen(function*() {
        const service = yield* Replica.Replica
        return Replica.Replica.of({
          ...service,
          space: (address) =>
            service.space(address).pipe(Effect.map((space) => ({
              ...space,
              get: (model, key) => {
                if (model.name === Todo.name && Object.is(key, "unrelated")) unrelatedReads++
                return space.get(model, key)
              }
            })))
        })
      }).pipe(
        Layer.effect(Replica.Replica),
        Layer.provideMerge(layerReplica)
      )
      const graph = BrowserReplica.make(layerObserved)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const changed = graph.entity(spaceId, Todo)("changed")
      const unrelated = graph.entity(spaceId, Todo)("unrelated")
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountChanged = registry.mount(changed)
      const unmountUnrelated = registry.mount(unrelated)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountUnrelated()
          unmountChanged()
        })
      )
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, changed)))
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, unrelated)))
      assert.strictEqual(unrelatedReads, 1)

      registry.set(mutation, { id: "changed", title: "new" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      ;(yield* AtomRegistry.getResult(registry, changed)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: "changed",
            title: "new"
          })
      )
      assert.isTrue(Option.isNone(
        yield* AtomRegistry.getResult(registry, unrelated, { suspendOnWaiting: true })
      ))
      assert.strictEqual(unrelatedReads, 1)
    })
  )

  it.effect(
    "does not refresh membership or another space status for an addressed write",
    Effect.fnUntraced(function*() {
      let spacesReads = 0
      let secondStatusReads = 0
      const layerObserved = Effect.gen(function*() {
        const service = yield* Replica.Replica
        return Replica.Replica.of({
          ...service,
          spaces: Effect.suspend(() => {
            spacesReads++
            return service.spaces
          }),
          space: (address) =>
            service.space(address).pipe(Effect.map((space) => {
              if (address !== secondSpaceId) return space
              return {
                ...space,
                status: Effect.suspend(() => {
                  secondStatusReads++
                  return space.status
                })
              }
            }))
        })
      }).pipe(
        Layer.effect(Replica.Replica),
        Layer.provideMerge(layerReplica)
      )
      const graph = BrowserReplica.make(layerObserved)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const membership = graph.spaces
      const unrelatedStatus = graph.status(secondSpaceId)
      const mutation = graph.mutation(spaceId, PutTodo)
      const mounted = [
        registry.mount(membership),
        registry.mount(unrelatedStatus),
        registry.mount(mutation)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))
      assert.lengthOf(yield* AtomRegistry.getResult(registry, membership), 2)
      assert.strictEqual((yield* AtomRegistry.getResult(registry, unrelatedStatus)).spaceId, secondSpaceId)
      const spacesBefore = spacesReads
      const secondStatusBefore = secondStatusReads

      registry.set(mutation, { id: "addressed-write", title: "first" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      assert.strictEqual(spacesReads, spacesBefore)
      assert.strictEqual(secondStatusReads, secondStatusBefore)
    })
  )

  it.effect(
    "reacts to per-space scope and activation commands",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const scopeAtom = graph.scope(spaceId)
      const activationAtom = graph.activation(spaceId)
      const setScope = graph.setScope(spaceId)
      const activate = graph.activate(spaceId)
      const deactivate = graph.deactivate(spaceId)
      const mounted = [
        registry.mount(scopeAtom),
        registry.mount(activationAtom),
        registry.mount(setScope),
        registry.mount(activate),
        registry.mount(deactivate)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))

      registry.set(deactivate, undefined)
      yield* AtomRegistry.getResult(registry, deactivate, { suspendOnWaiting: true })
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Inactive")

      const empty = Protocol.ReplicationScope.make({ models: [] })
      registry.set(setScope, empty)
      yield* AtomRegistry.getResult(registry, setScope, { suspendOnWaiting: true })
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, scopeAtom), empty)
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Active")

      registry.set(deactivate, undefined)
      yield* AtomRegistry.getResult(registry, deactivate, { suspendOnWaiting: true })
      registry.set(activate, undefined)
      yield* AtomRegistry.getResult(registry, activate, { suspendOnWaiting: true })
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Active")
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, scopeAtom), empty)
    }, Effect.scoped)
  )

  it("uses the shared runtime factory by default and preserves an explicit factory", () => {
    const graph = BrowserReplica.make(layerReplica)
    assert.strictEqual(graph.factory, Atom.runtime)

    const factory = Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })
    const customGraph = BrowserReplica.make(layerReplica, { factory })
    assert.strictEqual(customGraph.factory, factory)
  })

  it("normalizes the configured idle duration once when the graph is constructed", () => {
    let reads = 0
    const idleTTL = {
      get milliseconds() {
        reads++
        return 17
      }
    } satisfies Duration.Input
    const graph = BrowserReplica.make(layerReplica, { idleTTL })
    const readsAfterConstruction = reads

    assert.isAbove(readsAfterConstruction, 0)
    assert.strictEqual(graph.entity(spaceId, Todo)("1").idleTTL, 17)
    assert.strictEqual(graph.query(spaceId, ListTodos)(undefined).idleTTL, 17)
    const receipt = graph.receipt(
      spaceId,
      PutTodo,
      Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
    )
    assert.strictEqual(receipt.idleTTL, 17)
    assert.strictEqual(reads, readsAfterConstruction)
  })

  it.live(
    "runs mutation, entity, query, receipt, and status state through one reactive runtime",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica, {
        factory: Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })
      })
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const entity = graph.entity(spaceId, Todo)("1")
      const query = graph.query(spaceId, ListTodos)(undefined)
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountEntity = registry.mount(entity)
      const unmountQuery = registry.mount(query)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountQuery()
          unmountEntity()
        })
      )
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, entity)))
      pipe(
        (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
        (todos) => assert.deepStrictEqual(todos, [])
      )
      registry.set(mutation, { id: "1", title: "atom" })
      const pending = yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      ;(yield* AtomRegistry.getResult(registry, entity)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: "1",
            title: "atom"
          })
      )
      assert.strictEqual(pending.envelope.name, PutTodo.name)
      const receipt = graph.receipt(spaceId, PutTodo, pending.envelope.mutationId)
      const accepted = Option.getOrThrow(Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, receipt).pipe(
          Stream.filter(Option.isSome),
          Stream.runHead
        )
      ))
      assert.strictEqual(accepted._tag, "Accepted")
      pipe(
        (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
        (todos) => assert.deepStrictEqual(todos, [{ id: "1", title: "atom" }])
      )
      const status = yield* AtomRegistry.getResult(registry, graph.status(spaceId))
      assert.strictEqual(status._tag, "Online")
    })
  )

  it.effect(
    "keeps addressed atoms isolated and shares membership through one runtime",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const firstEntity = graph.entity(spaceId, Todo)("shared")
      const secondEntity = graph.entity(secondSpaceId, Todo)("shared")
      const firstQuery = graph.query(spaceId, ListTodos)(undefined)
      const secondQuery = graph.query(secondSpaceId, ListTodos)(undefined)
      const firstMutation = graph.mutation(spaceId, PutTodo)
      const secondMutation = graph.mutation(secondSpaceId, PutTodo)
      const mounted = [
        registry.mount(firstEntity),
        registry.mount(secondEntity),
        registry.mount(firstQuery),
        registry.mount(secondQuery),
        registry.mount(firstMutation),
        registry.mount(secondMutation),
        registry.mount(graph.spaces),
        registry.mount(graph.aggregateStatus),
        registry.mount(graph.join),
        registry.mount(graph.leave)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))

      registry.set(firstMutation, { id: "shared", title: "first" })
      registry.set(secondMutation, { id: "shared", title: "second" })
      const [firstPending, secondPending] = yield* Effect.all([
        AtomRegistry.getResult(registry, firstMutation, { suspendOnWaiting: true }),
        AtomRegistry.getResult(registry, secondMutation, { suspendOnWaiting: true })
      ])
      const firstValue = Option.getOrThrow(
        yield* AtomRegistry.getResult(registry, firstEntity, { suspendOnWaiting: true })
      )
      const secondValue = Option.getOrThrow(
        yield* AtomRegistry.getResult(registry, secondEntity, { suspendOnWaiting: true })
      )
      assert.strictEqual(firstValue.title, "first")
      assert.strictEqual(secondValue.title, "second")
      pipe(
        (yield* AtomRegistry.getResult(registry, firstQuery, { suspendOnWaiting: true })).filter((todo) =>
          todo.id === "shared"
        ),
        (todos) =>
          assert.deepStrictEqual(todos, [{
            id: "shared",
            title: "first"
          }])
      )
      pipe(
        (yield* AtomRegistry.getResult(registry, secondQuery, { suspendOnWaiting: true })).filter((todo) =>
          todo.id === "shared"
        ),
        (todos) =>
          assert.deepStrictEqual(todos, [{
            id: "shared",
            title: "second"
          }])
      )

      const awaitReceipt = (address: Identity.SpaceId, mutationId: Identity.MutationId) =>
        AtomRegistry.toStreamResult(registry, graph.receipt(address, PutTodo, mutationId)).pipe(
          Stream.filter(Option.isSome),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
          Effect.map(Option.getOrThrow)
        )
      const [firstReceipt, secondReceipt] = yield* Effect.all([
        awaitReceipt(spaceId, firstPending.envelope.mutationId),
        awaitReceipt(secondSpaceId, secondPending.envelope.mutationId)
      ], { concurrency: "unbounded" })
      assert.strictEqual(firstReceipt.spaceId, spaceId)
      assert.strictEqual(secondReceipt.spaceId, secondSpaceId)
      const status = yield* AtomRegistry.getResult(registry, graph.status(spaceId))
      assert.strictEqual(status.spaceId, spaceId)
      assert.strictEqual((yield* AtomRegistry.getResult(registry, graph.aggregateStatus)).spaces, 2)

      registry.set(graph.join, thirdSpaceId)
      const joinedSpaces = Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, graph.spaces).pipe(
          Stream.filter((spaces) => spaces.some((space) => space.spaceId === thirdSpaceId)),
          Stream.runHead
        )
      )
      assert.deepStrictEqual(joinedSpaces.map((space) => space.spaceId), [spaceId, secondSpaceId, thirdSpaceId])

      registry.set(graph.leave, secondSpaceId)
      const remainingSpaces = Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, graph.spaces).pipe(
          Stream.filter((spaces) => !spaces.some((space) => space.spaceId === secondSpaceId)),
          Stream.runHead
        )
      )
      assert.deepStrictEqual(remainingSpaces.map((space) => space.spaceId), [spaceId, thirdSpaceId])
      const error = yield* AtomRegistry.getResult(registry, secondEntity).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SpaceNotJoined")
    })
  )
})
