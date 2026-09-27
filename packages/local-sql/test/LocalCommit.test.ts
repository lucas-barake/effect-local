import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const layerMemoryDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerServer = ServerStore.layerTrusted({
  definition: Domain.definition,
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
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(Layer.merge(layerMemoryDatabase, NodeCrypto.layer))
)

const makeRemote = Effect.fnUntraced(function*() {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const submitted = yield* Queue.unbounded<Protocol.MutationEnvelope>()
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) =>
      Queue.offerAll(submitted, request.envelopes).pipe(Effect.andThen(server.admitBatch(request, null))),
    discard: (request) => server.discard(request, null),
    pull: server.pull,
    bootstrap: server.bootstrap,
    watch: server.watch
  })
  return { remote, submitted }
})

const makeInvalidationProbe = Effect.fnUntraced(function*(target: string) {
  const base = Context.get(yield* Layer.build(Reactivity.layer), Reactivity.Reactivity)
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const service = Reactivity.Reactivity.of({
    ...base,
    invalidate: (keys) => {
      if (!Array.isArray(keys) || !keys.includes(target)) return base.invalidate(keys)
      return Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.andThen(base.invalidate(keys))
      )
    }
  })
  return { service, entered, release }
})

const makeRecordingProbe = Effect.fnUntraced(function*(target: string) {
  const probe = yield* makeInvalidationProbe(target)
  const invalidations: Array<ReadonlyArray<unknown>> = []
  const service = Reactivity.Reactivity.of({
    ...probe.service,
    invalidate: (keys) => {
      if (Array.isArray(keys)) invalidations.push(keys)
      return probe.service.invalidate(keys)
    }
  })
  return { ...probe, service, invalidations }
})

const localStore = Effect.fnUntraced(function*(
  reactivity: Layer.Layer<Reactivity.Reactivity>,
  onMutationsCommitted: LocalStore.Options["onMutationsCommitted"] = () => Effect.void
) {
  const database = yield* Layer.build(
    Layer.mergeAll(
      ConnectionLane.makeLayer().pipe(Layer.provideMerge(layerMemoryDatabase)),
      NodeCrypto.layer,
      reactivity
    )
  )
  const layerLocalStore = LocalStore.layer({
    definition: Domain.definition,
    spaceId,
    clientId,
    scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration,
    onMutationsCommitted
  })
  const context = yield* Layer.build(layerLocalStore.pipe(
    Layer.provide(layerRuntime),
    Layer.provide(QueryReactivity.layer),
    Layer.provide(Layer.succeedContext(database))
  ))
  return { local: Context.get(context, LocalStore.Store), sql: Context.get(database, SqlClient.SqlClient) }
})

const layerReplica = (remote: SyncEngine.Service) =>
  SqlReplica.layer({
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceId],
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration,
    retryDelay: "1 minute",
    maximumRetryDelay: "1 minute"
  }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
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

describe("local commit", () => {
  it.effect(
    "submits a committed mutation whose caller is interrupted before the mutation returns",
    Effect.fnUntraced(function*() {
      const { remote, submitted } = yield* makeRemote()
      const probe = yield* makeInvalidationProbe(ReactivityKey.entity(spaceId, Domain.Todo.name, "interrupted-caller"))
      const layerProbe = Layer.succeed(Reactivity.Reactivity, probe.service)
      const database = yield* Layer.build(Layer.mergeAll(layerMemoryDatabase, NodeCrypto.layer, layerProbe))
      const context = yield* Layer.build(layerReplica(remote).pipe(Layer.provide(Layer.succeedContext(database))))
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.activate
      yield* awaitStatus(Context.get(database, Reactivity.Reactivity), space, (status) => status._tag === "Online")

      const caller = yield* space.mutate(Domain.PutTodo, Domain.todo("interrupted-caller")).pipe(Effect.forkChild)
      yield* Deferred.await(probe.entered)
      const interruption = yield* Fiber.interrupt(caller).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(probe.release, undefined)
      yield* Fiber.join(interruption)

      const envelope = yield* Queue.take(submitted)
      assert.strictEqual(envelope.name, Domain.PutTodo.name)
      assert.deepStrictEqual(envelope.payload, Domain.todo("interrupted-caller"))
    })
  )

  it.effect(
    "commits mutations queued behind a running commit together and invalidates them in one round",
    Effect.fnUntraced(function*() {
      const probe = yield* makeRecordingProbe(ReactivityKey.entity(spaceId, Domain.Todo.name, "running"))
      const { local } = yield* localStore(Layer.succeed(Reactivity.Reactivity, probe.service))
      const running = yield* local.mutate(Domain.PutTodo, Domain.todo("running")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(probe.entered)
      const ids = ["queued-1", "queued-2", "queued-3", "queued-4"]
      const queued = yield* Effect.forEach(
        ids,
        (id) => local.mutate(Domain.PutTodo, Domain.todo(id)).pipe(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.succeed(probe.release, undefined)
      yield* Fiber.join(running)
      const committed = yield* Effect.forEach(queued, Fiber.join)

      const queuedKeys = ids.map((id) => ReactivityKey.entity(spaceId, Domain.Todo.name, id))
      const rounds = probe.invalidations.filter((keys) => queuedKeys.some((key) => keys.includes(key)))
      assert.strictEqual(rounds.length, 1)
      assert.isTrue(queuedKeys.every((key) => rounds[0].includes(key)))
      assert.deepStrictEqual(committed.map((pending) => pending.envelope.localSequence), [2, 3, 4, 5])
      assert.strictEqual(yield* local.pendingCount, 5)
    }, Effect.scoped)
  )

  it.effect(
    "runs a sync step only after the local mutations admitted before it have committed",
    Effect.fnUntraced(function*() {
      const probe = yield* makeRecordingProbe(ReactivityKey.entity(spaceId, Domain.Todo.name, "first"))
      const { local } = yield* localStore(Layer.succeed(Reactivity.Reactivity, probe.service))
      const first = yield* local.mutate(Domain.PutTodo, Domain.todo("first")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(probe.entered)
      const second = yield* local.mutate(Domain.PutTodo, Domain.todo("second")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      const submission = yield* local.pendingToSubmit.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(probe.release, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      const pending = yield* Fiber.join(submission)
      assert.deepStrictEqual(pending.map((item) => item.envelope.payload), [
        Domain.todo("first"),
        Domain.todo("second")
      ])
    }, Effect.scoped)
  )

  it.effect(
    "keeps sync steps runnable after callers are interrupted at every point of admitting a mutation",
    Effect.fnUntraced(function*() {
      const { local } = yield* localStore(Reactivity.layer)
      for (let yields = 0; yields < 48; yields++) {
        const caller = yield* local.mutate(Domain.PutTodo, Domain.todo(`interrupted-${yields}`)).pipe(
          Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
          Effect.forkChild({ startImmediately: true })
        )
        for (let step = 0; step < yields; step++) yield* Effect.yieldNow
        yield* Fiber.interrupt(caller)
      }
      const last = yield* local.mutate(Domain.PutTodo, Domain.todo("after-interruptions"))
      const pending = yield* local.pendingToSubmit
      assert.strictEqual(pending.at(-1)?.envelope.mutationId, last.envelope.mutationId)
    }, Effect.scoped)
  )

  it.effect(
    "reports every queued mutation's outcome as what is durable when one aborts the whole transaction",
    Effect.fnUntraced(function*() {
      const probe = yield* makeRecordingProbe(ReactivityKey.entity(spaceId, Domain.Todo.name, "running"))
      const { local, sql } = yield* localStore(Layer.succeed(Reactivity.Reactivity, probe.service))
      const poison = Identity.MutationId.make("mut_00000000-0000-4000-8000-00000000dead")
      yield* sql.unsafe(`CREATE TRIGGER abort_poisoned_mutation BEFORE INSERT ON effect_local_client_pending_data
        WHEN NEW.mutation_id = '${poison}' BEGIN SELECT RAISE(ROLLBACK, 'poisoned'); END`)
      const running = yield* local.mutate(Domain.PutTodo, Domain.todo("running")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(probe.entered)
      const before = yield* local.mutate(Domain.PutTodo, Domain.todo("before")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      const poisoned = yield* local.mutate(Domain.PutTodo, Domain.todo("poisoned"), { mutationId: poison }).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      const after = yield* local.mutate(Domain.PutTodo, Domain.todo("after")).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.succeed(probe.release, undefined)
      yield* Fiber.join(running)
      const outcomes = {
        before: Exit.isSuccess(yield* Fiber.join(before)),
        poisoned: Exit.isSuccess(yield* Fiber.join(poisoned)),
        after: Exit.isSuccess(yield* Fiber.join(after))
      }
      const durable = (yield* local.pending).map((item) => item.envelope.payload)
      assert.deepStrictEqual(outcomes, { before: true, poisoned: false, after: true })
      assert.deepStrictEqual(durable, [Domain.todo("running"), Domain.todo("before"), Domain.todo("after")])
    }, Effect.scoped)
  )

  it.effect(
    "reports a committed mutation as committed when scheduling reconciliation afterwards fails",
    Effect.fnUntraced(function*() {
      const { local } = yield* localStore(
        Reactivity.layer,
        () => Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId }))
      )
      const committed = yield* local.mutate(Domain.PutTodo, Domain.todo("committed")).pipe(Effect.exit)
      assert.isTrue(Exit.isSuccess(committed))
      assert.strictEqual(yield* local.pendingCount, 1)
    }, Effect.scoped)
  )

  it.effect(
    "schedules reconciliation again when a recorded mutation is retried with its id",
    Effect.fnUntraced(function*() {
      const scheduled = yield* Ref.make<ReadonlyArray<number>>([])
      const { local } = yield* localStore(
        Reactivity.layer,
        (pending) => Ref.update(scheduled, (counts) => [...counts, pending])
      )
      const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000042")
      yield* local.mutate(Domain.PutTodo, Domain.todo("retried"), { mutationId })
      yield* local.mutate(Domain.PutTodo, Domain.todo("retried"), { mutationId })
      assert.deepStrictEqual(yield* Ref.get(scheduled), [1, 1])
    }, Effect.scoped)
  )

  it.effect(
    "runs a sync step only after earlier admitted mutations commit when a later mutation aborts their batch",
    Effect.fnUntraced(function*() {
      const probe = yield* makeRecordingProbe(ReactivityKey.entity(spaceId, Domain.Todo.name, "running"))
      const { local, sql } = yield* localStore(Layer.succeed(Reactivity.Reactivity, probe.service))
      const poison = Identity.MutationId.make("mut_00000000-0000-4000-8000-00000000dead")
      yield* sql.unsafe(`CREATE TRIGGER abort_poisoned_mutation BEFORE INSERT ON effect_local_client_pending_data
        WHEN NEW.mutation_id = '${poison}' BEGIN SELECT RAISE(ROLLBACK, 'poisoned'); END`)
      const running = yield* local.mutate(Domain.PutTodo, Domain.todo("running")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(probe.entered)
      const before = yield* local.mutate(Domain.PutTodo, Domain.todo("before")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      const submission = yield* local.pendingToSubmit.pipe(Effect.forkChild({ startImmediately: true }))
      const poisoned = yield* local.mutate(Domain.PutTodo, Domain.todo("poisoned"), { mutationId: poison }).pipe(
        Effect.exit,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.succeed(probe.release, undefined)
      yield* Fiber.join(running)
      yield* Fiber.join(before)
      yield* Fiber.join(poisoned)
      const pending = yield* Fiber.join(submission)
      const payloads = pending.map((item) => item.envelope.payload)
      assert.deepStrictEqual(payloads, [Domain.todo("running"), Domain.todo("before")])
    }, Effect.scoped)
  )
})
