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
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as VirtualTime from "./fixtures/VirtualTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000801")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000801")
const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000801")

const constructors = ["layer", "layerWorkflow"] as const

type Constructor = typeof constructors[number]

type Remote = SyncEngine.SyncEngine["Service"]

const idleRemote = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  transportGeneration: Effect.succeed(0),
  waitForTransportChange: () => Effect.never,
  submitBatch: () => Effect.never,
  discard: () => Effect.die("unexpected discard"),
  pull: () => Effect.never,
  bootstrap: () => Effect.die("unexpected bootstrap"),
  watch: () => Stream.never
})

const acceptSubmission: Remote["submitBatch"] = (request) =>
  Effect.succeed(Protocol.SubmitBatchResult.make({
    receipts: request.envelopes.map((envelope) =>
      Protocol.AcceptedReceipt.make({
        ...envelope,
        serverSequence: Identity.ServerSequence.make(1),
        result: Domain.todo(envelope.mutationId, "accepted")
      })
    )
  }))

const emptyPage = (crypto: Crypto.Crypto, request: Parameters<Remote["pull"]>[0]) =>
  Protocol.viewChangesDigest([]).pipe(
    Effect.map((digest) =>
      Protocol.PullPage.make({
        scopeGeneration: request.scopeGeneration,
        cursor: Protocol.ReplicationCursor.make({
          viewId,
          revision: Identity.ReplicationViewRevision.make((request.cursor?.revision ?? 0) + 1)
        }),
        serverSequence: Identity.ServerSequence.make(1),
        changes: [],
        contentBytes: Protocol.encodedBytes([]),
        digest,
        hasMore: false,
        serverSchema: Domain.definition.schemaIdentity
      })
    ),
    Effect.provideService(Crypto.Crypto, crypto)
  )

const makeAttempts = Effect.gen(function*() {
  const reached = [
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>(),
    yield* Deferred.make<void>()
  ]
  let count = 0
  const record = Effect.suspend(() => {
    count += 1
    const signal = reached[count - 1]
    if (signal === undefined) return Effect.void
    return Deferred.succeed(signal, undefined).pipe(Effect.asVoid)
  })
  return {
    record,
    count: () => count,
    reached: (attempt: 1 | 2 | 3 | 4) => Deferred.await(reached[attempt - 1])
  }
})

const backgroundServices = Effect.fnUntraced(function*(constructor: Constructor) {
  const databaseContext = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    WorkflowEngine.layerMemory
  ).pipe(Layer.build)
  const sql = Context.get(databaseContext, SqlClient.SqlClient)
  let lockedStatement: string | undefined
  const lockingSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const source: unknown = args[0]
      if (lockedStatement === undefined || !Array.isArray(source) || !source.join("?").includes(lockedStatement)) {
        return Reflect.apply(target, thisArg, args)
      }
      lockedStatement = undefined
      const reason = new SqlError.LockTimeoutError({ cause: "injected lock timeout" })
      return Effect.fail(new SqlError.SqlError({ reason }))
    }
  })
  const crypto = Context.get(databaseContext, Crypto.Crypto)
  const reactivity = Context.get(databaseContext, Reactivity.Reactivity)
  const options = {
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceId],
    defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    retryDelay: "1 second",
    maximumRetryDelay: "1 second"
  } satisfies SqlReplica.Options<typeof Domain.definition>
  const lockingContext = Context.add(databaseContext, SqlClient.SqlClient, lockingSql)
  const start = (remote: Remote) => {
    const layerServices = Layer.mergeAll(
      Domain.layerHandlers,
      Layer.succeed(SyncEngine.SyncEngine, remote),
      Layer.succeedContext(lockingContext)
    )
    let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
    if (constructor === "layerWorkflow") {
      layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
    }
    return Layer.build(layerReplica).pipe(Effect.map(Context.get(Replica.Replica)))
  }
  const lockNext = (statement: string) => {
    lockedStatement = statement
  }
  return { sql, crypto, reactivity, start, lockNext }
})

const pendingBackgroundSpace = Effect.fnUntraced(function*(constructor: Constructor) {
  const services = yield* backgroundServices(constructor)
  const seedScope = yield* Scope.make()
  const seedReplica = yield* services.start(idleRemote).pipe(Scope.provide(seedScope))
  const seedSpace = yield* seedReplica.space(spaceId)
  yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("pending"))
  yield* seedSpace.deactivate
  yield* Scope.close(seedScope, Exit.void)
  yield* services.sql`UPDATE effect_local_client_spaces
    SET replication_view_id = ${viewId}, replication_view_revision = 0`
  return services
})

const pendingCountStatement = "SELECT COUNT(p.mutation_id) AS count"

const awaitAggregate = Effect.fnUntraced(function*(
  replica: Replica.Replica["Service"],
  reactivity: Reactivity.Reactivity,
  matches: (aggregate: ReplicaStatus.Aggregate) => boolean
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  let aggregate = yield* replica.status
  while (!matches(aggregate)) {
    yield* Queue.take(changes)
    aggregate = yield* replica.status
  }
  return aggregate
})

const awaitActivation = Effect.fnUntraced(function*(
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  activation: Replica.Activation
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.activation(space.spaceId)], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  while ((yield* space.activation) !== activation) yield* Queue.take(changes)
})

const protocolInvalid = new ReplicaError.ProtocolInvalid({ message: "rejected by the server" })

const workflowRetryAfterRelease = Effect.fnUntraced(function*() {
  const services = yield* backgroundServices("layerWorkflow")
  const attempts = yield* makeAttempts
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    pull: () => {
      if (attempts.count() === 0) {
        return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
      }
      return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
    }
  }))
  const space = yield* replica.space(spaceId)
  yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
  yield* attempts.reached(1)
  yield* space.deactivate
  yield* attempts.reached(2)
  yield* awaitSpaceStatus(space, services.reactivity, "Failed")
  return { services, attempts, replica, space }
})

const awaitSpaceStatusWhere = Effect.fnUntraced(function*(
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  matches: (status: ReplicaStatus.SpaceStatus) => boolean
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.status(space.spaceId), ReactivityKey.aggregateStatus], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  let status = yield* space.status
  while (!matches(status)) {
    yield* Queue.take(changes)
    status = yield* space.status
  }
  return status
})

const awaitSpaceStatus = (
  space: Replica.Space,
  reactivity: Reactivity.Reactivity,
  tag: ReplicaStatus.ReplicaStatus["_tag"]
) => awaitSpaceStatusWhere(space, reactivity, (status) => status._tag === tag)

const corruptPending = (sql: SqlClient.SqlClient) =>
  sql`UPDATE effect_local_client_pending_data SET digest = 'x' || digest`

const repairPending = (sql: SqlClient.SqlClient) =>
  sql`UPDATE effect_local_client_pending_data SET digest = substr(digest, 2)`

describe("background sync terminal failures", () => {
  it.effect.each(constructors)(
    "stops retrying a background space whose storage is corrupt and reports it as failed with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      yield* corruptPending(services.sql)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => Effect.andThen(attempts.record, emptyPage(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      assert.strictEqual(status.pending, 1)
      if (status._tag === "Failed") assert.strictEqual(status.message, "StorageCorrupt")
      assert.strictEqual(yield* space.activation, "Inactive")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.state, "Failed")
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(aggregate.counts.idle, 0)
      assert.strictEqual(aggregate.totalPending, 1)
    }, Effect.scoped)
  )

  it.effect.each(["ProtocolInvalid", "StaleSchema"] as const)(
    "stops retrying a background space whose sync fails with %s",
    Effect.fnUntraced(function*(tag) {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      let failure: ReplicaError.ReplicaError = new ReplicaError.ProtocolInvalid({ message: "rejected by the server" })
      if (tag === "StaleSchema") {
        failure = new ReplicaError.StaleSchema({
          expectedVersion: 2,
          expectedHash: "expected",
          actualVersion: 1,
          actualHash: "actual"
        })
      }
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failure))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, tag)
    }, Effect.scoped)
  )

  it.effect.each(constructors)(
    "keeps retrying a background space whose sync fails transiently with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
      }))
      yield* attempts.reached(1)
      assert.strictEqual(attempts.count(), 1)

      yield* VirtualTime.advanceUntil(attempts.reached(3))

      assert.strictEqual(attempts.count(), 3)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.totalPending, 1)
    }, Effect.scoped)
  )

  it.effect.each(constructors)(
    "drains a background space after one lock timeout on its storage with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      services.lockNext("SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id")
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => Effect.andThen(attempts.record, emptyPage(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)

      const idleWithoutPending = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Idle" && status.pending === 0
      ).pipe(Effect.scoped)

      const drained = yield* VirtualTime.advanceUntil(idleWithoutPending).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(drained))
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.totalPending, 0)
    }, Effect.scoped)
  )

  it.effect(
    "retries a terminally failed background space once each time it is activated and released",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      yield* corruptPending(services.sql)
      const attempts = yield* makeAttempts
      let observe = true
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (!observe) return Effect.never
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      observe = false
      yield* space.activate
      yield* space.deactivate
      observe = true
      yield* attempts.reached(2)
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 2)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, "StorageCorrupt")
    }, Effect.scoped)
  )

  it.effect(
    "recovers a terminally failed background space when it is activated after its storage is repaired",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      yield* corruptPending(services.sql)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => Effect.andThen(attempts.record, emptyPage(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      yield* repairPending(services.sql)
      yield* space.activate

      const status = yield* awaitSpaceStatus(space, services.reactivity, "Online")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.totalPending, 0)

      yield* space.deactivate
      assert.strictEqual((yield* space.status)._tag, "Idle")
      assert.strictEqual((yield* replica.status).counts.idle, 1)
    }, Effect.scoped)
  )

  it.effect(
    "notifies subscribers of the space status once the terminal failure is visible",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      yield* corruptPending(services.sql)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      const notified: Array<"aggregate" | "status"> = []
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const aggregate = services.reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
            notified.push("aggregate")
          })
          const status = services.reactivity.registerUnsafe([ReactivityKey.status(spaceId)], () => {
            notified.push("status")
          })
          return () => {
            aggregate()
            status()
          }
        }),
        (unregister) => Effect.sync(unregister)
      )

      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      assert.strictEqual(notified.at(-1), "status")
    }, Effect.scoped)
  )

  it.effect(
    "shows a terminally failed background space as connecting while it is active again",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => {
          if (attempts.count() > 0) return Effect.never
          return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      yield* space.activate

      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.connecting, 1)
      assert.strictEqual(aggregate.counts.idle, 0)
      assert.strictEqual(aggregate.counts.failed, 0)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a terminally failed background space failed when its activation fails",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      yield* services.sql`UPDATE effect_local_client_spaces SET desired_scope_json = '{"models":["Missing"]}'`

      const activation = yield* Effect.result(space.activate)

      assert.strictEqual(activation._tag, "Failure")
      const status = yield* space.status
      assert.strictEqual(status._tag, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, "ProtocolInvalid")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(aggregate.counts.idle, 0)
      assert.strictEqual(attempts.count(), 1)
    }, Effect.scoped)
  )

  it.effect(
    "runs another background turn when the release bookkeeping of a failed turn hits a lock timeout",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      services.lockNext(pendingCountStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      assert.strictEqual(status.pending, 1)
    }, Effect.scoped)
  )

  it.effect(
    "drops the retry scheduled for failed release bookkeeping once a later turn fails terminally",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      services.lockNext(pendingCountStatement)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() < 2) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(3)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(4)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isNone(retried))
      assert.strictEqual(attempts.count(), 3)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a background space failed after a workflow attempt on it fails and releases it",
    Effect.fnUntraced(function*() {
      const { services, attempts, replica, space } = yield* workflowRetryAfterRelease()

      yield* VirtualTime.advanceUntil(attempts.reached(3))
      const inactive = awaitActivation(space, services.reactivity, "Inactive").pipe(Effect.scoped)
      yield* VirtualTime.advanceUntil(inactive)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      const aggregate = yield* replica.status

      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(aggregate.counts.idle, 0)
      const status = yield* space.status
      assert.strictEqual(status._tag, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, "ProtocolInvalid")
      assert.strictEqual(yield* space.activation, "Inactive")
    }, Effect.scoped)
  )

  it.effect(
    "runs another background turn when releasing a workflow attempt hits a lock timeout",
    Effect.fnUntraced(function*() {
      const { services, attempts, replica } = yield* workflowRetryAfterRelease()
      services.lockNext(pendingCountStatement)
      const failedAgain = awaitAggregate(
        replica,
        services.reactivity,
        (aggregate) => attempts.count() >= 4 && aggregate.counts.failed === 1
      ).pipe(Effect.scoped)

      const settled = yield* VirtualTime.advanceUntil(failedAgain).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(settled))
    }, Effect.scoped)
  )

  it.effect(
    "stops waiting for a new credential when the space is activated",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const waitInterrupted = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Effect.onInterrupt(Effect.never, () => Deferred.succeed(waitInterrupted, undefined)),
        pull: () => {
          if (attempts.count() > 0) return Effect.never
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      yield* space.activate

      const interrupted = yield* Deferred.await(waitInterrupted).pipe(Effect.timeoutOption("1 minute"))
      assert.isTrue(Option.isSome(interrupted))
    }, Effect.scoped)
  )

  it.effect(
    "stops waiting for a new credential when the space is left",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const waitInterrupted = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Effect.onInterrupt(Effect.never, () => Deferred.succeed(waitInterrupted, undefined)),
        pull: () => Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      yield* replica.leave(spaceId)

      const interrupted = yield* Deferred.await(waitInterrupted).pipe(Effect.timeoutOption("1 minute"))
      assert.isTrue(Option.isSome(interrupted))
    }, Effect.scoped)
  )

  it.effect(
    "reports a background space as idle again when its retry after a new credential fails transiently",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => Deferred.await(credentialChanged),
        pull: () => {
          if (attempts.count() > 0) {
            return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
          }
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      yield* Deferred.succeed(credentialChanged, undefined)
      yield* attempts.reached(2)

      const status = yield* awaitSpaceStatus(space, services.reactivity, "Idle")
      assert.strictEqual(status.pending, 1)
      assert.strictEqual((yield* replica.status).counts.needsAuthentication, 0)
    }, Effect.scoped)
  )

  it.effect(
    "waits for a new credential instead of retrying a background space whose credential was rejected",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      const waitedGenerations: Array<number> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: (generation) => {
          waitedGenerations.push(generation)
          return Deferred.await(credentialChanged)
        },
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() > 0) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const paused = yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      assert.strictEqual(paused.pending, 1)
      assert.deepStrictEqual(waitedGenerations, [7])

      yield* Deferred.succeed(credentialChanged, undefined)
      yield* attempts.reached(2)
      const settled = yield* awaitSpaceStatus(space, services.reactivity, "Idle")
      assert.strictEqual(settled.pending, 0)
      assert.strictEqual((yield* replica.status).counts.needsAuthentication, 0)
    }, Effect.scoped)
  )
})
