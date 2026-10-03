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
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as Struct from "effect/Struct"
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
  let heldRelease:
    | { remaining: number; readonly entered: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
    | undefined
  const gatedReactivity = new Proxy(reactivity, {
    get: (target, property, receiver) => {
      if (property !== "invalidate") return Reflect.get(target, property, receiver)
      return (keys: Parameters<typeof reactivity.invalidate>[0]) => {
        const held = heldRelease
        if (held === undefined || !Array.isArray(keys) || !keys.includes(ReactivityKey.activation(spaceId))) {
          return target.invalidate(keys)
        }
        held.remaining -= 1
        if (held.remaining > 0) return target.invalidate(keys)
        heldRelease = undefined
        return target.invalidate(keys).pipe(
          Effect.andThen(Deferred.succeed(held.entered, undefined)),
          Effect.andThen(Deferred.await(held.release))
        )
      }
    }
  })
  const lockingContext = databaseContext.pipe(
    Context.add(SqlClient.SqlClient, lockingSql),
    Context.add(Reactivity.Reactivity, gatedReactivity)
  )
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
  const holdRelease = Effect.fnUntraced(function*() {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const arm = () => {
      heldRelease = { remaining: 2, entered, release }
    }
    return { arm, entered: Deferred.await(entered), release: Deferred.succeed(release, undefined) }
  })
  return { sql, crypto, reactivity, start, lockNext, holdRelease }
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

type FailureTag = ReplicaError.ReplicaError["_tag"]

const failures: { readonly [Tag in FailureTag]: Extract<ReplicaError.ReplicaError, { readonly _tag: Tag }> } = {
  StorageUnavailable: new ReplicaError.StorageUnavailable({ cause: "injected" }),
  StorageCorrupt: new ReplicaError.StorageCorrupt({ message: "injected" }),
  CanonicalEncodeError: new ReplicaError.CanonicalEncodeError({ cause: "injected" }),
  DefinitionMismatch: new ReplicaError.DefinitionMismatch({ expected: "expected", actual: "actual" }),
  StaleSchema: new ReplicaError.StaleSchema({
    expectedVersion: 2,
    expectedHash: "expected",
    actualVersion: 1,
    actualHash: "actual"
  }),
  SchemaGenerationConflict: new ReplicaError.SchemaGenerationConflict({ expected: 2, actual: 1 }),
  SchemaEvolutionUnsupported: new ReplicaError.SchemaEvolutionUnsupported({
    sourceVersion: 1,
    sourceHash: "source",
    targetVersion: 2,
    targetHash: "target"
  }),
  SchemaEvolutionFailed: new ReplicaError.SchemaEvolutionFailed({
    stepId: null,
    componentKind: "Model",
    componentName: "Todo",
    part: "Value",
    fromVersion: 1,
    toVersion: 2,
    cause: "injected"
  }),
  StorageMigrationMismatch: new ReplicaError.StorageMigrationMismatch({ catalog: "Client", message: "injected" }),
  StorageMigrationPending: new ReplicaError.StorageMigrationPending({ catalog: "Client", message: "injected" }),
  SchemaKeyCollision: new ReplicaError.SchemaKeyCollision({ model: "Todo", key: "key" }),
  PendingMutationEvolutionRejected: new ReplicaError.PendingMutationEvolutionRejected({
    mutationId: "mutation",
    rejection: null
  }),
  ReplicaIdentityMismatch: new ReplicaError.ReplicaIdentityMismatch({
    expectedClientId: "expected",
    actualClientId: "actual"
  }),
  SpaceNotJoined: new ReplicaError.SpaceNotJoined({ spaceId }),
  SpaceUnavailable: new ReplicaError.SpaceUnavailable({ spaceId }),
  EphemeralSessionUnavailable: new ReplicaError.EphemeralSessionUnavailable({
    spaceId,
    clientId,
    membershipIncarnation: "incarnation"
  }),
  MutationIdentityConflict: new ReplicaError.MutationIdentityConflict({ mutationId: "mutation" }),
  QuarantineResubmissionConflict: new ReplicaError.QuarantineResubmissionConflict({ mutationId: "mutation" }),
  OutOfOrderMutation: new ReplicaError.OutOfOrderMutation({ expected: 2, actual: 1 }),
  CursorGap: new ReplicaError.CursorGap({ expected: 2, actual: 1 }),
  SettlementReplayTruncated: new ReplicaError.SettlementReplayTruncated({ requested: 1, oldestAvailable: 2 }),
  StaleReplicationScope: new ReplicaError.StaleReplicationScope({ expected: 2, actual: 1 }),
  SnapshotUnavailable: new ReplicaError.SnapshotUnavailable({ snapshotId: "snapshot" }),
  CapacityExceeded: new ReplicaError.CapacityExceeded({ resource: "read authorizations", limit: 1 }),
  InvalidConfiguration: new ReplicaError.InvalidConfiguration({ option: "option", message: "injected" }),
  UnknownCommitOutcome: new ReplicaError.UnknownCommitOutcome({ mutationId: "mutation", cause: "injected" }),
  ProtocolInvalid: new ReplicaError.ProtocolInvalid({ message: "rejected by the server" }),
  UpgradeRequired: new ReplicaError.UpgradeRequired({ clientVersions: [1], serverVersions: [2] }),
  ProtocolVersionRejected: new ReplicaError.ProtocolVersionRejected({ version: 1, serverVersions: [2] }),
  ServerUnavailable: new ReplicaError.ServerUnavailable(),
  CredentialRejected: new ReplicaError.CredentialRejected({}),
  AuthenticatorUnavailable: new ReplicaError.AuthenticatorUnavailable(),
  OperationTimeout: new ReplicaError.OperationTimeout({ operation: "pull", timeoutMillis: 1 }),
  AuthorizationDenied: new ReplicaError.AuthorizationDenied({ reason: null }),
  OwnerUnavailable: new ReplicaError.OwnerUnavailable({ reason: "transport" }),
  BuildSuperseded: new ReplicaError.BuildSuperseded({ version: 1, supersedingVersion: 2 })
}

const retryingTags: ReadonlyArray<FailureTag> = [
  "ServerUnavailable",
  "OperationTimeout",
  "AuthenticatorUnavailable",
  "StorageUnavailable",
  "UnknownCommitOutcome",
  "CapacityExceeded",
  "OwnerUnavailable"
]

const stoppingTags = Struct.keys(failures).filter((tag) => !retryingTags.includes(tag))

const schedulerCases = (tags: ReadonlyArray<FailureTag>) =>
  tags.flatMap((tag) => constructors.map((constructor) => [tag, constructor] as const))

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

const protocolInvalid = failures.ProtocolInvalid

const workflowRetryAfterRelease = Effect.fnUntraced(function*(workflowRetry: "fails" | "succeeds") {
  const services = yield* backgroundServices("layerWorkflow")
  const attempts = yield* makeAttempts
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    submitBatch: acceptSubmission,
    pull: (request) => {
      if (attempts.count() === 0) {
        return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
      }
      if (attempts.count() === 1 || workflowRetry === "fails") {
        return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
      }
      return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
    }
  }))
  const space = yield* replica.space(spaceId)
  yield* services.sql`UPDATE effect_local_client_spaces
    SET replication_view_id = ${viewId}, replication_view_revision = 0`
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

  it.effect.each(schedulerCases(stoppingTags))(
    "stops a background space after one attempt that fails with %s on %s",
    Effect.fnUntraced(function*([tag, constructor]) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failures[tag]))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatusWhere(space, services.reactivity, (current) => current._tag !== "Idle")
      if (tag === "CredentialRejected") {
        assert.strictEqual(status._tag, "NeedsAuthentication")
      } else {
        assert.strictEqual(status._tag, "Failed")
        if (status._tag === "Failed") assert.strictEqual(status.message, tag)
      }
    }, Effect.scoped)
  )

  it.effect.each(schedulerCases(retryingTags))(
    "keeps retrying a background space whose sync fails with %s on %s",
    Effect.fnUntraced(function*([tag, constructor]) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failures[tag]))
      }))
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(retried))
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.totalPending, 1)
    }, Effect.scoped)
  )

  it.effect.each(schedulerCases(stoppingTags))(
    "stops a foreground space after one attempt that fails with %s on %s",
    Effect.fnUntraced(function*([tag, constructor]) {
      const services = yield* backgroundServices(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failures[tag]))
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
    }, Effect.scoped)
  )

  it.effect.each(schedulerCases(retryingTags))(
    "keeps retrying a foreground space whose sync fails with %s on %s",
    Effect.fnUntraced(function*([tag, constructor]) {
      const services = yield* backgroundServices(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failures[tag]))
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(retried))
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

  it.effect.each(constructors)(
    "submits again and drains after the server reports an unknown commit outcome with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      let submissions = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: (request) => {
          submissions += 1
          if (submissions > 1) return acceptSubmission(request)
          return Effect.fail(
            new ReplicaError.UnknownCommitOutcome({
              mutationId: request.envelopes[0].mutationId,
              cause: "injected commit failure"
            })
          )
        },
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      const idleWithoutPending = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Idle" && status.pending === 0
      ).pipe(Effect.scoped)

      const drained = yield* VirtualTime.advanceUntil(idleWithoutPending).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(submissions, 2)
      assert.isTrue(Option.isSome(drained))
    }, Effect.scoped)
  )

  it.effect.each(constructors)(
    "retries a background turn whose final status read hits a lock timeout with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() === 1) {
            services.lockNext("SELECT COUNT(*) AS count FROM effect_local_client_pending_data")
          }
          return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
        }
      }))
      yield* attempts.reached(2)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(retried))
      assert.strictEqual((yield* replica.status).counts.failed, 0)
    }, Effect.scoped)
  )

  it.effect(
    "stops a background space whose membership row is gone from storage",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
      }))
      yield* attempts.reached(1)
      const idle = awaitAggregate(replica, services.reactivity, (aggregate) => aggregate.counts.idle === 1)
      yield* Effect.scoped(idle)
      yield* services.sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`

      const failed = awaitAggregate(replica, services.reactivity, (aggregate) => aggregate.counts.failed === 1)
      const stopped = yield* VirtualTime.advanceUntil(Effect.scoped(failed)).pipe(Effect.timeoutOption("1 minute"))
      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(stopped))
      assert.isTrue(Option.isNone(retried))
      assert.strictEqual(attempts.count(), 1)
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

  it.effect.each(constructors)(
    "recovers a terminally failed background space when it is activated after its storage is repaired with %s",
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

  it.effect.each(
    [
      ["layer", "ProtocolInvalid"],
      ["layer", "CredentialRejected"],
      ["layerWorkflow", "ProtocolInvalid"],
      ["layerWorkflow", "CredentialRejected"]
    ] as const
  )(
    "ignores a background failure that arrives after the foreground took the space over with %s and %s",
    Effect.fnUntraced(function*([constructor, tag]) {
      const services = yield* pendingBackgroundSpace(constructor)
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let failure: ReplicaError.ReplicaError = protocolInvalid
      if (tag === "CredentialRejected") failure = new ReplicaError.CredentialRejected({ credentialGeneration: 7 })
      let pulls = 0
      let credentialWaits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => {
          credentialWaits += 1
          return Effect.never
        },
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls > 1) return emptyPage(services.crypto, request)
          return Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Effect.fail(failure)),
            Effect.uninterruptible
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* Deferred.await(held)
      const activation = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(activation)
      const online = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Online" && status.pending === 0
      ).pipe(Effect.scoped)
      yield* VirtualTime.advanceUntil(online)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      yield* space.deactivate

      assert.strictEqual((yield* space.status)._tag, "Idle")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.idle, 1)
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.counts.needsAuthentication, 0)
      assert.strictEqual(credentialWaits, 0)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a failed background space failed when the foreground does not reconcile it",
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

      yield* space.activate
      yield* attempts.reached(4)
      yield* space.deactivate

      const status = yield* space.status
      assert.strictEqual(status._tag, "Failed")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.failed, 1)
      assert.strictEqual(aggregate.counts.idle, 0)
    }, Effect.scoped)
  )

  it.effect.each(
    [
      ["layer", "ProtocolInvalid"],
      ["layer", "CredentialRejected"],
      ["layerWorkflow", "ProtocolInvalid"],
      ["layerWorkflow", "CredentialRejected"]
    ] as const
  )(
    "ignores a background failure settled after the foreground reconciled and released the space with %s and %s",
    Effect.fnUntraced(function*([constructor, tag]) {
      const services = yield* pendingBackgroundSpace(constructor)
      const bookkeeping = yield* services.holdRelease()
      let failure: ReplicaError.ReplicaError = protocolInvalid
      if (tag === "CredentialRejected") failure = new ReplicaError.CredentialRejected({ credentialGeneration: 7 })
      let pulls = 0
      let credentialWaits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => {
          credentialWaits += 1
          return Effect.never
        },
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls > 1) return emptyPage(services.crypto, request)
          bookkeeping.arm()
          return Effect.fail(failure)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* bookkeeping.entered
      assert.strictEqual(yield* space.activation, "Inactive")
      yield* space.activate
      const online = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Online" && status.pending === 0
      ).pipe(Effect.scoped)
      yield* VirtualTime.advanceUntil(online)
      yield* space.deactivate
      assert.strictEqual((yield* space.status)._tag, "Idle")

      yield* bookkeeping.release
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual((yield* space.status)._tag, "Idle")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.idle, 1)
      assert.strictEqual(aggregate.counts.failed, 0)
      assert.strictEqual(aggregate.counts.needsAuthentication, 0)
      assert.strictEqual(credentialWaits, 0)
    }, Effect.scoped)
  )

  it.effect(
    "drops a scheduled background retry once the foreground takes the space over",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
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
      yield* attempts.reached(1)
      yield* awaitActivation(space, services.reactivity, "Inactive")
      yield* space.activate
      yield* attempts.reached(2)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isNone(retried))
      assert.strictEqual(attempts.count(), 2)
      assert.strictEqual(yield* space.activation, "Active")
    }, Effect.scoped)
  )

  it.effect(
    "ignores a background failure settled after a workflow attempt drained and released the space",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow")
      const attempts = yield* makeAttempts
      const bookkeeping = yield* services.holdRelease()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() === 0) {
            return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
          }
          if (attempts.count() > 1) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          bookkeeping.arm()
          return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
      yield* attempts.reached(1)
      yield* space.deactivate
      yield* bookkeeping.entered
      const drained = awaitAggregate(replica, services.reactivity, (aggregate) => aggregate.totalPending === 0)
      yield* VirtualTime.advanceUntil(Effect.scoped(drained))
      const idle = awaitAggregate(replica, services.reactivity, (aggregate) => aggregate.counts.idle === 1)
      yield* VirtualTime.advanceUntil(Effect.scoped(idle))

      yield* bookkeeping.release
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      const status = yield* space.status
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.idle, 1)
      assert.strictEqual(aggregate.counts.failed, 0)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a background space failed after a workflow attempt on it fails and releases it",
    Effect.fnUntraced(function*() {
      const { services, attempts, replica, space } = yield* workflowRetryAfterRelease("fails")

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
    "reports a failed background space as idle once a workflow attempt on it drains its mutations",
    Effect.fnUntraced(function*() {
      const { services, attempts, replica, space } = yield* workflowRetryAfterRelease("succeeds")

      yield* VirtualTime.advanceUntil(attempts.reached(3))
      const drained = awaitAggregate(replica, services.reactivity, (aggregate) => aggregate.totalPending === 0)
      yield* VirtualTime.advanceUntil(Effect.scoped(drained))
      const inactive = awaitActivation(space, services.reactivity, "Inactive").pipe(Effect.scoped)
      yield* VirtualTime.advanceUntil(inactive)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      const status = yield* space.status
      assert.strictEqual(status._tag, "Idle")
      assert.strictEqual(status.pending, 0)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.idle, 1)
      assert.strictEqual(aggregate.counts.failed, 0)
    }, Effect.scoped)
  )

  it.effect(
    "leaves the status to the runtime while a workflow attempt still holds a space whose background turn failed",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow")
      const attempts = yield* makeAttempts
      const release = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => {
          if (attempts.count() === 0) {
            return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
          }
          if (attempts.count() > 1) return Effect.andThen(attempts.record, Effect.never)
          return attempts.record.pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
      yield* attempts.reached(1)
      yield* space.deactivate
      yield* attempts.reached(2)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      yield* Deferred.succeed(release, undefined)
      yield* VirtualTime.advanceUntil(attempts.reached(3))

      assert.strictEqual(yield* space.activation, "Active")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.counts.idle, 0)
      assert.strictEqual(aggregate.counts.offline, 1)
    }, Effect.scoped)
  )

  it.effect(
    "ignores a failed release of a workflow attempt once the foreground took the space over",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow")
      const attempts = yield* makeAttempts
      const release = yield* services.holdRelease()
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (attempts.count() === 0) {
            return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
          }
          if (attempts.count() === 1) return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
          if (attempts.count() > 2) return emptyPage(services.crypto, request)
          release.arm()
          return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
      yield* attempts.reached(1)
      yield* space.deactivate
      yield* attempts.reached(2)
      yield* VirtualTime.advanceUntil(release.entered)
      yield* space.activate
      const online = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Online" && status.pending === 0
      ).pipe(Effect.scoped)
      yield* VirtualTime.advanceUntil(online)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const pullsBefore = pulls

      services.lockNext(pendingCountStatement)
      yield* release.release
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(pulls, pullsBefore)
      assert.strictEqual(yield* space.activation, "Active")
      assert.strictEqual((yield* space.status)._tag, "Online")
    }, Effect.scoped)
  )

  it.effect(
    "runs another background turn when releasing a workflow attempt hits a lock timeout",
    Effect.fnUntraced(function*() {
      const { services, attempts, replica } = yield* workflowRetryAfterRelease("fails")
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
    "stops waiting for a new credential once the foreground reconciles the space",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const waitInterrupted = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Effect.onInterrupt(Effect.never, () => Deferred.succeed(waitInterrupted, undefined)),
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() > 0) return emptyPage(services.crypto, request)
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      yield* space.activate

      const interrupted = yield* VirtualTime.advanceUntil(Deferred.await(waitInterrupted)).pipe(
        Effect.timeoutOption("1 minute")
      )
      assert.isTrue(Option.isSome(interrupted))
    }, Effect.scoped)
  )

  it.effect(
    "stops waiting for a new credential when a later background turn fails for another reason",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const waitInterrupted = yield* Deferred.make<void>()
      let foreground = false
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () =>
          Effect.onInterrupt(Effect.never, () => Deferred.succeed(waitInterrupted, undefined)),
        pull: () => {
          if (foreground) return Effect.never
          if (attempts.count() > 0) return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")

      foreground = true
      yield* space.activate
      yield* space.deactivate
      foreground = false
      yield* attempts.reached(2)
      yield* awaitSpaceStatus(space, services.reactivity, "Failed")

      const interrupted = yield* VirtualTime.advanceUntil(Deferred.await(waitInterrupted)).pipe(
        Effect.timeoutOption("1 minute")
      )
      assert.isTrue(Option.isSome(interrupted))
    }, Effect.scoped)
  )

  it.effect(
    "starts no credential wait for a background turn that ran while the foreground owned the space",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const credentialChanged = yield* Deferred.make<void>()
      let credentialWaits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => {
          credentialWaits += 1
          return Deferred.await(credentialChanged)
        },
        pull: () => {
          if (attempts.count() === 1 || attempts.count() > 2) {
            return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
          }
          return Effect.andThen(
            attempts.record,
            Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* awaitSpaceStatus(space, services.reactivity, "NeedsAuthentication")
      yield* space.activate
      yield* attempts.reached(2)

      yield* Deferred.succeed(credentialChanged, undefined)
      yield* attempts.reached(3)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(yield* space.activation, "Active")
      assert.strictEqual(credentialWaits, 1)
      assert.strictEqual(attempts.count(), 3)
    }, Effect.scoped)
  )

  it.effect(
    "starts no credential wait when the space is left while its background turn fails",
    Effect.fnUntraced(function*() {
      const services = yield* pendingBackgroundSpace("layer")
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let credentialWaits = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        waitForCredentialChange: () => {
          credentialWaits += 1
          return Effect.never
        },
        pull: () =>
          Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 7 }))),
            Effect.uninterruptible
          )
      }))
      yield* Deferred.await(held)
      const leaving = yield* Effect.forkChild(replica.leave(spaceId), { startImmediately: true })
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(leaving)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(credentialWaits, 0)
      assert.strictEqual((yield* replica.status).spaces, 0)
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
