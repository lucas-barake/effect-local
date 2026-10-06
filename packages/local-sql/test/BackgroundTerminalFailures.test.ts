import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import type * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import type * as Reactivity from "effect/reactivity/Reactivity"
import type * as SqlClient from "effect/sql/SqlClient"
import * as Stream from "effect/Stream"
import * as Struct from "effect/Struct"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as BackgroundReplica from "./fixtures/BackgroundReplica.js"
import {
  acceptSubmission,
  awaitSpaceStatus,
  awaitSpaceStatusWhere,
  type Constructor,
  constructors,
  emptyPage,
  idleRemote,
  makeAttempts,
  type Remote,
  viewId
} from "./fixtures/BackgroundReplica.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000801")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000801")

const oversizedSnapshot = (crypto: Crypto.Crypto, request: Parameters<Remote["pull"]>[0]) =>
  Protocol.replicationScopeDigest(request.scope).pipe(
    Effect.map((scopeDigest) =>
      Protocol.BootstrapRequired.make({
        manifest: {
          spaceId: request.spaceId,
          clientId: request.clientId,
          definitionHash: Domain.definition.hash,
          schema: Domain.definition.schemaIdentity,
          scopeDigest,
          scopeGeneration: request.scopeGeneration,
          cursor: Protocol.ReplicationCursor.make({ viewId, revision: Identity.ReplicationViewRevision.make(1) }),
          snapshotId: Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000801"),
          sequence: Identity.ServerSequence.make(1),
          terminalSequenceThrough: Identity.TerminalSequence.make(0),
          entityCount: 10_001,
          contentBytes: 1,
          digest: Protocol.initialSnapshotDigest
        },
        serverSchema: Domain.definition.schemaIdentity
      })
    ),
    Effect.provideService(Crypto.Crypto, crypto)
  )

const backgroundServices = (constructor: Constructor, maximumRetryDelay: Duration.Input = "1 second") =>
  BackgroundReplica.services({
    constructor,
    clientId,
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retryDelay: "1 second",
    maximumRetryDelay
  })

const pendingBackgroundSpace = (constructor: Constructor, maximumRetryDelay: Duration.Input = "1 second") =>
  backgroundServices(constructor, maximumRetryDelay).pipe(
    Effect.tap((services) => BackgroundReplica.seedPending(services, [spaceId]))
  )

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
  BuildSuperseded: new ReplicaError.BuildSuperseded({ version: 1, supersedingVersion: 2 }),
  UnexpectedFailure: new ReplicaError.UnexpectedFailure({ message: "injected", cause: "injected" })
}

const retryingTags: ReadonlyArray<FailureTag> = [
  "ServerUnavailable",
  "OperationTimeout",
  "AuthenticatorUnavailable",
  "StorageUnavailable",
  "UnknownCommitOutcome",
  "CapacityExceeded",
  "OwnerUnavailable",
  "UnexpectedFailure"
]

const temporaryCapacity = new Set<ReplicaError.CapacityResource>([
  "read authorizations",
  "sync watchers",
  "sync watchers per principal",
  "server receipts",
  "server history",
  "bootstrap authorizations",
  "bootstrap pages",
  "ephemeral join verifications",
  "ephemeral watchers",
  "ephemeral watchers per principal",
  "ephemeral spaces",
  "ephemeral members",
  "ephemeral bytes per space",
  "ephemeral event keys per space",
  "ephemeral state keys per space",
  "ephemeral events",
  "pending mutations"
])

const stoppingTags = Struct.keys(failures).filter((tag) => !retryingTags.includes(tag))

const foregroundStoppingTags: ReadonlyArray<FailureTag> = [
  "ProtocolInvalid",
  "CredentialRejected",
  "AuthorizationDenied"
]

const foregroundRetryingTags: ReadonlyArray<FailureTag> = ["ServerUnavailable", "StorageUnavailable"]

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

const workflowBacksOff = VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 millis"))

const workflowRetryAfterRelease = Effect.fnUntraced(function*(workflowRetry: "fails" | "succeeds") {
  const services = yield* backgroundServices("layerWorkflow")
  const attempts = yield* makeAttempts
  const backgroundMayStart = yield* Deferred.make<void>()
  const replica = yield* services.start(SyncEngine.SyncEngine.of({
    ...idleRemote,
    transportGeneration: Effect.as(Deferred.await(backgroundMayStart), 0),
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
  yield* workflowBacksOff
  yield* Deferred.succeed(backgroundMayStart, undefined)
  yield* attempts.reached(2)
  yield* awaitSpaceStatus(space, services.reactivity, "Failed")
  return { services, attempts, replica, space }
})

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
    }, VirtualTime.scoped)
  )

  it.effect.each(stoppingTags)(
    "stops a background space after one attempt that fails with %s",
    Effect.fnUntraced(function*(tag) {
      const services = yield* pendingBackgroundSpace("layer")
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
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "stops a background space whose server snapshot exceeds the bootstrap limit with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: (request) => Effect.andThen(attempts.record, oversizedSnapshot(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, "CapacityExceeded")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "stops a foreground space whose server snapshot exceeds the bootstrap limit with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* backgroundServices(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: (request) => Effect.andThen(attempts.record, oversizedSnapshot(services.crypto, request))
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(attempts.count(), 1)
      assert.isTrue(Option.isNone(retried))
      const status = yield* awaitSpaceStatus(space, services.reactivity, "Failed")
      if (status._tag === "Failed") assert.strictEqual(status.message, "CapacityExceeded")
    }, VirtualTime.scoped)
  )

  it.effect.each(constructors)(
    "retries and drains a background space after the server was briefly at capacity with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* pendingBackgroundSpace(constructor)
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() > 0) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          return Effect.andThen(attempts.record, Effect.fail(failures.CapacityExceeded))
        }
      }))
      const space = yield* replica.space(spaceId)
      const idleWithoutPending = awaitSpaceStatusWhere(
        space,
        services.reactivity,
        (status) => status._tag === "Idle" && status.pending === 0
      ).pipe(Effect.scoped)

      const drained = yield* VirtualTime.advanceUntil(idleWithoutPending).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.scoped)
  )

  it.effect.each(ReplicaError.CapacityResource.literals)(
    "retries a background space at capacity for %s only when the capacity can free up on its own",
    Effect.fnUntraced(function*(resource) {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const failure = new ReplicaError.CapacityExceeded({ resource, limit: 1 })
      yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failure))
      }))
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(2)).pipe(Effect.timeoutOption("1 minute"))

      assert.strictEqual(Option.isSome(retried), temporaryCapacity.has(resource))
    }, VirtualTime.scoped)
  )

  it.effect.each(
    [
      ["layer", "background"],
      ["layerWorkflow", "background"],
      ["layer", "foreground"],
      ["layerWorkflow", "foreground"]
    ] as const
  )(
    "backs off a retryable failure as slowly as an unreachable server with %s in the %s",
    Effect.fnUntraced(function*([constructor, mode]) {
      const attemptTimes = Effect.fnUntraced(function*(failure: ReplicaError.ReplicaError) {
        const services = yield* pendingBackgroundSpace(constructor, "1 minute")
        const times: Array<number> = []
        const replica = yield* services.start(SyncEngine.SyncEngine.of({
          ...idleRemote,
          pull: () =>
            Clock.currentTimeMillis.pipe(
              Effect.tap((now) => Effect.sync(() => times.push(now))),
              Effect.andThen(Effect.fail(failure))
            )
        }))
        if (mode === "foreground") yield* (yield* replica.space(spaceId)).activate
        yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("2 minutes"))
        return times.map((time) => time - times[0])
      }, Effect.scoped)

      const retryable = yield* attemptTimes(failures.UnknownCommitOutcome)
      const unreachable = yield* attemptTimes(failures.ServerUnavailable)

      assert.deepStrictEqual(retryable, unreachable)
      const gaps = retryable.slice(1).map((time, index) => time - retryable[index])
      assert.deepStrictEqual(gaps.filter((gap) => gap > 0), [1000, 2000, 4000, 8000, 16_000, 32_000])
    }, VirtualTime.provide)
  )

  it.effect.each(schedulerCases(foregroundRetryingTags))(
    "grows the delay between watch subscriptions when every watch wakes once and then fails with %s on %s",
    Effect.fnUntraced(function*([tag, constructor]) {
      const services = yield* backgroundServices(constructor, "1 minute")
      const subscribedAt: Array<number> = []
      let pulls = 0
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        watch: (request) =>
          Stream.fromEffect(
            Effect.map(Clock.currentTimeMillis, (now) => {
              subscribedAt.push(now)
              return Protocol.Wake.make({ spaceId: request.spaceId })
            })
          ).pipe(Stream.concat(Stream.fail(failures[tag]))),
        pull: (request) => {
          pulls += 1
          return emptyPage(services.crypto, request)
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate

      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("10 minutes"))

      const gaps = subscribedAt.slice(1).map((time, index) => (time - subscribedAt[index]) / 1000)
      assert.deepStrictEqual(gaps, [1, 2, 4, 8, 16, 32, 60, 60, 60, 60, 60, 60, 60, 60])
      assert.isAtMost(pulls, 2 * subscribedAt.length + 2)
    }, VirtualTime.scoped),
    60_000
  )

  it.effect.each(constructors)(
    "reconnects a watch quickly when it fails after it stayed open for hours with %s",
    Effect.fnUntraced(function*(constructor) {
      const services = yield* backgroundServices(constructor, "1 minute")
      const subscribedAt: Array<number> = []
      const wake = (request: Parameters<Remote["watch"]>[0]) =>
        Stream.fromEffect(
          Effect.map(Clock.currentTimeMillis, (now) => {
            subscribedAt.push(now)
            return Protocol.Wake.make({ spaceId: request.spaceId })
          })
        )
      const unavailable = Stream.fail(failures.StorageUnavailable)
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        submitBatch: acceptSubmission,
        watch: (request) => {
          if (subscribedAt.length < 5) return Stream.concat(wake(request), unavailable)
          if (subscribedAt.length > 5) return Stream.concat(wake(request), Stream.never)
          const open = Stream.fromEffect(Effect.sleep("3 hours")).pipe(Stream.drain)
          return wake(request).pipe(Stream.concat(open), Stream.concat(unavailable))
        },
        pull: (request) => emptyPage(services.crypto, request)
      }))
      const space = yield* replica.space(spaceId)
      yield* space.activate

      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("4 hours"))

      assert.strictEqual(subscribedAt.length, 7)
      const healthyFor = Duration.toMillis(Duration.hours(3))
      assert.strictEqual(subscribedAt[6] - subscribedAt[5], healthyFor + 1000)
    }, VirtualTime.scoped),
    60_000
  )

  it.effect.each(retryingTags)(
    "keeps retrying a background space whose sync fails with %s",
    Effect.fnUntraced(function*(tag) {
      const services = yield* pendingBackgroundSpace("layer")
      const attempts = yield* makeAttempts
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        pull: () => Effect.andThen(attempts.record, Effect.fail(failures[tag]))
      }))
      yield* attempts.reached(1)

      const retried = yield* VirtualTime.advanceUntil(attempts.reached(3)).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(retried))
      const aggregate = yield* replica.status
      let reportedFailed = 0
      if (tag === "UnexpectedFailure") reportedFailed = 1
      assert.strictEqual(aggregate.counts.failed, reportedFailed)
      assert.strictEqual(aggregate.totalPending, 1)
    }, VirtualTime.scoped)
  )

  it.effect.each(schedulerCases(foregroundStoppingTags))(
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
    }, VirtualTime.scoped)
  )

  it.effect.each(schedulerCases(foregroundRetryingTags))(
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
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
          bookkeeping.arm(2)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )

  it.effect(
    "ignores a background failure settled after a workflow attempt drained and released the space",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow")
      const attempts = yield* makeAttempts
      const bookkeeping = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
      const backgroundMayStart = yield* Deferred.make<void>()
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.as(Deferred.await(backgroundMayStart), 0),
        submitBatch: acceptSubmission,
        pull: (request) => {
          if (attempts.count() === 0) {
            return Effect.andThen(attempts.record, Effect.fail(new ReplicaError.ServerUnavailable()))
          }
          if (attempts.count() > 1) return Effect.andThen(attempts.record, emptyPage(services.crypto, request))
          bookkeeping.arm(2)
          return Effect.andThen(attempts.record, Effect.fail(protocolInvalid))
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
      yield* attempts.reached(1)
      yield* space.deactivate
      yield* workflowBacksOff
      yield* Deferred.succeed(backgroundMayStart, undefined)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )

  it.effect(
    "replaces the retry of a failed turn when releasing the workflow attempt that outlived it then fails",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow", "1 minute")
      const attempts = yield* makeAttempts
      const release = yield* Deferred.make<void>()
      const backgroundMayStart = yield* Deferred.make<void>()
      const times: Array<number> = []
      const replica = yield* services.start(SyncEngine.SyncEngine.of({
        ...idleRemote,
        transportGeneration: Effect.as(Deferred.await(backgroundMayStart), 0),
        pull: () => {
          const unavailable = Effect.fail(new ReplicaError.ServerUnavailable())
          if (attempts.count() === 0) return Effect.andThen(attempts.record, unavailable)
          if (attempts.count() === 1) {
            return attempts.record.pipe(Effect.andThen(Deferred.await(release)), Effect.andThen(unavailable))
          }
          return attempts.record.pipe(
            Effect.andThen(Clock.currentTimeMillis),
            Effect.tap((now) => Effect.sync(() => times.push(now))),
            Effect.andThen(unavailable)
          )
        }
      }))
      const space = yield* replica.space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending"))
      yield* attempts.reached(1)
      yield* space.deactivate
      yield* workflowBacksOff
      yield* Deferred.succeed(backgroundMayStart, undefined)
      yield* attempts.reached(2)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const before = yield* Clock.currentTimeMillis
      services.lockNext(pendingCountStatement)
      yield* Deferred.succeed(release, undefined)
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("2 minutes"))
      const sinceRelease = times.map((time) => time - before)
      assert.strictEqual(sinceRelease[0], 0)
      assert.isTrue(sinceRelease.includes(2000))
      assert.deepStrictEqual(sinceRelease.filter((elapsed) => elapsed > 0 && elapsed < 2000), [])
    }, VirtualTime.scoped)
  )

  it.effect(
    "ignores a failed release of a workflow attempt once the foreground took the space over",
    Effect.fnUntraced(function*() {
      const services = yield* backgroundServices("layerWorkflow")
      const attempts = yield* makeAttempts
      const release = yield* services.holdInvalidation(ReactivityKey.activation(spaceId))
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
          release.arm(2)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )

  it.effect.each(["ProtocolInvalid", "ServerUnavailable"] as const)(
    "stops waiting for a new credential when a later background turn fails with %s",
    Effect.fnUntraced(function*(tag) {
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
          if (attempts.count() > 0) return Effect.andThen(attempts.record, Effect.fail(failures[tag]))
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

      const interrupted = yield* VirtualTime.advanceUntil(Deferred.await(waitInterrupted)).pipe(
        Effect.timeoutOption("1 minute")
      )
      assert.isTrue(Option.isSome(interrupted))
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
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
    }, VirtualTime.scoped)
  )
})
