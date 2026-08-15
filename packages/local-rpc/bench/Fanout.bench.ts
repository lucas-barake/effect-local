import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { afterAll, assert, beforeAll, bench } from "vitest"
import * as EphemeralHub from "../src/EphemeralHub.js"
import * as PrincipalAssertion from "../src/PrincipalAssertion.js"
import * as SpaceEntity from "../src/SpaceEntity.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction, effect/noGlobals -- Vitest owns benchmark fixture setup, timing callbacks, teardown, and validation overrides. */

const readBenchmarkOption = (name: string, fallback: number, minimum: number) => {
  const value = process.env[name]
  if (value === undefined) return fallback
  const parsed = Number(value)
  assert.isTrue(
    Number.isSafeInteger(parsed) && parsed >= minimum,
    `${name} must be a safe integer greater than or equal to ${minimum}`
  )
  return parsed
}

const iterations = readBenchmarkOption("EFFECT_LOCAL_FANOUT_ITERATIONS", 10, 1)
const warmupIterations = readBenchmarkOption("EFFECT_LOCAL_FANOUT_WARMUP_ITERATIONS", 2, 0)
const preparedSubmissions = iterations + warmupIterations + 4
const watcherCounts = [64, 256, 1_024] as const
type WatcherCount = (typeof watcherCounts)[number]

const Todo = Model.make("FanoutBenchTodo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("FanoutBenchPutTodo", {
  version: 1,
  payload: Todo.schema,
  success: Todo.schema
})
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const layerPutTodoHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const layerMutationRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerPutTodoHandlers))
const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)
const layerStore = ServerStore.layerTrusted({
  definition,
  retainedHistoryEntries: 0,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 0,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: Duration.days(1),
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(layerMutationRuntime),
  Layer.provide(layerDatabase)
)
const layerEphemeralHub = EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 1_024 }).pipe(
  Layer.provide(NodeCrypto.layer)
)
const assertion = PrincipalAssertion.PrincipalAssertion.make("fanout-benchmark")
const layerAssertionVerifier = PrincipalAssertion.layerVerifier(() => Effect.succeed(null))
const layerCluster = SpaceEntity.layer({
  admissionMailboxCapacity: 32,
  readMailboxCapacity: 32,
  watchMailboxCapacity: 1_024,
  ephemeralJoinMailboxCapacity: 1_280,
  ephemeralCommandMailboxCapacity: 32,
  maximumConcurrentBootstrapAuthorizations: 16,
  maximumConcurrentBootstrapPagesPerSpace: 4,
  maximumConcurrentEphemeralJoinVerificationsPerSpace: 16,
  maximumConcurrentEphemeralRequestsPerSpace: 16
}).pipe(
  Layer.provide(layerStore),
  Layer.provide(layerEphemeralHub),
  Layer.provide(layerAssertionVerifier),
  Layer.provide(
    SingleRunner.layer({
      runnerStorage: "memory",
      shardingConfig: {
        entityTerminationTimeout: 0,
        entityMessagePollInterval: 5_000,
        sendRetryInterval: 100
      }
    }).pipe(Layer.provide(layerDatabase))
  )
)

const fixtures = watcherCounts.map((watcherCount) => ({
  watcherCount,
  spaceId: Identity.SpaceId.make(
    `spc_00000000-0000-4000-8000-${String(watcherCount).padStart(12, "0")}`
  ),
  clientId: Identity.ClientId.make(
    `cli_00000000-0000-4000-8000-${String(watcherCount).padStart(12, "0")}`
  ),
  membershipIncarnation: Identity.MembershipIncarnation.make(
    `inc_00000000-0000-4000-8000-${String(watcherCount).padStart(12, "0")}`
  )
}))

interface FanoutBenchService {
  readonly submitAndObserve: (
    watcherCount: WatcherCount
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

class FanoutBench extends Context.Service<FanoutBench, FanoutBenchService>()(
  "@lucas-barake/effect-local-rpc/bench/FanoutBench"
) {}

const layerFanoutBench = Layer.effect(
  FanoutBench,
  Effect.gen(function*() {
    const client = yield* SpaceEntity.Client
    const runs = new Map<WatcherCount, Effect.Effect<void, ReplicaError.ReplicaError>>()

    for (const fixture of fixtures) {
      const wakes: Array<Queue.Queue<Protocol.Wake>> = []
      const watchRequest: Protocol.WatchRequest = {
        spaceId: fixture.spaceId,
        clientId: fixture.clientId,
        schema: definition.schemaIdentity,
        scope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null
      }
      for (let index = 0; index < fixture.watcherCount; index++) {
        const queue = yield* Effect.acquireRelease(Queue.unbounded<Protocol.Wake>(), Queue.shutdown)
        wakes.push(queue)
        yield* client.watch(fixture.spaceId, watchRequest, assertion).pipe(
          Stream.runForEach((wake) => Queue.offer(queue, wake)),
          Effect.ensuring(Queue.shutdown(queue)),
          Effect.forkScoped({ startImmediately: true })
        )
        assert.deepStrictEqual(yield* Queue.take(queue), {
          spaceId: fixture.spaceId
        })
      }

      const submissions: Array<Protocol.MutationEnvelope> = []
      for (let sequence = 1; sequence <= preparedSubmissions; sequence++) {
        const identity = {
          spaceId: fixture.spaceId,
          clientId: fixture.clientId,
          mutationId: Identity.MutationId.make(
            `mut_00000000-0000-4000-8000-${String(fixture.watcherCount).padStart(4, "0")}${
              String(sequence).padStart(8, "0")
            }`
          ),
          localSequence: Identity.LocalSequence.make(sequence),
          basis: Identity.ServerSequence.make(sequence - 1),
          name: PutTodo.name,
          payload: { id: `${fixture.watcherCount}-${sequence}`, title: "fanout" },
          digestVersion: 3 as const,
          membershipIncarnation: fixture.membershipIncarnation,
          sourceSchema: definition.schemaIdentity,
          mutationVersion: PutTodo.version
        }
        const envelope = Protocol.MutationEnvelope.make({
          ...identity,
          digest: yield* Protocol.mutationDigest(identity)
        })
        submissions.push(envelope)
      }

      let nextSubmission = 0
      const runSubmission = Effect.fnUntraced(function*() {
        const envelope = submissions[nextSubmission++]
        if (envelope === undefined) yield* Effect.die("Fanout benchmark exhausted its prepared submissions")
        for (const wake of wakes) assert.strictEqual(Queue.sizeUnsafe(wake), 0)
        const receipt = yield* client.submit(
          fixture.spaceId,
          { envelope, schema: definition.schemaIdentity },
          assertion
        )
        assert(receipt._tag === "Accepted")
        const expected = {
          spaceId: fixture.spaceId
        }
        for (const wake of wakes) {
          assert.deepStrictEqual(yield* Queue.take(wake), expected)
          assert.strictEqual(Queue.sizeUnsafe(wake), 0)
        }
      })
      runs.set(
        fixture.watcherCount,
        Effect.suspend(runSubmission)
      )
    }

    return FanoutBench.of({
      submitAndObserve: (watcherCount) => {
        const run = runs.get(watcherCount)
        if (run === undefined) return Effect.die(`Missing fanout fixture for ${watcherCount} watchers`)
        return run
      }
    })
  })
)
const layerRuntime = layerFanoutBench.pipe(
  Layer.provide(Layer.merge(layerCluster, layerDatabase))
)
const runtime = ManagedRuntime.make(layerRuntime)

beforeAll(async () => {
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest owns this asynchronous benchmark fixture setup boundary.
  await runtime.runPromise(FanoutBench)
}, 120_000)

afterAll(async () => {
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest owns teardown of the ManagedRuntime and its scoped resources.
  await runtime.dispose()
}, 120_000)

for (const watcherCount of watcherCounts) {
  bench(`${watcherCount} same-space watchers observe every submitted sequence`, async () => {
    // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest invokes this Promise returning benchmark host callback.
    await runtime.runPromise(FanoutBench.use((service) => service.submitAndObserve(watcherCount)))
  }, { iterations, time: 0, warmupIterations, warmupTime: 0, throws: true })
}
