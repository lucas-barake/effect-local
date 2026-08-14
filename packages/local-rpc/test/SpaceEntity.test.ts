import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as EphemeralHub from "../src/EphemeralHub.js"
import * as PrincipalAssertion from "../src/PrincipalAssertion.js"
import * as SpaceEntity from "../src/SpaceEntity.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
const member = Protocol.EphemeralMember.make({
  clientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})
const secondMember = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000002")
})

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const layerHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)
const layerStore = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "1 second" as const,
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
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerDatabase)
)
const assertionCodec = Schema.fromJsonString(Schema.Json)
const assertionOf = (principal: typeof Schema.Json.Type) =>
  Schema.encodeUnknownEffect(assertionCodec)(principal).pipe(
    Effect.map((encoded) => PrincipalAssertion.PrincipalAssertion.make(encoded))
  )
const layerAssertionVerifier = PrincipalAssertion.layerVerifier((assertion) =>
  Schema.decodeUnknownEffect(assertionCodec)(assertion).pipe(
    Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" }))
  )
)

const layerShardingConfig = ShardingConfig.layer({
  shardsPerGroup: 32,
  entityMailboxCapacity: 32,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5_000,
  sendRetryInterval: 100
})
const provideShardingConfig = Effect.provide(layerShardingConfig)
const provideNodeCrypto = Effect.provide(NodeCrypto.layer)
const handlerOptions = {
  admissionMailboxCapacity: 32,
  readMailboxCapacity: 32,
  watchMailboxCapacity: 32,
  ephemeralMailboxCapacity: 32,
  maximumConcurrentBootstrapAuthorizations: 4,
  maximumConcurrentBootstrapPagesPerSpace: 1,
  maximumConcurrentEphemeralRequestsPerSpace: 4
} satisfies SpaceEntity.HandlerOptions

const envelope = (spaceId: Identity.SpaceId) => {
  const identity = {
    spaceId,
    clientId,
    mutationId,
    localSequence: Identity.LocalSequence.make(1),
    basis: Identity.ServerSequence.make(0),
    name: PutTodo.name,
    payload: { id: "1", title: "cluster" },
    digestVersion: 3 as const,
    membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001"),
    sourceSchema: definition.schemaIdentity,
    mutationVersion: PutTodo.version
  }
  return Protocol.mutationDigest(identity).pipe(Effect.map((digest) => ({ ...identity, digest })))
}

describe("SpaceEntity", () => {
  it.effect("routes synchronization and ephemera through the split space boundaries", () =>
    Effect.scoped(Effect.gen(function*() {
      const ephemeralReady = yield* Deferred.make<void>()
      const layerEphemeral = EphemeralHub.layer({
        maximumWatchersPerSpace: 1_024,
        authorize: (input) => {
          if (input._tag === "Join") return Deferred.succeed(ephemeralReady, undefined).pipe(Effect.asVoid)
          return Effect.void
        }
      })
      const actualStore = yield* Layer.build(layerStore).pipe(
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const actualEphemeral = yield* Layer.build(layerEphemeral).pipe(
        Effect.map(Context.get(EphemeralHub.EphemeralHub))
      )
      const layerEntityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(layerAssertionVerifier),
        Layer.provide(Layer.succeed(ServerStore.ServerStore, actualStore)),
        Layer.provide(Layer.succeed(EphemeralHub.EphemeralHub, actualEphemeral))
      )
      const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, layerEntityHandlers)
      const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, layerEntityHandlers)
      const makeWatchClient = yield* Entity.makeTestClient(SpaceEntity.SpaceWatchEntity, layerEntityHandlers)
      const makeEphemeralClient = yield* Entity.makeTestClient(
        SpaceEntity.SpaceEphemeralEntity,
        layerEntityHandlers
      )
      const admissionClient = yield* makeAdmissionClient(spaceA)
      const readClient = yield* makeReadClient(spaceA)
      const watchClient = yield* makeWatchClient(spaceA)
      const ephemeralClient = yield* makeEphemeralClient(spaceA)
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const watchAssertion = yield* assertionOf({ subject: "reader" })
      const watch = yield* watchClient.Watch({
        request: {
          spaceId: spaceA,
          clientId,
          schema: definition.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: null
        },
        assertion: watchAssertion
      }).pipe(
        Stream.runForEach((wake) => Queue.offer(wakes, wake)),
        Effect.forkChild({ startImmediately: true })
      )

      assert.deepStrictEqual(yield* Queue.take(wakes), {
        spaceId: spaceA
      })

      const submitted = yield* envelope(spaceA)
      const submitAssertion = yield* assertionOf({ subject: "writer" })
      const receipt = yield* admissionClient.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        assertion: submitAssertion
      })
      assert.strictEqual(receipt._tag, "Accepted")
      assert.deepStrictEqual(yield* Queue.take(wakes), {
        spaceId: spaceA
      })

      const pullAssertion = yield* assertionOf({ subject: "reader" })
      const page = yield* readClient.Pull({
        request: {
          spaceId: spaceA,
          clientId,
          schema: definition.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: null,
          limit: 10
        },
        assertion: pullAssertion
      })
      if (!("_tag" in page)) assert.fail("expected bootstrap")
      const bootstrapAssertion = yield* assertionOf({ subject: "reader" })
      const bootstrap = yield* readClient.Bootstrap({
        request: {
          spaceId: spaceA,
          clientId,
          schema: definition.schemaIdentity,
          scope,
          scopeGeneration,
          cursor: page.manifest.cursor,
          snapshotId: page.manifest.snapshotId,
          afterOrdinal: -1,
          limit: 10
        },
        assertion: bootstrapAssertion
      })
      assert.deepStrictEqual(bootstrap.entries.map((entry) => entry.change._tag), ["Upsert"])

      const firstReady = yield* Deferred.make<void>()
      const secondReady = yield* Deferred.make<void>()
      const secondLeft = yield* Deferred.make<void>()
      const joinAssertion = yield* assertionOf({ subject: "reader" })
      const joined = yield* ephemeralClient.JoinEphemeral({
        request: { spaceId: spaceA, member, value: { status: "online" }, ttlMillis: 5_000 },
        assertion: joinAssertion
      }).pipe(
        Stream.tap((message) => {
          if (message._tag === "Snapshot") return Deferred.succeed(firstReady, undefined)
          if (message._tag === "MemberLeft" && message.member.clientId === secondMember.clientId) {
            return Deferred.succeed(secondLeft, undefined)
          }
          return Effect.void
        }),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(ephemeralReady)
      yield* Deferred.await(firstReady)
      const secondAssertion = yield* assertionOf({ subject: "reader" })
      const secondJoined = yield* ephemeralClient.JoinEphemeral({
        request: { spaceId: spaceA, member: secondMember, value: null, ttlMillis: 5_000 },
        assertion: secondAssertion
      }).pipe(
        Stream.tap((message) => {
          if (message._tag === "Snapshot") return Deferred.succeed(secondReady, undefined)
          return Effect.void
        }),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(secondReady)
      yield* Fiber.interrupt(secondJoined)
      yield* Deferred.await(secondLeft)
      yield* Fiber.interrupt(joined)
      yield* Fiber.interrupt(watch)
    })).pipe(
      Effect.provide(layerShardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect(
    "rejects payloads addressed through a different space owner",
    Effect.fnUntraced(
      function*() {
        const layerEntityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
          Layer.provide(layerAssertionVerifier),
          Layer.provide(layerStore),
          Layer.provide(EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 1_024 }))
        )
        const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, layerEntityHandlers)
        const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, layerEntityHandlers)
        const makeEphemeralClient = yield* Entity.makeTestClient(
          SpaceEntity.SpaceEphemeralEntity,
          layerEntityHandlers
        )
        const admissionClient = yield* makeAdmissionClient(spaceA)
        const readClient = yield* makeReadClient(spaceA)
        const ephemeralClient = yield* makeEphemeralClient(spaceA)
        const submitted = yield* envelope(spaceB)

        const submitAssertion = yield* assertionOf(null)
        const submitResult = yield* admissionClient.Submit({
          request: { envelope: submitted, schema: definition.schemaIdentity },
          assertion: submitAssertion
        }).pipe(Effect.result)
        if (!Result.isFailure(submitResult)) assert.fail("expected submit protocol failure")
        const submitError = submitResult.failure
        assert.strictEqual(submitError._tag, "ProtocolInvalid")

        const pullAssertion = yield* assertionOf(null)
        const pullResult = yield* readClient.Pull({
          request: {
            spaceId: spaceB,
            clientId,
            schema: definition.schemaIdentity,
            scope,
            scopeGeneration,
            cursor: null,
            limit: 10
          },
          assertion: pullAssertion
        }).pipe(Effect.result)
        if (!Result.isFailure(pullResult)) assert.fail("expected pull protocol failure")
        const pullError = pullResult.failure
        assert.strictEqual(pullError._tag, "ProtocolInvalid")

        const bootstrapAssertion = yield* assertionOf(null)
        const bootstrapResult = yield* readClient.Bootstrap({
          request: {
            spaceId: spaceB,
            clientId,
            schema: definition.schemaIdentity,
            scope,
            scopeGeneration,
            cursor: {
              viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
              revision: Identity.ReplicationViewRevision.make(0)
            },
            snapshotId: Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001"),
            afterOrdinal: -1,
            limit: 10
          },
          assertion: bootstrapAssertion
        }).pipe(Effect.result)
        if (!Result.isFailure(bootstrapResult)) assert.fail("expected bootstrap protocol failure")
        const bootstrapError = bootstrapResult.failure
        assert.strictEqual(bootstrapError._tag, "ProtocolInvalid")

        const ephemeralAssertion = yield* assertionOf(null)
        const ephemeralResult = yield* ephemeralClient.PublishEphemeral({
          request: {
            _tag: "Event",
            spaceId: spaceB,
            member,
            channel: "typing",
            value: null,
            ttlMillis: 5_000
          },
          assertion: ephemeralAssertion
        }).pipe(Effect.result)
        if (!Result.isFailure(ephemeralResult)) assert.fail("expected ephemeral protocol failure")
        assert.strictEqual(ephemeralResult.failure._tag, "ProtocolInvalid")
      },
      provideShardingConfig,
      provideNodeCrypto
    )
  )

  it.effect("completes Submit while a Bootstrap page is paused", () =>
    Effect.scoped(Effect.gen(function*() {
      const actual = yield* Layer.build(layerStore).pipe(
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const firstEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const submitEntered = yield* Deferred.make<void>()
      const submitCompleted = yield* Deferred.make<Exit.Exit<Protocol.Receipt, ReplicaError.ReplicaError>>()
      const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
      const page = Protocol.BootstrapPage.make({
        manifest: {
          spaceId: spaceA,
          clientId,
          definitionHash: definition.hash,
          schema: definition.schemaIdentity,
          scopeDigest: Protocol.MutationDigest.make("0".repeat(64)),
          scopeGeneration,
          cursor: {
            viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
            revision: Identity.ReplicationViewRevision.make(0)
          },
          snapshotId,
          sequence: Identity.ServerSequence.make(0),
          terminalSequenceThrough: Identity.TerminalSequence.make(0),
          entityCount: 0,
          contentBytes: 0,
          digest: Protocol.initialSnapshotDigest
        },
        entries: [],
        hasMore: false,
        serverSchema: definition.schemaIdentity
      })
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        admit: (request, principal) =>
          Deferred.succeed(submitEntered, undefined).pipe(
            Effect.andThen(actual.admit(request, principal)),
            Effect.onExit((exit) => Deferred.succeed(submitCompleted, exit))
          ),
        prepareBootstrapAuthorized: () => {
          const prepare = Effect.gen(function*() {
            yield* Deferred.succeed(firstEntered, undefined)
            yield* Deferred.await(releaseFirst)
            return page
          })
          return Effect.succeed(prepare)
        }
      })
      const layerCluster = SpaceEntity.layer(handlerOptions).pipe(
        Layer.provide(layerAssertionVerifier),
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
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
      const request: Protocol.BootstrapRequest = {
        spaceId: spaceA,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: page.manifest.cursor,
        snapshotId,
        afterOrdinal: -1,
        limit: 10
      }
      yield* Effect.gen(function*() {
        const client = yield* SpaceEntity.Client
        const bootstrapAssertion = yield* assertionOf(null)
        const first = yield* client.bootstrap(spaceA, request, bootstrapAssertion).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(firstEntered)
        const submitted = yield* envelope(spaceA)
        const submitAssertion = yield* assertionOf(null)
        const submit = yield* client.submit(
          spaceA,
          { envelope: submitted, schema: definition.schemaIdentity },
          submitAssertion
        ).pipe(Effect.forkChild({ startImmediately: true }))

        yield* Deferred.await(submitEntered)
        const submitExit = yield* Deferred.await(submitCompleted)
        if (Exit.isFailure(submitExit)) {
          assert.fail(Cause.pretty(submitExit.cause))
        }
        assert.strictEqual(submitExit.value._tag, "Accepted")

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(submit)
      }).pipe(Effect.provide(layerCluster))
    })).pipe(
      TestClock.withLive,
      Effect.provide(layerShardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("does not reveal Bootstrap page occupancy to a denied principal", () =>
    Effect.scoped(Effect.gen(function*() {
      const actual = yield* Layer.build(layerStore).pipe(
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const firstEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
      const page = Protocol.BootstrapPage.make({
        manifest: {
          spaceId: spaceA,
          clientId,
          definitionHash: definition.hash,
          schema: definition.schemaIdentity,
          scopeDigest: Protocol.MutationDigest.make("0".repeat(64)),
          scopeGeneration,
          cursor: {
            viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
            revision: Identity.ReplicationViewRevision.make(0)
          },
          snapshotId,
          sequence: Identity.ServerSequence.make(0),
          terminalSequenceThrough: Identity.TerminalSequence.make(0),
          entityCount: 0,
          contentBytes: 0,
          digest: Protocol.initialSnapshotDigest
        },
        entries: [],
        hasMore: false,
        serverSchema: definition.schemaIdentity
      })
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        prepareBootstrapAuthorized: (_request, principal) => {
          if (principal === "denied") {
            return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "policy denied" }))
          }
          const prepare = Deferred.succeed(firstEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFirst)),
            Effect.as(page)
          )
          return Effect.succeed(prepare)
        }
      })
      const layerCluster = SpaceEntity.layer(handlerOptions).pipe(
        Layer.provide(layerAssertionVerifier),
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
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
      const request: Protocol.BootstrapRequest = {
        spaceId: spaceA,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: page.manifest.cursor,
        snapshotId,
        afterOrdinal: -1,
        limit: 10
      }
      yield* Effect.gen(function*() {
        const client = yield* SpaceEntity.Client
        const allowedAssertion = yield* assertionOf("allowed")
        const first = yield* client.bootstrap(spaceA, request, allowedAssertion).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(firstEntered)

        const deniedAssertion = yield* assertionOf("denied")
        const deniedResult = yield* client.bootstrap(spaceA, request, deniedAssertion).pipe(Effect.result)
        if (!Result.isFailure(deniedResult)) assert.fail("expected authorization denial")
        const denied = deniedResult.failure
        assert.strictEqual(denied._tag, "AuthorizationDenied")

        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Fiber.join(first)
      }).pipe(Effect.provide(layerCluster))
    })).pipe(
      TestClock.withLive,
      Effect.provide(layerShardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("bounds Bootstrap assertion and preparation work across spaces", () =>
    Effect.scoped(Effect.gen(function*() {
      const actual = yield* Layer.build(layerStore).pipe(
        Effect.map(Context.get(ServerStore.ServerStore))
      )
      const activePreflights = yield* Ref.make(0)
      const maximumActivePreflights = yield* Ref.make(0)
      const assertionEntered = yield* Deferred.make<void>()
      const releaseAssertion = yield* Deferred.make<void>()
      const preparationEntered = yield* Deferred.make<void>()
      const releasePreparation = yield* Deferred.make<void>()
      const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
      const page = Protocol.BootstrapPage.make({
        manifest: {
          spaceId: spaceA,
          clientId,
          definitionHash: definition.hash,
          schema: definition.schemaIdentity,
          scopeDigest: Protocol.MutationDigest.make("0".repeat(64)),
          scopeGeneration,
          cursor: {
            viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
            revision: Identity.ReplicationViewRevision.make(0)
          },
          snapshotId,
          sequence: Identity.ServerSequence.make(0),
          terminalSequenceThrough: Identity.TerminalSequence.make(0),
          entityCount: 0,
          contentBytes: 0,
          digest: Protocol.initialSnapshotDigest
        },
        entries: [],
        hasMore: false,
        serverSchema: definition.schemaIdentity
      })
      const trackPreflight = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
        Effect.acquireUseRelease(
          Ref.updateAndGet(activePreflights, (active) => active + 1).pipe(
            Effect.tap((active) => Ref.update(maximumActivePreflights, (maximum) => Math.max(maximum, active)))
          ),
          () => effect,
          () => Ref.update(activePreflights, (active) => active - 1)
        )
      const layerVerifier = PrincipalAssertion.layerVerifier((assertion) =>
        Schema.decodeUnknownEffect(assertionCodec)(assertion).pipe(
          Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" })),
          Effect.flatMap((principal) => {
            if (principal !== "first") return trackPreflight(Effect.succeed(principal))
            return trackPreflight(
              Deferred.succeed(assertionEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseAssertion)),
                Effect.as(principal)
              )
            )
          })
        )
      )
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        prepareBootstrapAuthorized: (_request, principal) => {
          if (principal !== "first") return Effect.succeed(Effect.succeed(page)).pipe(trackPreflight)
          return trackPreflight(
            Deferred.succeed(preparationEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releasePreparation)),
              Effect.as(Effect.succeed(page))
            )
          )
        }
      })
      const layerCluster = SpaceEntity.layer({
        ...handlerOptions,
        maximumConcurrentBootstrapAuthorizations: 1
      }).pipe(
        Layer.provide(layerVerifier),
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
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
      const request: Protocol.BootstrapRequest = {
        spaceId: spaceA,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: page.manifest.cursor,
        snapshotId,
        afterOrdinal: -1,
        limit: 10
      }
      yield* Effect.gen(function*() {
        const client = yield* SpaceEntity.Client
        const firstAssertion = yield* assertionOf("first")
        const first = yield* client.bootstrap(spaceA, request, firstAssertion).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(assertionEntered)

        const secondAssertion = yield* assertionOf("second")
        const assertionOverflowResult = yield* client.bootstrap(
          spaceB,
          { ...request, spaceId: spaceB },
          secondAssertion
        ).pipe(Effect.result)
        if (!Result.isFailure(assertionOverflowResult)) assert.fail("expected assertion capacity failure")
        const assertionOverflow = assertionOverflowResult.failure
        assert.deepStrictEqual(
          assertionOverflow,
          new ReplicaError.CapacityExceeded({
            resource: "bootstrap authorizations",
            limit: 1
          })
        )

        yield* Deferred.succeed(releaseAssertion, undefined)
        yield* Deferred.await(preparationEntered)

        const thirdAssertion = yield* assertionOf("third")
        const preparationOverflowResult = yield* client.bootstrap(
          spaceB,
          { ...request, spaceId: spaceB },
          thirdAssertion
        ).pipe(Effect.result)
        if (!Result.isFailure(preparationOverflowResult)) assert.fail("expected preparation capacity failure")
        const preparationOverflow = preparationOverflowResult.failure
        assert.deepStrictEqual(
          preparationOverflow,
          new ReplicaError.CapacityExceeded({
            resource: "bootstrap authorizations",
            limit: 1
          })
        )
        assert.strictEqual(yield* Ref.get(maximumActivePreflights), 1)

        yield* Deferred.succeed(releasePreparation, undefined)
        yield* Fiber.join(first)
        assert.strictEqual(yield* Ref.get(activePreflights), 0)
      }).pipe(Effect.provide(layerCluster))
    })).pipe(
      TestClock.withLive,
      Effect.provide(layerShardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))
})
