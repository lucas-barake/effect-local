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
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as PresenceHub from "../src/PresenceHub.js"
import * as SpaceEntity from "../src/SpaceEntity.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const handlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const runtime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers))
const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)
const store = ServerStore.layerTrusted({
  definition,
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
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds" as const,
  maximumConcurrentReadAuthorizations: 64,
  readAuthorizationCacheCapacity: 4_096,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(runtime),
  Layer.provide(database)
)
const shardingConfig = ShardingConfig.layer({
  shardsPerGroup: 32,
  entityMailboxCapacity: 32,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5_000,
  sendRetryInterval: 100
})

const handlerOptions = {
  admissionMailboxCapacity: 32,
  readMailboxCapacity: 32,
  watchMailboxCapacity: 32,
  presencePublicationMailboxCapacity: 32,
  maximumConcurrentBootstrapPagesPerSpace: 1,
  maximumConcurrentPresencePublicationsPerSpace: 4
} satisfies SpaceEntity.HandlerOptions

const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
const bootstrapRequest: Protocol.BootstrapRequest = {
  spaceId: spaceA,
  schema: definition.schemaIdentity,
  snapshotId,
  afterOrdinal: -1,
  limit: 10
}
const bootstrapPage = Protocol.BootstrapPage.make({
  manifest: {
    spaceId: spaceA,
    definitionHash: definition.hash,
    schema: definition.schemaIdentity,
    snapshotId,
    sequence: Identity.ServerSequence.make(0),
    terminalSequenceThrough: Identity.TerminalSequence.make(0),
    entityCount: 0,
    contentBytes: 0,
    digest: Protocol.initialSnapshotDigest
  },
  entities: [],
  hasMore: false,
  serverSchema: definition.schemaIdentity
})

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
  it.effect("routes synchronization and presence through the split space boundaries", () =>
    Effect.gen(function*() {
      const presenceReady = yield* Deferred.make<void>()
      const presenceLayer = PresenceHub.layer({
        maximumWatchersPerSpace: 1_024,
        authorize: (input) => {
          if (input._tag === "Watch") return Deferred.succeed(presenceReady, undefined).pipe(Effect.asVoid)
          return Effect.void
        }
      })
      const actualStore = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const actualPresence = yield* Layer.build(presenceLayer).pipe(
        Effect.map((context) => Context.get(context, PresenceHub.PresenceHub))
      )
      const entityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, actualStore)),
        Layer.provide(Layer.succeed(PresenceHub.PresenceHub, actualPresence))
      )
      const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, entityHandlers)
      const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, entityHandlers)
      const makeWatchClient = yield* Entity.makeTestClient(SpaceEntity.SpaceWatchEntity, entityHandlers)
      const makePresencePublishClient = yield* Entity.makeTestClient(
        SpaceEntity.SpacePresencePublishEntity,
        entityHandlers
      )
      const admissionClient = yield* makeAdmissionClient(spaceA)
      const readClient = yield* makeReadClient(spaceA)
      const watchClient = yield* makeWatchClient(spaceA)
      const presencePublishClient = yield* makePresencePublishClient(spaceA)
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const watch = yield* watchClient.Watch({
        request: { spaceId: spaceA, schema: definition.schemaIdentity },
        principal: { subject: "reader" }
      }).pipe(
        Stream.runForEach((wake) => Queue.offer(wakes, wake)),
        Effect.forkChild({ startImmediately: true })
      )

      assert.deepStrictEqual(yield* Queue.take(wakes), {
        spaceId: spaceA,
        sequence: Identity.ServerSequence.make(0)
      })

      const submitted = yield* envelope(spaceA)
      const receipt = yield* admissionClient.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: { subject: "writer" }
      })
      assert.strictEqual(receipt._tag, "Accepted")
      assert.deepStrictEqual(yield* Queue.take(wakes), {
        spaceId: spaceA,
        sequence: Identity.ServerSequence.make(1)
      })

      const page = yield* readClient.Pull({
        request: {
          spaceId: spaceA,
          schema: definition.schemaIdentity,
          after: Identity.ServerSequence.make(0),
          limit: 10
        },
        principal: { subject: "reader" }
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.deepStrictEqual(page.entries.map((entry) => entry.mutationId), [mutationId])

      const watchedPresence = yield* watchClient.WatchPresence({ principal: { subject: "reader" } }).pipe(
        Stream.runHead,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(presenceReady)
      const update: Protocol.PresenceUpdate = {
        spaceId: spaceA,
        clientId,
        value: { cursor: 4 },
        ttlMillis: 5_000
      }
      yield* presencePublishClient.PublishPresence({ update, principal: { subject: "writer" } })
      const received = yield* Fiber.join(watchedPresence)
      assert.deepStrictEqual(Option.getOrUndefined(received), update)
      yield* Fiber.interrupt(watch)
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("rejects payloads addressed through a different space owner", () =>
    Effect.gen(function*() {
      const actualStore = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const actualPresence = yield* Layer.build(
        PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })
      ).pipe(Effect.map((context) => Context.get(context, PresenceHub.PresenceHub)))
      const entityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, actualStore)),
        Layer.provide(Layer.succeed(PresenceHub.PresenceHub, actualPresence))
      )
      const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, entityHandlers)
      const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, entityHandlers)
      const makeWatchClient = yield* Entity.makeTestClient(SpaceEntity.SpaceWatchEntity, entityHandlers)
      const makePresencePublishClient = yield* Entity.makeTestClient(
        SpaceEntity.SpacePresencePublishEntity,
        entityHandlers
      )
      const admissionClient = yield* makeAdmissionClient(spaceA)
      const readClient = yield* makeReadClient(spaceA)
      const watchClient = yield* makeWatchClient(spaceA)
      const presencePublishClient = yield* makePresencePublishClient(spaceA)
      const submitted = yield* envelope(spaceB)

      const submitError = yield* admissionClient.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(submitError._tag, "ProtocolInvalid")

      const discardError = yield* admissionClient.Discard({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(discardError._tag, "ProtocolInvalid")

      const pullError = yield* readClient.Pull({
        request: {
          spaceId: spaceB,
          schema: definition.schemaIdentity,
          after: Identity.ServerSequence.make(0),
          limit: 10
        },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(pullError._tag, "ProtocolInvalid")

      const bootstrapError = yield* readClient.Bootstrap({
        request: {
          spaceId: spaceB,
          schema: definition.schemaIdentity,
          snapshotId: Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001"),
          afterOrdinal: -1,
          limit: 10
        },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(bootstrapError._tag, "ProtocolInvalid")

      const watchError = yield* watchClient.Watch({
        request: { spaceId: spaceB, schema: definition.schemaIdentity },
        principal: null
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(watchError._tag, "ProtocolInvalid")

      const presenceError = yield* presencePublishClient.PublishPresence({
        update: { spaceId: spaceB, clientId, value: null, ttlMillis: 5_000 },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(presenceError._tag, "ProtocolInvalid")
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("completes Submit while a Bootstrap page is paused", () =>
    Effect.gen(function*() {
      const actual = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const firstEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const submitEntered = yield* Deferred.make<void>()
      const submitCompleted = yield* Deferred.make<Exit.Exit<Protocol.Receipt, unknown>>()
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        admit: (request, principal) =>
          Deferred.succeed(submitEntered, undefined).pipe(
            Effect.andThen(actual.admit(request, principal)),
            Effect.onExit((exit) => Deferred.succeed(submitCompleted, exit))
          ),
        bootstrapAuthorized: () =>
          Effect.gen(function*() {
            yield* Deferred.succeed(firstEntered, undefined)
            yield* Deferred.await(releaseFirst)
            return bootstrapPage
          })
      })
      const entityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 }))
      )
      const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, entityHandlers)
      const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, entityHandlers)
      const admissionClient = yield* makeAdmissionClient(spaceA)
      const readClient = yield* makeReadClient(spaceA)
      const first = yield* readClient.Bootstrap({ request: bootstrapRequest, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)
      const submitted = yield* envelope(spaceA)
      const submit = yield* admissionClient.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: null
      }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.isTrue(yield* Deferred.isDone(submitEntered))
      const submitExit = yield* Deferred.await(submitCompleted)
      if (Exit.isFailure(submitExit)) assert.fail(Cause.pretty(submitExit.cause))
      assert.strictEqual(submitExit.value._tag, "Accepted")
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(submit)
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("serializes concurrent mutation admission", () =>
    Effect.gen(function*() {
      const actual = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const admitEntered = yield* Deferred.make<void>()
      const releaseAdmit = yield* Deferred.make<void>()
      const discardEntered = yield* Deferred.make<void>()
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        admit: (request, principal) =>
          Deferred.succeed(admitEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseAdmit)),
            Effect.andThen(actual.admit(request, principal))
          ),
        discard: (request, principal) =>
          Deferred.succeed(discardEntered, undefined).pipe(
            Effect.andThen(actual.discard(request, principal))
          )
      })
      const entityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 }))
      )
      const makeAdmissionClient = yield* Entity.makeTestClient(SpaceEntity.SpaceAdmissionEntity, entityHandlers)
      const client = yield* makeAdmissionClient(spaceA)
      const submitted = yield* envelope(spaceA)
      const request = { envelope: submitted, schema: definition.schemaIdentity }
      const submit = yield* client.Submit({ request, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(admitEntered)
      const discard = yield* client.Discard({ request, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.isFalse(yield* Deferred.isDone(discardEntered))
      yield* Deferred.succeed(releaseAdmit, undefined)
      yield* Fiber.join(submit)
      yield* Deferred.await(discardEntered)
      yield* Fiber.join(discard)
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("isolates admission from a full watch mailbox through the real EntityManager", () =>
    Effect.gen(function*() {
      const actual = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const watchersEntered = yield* Queue.unbounded<void>()
      const submitEntered = yield* Deferred.make<void>()
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        admit: (request, principal) =>
          Deferred.succeed(submitEntered, undefined).pipe(
            Effect.andThen(actual.admit(request, principal))
          ),
        watchAuthorized: () =>
          Queue.offer(watchersEntered, undefined).pipe(
            Effect.as(Stream.never)
          )
      })
      const cluster = SpaceEntity.layer({
        ...handlerOptions,
        admissionMailboxCapacity: 1,
        watchMailboxCapacity: 2
      }).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
        Layer.provide(
          SingleRunner.layer({
            runnerStorage: "memory",
            shardingConfig: {
              entityTerminationTimeout: 0,
              entityMessagePollInterval: 5_000,
              sendRetryInterval: 100
            }
          }).pipe(Layer.provide(database))
        )
      )

      yield* Effect.gen(function*() {
        const client = yield* SpaceEntity.Client
        const request = { spaceId: spaceA, schema: definition.schemaIdentity }
        const first = yield* client.watch(spaceA, request, null).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        const second = yield* client.watch(spaceA, request, null).pipe(
          Stream.runDrain,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(watchersEntered)
        yield* Queue.take(watchersEntered)

        const watchFailure = yield* client.watch(spaceA, request, null).pipe(Stream.runDrain, Effect.flip)
        assert.strictEqual(watchFailure._tag, "ServerUnavailable")

        const submitted = yield* envelope(spaceA)
        const receipt = yield* client.submit(
          spaceA,
          { envelope: submitted, schema: definition.schemaIdentity },
          null
        )
        assert.isTrue(yield* Deferred.isDone(submitEntered))
        assert.strictEqual(receipt._tag, "Accepted")

        yield* Fiber.interrupt(first)
        yield* Fiber.interrupt(second)
      }).pipe(Effect.provide(cluster))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects excess Bootstrap pages and releases permits after completion and failure", () =>
    Effect.gen(function*() {
      const actual = yield* Layer.build(store).pipe(
        Effect.map((context) => Context.get(context, ServerStore.ServerStore))
      )
      const firstEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      let calls = 0
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        bootstrapAuthorized: () =>
          Effect.suspend(() => {
            calls++
            if (calls === 1) {
              return Deferred.succeed(firstEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirst)),
                Effect.as(bootstrapPage)
              )
            }
            if (calls === 3) {
              return Effect.fail(new ReplicaError.ProtocolInvalid({ message: "typed bootstrap failure" }))
            }
            return Effect.succeed(bootstrapPage)
          })
      })
      const entityHandlers = SpaceEntity.layerHandlers(handlerOptions).pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 }))
      )
      const makeReadClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, entityHandlers)
      const client = yield* makeReadClient(spaceA)
      const first = yield* client.Bootstrap({ request: bootstrapRequest, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)

      const atCapacity = yield* client.Bootstrap({ request: bootstrapRequest, principal: null }).pipe(Effect.flip)
      assert.strictEqual(atCapacity._tag, "CapacityExceeded")
      if (atCapacity._tag === "CapacityExceeded") {
        assert.strictEqual(atCapacity.resource, "bootstrap pages")
        assert.strictEqual(atCapacity.limit, 1)
      }
      assert.strictEqual(calls, 1)

      yield* Deferred.succeed(releaseFirst, undefined)
      assert.deepStrictEqual(yield* Fiber.join(first), bootstrapPage)
      assert.deepStrictEqual(
        yield* client.Bootstrap({ request: bootstrapRequest, principal: null }),
        bootstrapPage
      )

      const failed = yield* client.Bootstrap({ request: bootstrapRequest, principal: null }).pipe(Effect.flip)
      assert.strictEqual(failed._tag, "ProtocolInvalid")
      assert.deepStrictEqual(
        yield* client.Bootstrap({ request: bootstrapRequest, principal: null }),
        bootstrapPage
      )
      assert.strictEqual(calls, 4)
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("rejects invalid Bootstrap concurrency during layer construction", () =>
    Effect.gen(function*() {
      const invalid = SpaceEntity.layerHandlers({
        ...handlerOptions,
        maximumConcurrentBootstrapPagesPerSpace: 0
      }).pipe(
        Layer.provide(store),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
        Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(database)))
      )
      const failure = yield* Layer.build(invalid).pipe(Effect.flip)
      assert.strictEqual(failure._tag, "InvalidConfiguration")
      if (failure._tag === "InvalidConfiguration") {
        assert.strictEqual(failure.option, "maximumConcurrentBootstrapPagesPerSpace")
      }
    }))
})
