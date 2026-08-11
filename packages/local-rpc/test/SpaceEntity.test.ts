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
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
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

const envelope = (spaceId: Identity.SpaceId) => {
  const identity = {
    spaceId,
    clientId,
    mutationId,
    localSequence: Identity.LocalSequence.make(1),
    basis: Identity.ServerSequence.make(0),
    name: PutTodo.name,
    payload: { id: "1", title: "cluster" },
    digestVersion: 2 as const,
    sourceSchema: definition.schemaIdentity,
    mutationVersion: PutTodo.version
  }
  return Protocol.mutationDigest(identity).pipe(Effect.map((digest) => ({ ...identity, digest })))
}

describe("SpaceEntity", () => {
  it.effect("routes synchronization and presence through one concurrent space owner", () =>
    Effect.gen(function*() {
      const presenceReady = yield* Deferred.make<void>()
      const presence = PresenceHub.layer({
        authorize: (input) => {
          if (input._tag === "Watch") return Deferred.succeed(presenceReady, undefined).pipe(Effect.asVoid)
          return Effect.void
        }
      })
      const entityHandlers = SpaceEntity.layerHandlers().pipe(
        Layer.provide(store),
        Layer.provide(presence)
      )
      const makeClient = yield* Entity.makeTestClient(SpaceEntity.SpaceEntity, entityHandlers)
      const client = yield* makeClient(spaceA)
      const wakes = yield* Queue.unbounded<Protocol.Wake>()
      const watch = yield* client.Watch({
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
      const receipt = yield* client.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: { subject: "writer" }
      })
      assert.strictEqual(receipt._tag, "Accepted")
      assert.deepStrictEqual(yield* Queue.take(wakes), {
        spaceId: spaceA,
        sequence: Identity.ServerSequence.make(1)
      })

      const page = yield* client.Pull({
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

      const watchedPresence = yield* client.WatchPresence({ principal: { subject: "reader" } }).pipe(
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
      yield* client.PublishPresence({ update, principal: { subject: "writer" } })
      const received = yield* Fiber.join(watchedPresence)
      assert.deepStrictEqual(Option.getOrUndefined(received), update)
      yield* Fiber.interrupt(watch)
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("rejects payloads addressed through a different space owner", () =>
    Effect.gen(function*() {
      const entityHandlers = SpaceEntity.layerHandlers().pipe(
        Layer.provide(store),
        Layer.provide(PresenceHub.layerTrusted())
      )
      const makeClient = yield* Entity.makeTestClient(SpaceEntity.SpaceEntity, entityHandlers)
      const client = yield* makeClient(spaceA)
      const submitted = yield* envelope(spaceB)

      const submitError = yield* client.Submit({
        request: { envelope: submitted, schema: definition.schemaIdentity },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(submitError._tag, "ProtocolInvalid")

      const pullError = yield* client.Pull({
        request: {
          spaceId: spaceB,
          schema: definition.schemaIdentity,
          after: Identity.ServerSequence.make(0),
          limit: 10
        },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(pullError._tag, "ProtocolInvalid")

      const bootstrapError = yield* client.Bootstrap({
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

      const presenceError = yield* client.PublishPresence({
        update: { spaceId: spaceB, clientId, value: null, ttlMillis: 5_000 },
        principal: null
      }).pipe(Effect.flip)
      assert.strictEqual(presenceError._tag, "ProtocolInvalid")
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("serializes bootstrap page work through the entity concurrency limit", () =>
    Effect.gen(function*() {
      const actual = yield* Effect.provide(ServerStore.ServerStore, store)
      const firstEntered = yield* Deferred.make<void>()
      const secondEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const calls = yield* Ref.make(0)
      const snapshotId = Identity.SnapshotId.make("snp_00000000-0000-4000-8000-000000000001")
      const page = Protocol.BootstrapPage.make({
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
        hasMore: false
      })
      const wrapped = ServerStore.ServerStore.of({
        ...actual,
        bootstrapAuthorized: () =>
          Effect.gen(function*() {
            const call = yield* Ref.getAndUpdate(calls, (count) => count + 1)
            if (call === 0) {
              yield* Deferred.succeed(firstEntered, undefined)
              yield* Deferred.await(releaseFirst)
            } else {
              yield* Deferred.succeed(secondEntered, undefined)
            }
            return page
          })
      })
      const entityHandlers = SpaceEntity.layerHandlers().pipe(
        Layer.provide(Layer.succeed(ServerStore.ServerStore, wrapped)),
        Layer.provide(PresenceHub.layerTrusted())
      )
      const makeClient = yield* Entity.makeTestClient(SpaceEntity.SpaceEntity, entityHandlers)
      const client = yield* makeClient(spaceA)
      const request: Protocol.BootstrapRequest = {
        spaceId: spaceA,
        schema: definition.schemaIdentity,
        snapshotId,
        afterOrdinal: -1,
        limit: 10
      }
      const first = yield* client.Bootstrap({ request, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)
      const second = yield* client.Bootstrap({ request, principal: null }).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.isFalse(yield* Deferred.isDone(secondEntered))
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      assert.isTrue(yield* Deferred.isDone(secondEntered))
    }).pipe(
      Effect.provide(shardingConfig),
      Effect.provide(NodeCrypto.layer)
    ))
})
