import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { AtomRegistry } from "effect/unstable/reactivity"
import * as BrowserReplica from "../src/BrowserReplica.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const MessageSchema = Schema.Struct({
  id: Schema.String,
  chatId: Schema.String,
  sentAt: Schema.Number
})
const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: MessageSchema,
  indexes: {
    byChat: {
      version: 1,
      partition: [{
        name: "chatId",
        affinity: "text",
        schema: Schema.String,
        extract: (message: typeof MessageSchema.Type) => message.chatId
      }],
      sort: [{
        name: "sentAt",
        affinity: "real",
        schema: Schema.Number,
        extract: (message: typeof MessageSchema.Type) => message.sentAt
      }]
    }
  }
})
const PutManyMessages = Mutation.make("PutManyMessages", {
  version: 1,
  payload: Schema.Struct({
    chatId: Schema.String,
    count: Schema.Number,
    startAt: Schema.Number
  }),
  success: Schema.Number
})
const LatestMessages = Query.make("LatestMessages", {
  payload: { chatId: Schema.String },
  success: Schema.Array(Message.schema)
})
const chatReads = new Map<string, number>()
const definition = Definition.make({
  version: 1,
  models: [Message],
  mutations: [PutManyMessages],
  queries: [LatestMessages]
})
const layerPutMany = PutManyMessages.toLayer(
  Effect.fnUntraced(function*({ payload, transaction }) {
    for (let position = 0; position < payload.count; position++) {
      const sentAt = payload.startAt + position
      yield* transaction.set(Message, `${payload.chatId}-${sentAt}`, {
        id: `${payload.chatId}-${sentAt}`,
        chatId: payload.chatId,
        sentAt
      })
    }
    return payload.count
  })
)
const layerHandlers = Layer.mergeAll(
  layerPutMany,
  LatestMessages.toLayer(({ payload, query }) => {
    chatReads.set(payload.chatId, (chatReads.get(payload.chatId) ?? 0) + 1)
    return query.from(Message, "byChat")
      .where({ chatId: payload.chatId })
      .order("desc")
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
const layerServer = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds" as const,
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 100_000,
  retainedReceipts: 256,
  maximumReceipts: 100_000,
  maximumSnapshotEntities: 100_000,
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
const layerEphemeralInactive = Layer.succeed(EphemeralClient.EphemeralClient, {
  session: () => Effect.never,
  publish: () => Effect.void,
  clear: () => Effect.void,
  remove: () => Effect.void
})
const layerReplica = Layer.merge(
  SqlReplica.layer({
    defaultScope: Protocol.ReplicationScope.make({ models: [Message.name] }),
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retainedReceipts: 256,
    maximumReceipts: 100_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 100_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration,
    definition,
    initialSpaces: [spaceId],
    clientId,
    retryDelay: "10 millis"
  }).pipe(
    Layer.provide(layerSync),
    Layer.provide(layerDatabase),
    Layer.provide(layerHandlers)
  ),
  layerEphemeralInactive
)

describe("broad invalidation", () => {
  it.effect(
    "a large single-chat batch does not rerun queries over other chats",
    Effect.fnUntraced(function*() {
      chatReads.clear()
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const mutation = graph.mutation(spaceId, PutManyMessages)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() => Effect.sync(unmountMutation))
      registry.set(mutation, { chatId: "chat-b", count: 5, startAt: 0 })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      const queries = graph.query(spaceId, LatestMessages)
      const chatA = queries({ chatId: "chat-a" })
      const chatB = queries({ chatId: "chat-b" })
      const unmountChatA = registry.mount(chatA)
      const unmountChatB = registry.mount(chatB)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountChatB()
          unmountChatA()
        })
      )
      yield* AtomRegistry.getResult(registry, chatA, { suspendOnWaiting: true })
      const chatBBefore = yield* AtomRegistry.getResult(registry, chatB, { suspendOnWaiting: true })
      assert.strictEqual(chatBBefore.length, 5)
      const chatAReadsBefore = chatReads.get("chat-a") ?? 0
      const chatBReadsBefore = chatReads.get("chat-b") ?? 0

      registry.set(mutation, { chatId: "chat-a", count: 2_100, startAt: 100 })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      const chatAAfter = yield* AtomRegistry.getResult(registry, chatA, { suspendOnWaiting: true })
      yield* AtomRegistry.getResult(registry, chatB, { suspendOnWaiting: true })
      assert.strictEqual(chatAAfter.length, 20)
      assert.isAbove(chatReads.get("chat-a") ?? 0, chatAReadsBefore)
      assert.strictEqual(chatReads.get("chat-b") ?? 0, chatBReadsBefore)
    }, Effect.scoped),
    20_000
  )
})
