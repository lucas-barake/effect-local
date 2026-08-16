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
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { AtomRegistry } from "effect/unstable/reactivity"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
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
const NoteSchema = Schema.Struct({ id: Schema.String, body: Schema.String })
const Note = Model.make("Note", {
  version: 1,
  key: Schema.String,
  schema: NoteSchema
})
const PutNote = Mutation.make("PutNote", {
  version: 1,
  payload: NoteSchema,
  success: Schema.String
})
const LatestMessages = Query.make("LatestMessages", {
  payload: { chatId: Schema.String },
  success: Schema.Array(Message.schema)
})
const ListNotes = Query.make("ListNotes", {
  success: Schema.Array(NoteSchema)
})
const chatReads = new Map<string, number>()
let noteReads = 0
const definition = Definition.make({
  version: 1,
  models: [Message, Note],
  mutations: [PutManyMessages, PutNote],
  queries: [LatestMessages, ListNotes]
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
const decodeRows = <A, I,>(
  query: Transaction.Query,
  model: Parameters<Transaction.Query["sql"]>[0][number],
  row: Schema.Codec<A, I>,
  statement: Parameters<Transaction.Query["sql"]>[1]
) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ value: Schema.fromJsonString(row) }),
    execute: () => query.sql([model], statement)
  })(undefined).pipe(
    Effect.map((rows) => rows.map((entry) => entry.value)),
    Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
  )
const layerHandlers = Layer.mergeAll(
  layerPutMany,
  PutNote.toLayer(({ payload, transaction }) => transaction.set(Note, payload.id, payload).pipe(Effect.as(payload.id))),
  LatestMessages.toLayer(({ payload, query }) => {
    chatReads.set(payload.chatId, (chatReads.get(payload.chatId) ?? 0) + 1)
    return decodeRows(
      query,
      Message,
      MessageSchema,
      (sql) =>
        sql`SELECT "value" FROM "Message" WHERE "chatId" = ${payload.chatId}
          ORDER BY "sentAt" DESC, "key" DESC LIMIT 20`
    )
  }),
  ListNotes.toLayer(({ query }) => {
    noteReads++
    return decodeRows(query, Note, NoteSchema, (sql) => sql`SELECT "value" FROM "Note" ORDER BY "key" ASC`)
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
    defaultScope: Protocol.ReplicationScope.make({ models: [Message.name, Note.name] }),
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
    "a large batch to one model reruns its readers but not queries over another model",
    Effect.fnUntraced(function*() {
      chatReads.clear()
      noteReads = 0
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const mutation = graph.mutation(spaceId, PutManyMessages)
      const noteMutation = graph.mutation(spaceId, PutNote)
      yield* AtomRegistry.mount(registry, mutation)
      yield* AtomRegistry.mount(registry, noteMutation)
      registry.set(mutation, { chatId: "chat-b", count: 5, startAt: 0 })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      registry.set(noteMutation, { id: "note-1", body: "before" })
      yield* AtomRegistry.getResult(registry, noteMutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      const queries = graph.query(spaceId, LatestMessages)
      const chatB = queries({ chatId: "chat-b" })
      const notes = graph.query(spaceId, ListNotes)(undefined)
      yield* AtomRegistry.mount(registry, chatB)
      yield* AtomRegistry.mount(registry, notes)
      const chatBBefore = yield* AtomRegistry.getResult(registry, chatB, { suspendOnWaiting: true })
      const notesBefore = yield* AtomRegistry.getResult(registry, notes, { suspendOnWaiting: true })
      assert.strictEqual(chatBBefore.length, 5)
      assert.strictEqual(notesBefore.length, 1)
      const chatBReadsBefore = chatReads.get("chat-b") ?? 0
      const noteReadsBefore = noteReads

      registry.set(mutation, { chatId: "chat-a", count: 400, startAt: 100 })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      // Model-granular reactivity: every Message reader re-runs, no Note reader does.
      const chatBAfter = yield* AtomRegistry.getResult(registry, chatB, { suspendOnWaiting: true })
      yield* AtomRegistry.getResult(registry, notes, { suspendOnWaiting: true })
      assert.strictEqual(chatBAfter.length, 5)
      assert.isAbove(chatReads.get("chat-b") ?? 0, chatBReadsBefore)
      assert.strictEqual(noteReads, noteReadsBefore)
    }, Effect.scoped)
  )
})
