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
  createdAt: Schema.Number,
  text: Schema.String
})
const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: MessageSchema,
  indexes: {
    byCreatedAt: {
      version: 1,
      partition: [],
      sort: [{
        name: "createdAt",
        affinity: "real",
        schema: Schema.Number,
        extract: (message: typeof MessageSchema.Type) => message.createdAt
      }]
    }
  }
})
const PutMessage = Mutation.make("PutMessage", {
  version: 1,
  payload: Message.schema,
  success: Message.schema
})
const PageMessages = Query.make("PageMessages", {
  payload: { cursor: Schema.NullOr(Schema.String), limit: Schema.Number },
  success: Schema.Struct({
    items: Schema.Array(Message.schema),
    next: Schema.NullOr(Schema.String)
  })
})
const PageAscending = Query.make("PageAscending", {
  payload: { cursor: Schema.NullOr(Schema.String), limit: Schema.Number },
  success: Schema.Struct({
    items: Schema.Array(Message.schema),
    next: Schema.NullOr(Schema.String)
  })
})
const TwoPages = Query.make("TwoPages", {
  success: Schema.Struct({
    first: Schema.Array(Schema.String),
    second: Schema.Array(Schema.String)
  })
})
const pageReads = new Map<string, number>()
const ascendingReads = new Map<string, number>()
let twoPageReads = 0
const definition = Definition.make({
  version: 1,
  models: [Message],
  mutations: [PutMessage],
  queries: [PageMessages, PageAscending, TwoPages]
})
const layerTwoPages = TwoPages.toLayer(
  Effect.fnUntraced(function*({ query }) {
    twoPageReads++
    const builder = query.from(Message, "byCreatedAt").order("asc").limit(2)
    const first = yield* builder.page()
    let second: ReadonlyArray<typeof Message.schema.Type> = []
    if (first.next !== undefined) second = (yield* builder.after(first.next).page()).items
    return {
      first: first.items.map((message) => message.id),
      second: second.map((message) => message.id)
    }
  })
)
const layerHandlers = Layer.mergeAll(
  PutMessage.toLayer(({ payload, transaction }) =>
    transaction.set(Message, payload.id, payload).pipe(Effect.as(payload))
  ),
  PageMessages.toLayer(({ payload, query }) => {
    const key = `${payload.cursor ?? "head"}:${payload.limit}`
    pageReads.set(key, (pageReads.get(key) ?? 0) + 1)
    let builder = query.from(Message, "byCreatedAt").order("desc").limit(payload.limit)
    if (payload.cursor !== null) builder = builder.after(payload.cursor)
    return builder.page().pipe(
      Effect.map((page) => ({ items: page.items, next: page.next?.token ?? null }))
    )
  }),
  PageAscending.toLayer(({ payload, query }) => {
    const key = `${payload.cursor ?? "head"}:${payload.limit}`
    ascendingReads.set(key, (ascendingReads.get(key) ?? 0) + 1)
    let builder = query.from(Message, "byCreatedAt").order("asc").limit(payload.limit)
    if (payload.cursor !== null) builder = builder.after(payload.cursor)
    return builder.page().pipe(
      Effect.map((page) => ({ items: page.items, next: page.next?.token ?? null }))
    )
  }),
  layerTwoPages
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
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: [Message.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Message.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const layerServer = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds" as const,
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
    ...clientHistory,
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

describe("query pagination", () => {
  it.effect(
    "retains every executed page footprint from one builder",
    Effect.fnUntraced(function*() {
      twoPageReads = 0
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const mutation = graph.mutation(spaceId, PutMessage)
      yield* AtomRegistry.mount(registry, mutation)
      for (let n = 1; n <= 4; n++) {
        registry.set(mutation, { id: `s${n}`, createdAt: n, text: `message ${n}` })
        yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      }
      yield* Effect.yieldNow

      const twoPages = graph.query(spaceId, TwoPages)(undefined)
      yield* AtomRegistry.mount(registry, twoPages)
      const value = yield* AtomRegistry.getResult(registry, twoPages, { suspendOnWaiting: true })
      assert.deepStrictEqual(value, { first: ["s1", "s2"], second: ["s3", "s4"] })
      const readsBefore = twoPageReads

      registry.set(mutation, { id: "s1", createdAt: 1, text: "edited first page row" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      yield* AtomRegistry.getResult(registry, twoPages, { suspendOnWaiting: true })

      assert.isAbove(twoPageReads, readsBefore)
    }, Effect.scoped)
  )

  it.effect(
    "a windowed query refreshes exactly on head insert and ignores writes past its boundary",
    Effect.fnUntraced(function*() {
      pageReads.clear()
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const mutation = graph.mutation(spaceId, PutMessage)
      yield* AtomRegistry.mount(registry, mutation)
      for (let n = 1; n <= 6; n++) {
        registry.set(mutation, { id: `m${n}`, createdAt: n, text: `message ${n}` })
        yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      }
      yield* Effect.yieldNow

      const pages = graph.query(spaceId, PageMessages)
      const head = pages({ cursor: null, limit: 2 })
      yield* AtomRegistry.mount(registry, head)
      const first = yield* AtomRegistry.getResult(registry, head, { suspendOnWaiting: true })
      assert.deepStrictEqual(first.items.map((message) => message.id), ["m6", "m5"])
      const olderCursor = first.next!
      const older = pages({ cursor: olderCursor, limit: 2 })
      yield* AtomRegistry.mount(registry, older)
      const olderValue = yield* AtomRegistry.getResult(registry, older, { suspendOnWaiting: true })
      assert.deepStrictEqual(olderValue.items.map((message) => message.id), ["m4", "m3"])

      const window = pages({ cursor: null, limit: 4 })
      yield* AtomRegistry.mount(registry, window)
      const windowBefore = yield* AtomRegistry.getResult(registry, window, { suspendOnWaiting: true })
      assert.deepStrictEqual(windowBefore.items.map((message) => message.id), ["m6", "m5", "m4", "m3"])

      const olderReadsBefore = pageReads.get(`${olderCursor}:2`) ?? 0
      const windowReadsBefore = pageReads.get("head:4") ?? 0
      registry.set(mutation, { id: "m7", createdAt: 7, text: "message 7" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      const windowAfter = yield* AtomRegistry.getResult(registry, window, { suspendOnWaiting: true })
      yield* AtomRegistry.getResult(registry, older, { suspendOnWaiting: true })
      assert.deepStrictEqual(windowAfter.items.map((message) => message.id), ["m7", "m6", "m5", "m4"])
      assert.isAbove(pageReads.get("head:4") ?? 0, windowReadsBefore)
      assert.strictEqual(pageReads.get(`${olderCursor}:2`) ?? 0, olderReadsBefore)

      const windowReadsAfterInsert = pageReads.get("head:4") ?? 0
      registry.set(mutation, { id: "m1", createdAt: 1, text: "edited beyond the boundary" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      yield* AtomRegistry.getResult(registry, window, { suspendOnWaiting: true })
      assert.strictEqual(pageReads.get("head:4") ?? 0, windowReadsAfterInsert)
    }, Effect.scoped)
  )

  it.effect(
    "an ascending tail page picks up appends past its boundary",
    Effect.fnUntraced(function*() {
      ascendingReads.clear()
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const mutation = graph.mutation(spaceId, PutMessage)
      yield* AtomRegistry.mount(registry, mutation)
      for (let n = 1; n <= 3; n++) {
        registry.set(mutation, { id: `a${n}`, createdAt: n, text: `message ${n}` })
        yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      }
      yield* Effect.yieldNow

      const pages = graph.query(spaceId, PageAscending)
      const head = pages({ cursor: null, limit: 2 })
      yield* AtomRegistry.mount(registry, head)
      const first = yield* AtomRegistry.getResult(registry, head, { suspendOnWaiting: true })
      assert.deepStrictEqual(first.items.map((message) => message.id), ["a1", "a2"])
      const tailCursor = first.next!
      const tail = pages({ cursor: tailCursor, limit: 2 })
      yield* AtomRegistry.mount(registry, tail)
      const second = yield* AtomRegistry.getResult(registry, tail, { suspendOnWaiting: true })
      assert.deepStrictEqual(second.items.map((message) => message.id), ["a3"])
      assert.isNull(second.next)

      const headReadsBefore = ascendingReads.get("head:2") ?? 0
      const tailReadsBefore = ascendingReads.get(`${tailCursor}:2`) ?? 0
      registry.set(mutation, { id: "a4", createdAt: 4, text: "appended" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      const secondAfter = yield* AtomRegistry.getResult(registry, tail, { suspendOnWaiting: true })
      yield* AtomRegistry.getResult(registry, head, { suspendOnWaiting: true })
      assert.deepStrictEqual(secondAfter.items.map((message) => message.id), ["a3", "a4"])
      assert.isAbove(ascendingReads.get(`${tailCursor}:2`) ?? 0, tailReadsBefore)
      assert.strictEqual(ascendingReads.get("head:2") ?? 0, headReadsBefore)
    }, Effect.scoped)
  )
})
