import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { serverDatabases } from "./fixtures/ServerDatabase.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000b01")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000b01")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000b02")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000b01")

const escapedChat = "\u001dchat"
const escapedReader = "\u001dreader"

type MessageValue = typeof Domain.Message.schema.Type

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const put = Effect.fnUntraced(function*(sequence: number, payload: MessageValue) {
  const identity = {
    spaceId,
    clientId: writerId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(sequence),
    basis: Identity.ServerSequence.make(0),
    name: Domain.PutMessage.name,
    payload,
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: Domain.definition.schemaIdentity,
    mutationVersion: Domain.PutMessage.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

const windowScope = (count: number, partitions: ReadonlyArray<Protocol.ReplicationWindowPartition> = []) =>
  Protocol.ReplicationScope.make({
    models: [],
    windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count, partitions })]
  })

const context = (scope: Protocol.ReplicationScope) => ({
  spaceId,
  clientId: readerId,
  schema: Domain.definition.schemaIdentity,
  scope,
  scopeGeneration: Identity.ReplicationScopeGeneration.make(1)
})

const pullRequest = (scope: Protocol.ReplicationScope, cursor: Protocol.ReplicationCursor | null) =>
  Protocol.PullRequest.make({ ...context(scope), membershipIncarnation, cursor, limit: 100 })

const bootstrapRequest = (scope: Protocol.ReplicationScope, manifest: Protocol.SnapshotManifest) =>
  Protocol.BootstrapRequest.make({
    ...context(scope),
    membershipIncarnation,
    scopeGeneration: manifest.scopeGeneration,
    cursor: manifest.cursor,
    snapshotId: manifest.snapshotId,
    afterOrdinal: -1,
    limit: 100
  })

const keyOf = (change: Protocol.ViewChange) => {
  const key = change.entity.key
  if (typeof key !== "string") assert.fail("expected a string entity key")
  return key
}

const provideNodeCrypto = Effect.provide(NodeCrypto.layer)

describe.each(serverDatabases)("partition values and principals that start with U+001D ($dialect)", (database) => {
  const layerServices = Layer.mergeAll(database.layer(), NodeCrypto.layer, Reactivity.layer)
  const makeServer = (observed: Array<ServerStore.ReadAuthorizationInput> = []) =>
    ServerStore.layer({
      definition: Domain.definition,
      migration: { retryDelay: "1 millis", maximumAttempts: 8 },
      authorizeAccess: () => Effect.void,
      authorizeMutation: () => Effect.void,
      authorizeRead: (input) => Effect.sync(() => observed.push(input))
    }).pipe(
      Layer.provide(layerRuntime),
      Layer.provide(layerServices),
      Layer.build,
      Effect.map(Context.get(ServerStore.ServerStore))
    )

  it.effect(
    "an authorized bootstrap serves the scope the client asked for",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer()
        yield* server.submit(yield* put(1, { id: "m-1", chatId: escapedChat, sentAt: 1, body: "" }))
        yield* server.submit(yield* put(2, { id: "m-2", chatId: escapedChat, sentAt: 2, body: "" }))
        const scope = windowScope(1, [Protocol.ReplicationWindowPartition.make({ key: [escapedChat], count: 2 })])
        const required = yield* server.pullAuthorized(pullRequest(scope, null), "reader")
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrapAuthorized(bootstrapRequest(scope, required.manifest), "reader")
        assert.strictEqual(page.manifest.scopeDigest, required.manifest.scopeDigest)
        assert.deepStrictEqual(page.entries.map((entry) => keyOf(entry.change)).toSorted(), ["m-1", "m-2"])
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "read authorization sees the principal and partition keys the caller passed",
    Effect.fnUntraced(
      function*() {
        const observed: Array<ServerStore.ReadAuthorizationInput> = []
        const server = yield* makeServer(observed)
        yield* server.submit(yield* put(1, { id: "m-1", chatId: escapedChat, sentAt: 1, body: "" }))
        const scope = windowScope(1, [Protocol.ReplicationWindowPartition.make({ key: [escapedChat], count: 1 })])
        const required = yield* server.pullAuthorized(pullRequest(scope, null), escapedReader)
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        yield* server.bootstrapAuthorized(bootstrapRequest(scope, required.manifest), escapedReader)
        yield* server.watchAuthorized({ ...context(scope), cursor: required.manifest.cursor }, escapedReader)
        assert.isAbove(observed.length, 2)
        for (const input of observed) {
          assert.strictEqual(input.principal, escapedReader)
          assert.deepStrictEqual(input.scope.windows?.[0]?.partitions?.[0]?.key, [escapedChat])
        }
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "an incremental pull retracts the member that slid out of the partition window",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer()
        yield* server.submit(yield* put(1, { id: "old", chatId: escapedChat, sentAt: 1, body: "" }))
        const scope = windowScope(1)
        const required = yield* server.pull(pullRequest(scope, null))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        yield* server.bootstrap(bootstrapRequest(scope, required.manifest))
        const settled = yield* server.pull(pullRequest(scope, required.manifest.cursor))
        if ("_tag" in settled) assert.fail("expected a page")
        const acknowledged = yield* server.pull(pullRequest(scope, settled.cursor))
        if ("_tag" in acknowledged) assert.fail("expected a page")
        yield* server.submit(yield* put(2, { id: "new", chatId: escapedChat, sentAt: 2, body: "" }))
        const slid = yield* server.pull(pullRequest(scope, acknowledged.cursor))
        if ("_tag" in slid) assert.fail("expected a page")
        assert.deepStrictEqual(
          slid.changes.map((change) => `${change._tag}:${keyOf(change)}`).toSorted(),
          ["Retract:old", "Upsert:new"]
        )
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "a partition override replaces the default window count for its partition",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer()
        for (let sentAt = 1; sentAt <= 3; sentAt++) {
          yield* server.submit(yield* put(sentAt, { id: `m-${sentAt}`, chatId: escapedChat, sentAt, body: "" }))
        }
        const scope = windowScope(3, [Protocol.ReplicationWindowPartition.make({ key: [escapedChat], count: 1 })])
        const required = yield* server.pull(pullRequest(scope, null))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(scope, required.manifest))
        assert.deepStrictEqual(page.entries.map((entry) => keyOf(entry.change)), ["m-3"])
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )
})

describe.each(serverDatabases)("partition values that contain an unpaired surrogate ($dialect)", (database) => {
  const unpairedChat = "\ud800chat"
  const layerServices = Layer.mergeAll(database.layer(), NodeCrypto.layer, Reactivity.layer)
  const makeServer = ServerStore.layerTrusted({
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerServices),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )

  it.effect(
    "a partition override replaces the default window count for its partition",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        for (let sentAt = 1; sentAt <= 3; sentAt++) {
          yield* server.submit(yield* put(sentAt, { id: `m-${sentAt}`, chatId: unpairedChat, sentAt, body: "" }))
        }
        const scope = windowScope(3, [Protocol.ReplicationWindowPartition.make({ key: [unpairedChat], count: 1 })])
        const required = yield* server.pull(pullRequest(scope, null))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(scope, required.manifest))
        assert.deepStrictEqual(page.entries.map((entry) => keyOf(entry.change)), ["m-3"])
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "an incremental pull retracts the member that slid out of the partition window",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        yield* server.submit(yield* put(1, { id: "old", chatId: unpairedChat, sentAt: 1, body: "" }))
        const scope = windowScope(1)
        const required = yield* server.pull(pullRequest(scope, null))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        yield* server.bootstrap(bootstrapRequest(scope, required.manifest))
        const settled = yield* server.pull(pullRequest(scope, required.manifest.cursor))
        if ("_tag" in settled) assert.fail("expected a page")
        const acknowledged = yield* server.pull(pullRequest(scope, settled.cursor))
        if ("_tag" in acknowledged) assert.fail("expected a page")
        yield* server.submit(yield* put(2, { id: "new", chatId: unpairedChat, sentAt: 2, body: "" }))
        const slid = yield* server.pull(pullRequest(scope, acknowledged.cursor))
        if ("_tag" in slid) assert.fail("expected a page")
        assert.deepStrictEqual(
          slid.changes.map((change) => `${change._tag}:${keyOf(change)}`).toSorted(),
          ["Retract:old", "Upsert:new"]
        )
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "partitions that differ only in unpaired surrogates or their replacement character stay distinct",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        const chats = ["chat-\ud800", "chat-\udfff", "chat-\ufffd", "chat-\ud7ff", "chat-\ud83d\ude00"]
        for (let position = 0; position < chats.length; position++) {
          const sentAt = position + 1
          yield* server.submit(yield* put(sentAt, { id: `m-${sentAt}`, chatId: chats[position], sentAt, body: "" }))
        }
        const scope = windowScope(1)
        const required = yield* server.pull(pullRequest(scope, null))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrap(bootstrapRequest(scope, required.manifest))
        assert.deepStrictEqual(
          page.entries.map((entry) => keyOf(entry.change)).toSorted(),
          ["m-1", "m-2", "m-3", "m-4", "m-5"]
        )
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )
})

const NoteSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
const Note = Model.make("Note", {
  version: 1,
  key: Schema.String,
  schema: NoteSchema,
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (note: typeof NoteSchema.Type) => note.title
      }]
    }
  }
})
const PutNote = Mutation.make("PutNote", { version: 1, payload: Note.schema })
const noteDefinition = Definition.make({ version: 1, models: [Note], mutations: [PutNote], queries: [] })
const layerNoteRuntime = MutationRuntime.layer(noteDefinition).pipe(
  Layer.provide(PutNote.toLayer(({ payload, transaction }) => transaction.set(Note, payload.id, payload)))
)

const putNote = Effect.fnUntraced(function*(sequence: number, payload: typeof NoteSchema.Type) {
  const identity = {
    spaceId,
    clientId: writerId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(sequence),
    basis: Identity.ServerSequence.make(0),
    name: PutNote.name,
    payload,
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: noteDefinition.schemaIdentity,
    mutationVersion: PutNote.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

describe.each(serverDatabases)("sort values that contain an unpaired surrogate ($dialect)", (database) => {
  const layerServices = Layer.mergeAll(database.layer(), NodeCrypto.layer, Reactivity.layer)
  const makeServer = ServerStore.layerTrusted({
    definition: noteDefinition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerNoteRuntime),
    Layer.provide(layerServices),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )

  it.effect(
    "window order and bounds place unpaired surrogates at their code point",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        const titles = ["\ud7ff", "\ud800", "\udbff", "\udc00", "\udfff", "\ue000", "\ufffd", "\ud83d\ude00"]
        for (let position = 0; position < titles.length; position++) {
          yield* server.submit(yield* putNote(position + 1, { id: `n-${position + 1}`, title: titles[position] }))
        }
        const scope = Protocol.ReplicationScope.make({
          models: [],
          windows: [
            Protocol.ReplicationWindow.make({
              model: Note.name,
              index: "byTitle",
              count: 1,
              partitions: [
                Protocol.ReplicationWindowPartition.make({
                  key: [],
                  bounds: Protocol.ReplicationWindowBounds.make({ gt: "\ud7ff", lt: "\ue000" })
                })
              ]
            })
          ]
        })
        const request = {
          spaceId,
          clientId: readerId,
          schema: noteDefinition.schemaIdentity,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          membershipIncarnation
        }
        const required = yield* server.pull(Protocol.PullRequest.make({ ...request, cursor: null, limit: 100 }))
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrap(Protocol.BootstrapRequest.make({
          ...request,
          scopeGeneration: required.manifest.scopeGeneration,
          cursor: required.manifest.cursor,
          snapshotId: required.manifest.snapshotId,
          afterOrdinal: -1,
          limit: 100
        }))
        assert.deepStrictEqual(
          page.entries.map((entry) => keyOf(entry.change)).toSorted(),
          ["n-2", "n-3", "n-4", "n-5", "n-8"]
        )
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )
})
