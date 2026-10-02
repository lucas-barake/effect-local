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
import { serverDatabases } from "./fixtures/ServerDatabase.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000601")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000601")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000602")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000601")

const MessageSchema = Schema.Struct({ id: Schema.String, chatId: Schema.String, sentAt: Schema.Number })
type MessageValue = typeof MessageSchema.Type

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
        extract: (message: MessageValue) => message.chatId
      }],
      sort: [{
        name: "sentAt",
        affinity: "real",
        schema: Schema.Number,
        extract: (message: MessageValue) => message.sentAt
      }]
    }
  }
})

const PutMessage = Mutation.make("PutMessage", { version: 1, payload: Message.schema })
const EditMessageTwice = Mutation.make("EditMessageTwice", { version: 1, payload: Message.schema })
const PostAndRetract = Mutation.make("PostAndRetract", { version: 1, payload: Message.schema })
const definition = Definition.make({
  version: 1,
  models: [Message],
  mutations: [PutMessage, EditMessageTwice, PostAndRetract]
})

const layerHandlers = Layer.mergeAll(
  PutMessage.toLayer(({ payload, transaction }) => transaction.set(Message, payload.id, payload)),
  EditMessageTwice.toLayer(({ payload, transaction }) =>
    transaction.set(Message, payload.id, { ...payload, sentAt: payload.sentAt - 1 }).pipe(
      Effect.andThen(transaction.set(Message, payload.id, payload))
    )
  ),
  PostAndRetract.toLayer(({ payload, transaction }) =>
    transaction.set(Message, payload.id, payload).pipe(Effect.andThen(transaction.delete(Message, payload.id)))
  )
)

const envelope = Effect.fnUntraced(function*(
  mutation: typeof PutMessage | typeof EditMessageTwice | typeof PostAndRetract,
  sequence: number,
  payload: MessageValue
) {
  const identity = {
    spaceId,
    clientId: writerId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(sequence),
    basis: Identity.ServerSequence.make(0),
    name: mutation.name,
    payload,
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: definition.schemaIdentity,
    mutationVersion: mutation.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

const provideNodeCrypto = Effect.provide(NodeCrypto.layer)

const windowOfOne = Protocol.ReplicationScope.make({
  models: [],
  windows: [Protocol.ReplicationWindow.make({ model: Message.name, index: "byChat", count: 1 })]
})

const pull = (server: ServerStore.Service, scope: Protocol.ReplicationScope) =>
  server.pullAuthorized(
    Protocol.PullRequest.make({
      spaceId,
      clientId: readerId,
      schema: definition.schemaIdentity,
      scope,
      membershipIncarnation,
      scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
      cursor: null,
      limit: 100
    }),
    "reader"
  ).pipe(
    Effect.map((page) => {
      if ("_tag" in page) return page._tag
      return "Page"
    }),
    Effect.catch((error) => Effect.succeed(error._tag))
  )

const outcomeOf = (submitted: Effect.Effect<Protocol.Receipt, { readonly _tag: string }>) =>
  submitted.pipe(
    Effect.map((receipt): string => receipt._tag),
    Effect.catch((error) => Effect.succeed(error._tag))
  )

describe.each(serverDatabases)("server index parity ($dialect)", (database) => {
  const layerServices = Layer.mergeAll(database.layer(), NodeCrypto.layer, Reactivity.layer)
  const makeServer = ServerStore.layer({
    definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () => Effect.void
  }).pipe(
    Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))),
    Layer.provide(layerServices),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )

  it.effect(
    "accepts a mutation that writes the same indexed entity twice",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        const message = { id: "m", chatId: "chat", sentAt: 1 }
        assert.strictEqual(yield* outcomeOf(server.submit(yield* envelope(PutMessage, 1, message))), "Accepted")
        const window = Protocol.ReplicationScope.make({
          models: [],
          windows: [Protocol.ReplicationWindow.make({ model: Message.name, index: "byChat", count: 1 })]
        })
        assert.strictEqual(yield* pull(server, window), "BootstrapRequired")
        const edited = yield* envelope(EditMessageTwice, 2, { ...message, sentAt: 5 })
        assert.strictEqual(yield* outcomeOf(server.submit(edited)), "Accepted")
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "serves a window partition whose text key holds an unpaired surrogate",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        const chatId = "chat-\ud83d"
        const message = { id: "m", chatId, sentAt: 1 }
        assert.strictEqual(yield* outcomeOf(server.submit(yield* envelope(PutMessage, 1, message))), "Accepted")
        const window = Protocol.ReplicationScope.make({
          models: [],
          windows: [Protocol.ReplicationWindow.make({
            model: Message.name,
            index: "byChat",
            count: 1,
            partitions: [Protocol.ReplicationWindowPartition.make({ key: [chatId], count: 2 })]
          })]
        })
        assert.strictEqual(yield* pull(server, window), "BootstrapRequired")
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "keeps a key out of its window when one mutation writes and then deletes it",
    Effect.fnUntraced(
      function*() {
        const server = yield* makeServer
        const kept = { id: "kept", chatId: "chat", sentAt: 1 }
        assert.strictEqual(yield* outcomeOf(server.submit(yield* envelope(PutMessage, 1, kept))), "Accepted")
        assert.strictEqual(yield* pull(server, windowOfOne), "BootstrapRequired")
        const retracted = yield* envelope(PostAndRetract, 2, { id: "retracted", chatId: "chat", sentAt: 5 })
        assert.strictEqual(yield* outcomeOf(server.submit(retracted)), "Accepted")
        const required = yield* server.pullAuthorized(
          Protocol.PullRequest.make({
            spaceId,
            clientId: readerId,
            schema: definition.schemaIdentity,
            scope: windowOfOne,
            membershipIncarnation,
            scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
            cursor: null,
            limit: 100
          }),
          "reader"
        )
        if (!("_tag" in required)) assert.fail("expected a window bootstrap")
        const page = yield* server.bootstrapAuthorized(
          Protocol.BootstrapRequest.make({
            spaceId,
            clientId: readerId,
            schema: definition.schemaIdentity,
            scope: windowOfOne,
            membershipIncarnation,
            scopeGeneration: required.manifest.scopeGeneration,
            cursor: required.manifest.cursor,
            snapshotId: required.manifest.snapshotId,
            afterOrdinal: -1,
            limit: 100
          }),
          "reader"
        )
        const keys = page.entries.map((entry) => entry.change.entity.key)
        assert.deepStrictEqual(keys, ["kept"])
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )
})
