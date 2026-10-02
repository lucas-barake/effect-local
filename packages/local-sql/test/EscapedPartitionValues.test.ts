import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
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
  Protocol.PullRequest.make({ ...context(scope), cursor, limit: 100 })

const bootstrapRequest = (scope: Protocol.ReplicationScope, manifest: Protocol.SnapshotManifest) =>
  Protocol.BootstrapRequest.make({
    ...context(scope),
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
