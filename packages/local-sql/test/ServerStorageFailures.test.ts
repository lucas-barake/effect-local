import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/sql/SqlClient"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { type ServerDatabase, serverDatabases } from "./fixtures/ServerDatabase.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000702")
const unknownSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000703")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000702")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000702")

const envelope = Effect.fnUntraced(function*(localSequence: number) {
  const identity = {
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: Domain.PutTodo.name,
    payload: Domain.todo(`todo-${localSequence}`),
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: Domain.definition.schemaIdentity,
    mutationVersion: Domain.PutTodo.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
}, Effect.provide(NodeCrypto.layer))

const unexpectedSuccess = { _tag: "UnexpectedSuccess" } as const

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const buildStore = Effect.fnUntraced(function*(database: ServerDatabase) {
  const layerDatabase = Layer.mergeAll(database.layer(), NodeCrypto.layer)
  const context = yield* ServerStore.layerTrusted({
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provideMerge(layerDatabase),
    Layer.build
  )
  return { store: Context.get(context, ServerStore.ServerStore), sql: Context.get(context, SqlClient.SqlClient) }
})

describe.each(serverDatabases)("server storage failures ($dialect)", (database) => {
  it.effect(
    "reports a receipt row that no longer decodes as corrupt storage",
    Effect.fnUntraced(function*() {
      const { store, sql } = yield* buildStore(database)
      const submitted = yield* envelope(1)
      assert.strictEqual((yield* store.submit(submitted))._tag, "Accepted")
      yield* sql`UPDATE effect_local_server_receipts SET digest = 'not-a-digest' WHERE space_id = ${spaceId}`

      const error = yield* store.submit(submitted).pipe(Effect.flip)

      assert.strictEqual(error._tag, "StorageCorrupt")
      if (error._tag === "StorageCorrupt") assert.strictEqual(error.message, "Server receipt row is corrupt")
    })
  )

  it.effect(
    "maintains a space the server has never stored without failing",
    Effect.fnUntraced(function*() {
      const { store } = yield* buildStore(database)

      yield* store.maintain(unknownSpaceId)
    })
  )

  it.effect(
    "rejects a fractional bootstrap page limit as an invalid request",
    Effect.fnUntraced(function*() {
      const { store } = yield* buildStore(database)
      assert.strictEqual((yield* store.submit(yield* envelope(1)))._tag, "Accepted")
      const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
      const required = yield* store.pull(Protocol.PullRequest.make({
        spaceId,
        clientId,
        schema: Domain.definition.schemaIdentity,
        scope,
        membershipIncarnation,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null,
        limit: 10
      }))
      if (!("_tag" in required)) assert.fail("expected a required bootstrap")

      const error = yield* store.bootstrap({
        spaceId,
        clientId,
        schema: Domain.definition.schemaIdentity,
        scope,
        membershipIncarnation,
        scopeGeneration: required.manifest.scopeGeneration,
        cursor: required.manifest.cursor,
        snapshotId: required.manifest.snapshotId,
        afterOrdinal: -1,
        limit: 1.5
      }).pipe(Effect.as(unexpectedSuccess), Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
    })
  )
})
