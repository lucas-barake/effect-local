import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as PresenceHub from "../src/PresenceHub.js"
import * as PrincipalAssertion from "../src/PrincipalAssertion.js"
import * as SpaceEntity from "../src/SpaceEntity.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const definition = Definition.make({ version: 1, models: [Todo], mutations: [] })
const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const runtime = MutationRuntime.layer(definition)
const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true }).pipe(
  (sqlite) => Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer)
)
const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )
class TestAuthorizationError extends Schema.TaggedErrorClass<TestAuthorizationError, Schema.JsonObject>(
  "@lucas-barake/effect-local-rpc/test/PrincipalAssertion/TestAuthorizationError"
)("TestAuthorizationError", { reason: Schema.String }) {}
const store = ServerStore.layer({
  definition,
  readAuthorizationRefreshInterval: "1 second",
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
  migration: { retryDelay: "1 millis", maximumAttempts: 8 },
  authorizeAccess: () => Effect.void,
  authorizeMutation: () => Effect.void,
  authorizeRead: ({ principal }) => {
    if (principal !== null && typeof principal === "object" && "subject" in principal) return Effect.void
    return Effect.fail(new TestAuthorizationError({ reason: "missing principal" }))
  }
}).pipe(Layer.provide(runtime), Layer.provide(database))

describe("principal assertions", () => {
  it.effect("rejects a forged internal assertion before reaching entity authorization", () => {
    return Effect.gen(function*() {
      const verifier = PrincipalAssertion.layerVerifier((assertion) => {
        if (assertion === PrincipalAssertion.PrincipalAssertion.make("trusted")) {
          return Effect.succeed({ subject: "reader" })
        }
        return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "forged assertion" }))
      })
      const handlers = SpaceEntity.layerHandlers({
        admissionMailboxCapacity: 32,
        readMailboxCapacity: 32,
        watchMailboxCapacity: 32,
        presencePublicationMailboxCapacity: 32,
        maximumConcurrentBootstrapAuthorizations: 16,
        maximumConcurrentBootstrapPagesPerSpace: 4,
        maximumConcurrentPresencePublicationsPerSpace: 16
      })
      const liveHandlers = handlers.pipe(
        Layer.provide(store),
        Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
        Layer.provide(verifier)
      )
      const makeClient = yield* Entity.makeTestClient(SpaceEntity.SpaceReadEntity, liveHandlers)
      const client = yield* makeClient(spaceId)
      const request = Protocol.PullRequest.make({
        spaceId,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null,
        limit: 10
      })

      const forged = yield* client.Pull({
        request,
        assertion: PrincipalAssertion.PrincipalAssertion.make("forged")
      }).pipe(failureOf)
      if (forged._tag !== "AuthorizationDenied") assert.fail("expected forged assertion denial")
      assert.strictEqual(forged.reason, "forged assertion")

      const accepted = yield* client.Pull({
        request,
        assertion: PrincipalAssertion.PrincipalAssertion.make("trusted")
      })
      assert.strictEqual("_tag" in accepted, true)
    }).pipe(
      Effect.provide(ShardingConfig.layer({
        shardsPerGroup: 32,
        entityMailboxCapacity: 32,
        entityTerminationTimeout: 0,
        entityMessagePollInterval: 5_000,
        sendRetryInterval: 100
      })),
      Effect.provide(NodeCrypto.layer)
    )
  })
})
