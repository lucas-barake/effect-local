import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { gateLimits } from "./fixtures/limits.js"

/**
 * What a persisted cluster entity handler does when the replica loses its identity.
 *
 * This is the pairing issue #76 asked to be pinned. `Create`, `Mutate`, `Delete` and `ApplySync` are
 * all `ClusterSchema.Persisted` with a `primaryKey`, so the worry was that failing them typed would
 * durably record a terminal reply and replay it for that key forever. They are also
 * `ClusterSchema.WithTransaction`, which makes the reply write share the handler's transaction, so a
 * failing handler rolls it back: the caller is answered and NOTHING is recorded. Both halves are
 * asserted here, because either one alone would be satisfied by the wrong design.
 */
describe("DocumentEntity with a missing metadata singleton", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
  const definition = ReplicaDefinition.make({
    name: "metadata-missing",
    documents: DocumentSet.make(Task),
    mutations: [Rename],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Handler = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  })
  const Live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, ReplicaLimits.layer(gateLimits)))
  )

  const clusterRows = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    return yield* sql<{
      readonly tag: string
      readonly processed: number
      readonly replies: number
    }>`SELECT m.tag AS tag, m.processed AS processed,
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)} r
           WHERE r.request_id = m.request_id) AS replies
      FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)} m
      WHERE m.kind = 0
      ORDER BY m.id`
  })

  it.effect("answers the caller and records no terminal reply", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "one" }
      })

      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`

      // Bounded on purpose. Before the typed conversion this call never returned at all: the handler
      // died, `onDefect` rebuilt the entity and redelivered the request on a backoff forever, and no
      // reply was ever written. A timeout here would mean that behaviour is back.
      const result = yield* Effect.result(
        replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId,
          payload: "two"
        }).pipe(Effect.timeout("10 seconds"))
      )

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "ReplicaError")
        if (result.failure._tag === "ReplicaError") {
          assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
          if (result.failure.reason._tag === "ReplicaMetadataMissing") {
            assert.strictEqual(result.failure.reason.operation, "ReplicaGate.validate")
          }
        }
      }

      // The other half. `Mutate` must leave no reply row and stay unprocessed, so the command id is
      // not burned: a repaired replica can serve it instead of replaying a stored failure. The
      // `Create` above is the control - it succeeded, so it DID record a reply and is processed.
      assert.deepStrictEqual(yield* clusterRows, [
        { tag: "Create", processed: 1, replies: 1 },
        { tag: "Mutate", processed: 0, replies: 0 }
      ])

      // And no receipt, so the command id is not burned: the documented recovery for an uncertain
      // command reads this table directly through `gate.admit`, never through the entity.
      const receipts = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_command_receipts`
      assert.strictEqual(receipts[0]?.count, 1)
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive), 60_000)
})
