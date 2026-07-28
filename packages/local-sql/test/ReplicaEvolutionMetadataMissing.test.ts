import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as DocumentStore from "../src/DocumentStore.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaEvolution from "../src/ReplicaEvolution.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { withGateLimits } from "./fixtures/limits.js"

const TaskV1 = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const TaskV2 = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
  version: 2,
  migrations: [
    Document.migration({
      from: 1,
      schema: Schema.Struct({ title: Schema.String }),
      migrate: (value) => ({ ...value, done: false })
    })
  ]
})

describe("ReplicaEvolution metadata-missing attribution", () => {
  const environment = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )

  // `ReplicaEvolution` quarantines the document it was reading when it sees `StorageCorrupt`
  // (ReplicaEvolution.ts:125-135). A lost replica identity is replica wide, so it must never land in
  // that arm: a healthy document would be durably marked `projection_status = 'Blocked'` and stay
  // invisible to every projection query even after the replica is repaired.
  it.effect("fails replica-wide instead of quarantining the document it was migrating", () =>
    Effect.gen(function*() {
      const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
      const definitionV2 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
      yield* Effect.scoped(Effect.gen(function*() {
        const context = yield* Layer.build(
          SqlReplica.layerWithBindings(definitionV1, { projections: [] }).pipe(withGateLimits)
        )
        const replica = Context.get(context, Replica.Replica)
        yield* replica.create(TaskV1, { commandId: yield* Identity.makeCommandId, value: { title: "one" } })
      }))

      const sql = yield* SqlClient.SqlClient
      const bootstrap = ReplicaBootstrap.layer(definitionV2)
      const gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provideMerge(bootstrap))
      const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
      const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
      const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))

      yield* Effect.scoped(Effect.gen(function*() {
        const context = yield* Layer.build(projections)
        yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
        const result = yield* Effect.result(ReplicaEvolution.make(definitionV2).pipe(Effect.provide(context)))
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure._tag, "ReplicaError")
          if (result.failure._tag === "ReplicaError") {
            assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
          }
        }
        const rows = yield* sql<{
          readonly projection_status: string
          readonly schema_version: number
        }>`SELECT projection_status, schema_version FROM effect_local_documents`
        assert.strictEqual(rows.length, 1)
        assert.strictEqual(rows[0]?.projection_status, "Ready")
        assert.strictEqual(rows[0]?.schema_version, 1)
      }))
    }).pipe(Effect.provide(environment)))
})
