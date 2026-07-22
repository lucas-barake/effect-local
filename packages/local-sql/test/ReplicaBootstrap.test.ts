import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"

describe("ReplicaBootstrap", () => {
  const Task = Document.make("Task", { schema: Schema.String, version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })

  it.effect("runs migrations before atomically claiming writer generations", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const second = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(first.replicaId, second.replicaId)
      assert.strictEqual(first.writerGeneration, 1)
      assert.strictEqual(second.writerGeneration, 2)
      assert.strictEqual(second.incarnation, 0)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("reports corrupt persisted identity through the typed error channel", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET replica_id = 'invalid' WHERE singleton = 1`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects missing metadata in a populated migrated database", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
      const generations = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_writer_generations
      `
      assert.strictEqual(generations[0]?.count, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects an incompatible replica definition without modifying metadata", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const TaskV2 = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 2 })
      const incompatible = ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(TaskV2),
        mutations: [],
        projections: [],
        queries: []
      })

      const result = yield* Effect.result(ReplicaBootstrap.make(incompatible))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "ProtocolMismatch")
      }
      const sql = yield* SqlClient.SqlClient
      const metadata = yield* sql<{ readonly definition_hash: string; readonly writer_generation: number }>`
        SELECT definition_hash, writer_generation FROM effect_local_metadata WHERE singleton = 1
      `
      assert.deepStrictEqual(metadata, [{
        definition_hash: definition.hash,
        writer_generation: first.writerGeneration
      }])
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))
})
