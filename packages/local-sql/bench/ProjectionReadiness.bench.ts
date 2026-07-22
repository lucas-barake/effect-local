import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Effect from "effect/Effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { afterAll, bench } from "vitest"
import * as Migrations from "../src/Migrations.js"

const runtime = ManagedRuntime.make(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))

await runtime.runPromise(Effect.gen(function*() {
  yield* Migrations.run
  const sql = yield* SqlClient.SqlClient
  yield* sql`INSERT INTO effect_local_projection_registry (
    projection_name, table_name, projection_version, schema_checksum, status
  ) VALUES
    ('projection_1', 'projection_1_v1', 1, 'checksum_1', 'Ready'),
    ('projection_2', 'projection_2_v1', 1, 'checksum_2', 'Ready'),
    ('projection_3', 'projection_3_v1', 1, 'checksum_3', 'Ready'),
    ('projection_4', 'projection_4_v1', 1, 'checksum_4', 'Ready')`
  yield* sql`WITH RECURSIVE sequence(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1 FROM sequence WHERE value < 100000
  )
  INSERT INTO effect_local_documents (
    document_id, document_type, schema_version, observed_versions, materialized_heads,
    accepted_heads, tombstone, projection_status, checkpoint_hash
  )
  SELECT 'document_' || value, 'Task', 1, '[]', '[]', '[]', 0, 'Ready', NULL
  FROM sequence`
  yield* sql`INSERT INTO effect_local_document_projections (
    document_id, projection_name, projected_heads, status
  )
  SELECT document_id, projection_name, '[]', 'Ready'
  FROM effect_local_documents
  CROSS JOIN effect_local_projection_registry`
  yield* sql`UPDATE effect_local_document_projections
    SET status = 'Blocked'
    WHERE document_id = 'document_100000' AND projection_name = 'projection_1'`
}))

const countBlocked = SqlClient.SqlClient.use((sql) =>
  sql<{ readonly count: number }>`SELECT COUNT(*) AS count
    FROM effect_local_document_projections
    WHERE projection_name = 'projection_1' AND status != 'Ready'`
)

const transitionProjectionStatus = SqlClient.SqlClient.use((sql) =>
  Effect.gen(function*() {
    yield* sql`UPDATE effect_local_document_projections
      SET status = 'Blocked'
      WHERE document_id = 'document_99999' AND projection_name = 'projection_2'`
    yield* sql`UPDATE effect_local_document_projections
      SET status = 'Ready'
      WHERE document_id = 'document_99999' AND projection_name = 'projection_2'`
  })
)

bench("projection readiness across 400k rows", async () => {
  await runtime.runPromise(countBlocked)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 20,
  warmupTime: 0
})

bench("projection status transition across 400k rows", async () => {
  await runtime.runPromise(transitionProjectionStatus)
}, {
  iterations: 500,
  time: 0,
  warmupIterations: 20,
  warmupTime: 0
})

afterAll(() => runtime.dispose())
