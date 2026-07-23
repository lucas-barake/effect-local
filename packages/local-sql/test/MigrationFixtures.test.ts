import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { FileSystem } from "effect/FileSystem"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { tables } from "../src/internal/schema.js"
import * as Migrations from "../src/Migrations.js"
import type { FixtureSpec } from "./fixtures/versions.js"
import { documentId, fixturePath, fixtures, outboxMessageHash } from "./fixtures/versions.js"

const readinessIndexes: ReadonlyArray<string> = [
  "effect_local_checkpoints_document_verified_sequence",
  "effect_local_commit_outbox_published_sequence",
  "effect_local_document_projections_not_ready",
  "effect_local_documents_type_projection_status",
  "effect_local_peer_outbox_incarnation_created",
  "effect_local_peer_receipts_incarnation_accepted",
  "effect_local_projection_registry_name_status"
]

const assertUpgradesToCurrentSchema = (spec: FixtureSpec) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient

    const applied = yield* Migrations.run
    assert.deepStrictEqual(applied, spec.expectedApplied)

    const presentTables = new Set(
      (yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect_local_%'
      `).map((row) => row.name)
    )
    for (const table of tables) assert.isTrue(presentTables.has(table), `missing table ${table}`)

    const indexes = yield* sql<{ readonly name: string }>`
      SELECT name FROM sqlite_master WHERE type = 'index' AND name IN ${sql.in(readinessIndexes)}
    `
    assert.deepStrictEqual(indexes.map((row) => row.name).toSorted(), readinessIndexes)

    const catalog = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ migration_id: Schema.Int, name: Schema.String, checksum: Schema.String }),
      execute: () => sql`SELECT migration_id, name, checksum FROM effect_local_migration_catalog ORDER BY migration_id`
    })(undefined)
    assert.deepStrictEqual(catalog, [
      { migration_id: 1, name: "canonical_store", checksum: Migrations.canonicalStoreChecksum },
      { migration_id: 2, name: "peer_sync", checksum: Migrations.peerSyncChecksum },
      { migration_id: 3, name: "durability_indexes", checksum: Migrations.durabilityIndexesChecksum },
      { migration_id: 4, name: "projection_readiness", checksum: Migrations.projectionReadinessChecksum }
    ])

    const documents = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ document_id: Schema.String, document_type: Schema.String }),
      execute: () => sql`SELECT document_id, document_type FROM effect_local_documents`
    })(undefined)
    assert.deepStrictEqual(documents, [{ document_id: documentId, document_type: "Task" }])

    const checkpoints = yield* sql<{ readonly checkpoint_hash: string }>`
      SELECT checkpoint_hash FROM effect_local_checkpoints WHERE document_id = ${documentId}
      ORDER BY checkpoint_hash
    `
    assert.deepStrictEqual(checkpoints.map((row) => row.checkpoint_hash), spec.expectedCheckpoints)

    const outbox = yield* sql<{ readonly created_at: string }>`
      SELECT created_at FROM effect_local_peer_outbox WHERE message_hash = ${outboxMessageHash}
    `
    if (spec.outbox === "none") {
      assert.strictEqual(outbox.length, 0)
    } else if (spec.outbox === "backfilled") {
      assert.match(outbox[0]?.created_at ?? "", /^\d{4}-\d{2}-\d{2}T/)
    } else {
      assert.strictEqual(outbox[0]?.created_at, spec.outbox.frozen)
    }
  })

describe("Migrations against frozen released-version fixtures", () => {
  for (const spec of fixtures) {
    it.effect(`upgrades the version ${spec.version} database to the current schema`, () =>
      Effect.gen(function*() {
        const fs = yield* FileSystem
        const dir = yield* fs.makeTempDirectoryScoped()
        const database = `${dir}/${spec.file}`
        yield* fs.copyFile(fixturePath(spec.file), database)
        yield* assertUpgradesToCurrentSchema(spec).pipe(
          Effect.provide(SqliteClient.layer({ filename: database, disableWAL: true }))
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)))
  }
})
