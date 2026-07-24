import * as Effect from "effect/Effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import { fileURLToPath } from "node:url"

const documentId = "task-1"
const outboxMessageHash = "message-1"

const frozenOutboxCreatedAt = "2020-01-01T00:00:00.000Z"
const outboxMessage = new Uint8Array([1])

export const fixtureDirectory = fileURLToPath(new URL(".", import.meta.url))
export const fixturePath = (file: string): string => fileURLToPath(new URL(`./${file}`, import.meta.url))

type Seed = Effect.Effect<void, SqlError.SqlError, SqlClient.SqlClient>

const seedDocument = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`INSERT INTO effect_local_documents (
    document_id, document_type, schema_version, observed_versions, materialized_heads,
    accepted_heads, tombstone, projection_status, checkpoint_hash
  ) VALUES (${documentId}, 'Task', 1, '[]', '[]', '[]', 0, 'ready', NULL)`
})

const seedCheckpoint = (hash: string, sequence: number, verified: 0 | 1) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`INSERT INTO effect_local_checkpoints (
      checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
    ) VALUES (
      ${hash}, ${documentId}, '[]', ${new Uint8Array([sequence])}, ${`checksum-${sequence}`}, ${sequence}, ${verified}
    )`
  })

// A version-one application predates checkpoint retention, so it can hold more than two per document.
const seedV1: Seed = Effect.gen(function*() {
  yield* seedDocument
  yield* seedCheckpoint("checkpoint-1", 1, 1)
  yield* seedCheckpoint("checkpoint-2", 2, 0)
  yield* seedCheckpoint("checkpoint-3", 3, 1)
  yield* seedCheckpoint("checkpoint-4", 4, 0)
})

const seedV2: Seed = Effect.gen(function*() {
  yield* seedV1
  const sql = yield* SqlClient.SqlClient
  yield* sql`INSERT INTO effect_local_peer_outbox (
    replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
    message, message_hash, heads, status
  ) VALUES (0, 'peer-1', 'connection-1', ${documentId}, 1, ${outboxMessage}, ${outboxMessageHash}, '[]', 'pending')`
})

// A version-three application has already run the retention migration, so it keeps two checkpoints and
// carries an explicit outbox timestamp rather than the empty-string default backfilled during upgrade.
const seedV3: Seed = Effect.gen(function*() {
  yield* seedDocument
  yield* seedCheckpoint("checkpoint-1", 1, 1)
  yield* seedCheckpoint("checkpoint-3", 3, 1)
  const sql = yield* SqlClient.SqlClient
  yield* sql`INSERT INTO effect_local_peer_outbox (
    replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
    message, message_hash, heads, status, created_at
  ) VALUES (
    0, 'peer-1', 'connection-1', ${documentId}, 1, ${outboxMessage}, ${outboxMessageHash}, '[]', 'pending',
    ${frozenOutboxCreatedAt}
  )`
})

export type HistoricalVersion = 1 | 2 | 3

export interface FixtureSpec {
  readonly version: HistoricalVersion
  readonly file: string
  readonly appliedThroughId: number
  readonly seed: Seed
}

export const fixtures: ReadonlyArray<FixtureSpec> = [
  {
    version: 1,
    file: "v1_canonical_store.db",
    appliedThroughId: 1,
    seed: seedV1
  },
  {
    version: 2,
    file: "v2_peer_sync.db",
    appliedThroughId: 2,
    seed: seedV2
  },
  {
    version: 3,
    file: "v3_durability_indexes.db",
    appliedThroughId: 3,
    seed: seedV3
  }
]
