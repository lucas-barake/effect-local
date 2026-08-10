import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as Statement from "effect/unstable/sql/Statement"
import * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { withGateLimits } from "./fixtures/limits.js"

describe("CommandDeliveryStore", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "command-delivery-store",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })

  const instrumentedLayer = (
    observe: (statement: Statement.Statement<object>, rows: ReadonlyArray<object>) => void
  ) => {
    const database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const instrumented = Layer.effect(
      SqlClient.SqlClient,
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        const instrumentedSql: SqlClient.SqlClient = (...args: Array<any>) => {
          const value = sql(...args)
          if (value === undefined || typeof value.compile !== "function") return value
          return value.pipe(
            Effect.tap((rows: ReadonlyArray<object>) => Effect.sync(() => observe(value, rows)))
          )
        }
        return Object.assign(instrumentedSql, sql)
      })
    ).pipe(Layer.provide(database))
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const base = Layer.merge(database, bootstrap)
    const gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provideMerge(base))
    const store = CommandDeliveryStore.layer.pipe(
      Layer.provide(Layer.merge(gate, instrumented))
    )
    return Layer.mergeAll(base, gate, store)
  }

  it.effect("reads one aggregate row per destination", () => {
    let deliveryRowCount = -1
    const layer = instrumentedLayer((statement, rows) => {
      const [query] = statement.compile()
      if (
        query.includes("effect_local_command_delivery_changes") &&
        query.includes("effect_local_peer_relay_delivery_messages")
      ) {
        deliveryRowCount = rows.length
      }
    })

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const permit = yield* gate.current
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
      const changeHashes = Array.from(
        { length: 64 },
        (_, index) => index.toString(16).padStart(64, "0")
      )
      const messages = Array.from({ length: 16 }, (_, index) => {
        const suffix = (index + 1).toString(16).padStart(12, "0")
        return {
          replica_id: permit.replicaId,
          replica_incarnation: permit.incarnation,
          expected_local_tenant_id: "tenant",
          expected_local_subject_id: "sender",
          expected_local_peer_id: "peer_00000000-0000-4000-8000-000000000001",
          remote_tenant_id: "tenant",
          remote_subject_id: "receiver",
          remote_peer_id: "peer_00000000-0000-4000-8000-000000000002",
          relay_peer_id: "peer_00000000-0000-4000-8000-000000000003",
          relay_message_id: `rly_00000000-0000-4000-8000-${suffix}`,
          outer_envelope_digest: "a".repeat(64),
          sender_connection_epoch: "aggregate-read",
          sender_sequence: index,
          document_id: documentId,
          created_at: "2026-01-01T00:00:00.000Z",
          retry_deadline: "2026-01-02T00:00:00.000Z",
          relay_custody_accepted_at: `2026-01-01T00:00:${index.toString().padStart(2, "0")}.000Z`,
          sender_custody_unconfirmed_at: null
        }
      })

      yield* sql`INSERT INTO effect_local_command_receipts (
        replica_incarnation, command_id, request_hash, mutation_name, result,
        document_id, heads, commit_sequence
      ) VALUES (
        ${permit.incarnation}, ${commandId}, 'request', 'Create', ${new Uint8Array()},
        ${documentId}, '[]', 1
      )`
      yield* sql`INSERT INTO effect_local_command_delivery_sources (
        replica_incarnation, command_id, document_id
      ) VALUES (${permit.incarnation}, ${commandId}, ${documentId})`
      yield* sql`INSERT INTO effect_local_command_delivery_changes ${
        sql.insert(changeHashes.map((changeHash) => ({
          replica_incarnation: permit.incarnation,
          command_id: commandId,
          change_hash: changeHash
        })))
      }`
      yield* sql`INSERT INTO effect_local_peer_relay_delivery_messages ${sql.insert(messages)}`
      const messageChanges = messages.flatMap((message) =>
        changeHashes.map((changeHash) => ({
          replica_incarnation: permit.incarnation,
          relay_message_id: message.relay_message_id,
          change_hash: changeHash
        }))
      )
      for (let offset = 0; offset < messageChanges.length; offset += 50) {
        yield* sql`INSERT INTO effect_local_peer_relay_delivery_changes ${
          sql.insert(messageChanges.slice(offset, offset + 50))
        }`
      }

      const delivery = yield* deliveries.lookup(commandId)
      assert.strictEqual(delivery._tag, "TrackedCommand")
      if (delivery._tag !== "TrackedCommand") return
      assert.strictEqual(deliveryRowCount, 1)
      assert.strictEqual(delivery.localChangeCount, changeHashes.length)
      assert.strictEqual(delivery.destinations.length, 1)
      const state = delivery.destinations[0]?.state
      assert.strictEqual(state?._tag, "RelayCustodyAccepted")
      if (state?._tag !== "RelayCustodyAccepted") return
      assert.strictEqual(state.acceptedChangeCount, changeHashes.length)
      assert.strictEqual(
        DateTime.formatIso(state.acceptedAt),
        "2026-01-01T00:00:15.000Z"
      )
    }).pipe(Effect.provide(layer))
  })

  it.effect("reports pending custody while a change has no relay message yet", () => {
    const layer = instrumentedLayer(() => {})

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const permit = yield* gate.current
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000003")
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000003")
      const carried = "a".repeat(64)
      const uncarried = "b".repeat(64)
      const relayMessageId = "rly_00000000-0000-4000-8000-000000000010"

      yield* sql`INSERT INTO effect_local_command_receipts (
        replica_incarnation, command_id, request_hash, mutation_name, result,
        document_id, heads, commit_sequence
      ) VALUES (
        ${permit.incarnation}, ${commandId}, 'request', 'Create', ${new Uint8Array()},
        ${documentId}, '[]', 1
      )`
      yield* sql`INSERT INTO effect_local_command_delivery_sources (
        replica_incarnation, command_id, document_id
      ) VALUES (${permit.incarnation}, ${commandId}, ${documentId})`
      yield* sql`INSERT INTO effect_local_command_delivery_changes ${
        sql.insert([carried, uncarried].map((change_hash) => ({
          replica_incarnation: permit.incarnation,
          command_id: commandId,
          change_hash
        })))
      }`
      yield* sql`INSERT INTO effect_local_peer_relay_delivery_messages ${
        sql.insert([{
          replica_id: permit.replicaId,
          replica_incarnation: permit.incarnation,
          expected_local_tenant_id: "tenant",
          expected_local_subject_id: "sender",
          expected_local_peer_id: "peer_00000000-0000-4000-8000-000000000001",
          remote_tenant_id: "tenant",
          remote_subject_id: "receiver",
          remote_peer_id: "peer_00000000-0000-4000-8000-000000000002",
          relay_peer_id: "peer_00000000-0000-4000-8000-000000000003",
          relay_message_id: relayMessageId,
          outer_envelope_digest: "c".repeat(64),
          sender_connection_epoch: "partial-coverage",
          sender_sequence: 0,
          document_id: documentId,
          created_at: "2026-01-01T00:00:00.000Z",
          retry_deadline: "2026-01-02T00:00:00.000Z",
          relay_custody_accepted_at: "2026-01-01T00:00:01.000Z",
          sender_custody_unconfirmed_at: null
        }])
      }`
      yield* sql`INSERT INTO effect_local_peer_relay_delivery_changes (
        replica_incarnation, relay_message_id, change_hash
      ) VALUES (${permit.incarnation}, ${relayMessageId}, ${carried})`

      const delivery = yield* deliveries.lookup(commandId)
      assert.strictEqual(delivery._tag, "TrackedCommand")
      if (delivery._tag !== "TrackedCommand") return
      assert.strictEqual(delivery.localChangeCount, 2)
      assert.strictEqual(delivery.destinations.length, 1)
      const state = delivery.destinations[0]?.state
      assert.strictEqual(state?._tag, "PendingRelayCustody")
      if (state?._tag !== "PendingRelayCustody") return
      assert.strictEqual(state.acceptedChangeCount, 1)
      assert.strictEqual(state.pendingChangeCount, 1)

      // Browser replicas return this value over the owner rpc, whose success schema is
      // `CommandDelivery.CommandDelivery`, so anything `lookup` reports has to encode.
      yield* Schema.encodeUnknownEffect(CommandDelivery.CommandDelivery)(delivery)
    }).pipe(Effect.provide(layer))
  })

  it.effect("counts every non accepted change as pending when coverage mixes message states", () => {
    const layer = instrumentedLayer(() => {})

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const permit = yield* gate.current
      const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000004")
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000004")
      const acceptedChange = "a".repeat(64)
      const pendingChange = "b".repeat(64)
      const uncarriedChange = "c".repeat(64)
      const message = (suffix: string, senderSequence: number, acceptedAt: string | null) => ({
        replica_id: permit.replicaId,
        replica_incarnation: permit.incarnation,
        expected_local_tenant_id: "tenant",
        expected_local_subject_id: "sender",
        expected_local_peer_id: "peer_00000000-0000-4000-8000-000000000001",
        remote_tenant_id: "tenant",
        remote_subject_id: "receiver",
        remote_peer_id: "peer_00000000-0000-4000-8000-000000000002",
        relay_peer_id: "peer_00000000-0000-4000-8000-000000000003",
        relay_message_id: `rly_00000000-0000-4000-8000-${suffix}`,
        outer_envelope_digest: "d".repeat(64),
        sender_connection_epoch: "mixed-coverage",
        sender_sequence: senderSequence,
        document_id: documentId,
        created_at: "2026-01-01T00:00:00.000Z",
        retry_deadline: "2026-01-02T00:00:00.000Z",
        relay_custody_accepted_at: acceptedAt,
        sender_custody_unconfirmed_at: null
      })

      yield* sql`INSERT INTO effect_local_command_receipts (
        replica_incarnation, command_id, request_hash, mutation_name, result,
        document_id, heads, commit_sequence
      ) VALUES (
        ${permit.incarnation}, ${commandId}, 'request', 'Create', ${new Uint8Array()},
        ${documentId}, '[]', 1
      )`
      yield* sql`INSERT INTO effect_local_command_delivery_sources (
        replica_incarnation, command_id, document_id
      ) VALUES (${permit.incarnation}, ${commandId}, ${documentId})`
      yield* sql`INSERT INTO effect_local_command_delivery_changes ${
        sql.insert([acceptedChange, pendingChange, uncarriedChange].map((change_hash) => ({
          replica_incarnation: permit.incarnation,
          command_id: commandId,
          change_hash
        })))
      }`
      yield* sql`INSERT INTO effect_local_peer_relay_delivery_messages ${
        sql.insert([
          message("000000000011", 0, "2026-01-01T00:00:01.000Z"),
          message("000000000012", 1, null)
        ])
      }`
      yield* sql`INSERT INTO effect_local_peer_relay_delivery_changes (
        replica_incarnation, relay_message_id, change_hash
      ) VALUES
        (${permit.incarnation}, 'rly_00000000-0000-4000-8000-000000000011', ${acceptedChange}),
        (${permit.incarnation}, 'rly_00000000-0000-4000-8000-000000000012', ${pendingChange})`

      const delivery = yield* deliveries.lookup(commandId)
      assert.strictEqual(delivery._tag, "TrackedCommand")
      if (delivery._tag !== "TrackedCommand") return
      assert.strictEqual(delivery.localChangeCount, 3)
      assert.strictEqual(delivery.destinations.length, 1)
      const state = delivery.destinations[0]?.state
      assert.strictEqual(state?._tag, "PendingRelayCustody")
      if (state?._tag !== "PendingRelayCustody") return
      assert.strictEqual(state.acceptedChangeCount, 1)
      // One change waits on a pending message and one has no message yet. Both await custody.
      assert.strictEqual(state.pendingChangeCount, 2)
    }).pipe(Effect.provide(layer))
  })

  it.effect("reads the publisher cursor over a large event backlog", () => {
    const layer = instrumentedLayer(() => {})

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
      const permit = yield* gate.current
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
      yield* sql`WITH RECURSIVE events(id) AS (
        VALUES(1)
        UNION ALL
        SELECT id + 1 FROM events WHERE id < 10000
      )
      INSERT INTO effect_local_command_delivery_events (
        replica_incarnation, command_id, document_id, published
      )
      SELECT ${permit.incarnation}, NULL, ${documentId}, 0 FROM events`
      yield* sql`ANALYZE`

      const cursor = yield* deliveries.cursor
      assert.strictEqual(cursor.sequence, 10_000)
    }).pipe(Effect.provide(layer))
  })
})
