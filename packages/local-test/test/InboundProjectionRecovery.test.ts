import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as TestReplica from "../src/TestReplica.js"

const Note = Document.make("RecoveryNote", {
  schema: Schema.Struct({ text: Schema.String, revisions: Schema.Number }),
  version: 1
})

const EditNote = Mutation.make("RecoveryNote.Edit", {
  document: Note,
  payload: Schema.String
})

const Touch = Mutation.make("RecoveryNote.Touch", {
  document: Note
})

const NoteRow = Schema.Struct({
  sourceDocumentId: Identity.DocumentId,
  text: Schema.String
})

const NoteRows = Projection.make("RecoveryNoteRows", {
  document: Note,
  version: 1,
  Row: NoteRow,
  key: (row) => row.sourceDocumentId,
  project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, text: snapshot.value.text }]
})

const ListNotes = Query.make("RecoveryNote.List", {
  success: Schema.Array(NoteRow),
  dependsOn: [NoteRows]
})

const definition = ReplicaDefinition.make({
  name: "inbound-projection-recovery",
  documents: DocumentSet.make(Note),
  mutations: [EditNote, Touch],
  projections: [NoteRows],
  queries: [ListNotes]
})

const NoteRowsSql = SqlProjection.make(NoteRows, {
  table: "recovery_note_rows_v1",
  migrations: [{
    id: 1,
    name: "recovery_note_rows_v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT PRIMARY KEY,
        text TEXT NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (source_document_id, text)
      VALUES (${row.sourceDocumentId}, ${row.text})`.pipe(Effect.asVoid)
})

const ListNotesSql = SqlSchema.findAll({
  Request: Schema.Void,
  Result: NoteRow,
  execute: () =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT source_document_id AS sourceDocumentId, text FROM recovery_note_rows_v1 ORDER BY source_document_id`
    )
})

const Handlers = Layer.mergeAll(
  EditNote.toLayer(({ draft, payload }) => {
    draft.text = payload
    return undefined
  }),
  Touch.toLayer(({ draft }) => {
    draft.revisions = draft.revisions + 1
    return undefined
  }),
  ListNotes.toLayer(() => ListNotesSql(undefined).pipe(Effect.orDie))
)

// The query handler reads the projection table through the same SqlClient instance the replica
// writes, so the database layer is shared explicitly instead of using the TestReplica sugar.
const SqliteMem = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
const Live = SqlReplica.layerWithBindings(definition, { projections: [NoteRowsSql] }).pipe(
  Layer.provide(Handlers.pipe(Layer.provide(SqliteMem))),
  Layer.provideMerge(Layer.mergeAll(
    SqliteMem,
    NodeCrypto.layer,
    ReplicaLimits.layer(TestReplica.defaultLimits)
  ))
)

const peerLeft = Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000f1")
const peerRight = Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000f2")

interface Side {
  readonly replica: Replica.Replica["Service"]
  readonly sync: PeerSync.PeerSync["Service"]
  session: PeerSync.Session
}

interface Packet {
  readonly from: Side
  readonly outbound: PeerSync.Outbound
  readonly to: Side
}

const buildSide = Effect.gen(function*() {
  const context = yield* Layer.build(Live)
  return {
    replica: Context.get(context, Replica.Replica),
    sync: Context.get(context, PeerSync.PeerSync)
  }
})

const seedPair = Effect.gen(function*() {
  const leftBuilt = yield* buildSide
  const rightBuilt = yield* buildSide
  const created = yield* leftBuilt.replica.create(Note, {
    commandId: (yield* Identity.makeCommandId),
    value: { text: "base", revisions: 0 }
  })
  const backup = yield* leftBuilt.replica.exportBackup({ maxBytes: TestReplica.defaultLimits.maxBackupBytes }).pipe(
    Stream.runCollect
  )
  yield* rightBuilt.replica.restoreBackup({
    expectedDefinitionHash: definition.hash,
    installationId: yield* Identity.makeBackupInstallationId,
    maxBytes: TestReplica.defaultLimits.maxBackupBytes,
    mode: "clone",
    source: Stream.fromIterable(backup)
  })
  const left: Side = { ...leftBuilt, session: yield* leftBuilt.sync.open(peerRight) }
  const right: Side = { ...rightBuilt, session: yield* rightBuilt.sync.open(peerLeft) }
  return { documentId: created, left, right }
})

const drain = (documentId: Identity.DocumentId, left: Side, right: Side) =>
  Effect.gen(function*() {
    const pending: Array<Packet> = []
    const enqueueGenerated = Effect.fnUntraced(function*(from: Side, to: Side) {
      const generated = yield* from.sync.generate(Note, documentId, from.session, { lineageAware: true })
      if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
      return generated
    })
    yield* enqueueGenerated(left, right)
    yield* enqueueGenerated(right, left)
    for (let round = 0; round < 32; round++) {
      while (pending.length > 0) {
        const packet = pending.shift()!
        const received = yield* packet.to.sync.receive(
          Note,
          documentId,
          packet.to.session,
          {
            remoteConnectionEpoch: packet.from.session.connectionEpoch,
            receiveSequence: packet.outbound.sendSequence,
            lineage: packet.outbound.lineage,
            message: packet.outbound.message,
            writerProvenance: packet.outbound.writerProvenance
          }
        )
        yield* packet.from.sync.markSent(
          packet.from.session,
          packet.outbound.sendSequence,
          packet.outbound.messageHash
        )
        if (received.reply !== null) {
          pending.push({
            from: packet.to,
            outbound: yield* packet.to.sync.enqueue(packet.to.session, received.reply),
            to: packet.from
          })
        }
      }
      const [fromLeft, fromRight] = yield* Effect.all([
        enqueueGenerated(left, right),
        enqueueGenerated(right, left)
      ])
      if (
        pending.length === 0 &&
        fromLeft.outbound === null &&
        fromRight.outbound === null &&
        !fromLeft.dirty &&
        !fromRight.dirty
      ) return
    }
    return yield* Effect.die("peer sync did not reach quiescence within 32 rounds")
  })

it.layer(NodeCrypto.layer)("inbound projection recovery", (it) => {
  it.effect("a local command after inbound sync leaves queries executable", () =>
    Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair
      yield* Effect.flatMap(Identity.makeCommandId, (commandId) =>
        left.replica.mutate(EditNote, { commandId, documentId, payload: "from left" }))
      yield* drain(documentId, left, right)

      // The inbound apply defers projection work by marking the document Blocked. A local command
      // rebuilds this document's projection rows from the merged snapshot, which must also make
      // the document queryable again.
      yield* Effect.flatMap(Identity.makeCommandId, (commandId) =>
        right.replica.mutate(Touch, { commandId, documentId }))

      const rows = yield* right.replica.query(ListNotes)
      assert.deepStrictEqual(
        rows.map((row) => row.text),
        ["from left"]
      )
    }).pipe(Effect.scoped))
})
