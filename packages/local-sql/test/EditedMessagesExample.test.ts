import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { chooseMessageRevision, EditedMessage } from "../examples/edited-messages.js"
import * as PeerSync from "../src/PeerSync.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("edited messages example", () => {
  const EditBody = Mutation.make("EditedMessage.EditBody", {
    document: EditedMessage,
    payload: Schema.String
  })
  const definition = ReplicaDefinition.make({
    name: "edited-messages-example",
    documents: DocumentSet.make(EditedMessage),
    mutations: [EditBody],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1024 * 1024,
    maxChunkBytes: 64 * 1024,
    maxArchiveRecords: 1000,
    maxJsonDepth: 32,
    maxConflictDepth: 64,
    maxConflictNodes: 100_000,
    maxConflictAlternatives: 10_000,
    maxConflictPathSegments: 128,
    maxConflictValueBytes: 16 * 1024 * 1024,
    maxConflictSourceChanges: 100_000,
    maxConflictSourceOperations: 100_000,
    maxConflictSourceBytes: 64 * 1024 * 1024,
    maxSyncMessageBytes: 64 * 1024,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1000,
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1024 * 1024,
    maxPendingBytesPerPeer: 1024 * 1024,
    maxPendingBytesPerReplica: 1024 * 1024,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1000,
    maxPendingChangesPerPeer: 1000,
    maxPendingChangesPerReplica: 1000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 8,
    maxStreamsPerSession: 8,
    maxInFlightPerSession: 32,
    maxQueuedRpc: 128,
    maxQueuedPermits: 128,
    maxActiveRestores: 128,
    maxRestoresPerSession: 32,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const infrastructure = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    ReplicaLimits.layer(limits),
    EditBody.toLayer(({ draft, payload }) => {
      draft.body = new Automerge.ImmutableString(payload)
      return undefined
    })
  )
  const services = SqlReplica.servicesLayerWithBindings(definition, { projections: [] })
  const sync = PeerSync.layer.pipe(Layer.provideMerge(services))
  const Live = SqlReplica.layerFromServices(definition).pipe(
    Layer.provideMerge(sync),
    Layer.provide(infrastructure)
  )
  const peerLeft = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000011")
  const peerRight = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000012")

  interface Side {
    readonly replica: Replica.Replica["Service"]
    readonly sql: SqlClient.SqlClient
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
      sql: Context.get(context, SqlClient.SqlClient),
      sync: Context.get(context, PeerSync.PeerSync)
    }
  })

  const seedPair = Effect.gen(function*() {
    const leftBuilt = yield* buildSide
    const rightBuilt = yield* buildSide
    const documentId = yield* leftBuilt.replica.create(EditedMessage, {
      commandId: yield* Identity.makeCommandId,
      value: { body: "base revision", editedAt: 0 }
    })
    const backup = yield* leftBuilt.replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(
      Stream.runCollect
    )
    yield* rightBuilt.replica.restoreBackup({
      expectedDefinitionHash: definition.hash,
      installationId: yield* Identity.makeBackupInstallationId,
      maxBytes: limits.maxBackupBytes,
      mode: "clone",
      source: Stream.fromIterable(backup)
    })
    const left: Side = {
      ...leftBuilt,
      session: yield* leftBuilt.sync.open(peerRight)
    }
    const right: Side = {
      ...rightBuilt,
      session: yield* rightBuilt.sync.open(peerLeft)
    }
    return { documentId, left, right }
  })

  const drain = (documentId: Identity.DocumentId, left: Side, right: Side) =>
    Effect.gen(function*() {
      const pending: Array<Packet> = []
      const enqueueGenerated = Effect.fnUntraced(function*(from: Side, to: Side) {
        const generated = yield* from.sync.generate(EditedMessage, documentId, from.session, {
          lineageAware: true
        })
        if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
        return generated
      })
      for (let round = 0; round < 32; round++) {
        yield* Effect.all([
          enqueueGenerated(left, right),
          enqueueGenerated(right, left)
        ])
        pending.reverse()
        while (pending.length > 0) {
          const packet = pending.shift()!
          const received = yield* packet.to.sync.receive(
            EditedMessage,
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
            const outbound = yield* packet.to.sync.enqueue(packet.to.session, received.reply)
            if (outbound !== null) {
              pending.push({
                from: packet.to,
                outbound,
                to: packet.from
              })
            }
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
      yield* Effect.die("edited message peers did not reach quiescence within 32 rounds")
    })

  const edit = (side: Side, documentId: Identity.DocumentId, body: string) =>
    Identity.makeCommandId.pipe(
      Effect.flatMap((commandId) =>
        side.replica.mutate(EditBody, {
          commandId,
          documentId,
          payload: body
        })
      )
    )

  it.effect("keeps atomic revisions intact and recovers the chosen revision through complete lookup", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair
      assert.strictEqual((yield* left.replica.get(EditedMessage, documentId)).value.body, "base revision")
      yield* Effect.all([
        edit(left, documentId, "left revision"),
        edit(right, documentId, "right revision")
      ], { concurrency: "unbounded" })
      yield* drain(documentId, left, right)

      const inspection = yield* left.replica.inspectConflicts(EditedMessage, documentId)
      const bodyConflict = inspection.conflicts.find((record) =>
        record.path.parents.length === 0 &&
        record.path.target._tag === "Key" &&
        record.path.target.key === "body"
      )
      assert.isDefined(bodyConflict)
      assert.isTrue(bodyConflict.alternatives.every(({ value }) => Automerge.isImmutableString(value)))
      assert.sameMembers(
        bodyConflict.alternatives.map(({ value }) => {
          if (!Automerge.isImmutableString(value)) return assert.fail("expected ImmutableString revision")
          return value.val
        }),
        ["left revision", "right revision"]
      )

      const pendingBeforeResolution = yield* left.sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_commit_outbox WHERE published = 0`
      yield* left.sql`CREATE TRIGGER fail_edited_message_outbox_delivery
        BEFORE UPDATE OF published ON effect_local_commit_outbox
        WHEN NEW.published = 1
        BEGIN SELECT RAISE(ABORT, 'delivery failed'); END`
      yield* chooseMessageRevision(documentId, (alternatives) => {
        const selected = alternatives.find(({ value }) =>
          Automerge.isImmutableString(value) && value.val === "right revision"
        )
        if (selected === undefined) return assert.fail("expected the right revision")
        return selected
      }).pipe(Effect.provideService(Replica.Replica, left.replica))

      const resolved = yield* left.replica.inspectConflicts(EditedMessage, documentId)
      assert.strictEqual(resolved.snapshot.value.body, "right revision")
      assert.deepStrictEqual(resolved.conflicts, [])
      const pending = yield* left.sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_commit_outbox WHERE published = 0`
      assert.strictEqual(pending[0].count, pendingBeforeResolution[0].count + 1)
    })).pipe(Effect.provide(NodeCrypto.layer)))
})
