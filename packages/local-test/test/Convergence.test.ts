import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { FastCheck } from "effect/testing"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as TestReplica from "../src/TestReplica.js"

const Message = Document.make("ConvergenceMessage", {
  schema: Schema.Struct({ message: Schema.String, labels: Schema.Array(Schema.String) }),
  version: 1
})

const EditMessage = Mutation.make("ConvergenceMessage.Edit", {
  document: Message,
  payload: Schema.String
})

const AddLabel = Mutation.make("ConvergenceMessage.AddLabel", {
  document: Message,
  payload: Schema.String
})

const definition = ReplicaDefinition.make({
  name: "convergence",
  documents: DocumentSet.make(Message),
  mutations: [EditMessage, AddLabel],
  projections: [],
  queries: []
})

const Handlers = Layer.mergeAll(
  EditMessage.toLayer(({ draft, payload }) => {
    draft.message = payload
    return undefined
  }),
  AddLabel.toLayer(({ draft, payload }) => {
    draft.labels.push(payload)
    return undefined
  })
)

const makeLive = (database?: Parameters<typeof TestReplica.layerWithSync>[1]["database"]) =>
  TestReplica.layerWithSync(definition, {
    ...(database === undefined ? {} : { database }),
    projections: []
  }).pipe(Layer.provide(Handlers))

const Live = makeLive()

const peerLeft = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const peerRight = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

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

const buildSide = (live = Live) =>
  Effect.gen(function*() {
    const context = yield* Layer.build(live)
    return {
      replica: Context.get(context, Replica.Replica),
      sync: Context.get(context, PeerSync.PeerSync)
    }
  })

const seedPair = (
  layers: {
    readonly left?: typeof Live
    readonly right?: typeof Live
  } = {}
) =>
  Effect.gen(function*() {
    const leftBuilt = yield* buildSide(layers.left)
    const rightBuilt = yield* buildSide(layers.right)
    const created = yield* leftBuilt.replica.create(Message, {
      commandId: (yield* Identity.makeCommandId),
      value: { message: "base", labels: [] }
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
    const left: Side = {
      ...leftBuilt,
      session: yield* leftBuilt.sync.open(peerRight)
    }
    const right: Side = {
      ...rightBuilt,
      session: yield* rightBuilt.sync.open(peerLeft)
    }
    return { documentId: created, left, right }
  })

const drain = (documentId: Identity.DocumentId, left: Side, right: Side, reverse: boolean) =>
  Effect.gen(function*() {
    const pending: Array<Packet> = []
    const enqueueGenerated = Effect.fnUntraced(function*(from: Side, to: Side) {
      const generated = yield* from.sync.generate(Message, documentId, from.session, { lineageAware: true })
      if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
      return generated
    })
    const firstLeft = yield* enqueueGenerated(left, right)
    const firstRight = yield* enqueueGenerated(right, left)
    assert.isAbove(pending.length, 0)
    const [heldLeft, heldRight] = yield* Effect.all([
      enqueueGenerated(left, right),
      enqueueGenerated(right, left)
    ])
    if (firstLeft.outbound !== null) assert.isTrue(heldLeft.dirty)
    if (firstRight.outbound !== null) assert.isTrue(heldRight.dirty)
    assert.isNull(heldLeft.outbound)
    assert.isNull(heldRight.outbound)
    if (reverse) pending.reverse()
    if (pending[0] !== undefined) pending.splice(1, 0, pending[0])
    for (let round = 0; round < 32; round++) {
      while (pending.length > 0) {
        const packet = pending.shift()!
        for (const entry of packet.outbound.writerProvenance) {
          assert.strictEqual(entry.writerSchemaVersion, Message.version)
          assert.strictEqual(entry.writerDefinitionHash, definition.hash)
        }
        const received = yield* packet.to.sync.receive(
          Message,
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
      ) return round + 1
    }
    return yield* Effect.die("peer sync did not reach quiescence within 32 rounds")
  })

const mutate = (
  side: Side,
  mutation: typeof EditMessage | typeof AddLabel,
  documentId: Identity.DocumentId,
  payload: string
) =>
  Effect.flatMap(Identity.makeCommandId, (commandId) =>
    side.replica.mutate(mutation, {
      commandId,
      documentId,
      payload
    }))

const inspectMessageConflict = (
  inspection: Conflict.Inspection<{ readonly message: string; readonly labels: ReadonlyArray<string> }>
) => {
  const record = inspection.conflicts.find(
    ({ path }) =>
      path.parents.length === 0 &&
      path.target._tag === "Key" &&
      path.target.key === "message"
  )
  if (record === undefined) throw new Error("expected a message conflict")
  return record
}

const selectMessage = (
  inspection: Conflict.Inspection<{ readonly message: string; readonly labels: ReadonlyArray<string> }>,
  value: string
): Conflict.Resolution => {
  const record = inspectMessageConflict(inspection)
  const matching = record.alternatives.filter((alternative) => alternative.value === value)
  if (matching.length !== 1) throw new Error(`expected exactly one message alternative for ${value}`)
  return {
    heads: [...inspection.snapshot.heads],
    path: record.path,
    choice: {
      _tag: "SelectAlternative",
      alternativeId: matching[0]!.id
    }
  }
}

const createMessageConflict = (
  documentId: Identity.DocumentId,
  left: Side,
  right: Side,
  options: {
    readonly left: string
    readonly right: string
    readonly reverse: boolean
  }
) =>
  Effect.all([
    mutate(left, EditMessage, documentId, options.left),
    mutate(right, EditMessage, documentId, options.right)
  ], { concurrency: "unbounded" }).pipe(
    Effect.andThen(drain(documentId, left, right, options.reverse))
  )

it.layer(NodeCrypto.layer)("two replica convergence", (it) => {
  it.effect("converges a tombstone with a concurrent list edit after reordered duplicate delivery", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      yield* Effect.all([
        left.replica.delete(Message, { commandId: (yield* Identity.makeCommandId), documentId }),
        mutate(right, AddLabel, documentId, "concurrent")
      ], { concurrency: "unbounded" })
      const rounds = yield* drain(documentId, left, right, true)
      assert.isAtMost(rounds, 32)
      const leftSnapshot = yield* left.replica.get(Message, documentId)
      const rightSnapshot = yield* right.replica.get(Message, documentId)
      assert.deepStrictEqual(leftSnapshot.heads.toSorted(), rightSnapshot.heads.toSorted())
      assert.isTrue(leftSnapshot.tombstone)
      assert.isTrue(rightSnapshot.tombstone)
      assert.deepStrictEqual(leftSnapshot.value, { message: "base", labels: ["concurrent"] })
      assert.deepStrictEqual(leftSnapshot.value, rightSnapshot.value)
    })))

  it.effect("inspects identical alternatives on converged replicas", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      yield* createMessageConflict(documentId, left, right, {
        left: "same",
        right: "same",
        reverse: false
      })
      const leftInspection = yield* left.replica.inspectConflicts(Message, documentId)
      const rightInspection = yield* right.replica.inspectConflicts(Message, documentId)
      assert.deepStrictEqual(leftInspection.snapshot.heads, rightInspection.snapshot.heads)
      assert.deepStrictEqual(leftInspection.conflicts, rightInspection.conflicts)
      const record = inspectMessageConflict(leftInspection)
      assert.lengthOf(record.alternatives, 2)
      assert.deepStrictEqual(record.alternatives.map(({ value }) => value), ["same", "same"])
      assert.strictEqual(
        record.alternatives.filter(({ id }) => id === record.visible).length,
        1
      )
    })))

  it.effect("inspects the same conflict after reordered duplicate delivery", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      const rounds = yield* createMessageConflict(documentId, left, right, {
        left: "left",
        right: "right",
        reverse: true
      })
      assert.isAtMost(rounds, 32)
      const leftInspection = yield* left.replica.inspectConflicts(Message, documentId)
      const rightInspection = yield* right.replica.inspectConflicts(Message, documentId)
      assert.deepStrictEqual(leftInspection.conflicts, rightInspection.conflicts)
      assert.sameMembers(
        inspectMessageConflict(leftInspection).alternatives.map(({ value }) => value),
        ["left", "right"]
      )
    })))

  it.effect("resolves a selected edited message and converges after reordered duplicate delivery", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      yield* createMessageConflict(documentId, left, right, {
        left: "left edit",
        right: "right edit",
        reverse: false
      })
      const resolution = selectMessage(
        yield* left.replica.inspectConflicts(Message, documentId),
        "right edit"
      )
      const commandId = yield* Identity.makeCommandId
      yield* left.replica.resolveConflict(Message, { commandId, documentId, resolution })
      const outcome = yield* left.replica.lookupConflictResolution(Message, {
        commandId,
        documentId,
        resolution
      })
      assert.strictEqual(outcome._tag, "DurablyCommittedLocal")
      assert.strictEqual(outcome.commandId, commandId)
      yield* drain(documentId, left, right, true)
      const leftInspection = yield* left.replica.inspectConflicts(Message, documentId)
      const rightInspection = yield* right.replica.inspectConflicts(Message, documentId)
      assert.strictEqual(leftInspection.snapshot.value.message, "right edit")
      assert.deepStrictEqual(leftInspection.snapshot, rightInspection.snapshot)
      assert.deepStrictEqual(leftInspection.conflicts, [])
      assert.deepStrictEqual(rightInspection.conflicts, [])
    })))

  it.effect("preserves concurrent conflict resolutions as a new conflict", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      yield* createMessageConflict(documentId, left, right, {
        left: "left",
        right: "right",
        reverse: true
      })
      const inspection = yield* left.replica.inspectConflicts(Message, documentId)
      const leftResolution = selectMessage(inspection, "left")
      const rightResolution = selectMessage(inspection, "right")
      const leftCommandId = yield* Identity.makeCommandId
      const rightCommandId = yield* Identity.makeCommandId
      yield* Effect.all([
        left.replica.resolveConflict(Message, {
          commandId: leftCommandId,
          documentId,
          resolution: leftResolution
        }),
        right.replica.resolveConflict(Message, {
          commandId: rightCommandId,
          documentId,
          resolution: rightResolution
        })
      ], { concurrency: "unbounded" })
      const [leftOutcome, rightOutcome] = yield* Effect.all([
        left.replica.lookupConflictResolution(Message, {
          commandId: leftCommandId,
          documentId,
          resolution: leftResolution
        }),
        right.replica.lookupConflictResolution(Message, {
          commandId: rightCommandId,
          documentId,
          resolution: rightResolution
        })
      ])
      assert.strictEqual(leftOutcome._tag, "DurablyCommittedLocal")
      assert.strictEqual(rightOutcome._tag, "DurablyCommittedLocal")
      yield* drain(documentId, left, right, true)
      const leftInspection = yield* left.replica.inspectConflicts(Message, documentId)
      const rightInspection = yield* right.replica.inspectConflicts(Message, documentId)
      assert.deepStrictEqual(leftInspection.conflicts, rightInspection.conflicts)
      assert.sameMembers(
        inspectMessageConflict(leftInspection).alternatives.map(({ value }) => value),
        ["left", "right"]
      )
    })))

  it.effect("reinspects and resolves after a durable stale rejection", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair()
      yield* createMessageConflict(documentId, left, right, {
        left: "left",
        right: "right",
        reverse: false
      })
      const staleResolution = selectMessage(
        yield* left.replica.inspectConflicts(Message, documentId),
        "left"
      )
      yield* mutate(left, AddLabel, documentId, "frontier advanced")
      const staleCommandId = yield* Identity.makeCommandId
      const staleError = yield* Effect.flip(
        left.replica.resolveConflict(Message, {
          commandId: staleCommandId,
          documentId,
          resolution: staleResolution
        })
      )
      assert.strictEqual(staleError._tag, "StaleConflictResolution")
      const staleOutcome = yield* left.replica.lookupConflictResolution(Message, {
        commandId: staleCommandId,
        documentId,
        resolution: staleResolution
      })
      assert.strictEqual(staleOutcome._tag, "Rejected")
      if (staleOutcome._tag === "Rejected") {
        assert.strictEqual(staleOutcome.error._tag, "StaleConflictResolution")
        assert.deepStrictEqual(staleOutcome.error, staleError)
      }

      const freshResolution = selectMessage(
        yield* left.replica.inspectConflicts(Message, documentId),
        "left"
      )
      const freshCommandId = yield* Identity.makeCommandId
      yield* left.replica.resolveConflict(Message, {
        commandId: freshCommandId,
        documentId,
        resolution: freshResolution
      })
      const freshOutcome = yield* left.replica.lookupConflictResolution(Message, {
        commandId: freshCommandId,
        documentId,
        resolution: freshResolution
      })
      assert.strictEqual(freshOutcome._tag, "DurablyCommittedLocal")
      yield* drain(documentId, left, right, true)
      const leftInspection = yield* left.replica.inspectConflicts(Message, documentId)
      const rightInspection = yield* right.replica.inspectConflicts(Message, documentId)
      assert.deepStrictEqual(leftInspection.snapshot, rightInspection.snapshot)
      assert.deepStrictEqual(leftInspection.conflicts, [])
      assert.deepStrictEqual(rightInspection.conflicts, [])
    })))

  it.effect("persists the resolution, heads, conflict absence, and receipt through a full SQL restart", () =>
    Effect.scoped(Effect.gen(function*() {
      const filename = join(
        tmpdir(),
        `effect-local-conflict-convergence-${globalThis.crypto.randomUUID()}.sqlite`
      )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          rmSync(filename, { force: true })
          rmSync(`${filename}-wal`, { force: true })
          rmSync(`${filename}-shm`, { force: true })
        })
      )
      const persisted = yield* Effect.scoped(Effect.gen(function*() {
        const { documentId, left, right } = yield* seedPair({
          left: makeLive({ filename, disableWAL: true })
        })
        yield* createMessageConflict(documentId, left, right, {
          left: "before",
          right: "selected",
          reverse: true
        })
        const resolution = selectMessage(
          yield* left.replica.inspectConflicts(Message, documentId),
          "selected"
        )
        const commandId = yield* Identity.makeCommandId
        yield* left.replica.resolveConflict(Message, { commandId, documentId, resolution })
        yield* drain(documentId, left, right, true)
        const inspection = yield* left.replica.inspectConflicts(Message, documentId)
        const outcome = yield* left.replica.lookupConflictResolution(Message, {
          commandId,
          documentId,
          resolution
        })
        assert.strictEqual(inspection.snapshot.value.message, "selected")
        assert.deepStrictEqual(inspection.conflicts, [])
        assert.strictEqual(outcome._tag, "DurablyCommittedLocal")
        return {
          commandId,
          documentId,
          heads: inspection.snapshot.heads,
          resolution
        }
      }))

      yield* Effect.scoped(Effect.gen(function*() {
        const reopened = yield* buildSide(makeLive({ filename, disableWAL: true }))
        const inspection = yield* reopened.replica.inspectConflicts(Message, persisted.documentId)
        const outcome = yield* reopened.replica.lookupConflictResolution(Message, {
          commandId: persisted.commandId,
          documentId: persisted.documentId,
          resolution: persisted.resolution
        })
        assert.strictEqual(inspection.snapshot.value.message, "selected")
        assert.deepStrictEqual(inspection.snapshot.heads, persisted.heads)
        assert.deepStrictEqual(inspection.conflicts, [])
        assert.strictEqual(outcome._tag, "DurablyCommittedLocal")
        assert.strictEqual(outcome.commandId, persisted.commandId)
      }))
    })))

  it.effect.prop(
    "converges bounded concurrent list edits",
    [
      FastCheck.uniqueArray(FastCheck.string({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 3 }),
      FastCheck.uniqueArray(FastCheck.string({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 3 })
    ],
    ([leftLabels, rightLabels]) =>
      Effect.scoped(Effect.gen(function*() {
        const { documentId, left, right } = yield* seedPair()
        yield* Effect.all([
          Effect.forEach(leftLabels, (label) => mutate(left, AddLabel, documentId, `left:${label}`), {
            discard: true
          }),
          Effect.forEach(rightLabels, (label) => mutate(right, AddLabel, documentId, `right:${label}`), {
            discard: true
          })
        ], { concurrency: "unbounded" })
        yield* drain(documentId, left, right, leftLabels.length % 2 === 0)
        const leftSnapshot = yield* left.replica.get(Message, documentId)
        const rightSnapshot = yield* right.replica.get(Message, documentId)
        assert.deepStrictEqual(leftSnapshot.heads.toSorted(), rightSnapshot.heads.toSorted())
        assert.deepStrictEqual(leftSnapshot.value, rightSnapshot.value)
        assert.sameMembers(
          [...leftSnapshot.value.labels],
          [...leftLabels.map((label) => `left:${label}`), ...rightLabels.map((label) => `right:${label}`)]
        )
      })),
    // Its own timeout: the only property check here, and it seeds a replica pair and drains a
    // partition eight times over.
    { timeout: 60_000, fastCheck: { numRuns: 8 } }
  )
})
