import * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as DocumentStore from "@lucas-barake/effect-local-sql/DocumentStore"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
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
import * as TestReplica from "../src/TestReplica.js"

const Task = Document.make("ConvergenceTask", {
  schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
  version: 1
})

const Rename = Mutation.make("ConvergenceTask.Rename", {
  document: Task,
  payload: Schema.String
})

const AddLabel = Mutation.make("ConvergenceTask.AddLabel", {
  document: Task,
  payload: Schema.String
})

const definition = ReplicaDefinition.make({
  name: "convergence",
  documents: DocumentSet.make(Task),
  mutations: [Rename, AddLabel],
  projections: [],
  queries: []
})

const Handlers = Layer.mergeAll(
  Mutation.layer(Rename, ({ draft, payload }) => {
    draft.title = payload
    return undefined
  }),
  Mutation.layer(AddLabel, ({ draft, payload }) => {
    draft.labels.push(payload)
    return undefined
  })
)

const Live = TestReplica.layerWithSync(definition, { projections: [] }).pipe(Layer.provide(Handlers))

const peerLeft = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const peerRight = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

interface Side {
  readonly replica: Replica.Service
  readonly store: DocumentStore.DocumentStore["Service"]
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
    store: Context.get(context, DocumentStore.DocumentStore),
    sync: Context.get(context, PeerSync.PeerSync)
  }
})

const seedPair = Effect.gen(function*() {
  const leftBuilt = yield* buildSide
  const rightBuilt = yield* buildSide
  const created = yield* leftBuilt.replica.create(Task, {
    commandId: Identity.makeCommandId(),
    value: { title: "base", labels: [] }
  })
  assert.strictEqual(created._tag, "DurablyCommittedLocal")
  if (created._tag !== "DurablyCommittedLocal") return yield* Effect.die("create did not commit")
  const backup = yield* leftBuilt.replica.exportBackup({ maxBytes: TestReplica.defaultLimits.maxBackupBytes }).pipe(
    Stream.runCollect
  )
  yield* rightBuilt.replica.restoreBackup({
    expectedDefinitionHash: definition.hash,
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
  return { documentId: created.value, left, right }
})

const drain = (documentId: Identity.DocumentId, left: Side, right: Side, reverse: boolean) =>
  Effect.gen(function*() {
    const pending: Array<Packet> = []
    const enqueueGenerated = Effect.fnUntraced(function*(from: Side, to: Side) {
      const generated = yield* from.sync.generate(Task, documentId, from.session)
      if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
      return generated
    })
    yield* enqueueGenerated(left, right)
    yield* enqueueGenerated(right, left)
    assert.isAbove(pending.length, 0)
    const [heldLeft, heldRight] = yield* Effect.all([
      enqueueGenerated(left, right),
      enqueueGenerated(right, left)
    ])
    assert.isTrue(heldLeft.dirty)
    assert.isTrue(heldRight.dirty)
    assert.isNull(heldLeft.outbound)
    assert.isNull(heldRight.outbound)
    if (reverse) pending.reverse()
    if (pending[0] !== undefined) pending.splice(1, 0, pending[0])
    for (let round = 0; round < 32; round++) {
      while (pending.length > 0) {
        const packet = pending.shift()!
        const received = yield* packet.to.sync.receive(
          Task,
          documentId,
          packet.to.session,
          {
            remoteConnectionEpoch: packet.from.session.connectionEpoch,
            receiveSequence: packet.outbound.sendSequence,
            message: packet.outbound.message
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
  mutation: typeof Rename | typeof AddLabel,
  documentId: Identity.DocumentId,
  payload: string
) =>
  side.replica.mutate(mutation, {
    commandId: Identity.makeCommandId(),
    documentId,
    payload
  })

describe("two replica convergence", () => {
  it.effect("converges a tombstone with a concurrent list edit after reordered duplicate delivery", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair
      yield* Effect.all([
        left.replica.delete(Task, { commandId: Identity.makeCommandId(), documentId }),
        mutate(right, AddLabel, documentId, "concurrent")
      ], { concurrency: "unbounded" })
      const rounds = yield* drain(documentId, left, right, true)
      assert.isAtMost(rounds, 32)
      const leftStored = yield* left.store.load(Task, documentId)
      const rightStored = yield* right.store.load(Task, documentId)
      try {
        assert.deepStrictEqual(
          [...leftStored.materializedHeads].toSorted(),
          [...rightStored.materializedHeads].toSorted()
        )
        assert.deepStrictEqual([...leftStored.snapshot.heads].toSorted(), [...rightStored.snapshot.heads].toSorted())
        assert.isTrue(leftStored.snapshot.tombstone)
        assert.isTrue(rightStored.snapshot.tombstone)
        assert.deepStrictEqual(leftStored.snapshot.value, { title: "base", labels: ["concurrent"] })
        assert.deepStrictEqual(leftStored.snapshot.value, rightStored.snapshot.value)
      } finally {
        Automerge.free(leftStored.automerge)
        Automerge.free(rightStored.automerge)
      }
    })))

  it.effect("converges concurrent edits after a partition with reorder and duplication", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, left, right } = yield* seedPair
      yield* Effect.all([
        mutate(left, Rename, documentId, "left").pipe(
          Effect.andThen(mutate(left, AddLabel, documentId, "from-left"))
        ),
        mutate(right, Rename, documentId, "right").pipe(
          Effect.andThen(mutate(right, AddLabel, documentId, "from-right"))
        )
      ], { concurrency: "unbounded" })
      const rounds = yield* drain(documentId, left, right, true)
      assert.isAtMost(rounds, 32)
      const leftStored = yield* left.store.load(Task, documentId)
      const rightStored = yield* right.store.load(Task, documentId)
      try {
        assert.deepStrictEqual(
          [...leftStored.materializedHeads].toSorted(),
          [...rightStored.materializedHeads].toSorted()
        )
        assert.deepStrictEqual(leftStored.snapshot.value, rightStored.snapshot.value)
        assert.sameMembers([...leftStored.snapshot.value.labels], ["from-left", "from-right"])
        const leftConflicts = Automerge.getConflicts(leftStored.automerge.value, "title")
        const rightConflicts = Automerge.getConflicts(rightStored.automerge.value, "title")
        assert.sameMembers(Object.values(leftConflicts ?? {}), ["left", "right"])
        assert.deepStrictEqual(leftConflicts, rightConflicts)
      } finally {
        Automerge.free(leftStored.automerge)
        Automerge.free(rightStored.automerge)
      }
    })))

  it.effect.prop(
    "converges bounded concurrent list edits",
    [
      FastCheck.uniqueArray(FastCheck.string({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 3 }),
      FastCheck.uniqueArray(FastCheck.string({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 3 })
    ],
    ([leftLabels, rightLabels]) =>
      Effect.scoped(Effect.gen(function*() {
        const { documentId, left, right } = yield* seedPair
        yield* Effect.all([
          Effect.forEach(leftLabels, (label) => mutate(left, AddLabel, documentId, `left:${label}`), {
            discard: true
          }),
          Effect.forEach(rightLabels, (label) => mutate(right, AddLabel, documentId, `right:${label}`), {
            discard: true
          })
        ], { concurrency: "unbounded" })
        yield* drain(documentId, left, right, leftLabels.length % 2 === 0)
        const leftSnapshot = yield* left.replica.get(Task, documentId)
        const rightSnapshot = yield* right.replica.get(Task, documentId)
        assert.deepStrictEqual(leftSnapshot.heads.toSorted(), rightSnapshot.heads.toSorted())
        assert.deepStrictEqual(leftSnapshot.value, rightSnapshot.value)
        assert.sameMembers(
          [...leftSnapshot.value.labels],
          [...leftLabels.map((label) => `left:${label}`), ...rightLabels.map((label) => `right:${label}`)]
        )
      })),
    { fastCheck: { numRuns: 8 } }
  )
})
