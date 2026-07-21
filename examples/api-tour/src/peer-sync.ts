import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { AddLabel, definition, RenameTask, SyncTestLive, Task } from "./domain.ts"

interface Side {
  readonly replica: Replica.Service
  readonly sync: PeerSync.PeerSync["Service"]
  session: PeerSync.Session
}

interface Packet {
  readonly from: Side
  readonly outbound: PeerSync.Outbound
  readonly to: Side
}

const leftPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const rightPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

const buildSide = Effect.gen(function*() {
  const context = yield* Layer.build(SyncTestLive)
  return {
    replica: Context.get(context, Replica.Replica),
    sync: Context.get(context, PeerSync.PeerSync)
  }
})

const synchronize = (documentId: Identity.DocumentId, left: Side, right: Side) =>
  Effect.gen(function*() {
    const pending: Array<Packet> = []
    const generate = Effect.fn(function*(from: Side, to: Side) {
      const generated = yield* from.sync.generate(Task, documentId, from.session)
      if (generated.outbound !== null) pending.push({ from, outbound: generated.outbound, to })
      return generated
    })

    yield* generate(left, right)
    yield* generate(right, left)
    pending.reverse()
    if (pending[0] !== undefined) pending.splice(1, 0, pending[0])

    for (let round = 1; round <= 32; round++) {
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

      const [fromLeft, fromRight] = yield* Effect.all([generate(left, right), generate(right, left)])
      if (
        pending.length === 0 &&
        fromLeft.outbound === null &&
        fromRight.outbound === null &&
        !fromLeft.dirty &&
        !fromRight.dirty
      ) {
        return {
          rounds: round,
          observedByLeft: fromLeft.observedByPeer,
          observedByRight: fromRight.observedByPeer
        }
      }
    }
    return yield* Effect.die("peer sync did not reach quiescence")
  })

const program = Effect.scoped(Effect.gen(function*() {
  const leftBuilt = yield* buildSide
  const rightBuilt = yield* buildSide
  const created = yield* leftBuilt.replica.create(Task, {
    commandId: Identity.makeCommandId(),
    value: { title: "Shared task", completed: false, labels: [] }
  })
  const documentId = yield* CommandOutcome.committedOrFail(created)
  const archive = yield* leftBuilt.replica.exportBackup({
    maxBytes: TestReplica.defaultLimits.maxBackupBytes
  }).pipe(Stream.runCollect)
  yield* rightBuilt.replica.restoreBackup({
    source: Stream.fromIterable(archive),
    mode: "clone",
    maxBytes: TestReplica.defaultLimits.maxBackupBytes,
    expectedDefinitionHash: definition.hash
  })

  const left: Side = {
    ...leftBuilt,
    session: yield* leftBuilt.sync.open(rightPeerId)
  }
  const right: Side = {
    ...rightBuilt,
    session: yield* rightBuilt.sync.open(leftPeerId)
  }

  yield* Effect.all([
    left.replica.mutate(RenameTask, {
      commandId: Identity.makeCommandId(),
      documentId,
      payload: "Edited on the left"
    }).pipe(Effect.andThen(left.replica.mutate(AddLabel, {
      commandId: Identity.makeCommandId(),
      documentId,
      payload: "left"
    }))),
    right.replica.mutate(AddLabel, {
      commandId: Identity.makeCommandId(),
      documentId,
      payload: "right"
    })
  ], { concurrency: "unbounded" })

  const sync = yield* synchronize(documentId, left, right)
  const leftSnapshot = yield* left.replica.get(Task, documentId)
  const rightSnapshot = yield* right.replica.get(Task, documentId)
  if (JSON.stringify(leftSnapshot.value) !== JSON.stringify(rightSnapshot.value)) {
    return yield* Effect.die("replicas did not converge")
  }
  if (!leftSnapshot.value.labels.includes("left") || !leftSnapshot.value.labels.includes("right")) {
    return yield* Effect.die("concurrent labels were not preserved")
  }

  yield* Effect.log({
    ...sync,
    heads: leftSnapshot.heads,
    value: leftSnapshot.value
  })
}))

await Effect.runPromise(program)
