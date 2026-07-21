import * as PeerSession from "@lucas-barake/effect-local-browser/PeerSession"
import * as FaultInjection from "@lucas-barake/effect-local-test/FaultInjection"
import * as TestPeer from "@lucas-barake/effect-local-test/TestPeer"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as Sharding from "effect/unstable/cluster/Sharding"
import { definition, EngineLive, RenameTask, Task } from "./domain.ts"

const leftPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const rightPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

const program = Effect.scoped(Effect.gen(function*() {
  const network = yield* TestPeer.make({
    queueCapacity: 16,
    maxCopies: 1,
    maxDelay: 0
  }).pipe(Effect.provide(FaultInjection.none))
  const NetworkLive = Layer.succeed(TestPeer.TestPeer, network)
  const leftTransportContext = yield* Layer.build(
    TestPeer.transportLayer(leftPeerId).pipe(Layer.provide(NetworkLive))
  )
  const rightTransportContext = yield* Layer.build(
    TestPeer.transportLayer(rightPeerId).pipe(Layer.provide(NetworkLive))
  )
  const leftEngineContext = yield* Layer.build(EngineLive)
  const rightEngineContext = yield* Layer.build(EngineLive)
  const leftReplica = Context.get(leftEngineContext, Replica.Replica)
  const rightReplica = Context.get(rightEngineContext, Replica.Replica)
  const leftSharding = Context.get(leftEngineContext, Sharding.Sharding)
  const rightSharding = Context.get(rightEngineContext, Sharding.Sharding)
  if (leftReplica === rightReplica) return yield* Effect.die("Peer engines must be isolated")

  const created = yield* leftReplica.create(Task, {
    commandId: Identity.makeCommandId(),
    value: { title: "PeerSession", completed: false, labels: [] }
  })
  const documentId = yield* CommandOutcome.committedOrFail(created)
  const archive = yield* leftReplica.exportBackup({ maxBytes: 16 * 1024 * 1024 }).pipe(Stream.runCollect)
  yield* rightReplica.restoreBackup({
    source: Stream.fromIterable(archive),
    mode: "clone",
    maxBytes: 16 * 1024 * 1024,
    expectedDefinitionHash: definition.hash
  })
  yield* leftReplica.mutate(RenameTask, {
    commandId: Identity.makeCommandId(),
    documentId,
    payload: "Synchronized through PeerSession"
  })

  const selected = [{ document: Task, documentId }]
  const [leftSession, rightSession] = yield* Effect.all([
    PeerSession.make({ peerId: rightPeerId, documents: selected }).pipe(
      Effect.provide(leftEngineContext),
      Effect.provide(leftTransportContext)
    ),
    PeerSession.make({ peerId: leftPeerId, documents: selected }).pipe(
      Effect.provide(rightEngineContext),
      Effect.provide(rightTransportContext)
    )
  ], { concurrency: "unbounded" })
  yield* Effect.all([leftSharding.pollStorage, rightSharding.pollStorage], { discard: true })

  let converged = false
  let observedByLeft = false
  let observedByRight = false
  for (let attempt = 0; attempt < 100; attempt++) {
    yield* Effect.all([leftSharding.pollStorage, rightSharding.pollStorage], { discard: true })
    const rightSnapshot = yield* rightReplica.get(Task, documentId)
    observedByLeft = yield* leftSession.observedByPeer(documentId)
    observedByRight = yield* rightSession.observedByPeer(documentId)
    converged = rightSnapshot.value.title === "Synchronized through PeerSession"
    if (converged && observedByLeft && observedByRight) break
    yield* Effect.sleep("10 millis")
  }

  if (!converged || !observedByLeft || !observedByRight) {
    return yield* Effect.die("PeerSession did not converge and observe both peers")
  }
  const leftSnapshot = yield* leftReplica.get(Task, documentId)
  const rightSnapshot = yield* rightReplica.get(Task, documentId)
  const durableConfirmation = yield* leftSession.durableConfirmation(documentId)
  if (JSON.stringify(leftSnapshot.value) !== JSON.stringify(rightSnapshot.value)) {
    return yield* Effect.die("PeerSession replicas diverged")
  }

  yield* Effect.log({
    documentId,
    leftPeer: leftSession.peerId,
    rightPeer: rightSession.peerId,
    observedByLeft,
    observedByRight,
    durableConfirmation,
    value: rightSnapshot.value
  })
}))

await Effect.runPromise(program)
