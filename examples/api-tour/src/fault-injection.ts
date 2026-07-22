import * as FaultInjection from "@lucas-barake/effect-local-test/FaultInjection"
import * as TestPeer from "@lucas-barake/effect-local-test/TestPeer"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

const leftPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const rightPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

const FaultsLive = FaultInjection.layerSequence([
  { drop: true, copies: 1, delay: 0, reorder: false },
  { drop: false, copies: 1, delay: 0, reorder: true },
  { drop: false, copies: 2, delay: 0, reorder: false },
  { drop: false, copies: 1, delay: "5 millis", reorder: true }
])

const NetworkLive = TestPeer.layer({
  queueCapacity: 4,
  maxCopies: 2,
  maxDelay: "10 millis"
}).pipe(Layer.provide(FaultsLive))

const Live = TestPeer.transportLayer(leftPeerId).pipe(Layer.provideMerge(NetworkLive))

const program = Effect.scoped(Effect.gen(function*() {
  const network = yield* TestPeer.TestPeer
  const transport = yield* PeerTransport.PeerTransport
  const [left, right] = yield* Effect.all([
    transport.connect({
      replicaId: (yield* Identity.makeReplicaId),
      peerId: rightPeerId
    }),
    network.connect(rightPeerId, leftPeerId)
  ], { concurrency: "unbounded" })

  yield* left.send(Uint8Array.of(1))
  yield* left.send(Uint8Array.of(2))
  yield* left.send(Uint8Array.of(3))
  yield* left.send(Uint8Array.of(4))
  yield* network.flush

  const received = Array.from(yield* right.receive.pipe(Stream.take(4), Stream.runCollect), (bytes) => bytes[0])
  if (received.join(",") !== "3,3,2,4") {
    return yield* Effect.die(`unexpected fault delivery: ${received.join(",")}`)
  }

  yield* Effect.log({
    capabilities: transport.capabilities,
    delivery: received,
    dropped: 1,
    duplicated: 3,
    reorderedAfter: 2,
    flushedAfterDelay: 4
  })
}))

await Effect.runPromise(program.pipe(Effect.provide(Live), Effect.provide(NodeCrypto.layer)))
import { NodeCrypto } from "@effect/platform-node"
