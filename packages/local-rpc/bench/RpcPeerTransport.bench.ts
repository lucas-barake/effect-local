import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import { afterAll, bench } from "vitest"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"

const makeBytes = (size: number) => {
  const bytes = new Uint8Array(size)
  for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251
  return bytes
}

const Task = Document.make("BenchmarkTask", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000001")
const serverPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const clientPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const limits = PeerRpcLimits.Values.make({
  ...PeerRpcLimits.defaults,
  authenticationRatePerSecond: 1_000_000,
  authenticationBurst: 1_000_000
})
const scope = await Effect.runPromise(Scope.make())
const handlers = PeerRpc.Rpcs.toLayer(PeerRpc.Rpcs.of({
  Open: () =>
    Stream.concat(
      Stream.make(PeerRpc.Opened.make({
        _tag: "Opened",
        protocolVersion: PeerRpc.protocolVersion,
        sessionId,
        peerId: serverPeerId,
        capabilities: { storeAndForward: false }
      })),
      Stream.never
    ),
  Push: () => Effect.void
}))
const client = await Effect.runPromise(
  RpcTest.makeClient(PeerRpc.Rpcs).pipe(
    Effect.provide(handlers),
    Effect.provide(PeerAuthentication.layerServer),
    Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
      authenticate: () =>
        Effect.succeed({
          principal: PeerAuthentication.PeerPrincipal.make({
            tenantId: "tenant",
            subjectId: "benchmark-client",
            peerId: clientPeerId
          }),
          validUntil: Number.MAX_SAFE_INTEGER,
          invalidated: Effect.never
        })
    }),
    Effect.provide(PeerAuthentication.layerClient),
    Effect.provideService(PeerCredentials.PeerCredentials, {
      get: Effect.succeed(Redacted.make("benchmark-credential"))
    }),
    Effect.provideService(PeerRpcLimits.PeerRpcLimits, limits),
    Effect.provideService(Scope.Scope, scope)
  )
)
const transportContext = await Effect.runPromise(
  Layer.build(RpcPeerTransport.layer(client, {
    documents: [{ document: Task, documentId }]
  })).pipe(Effect.provideService(Scope.Scope, scope))
)
const connection = await Effect.runPromise(
  Context.get(transportContext, PeerTransport.PeerTransport).connect({
    replicaId,
    peerId: serverPeerId
  }).pipe(Effect.provideService(Scope.Scope, scope))
)

for (
  const [label, size] of [
    ["1 KiB", 1024],
    ["64 KiB", 64 * 1024],
    ["1 MiB", 1024 * 1024]
  ] as const
) {
  const payload = makeBytes(size)

  bench(`RpcTest no-serialization generated Push ${label}`, async () => {
    await Effect.runPromise(client.Push({ sessionId, payload }))
  }, {
    iterations: 500,
    time: 0,
    warmupIterations: 50,
    warmupTime: 0
  })

  bench(`RpcTest no-serialization RpcPeerTransport send ${label}`, async () => {
    await Effect.runPromise(connection.send(payload))
  }, {
    iterations: 500,
    time: 0,
    warmupIterations: 50,
    warmupTime: 0
  })
}

afterAll(async () => {
  await Effect.runPromise(connection.close)
  await Effect.runPromise(Scope.close(scope, Exit.void))
})
