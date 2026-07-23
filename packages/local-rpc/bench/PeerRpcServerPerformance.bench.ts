import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { afterAll, bench } from "vitest"
import * as PeerRpcAdmissionBenchmark from "../src/internal/peerRpcAdmissionBenchmark.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerAuthorization from "../src/PeerAuthorization.js"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"

const sizes = [0, 1_000, 10_000] as const
const now = 1
const limits = PeerRpcLimits.Values.make({
  ...PeerRpcLimits.defaults,
  authenticationRatePerSecond: 1_000_000,
  authenticationBurst: 1_000_000,
  openRatePerSecond: 1_000_000,
  openBurst: 1_000_000,
  maxRetainedRateLimitedConnections: 10_001,
  maxRetainedRateLimitedSubjects: 10_001
})
const replicaLimits = ReplicaLimits.Values.make({
  maxBackupBytes: 1,
  maxChunkBytes: 1,
  maxArchiveRecords: 1,
  maxJsonDepth: 1,
  maxSyncMessageBytes: 1,
  maxPeerSendMillis: 1,
  maxSyncChangesPerMessage: 1,
  maxSyncDependencyEdgesPerMessage: 1,
  maxSyncOperationsPerMessage: 1,
  maxPendingBytesPerDocument: 1,
  maxPendingBytesPerPeer: 1,
  maxPendingBytesPerReplica: 1,
  maxPendingAgeMillis: 1,
  maxPendingChangesPerDocument: 1,
  maxPendingChangesPerPeer: 1,
  maxPendingChangesPerReplica: 1,
  maxPendingDependencyEdgesPerDocument: 1,
  maxPendingDependencyEdgesPerPeer: 1,
  maxPendingDependencyEdgesPerReplica: 1,
  maxSessions: 64,
  maxStreamsPerSession: 1,
  maxInFlightPerSession: 1,
  maxQueuedRpc: 1
})

const authenticationOperations = new Map<number, {
  readonly admit: (clientId: number, now: number) => Effect.Effect<boolean>
  readonly release: (clientId: number, now: number) => Effect.Effect<void>
}>()
const subjectOperations = new Map<number, {
  readonly acquire: (
    subjectId: string,
    operation: "Open" | "Push",
    now: number
  ) => Effect.Effect<"Unavailable" | "Capacity" | "Acquired">
  readonly release: (subjectId: string, operation: "Open" | "Push") => Effect.Effect<void>
}>()
const serverScopes: Array<Scope.Closeable> = []

await Effect.runPromise(Effect.gen(function*() {
  for (const size of sizes) {
    let authenticationController: typeof authenticationOperations extends Map<number, infer Operations> ? Operations
      : never
    PeerRpcAdmissionBenchmark.installAuthenticationCapture(
      (operations) => void (authenticationController = operations)
    )
    yield* Layer.build(PeerAuthentication.layerServer).pipe(
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: () => Effect.die("unused")
      }),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, limits),
      Effect.scoped
    )
    for (let clientId = 0; clientId < size; clientId++) {
      yield* authenticationController.admit(clientId, now)
      yield* authenticationController.release(clientId, now)
    }
    yield* authenticationController.admit(-1, now)
    yield* authenticationController.release(-1, now)
    authenticationOperations.set(size, authenticationController)

    let subjectController: typeof subjectOperations extends Map<number, infer Operations> ? Operations : never
    PeerRpcAdmissionBenchmark.installSubjectCapture((operations) => void (subjectController = operations))
    const scope = yield* Scope.make()
    serverScopes.push(scope)
    const publisher = CommitPublisher.CommitPublisher.of({
      publishPending: Effect.succeed(0),
      invalidate: () => Effect.void,
      subscribe: Effect.succeed({
        watermark: Identity.CommitSequence.make(0),
        refreshGeneration: 0,
        events: Stream.never
      })
    })
    yield* Layer.build(PeerRpcServer.layerHandlers({
      tenantId: "tenant",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001"),
      definitionHash: "def_00000000000000000000000000000000"
    })).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.provideService(CommitPublisher.CommitPublisher, publisher),
      Effect.provideService(PeerRpcLimits.PeerRpcLimits, limits),
      Effect.provideService(ReplicaLimits.ReplicaLimits, replicaLimits),
      Effect.provideService(PeerAuthorization.PeerAuthorization, {} as PeerAuthorization.PeerAuthorization["Service"]),
      Effect.provideService(Scope.Scope, scope)
    )
    for (let subjectId = 0; subjectId < size; subjectId++) {
      yield* subjectController.acquire(String(subjectId), "Open", now)
      yield* subjectController.release(String(subjectId), "Open")
    }
    yield* subjectController.acquire("hot", "Open", now)
    yield* subjectController.release("hot", "Open")
    subjectOperations.set(size, subjectController)
  }
}))

for (const size of sizes) {
  bench(`authentication hot admission with ${size} retained connections`, async () => {
    const operations = authenticationOperations.get(size)!
    await Effect.runPromise(operations.admit(-1, now))
    await Effect.runPromise(operations.release(-1, now))
  }, { iterations: 200, time: 0, warmupIterations: 20, warmupTime: 0 })

  bench(`subject hot admission with ${size} retained subjects`, async () => {
    const operations = subjectOperations.get(size)!
    await Effect.runPromise(operations.acquire("hot", "Open", now))
    await Effect.runPromise(operations.release("hot", "Open"))
  }, { iterations: 200, time: 0, warmupIterations: 20, warmupTime: 0 })
}

afterAll(async () => {
  await Promise.all(serverScopes.map((scope) => Effect.runPromise(Scope.close(scope, Exit.void))))
})
