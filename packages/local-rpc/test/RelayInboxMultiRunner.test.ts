import { NodeClusterSocket, NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import { EntityId } from "effect/unstable/cluster/EntityId"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SocketRunner from "effect/unstable/cluster/SocketRunner"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Net from "node:net"
import type * as PeerRpc from "../src/PeerRpc.js"
import * as RelayInbox from "../src/RelayInbox.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"
import { PgContainer } from "./PgContainer.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)
const sessionId = (value: string) => Identity.SessionId.make(`ses_00000000-0000-4000-8000-${value}`)

const inboxKey = "inbox-a"

const baseOptions: RelayInbox.Options = {
  maxDeliveries: 10,
  messageTtl: Duration.minutes(10),
  terminalRetention: Duration.minutes(10),
  sessionDeadline: Duration.seconds(90),
  sessionSweep: Duration.seconds(1),
  settleDeadline: Duration.seconds(30),
  maxConcurrentChannels: 4,
  storeRetry: Duration.zero,
  maxPendingMessages: 100,
  maxPendingBytes: 10_000_000,
  mailboxCapacity: 16,
  maxIdleTime: Duration.hours(1)
}

/**
 * The database, not the table prefix, is the isolation boundary: `SqlRunnerStorage` takes shard
 * locks with `pg_try_advisory_lock`, and those are scoped per database.
 */
const clientFor = (database?: string) =>
  Layer.unwrap(Effect.gen(function*() {
    const container = yield* PgContainer
    const uri = container.getConnectionUri()
    const url = database === undefined ? uri : uri.slice(0, uri.lastIndexOf("/")) + "/" + database
    return Layer.orDie(PgClient.layer({ url: Redacted.make(url) }))
  }))

const freePort: Effect.Effect<number> = Effect.acquireUseRelease(
  Effect.sync(() => Net.createServer()),
  (server) =>
    Effect.callback<number>((resume) => {
      server.listen(0, "localhost", () => {
        const address = server.address()
        resume(Effect.succeed(typeof address === "object" && address !== null ? address.port : 0))
      })
    }),
  (server) =>
    Effect.callback<void>((resume) => {
      server.close(() => resume(Effect.void))
    })
)

const createDatabase = (name: string) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql.unsafe(`CREATE DATABASE ${name}`)
  }).pipe(Effect.orDie, Effect.provide(clientFor()), Effect.scoped)

/**
 * `Runners.layerNoop` is deliberately absent. Every `RelayInbox` rpc is volatile, so a message for
 * a remote shard routes through `Runners.send`, which under `layerNoop` fails
 * `EntityNotAssignedToRunner` forever rather than surfacing.
 */
const runner = (port: number, sql: Layer.Layer<SqlClient.SqlClient, never, PgContainer>) => {
  const config = ShardingConfig.layer({
    runnerAddress: Option.some(RunnerAddress.make("localhost", port)),
    runnerListenAddress: Option.some(RunnerAddress.make("localhost", port)),
    entityTerminationTimeout: 0,
    entityMessagePollInterval: 1000,
    entityReplyPollInterval: 200,
    refreshAssignmentsInterval: 500,
    shardLockRefreshInterval: 2000,
    sendRetryInterval: 200,
    shardsPerGroup: 8
  })

  const clusterStorage = Layer.mergeAll(
    Layer.orDie(SqlMessageStorage.layerWith({ prefix: "effect_local_cluster" })),
    Layer.orDie(SqlRunnerStorage.layerWith({ prefix: "effect_local_runner" }))
  ).pipe(Layer.provide(config))

  // Real liveness: dropping an unhealthy runner from the hash ring is the mechanism that moves
  // ownership, so a noop health check would assert the property under test by construction.
  const health = RunnerHealth.layerPing.pipe(
    Layer.provide(Runners.layerRpc),
    Layer.provide(NodeClusterSocket.layerClientProtocol),
    Layer.provide(clusterStorage),
    Layer.provide(config),
    Layer.provide(RpcSerialization.layerMsgPack)
  )

  return RelayInbox.layer(baseOptions).pipe(
    Layer.provideMerge(SocketRunner.layer),
    Layer.provide(health),
    Layer.provide(NodeClusterSocket.layerSocketServer),
    Layer.provide(NodeClusterSocket.layerClientProtocol),
    Layer.provide(RpcSerialization.layerMsgPack),
    Layer.provideMerge(clusterStorage),
    Layer.provide(config),
    Layer.provideMerge(Layer.orDie(SqlRelayInboxStore.layer)),
    Layer.provide(sql),
    Layer.provide(NodeCrypto.layer)
  )
}

const channel = {
  tenantId: "tenant-a",
  senderSubjectId: "sender-a",
  senderPeerId: peer("00000000aaa1"),
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  senderConnectionEpoch: "epoch-1"
}

const deliver = (id: string, sequence: number) => ({
  channel,
  envelope: {
    relayMessageId: relayId(id),
    relayPeerId: peer("00000000ffff"),
    sender: {
      tenantId: channel.tenantId,
      subjectId: channel.senderSubjectId,
      peerId: channel.senderPeerId,
      replicaIncarnation: channel.senderReplicaIncarnation,
      connectionEpoch: channel.senderConnectionEpoch,
      sequence
    },
    recipient: {
      tenantId: "tenant-a",
      subjectId: "recipient-a",
      peerId: peer("00000000bbb1")
    },
    payloadVersion: 1 as const,
    document: { documentId: documentId("00000000dddd"), documentType: "note" },
    writerProvenance: [],
    messageHash: id.padStart(64, "a"),
    outerEnvelopeDigest: id.padStart(64, "b"),
    payload: new Uint8Array([1, 2, 3])
  },
  senderRetryHorizonMillis: 60_000
})

/**
 * `isolated` puts runner B in its own database, so neither sees the other and each acquires every
 * shard. That is the control: without it, a single owner proves nothing about sharing.
 */
const probe = (options: { readonly isolated: boolean; readonly window: Duration.Duration }) =>
  Effect.gen(function*() {
    if (options.isolated) yield* createDatabase("runner_b")

    const [portA, portB] = yield* Effect.forEach([0, 1], () => freePort)
    const contextA = yield* Layer.build(runner(portA!, clientFor()))
    const contextB = yield* Layer.build(
      runner(portB!, clientFor(options.isolated ? "runner_b" : undefined))
    )

    const makeA = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextA))
    const makeB = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextB))
    const clientA = makeA(inboxKey)
    const clientB = makeB(inboxKey)

    // `Deliver` is not a barrier: it returns as soon as any runner owns the shard, which is true
    // while runner A is still alone in the ring.
    const expectedRunners = options.isolated ? 1 : 2
    yield* Effect.forEach([contextA, contextB], (context) =>
      RunnerStorage.RunnerStorage.pipe(
        Effect.flatMap((storage) => storage.getRunners),
        Effect.filterOrFail(
          (runners) => runners.filter(([, healthy]) => healthy).length === expectedRunners,
          () => "converging" as const
        ),
        Effect.retry({ schedule: Schedule.spaced(100), times: 100 }),
        Effect.orDie,
        Effect.provideContext(context)
      ))

    // Confirmed over consecutive polls: while runner B is still acquiring, runner A holds every
    // shard and the owner count reads as one for the wrong reason.
    const shardId = yield* RelayInbox.RelayInbox.getShardId(EntityId.make(inboxKey)).pipe(
      Effect.provideContext(contextA)
    )
    const owners = Effect.forEach([contextA, contextB], (context) =>
      Sharding.Sharding.pipe(
        Effect.map((sharding) => sharding.hasShardId(shardId)),
        Effect.provideContext(context)
      )).pipe(Effect.map((held) => held.filter(Boolean).length))
    const expectedOwners = options.isolated ? 2 : 1
    yield* Effect.forEach(
      [0, 1, 2, 3, 4],
      () =>
        owners.pipe(
          Effect.filterOrFail((count) => count === expectedOwners, () => "unsettled" as const),
          Effect.andThen(Effect.sleep(Duration.millis(200)))
        )
    ).pipe(
      Effect.retry({ schedule: Schedule.spaced(200), times: 100 }),
      Effect.orDie
    )

    yield* clientA.Deliver(deliver("000000000001", 0))

    const delivered = yield* Queue.unbounded<PeerRpc.StoredMessage>()
    const streamA = yield* Effect.forkChild(
      Stream.runForEach(
        clientA.Subscribe({ sessionId: sessionId("00000000000a") }),
        (message) => Queue.offer(delivered, message)
      )
    )

    const head = yield* Queue.take(delivered)
    assert.strictEqual(head.relayMessageId, relayId("000000000001"))

    yield* Effect.forkChild(
      Stream.runDrain(clientB.Subscribe({ sessionId: sessionId("00000000000b") }))
    )

    const exit = yield* Fiber.await(streamA).pipe(
      Effect.timeoutOrElse({ duration: options.window, orElse: () => Effect.succeed(undefined) })
    )

    // Ownership read off each runner's own shard map rather than inferred from the stream.
    const hosting = yield* Effect.forEach([contextA, contextB], (context) =>
      Sharding.Sharding.pipe(
        Effect.flatMap((sharding) => sharding.activeEntityCount),
        Effect.provideContext(context)
      ))

    return { exit, hosting }
  }).pipe(Effect.scoped)

describe("RelayInbox multi-runner", () => {
  /**
   * `it.live`: assignment refresh, lock acquisition and expiry, the ping health check's own timeout
   * and the socket connect all run against the real clock in layers this test does not drive, so
   * under a virtual clock `Layer.build` of the first runner never returns.
   */
  it.live("gives one inbox key a single owning runner across two runners", () =>
    Effect.gen(function*() {
      const { exit, hosting } = yield* probe({ isolated: false, window: Duration.seconds(20) })

      assert.deepStrictEqual(
        hosting.toSorted(),
        [0, 1],
        "exactly one of the two runners hosts this inbox key, and the other serves its client over the wire"
      )
      assert.isTrue(exit !== undefined && Exit.isSuccess(exit), "runner A's stream ended cleanly")
    }).pipe(Effect.provide(PgContainer.layer)), 180_000)

  it.live("leaves both runners hosting the key when they do not share a shard map", () =>
    Effect.gen(function*() {
      // The hosting count is the whole control, and it is positive evidence: each runner having its
      // own instance is what distinguishes "sharding elected one owner" from "there was only ever
      // one candidate". Deliberately no assertion on `exit` being absent - an absence bounded by a
      // duration is satisfied by nothing happening, so it can only ever false-pass.
      const { hosting } = yield* probe({ isolated: true, window: Duration.seconds(5) })

      assert.deepStrictEqual(hosting, [1, 1], "each runner served its own instance of the inbox")
    }).pipe(Effect.provide(PgContainer.layer)), 180_000)
})
