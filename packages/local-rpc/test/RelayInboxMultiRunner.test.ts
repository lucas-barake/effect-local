import { NodeClusterSocket, NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
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
  maxConcurrentChannels: 4,
  storeRetry: Duration.zero,
  maxPendingMessages: 100,
  maxPendingBytes: 10_000_000,
  mailboxCapacity: 16,
  maxIdleTime: Duration.hours(1)
}

class ContainerError extends Data.TaggedError("ContainerError")<{
  readonly cause: unknown
}> {}

class PgContainer extends Context.Service<PgContainer>()(
  "@lucas-barake/effect-local-rpc/test/MultiRunnerPgContainer",
  {
    make: Effect.acquireRelease(
      Effect.tryPromise({
        try: () => new PostgreSqlContainer("postgres:alpine").start(),
        catch: (cause) => new ContainerError({ cause })
      }),
      (container) => Effect.promise(() => container.stop())
    )
  }
) {
  static readonly layer = Layer.effect(this)(this.make)
}

/**
 * A client for one database inside the shared container.
 *
 * The database, not just the table prefix, is the isolation boundary that matters here:
 * `SqlRunnerStorage` takes its shard locks with `pg_try_advisory_lock`, and PostgreSQL advisory
 * locks are scoped to a database rather than to a table. Two runners pointed at differently
 * prefixed tables in one database would still contend for the same locks.
 */
const clientFor = (database?: string) =>
  Layer.unwrap(Effect.gen(function*() {
    const container = yield* PgContainer
    const uri = container.getConnectionUri()
    const url = database === undefined ? uri : uri.slice(0, uri.lastIndexOf("/")) + "/" + database
    return Layer.orDie(PgClient.layer({ url: Redacted.make(url) }))
  }))

/** An ephemeral port the OS has just confirmed is free. */
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
 * One runner: the entity, a real socket transport, real ping health, and its own SQL cluster
 * storage over the shared database.
 *
 * `Runners.layerNoop` is deliberately absent. Every `RelayInbox` rpc is volatile, and
 * `Sharding.sendOutgoing` routes a volatile message for a remote shard through `Runners.send`,
 * which under `layerNoop` fails `EntityNotAssignedToRunner` forever. A socket transport is the
 * only thing that lets a non-owning runner reach the owner.
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

  // This runner's own SQL cluster storage over the shared database, not a shared in-process
  // instance. Two runners in production are two processes, each with its own client.
  const clusterStorage = Layer.mergeAll(
    Layer.orDie(SqlMessageStorage.layerWith({ prefix: "effect_local_cluster" })),
    Layer.orDie(SqlRunnerStorage.layerWith({ prefix: "effect_local_runner" }))
  ).pipe(Layer.provide(config))

  // Real liveness, not `RunnerHealth.layerNoop`: an unhealthy runner is dropped from the hash ring,
  // which is the mechanism that moves a shard's ownership. A noop health check asserts liveness by
  // construction, which is the exact thing this test exists to stop doing.
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
 * Brings two runners up and runs the ownership probe against them.
 *
 * `isolated` puts runner B in its own database. That is the negative control: neither runner sees
 * the other in `getRunners`, they take their shard locks in different advisory lock spaces, and so
 * each acquires every shard and serves its own instance of the inbox. Everything else is held
 * constant — same entity, same options, real sockets, real ping health — so the only thing the
 * probe can be reacting to is whether the two runners share a shard map.
 */
const probe = (options: { readonly isolated: boolean; readonly window: Duration.Duration }) =>
  Effect.gen(function*() {
    if (options.isolated) yield* createDatabase("runner_b")

    // Ports are taken from the ephemeral range at run time. Fixed ports collide with anything else
    // on the machine and with a second copy of this file, which is a failure mode that looks like a
    // clustering bug rather than a port clash.
    const [portA, portB] = yield* Effect.forEach([0, 1], () => freePort)
    const contextA = yield* Layer.build(runner(portA!, clientFor()))
    const contextB = yield* Layer.build(
      runner(portB!, clientFor(options.isolated ? "runner_b" : undefined))
    )

    const makeA = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextA))
    const makeB = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextB))
    const clientA = makeA(inboxKey)
    const clientB = makeB(inboxKey)

    // Membership has to converge before the probe, and `Deliver` alone is not that barrier: it
    // returns as soon as ANY runner owns the shard, which is true while runner A is still alone in
    // the ring. Probing then races the rebalance that follows runner B joining, and the entity is
    // observed mid-migration.
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

    // Membership converging is not the same as the shard being acquired: the ring is recomputed
    // locally by each runner, but ownership only actually moves when a runner takes the shard's
    // advisory lock. So wait on the acquisition itself, read off each runner's own shard map.
    //
    // Confirmed over consecutive polls rather than on first sight. While runner B is still
    // acquiring, runner A holds every shard and the count reads as one owner - the right number for
    // the wrong reason. Requiring the answer to hold still is what separates a settled assignment
    // from a snapshot taken mid-rebalance, and it waits on the condition rather than on a duration
    // guessed from the refresh intervals.
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

    // Positive evidence that runner A's session is live and the entity is delivering into it.
    const head = yield* Queue.take(delivered)
    assert.strictEqual(head.relayMessageId, relayId("000000000001"))

    // The same inbox key, a different session, through the OTHER runner.
    yield* Effect.forkChild(
      Stream.runDrain(clientB.Subscribe({ sessionId: sessionId("00000000000b") }))
    )

    const exit = yield* Fiber.await(streamA).pipe(
      Effect.timeoutOrElse({ duration: options.window, orElse: () => Effect.succeed(undefined) })
    )

    // How many runners are actually hosting an instance of this entity. `activeEntityCount` is each
    // runner's own count of live entities, so this reads ownership off the runners themselves
    // rather than inferring it from the stream.
    const hosting = yield* Effect.forEach([contextA, contextB], (context) =>
      Sharding.Sharding.pipe(
        Effect.flatMap((sharding) => sharding.activeEntityCount),
        Effect.provideContext(context)
      ))

    return { exit, hosting }
  }).pipe(Effect.scoped)

describe("RelayInbox multi-runner", () => {
  /**
   * `it.live` rather than `it.effect`. Everything that makes two runners cooperate is scheduled
   * against the real clock inside layers this test does not drive: the shard assignment refresh
   * loop, `RunnerStorage` lock acquisition and expiry, the ping health check's own timeout and
   * retry schedule, and the socket transport's connect. Under the virtual clock `it.effect`
   * installs, none of those fibers run and `Layer.build` of the first runner never returns.
   */
  it.live("gives one inbox key a single owning runner across two runners", () =>
    Effect.gen(function*() {
      const { exit, hosting } = yield* probe({ isolated: false, window: Duration.seconds(20) })

      assert.deepStrictEqual(
        hosting.toSorted(),
        [0, 1],
        "exactly one of the two runners hosts this inbox key, and the other serves its client over the wire"
      )
      // A session replacement ends the stream cleanly, so this also rules out the stream having
      // ended because the entity or its transport fell over.
      assert.isTrue(exit !== undefined && Exit.isSuccess(exit), "runner A's stream ended cleanly")
    }).pipe(Effect.provide(PgContainer.layer)), 180_000)

  /**
   * The control that gives the check above its meaning: with the runners split across databases,
   * the same script leaves two live instances of the inbox and runner A's stream open.
   */
  it.live("leaves both runners hosting the key when they do not share a shard map", () =>
    Effect.gen(function*() {
      // A shorter window than the positive case, which is a generous timeout on an event that is
      // expected to arrive: in the shared run the replacement lands in well under a second, so five
      // seconds is far past the point where one would have been observed. The load bearing
      // assertion here is the hosting count, which is positive evidence either way.
      const { exit, hosting } = yield* probe({ isolated: true, window: Duration.seconds(5) })

      assert.deepStrictEqual(hosting, [1, 1], "each runner served its own instance of the inbox")
      assert.strictEqual(exit, undefined, "so nothing replaced session A and its stream stayed open")
    }).pipe(Effect.provide(PgContainer.layer)), 180_000)
})
