import { NodeClusterSocket, NodeCrypto, NodeSocketServer } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import { EntityId } from "effect/unstable/cluster/EntityId"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import type * as ShardId from "effect/unstable/cluster/ShardId"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SocketRunner from "effect/unstable/cluster/SocketRunner"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SocketServer from "effect/unstable/socket/SocketServer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import type * as PeerRpc from "../src/PeerRpc.js"
import * as RelayInbox from "../src/RelayInbox.js"
import type * as RelayInboxStore from "../src/RelayInboxStore.js"
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
    let url = uri
    if (database !== undefined) url = uri.slice(0, uri.lastIndexOf("/")) + "/" + database
    return Layer.orDie(PgClient.layer({ url: Redacted.make(url) }))
  }))

const tcpPort = (address: SocketServer.Address) => {
  if (address._tag !== "TcpAddress") return assert.fail("Expected TCP address")
  return address.port
}

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
const runner = (
  port: number,
  socketServer: SocketServer.SocketServer["Service"],
  probe: {
    readonly registered: Deferred.Deferred<void>
    readonly clusterReady: Deferred.Deferred<void>
    readonly ownership: Queue.Queue<ReadonlyArray<ShardId.ShardId>>
  },
  sql: Layer.Layer<SqlClient.SqlClient, never, PgContainer>
) => {
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

  const runnerStorage = Layer.effect(RunnerStorage.RunnerStorage, RunnerStorage.RunnerStorage).pipe(
    // Hold both registration calls at their completion boundary so the shared runners cannot derive
    // a one-runner ring before both durable registrations exist. Acquisition results are the storage
    // operation's own ownership evidence, not a later sample of mutable sharding state.
    Layer.updateService(RunnerStorage.RunnerStorage, (storage) => {
      return RunnerStorage.RunnerStorage.of({
        ...storage,
        register: (runnerRegistration, healthy) =>
          storage.register(runnerRegistration, healthy).pipe(
            Effect.tap(() => Deferred.succeed(probe.registered, undefined)),
            Effect.tap(() => Deferred.await(probe.clusterReady))
          ),
        acquire: (address, shardIds) =>
          storage.acquire(address, shardIds).pipe(
            Effect.tap((shards) => Queue.offer(probe.ownership, shards))
          )
      })
    }),
    Layer.provide(Layer.orDie(SqlRunnerStorage.layerWith({ prefix: "effect_local_runner" })))
  )
  const clusterStorage = Layer.mergeAll(
    Layer.orDie(SqlMessageStorage.layerWith({ prefix: "effect_local_cluster" })),
    runnerStorage
  ).pipe(
    Layer.provide(config)
  )

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
    Layer.provide(Layer.succeed(SocketServer.SocketServer)(socketServer)),
    Layer.provide(NodeClusterSocket.layerClientProtocol),
    Layer.provide(RpcSerialization.layerMsgPack),
    Layer.provideMerge(clusterStorage),
    Layer.provide(config),
    Layer.provideMerge(Layer.orDie(SqlRelayInboxStore.layer)),
    Layer.provide(sql),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(PeerRelayLimits.layerDefaults)
  )
}

const channel = {
  tenantId: "tenant-a",
  senderSubjectId: "sender-a",
  senderPeerId: peer("00000000aaa1"),
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  senderConnectionEpoch: "epoch-1"
}

const deliver = (id: string, sequence: number): {
  readonly channel: RelayInboxStore.ChannelKey
  readonly envelope: RelayInboxStore.InboxEnvelope
  readonly senderRetryHorizonMillis: number
} => ({
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
    payloadVersion: 1,
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
const probe = (options: { readonly isolated: boolean }) =>
  Effect.gen(function*() {
    if (options.isolated) yield* createDatabase("runner_b")

    const scope = yield* Effect.scope
    const clusterReady = yield* Deferred.make<void>()
    const probeA = {
      registered: yield* Deferred.make<void>(),
      clusterReady,
      ownership: yield* Queue.unbounded<ReadonlyArray<ShardId.ShardId>>()
    }
    const probeB = {
      registered: yield* Deferred.make<void>(),
      clusterReady,
      ownership: yield* Queue.unbounded<ReadonlyArray<ShardId.ShardId>>()
    }
    const listenerA = yield* Layer.buildWithScope(NodeSocketServer.layer({ host: "localhost", port: 0 }), scope)
    const listenerB = yield* Layer.buildWithScope(NodeSocketServer.layer({ host: "localhost", port: 0 }), scope)
    const socketServerA = Context.get(listenerA, SocketServer.SocketServer)
    const socketServerB = Context.get(listenerB, SocketServer.SocketServer)
    const contextA = yield* Layer.build(runner(tcpPort(socketServerA.address), socketServerA, probeA, clientFor()))
    let databaseB: string | undefined
    if (options.isolated) databaseB = "runner_b"
    const contextB = yield* Layer.build(
      runner(
        tcpPort(socketServerB.address),
        socketServerB,
        probeB,
        clientFor(databaseB)
      )
    )

    const makeA = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextA))
    const makeB = yield* RelayInbox.RelayInbox.client.pipe(Effect.provideContext(contextB))
    const clientA = makeA(inboxKey)
    const clientB = makeB(inboxKey)

    yield* Effect.all([Deferred.await(probeA.registered), Deferred.await(probeB.registered)], { discard: true })
    yield* Deferred.succeed(clusterReady, undefined)

    const shardId = yield* RelayInbox.RelayInbox.getShardId(EntityId.make(inboxKey)).pipe(
      Effect.provideContext(contextA)
    )
    let expectedOwners = 1
    if (options.isolated) expectedOwners = 2
    const ownership = yield* Effect.all([Queue.take(probeA.ownership), Queue.take(probeB.ownership)])
    assert.strictEqual(
      ownership.filter((shards) => shards.some((owned) => Equal.equals(owned, shardId))).length,
      expectedOwners
    )

    yield* clientA.Deliver(deliver("000000000001", 0))
    if (options.isolated) yield* clientB.Deliver(deliver("000000000002", 0))

    const deliveredA = yield* Queue.unbounded<PeerRpc.StoredMessage>()
    const streamA = yield* Effect.forkChild(
      Stream.runForEach(
        clientA.Subscribe({ sessionId: sessionId("00000000000a") }).pipe(
          Stream.filter((message): message is PeerRpc.StoredMessage => message._tag === "StoredMessage")
        ),
        (message) => Queue.offer(deliveredA, message)
      )
    )

    const headA = yield* Queue.take(deliveredA)
    assert.strictEqual(headA.relayMessageId, relayId("000000000001"))

    const deliveredB = yield* Queue.unbounded<PeerRpc.StoredMessage>()
    yield* Effect.forkChild(
      Stream.runForEach(
        clientB.Subscribe({ sessionId: sessionId("00000000000b") }).pipe(
          Stream.filter((message): message is PeerRpc.StoredMessage => message._tag === "StoredMessage")
        ),
        (message) => Queue.offer(deliveredB, message)
      )
    )
    const headB = yield* Queue.take(deliveredB)
    let expectedHead = "000000000001"
    if (options.isolated) expectedHead = "000000000002"
    assert.strictEqual(headB.relayMessageId, relayId(expectedHead))

    let exit: Exit.Exit<unknown, unknown> | undefined
    if (!options.isolated) exit = yield* Fiber.await(streamA)

    const owners = yield* Effect.forEach([contextA, contextB], (context) =>
      Sharding.Sharding.pipe(
        Effect.map((sharding) => sharding.hasShardId(shardId)),
        Effect.provideContext(context)
      )).pipe(Effect.map((held) => held.filter(Boolean).length))
    assert.strictEqual(owners, expectedOwners)

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
      const { exit, hosting } = yield* probe({ isolated: false })

      assert.deepStrictEqual(
        hosting.toSorted((left, right) => left - right),
        [0, 1],
        "exactly one of the two runners hosts this inbox key, and the other serves its client over the wire"
      )
      assert.isTrue(exit !== undefined && Exit.isSuccess(exit), "runner A's stream ended cleanly")
    }).pipe(Effect.provide(PgContainer.layer)), 0)

  it.live("leaves both runners hosting the key when they do not share a shard map", () =>
    Effect.gen(function*() {
      // The hosting count is the whole control, and it is positive evidence: each runner having its
      // own instance is what distinguishes "sharding elected one owner" from "there was only ever
      // one candidate". There is deliberately no negative assertion on runner A's stream: only the
      // two observed live entities establish that the isolated shard maps each own an instance.
      const { hosting } = yield* probe({ isolated: true })

      assert.deepStrictEqual(hosting, [1, 1], "each runner served its own instance of the inbox")
    }).pipe(Effect.provide(PgContainer.layer)), 0)
})
