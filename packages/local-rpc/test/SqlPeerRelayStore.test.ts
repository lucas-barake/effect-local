import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { MysqlClient } from "@effect/sql-mysql2"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import { MySqlContainer, type StartedMySqlContainer } from "@testcontainers/mysql"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import { Context, Data, Effect, Exit, FileSystem, Layer, Option, Redacted } from "effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as SqlPeerRelayStore from "../src/SqlPeerRelayStore.js"
import { runPeerRelayStoreContract } from "./PeerRelayStoreContract.js"

class ContainerError extends Data.TaggedError("ContainerError")<{
  readonly cause: unknown
}> {}

class PgContainer extends Context.Service<PgContainer>()(
  "@lucas-barake/effect-local-rpc/test/PgContainer",
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

  static readonly layerClient = Layer.unwrap(
    Effect.gen(function*() {
      const container = yield* PgContainer
      return PgClient.layer({
        url: Redacted.make(container.getConnectionUri())
      })
    })
  ).pipe(Layer.provide(this.layer))
}

class MysqlContainer extends Context.Service<
  MysqlContainer,
  StartedMySqlContainer
>()("@lucas-barake/effect-local-rpc/test/MysqlContainer") {
  static readonly layer = Layer.effect(this)(
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () => new MySqlContainer("mysql:lts").start(),
        catch: (cause) => new ContainerError({ cause })
      }),
      (container) => Effect.promise(() => container.stop())
    )
  )

  static readonly layerClient = Layer.unwrap(
    Effect.gen(function*() {
      const container = yield* MysqlContainer
      return MysqlClient.layer({
        url: Redacted.make(container.getConnectionUri())
      })
    })
  ).pipe(Layer.provide(this.layer))
}

const SqliteLayer = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  return SqliteClient.layer({
    filename: `${directory}/relay.sqlite`
  })
}).pipe(Layer.unwrap, Layer.provide(NodeFileSystem.layer))

const StoreLive = SqlPeerRelayStore.layer.pipe(
  Layer.provideMerge(NodeCrypto.layer),
  Layer.provideMerge(PeerRelayLimits.layer(PeerRelayLimits.Values.make({
    ...PeerRelayLimits.defaults,
    maxActiveMessagesPerSenderPeer: 1
  })))
)

const MysqlMigrationTestLive = Layer.mergeAll(
  MysqlContainer.layerClient,
  NodeCrypto.layer,
  PeerRelayLimits.layerDefaults
)

const peerId = (suffix: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${suffix}`)

const relayMessageId = (suffix: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${suffix}`)

const documentId = (suffix: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${suffix}`)

describe("SqlPeerRelayStore", () => {
  it.layer(MysqlMigrationTestLive, {
    timeout: 120_000
  })("mysql migrations", (it) => {
    it.effect("recovers after DDL commits a partial first migration", () =>
      Effect.gen(function*() {
        const sql = (yield* SqlClient.SqlClient).withoutTransforms()

        yield* sql.unsafe(`
          CREATE TABLE effect_local_relay_channels (
            malformed INT NOT NULL
          )
        `)

        const firstAttempt = yield* Effect.exit(SqlPeerRelayStore.make)
        assert.strictEqual(Exit.isFailure(firstAttempt), true)

        const recordedAfterFailure = yield* sql.unsafe(
          "SELECT migration_id FROM effect_local_relay_migrations WHERE migration_id = 1"
        )
        assert.strictEqual(recordedAfterFailure.length, 0)

        yield* sql.unsafe("DROP TABLE effect_local_relay_channels")

        yield* SqlPeerRelayStore.make

        const recordedAfterRecovery = yield* sql.unsafe(
          "SELECT migration_id FROM effect_local_relay_migrations WHERE migration_id = 1"
        )
        assert.strictEqual(recordedAfterRecovery.length, 1)

        const relayTables = yield* sql.unsafe(`
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = DATABASE()
            AND table_name IN (
              'effect_local_relay_write_lock',
              'effect_local_relay_channels',
              'effect_local_relay_messages',
              'effect_local_relay_usage',
              'effect_local_relay_reservations'
            )
        `)
        assert.strictEqual(relayTables.length, 5)
      }))
  })
  ;([
    ["pg", Layer.orDie(PgContainer.layerClient)],
    ["mysql", Layer.orDie(MysqlContainer.layerClient)],
    ["sqlite", Layer.orDie(SqliteLayer)]
  ] as const).forEach(([label, layer]) => {
    it.layer(StoreLive.pipe(Layer.provideMerge(layer)), {
      timeout: 120_000
    })(label, (it) => {
      it.effect("conforms to the relay custody contract", () => runPeerRelayStoreContract)

      it.effect("keeps case-distinct routing custody and usage isolated", () =>
        Effect.gen(function*() {
          const store = yield* PeerRelayStore.PeerRelayStore
          const senderPeerId = peerId("000000000011")
          const recipientPeerId = peerId("000000000012")
          const upperChannel = PeerRelayStore.ChannelKey.make({
            tenantId: "Tenant-Case-Distinct",
            senderSubjectId: "Sender-Case-Distinct",
            senderPeerId,
            senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
            recipientSubjectId: "Recipient-Case-Distinct",
            recipientPeerId
          })
          const lowerChannel = PeerRelayStore.ChannelKey.make({
            tenantId: upperChannel.tenantId.toLowerCase(),
            senderSubjectId: upperChannel.senderSubjectId.toLowerCase(),
            senderPeerId,
            senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
            recipientSubjectId: upperChannel.recipientSubjectId.toLowerCase(),
            recipientPeerId
          })
          const makeAdmission = (
            channel: PeerRelayStore.ChannelKey,
            relayId: Identity.RelayMessageId,
            digest: string
          ) =>
            PeerRelayStore.Admission.make({
              channel,
              relayMessageId: relayId,
              relayPeerId: peerId("000000000013"),
              documentIds: [documentId("000000000011")],
              senderConnectionEpoch: "epoch-case-distinct",
              senderSequence: 0,
              payloadVersion: 1,
              messageHash: `message-hash-${digest}`,
              outerEnvelopeDigest: PeerRpc.RelayDigest.make(digest.repeat(64)),
              payload: new Uint8Array([1]),
              messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
              senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
              minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
            })

          assert.strictEqual(
            (yield* store.admit(
              makeAdmission(upperChannel, relayMessageId("000000000011"), "b")
            )).status,
            "Accepted"
          )
          assert.strictEqual(
            (yield* store.admit(
              makeAdmission(lowerChannel, relayMessageId("000000000012"), "c")
            )).status,
            "Accepted"
          )

          for (const channel of [upperChannel, lowerChannel]) {
            assert.deepStrictEqual(
              yield* store.usage(PeerRelayStore.UsageRequest.make({
                scopeKind: "Tenant",
                scopeKey: JSON.stringify([channel.tenantId])
              })),
              {
                activeCount: 1,
                activeBytes: 1,
                retainedCount: 1,
                retainedBytes: 1
              }
            )
            assert.deepStrictEqual(
              yield* store.usage(PeerRelayStore.UsageRequest.make({
                scopeKind: "RecipientSubject",
                scopeKey: JSON.stringify([channel.tenantId, channel.recipientSubjectId])
              })),
              {
                activeCount: 1,
                activeBytes: 1,
                retainedCount: 1,
                retainedBytes: 1
              }
            )
          }

          const lowerClaim = yield* store.claim(PeerRelayStore.ClaimRequest.make({
            recipient: {
              tenantId: lowerChannel.tenantId,
              subjectId: lowerChannel.recipientSubjectId,
              peerId: lowerChannel.recipientPeerId
            },
            sender: {
              subjectId: lowerChannel.senderSubjectId,
              peerId: lowerChannel.senderPeerId
            },
            sessionGeneration: 1,
            authorizedDocumentIds: [documentId("000000000011")]
          }))
          assert.strictEqual(Option.isSome(lowerClaim.message), true)
          if (Option.isSome(lowerClaim.message)) {
            assert.deepStrictEqual(lowerClaim.message.value.channel, lowerChannel)
          }
        }))
    })
  })
})
