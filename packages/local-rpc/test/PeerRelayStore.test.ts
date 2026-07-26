import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as PeerRpcObservability from "../src/internal/peerRpcObservability.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as SqlPeerRelayStore from "../src/SqlPeerRelayStore.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)

const makeLayer = (
  filename: string,
  limits: PeerRelayLimits.Values = PeerRelayLimits.defaults
) => {
  const base = Layer.mergeAll(
    SqliteClient.layer({ filename }),
    NodeCrypto.layer,
    PeerRelayLimits.layer(limits)
  )
  const store = SqlPeerRelayStore.layer.pipe(Layer.provide(base))
  return Layer.merge(base, store)
}

const withStore = <A, E,>(
  effect: Effect.Effect<
    A,
    E,
    PeerRelayStore.PeerRelayStore | SqlClient.SqlClient
  >,
  limits: PeerRelayLimits.Values = PeerRelayLimits.defaults
) =>
  Effect.gen(function*() {
    const filename = join(tmpdir(), `effect-local-relay-${globalThis.crypto.randomUUID()}.sqlite`)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        rmSync(filename, { force: true })
        rmSync(`${filename}-shm`, { force: true })
        rmSync(`${filename}-wal`, { force: true })
      })
    )
    return yield* Effect.scoped(effect.pipe(Effect.provide(makeLayer(filename, limits))))
  })

describe("PeerRelayStore", () => {
  it.effect("records fixed relay outcomes, acknowledgement latency, and exact pending gauges", () => {
    const registry = new Map()
    const limits = PeerRelayLimits.Values.make({
      ...PeerRelayLimits.defaults,
      maxActiveMessagesPerSenderPeer: 1,
      maxRetainedRowsPerSenderPeer: 1
    })
    const metricValue = <Input, State,>(metric: Metric.Metric<Input, State>) =>
      Metric.value(metric).pipe(
        Effect.provideService(Metric.CurrentMetricAttributes, {})
      )
    return withStore(
      Effect.gen(function*() {
        const store = yield* PeerRelayStore.PeerRelayStore
        const sql = yield* SqlClient.SqlClient
        const channel = PeerRelayStore.ChannelKey.make({
          tenantId: "tenant-observe",
          senderSubjectId: "sender-observe",
          senderPeerId: peer("000000000051"),
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
          recipientSubjectId: "recipient-observe",
          recipientPeerId: peer("000000000052")
        })
        const admission = PeerRelayStore.Admission.make({
          channel,
          relayMessageId: relayId("000000000051"),
          relayPeerId: peer("000000000053"),
          documentIds: [documentId("000000000051")],
          senderConnectionEpoch: "epoch-observe",
          senderSequence: 0,
          payloadVersion: 1,
          messageHash: "message-hash-observe",
          outerEnvelopeDigest: PeerRpc.RelayDigest.make("5".repeat(64)),
          payload: new Uint8Array([5]),
          messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
          senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
          minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
        })
        yield* store.admit(admission)
        const claimed = yield* store.claim({
          recipient: {
            tenantId: channel.tenantId,
            subjectId: channel.recipientSubjectId,
            peerId: channel.recipientPeerId
          },
          sender: {
            subjectId: channel.senderSubjectId,
            peerId: channel.senderPeerId
          },
          sessionGeneration: 1,
          authorizedDocumentIds: admission.documentIds
        })
        assert.strictEqual(Option.isSome(claimed.message), true)
        if (Option.isNone(claimed.message)) return
        const message = claimed.message.value
        yield* sql`UPDATE effect_local_relay_messages
        SET created_at = 0
        WHERE message_id = ${message.rowId}`
        yield* store.acknowledge({
          channel,
          relayMessageId: message.relayMessageId,
          claimToken: message.claimToken,
          messageHash: message.messageHash,
          sessionGeneration: message.sessionGeneration,
          recipient: {
            tenantId: channel.tenantId,
            subjectId: channel.recipientSubjectId,
            peerId: channel.recipientPeerId
          }
        })
        yield* store.usage()
        const rejected = yield* store.admit(PeerRelayStore.Admission.make({
          ...admission,
          relayMessageId: relayId("000000000054"),
          outerEnvelopeDigest: PeerRpc.RelayDigest.make("4".repeat(64))
        })).pipe(Effect.exit)
        assert.strictEqual(Exit.isFailure(rejected), true)
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayOutcomes("RelayAdmit", "Send", "Accepted")
          )).count,
          1
        )
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayOutcomes("RelayClaim", "Receive", "Claimed")
          )).count,
          1
        )
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayOutcomes(
              "RelayAcknowledge",
              "Receive",
              "Acknowledged"
            )
          )).count,
          1
        )
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayLatencyMillis(
              "RelayAcknowledge",
              "Receive",
              "Acknowledged"
            )
          )).count,
          1
        )
        assert.strictEqual(
          (yield* metricValue(PeerRpcObservability.relayPendingItems())).value,
          0
        )
        assert.strictEqual(
          (yield* metricValue(PeerRpcObservability.relayPendingBytes())).value,
          0
        )
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayQuotaRejections("SenderPeer")
          )).count,
          1
        )
      }),
      limits
    ).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("migrates a real WAL database and fences claim payload loading and terminal duplicates", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-relay-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          rmSync(filename, { force: true })
          rmSync(`${filename}-shm`, { force: true })
          rmSync(`${filename}-wal`, { force: true })
        })
      )
      yield* Effect.scoped(
        Effect.gen(function*() {
          const store = yield* PeerRelayStore.PeerRelayStore
          const sql = yield* SqlClient.SqlClient
          const channel = PeerRelayStore.ChannelKey.make({
            tenantId: "tenant",
            senderSubjectId: "sender",
            senderPeerId: peer("000000000001"),
            senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
            recipientSubjectId: "recipient",
            recipientPeerId: peer("000000000002")
          })
          const payload = new Uint8Array([1, 2, 3, 4])
          const admission = PeerRelayStore.Admission.make({
            channel,
            relayMessageId: relayId("000000000001"),
            relayPeerId: peer("000000000003"),
            documentIds: [documentId("000000000001")],
            senderConnectionEpoch: "epoch-1",
            senderSequence: 0,
            payloadVersion: 1,
            messageHash: "message-hash",
            outerEnvelopeDigest: PeerRpc.RelayDigest.make("a".repeat(64)),
            payload,
            messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
            senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
            minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
          })
          const accepted = yield* store.admit(admission)
          assert.strictEqual(accepted.status, "Accepted")
          assert.deepStrictEqual(yield* store.usage(), {
            activeCount: 1,
            activeBytes: payload.byteLength,
            retainedCount: 1,
            retainedBytes: payload.byteLength
          })
          const duplicate = yield* store.admit(admission)
          assert.strictEqual(duplicate.status, "Duplicate")
          assert.strictEqual((yield* store.usage()).activeCount, 1)

          const claimed = yield* store.claim({
            recipient: {
              tenantId: "tenant",
              subjectId: "recipient",
              peerId: channel.recipientPeerId
            },
            sender: {
              subjectId: "sender",
              peerId: channel.senderPeerId
            },
            sessionGeneration: 7,
            authorizedDocumentIds: admission.documentIds
          })
          assert.strictEqual(Option.isSome(claimed.message), true)
          if (Option.isNone(claimed.message)) return
          const message = claimed.message.value
          assert.strictEqual(Object.hasOwn(message, "payload"), false)
          assert.strictEqual(message.payloadBytes, payload.byteLength)
          const routeColumns = [
            ["tenant_id", "corrupt-tenant", channel.tenantId],
            ["sender_subject_id", "corrupt-sender", channel.senderSubjectId],
            ["sender_peer_id", "corrupt-peer", channel.senderPeerId]
          ] as const
          for (const [column, corrupt, original] of routeColumns) {
            yield* sql.unsafe(
              `UPDATE effect_local_relay_messages SET ${column} = ? WHERE message_id = ?`,
              [corrupt, message.rowId]
            )
            assert.strictEqual(
              (yield* Effect.exit(store.loadClaimedPayload({
                rowId: message.rowId,
                channel,
                relayMessageId: message.relayMessageId,
                claimToken: message.claimToken,
                sessionGeneration: message.sessionGeneration,
                payloadBytes: message.payloadBytes
              })))._tag,
              "Failure"
            )
            yield* sql.unsafe(
              `UPDATE effect_local_relay_messages SET ${column} = ? WHERE message_id = ?`,
              [original, message.rowId]
            )
          }
          assert.deepStrictEqual(
            yield* store.loadClaimedPayload({
              rowId: message.rowId,
              channel,
              relayMessageId: message.relayMessageId,
              claimToken: message.claimToken,
              sessionGeneration: message.sessionGeneration,
              payloadBytes: message.payloadBytes
            }),
            payload
          )
          const stalePayload = yield* Effect.exit(store.loadClaimedPayload({
            rowId: message.rowId,
            channel,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            sessionGeneration: message.sessionGeneration + 1,
            payloadBytes: message.payloadBytes
          }))
          assert.strictEqual(stalePayload._tag, "Failure")
          const terminal = {
            channel,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: message.messageHash,
            sessionGeneration: message.sessionGeneration,
            recipient: {
              tenantId: "tenant",
              subjectId: "recipient",
              peerId: channel.recipientPeerId
            }
          } as const
          assert.strictEqual((yield* store.acknowledge(terminal)).status, "Changed")
          assert.strictEqual((yield* store.acknowledge(terminal)).status, "Duplicate")
          assert.strictEqual((yield* store.usage()).activeCount, 0)
          assert.strictEqual((yield* store.usage()).retainedCount, 1)
          const stored = yield* sql<{ readonly payload: Uint8Array | null }>`
          SELECT payload FROM effect_local_relay_messages
        `
          assert.strictEqual(stored[0]?.payload, null)

          const expiringAdmission = PeerRelayStore.Admission.make({
            ...admission,
            relayMessageId: relayId("000000000002"),
            outerEnvelopeDigest: PeerRpc.RelayDigest.make("b".repeat(64))
          })
          yield* store.admit(expiringAdmission)
          const expiringClaim = yield* store.claim({
            recipient: terminal.recipient,
            sender: {
              subjectId: channel.senderSubjectId,
              peerId: channel.senderPeerId
            },
            sessionGeneration: 8,
            authorizedDocumentIds: expiringAdmission.documentIds
          })
          assert.strictEqual(Option.isSome(expiringClaim.message), true)
          if (Option.isNone(expiringClaim.message)) return
          const expiring = expiringClaim.message.value
          const loadExpiring = () =>
            store.loadClaimedPayload({
              rowId: expiring.rowId,
              channel,
              relayMessageId: expiring.relayMessageId,
              claimToken: expiring.claimToken,
              sessionGeneration: expiring.sessionGeneration,
              payloadBytes: expiring.payloadBytes
            })
          const claimColumns = [
            ["claimed_message_id", String(expiring.rowId + 1)],
            ["claim_token", "'clm_00000000-0000-4000-8000-000000000009'"],
            ["claim_session_generation", String(expiring.sessionGeneration + 1)],
            ["claim_deadline", String(expiring.claimDeadline + 1)]
          ] as const
          for (const [column, corrupt] of claimColumns) {
            yield* sql.unsafe(
              `UPDATE effect_local_relay_channels SET ${column} = ${corrupt} WHERE channel_id = ?`,
              [1]
            )
            assert.strictEqual((yield* Effect.exit(loadExpiring()))._tag, "Failure")
            yield* sql`UPDATE effect_local_relay_channels
            SET claimed_message_id = ${expiring.rowId},
                claim_token = ${expiring.claimToken},
                claim_session_generation = ${expiring.sessionGeneration},
                claim_deadline = ${expiring.claimDeadline}
            WHERE channel_id = 1`
          }
          yield* sql`UPDATE effect_local_relay_messages
          SET expires_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) - 1
          WHERE message_id = ${expiring.rowId}`
          assert.strictEqual((yield* Effect.exit(loadExpiring()))._tag, "Failure")
          assert.strictEqual(
            (yield* store.acknowledge({
              ...terminal,
              relayMessageId: expiring.relayMessageId,
              claimToken: expiring.claimToken,
              sessionGeneration: expiring.sessionGeneration
            })).status,
            "Stale"
          )
          yield* store.expire({ batchSize: 10 })
          const expired = yield* sql<{ readonly state: string }>`
          SELECT state FROM effect_local_relay_messages WHERE message_id = ${expiring.rowId}
        `
          assert.strictEqual(expired[0]?.state, "Expired")

          const corruptAdmission = PeerRelayStore.Admission.make({
            ...admission,
            relayMessageId: relayId("000000000003"),
            outerEnvelopeDigest: PeerRpc.RelayDigest.make("c".repeat(64))
          })
          yield* store.admit(corruptAdmission)
          yield* sql`UPDATE effect_local_relay_messages
          SET tenant_id = 'other'
          WHERE relay_message_id = ${corruptAdmission.relayMessageId}`
          const undisclosed = yield* store.claim({
            recipient: terminal.recipient,
            sender: {
              subjectId: channel.senderSubjectId,
              peerId: channel.senderPeerId
            },
            sessionGeneration: 9,
            authorizedDocumentIds: corruptAdmission.documentIds
          })
          assert.strictEqual(Option.isNone(undisclosed.message), true)
          yield* store.repair({ batchSize: 10 })
          const repaired = yield* sql<{ readonly state: string }>`
          SELECT state FROM effect_local_relay_messages
          WHERE relay_message_id = ${corruptAdmission.relayMessageId}
        `
          assert.strictEqual(repaired[0]?.state, "DeadLettered")

          const restartedChannel = PeerRelayStore.ChannelKey.make({
            ...channel,
            senderReplicaIncarnation: Identity.ReplicaIncarnation.make(2)
          })
          const restartedAdmission = PeerRelayStore.Admission.make({
            ...admission,
            channel: restartedChannel,
            relayMessageId: relayId("000000000004"),
            outerEnvelopeDigest: PeerRpc.RelayDigest.make("d".repeat(64))
          })
          yield* store.admit(restartedAdmission)
          const discovered = yield* store.claim({
            recipient: terminal.recipient,
            sender: {
              subjectId: restartedChannel.senderSubjectId,
              peerId: restartedChannel.senderPeerId
            },
            sessionGeneration: 10,
            authorizedDocumentIds: restartedAdmission.documentIds
          })
          assert.strictEqual(Option.isSome(discovered.message), true)
          if (Option.isSome(discovered.message)) {
            assert.strictEqual(discovered.message.value.channel.senderReplicaIncarnation, 2)
          }
        }).pipe(Effect.provide(makeLayer(filename)))
      )
    }))

  it.effect("collects reservations when SQLite foreign keys are disabled", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-relay-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          rmSync(filename, { force: true })
          rmSync(`${filename}-shm`, { force: true })
          rmSync(`${filename}-wal`, { force: true })
        })
      )
      yield* Effect.scoped(
        Effect.gen(function*() {
          const store = yield* PeerRelayStore.PeerRelayStore
          const sql = yield* SqlClient.SqlClient
          yield* sql`PRAGMA foreign_keys = OFF`
          const foreignKeys = yield* sql<{ readonly foreign_keys: number }>`PRAGMA foreign_keys`
          assert.strictEqual(foreignKeys[0]?.foreign_keys, 0)
          const channel = PeerRelayStore.ChannelKey.make({
            tenantId: "tenant",
            senderSubjectId: "sender",
            senderPeerId: peer("000000000011"),
            senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
            recipientSubjectId: "recipient",
            recipientPeerId: peer("000000000012")
          })
          const admission = PeerRelayStore.Admission.make({
            channel,
            relayMessageId: relayId("000000000011"),
            relayPeerId: peer("000000000013"),
            documentIds: [documentId("000000000011")],
            senderConnectionEpoch: "epoch-1",
            senderSequence: 0,
            payloadVersion: 1,
            messageHash: "message-hash",
            outerEnvelopeDigest: PeerRpc.RelayDigest.make("e".repeat(64)),
            payload: new Uint8Array([1]),
            messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
            senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
            minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
          })
          yield* store.admit(admission)
          const claimed = yield* store.claim({
            recipient: {
              tenantId: channel.tenantId,
              subjectId: channel.recipientSubjectId,
              peerId: channel.recipientPeerId
            },
            sender: {
              subjectId: channel.senderSubjectId,
              peerId: channel.senderPeerId
            },
            sessionGeneration: 1,
            authorizedDocumentIds: admission.documentIds
          })
          assert.strictEqual(Option.isSome(claimed.message), true)
          if (Option.isNone(claimed.message)) return
          const message = claimed.message.value
          yield* store.acknowledge({
            channel,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: message.messageHash,
            sessionGeneration: message.sessionGeneration,
            recipient: {
              tenantId: channel.tenantId,
              subjectId: channel.recipientSubjectId,
              peerId: channel.recipientPeerId
            }
          })
          yield* sql`UPDATE effect_local_relay_messages
            SET deduplicate_until = 0
            WHERE message_id = ${message.rowId}`
          yield* sql`INSERT INTO effect_local_relay_usage (
              scope_kind,
              scope_key,
              active_count,
              active_bytes,
              retained_count,
              retained_bytes
            ) VALUES ('Tenant', 'unrelated-zero', 0, 0, 0, 0)`
          yield* store.collect({ batchSize: 10 })
          const reservations = yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM effect_local_relay_reservations
            WHERE message_id = ${message.rowId}
          `
          assert.strictEqual(reservations[0]?.count, 0)
          const unrelated = yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM effect_local_relay_usage
            WHERE scope_kind = 'Tenant'
              AND scope_key = 'unrelated-zero'
          `
          assert.strictEqual(unrelated[0]?.count, 1)
        }).pipe(Effect.provide(makeLayer(filename)))
      )
    }))

  it.effect("repairs an active message whose channel is missing", () =>
    withStore(Effect.gen(function*() {
      const store = yield* PeerRelayStore.PeerRelayStore
      const sql = yield* SqlClient.SqlClient
      const channel = PeerRelayStore.ChannelKey.make({
        tenantId: "tenant-orphan",
        senderSubjectId: "sender-orphan",
        senderPeerId: peer("000000000081"),
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        recipientSubjectId: "recipient-orphan",
        recipientPeerId: peer("000000000082")
      })
      yield* store.admit(PeerRelayStore.Admission.make({
        channel,
        relayMessageId: relayId("000000000081"),
        relayPeerId: peer("000000000083"),
        documentIds: [documentId("000000000081")],
        senderConnectionEpoch: "epoch-orphan",
        senderSequence: 0,
        payloadVersion: 1,
        messageHash: "message-hash-orphan",
        outerEnvelopeDigest: PeerRpc.RelayDigest.make("8".repeat(64)),
        payload: new Uint8Array([8]),
        messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
        senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
        minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
      }))
      yield* sql`PRAGMA foreign_keys = OFF`
      yield* sql`DELETE FROM effect_local_relay_channels`
      yield* store.repair({ batchSize: 1 })
      const rows = yield* sql<{
        readonly state: string
        readonly payload: Uint8Array | null
      }>`SELECT state, payload FROM effect_local_relay_messages`
      assert.deepStrictEqual(rows, [{
        state: "DeadLettered",
        payload: null
      }])
      assert.deepStrictEqual(yield* store.usage(), {
        activeCount: 0,
        activeBytes: 0,
        retainedCount: 1,
        retainedBytes: 1
      })
    })))

  it.effect("dead letters a released claim at the delivery attempt cap and preserves it across restart", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-relay-${globalThis.crypto.randomUUID()}.sqlite`)
      const limits = PeerRelayLimits.Values.make({
        ...PeerRelayLimits.defaults,
        maximumDeliveryAttempts: 1
      })
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          rmSync(filename, { force: true })
          rmSync(`${filename}-shm`, { force: true })
          rmSync(`${filename}-wal`, { force: true })
        })
      )
      const channel = PeerRelayStore.ChannelKey.make({
        tenantId: "tenant-release-cap",
        senderSubjectId: "sender-release-cap",
        senderPeerId: peer("000000000071"),
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        recipientSubjectId: "recipient-release-cap",
        recipientPeerId: peer("000000000072")
      })
      const admission = PeerRelayStore.Admission.make({
        channel,
        relayMessageId: relayId("000000000071"),
        relayPeerId: peer("000000000073"),
        documentIds: [documentId("000000000071")],
        senderConnectionEpoch: "epoch-release-cap",
        senderSequence: 0,
        payloadVersion: 1,
        messageHash: "message-hash-release-cap",
        outerEnvelopeDigest: PeerRpc.RelayDigest.make("7".repeat(64)),
        payload: new Uint8Array([7]),
        messageTtlMillis: limits.messageTtlMillis,
        senderRetryHorizonMillis: limits.maximumSenderRetryHorizonMillis,
        minimumTerminalRetentionMillis: limits.minimumTerminalRetentionMillis
      })
      const claimRequest = {
        recipient: {
          tenantId: channel.tenantId,
          subjectId: channel.recipientSubjectId,
          peerId: channel.recipientPeerId
        },
        sender: {
          subjectId: channel.senderSubjectId,
          peerId: channel.senderPeerId
        },
        sessionGeneration: 1,
        authorizedDocumentIds: admission.documentIds
      } as const
      yield* Effect.scoped(
        Effect.gen(function*() {
          const store = yield* PeerRelayStore.PeerRelayStore
          const sql = yield* SqlClient.SqlClient
          yield* store.admit(admission)
          const claimed = yield* store.claim(claimRequest)
          assert.strictEqual(Option.isSome(claimed.message), true)
          if (Option.isNone(claimed.message)) return
          const message = claimed.message.value
          const releaseRequest = PeerRelayStore.ReleaseRequest.make({
            channel,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            sessionGeneration: message.sessionGeneration
          })
          assert.deepStrictEqual(yield* store.release(releaseRequest), {
            status: "Changed",
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry"
          })
          const rows = yield* sql<{
            readonly state: string
            readonly retryCount: number
            readonly payload: Uint8Array | null
            readonly payloadLength: number
            readonly claimToken: string | null
            readonly terminalReason: string | null
          }>`SELECT
              state,
              retry_count AS retryCount,
              payload,
              payload_length AS payloadLength,
              claim_token AS claimToken,
              terminal_reason AS terminalReason
            FROM effect_local_relay_messages`
          assert.deepStrictEqual(rows, [{
            state: "DeadLettered",
            retryCount: 1,
            payload: null,
            payloadLength: 0,
            claimToken: null,
            terminalReason: "MaximumDeliveryAttempts"
          }])
          const fences = yield* sql<{
            readonly claimedMessageId: number | null
            readonly claimToken: string | null
          }>`SELECT
              claimed_message_id AS claimedMessageId,
              claim_token AS claimToken
            FROM effect_local_relay_channels`
          assert.deepStrictEqual(fences, [{
            claimedMessageId: null,
            claimToken: null
          }])
          const reservations = yield* sql<{
            readonly activeConsumed: number
            readonly retainedConsumed: number
          }>`SELECT
              active_consumed AS activeConsumed,
              retained_consumed AS retainedConsumed
            FROM effect_local_relay_reservations`
          assert.deepStrictEqual(reservations, [{
            activeConsumed: 1,
            retainedConsumed: 0
          }])
          assert.deepStrictEqual(yield* store.usage(), {
            activeCount: 0,
            activeBytes: 0,
            retainedCount: 1,
            retainedBytes: 1
          })
          assert.strictEqual(Option.isNone((yield* store.claim(claimRequest)).message), true)
          assert.strictEqual((yield* store.release(releaseRequest)).status, "Stale")
        }).pipe(Effect.provide(makeLayer(filename, limits)))
      )
      yield* Effect.scoped(
        Effect.gen(function*() {
          const store = yield* PeerRelayStore.PeerRelayStore
          const sql = yield* SqlClient.SqlClient
          const rows = yield* sql<{
            readonly state: string
            readonly retryCount: number
            readonly payload: Uint8Array | null
          }>`SELECT
              state,
              retry_count AS retryCount,
              payload
            FROM effect_local_relay_messages`
          assert.deepStrictEqual(rows, [{
            state: "DeadLettered",
            retryCount: 1,
            payload: null
          }])
          assert.deepStrictEqual(yield* store.usage(), {
            activeCount: 0,
            activeBytes: 0,
            retainedCount: 1,
            retainedBytes: 1
          })
          assert.strictEqual(Option.isNone((yield* store.claim(claimRequest)).message), true)
        }).pipe(Effect.provide(makeLayer(filename, limits)))
      )
    }))

  it.effect("dead letters an abandoned claim at the delivery attempt cap exactly once", () => {
    const registry = new Map()
    const metricValue = <Input, State,>(metric: Metric.Metric<Input, State>) =>
      Metric.value(metric).pipe(
        Effect.provideService(Metric.CurrentMetricAttributes, {})
      )
    return withStore(
      Effect.gen(function*() {
        const store = yield* PeerRelayStore.PeerRelayStore
        const sql = yield* SqlClient.SqlClient
        const channel = PeerRelayStore.ChannelKey.make({
          tenantId: "tenant-recover-cap",
          senderSubjectId: "sender-recover-cap",
          senderPeerId: peer("000000000061"),
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
          recipientSubjectId: "recipient-recover-cap",
          recipientPeerId: peer("000000000062")
        })
        const admission = PeerRelayStore.Admission.make({
          channel,
          relayMessageId: relayId("000000000061"),
          relayPeerId: peer("000000000063"),
          documentIds: [documentId("000000000061")],
          senderConnectionEpoch: "epoch-recover-cap",
          senderSequence: 0,
          payloadVersion: 1,
          messageHash: "message-hash-recover-cap",
          outerEnvelopeDigest: PeerRpc.RelayDigest.make("6".repeat(64)),
          payload: new Uint8Array([6]),
          messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
          senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
          minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
        })
        yield* store.admit(admission)
        const claimRequest = {
          recipient: {
            tenantId: channel.tenantId,
            subjectId: channel.recipientSubjectId,
            peerId: channel.recipientPeerId
          },
          sender: {
            subjectId: channel.senderSubjectId,
            peerId: channel.senderPeerId
          },
          sessionGeneration: 1,
          authorizedDocumentIds: admission.documentIds
        } as const
        const claimed = yield* store.claim(claimRequest)
        assert.strictEqual(Option.isSome(claimed.message), true)
        if (Option.isNone(claimed.message)) return
        yield* sql`UPDATE effect_local_relay_messages
        SET claim_deadline = 0
        WHERE message_id = ${claimed.message.value.rowId}`
        yield* sql`UPDATE effect_local_relay_channels
        SET claim_deadline = 0
        WHERE claimed_message_id = ${claimed.message.value.rowId}`
        const recovery = store.recover({ batchSize: 1 })
        assert.strictEqual((yield* recovery).processed, 1)
        const rows = yield* sql<{
          readonly state: string
          readonly retryCount: number
          readonly payload: Uint8Array | null
          readonly terminalReason: string | null
        }>`SELECT
          state,
          retry_count AS retryCount,
          payload,
          terminal_reason AS terminalReason
        FROM effect_local_relay_messages`
        assert.deepStrictEqual(rows, [{
          state: "DeadLettered",
          retryCount: 1,
          payload: null,
          terminalReason: "MaximumDeliveryAttempts"
        }])
        assert.deepStrictEqual(yield* store.usage(), {
          activeCount: 0,
          activeBytes: 0,
          retainedCount: 1,
          retainedBytes: 1
        })
        assert.strictEqual((yield* recovery).processed, 0)
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayOutcomes(
              "RelayMaintenance",
              "Receive",
              "DeadLettered",
              "Recover"
            )
          )).count,
          1
        )
        assert.strictEqual(
          (yield* metricValue(
            PeerRpcObservability.relayOutcomes(
              "RelayMaintenance",
              "Receive",
              "Released",
              "Recover"
            )
          )).count,
          1
        )
        assert.strictEqual(Option.isNone((yield* store.claim(claimRequest)).message), true)
      }),
      PeerRelayLimits.Values.make({
        ...PeerRelayLimits.defaults,
        maximumDeliveryAttempts: 1
      })
    ).pipe(Effect.provideService(Metric.MetricRegistry, registry))
  })

  it.effect("drains maintenance by deadline even when the row cursor is ahead", () =>
    withStore(Effect.gen(function*() {
      const store = yield* PeerRelayStore.PeerRelayStore
      const sql = yield* SqlClient.SqlClient
      const channel = PeerRelayStore.ChannelKey.make({
        tenantId: "tenant",
        senderSubjectId: "sender",
        senderPeerId: peer("000000000021"),
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        recipientSubjectId: "recipient",
        recipientPeerId: peer("000000000022")
      })
      const admission = PeerRelayStore.Admission.make({
        channel,
        relayMessageId: relayId("000000000021"),
        relayPeerId: peer("000000000023"),
        documentIds: [documentId("000000000021")],
        senderConnectionEpoch: "epoch-1",
        senderSequence: 0,
        payloadVersion: 1,
        messageHash: "message-hash",
        outerEnvelopeDigest: PeerRpc.RelayDigest.make("f".repeat(64)),
        payload: new Uint8Array([1]),
        messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
        senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
        minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
      })
      yield* store.admit(admission)
      const claim = yield* store.claim({
        recipient: {
          tenantId: channel.tenantId,
          subjectId: channel.recipientSubjectId,
          peerId: channel.recipientPeerId
        },
        sender: {
          subjectId: channel.senderSubjectId,
          peerId: channel.senderPeerId
        },
        sessionGeneration: 1,
        authorizedDocumentIds: admission.documentIds
      })
      assert.strictEqual(Option.isSome(claim.message), true)
      if (Option.isNone(claim.message)) return
      const message = claim.message.value
      yield* sql`UPDATE effect_local_relay_messages
        SET claim_deadline = 0
        WHERE message_id = ${message.rowId}`
      yield* sql`UPDATE effect_local_relay_channels
        SET claim_deadline = 0
        WHERE claimed_message_id = ${message.rowId}`
      const recovered = yield* store.recover({
        cursor: message.rowId,
        batchSize: 1
      })
      assert.strictEqual(recovered.processed, 1)

      yield* sql`UPDATE effect_local_relay_messages
        SET expires_at = 0
        WHERE message_id = ${message.rowId}`
      const expired = yield* store.expire({
        cursor: message.rowId,
        batchSize: 1
      })
      assert.strictEqual(expired.processed, 1)

      yield* sql`UPDATE effect_local_relay_messages
        SET deduplicate_until = 0
        WHERE message_id = ${message.rowId}`
      const collected = yield* store.collect({
        cursor: message.rowId,
        batchSize: 1
      })
      assert.strictEqual(collected.processed, 1)
      const remaining = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM effect_local_relay_messages
        WHERE message_id = ${message.rowId}
      `
      assert.strictEqual(remaining[0]?.count, 0)
    })))

  it.effect("reconciles one bounded structural page without rebuilding usage", () =>
    withStore(Effect.gen(function*() {
      const store = yield* PeerRelayStore.PeerRelayStore
      const sql = yield* SqlClient.SqlClient
      const channel = PeerRelayStore.ChannelKey.make({
        tenantId: "tenant",
        senderSubjectId: "sender",
        senderPeerId: peer("000000000031"),
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        recipientSubjectId: "recipient",
        recipientPeerId: peer("000000000032")
      })
      for (let index = 1; index <= 3; index++) {
        yield* store.admit(PeerRelayStore.Admission.make({
          channel,
          relayMessageId: relayId(`00000000003${index}`),
          relayPeerId: peer("000000000033"),
          documentIds: [documentId("000000000031")],
          senderConnectionEpoch: "epoch-1",
          senderSequence: index,
          payloadVersion: 1,
          messageHash: `message-hash-${index}`,
          outerEnvelopeDigest: PeerRpc.RelayDigest.make(String(index).repeat(64)),
          payload: new Uint8Array([index]),
          messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
          senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
          minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
        }))
      }
      const ids = yield* sql<{ readonly messageId: number }>`
        SELECT message_id AS messageId
        FROM effect_local_relay_messages
        ORDER BY message_id
      `
      const first = yield* store.reconcile({ batchSize: 1 })
      assert.deepStrictEqual(first, {
        cursor: ids[0]!.messageId,
        processed: 1,
        hasMore: true
      })
      const firstCursor = first.cursor
      assert.isDefined(firstCursor)
      const second = yield* store.reconcile({
        cursor: firstCursor,
        batchSize: 1
      })
      assert.deepStrictEqual(second, {
        cursor: ids[1]!.messageId,
        processed: 1,
        hasMore: true
      })
      const secondCursor = second.cursor
      assert.isDefined(secondCursor)
      const third = yield* store.reconcile({
        cursor: secondCursor,
        batchSize: 1
      })
      assert.deepStrictEqual(third, {
        cursor: ids[2]!.messageId,
        processed: 1,
        hasMore: false
      })
    })))

  it.effect("plans cross incarnation claims without a temporary ordering tree", () =>
    withStore(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* sql`WITH RECURSIVE incarnations(value) AS (
          VALUES(1)
          UNION ALL
          SELECT value + 1 FROM incarnations WHERE value < 10000
        )
        INSERT INTO effect_local_relay_channels (
          tenant_id,
          sender_subject_id,
          sender_peer_id,
          sender_replica_incarnation,
          recipient_subject_id,
          recipient_peer_id,
          next_sequence
        )
        SELECT
          'tenant-plan',
          'sender-plan',
          'peer_00000000-0000-4000-8000-000000000041',
          value,
          'recipient-plan',
          'peer_00000000-0000-4000-8000-000000000042',
          1
        FROM incarnations`
      yield* sql`INSERT INTO effect_local_relay_messages (
          channel_id,
          channel_sequence,
          tenant_id,
          sender_subject_id,
          sender_peer_id,
          recipient_subject_id,
          recipient_peer_id,
          relay_message_id,
          relay_peer_id,
          sender_connection_epoch,
          sender_sequence,
          document_ids,
          payload_version,
          message_hash,
          outer_envelope_digest,
          payload,
          payload_length,
          state,
          created_at,
          expires_at,
          deduplicate_until,
          next_eligible_at
        )
        SELECT
          channel_id,
          0,
          tenant_id,
          sender_subject_id,
          sender_peer_id,
          recipient_subject_id,
          recipient_peer_id,
          'relay-' || channel_id,
          'peer_00000000-0000-4000-8000-000000000043',
          'epoch-1',
          0,
          '["doc_00000000-0000-4000-8000-000000000041"]',
          1,
          'hash-' || channel_id,
          ${"a".repeat(64)},
          x'01',
          1,
          'Pending',
          channel_id,
          9999999999999,
          9999999999999,
          0
        FROM effect_local_relay_channels`
      yield* sql`INSERT INTO effect_local_relay_reservations (
          message_id,
          sender_peer_usage_key,
          recipient_peer_usage_key,
          recipient_subject_usage_key,
          tenant_usage_key,
          shard_usage_key,
          active_count_delta,
          active_bytes_delta,
          retained_count_delta,
          retained_bytes_delta
        )
        SELECT
          message_id,
          'sender',
          'recipient-peer',
          'recipient-subject',
          'tenant',
          'shard',
          1,
          1,
          1,
          1
        FROM effect_local_relay_messages`
      yield* sql`ANALYZE`
      const plan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT m.message_id
        FROM effect_local_relay_messages m
          INDEXED BY effect_local_relay_messages_claim_admission
        JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
        JOIN effect_local_relay_reservations r ON r.message_id = m.message_id
        WHERE m.tenant_id = 'tenant-plan'
          AND m.sender_subject_id = 'sender-plan'
          AND m.sender_peer_id = 'peer_00000000-0000-4000-8000-000000000041'
          AND m.recipient_subject_id = 'recipient-plan'
          AND m.recipient_peer_id = 'peer_00000000-0000-4000-8000-000000000042'
          AND c.tenant_id = 'tenant-plan'
          AND c.recipient_subject_id = 'recipient-plan'
          AND c.recipient_peer_id = 'peer_00000000-0000-4000-8000-000000000042'
          AND c.sender_subject_id = 'sender-plan'
          AND c.sender_peer_id = 'peer_00000000-0000-4000-8000-000000000041'
          AND m.tenant_id = c.tenant_id
          AND m.sender_subject_id = c.sender_subject_id
          AND m.sender_peer_id = c.sender_peer_id
          AND c.claimed_message_id IS NULL
          AND m.state = 'Pending'
          AND m.next_eligible_at <= 1
          AND m.expires_at > 1
          AND m.payload IS NOT NULL
          AND m.payload_length = length(m.payload)
          AND r.active_consumed = 0
          AND NOT EXISTS (
            SELECT 1
            FROM effect_local_relay_messages earlier
            WHERE earlier.channel_id = m.channel_id
              AND earlier.channel_sequence < m.channel_sequence
              AND earlier.state IN ('Pending', 'Claimed')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM json_each(m.document_ids) document
            WHERE document.value NOT IN (
              SELECT value
              FROM json_each('["doc_00000000-0000-4000-8000-000000000041"]')
            )
          )
        ORDER BY m.created_at, m.message_id
        LIMIT 1`
      assert.strictEqual(
        plan.some((row) => row.detail.includes("effect_local_relay_messages_claim_admission")),
        true
      )
      assert.strictEqual(
        plan.some((row) => row.detail.includes("USE TEMP B-TREE")),
        false
      )
      const recoveryPlan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT message_id
        FROM effect_local_relay_messages
        WHERE state = 'Claimed'
          AND claim_deadline <= 1
        ORDER BY claim_deadline, message_id
        LIMIT 101`
      const expiryPlan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT message_id
        FROM effect_local_relay_messages
        WHERE state IN ('Pending', 'Claimed')
          AND expires_at <= 1
        ORDER BY expires_at, message_id
        LIMIT 101`
      const collectionPlan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT message_id
        FROM effect_local_relay_messages
        WHERE state IN ('Acknowledged', 'DeadLettered', 'Expired')
          AND deduplicate_until <= 1
        ORDER BY deduplicate_until, message_id
        LIMIT 101`
      for (
        const [maintenancePlan, index] of [
          [recoveryPlan, "effect_local_relay_messages_recovery"],
          [expiryPlan, "effect_local_relay_messages_expiry"],
          [collectionPlan, "effect_local_relay_messages_collection"]
        ] as const
      ) {
        assert.strictEqual(
          maintenancePlan.some((row) => row.detail.includes(index)),
          true
        )
        assert.strictEqual(
          maintenancePlan.some((row) => row.detail.includes("USE TEMP B-TREE")),
          false
        )
      }
    })))
})
