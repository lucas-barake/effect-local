import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayRpc from "../src/PeerRelayRpc.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)

const makeLayer = (filename: string) => {
  const base = Layer.mergeAll(
    SqliteClient.layer({ filename }),
    NodeCrypto.layer,
    PeerRelayLimits.layerDefaults
  )
  const store = PeerRelayStore.layerSqlite.pipe(Layer.provide(base))
  return Layer.merge(base, store)
}

describe("PeerRelayStore", () => {
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
            outerEnvelopeDigest: PeerRelayRpc.RelayDigest.make("a".repeat(64)),
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
            outerEnvelopeDigest: PeerRelayRpc.RelayDigest.make("b".repeat(64))
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
            outerEnvelopeDigest: PeerRelayRpc.RelayDigest.make("c".repeat(64))
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
            outerEnvelopeDigest: PeerRelayRpc.RelayDigest.make("d".repeat(64))
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
})
