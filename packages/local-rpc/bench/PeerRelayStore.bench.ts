import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterAll, bench } from "vitest"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayRpc from "../src/PeerRelayRpc.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"

const backlogSizes = [128, 1_024] as const
const drainBatchSize = 8
const payloadBytes = 1_024
const benchmarkOptions = {
  iterations: 8,
  time: 0,
  warmupIterations: 2,
  warmupTime: 0
} as const

const suffix = (value: number) => String(value).padStart(12, "0")
const peerId = (value: number) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${suffix(value)}`)
const relayMessageId = (value: number) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${suffix(value)}`)
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const relayPeerId = peerId(1)
const recipientPeerId = peerId(2)
const payload = Uint8Array.from({ length: payloadBytes }, (_, index) => index % 251)

let nextMessage = 1
let nextSenderPeer = 10

interface WorkItem {
  readonly admission: PeerRelayStore.Admission
  readonly claim: PeerRelayStore.ClaimRequest
}

const makeWorkItem = (
  workload: string,
  index: number,
  distribution: "Hot" | "Distributed"
): WorkItem => {
  const messageNumber = nextMessage++
  const senderPeerId = distribution === "Hot" ? peerId(3) : peerId(nextSenderPeer++)
  const channel = PeerRelayStore.ChannelKey.make({
    tenantId: `benchmark-${workload}`,
    senderSubjectId: "sender",
    senderPeerId,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
    recipientSubjectId: "recipient",
    recipientPeerId
  })
  return {
    admission: PeerRelayStore.Admission.make({
      channel,
      relayMessageId: relayMessageId(messageNumber),
      relayPeerId,
      documentIds: [documentId],
      senderConnectionEpoch: `epoch-${workload}`,
      senderSequence: distribution === "Hot" ? index : 0,
      payloadVersion: 1,
      messageHash: `message-${messageNumber}`,
      outerEnvelopeDigest: PeerRelayRpc.RelayDigest.make(messageNumber.toString(16).padStart(64, "0")),
      payload,
      messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
      senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
      minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis
    }),
    claim: PeerRelayStore.ClaimRequest.make({
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
      authorizedDocumentIds: [documentId]
    })
  }
}

const temporaryDirectory = mkdtempSync(join(tmpdir(), "effect-local-peer-relay-bench-"))
const filename = join(temporaryDirectory, "relay.sqlite")
const scope = await Effect.runPromise(Scope.make())
const baseLayer = Layer.mergeAll(
  SqliteClient.layer({ filename }),
  NodeCrypto.layer,
  PeerRelayLimits.layerDefaults
)
const benchmarkContext = await Effect.runPromise(
  Layer.build(
    Layer.merge(
      baseLayer,
      PeerRelayStore.layerSqlite.pipe(Layer.provide(baseLayer))
    )
  ).pipe(Effect.provideService(Scope.Scope, scope))
)
const store = Context.get(benchmarkContext, PeerRelayStore.PeerRelayStore)

const workloads = new Map<string, ReadonlyArray<WorkItem>>()

for (const backlogSize of backlogSizes) {
  for (const distribution of ["Hot", "Distributed"] as const) {
    const workload = `${distribution.toLowerCase()}-${backlogSize}`
    const items = Array.from(
      { length: backlogSize },
      (_, index) => makeWorkItem(workload, index, distribution)
    )
    workloads.set(workload, items)
  }
}

await Effect.runPromise(
  Effect.forEach(
    Array.from(workloads.values()).flat(),
    ({ admission }) => store.admit(admission),
    { concurrency: 1, discard: true }
  )
)

const drain = (
  items: ReadonlyArray<WorkItem>,
  cursor: { value: number }
) =>
  Effect.gen(function*() {
    for (let offset = 0; offset < drainBatchSize; offset++) {
      const item = items[cursor.value++]
      if (item === undefined) {
        return yield* Effect.die(new Error("PeerRelayStore benchmark exhausted its prepared backlog"))
      }
      const claimed = yield* store.claim(item.claim)
      if (Option.isNone(claimed.message)) {
        return yield* Effect.die(new Error("PeerRelayStore benchmark could not claim prepared work"))
      }
      const message = claimed.message.value
      const acknowledged = yield* store.acknowledge({
        channel: message.channel,
        relayMessageId: message.relayMessageId,
        claimToken: message.claimToken,
        messageHash: message.messageHash,
        sessionGeneration: message.sessionGeneration,
        recipient: item.claim.recipient
      })
      if (acknowledged.status !== "Changed") {
        return yield* Effect.die(new Error("PeerRelayStore benchmark did not acknowledge its active claim"))
      }
    }
  })

for (const backlogSize of backlogSizes) {
  for (const distribution of ["Hot", "Distributed"] as const) {
    const workload = `${distribution.toLowerCase()}-${backlogSize}`
    const items = workloads.get(workload)!
    const cursor = { value: 0 }

    bench(
      `SQLite drain batch=${drainBatchSize}, backlog=${backlogSize}, channels=${distribution.toLowerCase()}, payload=${payloadBytes} bytes`,
      async () => {
        await Effect.runPromise(drain(items, cursor))
      },
      benchmarkOptions
    )
  }
}

for (const distribution of ["Hot", "Distributed"] as const) {
  let sequence = 0
  const workload = `admission-${distribution.toLowerCase()}`

  bench(
    `SQLite admission, channels=${distribution.toLowerCase()}, payload=${payloadBytes} bytes`,
    async () => {
      await Effect.runPromise(
        store.admit(makeWorkItem(workload, sequence++, distribution).admission)
      )
    },
    benchmarkOptions
  )
}

afterAll(async () => {
  await Effect.runPromise(Scope.close(scope, Exit.void))
  rmSync(temporaryDirectory, { recursive: true, force: true })
})
