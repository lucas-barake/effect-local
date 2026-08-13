import { NodeCrypto } from "@effect/platform-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { bench } from "vitest"

const changes: ReadonlyArray<Protocol.ViewChange> = Array.from({ length: 256 }, (_, index) =>
  Protocol.Upsert.make({
    entity: Protocol.EntityKey.make({
      model: "Todo",
      modelVersion: Identity.SchemaVersion.make(1),
      key: String(index)
    }),
    value: { id: String(index), title: "x".repeat(512) }
  }))
const digest = Protocol.viewChangesDigest(changes).pipe(
  Effect.provide(NodeCrypto.layer),
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Benchmark module setup requires the synchronous digest before registering benchmark callbacks.
  Effect.runSync
)
const page = Protocol.PullPage.make({
  scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
  cursor: Protocol.ReplicationCursor.make({
    viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
    revision: Identity.ReplicationViewRevision.make(1)
  }),
  serverSequence: Identity.ServerSequence.make(256),
  changes,
  contentBytes: Protocol.encodedBytes(changes),
  digest,
  hasMore: true,
  serverSchema: Identity.SchemaIdentity.make({
    version: Identity.SchemaVersion.make(1),
    hash: Identity.SchemaHash.make("0123456789abcdef")
  })
})
const codec = Schema.toCodecJson(Protocol.PullPage)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- This benchmark intentionally measures the synchronous Schema encoder.
const encode = Schema.encodeSync(codec)
// oxlint-disable-next-line effect-local/noManualEffectBoundary -- This benchmark intentionally measures the synchronous Schema decoder.
const decode = Schema.decodeUnknownSync(codec)
const encoded = encode(page)

bench("pull page Schema encode with 256 changes", () => {
  encode(page)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })

bench("pull page Schema decode with 256 changes", () => {
  decode(encoded)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })

bench("pull page encoded byte accounting with 256 changes", () => {
  Protocol.encodedBytes(page)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })
