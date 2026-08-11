import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"
import { createHash } from "node:crypto"
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
const digest = createHash("sha256")
  .update(Canonical.stringify({ format: 1, changes }))
  .digest("hex")
const page = Protocol.PullPage.make({
  scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
  cursor: Protocol.ReplicationCursor.make({
    viewId: Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001"),
    revision: Identity.ReplicationViewRevision.make(1)
  }),
  serverSequence: Identity.ServerSequence.make(256),
  changes,
  contentBytes: Protocol.encodedBytes(changes),
  digest: Protocol.MutationDigest.make(digest),
  hasMore: true
})
const codec = Schema.toCodecJson(Protocol.PullPage)
const encode = Schema.encodeSync(codec)
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
