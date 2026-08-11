import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"
import { bench } from "vitest"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const sourceSchema = Identity.SchemaIdentity.make({
  version: Identity.SchemaVersion.make(1),
  hash: Identity.SchemaHash.make("0123456789abcdef")
})
const page: Protocol.PullPage = {
  entries: Array.from({ length: 256 }, (_, index) => ({
    sequence: Identity.ServerSequence.make(index + 1),
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(index).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(index + 1),
    membershipIncarnation: Identity.legacyMembershipIncarnation,
    sourceSchema,
    mutationVersion: Identity.SchemaVersion.make(1),
    digest: "a".repeat(64),
    changes: [{
      _tag: "Upsert" as const,
      entity: { model: "Todo", modelVersion: Identity.SchemaVersion.make(1), key: String(index) },
      value: { id: String(index), title: "x".repeat(512) }
    }]
  })),
  hasMore: true
}
const codec = Schema.toCodecJson(Protocol.PullPage)
const encode = Schema.encodeSync(codec)
const decode = Schema.decodeUnknownSync(codec)
const encoded = encode(page)

bench("pull page Schema encode with 256 entries", () => {
  encode(page)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })

bench("pull page Schema decode with 256 entries", () => {
  decode(encoded)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })

bench("pull page encoded byte accounting with 256 entries", () => {
  Protocol.encodedBytes(page)
}, { iterations: 100, time: 0, warmupIterations: 20, warmupTime: 0 })
