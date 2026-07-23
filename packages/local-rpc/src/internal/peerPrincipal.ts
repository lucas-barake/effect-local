import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Schema from "effect/Schema"

export const PeerPrincipal = Schema.Struct({
  tenantId: Schema.NonEmptyString,
  subjectId: Schema.NonEmptyString,
  peerId: Identity.PeerId
})
export type PeerPrincipal = typeof PeerPrincipal.Type
