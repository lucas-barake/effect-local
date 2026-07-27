import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Crypto from "effect/Crypto"
import type * as Effect from "effect/Effect"
import type { PeerPrincipal } from "./peerPrincipal.js"

/**
 * The durable inbox of a peer device is addressed by a cluster `EntityId` derived from its full
 * principal. Two properties are load bearing and neither is optional.
 *
 * Injectivity. `subjectId` is caller influenced text of up to 256 characters, so any delimiter
 * joined encoding is forgeable: a subject containing the delimiter can be crafted to collide with
 * a different tenant and subject pair. A collision routes one device's messages into another
 * device's inbox and merges their deduplication namespaces, so it is a confidentiality boundary
 * rather than a routing convenience.
 *
 * Bounded length. This value is both the cluster `EntityId` and the `inbox_key` column, which is a
 * fixed 64 character identity column across all three dialects, so the encoding must be fixed
 * width regardless of how long a principal is.
 *
 * Canonical digesting satisfies both. The principal is serialized as a structured value, which
 * removes delimiter ambiguity entirely, and the digest is a fixed 64 character hex string.
 */
export const encodeInboxKey = (
  principal: PeerPrincipal
): Effect.Effect<string, ReplicaError.ReplicaError, Crypto.Crypto> =>
  Canonical.digest({
    domain: inboxKeyDomain,
    tenantId: principal.tenantId,
    subjectId: principal.subjectId,
    peerId: principal.peerId
  })

/**
 * Domain separator. Keeps inbox keys from ever coinciding with another digest in the system that
 * happens to cover the same fields.
 */
export const inboxKeyDomain = "effect-local/relay-inbox-key"
