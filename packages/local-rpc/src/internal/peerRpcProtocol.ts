import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"

export const maximumNegotiatedDurationMillis = 90 * 24 * 60 * 60 * 1_000
export const maximumRelayPayloadBytes = PeerSyncEnvelope.maximumRelayPayloadBytes
