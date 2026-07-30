import * as Schema from "effect/Schema"

/**
 * The link states shared by every connection status union in this package.
 *
 * Defined once because `PeerConnectionStatus` and `RelayConnectionStatus` both encode
 * `Disconnected`, `Connecting`, and `Connected` onto the wire. Two spellings of one variant drift
 * apart under maintenance with nothing to catch it, since neither module can see the other's tags.
 * Each module still brands its own union over these, so the two remain mutually unassignable.
 */
export const Disconnected = Schema.TaggedStruct("Disconnected", {})

export const Connecting = Schema.TaggedStruct("Connecting", {})

export const Connected = Schema.TaggedStruct("Connected", {})
