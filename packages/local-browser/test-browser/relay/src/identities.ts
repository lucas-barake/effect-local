import * as Identity from "@lucas-barake/effect-local/Identity"

/**
 * The fixed cast for the fixture: one relay, one tenant, two devices.
 *
 * Shared by the relay server and the page so the two cannot drift. The peer ids and the tenant are
 * part of the version 1 handshake, so a disagreement here is a failed connection rather than a
 * mismatch anything reports usefully.
 */

const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-00000000ffff")

export interface DeviceIdentity {
  readonly name: "alpha" | "beta"
  readonly token: string
  readonly relayPeerId: Identity.PeerId
  readonly principal: {
    readonly tenantId: string
    readonly subjectId: string
    readonly peerId: Identity.PeerId
  }
}

export const devices: ReadonlyArray<DeviceIdentity> = [
  {
    name: "alpha",
    token: "token-alpha",
    relayPeerId,
    principal: {
      tenantId: "tenant-fixture",
      subjectId: "device-alpha",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000a1")
    }
  },
  {
    name: "beta",
    token: "token-beta",
    relayPeerId,
    principal: {
      tenantId: "tenant-fixture",
      subjectId: "device-beta",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-0000000000b1")
    }
  }
]

export const deviceByName = (name: string): DeviceIdentity => {
  const found = devices.find((device) => device.name === name)
  if (found === undefined) throw new Error(`unknown device ${name}`)
  return found
}
