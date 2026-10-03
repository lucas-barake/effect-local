import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as HashMap from "effect/HashMap"
import * as EphemeralView from "../src/internal/ephemeralView.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const member = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

const snapshot = Protocol.EphemeralSnapshot.make({
  spaceId,
  revision: Identity.EphemeralRevision.make(1),
  members: [],
  states: []
})

const stateSet = (revision: number, channel: string, key: string) =>
  Protocol.EphemeralStateSet.make({
    spaceId,
    revision: Identity.EphemeralRevision.make(revision),
    entry: {
      member,
      channel: Protocol.EphemeralChannel.make(channel),
      key: Protocol.EphemeralKey.make(key),
      value: { key },
      expiresAtMillis: 60_000
    }
  })

const stateRemoved = (revision: number, channel: string, key: string) =>
  Protocol.EphemeralStateRemoved.make({
    spaceId,
    revision: Identity.EphemeralRevision.make(revision),
    member,
    channel: Protocol.EphemeralChannel.make(channel),
    key: Protocol.EphemeralKey.make(key)
  })

const apply = (
  view: EphemeralView.View | undefined,
  message: Parameters<typeof EphemeralView.reduce>[2]
): EphemeralView.View => {
  const next = EphemeralView.reduce(view, (view?.sequence ?? 0) + 1, message)
  assert.isDefined(next)
  return next
}

const projectChannel = (channel: string) =>
  EphemeralView.projectSlice(
    (view) => EphemeralView.channelChangedAt(view, channel),
    (view) => EphemeralView.sortedValues(EphemeralView.channelEntries(view, channel))
  )

describe("EphemeralView", () => {
  it("retains no state for channels whose entries were all removed", () => {
    let view = apply(undefined, snapshot)
    for (let index = 0; index < 1_000; index = index + 1) {
      view = apply(view, stateSet(index * 2 + 2, `channel-${index}`, "key"))
      view = apply(view, stateRemoved(index * 2 + 3, `channel-${index}`, "key"))
    }
    assert.strictEqual(HashMap.size(view.channels), 0)
  })

  it("shows a skipped removal that emptied a channel to a projection that only sees later views", () => {
    const project = projectChannel("ReadPosition")
    let view = apply(undefined, snapshot)
    view = apply(view, stateSet(2, "ReadPosition", "conversation-1"))
    const [projection, emitted] = project(EphemeralView.noProjection(), view)
    assert.strictEqual(emitted[0]?.length, 1)
    view = apply(view, stateRemoved(3, "ReadPosition", "conversation-1"))
    for (let index = 0; index < 5; index = index + 1) {
      view = apply(view, stateSet(index + 4, "Sentinel", `probe-${index}`))
    }
    const [, latest] = project(projection, view)
    assert.deepStrictEqual(latest, [[]])
  })
})
