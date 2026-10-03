import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as Order from "effect/Order"

interface ChannelStates {
  readonly changedAt: number
  readonly entries: HashMap.HashMap<string, Protocol.EphemeralStateEntry>
}

export interface View {
  readonly sequence: number
  readonly resetAt: number
  readonly prunedAt: number
  readonly membersChangedAt: number
  readonly members: HashMap.HashMap<string, Protocol.EphemeralMemberEntry>
  readonly channels: HashMap.HashMap<string, ChannelStates>
}

const memberIdentity = (member: Protocol.EphemeralMember) => `${member.clientId}:${member.membershipIncarnation}`

const stateIdentity = (
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  key: Protocol.EphemeralKey
) =>
  [member.clientId, member.membershipIncarnation, channel, key]
    .map((component) => `${component.length}:${component}`)
    .join("")

export const empty = (sequence: number): View => ({
  sequence,
  resetAt: sequence,
  prunedAt: sequence,
  membersChangedAt: sequence,
  members: HashMap.empty(),
  channels: HashMap.empty()
})

const snapshotView = (sequence: number, message: Protocol.EphemeralSnapshot): View => {
  const grouped = new Map<string, Array<readonly [string, Protocol.EphemeralStateEntry]>>()
  for (const entry of message.states) {
    const identified = [stateIdentity(entry.member, entry.channel, entry.key), entry] as const
    const group = grouped.get(entry.channel)
    if (group === undefined) grouped.set(entry.channel, [identified])
    else group.push(identified)
  }
  const channels = [...grouped].map(([channel, entries]) =>
    [channel, { changedAt: sequence, entries: HashMap.fromIterable(entries) }] as const
  )
  return {
    ...empty(sequence),
    members: HashMap.fromIterable(message.members.map((entry) => [memberIdentity(entry.member), entry] as const)),
    channels: HashMap.fromIterable(channels)
  }
}

export const channelEntries = (view: View, channel: string) =>
  Option.match(HashMap.get(view.channels, channel), {
    onNone: () => HashMap.empty<string, Protocol.EphemeralStateEntry>(),
    onSome: (states) => states.entries
  })

export const channelChangedAt = (view: View, channel: string) =>
  Option.match(HashMap.get(view.channels, channel), {
    onNone: () => view.prunedAt,
    onSome: (states) => states.changedAt
  })

const withChannel = (
  view: View,
  sequence: number,
  channel: string,
  entries: HashMap.HashMap<string, Protocol.EphemeralStateEntry>
): View => {
  if (HashMap.isEmpty(entries)) {
    return { ...view, sequence, prunedAt: sequence, channels: HashMap.remove(view.channels, channel) }
  }
  return { ...view, sequence, channels: HashMap.set(view.channels, channel, { changedAt: sequence, entries }) }
}

export const reduce = (
  current: View | undefined,
  sequence: number,
  message: Exclude<Protocol.EphemeralMessage, Protocol.EphemeralEvent | Protocol.EphemeralEventCleared>
): View | undefined => {
  if (message._tag === "Snapshot") return snapshotView(sequence, message)
  if (current === undefined) return undefined
  if (message._tag === "MemberUpserted") {
    const members = HashMap.set(current.members, memberIdentity(message.entry.member), message.entry)
    return { ...current, sequence, membersChangedAt: sequence, members }
  }
  if (message._tag === "MemberLeft") {
    const members = HashMap.remove(current.members, memberIdentity(message.member))
    return { ...current, sequence, membersChangedAt: sequence, members }
  }
  if (message._tag === "StateSet") {
    const { entry } = message
    const entries = HashMap.set(
      channelEntries(current, entry.channel),
      stateIdentity(entry.member, entry.channel, entry.key),
      entry
    )
    return withChannel(current, sequence, entry.channel, entries)
  }
  const entries = HashMap.remove(
    channelEntries(current, message.channel),
    stateIdentity(message.member, message.channel, message.key)
  )
  return withChannel(current, sequence, message.channel, entries)
}

const byIdentity = <A,>(left: readonly [string, A], right: readonly [string, A]) => Order.String(left[0], right[0])

export const sortedValues = <A,>(entries: HashMap.HashMap<string, A>): ReadonlyArray<A> =>
  [...entries].sort(byIdentity).map(([, entry]) => entry)

interface Projection {
  readonly sequence: number
  readonly fingerprint: string
}

export const noProjection = (): Projection | undefined => undefined

export const projectSlice = <A,>(
  changedAt: (view: View) => number,
  slice: (view: View) => A
) =>
(projection: Projection | undefined, view: View) => {
  if (projection !== undefined && changedAt(view) <= projection.sequence) {
    return [{ ...projection, sequence: view.sequence }, []] as const
  }
  const next = slice(view)
  const fingerprint = Canonical.hash(next)
  if (fingerprint === projection?.fingerprint) return [{ sequence: view.sequence, fingerprint }, []] as const
  return [{ sequence: view.sequence, fingerprint }, [next]] as const
}
