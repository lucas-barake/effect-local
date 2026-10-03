import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as HashMap from "effect/HashMap"
import * as Option from "effect/Option"
import * as Order from "effect/Order"

type ChannelStates = HashMap.HashMap<string, Protocol.EphemeralStateEntry>

export interface View {
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

export const empty = (): View => ({
  members: HashMap.empty(),
  channels: HashMap.empty()
})

const snapshotView = (message: Protocol.EphemeralSnapshot): View => {
  const grouped = new Map<string, Array<readonly [string, Protocol.EphemeralStateEntry]>>()
  for (const entry of message.states) {
    const identified = [stateIdentity(entry.member, entry.channel, entry.key), entry] as const
    const group = grouped.get(entry.channel)
    if (group === undefined) grouped.set(entry.channel, [identified])
    else group.push(identified)
  }
  const channels = [...grouped].map(([channel, entries]) => [channel, HashMap.fromIterable(entries)] as const)
  return {
    members: HashMap.fromIterable(message.members.map((entry) => [memberIdentity(entry.member), entry] as const)),
    channels: HashMap.fromIterable(channels)
  }
}

export const channelStates = (view: View, channel: string): ChannelStates | undefined =>
  HashMap.get(view.channels, channel).pipe(Option.getOrUndefined)

const withChannel = (view: View, channel: string, entries: ChannelStates): View => {
  if (HashMap.isEmpty(entries)) return { ...view, channels: HashMap.remove(view.channels, channel) }
  return { ...view, channels: HashMap.set(view.channels, channel, entries) }
}

export const reduce = (
  current: View | undefined,
  message: Exclude<Protocol.EphemeralMessage, Protocol.EphemeralEvent | Protocol.EphemeralEventCleared>
): View | undefined => {
  if (message._tag === "Snapshot") return snapshotView(message)
  if (current === undefined) return undefined
  if (message._tag === "MemberUpserted") {
    return { ...current, members: HashMap.set(current.members, memberIdentity(message.entry.member), message.entry) }
  }
  if (message._tag === "MemberLeft") {
    return { ...current, members: HashMap.remove(current.members, memberIdentity(message.member)) }
  }
  if (message._tag === "StateSet") {
    const { entry } = message
    const entries = HashMap.set(
      channelStates(current, entry.channel) ?? HashMap.empty(),
      stateIdentity(entry.member, entry.channel, entry.key),
      entry
    )
    return withChannel(current, entry.channel, entries)
  }
  const existing = channelStates(current, message.channel)
  if (existing === undefined) return current
  const entries = HashMap.remove(existing, stateIdentity(message.member, message.channel, message.key))
  return withChannel(current, message.channel, entries)
}

const byIdentity = <A,>(left: readonly [string, A], right: readonly [string, A]) => Order.String(left[0], right[0])

export const sortedValues = <A,>(entries: HashMap.HashMap<string, A>): ReadonlyArray<A> =>
  [...entries].sort(byIdentity).map(([, entry]) => entry)

export const channelSlice = (view: View, channel: string): ReadonlyArray<Protocol.EphemeralStateEntry> =>
  sortedValues(channelStates(view, channel) ?? HashMap.empty())

interface Projection {
  readonly source: unknown
  readonly fingerprint: string
}

export const noProjection = (): Projection | undefined => undefined

export const projectSlice = <A,>(
  source: (view: View) => unknown,
  slice: (view: View) => A
) =>
(projection: Projection | undefined, view: View) => {
  const current = source(view)
  if (projection !== undefined && current === projection.source) return [projection, []] as const
  const next = slice(view)
  const fingerprint = Canonical.hash(next)
  if (fingerprint === projection?.fingerprint) return [{ source: current, fingerprint }, []] as const
  return [{ source: current, fingerprint }, [next]] as const
}
