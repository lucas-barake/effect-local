# @lucas-barake/effect-local-browser

Browser SQLite ports and the joined Effect Atom graph for Effect Local.

`BrowserSqlite.layerMessagePort` adapts an application-owned SQLite WASM worker port. `BrowserReplica.make` creates one
Atom runtime with space-addressed entities, queries, mutations, receipts, settlements, lifecycle operations, and
ephemera. It requires the production `Replica`, `QueryReactivity`, and `EphemeralClient` services in the supplied Layer.

```ts
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Layer from "effect/Layer"

const graph = BrowserReplica.make(Layer.merge(layerReplica, EphemeralClient.layer))

const member = Protocol.EphemeralMember.make({
  clientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

export const conversationLiveAtom = graph.ephemeral({
  spaceId,
  member,
  value: { status: "online" },
  ttlMillis: 30_000
})

export const publishEphemeralAtom = graph.publishEphemeral
```

The ephemeral atom resolves to:

```ts
interface EphemeralView {
  readonly spaceId: Identity.SpaceId
  readonly revision: Identity.EphemeralRevision
  readonly members: ReadonlyArray<Protocol.EphemeralMemberEntry>
  readonly states: HashMap.HashMap<string, Protocol.EphemeralStateEntry>
  readonly events: ReadonlyArray<Protocol.EphemeralEventEntry>
}
```

The first server snapshot replaces the roster and retained state and clears all live events. Later deltas upsert and
remove entries by member, channel, and key. `MemberLeft` removes that member and their current events while leaving
their retained state until its own server TTL. `EventCleared` and `StateRemoved` clear the matching entry. The browser
stores retained state in Effect `HashMap` so one position update does not scan the full collection. Iterate it with
`HashMap.values(view.states)`. The browser does not run a second TTL store. Server expiry messages are the source of
truth.

Set `graph.publishEphemeral` with any `Protocol.EphemeralPublishRequest`:

```ts
registry.set(graph.publishEphemeral, {
  _tag: "Event",
  spaceId,
  member,
  channel: "typing",
  value: { active: true },
  ttlMillis: 5_000
})

registry.set(graph.publishEphemeral, {
  _tag: "SetState",
  spaceId,
  member,
  channel: "read",
  key: "conversation-42",
  value: { message: 108 },
  ttlMillis: 60_000
})
```

Events reach current subscribers only. Retained state and the roster replay to later joiners. Every request includes a
space, so one atom cannot observe another space. The scoped client keeps the private server session capability out of
atom state, schedules heartbeats from the accepted lease, and rejoins with a replacement snapshot only when that
subscriber misses a revision. Disposing the registry or evicting the idle atom closes the joined stream and
lets the server emit member departure.

Ephemera never enters browser SQLite or the durable mutation log. Persist a read or delivery position with an ordinary
domain mutation when it must survive server restart or the configured state TTL.

The remaining graph families expose `entity`, `query`, `mutation`, `pending`, `receipt`, `settlements`, `scope`,
`setScope`, `activation`, `activate`, `deactivate`, `status`, `spaces`, `join`, `leave`, and the constant-size
`aggregateStatus`. Effect `Reactivity` refreshes only mounted reads whose exact space, entity, or index range changed.
Leaving a space invalidates retained atoms for that address.

Pass either `SqlReplica.layer` or `SqlReplica.layerWorkflow` as the replica portion of the Layer. The Workflow
composition can run in an application-owned dedicated Worker or SharedWorker. `BrowserReplica` does not choose the
Worker URL, database name, credentials, runner storage, or lifecycle policy. `Atom.runtime` stays on the page side when
the application bridges to a worker-owned replica.

See the [repository guide](https://github.com/lucas-barake/effect-local#readme).
