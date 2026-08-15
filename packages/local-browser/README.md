# @lucas-barake/effect-local-browser

Browser SQLite ports and the joined Effect Atom graph for Effect Local.

`BrowserSqlite.layerMessagePort` adapts an application-owned SQLite WASM worker port. `BrowserReplica.make` creates one
Atom runtime with space-addressed entities, queries, mutations, receipts, settlements, lifecycle operations, and
ephemera. It requires the production `Replica`, `QueryReactivity`, and `EphemeralClient` services in the supplied Layer.

```ts
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const graph = BrowserReplica.make(Layer.merge(layerReplica, EphemeralClient.layer))

const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"))

const Typing = Ephemeral.make("Typing", {
  kind: "event",
  payload: { conversationId: ConversationId, active: Schema.Boolean }
})

const ReadPosition = Ephemeral.make("ReadPosition", {
  kind: "state",
  key: ConversationId,
  payload: { messageId: Schema.String }
})

const Presence = Ephemeral.member({ status: Schema.String })

const member = Protocol.EphemeralMember.make({
  clientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})

export const sessionAtom = graph.ephemeral(Presence, {
  spaceId,
  member,
  value: { status: "online" },
  ttl: "30 seconds"
})

export const typingAtom = graph.ephemeralEvents(sessionAtom, Typing)
export const positionsAtom = graph.ephemeralState(sessionAtom, ReadPosition)
export const rosterAtom = graph.ephemeralMembers(sessionAtom)
export const publishTypingAtom = graph.publishEphemeral(Typing, { spaceId, member })
export const publishPositionAtom = graph.publishEphemeral(ReadPosition, { spaceId, member })
```

A definition is declared once and drives everything: the accepted payload type, the JSON encoding on the wire, the
channel filtering, and the automatic decoding on receive. Application code never supplies channel strings, protocol
tags, or `Schema.decode` calls.

`graph.ephemeral` returns the session atom for one `(space, member)` pair. Mounting any projection derived from it
opens exactly one joined server stream, shared by every typed projection. `ephemeralEvents` resolves to the latest
decoded `{ member, payload }` envelope and only observes events published while it is mounted. `ephemeralState`
resolves to the full decoded entry list for its definition, replayed immediately to late subscribers and updated
last-writer-wins per `(member, key)`. `ephemeralMembers` resolves to the decoded roster using the member definition
supplied at session creation.

Publish command atoms wrap the same typed client operation and expose their progress as an Atom `AsyncResult`, which
is why they are built with `runtime.fn`:

```ts
registry.set(publishTypingAtom, {
  payload: { conversationId: ConversationId.make("conversation-42"), active: true },
  ttl: "5 seconds"
})

registry.set(publishPositionAtom, {
  key: ConversationId.make("conversation-42"),
  payload: { messageId: "message-108" },
  ttl: "1 minute"
})
```

The result atom reports waiting, `Success`, or `Failure` (including a typed `EphemeralEncodeError` when a payload or
key does not satisfy its schema). Commands run concurrently; a later write never interrupts an in-flight publish.

A malformed remote value fails only the projection atom for that definition with a typed `EphemeralDecodeError`. The
shared session and every other projection stay live, and refreshing the failed state projection re-reads the current
session view. Peers running an incompatible schema for one channel therefore surface as an isolated decode failure on
that projection, never as a runtime defect. TTL inputs everywhere accept `Duration.Input`; only the serialized RPC
protocol uses integer `ttlMillis` fields.

The scoped client keeps the private server session capability out of atom state, schedules heartbeats from the
accepted lease, and rejoins with a replacement snapshot when its subscriber misses a revision. Disposing the registry
or evicting the idle session atom closes the joined stream and lets the server emit member departure.

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
