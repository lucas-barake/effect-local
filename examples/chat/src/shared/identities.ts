import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"

/**
 * Shared by the relay server, the seeder, and every browser replica. The tokens are the whole
 * "auth" story of this demo: every identity is public so any browser can impersonate any user. A
 * real deployment issues per-user credentials from its own identity system.
 *
 * The relay allows one live session per peer endpoint (the inbox key is tenant + subject + peer,
 * and a new session replaces the previous one for the same endpoint), and a session pushes to
 * exactly one remote. A user who chats with several counterparts concurrently therefore holds one
 * endpoint identity per conversation, which is also how the relay's channel model reads: an inbox
 * channel connects two endpoints.
 */

export const tenantId = "chat-demo"

/**
 * Part of the SharedWorker name, so bumping it forces every tab onto a fresh worker. A
 * SharedWorker keeps the module graph it was born with, and browsers may keep it alive longer
 * than its last tab, so engine code changes during development do not reach a surviving worker
 * until its identity changes. The OPFS database name stays version free — data survives bumps.
 */
export const engineGeneration = 1

export const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-00000000ffff")

export interface ChatUser {
  readonly id: string
  readonly displayName: string
  readonly color: string
}

export const users: ReadonlyArray<ChatUser> = [
  { id: "alice", displayName: "Alice", color: "#7c5cff" },
  { id: "bob", displayName: "Bob", color: "#00a884" },
  { id: "carol", displayName: "Carol", color: "#e8638a" },
  { id: "dave", displayName: "Dave", color: "#e5a50a" }
]

export const userById = (id: string): ChatUser => {
  const found = users.find((candidate) => candidate.id === id)
  if (found === undefined) throw new Error(`unknown chat user ${id}`)
  return found
}

export const counterpartsOf = (id: string): ReadonlyArray<ChatUser> => users.filter((candidate) => candidate.id !== id)

const pairKey = (a: string, b: string): string => [a, b].toSorted().join("|")

const pairTag: Record<string, string> = {
  "alice|bob": "ab",
  "alice|carol": "ac",
  "alice|dave": "ad",
  "bob|carol": "bc",
  "bob|dave": "bd",
  "carol|dave": "cd"
}

export interface PairEndpoint {
  readonly token: string
  readonly principal: {
    readonly tenantId: string
    readonly subjectId: string
    readonly peerId: Identity.PeerId
  }
}

/**
 * The endpoint identity `user` presents on the channel it shares with `other`. Deterministic and
 * hardcoded: the pair tag and the position in the sorted pair make the peer id, and the token
 * follows the same shape so the relay server can enumerate every valid credential.
 */
export const endpointFor = (user: string, other: string): PairEndpoint => {
  const key = pairKey(user, other)
  const tag = pairTag[key]
  if (tag === undefined) throw new Error(`unknown chat pair ${key}`)
  const position = key.startsWith(user) ? "1" : "2"
  return {
    token: `token-${user}-${tag}`,
    principal: {
      tenantId,
      subjectId: `user-${user}`,
      peerId: Identity.PeerId.make(`peer_00000000-0000-4000-8000-000000000${tag}${position}`)
    }
  }
}

export interface ChannelDescriptor {
  readonly pair: readonly [string, string]
  readonly left: PairEndpoint
  readonly right: PairEndpoint
}

/** Every channel in the roster, for the relay server's authenticator and authorization policy. */
export const channels: ReadonlyArray<ChannelDescriptor> = Object.keys(pairTag).map((key) => {
  const [a, b] = key.split("|") as [string, string]
  return { pair: [a, b] as const, left: endpointFor(a, b), right: endpointFor(b, a) }
})

const toHex = (bytes: Uint8Array) => [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")

const derivePairUuid = async (a: string, b: string) => {
  const digest = new Uint8Array(
    await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(`effect-local-chat|${pairKey(a, b)}`))
  )
  const hex = toHex(digest.subarray(0, 16))
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`
}

/**
 * Every replica must agree on the id of the conversation between two users, so the id is a pure
 * function of the sorted pair: sha-256 folded into a uuid shape (version nibble forced to 4 and
 * variant to 8 to satisfy the `CommandId`/`DocumentId` brands). Only the server side seeder ever
 * *creates* documents with these command ids. Two replicas independently creating the same id
 * would fork the Automerge genesis and one side's writes would be shadowed on merge.
 */
export const conversationIdFor = async (a: string, b: string): Promise<Identity.DocumentId> =>
  Identity.documentIdFromCommandId(Identity.CommandId.make(`cmd_${await derivePairUuid(a, b)}`))

export const conversationCommandId = (a: string, b: string): Effect.Effect<Identity.CommandId> =>
  Effect.promise(async () => Identity.CommandId.make(`cmd_${await derivePairUuid(a, b)}`))

export const conversationDocumentId = (a: string, b: string): Effect.Effect<Identity.DocumentId> =>
  Effect.map(conversationCommandId(a, b), Identity.documentIdFromCommandId)
