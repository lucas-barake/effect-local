import * as Automerge from "@automerge/automerge"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"

export interface Root<E,> {
  value: E
  tombstone: boolean
}

export type AnyDocument = Automerge.Doc<Root<any>>

export interface Change {
  readonly hash: string
  readonly actor: string
  readonly sequence: number
  readonly dependencies: ReadonlyArray<string>
  readonly bytes: Uint8Array
}

export const decode = (bytes: Uint8Array): Change => {
  const decoded = Automerge.decodeChange(bytes)
  return {
    hash: decoded.hash,
    actor: decoded.actor,
    sequence: decoded.seq,
    dependencies: decoded.deps,
    bytes
  }
}

const digestActor = (input: string): string => {
  let first = 0xcbf29ce484222325n
  let second = 0x84222325cbf29ce4n
  for (let index = 0; index < input.length; index++) {
    const character = BigInt(input.charCodeAt(index))
    first = BigInt.asUintN(64, (first ^ character) * 0x100000001b3n)
    second = BigInt.asUintN(64, (second ^ character) * 0x100000001b3n)
  }
  return `${first.toString(16).padStart(16, "0")}${second.toString(16).padStart(16, "0")}`
}

export const actorId = (
  replicaId: Identity.ReplicaId,
  generation: Identity.WriterGeneration,
  documentId: Identity.DocumentId
): string => digestActor(`${replicaId}:${generation}:${documentId}`)

/**
 * The actor for a re-rooted document. The lineage participates so two rewrites of the same
 * document under one writer generation start distinct actor chains: both produce a genesis change
 * at sequence 1, and `UNIQUE(document_id, actor, sequence)` would otherwise reject the second, or
 * a restored archive would collide with a live row carrying a different hash for the same key.
 */
export const rewriteActorId = (
  replicaId: Identity.ReplicaId,
  generation: Identity.WriterGeneration,
  documentId: Identity.DocumentId,
  lineage: Identity.DocumentLineage
): string => digestActor(`${replicaId}:${generation}:${documentId}:${lineage}`)

export const initialize = <E,>(value: E, actor: string): Automerge.Doc<Root<E>> =>
  Automerge.change(Automerge.init<Root<E>>({ actor }), (draft) => {
    draft.value = value
    draft.tombstone = false
  })

export const empty = <E,>(actor: string): Automerge.Doc<Root<E>> => Automerge.init<Root<E>>({ actor })

/**
 * Builds a fresh document carrying `value` and `tombstone` in a single change.
 *
 * `initialize` cannot be reused for a history rewrite: it hardcodes `tombstone = false`, and the
 * flag lives in the root beside `value` rather than inside it, so a rewritten deleted document
 * would come back alive and then fail `Recovery`'s tombstone check against its own row. Writing
 * both fields in one `Automerge.change` also keeps the re-rooted document at exactly one change,
 * which is what the checkpoint's writer provenance is validated against.
 */
export const reroot = <E,>(value: E, tombstone: boolean, actor: string): Automerge.Doc<Root<E>> =>
  Automerge.change(Automerge.init<Root<E>>({ actor }), (draft) => {
    draft.value = value
    draft.tombstone = tombstone
  })

export const stage = <E,>(
  durable: Automerge.Doc<Root<E>>,
  actor: string,
  change: (draft: Mutation.DraftValue<E>) => void
): Automerge.Doc<Root<E>> =>
  Automerge.change(Automerge.clone(durable, { actor }), (draft) => change(draft.value as Mutation.DraftValue<E>))

export const stageValue = <E,>(
  durable: Automerge.Doc<Root<E>>,
  actor: string,
  value: E
): Automerge.Doc<Root<E>> =>
  Automerge.change(Automerge.clone(durable, { actor }), (draft) => {
    draft.value = value
  })

export const stageTombstone = <E,>(
  durable: Automerge.Doc<Root<E>>,
  actor: string
): Automerge.Doc<Root<E>> =>
  Automerge.change(Automerge.clone(durable, { actor }), (draft) => {
    draft.tombstone = true
  })

export const changesSince = <E,>(
  staged: Automerge.Doc<Root<E>>,
  durableHeads: ReadonlyArray<string>
): ReadonlyArray<Change> => Automerge.getChangesSince(staged, [...durableHeads]).map(decode)

export const replay = <E,>(
  base: Automerge.Doc<Root<E>>,
  changes: ReadonlyArray<Uint8Array>
): Automerge.Doc<Root<E>> => Automerge.applyChanges(base, [...changes])[0]

/**
 * Forks a new handle over the same state. The actor is required: `Automerge.clone`
 * generates a random one when it is omitted, which would drop the replica scoped
 * actor every handle returned from this module carries.
 */
export const clone = <E,>(document: Automerge.Doc<Root<E>>, actor: string): Automerge.Doc<Root<E>> =>
  Automerge.clone(document, { actor })

export const heads = <E,>(document: Automerge.Doc<Root<E>>): ReadonlyArray<string> => Automerge.getHeads(document)

export const save = <E,>(document: Automerge.Doc<Root<E>>): Uint8Array => Automerge.save(document)

export const load = <E,>(bytes: Uint8Array, actor: string): Automerge.Doc<Root<E>> =>
  Automerge.load<Root<E>>(bytes, { actor })

export const value = <E,>(document: Automerge.Doc<Root<E>>): E => Automerge.toJS(document).value

export const tombstone = <E,>(document: Automerge.Doc<Root<E>>): boolean => Automerge.toJS(document).tombstone

export const free = <E,>(document: Automerge.Doc<Root<E>>): void => Automerge.free(document)
