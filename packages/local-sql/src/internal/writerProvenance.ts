import * as Automerge from "@automerge/automerge"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Equal from "effect/Equal"
import * as Schema from "effect/Schema"
import * as SyncChunks from "./syncChunks.js"

export const WriterSchemaVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))

export const WriterDefinitionHash = Schema.NonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[\x20-\x21\x23-\x5b\x5d-\x7e]+$/)
)

export const ChangeHash = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{64}$/)
)

export const maximumAuthorizationTokenBytes = 16 * 1_024
export const AuthorizationToken = Schema.Uint8Array.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(maximumAuthorizationTokenBytes)
)
export type AuthorizationToken = typeof AuthorizationToken.Type

export const ChangeProvenance = Schema.Struct({
  changeHash: ChangeHash,
  writerSchemaVersion: WriterSchemaVersion,
  writerDefinitionHash: WriterDefinitionHash
})

export type ChangeProvenance = typeof ChangeProvenance.Type

export const ChangeProvenances = Schema.Array(ChangeProvenance)
export const StoredChangeProvenances = Schema.fromJsonString(ChangeProvenances)

export const CompactCheckpointProvenance = Schema.TaggedStruct("Compact", {
  checkpointHash: ChangeHash,
  lineage: Identity.DocumentLineage,
  heads: Conflict.Heads,
  base: Schema.Union([
    Schema.TaggedStruct("Bootstrap", {}),
    Schema.TaggedStruct("Heads", { baseHeads: Conflict.Heads })
  ]),
  schemaVersion: WriterSchemaVersion,
  writerDefinitionHash: WriterDefinitionHash,
  authorization: Schema.Uint8ArrayFromBase64.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(maximumAuthorizationTokenBytes)
  )
})
export type CompactCheckpointProvenance = typeof CompactCheckpointProvenance.Type

export const CheckpointProvenance = Schema.Union([
  ChangeProvenances,
  CompactCheckpointProvenance
])
export type CheckpointProvenance = typeof CheckpointProvenance.Type
export const StoredCheckpointProvenance = Schema.fromJsonString(
  Schema.toCodecJson(CheckpointProvenance)
)

export const isCompactCheckpoint = (
  provenance: CheckpointProvenance
): provenance is CompactCheckpointProvenance => !Array.isArray(provenance)

export const exactEntries = (
  provenance: CheckpointProvenance
): ReadonlyArray<ChangeProvenance> => Array.isArray(provenance) ? provenance : []

export const canonicalize = (values: ReadonlyArray<ChangeProvenance>): ReadonlyArray<ChangeProvenance> =>
  values.toSorted((left, right) => left.changeHash.localeCompare(right.changeHash))

export const equals = (
  left: ReadonlyArray<ChangeProvenance>,
  right: ReadonlyArray<ChangeProvenance>
): boolean => Equal.equals(canonicalize(left), canonicalize(right))

export const changeHashes = <T,>(document: Automerge.Doc<T>): ReadonlyArray<string> =>
  Automerge.getAllChanges(document).map((bytes) => Automerge.decodeChange(bytes).hash).toSorted()

export const syncMessageChangeHashes = (message: Uint8Array): ReadonlyArray<string> =>
  SyncChunks.decodeSyncChanges(Automerge.decodeSyncMessage(message).changes)
    .map((change) => change.hash)
    .toSorted()

export const backfill = (
  changeHashes: ReadonlyArray<string>,
  entries: Iterable<ChangeProvenance>,
  fallback: Pick<ChangeProvenance, "writerDefinitionHash" | "writerSchemaVersion">
): ReadonlyArray<ChangeProvenance> => {
  const required = new Set(changeHashes)
  const byHash = new Map<string, ChangeProvenance>()
  for (const entry of entries) {
    if (!required.has(entry.changeHash)) continue
    const existing = byHash.get(entry.changeHash)
    if (
      existing !== undefined &&
      (
        existing.writerSchemaVersion !== entry.writerSchemaVersion ||
        existing.writerDefinitionHash !== entry.writerDefinitionHash
      )
    ) {
      throw new TypeError(`Conflicting writer provenance for change ${entry.changeHash}`)
    }
    byHash.set(entry.changeHash, entry)
  }
  return [...required].toSorted().map((changeHash) =>
    byHash.get(changeHash) ?? {
      changeHash,
      writerSchemaVersion: fallback.writerSchemaVersion,
      writerDefinitionHash: fallback.writerDefinitionHash
    }
  )
}

export const resolve = (
  changeHashes: ReadonlyArray<string>,
  entries: Iterable<ChangeProvenance>
): ReadonlyArray<ChangeProvenance> => {
  const required = new Set(changeHashes)
  const byHash = new Map<string, ChangeProvenance>()
  for (const entry of entries) {
    if (!required.has(entry.changeHash)) continue
    const existing = byHash.get(entry.changeHash)
    if (
      existing !== undefined &&
      (
        existing.writerSchemaVersion !== entry.writerSchemaVersion ||
        existing.writerDefinitionHash !== entry.writerDefinitionHash
      )
    ) {
      throw new TypeError(`Conflicting writer provenance for change ${entry.changeHash}`)
    }
    byHash.set(entry.changeHash, entry)
  }
  return [...required].toSorted().map((changeHash) => {
    const entry = byHash.get(changeHash)
    if (entry === undefined) throw new TypeError(`Missing writer provenance for change ${changeHash}`)
    return entry
  })
}

export const validateExact = (
  changeHashes: ReadonlyArray<string>,
  entries: ReadonlyArray<ChangeProvenance>
): ReadonlyArray<ChangeProvenance> => {
  const resolved = resolve(changeHashes, entries)
  if (resolved.length !== entries.length) {
    throw new TypeError("Checkpoint writer provenance contains duplicate or unrelated changes")
  }
  return resolved
}
