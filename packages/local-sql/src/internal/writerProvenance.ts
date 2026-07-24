import * as Automerge from "@automerge/automerge"
import * as Equal from "effect/Equal"
import * as Schema from "effect/Schema"

export const WriterSchemaVersion = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))

export const WriterDefinitionHash = Schema.NonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[\x20-\x21\x23-\x5b\x5d-\x7e]+$/)
)

export const ChangeHash = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{64}$/)
)

export const ChangeProvenance = Schema.Struct({
  changeHash: ChangeHash,
  writerSchemaVersion: WriterSchemaVersion,
  writerDefinitionHash: WriterDefinitionHash
})

export type ChangeProvenance = typeof ChangeProvenance.Type

export const ChangeProvenances = Schema.Array(ChangeProvenance)
export const StoredChangeProvenances = Schema.fromJsonString(ChangeProvenances)

export const canonicalize = (values: ReadonlyArray<ChangeProvenance>): ReadonlyArray<ChangeProvenance> =>
  values.toSorted((left, right) => left.changeHash.localeCompare(right.changeHash))

export const equals = (
  left: ReadonlyArray<ChangeProvenance>,
  right: ReadonlyArray<ChangeProvenance>
): boolean => Equal.equals(canonicalize(left), canonicalize(right))

export const changeHashes = <T,>(document: Automerge.Doc<T>): ReadonlyArray<string> =>
  Automerge.getAllChanges(document).map((bytes) => Automerge.decodeChange(bytes).hash).toSorted()

export const syncMessageChangeHashes = (message: Uint8Array): ReadonlyArray<string> => {
  const hashes = new Set<string>()
  for (const chunk of Automerge.decodeSyncMessage(message).changes) {
    try {
      hashes.add(Automerge.decodeChange(chunk).hash)
    } catch {
      const document = Automerge.load(chunk)
      try {
        for (const bytes of Automerge.getAllChanges(document)) {
          hashes.add(Automerge.decodeChange(bytes).hash)
        }
      } finally {
        Automerge.free(document)
      }
    }
  }
  return [...hashes].toSorted()
}

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
