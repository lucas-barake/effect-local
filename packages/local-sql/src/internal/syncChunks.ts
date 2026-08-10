import * as Automerge from "@automerge/automerge"
import * as Effect from "effect/Effect"
import * as NativeError from "./nativeError.js"

/**
 * Automerge's storage chunk envelope: magic, checksum, one type byte, then a ULEB128 byte length
 * (automerge 3.3.2 rust/automerge/src/storage/chunk.rs:229-262, storage.rs:26). Walked here
 * because a v2 sync message concatenates every change it carries into a single `changes` entry
 * (sync/message_builder.rs:56-61), and no public API decodes that entry without a document that
 * already holds the changes' dependencies — which the outbound callers below do not have.
 */
const chunkMagic = [0x85, 0x6f, 0x4a, 0x83]
const chunkTypeDocument = 0
const chunkTypeChange = 1
const chunkTypeCompressed = 2
const chunkTypeBundle = 3

const splitChunks = (bytes: Uint8Array): Array<{ type: number; bytes: Uint8Array }> => {
  const chunks: Array<{ type: number; bytes: Uint8Array }> = []
  let offset = 0
  while (offset < bytes.length) {
    if (bytes.length - offset < 9) return NativeError.throwError(`truncated chunk header at ${offset}`)
    for (let index = 0; index < 4; index++) {
      if (bytes[offset + index] !== chunkMagic[index]) {
        return NativeError.throwError(`bad chunk magic at ${offset}`)
      }
    }
    const type = bytes[offset + 8]
    // ULEB128 built with multiplication: `<<` is 32-bit in JS and would wrap on a large chunk.
    let cursor = offset + 9
    let length = 0
    let scale = 1
    let byte: number
    do {
      if (cursor >= bytes.length) return NativeError.throwError(`truncated chunk length at ${cursor}`)
      byte = bytes[cursor++]!
      length += (byte & 0x7f) * scale
      scale *= 128
      if (scale > 2 ** 53) return NativeError.throwError(`chunk length out of range at ${offset}`)
    } while (byte & 0x80)
    const end = cursor + length
    if (end > bytes.length) {
      return NativeError.throwError(`chunk length ${length} overruns buffer at ${offset}`)
    }
    chunks.push({ type, bytes: bytes.subarray(offset, end) })
    offset = end
  }
  return chunks
}

export function decodeSyncChanges(
  chunks: ReadonlyArray<Uint8Array>
): ReadonlyArray<Automerge.DecodedChange>
export function decodeSyncChanges(
  chunks: ReadonlyArray<Uint8Array>,
  options: { readonly maxChanges: number }
): ReadonlyArray<Automerge.DecodedChange> | null
export function decodeSyncChanges(
  chunks: ReadonlyArray<Uint8Array>,
  options?: { readonly maxChanges?: number }
): ReadonlyArray<Automerge.DecodedChange> | null {
  const changes = new Map<string, Automerge.DecodedChange>()
  for (const chunk of chunks) {
    for (const part of splitChunks(chunk)) {
      if (part.type === chunkTypeChange || part.type === chunkTypeCompressed) {
        const change = Automerge.decodeChange(part.bytes)
        changes.set(change.hash, change)
        if (options?.maxChanges !== undefined && changes.size > options.maxChanges) return null
      } else if (part.type === chunkTypeDocument) {
        // A whole-document chunk is self-contained: it carries every change it describes, so it
        // loads standalone without the local document supplying dependencies.
        const shouldStop = Effect.runSync(Effect.acquireUseRelease(
          Effect.sync(() => Automerge.load(part.bytes)),
          (document) =>
            Effect.sync(() => {
              if (options?.maxChanges !== undefined && Automerge.stats(document).numChanges > options.maxChanges) {
                return true
              }
              for (const bytes of Automerge.getAllChanges(document)) {
                const change = Automerge.decodeChange(bytes)
                changes.set(change.hash, change)
                if (options?.maxChanges !== undefined && changes.size > options.maxChanges) return true
              }
              return false
            }),
          (document) => Effect.sync(() => Automerge.free(document))
        ))
        if (shouldStop) return null
      } else if (part.type === chunkTypeBundle) {
        Automerge.free(Automerge.load(part.bytes, { allowMissingChanges: true }))
        for (const change of Automerge.readBundle(part.bytes).changes) {
          changes.set(change.hash, change)
          if (options?.maxChanges !== undefined && changes.size > options.maxChanges) return null
        }
      } else {
        // Guessing at a future chunk type would misattribute provenance, which the receiving
        // replica validates count for count.
        return NativeError.throwError(`unsupported Automerge chunk type ${part.type}`)
      }
    }
  }
  return [...changes.values()]
}

const dependencyOrder = (
  changes: ReadonlyArray<Automerge.DecodedChange>
): ReadonlyArray<Automerge.DecodedChange> => {
  const hashes = new Set(changes.map((change) => change.hash))
  const remaining = changes.map((change) =>
    change.deps.reduce(
      (count, dependency) =>
        count + ((() => {
          if (hashes.has(dependency)) return (1)
          return (0)
        })()),
      0
    )
  )
  const dependents = new Map<string, Array<number>>()
  for (let index = 0; index < changes.length; index++) {
    for (const dependency of changes[index].deps) {
      if (!hashes.has(dependency)) continue
      const waiting = dependents.get(dependency) ?? []
      waiting.push(index)
      dependents.set(dependency, waiting)
    }
  }
  const ready = remaining.flatMap((count, index) =>
    (() => {
      if (count === 0) return [index]
      return []
    })()
  )
  const ordered: Array<Automerge.DecodedChange> = []
  for (let cursor = 0; cursor < ready.length; cursor++) {
    const index = ready[cursor]
    const change = changes[index]
    ordered.push(change)
    for (const dependent of dependents.get(change.hash) ?? []) {
      remaining[dependent] = remaining[dependent] - 1
      if (remaining[dependent] === 0) ready.push(dependent)
    }
  }
  if (ordered.length !== changes.length) {
    return NativeError.throwError("cyclic Automerge change dependencies")
  }
  return ordered
}

export const batchSyncMessage = (
  message: Automerge.SyncMessage,
  options: {
    readonly maxChanges: number
    readonly maxBytes: number
    readonly maxMessages: number
    readonly maxTotalBytes: number
  }
): ReadonlyArray<Automerge.SyncMessage> | null => {
  if (message.byteLength > options.maxTotalBytes) return null
  const decoded = Automerge.decodeSyncMessage(message)
  const maxTotalChanges = Math.min(Number.MAX_SAFE_INTEGER, options.maxChanges * options.maxMessages)
  const decodedChanges = decodeSyncChanges(decoded.changes, { maxChanges: maxTotalChanges })
  if (decodedChanges === null) return null
  const changes = dependencyOrder(decodedChanges)
  if (changes.length <= options.maxChanges && message.byteLength <= options.maxBytes) return [message]
  if (changes.length === 0) return null
  if (Math.ceil(changes.length / options.maxChanges) > options.maxMessages) return null

  const encodedChanges = changes.map(Automerge.encodeChange)
  const messages: Array<Automerge.SyncMessage> = []
  let totalBytes = 0
  const append = (candidate: Automerge.SyncMessage) => {
    if (messages.length >= options.maxMessages || totalBytes + candidate.byteLength > options.maxTotalBytes) {
      return false
    }
    messages.push(candidate)
    totalBytes += candidate.byteLength
    return true
  }
  const encode = (candidate: ReadonlyArray<Automerge.Change>) =>
    Automerge.encodeSyncMessage({ ...decoded, changes: [...candidate] })

  for (let start = 0; start < encodedChanges.length;) {
    const maximumEnd = Math.min(start + options.maxChanges, encodedChanges.length)
    const maximum = encode(encodedChanges.slice(start, maximumEnd))
    if (maximum.byteLength <= options.maxBytes) {
      if (!append(maximum)) return null
      start = maximumEnd
      continue
    }
    let lower = start + 1
    let upper = maximumEnd
    let fittedEnd = start
    let fitted: Automerge.SyncMessage | undefined
    while (lower <= upper) {
      const middle = Math.floor((lower + upper) / 2)
      const candidate = encode(encodedChanges.slice(start, middle))
      if (candidate.byteLength <= options.maxBytes) {
        fittedEnd = middle
        fitted = candidate
        lower = middle + 1
      } else {
        upper = middle - 1
      }
    }
    if (fitted === undefined) return null
    if (!append(fitted)) return null
    start = fittedEnd
  }
  return messages
}
