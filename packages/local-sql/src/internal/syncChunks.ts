import * as Automerge from "@automerge/automerge"

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
    if (bytes.length - offset < 9) throw new Error(`truncated chunk header at ${offset}`)
    for (let index = 0; index < 4; index++) {
      if (bytes[offset + index] !== chunkMagic[index]) {
        throw new Error(`bad chunk magic at ${offset}`)
      }
    }
    const type = bytes[offset + 8]!
    // ULEB128 built with multiplication: `<<` is 32-bit in JS and would wrap on a large chunk.
    let cursor = offset + 9
    let length = 0
    let scale = 1
    let byte: number
    do {
      if (cursor >= bytes.length) throw new Error(`truncated chunk length at ${cursor}`)
      byte = bytes[cursor++]!
      length += (byte & 0x7f) * scale
      scale *= 128
      if (scale > 2 ** 53) throw new Error(`chunk length out of range at ${offset}`)
    } while (byte & 0x80)
    const end = cursor + length
    if (end > bytes.length) throw new Error(`chunk length ${length} overruns buffer at ${offset}`)
    chunks.push({ type, bytes: bytes.subarray(offset, end) })
    offset = end
  }
  return chunks
}

export const decodeSyncChanges = (
  chunks: ReadonlyArray<Uint8Array>
): ReadonlyArray<Automerge.DecodedChange> => {
  const changes = new Map<string, Automerge.DecodedChange>()
  for (const chunk of chunks) {
    for (const part of splitChunks(chunk)) {
      if (part.type === chunkTypeChange || part.type === chunkTypeCompressed) {
        const change = Automerge.decodeChange(part.bytes)
        changes.set(change.hash, change)
      } else if (part.type === chunkTypeDocument) {
        // A whole-document chunk is self-contained: it carries every change it describes, so it
        // loads standalone without the local document supplying dependencies.
        const document = Automerge.load(part.bytes)
        try {
          for (const bytes of Automerge.getAllChanges(document)) {
            const change = Automerge.decodeChange(bytes)
            changes.set(change.hash, change)
          }
        } finally {
          Automerge.free(document)
        }
      } else if (part.type === chunkTypeBundle) {
        Automerge.free(Automerge.load(part.bytes, { allowMissingChanges: true }))
        for (const change of Automerge.readBundle(part.bytes).changes) {
          changes.set(change.hash, change)
        }
      } else {
        // Guessing at a future chunk type would misattribute provenance, which the receiving
        // replica validates count for count.
        throw new Error(`unsupported Automerge chunk type ${part.type}`)
      }
    }
  }
  return [...changes.values()]
}
