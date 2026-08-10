import * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as WriterProvenance from "../src/internal/writerProvenance.js"

describe("WriterProvenance.syncMessageChangeHashes", () => {
  /**
   * Automerge's v2 sync protocol concatenates every change a message carries into one chunk when
   * the peer is more than one change behind. Every reply after a reconnect has this shape because
   * a fresh session restarts the peer's sync state. Decoding must cover all of them, or the relay
   * outbox rejects the replica's own message and the session crash-loops.
   */
  it.effect("decodes every change of a concatenated multi-change sync message", () =>
    Effect.gen(function*() {
      let document = Automerge.from<{ value: { labels: Array<string> } }>(
        { value: { labels: [] } },
        { actor: "1".repeat(32) }
      )
      let stale: Automerge.Doc<{ value: { labels: Array<string> } }> | undefined
      yield* Effect.ensuring(
        Effect.sync(() => {
          for (let index = 0; index < 8; index++) {
            document = Automerge.change(document, (draft) => {
              draft.value.labels.push(`base-${index}`)
            })
          }
          // Keep a peer at the shared history so applying the encoded chunk proves it is a valid
          // sync payload for a stale receiver.
          stale = Automerge.clone(document, { actor: "2".repeat(32) })
          const changes: Array<Uint8Array> = []
          const expected: Array<string> = []
          for (const label of ["newest-1", "newest-2"]) {
            document = Automerge.change(document, (draft) => {
              draft.value.labels.push(label)
            })
            const change = Automerge.getLastLocalChange(document)!
            changes.push(change)
            expected.push(Automerge.decodeChange(change).hash)
          }
          const concatenated = new Uint8Array(changes.reduce((total, change) => total + change.byteLength, 0))
          let offset = 0
          for (const change of changes) {
            concatenated.set(change, offset)
            offset += change.byteLength
          }
          const message = Automerge.encodeSyncMessage({
            heads: Automerge.getHeads(document),
            need: [],
            have: [],
            changes: [concatenated]
          })
          const decoded = Automerge.decodeSyncMessage(message)
          assert.deepStrictEqual(decoded.changes, [concatenated])
          stale = Automerge.receiveSyncMessage(stale, Automerge.initSyncState(), message)[0]
          assert.deepStrictEqual(new Set(Automerge.getHeads(stale)), new Set(Automerge.getHeads(document)))
          assert.deepStrictEqual(WriterProvenance.syncMessageChangeHashes(message), expected.toSorted())
        }),
        Effect.sync(() => {
          Automerge.free(document)
          if (stale !== undefined) Automerge.free(stale)
        })
      )
    }))

  it.effect("decodes every change of a bundle sync message", () =>
    Effect.gen(function*() {
      let document = Automerge.from<{ labels: Array<string> }>(
        { labels: [] },
        { actor: "1".repeat(32) }
      )
      let received = Automerge.init<{ labels: Array<string> }>({ actor: "2".repeat(32) })
      yield* Effect.ensuring(
        Effect.sync(() => {
          for (const label of ["one", "two", "three"]) {
            document = Automerge.change(document, (draft) => {
              draft.labels.push(label)
            })
          }
          const expected = Automerge.getAllChanges(document).map((change) => Automerge.decodeChange(change).hash)
          const bundle = Automerge.saveBundle(document, expected)
          const message = Automerge.encodeSyncMessage({
            heads: Automerge.getHeads(document),
            need: [],
            have: [],
            changes: [bundle]
          })
          const next = Automerge.receiveSyncMessage(received, Automerge.initSyncState(), message)[0]
          received = next
          assert.strictEqual(Automerge.getAllChanges(received).length, expected.length)

          assert.deepStrictEqual(WriterProvenance.syncMessageChangeHashes(message), expected.toSorted())
        }),
        Effect.sync(() => {
          Automerge.free(document)
          Automerge.free(received)
        })
      )
    }))

  it.effect("rejects a bundle sync message with an invalid checksum", () =>
    Effect.gen(function*() {
      let document = Automerge.from<{ labels: Array<string> }>(
        { labels: [] },
        { actor: "1".repeat(32) }
      )
      yield* Effect.ensuring(
        Effect.sync(() => {
          document = Automerge.change(document, (draft) => {
            draft.labels.push("one")
          })
          const hashes = Automerge.getAllChanges(document).map((change) => Automerge.decodeChange(change).hash)
          const bundle = Automerge.saveBundle(document, hashes).slice()
          bundle[4] ^= 0xff
          const message = Automerge.encodeSyncMessage({
            heads: Automerge.getHeads(document),
            need: [],
            have: [],
            changes: [bundle]
          })
          assert.throws(() => WriterProvenance.syncMessageChangeHashes(message), /bad checksum/)
        }),
        Effect.sync(() => Automerge.free(document))
      )
    }))
})
