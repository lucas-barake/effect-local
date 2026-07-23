import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthorization from "../src/PeerAuthorization.js"
import * as PeerRpcError from "../src/PeerRpcError.js"

const document = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const otherDocument = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
const otherDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
const principal = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "subject",
  peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
})

describe("PeerAuthorization", () => {
  it.effect("resolves an exactly matching requested document set", () =>
    Effect.gen(function*() {
      const authorization = yield* PeerAuthorization.PeerAuthorization
      const result = yield* authorization.authorize({
        principal,
        documents: [{ documentType: document.name, documentId }]
      })
      assert.deepStrictEqual(result.documents, [{ document, documentId }])
    }).pipe(
      Effect.provide(PeerAuthorization.layer(() =>
        Effect.succeed({
          documents: [{ document, documentId }],
          validUntil: Number.MAX_SAFE_INTEGER,
          invalidated: Effect.void
        })
      ))
    ))

  it.effect("rejects duplicate requests before policy evaluation", () => {
    let calls = 0
    return Effect.gen(function*() {
      const authorization = yield* PeerAuthorization.PeerAuthorization
      const error = yield* authorization.authorize({
        principal,
        documents: [
          { documentType: document.name, documentId },
          { documentType: document.name, documentId }
        ]
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.AccessDenied)
      assert.strictEqual(calls, 0)
    }).pipe(
      Effect.provide(PeerAuthorization.layer(() => {
        calls++
        return Effect.never
      }))
    )
  })

  it.effect("rejects missing extra duplicate and substituted authorization results", () =>
    Effect.gen(function*() {
      const requested = [{ documentType: document.name, documentId }]
      const cases = [
        [],
        [{ document, documentId }, { document: otherDocument, documentId: otherDocumentId }],
        [{ document, documentId }, { document, documentId }],
        [{ document: otherDocument, documentId }],
        [{ document, documentId: otherDocumentId }]
      ]

      for (const documents of cases) {
        const authorization = yield* PeerAuthorization.PeerAuthorization.pipe(
          Effect.provide(
            PeerAuthorization.layer(() =>
              Effect.succeed({ documents, validUntil: Number.MAX_SAFE_INTEGER, invalidated: Effect.void })
            )
          )
        )
        const error = yield* authorization.authorize({ principal, documents: requested }).pipe(Effect.flip)
        assert.instanceOf(error, PeerRpcError.AccessDenied)
      }
    }))

  it.effect("rejects an expired authorization lease", () =>
    Effect.gen(function*() {
      const authorization = yield* PeerAuthorization.PeerAuthorization
      const error = yield* authorization.authorize({
        principal,
        documents: [{ documentType: document.name, documentId }]
      }).pipe(Effect.flip)
      assert.instanceOf(error, PeerRpcError.AccessDenied)
    }).pipe(
      Effect.provide(
        PeerAuthorization.layer(() =>
          Effect.succeed({ documents: [{ document, documentId }], validUntil: 0, invalidated: Effect.void })
        )
      )
    ))
})
