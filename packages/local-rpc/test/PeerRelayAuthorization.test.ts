import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRpcError from "../src/PeerRpcError.js"

const task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})
const note = Document.make("Note", {
  schema: Schema.Struct({ body: Schema.String }),
  version: 1
})
const taskId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const noteId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const otherPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const principal = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "local",
  peerId: localPeerId
})
const remote = PeerRelayAuthorization.RemotePeer.make({
  subjectId: "remote",
  peerId: remotePeerId
})
const resolvedRemote = PeerAuthentication.PeerPrincipal.make({
  tenantId: principal.tenantId,
  subjectId: remote.subjectId,
  peerId: remote.peerId
})
const documents = [
  { documentType: task.name, documentId: taskId },
  { documentType: note.name, documentId: noteId }
] as const

const request = (direction: PeerRelayAuthorization.Direction): PeerRelayAuthorization.Request => ({
  direction,
  principal,
  remote,
  documents
})

const result = (
  selected: PeerRelayAuthorization.Result["documents"] = [
    { document: task, documentId: taskId },
    { document: note, documentId: noteId }
  ],
  endpoint: PeerAuthentication.PeerPrincipal = resolvedRemote,
  validUntil = Number.MAX_SAFE_INTEGER,
  invalidated: Effect.Effect<void> = Effect.void
): PeerRelayAuthorization.Result => ({
  remote: endpoint,
  documents: selected,
  validUntil,
  invalidated
})

const authorizationLayer = (
  authorize: PeerRelayAuthorization.Authorize,
  authorizeUnsafeUnboundedAutomerge3Decode: PeerRelayAuthorization.AuthorizeUnsafeUnboundedAutomerge3Decode =
    PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode
) => PeerRelayAuthorization.layer(authorize, authorizeUnsafeUnboundedAutomerge3Decode)

const unsafeRequest = (
  direction: PeerRelayAuthorization.Direction,
  selectedDocuments: PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest["documents"] = documents
): PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest => ({
  risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
  direction,
  principal,
  remote,
  documents: selectedDocuments
})

const unsafeGrant = (
  overrides: Partial<PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeGrant> = {}
): PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeGrant => ({
  _tag: "UnsafeUnboundedAutomerge3DecodeGrant",
  risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
  principal,
  remote: resolvedRemote,
  direction: "Send",
  documents: [documents[0], documents[1]],
  validUntil: Number.MAX_SAFE_INTEGER,
  invalidated: Effect.void,
  ...overrides
})

describe("PeerRelayAuthorization", () => {
  it.effect("does not promote ordinary authorization into unsafe Automerge decode trust", () => {
    let unsafeCalls = 0
    return Effect.gen(function*() {
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
      yield* authorization.authorize(request("Send"))
      assert.strictEqual(unsafeCalls, 0)
      const error = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode(
        unsafeRequest("Send")
      ).pipe(Effect.flip)
      assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
      assert.strictEqual(unsafeCalls, 1)
    }).pipe(
      Effect.provide(
        authorizationLayer(
          () => Effect.succeed(result()),
          (input) => {
            unsafeCalls++
            return PeerRelayAuthorization.denyUnsafeUnboundedAutomerge3Decode(input)
          }
        )
      )
    )
  })

  it.effect.each(["Send", "Receive"] as const)(
    "validates and canonicalizes the explicit unsafe Automerge decode grant for %s",
    (direction) =>
      Effect.gen(function*() {
        const invalidated = yield* Deferred.make<void>()
        let observed:
          | PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest
          | undefined
        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
          Effect.provide(
            authorizationLayer(
              () => Effect.succeed(result()),
              (input) => {
                observed = input
                return Effect.succeed(unsafeGrant({
                  direction,
                  documents: [documents[0], documents[1]],
                  validUntil: 2_000,
                  invalidated: Deferred.await(invalidated)
                }))
              }
            )
          )
        )
        yield* TestClock.setTime(1_000)
        const grant = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode(
          unsafeRequest(direction, [documents[1], documents[0]])
        )
        assert.deepStrictEqual(observed, unsafeRequest(direction, [documents[1], documents[0]]))
        assert.deepStrictEqual(grant, {
          _tag: "UnsafeUnboundedAutomerge3DecodeGrant",
          risk: "Automerge3.3.2DecodeIsNotAllocationBounded",
          principal,
          remote: resolvedRemote,
          direction,
          documents: [documents[1], documents[0]],
          validUntil: 2_000,
          invalidated: grant.invalidated
        })
        yield* Deferred.succeed(invalidated, undefined)
        yield* grant.invalidated
      })
  )

  it.effect("rejects malformed and duplicate unsafe requests before policy evaluation", () => {
    let calls = 0
    const cases = [
      { ...unsafeRequest("Send"), risk: "AutomergeDecodeIsSafe" },
      { ...unsafeRequest("Send"), direction: "Forward" },
      { ...unsafeRequest("Send"), principal: { ...principal, tenantId: "" } },
      { ...unsafeRequest("Send"), remote: { ...remote, subjectId: "" } },
      { ...unsafeRequest("Send"), documents: [] },
      {
        ...unsafeRequest("Send"),
        documents: [documents[0], documents[0]]
      },
      {
        ...unsafeRequest("Send"),
        documents: [
          documents[0],
          { documentType: note.name, documentId: taskId }
        ]
      },
      {
        ...unsafeRequest("Send"),
        documents: [{ ...documents[0], documentType: "" }]
      },
      {
        ...unsafeRequest("Send"),
        documents: [{ ...documents[0], documentId: "doc_invalid" }]
      }
    ] as const
    return Effect.gen(function*() {
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
      for (const invalid of cases) {
        const error = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode(
          invalid as unknown as PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest
        ).pipe(Effect.flip)
        assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
      }
      assert.strictEqual(calls, 0)
    }).pipe(
      Effect.provide(
        authorizationLayer(
          () => Effect.succeed(result()),
          () => {
            calls++
            return Effect.never
          }
        )
      )
    )
  })

  it.effect("rejects principal remote direction and document substitutions in unsafe grants", () =>
    Effect.gen(function*() {
      const cases = [
        unsafeGrant({ principal: { ...principal, tenantId: "other-tenant" } }),
        unsafeGrant({ principal: { ...principal, subjectId: "other-subject" } }),
        unsafeGrant({ principal: { ...principal, peerId: otherPeerId } }),
        unsafeGrant({ remote: { ...resolvedRemote, tenantId: "other-tenant" } }),
        unsafeGrant({ remote: { ...resolvedRemote, subjectId: "other-subject" } }),
        unsafeGrant({ remote: { ...resolvedRemote, peerId: otherPeerId } }),
        unsafeGrant({ direction: "Receive" }),
        unsafeGrant({ documents: [documents[0]] }),
        unsafeGrant({ documents: [documents[0], documents[0]] }),
        unsafeGrant({
          documents: [
            documents[0],
            { documentType: note.name, documentId: taskId }
          ]
        }),
        unsafeGrant({
          documents: [
            documents[0],
            {
              documentType: note.name,
              documentId: Identity.DocumentId.make(
                "doc_00000000-0000-4000-8000-000000000003"
              )
            }
          ]
        })
      ]
      for (const invalid of cases) {
        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
          Effect.provide(
            authorizationLayer(
              () => Effect.succeed(result()),
              () => Effect.succeed(invalid)
            )
          )
        )
        const error = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode(
          unsafeRequest("Send")
        ).pipe(Effect.flip)
        assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
        assert.deepStrictEqual(Object.keys(error), ["_tag"])
      }
    }))

  const missingTagGrant = Object.fromEntries(
    Object.entries(unsafeGrant()).filter(([key]) => key !== "_tag")
  )

  it.effect.each(
    [
      ["expired", unsafeGrant({ validUntil: 1_000 })],
      ["past", unsafeGrant({ validUntil: 999 })],
      ["NaN", unsafeGrant({ validUntil: Number.NaN })],
      ["positive infinity", unsafeGrant({ validUntil: Number.POSITIVE_INFINITY })],
      ["negative infinity", unsafeGrant({ validUntil: Number.NEGATIVE_INFINITY })],
      ["missing tag", missingTagGrant],
      ["wrong tag", { ...unsafeGrant(), _tag: "UnsafeAutomergeGrant" }],
      ["wrong risk", { ...unsafeGrant(), risk: "AutomergeDecodeIsSafe" }],
      ["missing invalidation", { ...unsafeGrant(), invalidated: undefined }],
      ["malformed invalidation", { ...unsafeGrant(), invalidated: "not-an-effect" }]
    ] as const
  )("rejects an unsafe grant with %s", ([, grant]) =>
    Effect.gen(function*() {
      yield* TestClock.setTime(1_000)
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
      const error = yield* authorization.authorizeUnsafeUnboundedAutomerge3Decode(
        unsafeRequest("Send")
      ).pipe(Effect.flip)
      assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
    }).pipe(
      Effect.provide(
        authorizationLayer(
          () => Effect.succeed(result()),
          () =>
            Effect.succeed(
              grant as unknown as PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeGrant
            )
        )
      )
    ))

  it.effect.each(["Send", "Receive"] as const)(
    "resolves the exact duplex endpoint and document set for %s",
    (direction) => {
      let observed: PeerRelayAuthorization.Request | undefined
      return Effect.gen(function*() {
        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
        const authorized = yield* authorization.authorize(request(direction))
        assert.deepStrictEqual(observed, request(direction))
        assert.deepStrictEqual(authorized.remote, resolvedRemote)
        assert.deepStrictEqual(authorized.documents, [
          { document: note, documentId: noteId },
          { document: task, documentId: taskId }
        ])
      }).pipe(
        Effect.provide(
          authorizationLayer((input) => {
            observed = input
            return Effect.succeed(result([
              { document: note, documentId: noteId },
              { document: task, documentId: taskId }
            ]))
          })
        )
      )
    }
  )

  it.effect("rejects invalid direction endpoint and duplicate request shapes before policy evaluation", () => {
    let calls = 0
    const cases = [
      { ...request("Send"), direction: "Forward" },
      { ...request("Send"), remote: { ...remote, subjectId: "" } },
      { ...request("Send"), remote: { ...remote, peerId: "peer_invalid" } },
      {
        ...request("Send"),
        documents: [documents[0], documents[0]]
      },
      {
        ...request("Send"),
        documents: [
          documents[0],
          { documentType: note.name, documentId: taskId }
        ]
      }
    ] as const

    return Effect.gen(function*() {
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
      for (const invalid of cases) {
        const error = yield* authorization.authorize(
          invalid as unknown as PeerRelayAuthorization.Request
        ).pipe(Effect.flip)
        assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
      }
      assert.strictEqual(calls, 0)
    }).pipe(
      Effect.provide(
        authorizationLayer(() => {
          calls++
          return Effect.never
        })
      )
    )
  })

  it.effect("rejects missing extra duplicate and substituted document grants", () =>
    Effect.gen(function*() {
      const cases: ReadonlyArray<PeerRelayAuthorization.Result["documents"]> = [
        [],
        [
          { document: task, documentId: taskId },
          { document: note, documentId: noteId },
          { document: task, documentId: noteId }
        ],
        [
          { document: task, documentId: taskId },
          { document: task, documentId: taskId }
        ],
        [
          { document: note, documentId: taskId },
          { document: note, documentId: noteId }
        ],
        [
          { document: task, documentId: noteId },
          { document: note, documentId: noteId }
        ]
      ]
      for (const selected of cases) {
        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
          Effect.provide(authorizationLayer(() => Effect.succeed(result(selected))))
        )
        const error = yield* authorization.authorize(request("Send")).pipe(Effect.flip)
        assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
      }
    }))

  it.effect("rejects every substituted resolved endpoint with the same fieldless error", () =>
    Effect.gen(function*() {
      const endpoints = [
        { ...resolvedRemote, tenantId: "other-tenant" },
        { ...resolvedRemote, subjectId: "other-subject" },
        { ...resolvedRemote, peerId: otherPeerId }
      ]
      for (const endpoint of endpoints) {
        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
          Effect.provide(
            authorizationLayer(() => Effect.succeed(result(undefined, endpoint)))
          )
        )
        const error = yield* authorization.authorize(request("Receive")).pipe(Effect.flip)
        assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
        assert.deepStrictEqual(Object.keys(error), ["_tag"])
      }
    }))

  it.effect.each(
    [
      ["expired", 0],
      ["NaN", Number.NaN],
      ["positive infinity", Number.POSITIVE_INFINITY],
      ["negative infinity", Number.NEGATIVE_INFINITY]
    ] as const
  )("rejects a %s authorization lease", ([, validUntil]) =>
    Effect.gen(function*() {
      yield* TestClock.setTime(1_000)
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization
      const error = yield* authorization.authorize(request("Send")).pipe(Effect.flip)
      assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
    }).pipe(
      Effect.provide(
        authorizationLayer(() => Effect.succeed(result(undefined, undefined, validUntil)))
      )
    ))

  it.effect("exposes the finite lease and live invalidation effect without caching authority", () =>
    Effect.gen(function*() {
      const invalidated = yield* Deferred.make<void>()
      let allowed = true
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(
          authorizationLayer(() =>
            allowed
              ? Effect.succeed(result(undefined, undefined, 2_000, Deferred.await(invalidated)))
              : Effect.fail(new PeerRpcError.AccessDenied())
          )
        )
      )
      yield* TestClock.setTime(1_000)
      const grant = yield* authorization.authorize(request("Receive"))
      assert.strictEqual(grant.validUntil, 2_000)
      allowed = false
      yield* Deferred.succeed(invalidated, undefined)
      yield* grant.invalidated
      const error = yield* authorization.authorize(request("Receive")).pipe(Effect.flip)
      assert.deepStrictEqual(error, new PeerRpcError.AccessDenied())
    }))

  it.effect("preserves target independent policy failure and defects", () =>
    Effect.gen(function*() {
      const unavailable = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(
          authorizationLayer(() => Effect.fail(new PeerRpcError.ServerUnavailable()))
        )
      )
      assert.deepStrictEqual(
        yield* unavailable.authorize(request("Send")).pipe(Effect.flip),
        new PeerRpcError.ServerUnavailable()
      )

      const defect = new Error("policy defect")
      const defective = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(authorizationLayer(() => Effect.die(defect)))
      )
      const cause = yield* defective.authorize(request("Send")).pipe(
        Effect.sandbox,
        Effect.flip
      )
      assert.isTrue(Cause.hasDies(cause))
      assert.strictEqual(Cause.squash(cause), defect)

      const unsafeUnavailable = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(
          authorizationLayer(
            () => Effect.succeed(result()),
            () => Effect.fail(new PeerRpcError.ServerUnavailable())
          )
        )
      )
      assert.deepStrictEqual(
        yield* unsafeUnavailable.authorizeUnsafeUnboundedAutomerge3Decode(
          unsafeRequest("Send")
        ).pipe(Effect.flip),
        new PeerRpcError.ServerUnavailable()
      )

      const unsafeDefective = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(
          authorizationLayer(
            () => Effect.succeed(result()),
            () => Effect.die(defect)
          )
        )
      )
      const unsafeCause = yield* unsafeDefective.authorizeUnsafeUnboundedAutomerge3Decode(
        unsafeRequest("Send")
      ).pipe(
        Effect.sandbox,
        Effect.flip
      )
      assert.isTrue(Cause.hasDies(unsafeCause))
      assert.strictEqual(Cause.squash(unsafeCause), defect)
    }))
})
