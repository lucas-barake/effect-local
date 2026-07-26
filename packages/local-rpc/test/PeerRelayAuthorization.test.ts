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

describe("PeerRelayAuthorization", () => {
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
          PeerRelayAuthorization.layer((input) => {
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
        PeerRelayAuthorization.layer(() => {
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
          Effect.provide(PeerRelayAuthorization.layer(() => Effect.succeed(result(selected))))
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
            PeerRelayAuthorization.layer(() => Effect.succeed(result(undefined, endpoint)))
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
        PeerRelayAuthorization.layer(() => Effect.succeed(result(undefined, undefined, validUntil)))
      )
    ))

  it.effect("exposes the finite lease and live invalidation effect without caching authority", () =>
    Effect.gen(function*() {
      const invalidated = yield* Deferred.make<void>()
      let allowed = true
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(
          PeerRelayAuthorization.layer(() =>
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
          PeerRelayAuthorization.layer(() => Effect.fail(new PeerRpcError.ServerUnavailable()))
        )
      )
      assert.deepStrictEqual(
        yield* unavailable.authorize(request("Send")).pipe(Effect.flip),
        new PeerRpcError.ServerUnavailable()
      )

      const defect = new Error("policy defect")
      const defective = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(PeerRelayAuthorization.layer(() => Effect.die(defect)))
      )
      const cause = yield* defective.authorize(request("Send")).pipe(
        Effect.sandbox,
        Effect.flip
      )
      assert.isTrue(Cause.hasDies(cause))
      assert.strictEqual(Cause.squash(cause), defect)
    }))
})
