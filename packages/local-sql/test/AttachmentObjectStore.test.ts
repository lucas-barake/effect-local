import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as AttachmentObjectStore from "../src/AttachmentObjectStore.js"

const reference = Attachment.Reference.make({
  _tag: "Attachment",
  digest: Attachment.Digest.make(
    "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
  ),
  bytes: 5
})

const namespace = AttachmentObjectStore.Namespace.make("primary")
const upload = AttachmentObjectStore.UploadIdentity.make({
  _tag: "UploadIdentity",
  namespace,
  id: AttachmentObjectStore.ProviderId.make("upload-1")
})
const object = AttachmentObjectStore.ObjectIdentity.make({
  _tag: "ObjectIdentity",
  namespace,
  id: AttachmentObjectStore.ProviderId.make("object-1"),
  version: AttachmentObjectStore.ProviderVersion.make("version-1")
})

const adapter: AttachmentObjectStore.Adapter = {
  namespace,
  beginUpload: () => Effect.succeed({ upload, partSize: 5 }),
  listUploadedParts: () => Effect.succeed({ parts: [], nextPartNumber: null }),
  grantUploadPart: () =>
    Effect.succeed({
      expiresAt: 1_900_000_000_000,
      request: AttachmentTransfer.DirectUploadRequest.make({
        method: "PUT",
        url: AttachmentTransfer.GrantUrl.make("https://objects.example/upload"),
        headers: []
      })
    }),
  finalizeUpload: () => Effect.succeed({ object, reference, chunkBytes: 5, chunkCount: 1 }),
  inspectFinalized: () => Effect.succeed({ object, reference, chunkBytes: 5, chunkCount: 1 }),
  listVerifiedChunks: () =>
    Effect.succeed({
      chunks: [{ index: 0, offset: 0, bytes: 5, digest: reference.digest }],
      nextIndex: null
    }),
  grantDownload: () =>
    Effect.succeed({
      expiresAt: 1_900_000_000_000,
      request: AttachmentTransfer.DirectDownloadRequest.make({
        method: "GET",
        url: AttachmentTransfer.GrantUrl.make("https://objects.example/object"),
        headers: []
      })
    }),
  abortUpload: () => Effect.void,
  deleteObject: () => Effect.void
}

describe("attachment object store", () => {
  it.effect(
    "routes durable identities only to their configured namespace",
    Effect.fnUntraced(function*() {
      const registry = yield* AttachmentObjectStore.make({
        namespaceForNewObjects: namespace,
        adapters: [adapter]
      })
      assert.strictEqual(yield* registry.resolve(namespace), adapter)

      const missing = yield* registry.resolve(AttachmentObjectStore.Namespace.make("retired")).pipe(Effect.result)
      assert.isTrue(Result.isFailure(missing))
      if (Result.isFailure(missing)) {
        assert.strictEqual(missing.failure._tag, "AttachmentObjectStoreUnavailable")
        assert.strictEqual(missing.failure.namespace, "retired")
      }
    })
  )

  it.effect(
    "rejects duplicate adapter namespaces",
    Effect.fnUntraced(function*() {
      const result = yield* AttachmentObjectStore.make({
        namespaceForNewObjects: namespace,
        adapters: [adapter, adapter]
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "InvalidConfiguration")
    })
  )

  it.effect(
    "bounds and decodes provider pages and verified manifests",
    Effect.fnUntraced(function*() {
      const partPage = yield* Schema.decodeUnknownEffect(AttachmentObjectStore.UploadedPartPage)({
        parts: [{ partNumber: 1, bytes: 5 }],
        nextPartNumber: null
      })
      assert.deepStrictEqual(partPage.parts, [{ partNumber: 1, bytes: 5 }])

      const oversized = yield* Schema.decodeUnknownEffect(AttachmentObjectStore.VerifiedChunkPage)({
        chunks: Array.from({ length: AttachmentObjectStore.maximumManifestPageChunks + 1 }, (_, index) => ({
          index,
          offset: index,
          bytes: 1,
          digest: reference.digest
        })),
        nextIndex: null
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(oversized))
    })
  )
})
