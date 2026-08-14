import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

export const maximumProviderIdLength = 2 * 1024
export const maximumProviderVersionLength = 1024
export const maximumUploadedPartPageSize = 1_000
export const maximumManifestPageChunks = 1_000

export const Namespace = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/AttachmentObjectStoreNamespace")
)
export type Namespace = typeof Namespace.Type

export const PhysicalKey = Schema.String.check(Schema.isPattern(/^[0-9a-f]{32}$/)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/AttachmentPhysicalKey")
)
export type PhysicalKey = typeof PhysicalKey.Type

export const ProviderId = Schema.NonEmptyString.check(Schema.isMaxLength(maximumProviderIdLength)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/AttachmentProviderId")
)
export type ProviderId = typeof ProviderId.Type

export const ProviderVersion = Schema.NonEmptyString.check(Schema.isMaxLength(maximumProviderVersionLength)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/AttachmentProviderVersion")
)
export type ProviderVersion = typeof ProviderVersion.Type

export const UploadIdentity = Schema.Struct({
  _tag: Schema.tag("UploadIdentity"),
  namespace: Namespace,
  id: ProviderId
})
export type UploadIdentity = typeof UploadIdentity.Type

export const ObjectIdentity = Schema.Struct({
  _tag: Schema.tag("ObjectIdentity"),
  namespace: Namespace,
  id: ProviderId,
  version: ProviderVersion
})
export type ObjectIdentity = typeof ObjectIdentity.Type

const PositiveBytes = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const PartNumber = PositiveInt.check(Schema.isLessThanOrEqualTo(10_000))
const NullablePartNumber = Schema.NullOr(PartNumber)
const NullableChunkIndex = Schema.NullOr(NonNegativeInt)

export const BegunUpload = Schema.Struct({
  upload: UploadIdentity,
  partSize: PositiveBytes
})
export type BegunUpload = typeof BegunUpload.Type

export const UploadedPart = Schema.Struct({
  partNumber: PartNumber,
  bytes: PositiveBytes
})
export type UploadedPart = typeof UploadedPart.Type

export const UploadedPartPage = Schema.Struct({
  parts: Schema.Array(UploadedPart).check(Schema.isMaxLength(maximumUploadedPartPageSize)),
  nextPartNumber: NullablePartNumber
})
export type UploadedPartPage = typeof UploadedPartPage.Type

export const DirectUploadGrant = Schema.Struct({
  expiresAt: NonNegativeInt,
  request: AttachmentTransfer.DirectUploadRequest
})
export type DirectUploadGrant = typeof DirectUploadGrant.Type

export const VerifiedObject = Schema.Struct({
  object: ObjectIdentity,
  reference: Attachment.Reference,
  chunkBytes: PositiveBytes,
  chunkCount: NonNegativeInt
})
export type VerifiedObject = typeof VerifiedObject.Type

export const VerifiedChunkPage = Schema.Struct({
  chunks: Schema.Array(AttachmentTransfer.VerifiedChunk).check(
    Schema.isMaxLength(maximumManifestPageChunks)
  ),
  nextIndex: NullableChunkIndex
})
export type VerifiedChunkPage = typeof VerifiedChunkPage.Type

export const DirectDownloadGrant = Schema.Struct({
  expiresAt: NonNegativeInt,
  request: AttachmentTransfer.DirectDownloadRequest
})
export type DirectDownloadGrant = typeof DirectDownloadGrant.Type

export class AttachmentObjectStoreUnavailable extends Schema.TaggedErrorClass<AttachmentObjectStoreUnavailable>(
  "@lucas-barake/effect-local-sql/AttachmentObjectStoreUnavailable"
)("AttachmentObjectStoreUnavailable", {
  namespace: Namespace,
  operation: Schema.String,
  cause: Schema.optionalKey(Schema.Defect())
}) {}

export class AttachmentProviderUploadNotFound extends Schema.TaggedErrorClass<AttachmentProviderUploadNotFound>(
  "@lucas-barake/effect-local-sql/AttachmentProviderUploadNotFound"
)("AttachmentProviderUploadNotFound", { upload: UploadIdentity }) {}

export class AttachmentProviderObjectNotFound extends Schema.TaggedErrorClass<AttachmentProviderObjectNotFound>(
  "@lucas-barake/effect-local-sql/AttachmentProviderObjectNotFound"
)("AttachmentProviderObjectNotFound", { object: ObjectIdentity }) {}

export type ProviderError =
  | AttachmentObjectStoreUnavailable
  | AttachmentProviderUploadNotFound
  | AttachmentProviderObjectNotFound

export interface Adapter {
  readonly namespace: Namespace
  readonly beginUpload: (input: {
    readonly spaceId: Identity.SpaceId
    readonly attemptId: AttachmentTransfer.AttemptId
    readonly physicalKey: PhysicalKey
    readonly reference: Attachment.Reference
    readonly verificationChunkBytes: number
  }) => Effect.Effect<BegunUpload, ProviderError>
  readonly listUploadedParts: (input: {
    readonly spaceId: Identity.SpaceId
    readonly upload: UploadIdentity
    readonly afterPartNumber: number
    readonly limit: number
  }) => Effect.Effect<UploadedPartPage, ProviderError>
  readonly grantUploadPart: (input: {
    readonly spaceId: Identity.SpaceId
    readonly upload: UploadIdentity
    readonly partNumber: number
    readonly offset: number
    readonly bytes: number
    readonly expiresAt: number
  }) => Effect.Effect<DirectUploadGrant, ProviderError>
  readonly finalizeUpload: (input: {
    readonly spaceId: Identity.SpaceId
    readonly upload: UploadIdentity
    readonly reference: Attachment.Reference
  }) => Effect.Effect<VerifiedObject, ProviderError>
  readonly inspectFinalized: (input: {
    readonly spaceId: Identity.SpaceId
    readonly upload: UploadIdentity
    readonly reference: Attachment.Reference
  }) => Effect.Effect<VerifiedObject | null, ProviderError>
  readonly listVerifiedChunks: (input: {
    readonly spaceId: Identity.SpaceId
    readonly object: ObjectIdentity
    readonly afterIndex: number
    readonly limit: number
  }) => Effect.Effect<VerifiedChunkPage, ProviderError>
  readonly grantDownload: (input: {
    readonly spaceId: Identity.SpaceId
    readonly object: ObjectIdentity
    readonly chunk: AttachmentTransfer.VerifiedChunk
    readonly expiresAt: number
  }) => Effect.Effect<DirectDownloadGrant, ProviderError>
  readonly abortUpload: (input: {
    readonly spaceId: Identity.SpaceId
    readonly upload: UploadIdentity
  }) => Effect.Effect<void, AttachmentObjectStoreUnavailable>
  readonly deleteObject: (input: {
    readonly spaceId: Identity.SpaceId
    readonly object: ObjectIdentity
  }) => Effect.Effect<void, AttachmentObjectStoreUnavailable>
}

export interface Service {
  readonly namespaceForNewObjects: Namespace
  readonly resolve: (namespace: Namespace) => Effect.Effect<Adapter, AttachmentObjectStoreUnavailable>
}

export class AttachmentObjectStore extends Context.Service<AttachmentObjectStore, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentObjectStore"
) {}

export interface Options {
  readonly namespaceForNewObjects: Namespace
  readonly adapters: ReadonlyArray<Adapter>
}

export const make = Effect.fnUntraced(function*(options: Options): Effect.fn.Return<
  Service,
  ReplicaError.InvalidConfiguration
> {
  const adapters = new Map<Namespace, Adapter>()
  for (const adapter of options.adapters) {
    if (adapters.has(adapter.namespace)) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "attachments.objectStore.adapters",
        message: `Attachment object store namespace ${adapter.namespace} is configured more than once`
      })
    }
    adapters.set(adapter.namespace, adapter)
  }
  if (!adapters.has(options.namespaceForNewObjects)) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: "attachments.objectStore.namespaceForNewObjects",
      message: "The attachment object store namespace for new objects has no configured adapter"
    })
  }
  return AttachmentObjectStore.of({
    namespaceForNewObjects: options.namespaceForNewObjects,
    resolve: (namespace) => {
      const adapter = adapters.get(namespace)
      if (adapter !== undefined) return Effect.succeed(adapter)
      return Effect.fail(
        new AttachmentObjectStoreUnavailable({
          namespace,
          operation: "resolve"
        })
      )
    }
  })
})

export const layer = (options: Options): Layer.Layer<AttachmentObjectStore, ReplicaError.InvalidConfiguration> =>
  Layer.effect(AttachmentObjectStore, make(options))
