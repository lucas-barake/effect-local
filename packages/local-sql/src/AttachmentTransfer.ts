import type * as Attachment from "@lucas-barake/effect-local/Attachment"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"

export interface UploadRequest {
  readonly spaceId: Identity.SpaceId
  readonly reference: Attachment.Reference
  readonly bytes: Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
}

export interface DownloadRequest {
  readonly spaceId: Identity.SpaceId
  readonly reference: Attachment.Reference
  readonly range?: Attachment.Range
}

export interface Service {
  readonly upload: (request: UploadRequest) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly download: (request: DownloadRequest) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
}

export class AttachmentTransfer extends Context.Service<AttachmentTransfer, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentTransfer"
) {}
