import type * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import type * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"

export type Failure =
  | Attachment.AttachmentNotFound
  | Attachment.AttachmentStorageError

export interface Service {
  readonly create: (key: AttachmentStorage.ObjectKey) => Effect.Effect<void, Attachment.AttachmentStorageError>
  readonly offset: (key: AttachmentStorage.ObjectKey) => Effect.Effect<number, Failure>
  readonly write: (
    key: AttachmentStorage.ObjectKey,
    expectedOffset: number,
    bytes: Uint8Array
  ) => Effect.Effect<number, Failure | Attachment.AttachmentOffsetConflict>
  readonly read: (
    key: AttachmentStorage.ObjectKey,
    offset: number,
    length: number
  ) => Effect.Effect<Uint8Array, Failure>
  readonly exists: (key: AttachmentStorage.ObjectKey) => Effect.Effect<boolean, Attachment.AttachmentStorageError>
  readonly remove: (key: AttachmentStorage.ObjectKey) => Effect.Effect<void, Attachment.AttachmentStorageError>
}

export class AttachmentDirectory extends Context.Service<AttachmentDirectory, Service>()(
  "@lucas-barake/effect-local-browser/internal/AttachmentDirectory"
) {}
