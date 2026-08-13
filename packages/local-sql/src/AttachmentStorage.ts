import type * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"

export const ObjectKey = Schema.String.check(Schema.isPattern(/^[0-9a-f]{32}$/)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/AttachmentObjectKey")
)
export type ObjectKey = typeof ObjectKey.Type

export interface Staged {
  readonly key: ObjectKey
  readonly reference: Attachment.Reference
}

export type StorageFailure =
  | Attachment.AttachmentNotFound
  | Attachment.AttachmentStorageError

export type VerificationFailure =
  | StorageFailure
  | Attachment.AttachmentLengthMismatch
  | Attachment.AttachmentDigestMismatch
  | Attachment.AttachmentTooLarge

export type ReadFailure =
  | StorageFailure
  | Attachment.AttachmentLengthMismatch
  | Attachment.InvalidAttachmentRange

export interface Service {
  readonly create: () => Effect.Effect<ObjectKey, Attachment.AttachmentStorageError>
  readonly stage: <E extends { readonly _tag: string }, R,>(
    bytes: Stream.Stream<Uint8Array, E, R>
  ) => Effect.Effect<Staged, E | Attachment.AttachmentTooLarge | Attachment.AttachmentStorageError, R>
  readonly append: <E extends { readonly _tag: string }, R,>(
    key: ObjectKey,
    reference: Attachment.Reference,
    expectedOffset: number,
    bytes: Stream.Stream<Uint8Array, E, R>
  ) => Effect.Effect<
    number,
    E | StorageFailure | Attachment.AttachmentOffsetConflict | Attachment.AttachmentTooLarge,
    R
  >
  readonly offset: (key: ObjectKey) => Effect.Effect<number, StorageFailure>
  readonly verify: (
    key: ObjectKey,
    reference: Attachment.Reference
  ) => Effect.Effect<void, VerificationFailure>
  readonly read: (
    key: ObjectKey,
    reference: Attachment.Reference,
    range?: Attachment.Range
  ) => Stream.Stream<Uint8Array, ReadFailure>
  readonly exists: (key: ObjectKey) => Effect.Effect<boolean, Attachment.AttachmentStorageError>
  readonly remove: (key: ObjectKey) => Effect.Effect<void, Attachment.AttachmentStorageError>
}

export class AttachmentStorage extends Context.Service<AttachmentStorage, Service>()(
  "@lucas-barake/effect-local-sql/AttachmentStorage"
) {}
