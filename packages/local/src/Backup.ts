import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"

export const FormatVersion = Schema.Literal(1)
export type FormatVersion = typeof FormatVersion.Type

export const Header = Schema.Struct({
  formatVersion: FormatVersion,
  definitionHash: Schema.String,
  replicaId: Identity.ReplicaId,
  incarnation: Identity.ReplicaIncarnation,
  createdAt: Schema.String
})
export type Header = typeof Header.Type

export const MaxBytes = Schema.Int.check(Schema.isGreaterThan(0))

export const validateMaxBytes = (maxBytes: number): Effect.Effect<number, ReplicaError.ReplicaError> =>
  Schema.decodeEffect(MaxBytes)(maxBytes).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.BackupInvalid({
          cause
        })
      })
    )
  )

export interface ExportOptions {
  readonly maxBytes: number
}

export interface RestoreOptions<R,> {
  readonly source: Stream.Stream<Uint8Array, ReplicaError.ReplicaError, R>
  readonly mode: "clone" | "replace"
  readonly maxBytes: number
  readonly expectedDefinitionHash: string
  readonly installationId: Identity.BackupInstallationId
}

export interface ExportedDocument<E,> {
  readonly documentName: string
  readonly schemaVersion: number
  readonly value: E
}
