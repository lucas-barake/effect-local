import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import * as Identity from "./Identity.js"
import * as ReplicaError from "./ReplicaError.js"

/**
 * The archive format versions this build can read.
 *
 * Version 2 exists for one reason: a document lineage cannot be dropped silently. An archive whose
 * records carry no lineage restores onto the genesis lineage, which is correct for every archive
 * written before the concept existed and wrong for an archive written after a history rewrite. A
 * build that predates lineage decodes this field against `Schema.Literal(1)`, so declaring 2 makes
 * it reject such an archive outright instead of importing it, landing the document back on genesis
 * and re-enabling the cross lineage merge the rewrite exists to refuse.
 *
 * The read path accepts both, and only an archive that actually carries a non-genesis lineage is
 * written at 2. An archive that carries none loses nothing when a prior build reads it, so raising
 * its version would strand readers for no gain.
 */
export const FormatVersion = Schema.Literals([1, 2])
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
