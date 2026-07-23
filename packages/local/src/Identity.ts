import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

const identifier = <const Name extends string,>(name: Name, prefix: string) =>
  Schema.String.check(
    Schema.isPattern(
      new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, "i")
    )
  ).pipe(Schema.brand(`@lucas-barake/effect-local/${name}`))

const sequence = <const Name extends string,>(name: Name) =>
  Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).pipe(Schema.brand(`@lucas-barake/effect-local/${name}`))

export const ReplicaId = identifier("ReplicaId", "rep")
export type ReplicaId = typeof ReplicaId.Type

export const ReplicaIncarnation = sequence("ReplicaIncarnation")
export type ReplicaIncarnation = typeof ReplicaIncarnation.Type

export const SessionId = identifier("SessionId", "ses")
export type SessionId = typeof SessionId.Type

export const DocumentId = identifier("DocumentId", "doc")
export type DocumentId = typeof DocumentId.Type

export const CommandId = identifier("CommandId", "cmd")
export type CommandId = typeof CommandId.Type

export const WriterGeneration = sequence("WriterGeneration")
export type WriterGeneration = typeof WriterGeneration.Type

export const CommitSequence = sequence("CommitSequence")
export type CommitSequence = typeof CommitSequence.Type

export const PeerId = identifier("PeerId", "peer")
export type PeerId = typeof PeerId.Type

export const BackupInstallationId = identifier("BackupInstallationId", "bak")
export type BackupInstallationId = typeof BackupInstallationId.Type

export const ProjectionVersion = Schema.Int.check(Schema.isGreaterThan(0)).pipe(
  Schema.brand("@lucas-barake/effect-local/ProjectionVersion")
)
export type ProjectionVersion = typeof ProjectionVersion.Type

export const makeReplicaId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => ReplicaId.make(`rep_${uuid}`)))
)
export const makeSessionId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => SessionId.make(`ses_${uuid}`)))
)
export const makeDocumentId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => DocumentId.make(`doc_${uuid}`)))
)
export const makeCommandId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`cmd_${uuid}`)))
)
export const makePeerId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => PeerId.make(`peer_${uuid}`)))
)
export const makeBackupInstallationId = Crypto.Crypto.use((crypto) =>
  crypto.randomUUIDv4.pipe(Effect.map((uuid) => BackupInstallationId.make(`bak_${uuid}`)))
)

export const documentIdFromCommandId = (commandId: CommandId): DocumentId =>
  DocumentId.make(`doc_${commandId.slice(4)}`)
