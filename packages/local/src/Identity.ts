import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

const identifier = <const Name extends string,>(name: Name, prefix: string) => {
  const pattern = Schema.isPattern(
    new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
  )
  return Schema.String.check(pattern).pipe(Schema.brand(`@lucas-barake/effect-local/${name}`))
}

const sequence = <const Name extends string,>(name: Name, minimum: number) => {
  const minimumCheck = Schema.isGreaterThanOrEqualTo(minimum)
  return Schema.Int.check(minimumCheck).pipe(Schema.brand(`@lucas-barake/effect-local/${name}`))
}

export const SpaceId = identifier("SpaceId", "spc")
export type SpaceId = typeof SpaceId.Type

export const ClientId = identifier("ClientId", "cli")
export type ClientId = typeof ClientId.Type

export const MembershipIncarnation = identifier("MembershipIncarnation", "inc")
export type MembershipIncarnation = typeof MembershipIncarnation.Type

export const legacyMembershipIncarnation = MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000000"
)

export const MutationId = identifier("MutationId", "mut")
export type MutationId = typeof MutationId.Type

export const SnapshotId = identifier("SnapshotId", "snp")
export type SnapshotId = typeof SnapshotId.Type

export const ReplicationViewId = identifier("ReplicationViewId", "viw")
export type ReplicationViewId = typeof ReplicationViewId.Type

export const LocalSequence = sequence("LocalSequence", 1)
export type LocalSequence = typeof LocalSequence.Type

export const ServerSequence = sequence("ServerSequence", 0)
export type ServerSequence = typeof ServerSequence.Type

export const TerminalSequence = sequence("TerminalSequence", 0)
export type TerminalSequence = typeof TerminalSequence.Type

export const VisibleRevision = sequence("VisibleRevision", 0)
export type VisibleRevision = typeof VisibleRevision.Type

export const ReplicationViewRevision = sequence("ReplicationViewRevision", 0)
export type ReplicationViewRevision = typeof ReplicationViewRevision.Type

export const ReplicationScopeGeneration = sequence("ReplicationScopeGeneration", 0)
export type ReplicationScopeGeneration = typeof ReplicationScopeGeneration.Type

export const SchemaVersion = sequence("SchemaVersion", 1)
export type SchemaVersion = typeof SchemaVersion.Type

const schemaHashPattern = Schema.isPattern(/^[0-9a-f]{16}$/)
export const SchemaHash = Schema.String.check(schemaHashPattern).pipe(
  Schema.brand("@lucas-barake/effect-local/SchemaHash")
)
export type SchemaHash = typeof SchemaHash.Type

export const SchemaIdentity = Schema.Struct({
  version: SchemaVersion,
  hash: SchemaHash
})
export type SchemaIdentity = typeof SchemaIdentity.Type

const makeIdentifier = <A,>(schema: { readonly make: (value: string) => A }, prefix: string) =>
  Crypto.Crypto.use((crypto) => crypto.randomUUIDv4.pipe(Effect.map((uuid) => schema.make(`${prefix}_${uuid}`))))

export const makeSpaceId = makeIdentifier(SpaceId, "spc")
export const makeClientId = makeIdentifier(ClientId, "cli")
export const makeMembershipIncarnation = makeIdentifier(MembershipIncarnation, "inc")
export const makeMutationId = makeIdentifier(MutationId, "mut")
export const makeSnapshotId = makeIdentifier(SnapshotId, "snp")
export const makeReplicationViewId = makeIdentifier(ReplicationViewId, "viw")
