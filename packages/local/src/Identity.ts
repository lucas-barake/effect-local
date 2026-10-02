import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

const identifier = (prefix: string) =>
  Schema.String.check(
    Schema.isPattern(new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`))
  )

const sequence = (minimum: number) => Schema.Int.check(Schema.isGreaterThanOrEqualTo(minimum))

export const SpaceId = identifier("spc").pipe(Schema.brand("@lucas-barake/effect-local/SpaceId"))
export type SpaceId = typeof SpaceId.Type

export const ClientId = identifier("cli").pipe(Schema.brand("@lucas-barake/effect-local/ClientId"))
export type ClientId = typeof ClientId.Type

export const MembershipIncarnation = identifier("inc").pipe(
  Schema.brand("@lucas-barake/effect-local/MembershipIncarnation")
)
export type MembershipIncarnation = typeof MembershipIncarnation.Type

export const MutationId = identifier("mut").pipe(Schema.brand("@lucas-barake/effect-local/MutationId"))
export type MutationId = typeof MutationId.Type

export const SnapshotId = identifier("snp").pipe(Schema.brand("@lucas-barake/effect-local/SnapshotId"))
export type SnapshotId = typeof SnapshotId.Type

export const ReplicationViewId = identifier("viw").pipe(Schema.brand("@lucas-barake/effect-local/ReplicationViewId"))
export type ReplicationViewId = typeof ReplicationViewId.Type

export const WakeId = identifier("wak").pipe(Schema.brand("@lucas-barake/effect-local/WakeId"))
export type WakeId = typeof WakeId.Type

export const LocalSequence = sequence(1).pipe(Schema.brand("@lucas-barake/effect-local/LocalSequence"))
export type LocalSequence = typeof LocalSequence.Type

export const ServerSequence = sequence(0).pipe(Schema.brand("@lucas-barake/effect-local/ServerSequence"))
export type ServerSequence = typeof ServerSequence.Type

export const SettlementSequence = sequence(0).pipe(Schema.brand("@lucas-barake/effect-local/SettlementSequence"))
export type SettlementSequence = typeof SettlementSequence.Type

export const TerminalSequence = sequence(0).pipe(Schema.brand("@lucas-barake/effect-local/TerminalSequence"))
export type TerminalSequence = typeof TerminalSequence.Type

export const VisibleRevision = sequence(0).pipe(Schema.brand("@lucas-barake/effect-local/VisibleRevision"))
export type VisibleRevision = typeof VisibleRevision.Type

export const ReplicationViewRevision = sequence(0).pipe(
  Schema.brand("@lucas-barake/effect-local/ReplicationViewRevision")
)
export type ReplicationViewRevision = typeof ReplicationViewRevision.Type

export const ReplicationScopeGeneration = sequence(0).pipe(
  Schema.brand("@lucas-barake/effect-local/ReplicationScopeGeneration")
)
export type ReplicationScopeGeneration = typeof ReplicationScopeGeneration.Type

export const EphemeralRevision = sequence(0).pipe(Schema.brand("@lucas-barake/effect-local/EphemeralRevision"))
export type EphemeralRevision = typeof EphemeralRevision.Type

export const EphemeralSessionToken = identifier("eps").pipe(
  Schema.brand("@lucas-barake/effect-local/EphemeralSessionToken")
)
export type EphemeralSessionToken = typeof EphemeralSessionToken.Type

export const SchemaVersion = sequence(1).pipe(Schema.brand("@lucas-barake/effect-local/SchemaVersion"))
export type SchemaVersion = typeof SchemaVersion.Type

export const SchemaHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{16}$/)).pipe(
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
export const makeWakeId = makeIdentifier(WakeId, "wak")
export const makeEphemeralSessionToken = makeIdentifier(EphemeralSessionToken, "eps")
